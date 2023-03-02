//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// Z Database

// Notes on replication and failover:

// voted (connected, associated and heartbeated) hosts are sorted in
// priority order (i.e. dbState then priority):
//   first-ranked is master
//   second-ranked is master's next
//   third-ranked is second-ranked's next
//   etc.

// a new next is identified and recovery/replication restarts when
// * an election ends
// * a new host heartbeats for first time after election completes
// * an existing host disconnects

// a new master is identified (and the local instance may activate/deactivate)
// when:
// * an election ends
// * a new host heartbeats for first time after election completes
//   - possible deactivation of local instance only -
//   - if self is master and the new host < this one, we just heartbeat it
// * an existing host disconnects (if that is master, a new election begins)

// if replicating from primary to DR and a down secondary comes back up,
// then primary's m_next will be DR and DR's m_next will be secondary

// if master and not replicating, then no host is a replica, so master runs
// as standalone until peers have recovered

#include <zlib/Zdb.hpp>

#include <zlib/ZtBitWindow.hpp>

#include <zlib/ZiDir.hpp>
#include <zlib/ZiRx.hpp>
#include <zlib/ZiTx.hpp>

#include <assert.h>
#include <errno.h>

namespace Zdb_ {

void Env::init(ZdbEnvCf config, ZiMultiplex *mx, EnvHandler handler)
{
  if (!ZmEngine<Env>::lock(
	ZmEngineState::Stopped, [this, &config, mx, &handler]() {
    if (state() != HostState::Instantiated) return false;

    auto invalidSID = [mx](unsigned sid) -> bool {
      return !sid ||
	  sid > mx->params().nThreads() ||
	  sid == mx->rxThread() ||
	  sid == mx->txThread();
    };

    config.sid = mx->sid(config.thread);
    if (invalidSID(config.sid))
      throw ZtString{} <<
	"ZdbEnv thread misconfigured: " << config.thread;

    if (!config.fileThread)
      config.fileSID = config.sid;
    else {
      config.fileSID = mx->sid(config.fileThread);
      if (invalidSID(config.fileSID))
	throw ZtString{} <<
	  "ZdbEnv fileThread misconfigured: " << config.fileThread;
    }

    {
      auto i = config.dbCfs.readIterator();
      while (dbCf_ = i.iterate()) {
	const auto &dbCf = dbCf_->val();
	if (!dbCf.thread)
	  dbCf.sid = config.sid;
	else {
	  dbCf.sid = mx->sid(dbCf.thread);
	  if (invalidSID(dbCf.sid))
	    throw ZtString{} <<
	      "Zdb " << dbCf.id << " thread misconfigured: " << dbCf.thread;
	}
	if (!dbCf.fileThread)
	  dbCf.fileSID = config.fileSID;
	else {
	  dbCf.fileSID = mx->sid(dbCf.fileThread);
	  if (invalidSID(dbCf.fileSID))
	    throw ZtString{} <<
	      "Zdb " << dbCf.id <<
	      " fileThread misconfigured: " << dbCf.fileThread;
	}
      }
    }

    m_cf = ZuMv(config);
    m_mx = mx;

    m_handler = ZuMv(handler);

    return true;
  }))
    throw ZtString{} << "ZdbEnv::init called out of order";
}

void Env::final()
{
  if (!ZmEngine<Env>::lock(ZmEngineState::Stopped, [this]() {
    if (state() != HostState::Instantiated) return false;
    allDBs_([](DB *db) { db->final(); });
    m_handler = {};
    return true;
  }))
    throw ZtString{} << "ZdbEnv::final called out of order";
}

DB *Env::db(ZuID id, DBHandler handler)
{
  DB *db;
  if (!ZmEngine<Env>::lock(ZmEngineState::Stopped,
	[this, &db, handler = ZuMv(handler)]() {
    if (state() != HostState::Initialized) return false;
    db = db_(id, ZuMv(handler));
    return true;
  }))
    throw ZtString{} << "ZdbEnv::db called out of order";
  return db;
}

DB *Env::db_(ZuID id, DBHandler handler)
{
  DB *db;
  if (db = m_dbs.findPtr(id)) return db;
  auto cf = m_cf.dbCfs.find(id);
  if (!cf) m_cf.dbCfs.addNode(cf = new ZdbCfs::Node{id});
  db = new Zdbs::Node{this, &(cf->val())};
  db->init(DBHandler{});
  m_dbs.addNode(db);
  return db;
}

void Env::wake()
{
  run([this]() { stopped(); });
}

void Env::start_()
{
  if (state() != HostState::Initialized) {
    ZeLOG(Fatal, "Env::start_() called out of order");
    started(false);
    return;
  }

  ZeLOG(Info, "Zdb starting");

  // initialize hosts, connections
  // finalize connections, hosts
  m_hosts = new Hosts{};
  m_hostIndex.clean();
  {
    unsigned dbCount = m_dbs.count_();
    auto i = m_cf.hostCfs.readIterator();
    while (auto node = i.iterate()) {
      auto host = new Hosts::Node{this, &(node->data()), dbCount}
      m_hosts->addNode(host);
      m_hostIndex.addNode(host);
    }
  }
  m_self = m_hosts.findPtr(m_cf.hostID);
  if (!m_self) {
    ZeLOG(Fatal, (ZtString{} <<
      "Zdb own host ID " << m_cf.hostID << " not in hosts table"));
    started(false);
    return;
  }
  m_self->state(HostState::Initialized);

  // open and recover all databases
  {
    bool ok = true; 
    allDBs_([&ok](DB *db) { if (ZuLikely(ok)) ok &&= db->open(); });
    if (!ok) {
      allDBs_([](DB *db) { db->close(); });
      started(false);
      return;
    }
  }

  // refresh db state vector, begin election
  dbStateRefresh();
  m_self->state(Stopped);
  stopReplication();
  m_self->state(Electing);

  if (!(m_nPeers = m_hosts.count_() - 1)) { // standalone
    holdElection();
    return;
  }

  run([this]() { hbSend(); },
      m_hbSendTime = ZmTimeNow(), &m_hbSendTimer);
  run([this]() { holdElection(); },
      ZmTimeNow((int)m_cf.electionTimeout), &m_electTimer);

  listen();

  {
    auto i = m_hostIndex.readIterator<ZmRBTreeLess>(Host::IndexAxor(*m_self));
    while (Host *host = i.iterate()) host->connect();
  }
}

void Env::stop_()
{
  ZmAssert(invoked());

  using namespace HostState;

  switch (state()) {
    case Active:
    case Inactive:
      break;
    case Electing:	// holdElection will resume stop_() at completion
      return;
    default:
      ZeLOG(Fatal, "Env::stop_ called out of order");
      stopped(false);
      return;
  }

  ZeLOG(Info, "Zdb stopping");

  stop_1();
}

void Env::stop_1()
{
  ZmAssert(invoked());

  state(Stopping);
  stopReplication();
  m_mx->del(&m_hbSendTimer);
  m_mx->del(&m_electTimer);

  // cancel reconnects
  {
    auto i = m_hostIndex.readIterator<ZmRBTreeLess>(Host::IndexAxor(*m_self));
    while (Host *host = i.iterate()) host->cancelConnect();
  }

  stopListening();

  // close all connections (and wait for them to be disconnected)
  if (!disconnectAll()) stop_2();
}

void Env::stop_2()
{
  ZmAssert(invoked());

  allDBs_([](DB *db) { db->close(); });

  stopped(true);
}

bool Env::disconnectAll()
{
  ZmAssert(invoked());

  bool disconnected = false;
  auto i = m_cxns->readIterator();
  while (auto cxn = i.iterateNode())
    if (cxn->up()) {
      disconnected = true;
      cxn->disconnect();
    }
  return disconnected;
}

void Env::listen()
{
  ZmAssert(invoked());

  m_mx->listen(
      ZiListenFn::Member<&Env::listening>::fn(this),
      ZiFailFn::Member<&Env::listenFailed>::fn(this),
      ZiConnectFn::Member<&Env::accepted>::fn(this),
      m_self->ip(), m_self->port(), m_cf.nAccepts);
}

void Env::listening(const ZiListenInfo &)
{
  ZeLOG(Info, ZtString{} << "Zdb listening on (" <<
      m_self->ip() << ':' << m_self->port() << ')');
}

void Env::listenFailed(bool transient)
{
  ZtString warning;
  warning << "Zdb listen failed on (" <<
      m_self->ip() << ':' << m_self->port() << ')';
  if (transient && running()) {
    warning << " - retrying...";
    run([this]() { listen(); }, ZmTimeNow((int)m_cf.reconnectFreq));
  }
  ZeLOG(Warning, ZuMv(warning));
}

void Env::stopListening()
{
  ZeLOG(Info, "Zdb stop listening");
  m_mx->stopListening(m_self->ip(), m_self->port());
}

void Env::holdElection()
{
  ZmAssert(invoked());

  bool won, appActive;
  Host *oldMaster;

  m_mx->del(&m_electTimer);

  if (state() != HostState::Electing) return;

  appActive = m_appActive;

  oldMaster = setMaster();

  if (won = m_master == m_self) {
    m_appActive = true;
    m_prev = nullptr;
    if (!m_nPeers)
      ZeLOG(Warning, "Zdb activating standalone");
    else
      hbSend_(); // announce new master
  } else {
    m_appActive = false;
  }

  if (won) {
    if (!appActive) up_();
  } else {
    if (appActive) down_();
  }

  m_self->state(won ? HostState::Active : HostState::Inactive);
  setNext();

  switch (ZmEngine::state()) {
    case ZmEngineState::Starting:
    case ZmEngineState::StopPending:
      started(true);
      break;
    case ZmEngineState::Stopping:
    case ZmEngineState::StartPending:
      run([this]() { stop_1(); });
      break;
  }
}

void Env::deactivate()
{
  ZmAssert(invoked());

  using namespace HostState;

  if (!m_self) {
badorder:
    ZeLOG(Fatal, "Env::deactivate called out of order");
    return;
  }

  switch (state()) {
    case Instantiated:
    case Initialized:
    case Stopped:
    case Stopping:
      goto badorder;
    case Inactive:
      return;
    default:
      break;
  }

  bool appActive = m_appActive;
  m_self->voted(false);
  setMaster();
  m_self->voted(true);
  m_appActive = false;

  if (appActive) down_();

  m_self->state(Inactive);
  setNext();
}

void Host::reactivate()
{
  m_env->reactivate(static_cast<Host *>(this));
}

void Env::reactivate(Host *host)
{
  ZmAssert(invoked());

  if (ZmRef<Cxn> cxn = host->cxn()) cxn->hbSend();

  bool appActive = m_appActive;
  m_appActive = true;
  if (!appActive) up_();
}

void Env::up_()
{
  ZeLOG(Info, "Zdb ACTIVE");
  if (ZtString cmd = m_self->config().up) {
    if (oldMaster) cmd << ' ' << oldMaster->config().ip;
    ZeLOG(Info, ZtString{} << "Zdb invoking \"" << cmd << '\"');
    ::system(cmd);
  }
  m_handler.activeFn(this, m_self);
}

void Env::down_()
{
  ZeLOG(Info, "Zdb INACTIVE");
  if (ZuString cmd = m_self->config().down) {
    ZeLOG(Info, ZtString{} << "Zdb invoking \"" << cmd << '\"');
    ::system(cmd);
  }
  m_handler.inactiveFn(this, m_self);
}

ZvTelemetry::DBEnvFn Env::telFn()
{
  return ZvTelemetry::DBEnvFn{ZmMkRef(this), [](
      Env *dbEnv,
      ZmFn<IOBuilder &, Zfb::Offset<fbs::DBEnv>> envFn,
      ZmFn<IOBuilder &, Zfb::Offset<fbs::DBHost>> hostFn,
      ZmFn<IOBuilder &, Zfb::Offset<fbs::DB>>> dbFn) {
    dbEnv->invoke(
  [dbEnv, envFn = ZuMv(envFn), hostFn = ZuMv(hostFn), dbFn = ZuMv(dbFn)]() {
      {
	ZvTelemetry::IOBuilder fbb;
	envFn(fbb, dbEnv->telemetry(fbb));
	dbEnv->allHosts([&hostFn](const Host *host) {
	  hostFn(fbb, host->telemetry(fbb));
	});
      }
      dbEnv->allDBs([&dbFn](const DB *db) {
	ZvTelemetry::IOBuilder fbb;
	dbFn(fbb, db->telemetry(fbb));
      });
    });
  }};
}

Zfb::Offset<ZvTelemetry::fbs::ZdbEnv> DBEnv::telemetry(IOBuilder &fbb_)
{
  using namespace Zfb;
  using namespace Zfb::Save;
  auto appID = str(fbb_, m_cf.appID);
  auto thread = str(fbb_, m_cf.thread);
  auto writeThread = str(fbb_, m_cf.writeThread);
  fbs::ZdbEnvBuilder fbb{fbb_};
  fbb.add_appID(appID);
  fbb.add_thread(thread);
  fbb.add_writeThread(writeThread);
  { auto v = id(m_self->id()); fbb.add_self(&v); }
  { auto v = id(m_master ? m_master->id() : ZuID{}); fbb.add_master(&v); }
  { auto v = id(m_prev ? m_prev->id() : ZuID{}); fbb.add_prev(&v); }
  { auto v = id(m_next ? m_next->id() : ZuID{}); fbb.add_next(&v); }
  fbb.add_nCxns(m_cxns.count_());
  fbb.add_heartbeatFreq(m_cf.heartbeatFreq);
  fbb.add_heartbeatTimeout(m_cf.heartbeatTimeout);
  fbb.add_reconnectFreq(m_cf.reconnectFreq);
  fbb.add_electionTimeout(m_cf.electionTimeout);
  fbb.add_nDBs(m_dbs.count_());
  fbb.add_nHosts(m_hosts.count_());
  fbb.add_nPeers(m_nPeers);
  auto state = this->state();
  fbb.add_state(state);
  fbb.add_active(state == HostState::Active);
  fbb.add_recovering(m_recovering);
  fbb.add_replicating(!!m_nextCxn);
  return fbb.Finish();
}

Zfb::Offset<ZvTelemetry::fbs::ZdbHost> Host::telemetry(IOBuilder &fbb_)
{
  using namespace Zfb;
  using namespace Zfb::Save;
  fbs::ZdbHostBuilder fbb{fbb_};
  { auto v = ip(config().ip); fbb.add_ip(&v); }
  { auto v = id(config().id); fbb.add_id(&v); }
  fbb.add_priority(config().priority);
  fbb.add_port(config().port);
  fbb.add_state(m_state);
  fbb.add_voted(m_voted);
  return fbb.Finish();
}

Host::Host(ZdbEnv *env, const HostCf *cf, unsigned dbCount) :
  m_env{env},
  m_cf{cf},
  m_mx{env->mx()},
  m_dbState{dbCount}
{
}

void Host::connect()
{
  if (m_cxn) return;

  ZeLOG(Info, ZtString{} << "Zdb connecting to host " << id() <<
      " (" << config().ip << ':' << config().port << ')');

  m_mx->connect(
      ZiConnectFn::Member<&Host::connected>::fn(this),
      ZiFailFn::Member<&Host::connectFailed>::fn(this),
      ZiIP{}, 0, config().ip, config().port);
}

void Host::connectFailed(bool transient)
{
  ZtString warning;
  warning << "Zdb failed to connect to host " << id() <<
      " (" << config().ip << ':' << config().port << ')';
  if (transient && m_env->running()) {
    warning << " - retrying...";
    reconnect();
  }
  ZeLOG(Warning, ZuMv(warning));
}

ZiConnection *Host::connected(const ZiCxnInfo &ci)
{
  ZeLOG(Info, ZtString{} <<
      "Zdb connected to host " << id() <<
      " (" << ci.remoteIP << ':' << ci.remotePort << "): " <<
      ci.localIP << ':' << ci.localPort);

  if (!m_env->running()) return nullptr;

  return new Cxn{m_env, this, ci};
}

ZiConnection *Env::accepted(const ZiCxnInfo &ci)
{
  ZeLOG(Info, ZtString{} << "Zdb accepted cxn on (" <<
      ci.localIP << ':' << ci.localPort << "): " <<
      ci.remoteIP << ':' << ci.remotePort);

  if (!running()) return nullptr;

  return new Cxn{this, nullptr, ci};
}

Cxn_::Cxn_(ZdbEnv *env, Host *host, const ZiCxnInfo &ci) :
  ZiConnection{env->mx(), ci},
  m_env{env},
  m_host{host}
{
}

void Cxn_::connected(ZiIOContext &io)
{
  if (!m_env->running()) { io.disconnect(); return; }

  m_env->run([this]() { m_env->connected(this); });

  m_env->run([this]() { hbTimeout(); },
      ZmTimeNow(static_cast<int>(m_env->config().heartbeatTimeout)),
      ZmScheduler::Defer, &m_hbTimer);

  msgRead(io);
}

void Env::connected(Cxn *cxn)
{
  ZmAssert(invoked());

  if (!cxn->up()) return;

  m_cxns.add(cxn);

  if (Host *host = cxn->host()) associate(cxn, host);

  hbSend_(cxn);
}

void Env::associate(Cxn *cxn, ZuID hostID)
{
  ZmAssert(invoked());

  Host *host = m_hosts.find(hostID);

  if (!host) {
    ZeLOG(Error, ZtString{} <<
	"Zdb cannot associate incoming cxn: host ID " << hostID <<
	" not found");
    return;
  }

  if (host == m_self) {
    ZeLOG(Error, ZtString{} <<
	"Zdb cannot associate incoming cxn: host ID " << hostID <<
	" is same as self");
    return;
  }

  if (cxn->host() == host) return;

  associate(cxn, host);
}

void Env::associate(Cxn *cxn, Host *host)
{
  ZmAssert(invoked());

  ZeLOG(Info, ZtString{} << "Zdb host " << host->id() << " CONNECTED");

  cxn->host(host);

  host->associate(cxn);

  host->voted(false);
}

void Host::associate(Cxn *cxn)
{
  ZmAssert(m_env->invoked());

  if (ZuUnlikely(m_cxn && m_cxn.ptr() != cxn)) {
    m_cxn->host(nullptr);
    m_cxn->disconnect();
  }
  m_cxn = cxn;
}

void Host::reconnect()
{
  m_env->run([this]() { connect(); },
      ZmTimeNow(static_cast<int>(m_env->config().reconnectFreq),
      ZmScheduler::Defer, &m_connectTimer);
}

void Host::cancelConnect()
{
  m_mx->del(&m_connectTimer);
}

void Cxn_::hbTimeout()
{
  ZeLOG(Info, ZtString{} << "Zdb heartbeat timeout on host " <<
      ZuBoxed(m_host ? (int)m_host->id() : -1) << " (" <<
      info().remoteIP << ':' << info().remotePort << ')');

  disconnect();
}

void Cxn_::disconnected()
{
  ZeLOG(Info, ZtString{} << "Zdb disconnected from host " <<
      ZuBoxed(m_host ? (int)m_host->id() : -1) << " (" <<
      info().remoteIP << ':' << info().remotePort << ')');

  m_mx->del(&m_hbTimer);

  m_env->run([this]() { m_env->disconnected(this); });
}

void Env::disconnected(Cxn *cxn)
{
  ZmAssert(invoked());

  m_cxns->del(cxn);

  if (cxn == m_nextCxn) m_nextCxn = nullptr;

  Host *host = cxn->host();

  if (!host || host->cxn() != cxn) return;

  ZeLOG(Info, ZtString{} << "Zdb host " << host->id() << " DISCONNECTED");

  host->disconnected();

  switch (ZmEngine::state()) {
    case ZmEngineState::Stopping:
    case ZmEngineState::StartPending:
      if (--m_nPeers <= 0) run([this]() { stop_2(); });
      break;
  }

  using namespace HostState;

  host->state(Instantiated);
  host->voted(false);

  switch (state()) {
    case Active:
    case Inactive:
      break;
    default:
      goto ret;
  }

  if (host == m_prev) m_prev = nullptr;

  if (host == m_master) {
    switch (state()) {
      case Inactive:
	state(Electing);
	holdElection();
	break;
    }
    goto ret;
  }

  if (host == m_next) setNext();

ret:
  if (running() && Host::IndexAxor(*host) < Host::IndexAxor(*m_self))
    host->reconnect();
}

void Host::disconnected()
{
  m_cxn = nullptr;
}

Host *Env::setMaster()
{
  ZmAssert(invoked());

  Host *oldMaster = m_master;

  dbStateRefresh();

  m_master = nullptr;
  m_nPeers = 0;

  {
    auto i = m_hostIndex.readIterator();
    ZdbDEBUG(this, ZtString{} << "setMaster()\n" << 
	" self=" << m_self << '\n' <<
	" prev=" << m_prev << '\n' <<
	" next=" << m_next << '\n' <<
	" recovering=" << m_recovering << " replicating=" << !!m_nextCxn);

    while (Host *host = i.iterate()) {
      ZdbDEBUG(this, ZtString{} <<
	  " host=" << *host << '\n' <<
	  " master=" << m_master);

      if (host->voted()) {
	if (host != m_self) ++m_nPeers;
	if (!m_master) { m_master = host; continue; }
	int diff = host->cmp(m_master);
	if (ZuCmp<int>::null(diff)) {
	  m_master = nullptr;
	  break;
	} else if (diff > 0)
	  m_master = host;
      }
    }
  }

  if (m_master)
    ZeLOG(Info, ZtString{} << "Zdb host " << m_master->id() << " is master");
  else
    ZeLOG(Error, "Zdb master election failed - hosts inconsistent");

  return oldMaster;
}

void Env::setNext(Host *host)
{
  ZmAssert(invoked());

  m_next = host;
  m_recovering = false;
  m_nextCxn = nullptr;

  if (m_next) {
    m_standalone = false;
    startReplication();
  } else {
    m_standalone = true;
    allDBs([](DB *db) { db->standalone(); });
  }
}

void Env::setNext()
{
  ZmAssert(invoked());

  Host *next = nullptr;

  {
    auto i = m_hostIndex.readIterator();

    ZdbDEBUG(this, ZtString{} << "setNext()\n" <<
	" self=" << m_self << '\n' <<
	" master=" << m_master << '\n' <<
	" prev=" << m_prev << '\n' <<
	" next=" << m_next << '\n' <<
	" recovering=" << m_recovering << " replicating=" << !!m_nextCxn);

    while (Host *host = i.iterate()) {
      if (host != m_self && host != m_prev && host->voted() &&
	  m_self->cmp(host) >= 0 && (!next || host->cmp(next) > 0))
	next = host;

      ZdbDEBUG(this, ZtString{} <<
	  " host=" << host << '\n' <<
	  " next=" << next);
    }
  }

  setNext(next);
}

void Env::startReplication()
{
  ZmAssert(invoked());

  ZeLOG(Info, ZtString{} <<
	"Zdb host " << m_next->id() << " is next in line");

  m_nextCxn = m_next->m_cxn;	// starts replication
  dbStateRefresh();		// must be called after m_nextCxn assignment

  ZdbDEBUG(this, ZtString{} << "startReplication()\n" <<
      " self=" << m_self << '\n' <<
      " master=" << m_master << '\n' <<
      " prev=" << m_prev << '\n' <<
      " next=" << m_next << '\n' <<
      " recovering=" << m_recovering << " replicating=" << !!m_nextCxn);

  if (m_self->dbState().cmp(m_next->dbState()) >= 0 && !m_recovering) {
    // ZeLOG(Info, "startReplication() initiating recovery");
    m_recovering = true;
    m_recover = m_next->dbState();
    m_recoverEnd = m_self->dbState();
    run([this]() { recSend(); });
  }
}

void Env::stopReplication()
{
  ZmAssert(invoked());

  m_master = nullptr;
  m_prev = nullptr;
  m_next = nullptr;
  m_recovering = false;
  m_nextCxn = nullptr;
  {
    auto i = m_hostIndex.readIterator();
    while (Host *host = i.iterate()) host->voted(false);
  }
  m_self->voted(true);
  m_nPeers = 1;
}

void Cxn_::msgRead(ZiIOContext &io)
{
  Rx::recv<
    [](const ZiIOContext &, const Buf *buf) -> int {
      return loadHdr(buf);
    },
    [](const ZiIOContext &io, const Buf *buf, unsigned) -> int {
      return static_cast<Cxn *>(io.cxn)->msgRead2(buf);
    }>(io);
}
int Cxn_::msgRead2(const Buf *buf)
{
  return verifyHdr(buf, [this](const Hdr *hdr, const Buf *buf) -> int {
    auto msg = Zdb_::msg(hdr);
    if (ZuUnlikely(!msg)) return -1;

    auto length = static_cast<unsigned>(hdr->length);

    switch (static_cast<int>(msg->body_type())) {
      case fbs::Body_HB:
      case fbs::Body_Rep:
      case fbs::Body_Rec:
      case fbs::Body_Gap:
	ZmRef<Buf> buf = new Buf{this};
	*buf << msgData(hdr);
	if (ZuLikely(buf->length))
	  m_env->run(
	      [this, buf = ZuMv(buf)]() mutable { msgRead3(ZuMv(buf)); });
	break;
    }

    m_env->run([this]() { hbTimeout(); },
	ZmTimeNow(static_cast<int>(m_env->config().heartbeatTimeout)),
	ZmScheduler::Defer, &m_hbTimer);

    return length;
  });
}
void Cxn_::msgRead3(ZmRef<Buf> buf)
{
  ZmAssert(m_env->invoked());

  auto msg = Zdb_::msg(buf->hdr());
  if (!msg) return;
  switch (static_cast<int>(msg->body_type())) {
    case fbs::Body_HB:
      hbRcvd(hb_(msg));
      break;
    case fbs::Body_Rep:
    case fbs::Body_Rec:
      repRecord(ZuMv(buf));
      break;
    case fbs::Body_Gap:
      repGap(ZuMv(buf));
      break;
  }
}

void Cxn_::hbRcvd(const fbs::Heartbeat *hb)
{
  if (!m_host) m_env->associate(this, hb->hostID());

  if (!m_host) { disconnect(); return; }

  m_env->hbRcvd(m_host, hb);
}

// process received heartbeat
void Env::hbRcvd(Host *host, const fbs::Heartbeat *hb)
{
  ZdbDEBUG(this, ZtString{} << "hbDataRcvd()\n" << 
	" host=" << host << '\n' <<
	" self=" << m_self << '\n' <<
	" master=" << m_master << '\n' <<
	" prev=" << m_prev << '\n' <<
	" next=" << m_next << '\n' <<
	" recovering=" << m_recovering <<
	" replicating=" << !!m_nextCxn);

  host->state(hb->state());
  host->dbState().load(hb->envState());

  using namespace HostState;

  int state = this->state();

  switch (state) {
    case Electing:
      if (!host->voted()) {
	host->voted(true);
	if (--m_nPeers <= 0) holdElection();
      }
      return;
    case Active:
    case Inactive:
      break;
    default:
      return;
  }

  // check for duplicate master (dual active)
  switch (state) {
    case Active:
      switch (host->state()) {
	case Active:
	  vote(host);
	  if (host->cmp(m_self) > 0)
	    deactivate();
	  else
	    reactivate();
	  return;
      }
  }

  // check for new host joining after election
  if (!host->voted()) {
    ++m_nPeers;
    vote(host);
  }

  // trigger DB vacuuming
  Zfb::Load::all(hb->envState(),
      [this](unsigned, const fbs::DBState *dbState) {
    auto id = Zfb::Load::id(&(dbState->db()));
    RN rn = dbState->rn();
    if (auto db = m_dbs.findPtr(id))
      db->run([db, rn]() {
	if (db->ack(rn)) db->vacuum();
      });
  });
}

// check if new host should be our next in line
void Env::vote(Host *host)
{
  host->voted(true);
  envStateRefresh();
  if (host != m_next && host != m_prev &&
      m_self->cmp(host) >= 0 && (!m_next || host->cmp(m_next) > 0))
    setNext(host);
}

// send recovery message to next-in-line (continues repeatedly until completed)
void Env::recSend()
{
  ZmAssert(invoked());

  if (!m_self) {
    ZeLOG(Fatal, "Env::recSend called out of order");
    return;
  }
  if (!m_recovering) return;
  ZmRef<Cxn> cxn = m_nextCxn;
  if (!cxn) return;
  RN gap = nullRN();
  {
    auto i = m_recover.readIterator();
    while (auto state = i.iterate()) {
      ZuID id = state->template p<0>();
      if (auto endState = m_recoverEnd.find(id))
	if (auto db = m_dbs.findPtr(id)) {
	  auto &recRN = const_cast<EnvState::T *>(state)->template p<1>();
	  auto endRN = endState->template p<1>();
	  while (recRN < endRN) {
	    RN rn = recRN++;
	    ZmRef<Buf> buf = db->recovery(rn);
	    if (buf) {
	      if (gap != nullRN()) {
		cxn->repSend(db->gap(gap, rn - gap));
		// gap = nullRN();
	      }
	      cxn->repSend(ZuMv(buf));
	      return;
	    }
	    if (gap == nullRN()) gap = rn;
	  }
	}
    }
  }
  m_recovering = false;
}

// send replication message to next-in-line
bool Env::repSend(ZmRef<Buf> buf)
{
  if (ZmRef<Cxn> cxn = m_nextCxn) {
    cxn->repSend(ZuMv(buf));
    return true;
  }
  return false;
}

// send replication message (directed)
void Cxn_::repSend(ZmRef<Buf> buf)
{
  if (recovery_(msg_(buf->hdr())))
    ZiTx::send<[](ZmRef<Buf> buf) {
      auto db = reinterpret_cast<DB *>(buf->owner);
      auto env = db->env();
      env->run([env]() { env->recSend(); });
    }>(this, ZuMv(buf));
  else
    ZiTx::send(this, ZuMv(buf));
}

// broadcast heartbeat
void Env::hbSend()
{
  ZmAssert(invoked());

  hbSend_();

  m_mx->add(m_cf.sid, ZmFn<>::Member<&Env::hbSend>::fn(this),
    m_hbSendTime += (time_t)m_cf.heartbeatFreq,
    ZmScheduler::Defer, &m_hbSendTimer);
}

// send heartbeat (broadcast)
void Env::hbSend_()
{
  ZmAssert(invoked());
  envStateRefresh();
  auto i = m_cxns->readIterator();
  while (auto cxn = i.iterateNode()) cxn->hbSend();
}

// send heartbeat (directed)
void Env::hbSend_(Cxn *cxn)
{
  ZmAssert(invoked());
  envStateRefresh();
  cxn->hbSend();
}

// send heartbeat on a specific connection
void Cxn_::hbSend()
{
  ZmAssert(m_env->invoked());

  Host *self = m_env->self();
  Zfb::IOBuilder<Buf> fbb;
  {
    const auto &envState = self->envState();
    auto msg = fbs::CreateMsg(fbb, fbs::Body_HB, 
	fbs::CreateHeartbeat(fbb,
	  self->id(), m_env->state(), envState.save(fbb)).Union());
    fbb.Finish(msg);
  }
  ZiTx::send(this, saveHdr(fbb));
  ZdbDEBUG(m_env, ZtString{} << "hbSend()"
      "  self[ID:" << self->id() << " S:" << m_env->state() <<
      " N:" << self->envState().count_() << "] " << self->envState());
}

// refresh db state vector
void Env::envStateRefresh()
{
  ZmAssert(m_env->invoked());

  EnvState &envState = m_self->envState();
  allDBs_([&envState](DB *db) {
    envState.update(db->config().id, db->nextRN());
  });
}

// process received replicated record
void Cxn_::repRecord(ZmRef<Buf> buf)
{
  ZmAssert(m_env->invoked());

  if (!m_host) return;
  auto record = record_(msg_(buf->hdr()));
  auto id = Zfb::Load::id(record->db());
  DB *db = m_env->db_(id);
  ZmAssert(db);
  if (ZuUnlikely(!db)) return;
  buf->owner = db;
  ZdbDEBUG(m_env, ZtString{} << "replicated(" << id << ", " <<
      host << ", Record{" << record->rn() << "})");
  m_env->replicated(m_host, db->config().id, record->rn() + 1);
  db->invoke([db, buf = ZuMv(buf)]() mutable { db->repRecord(ZuMv(buf)); });
}

// process received replicated gap
void Cxn_::repGap(ZmRef<Buf> buf)
{
  ZmAssert(m_env->invoked());

  if (!m_host) return;
  auto gap = gap_(msg_(buf->hdr()));
  auto id = Zfb::Load::id(gap->db());
  DB *db = m_env->db_(id);
  ZmAssert(db);
  if (ZuUnlikely(!db)) return;
  buf->owner = db;
  ZdbDEBUG(m_env, ZtString{} << "replicated(" << id << ", " <<
      host << ", Gap{" << gap->rn() << ", " << gap->count() << "})");
  m_env->replicated(m_host, db->config().id, gap->rn() + gap->count());
  db->invoke([db, buf = ZuMv(buf)]() mutable { db->repGap(ZuMv(buf)); });
}

void Env::replicated(Host *host, ZuID dbID, RN rn)
{
  ZmAssert(invoked());

  bool updated = host->envState().update(dbID, rn);
  if ((active() || host == m_next) && !updated) return;
  if (!m_prev) {
    m_prev = host;
    ZeLOG(Info,
	ZtString{} << "Zdb host " << m_prev->id() << " is previous in line");
  }
}

DB::DB(ZdbEnv *env, ZdbCf *cf) :
  m_env{env}, m_mx{env->mx()}, m_cf{cf},
  m_path{ZiFile::append(env->config().path, cf->id)},
  m_cache{new Cache{}},
  m_buffers{new Buffers{}},
  m_files{new FileCache{}},
  m_indexBlks{new IndexBlkCache{}}
{
}

DB::~DB()
{
  close();
}

void DB::init(DBHandler handler)
{
  m_handler = ZuMv(handler);
}

void DB::final()
{
  m_handler = DBHandler{};
}

// telemetry
Zfb::Offset<ZvTelemetry::fbs::Zdb> DB::telemetry(IOBuilder &fbb_)
{
  using namespace Zfb;
  using namespace Zfb::Save;
  auto path = str(fbb_, m_path);
  auto name = str(fbb_, config().id);
  fbs::ZdbBuilder fbb{fbb_};
  fbb.add_path(path);
  fbb.add_name(name);
  fbb.add_minRN(m_minRN);
  fbb.add_nextRN(m_nextRN);
  {
    ObjectCache::Stats stats;
    m_cache->stats(stats);
    fbb.add_cacheLoads(stats.loads);
    fbb.add_cacheMisses(stats.misses);
    fbb.add_cacheSize(stats.size);
  }
  {
    FileCache::Stats stats;
    m_files->stats(stats);
    fbb.add_fileLoads(stats.loads);
    fbb.add_fileMisses(stats.misses);
    fbb.add_fileCacheSize(stats.size);
  }
  {
    IndexBlkCache::Stats stats;
    m_indexBlks->stats(stats);
    fbb.add_indexBlkLoads(stats.loads);
    fbb.add_indexBlkMisses(stats.misses);
    fbb.add_indexBlkCacheSize(stats.size);
  }
  fbb.add_cacheMode(config().cacheMode);
  fbb.add_warmUp(config().warmUp);
  return fbb.Finish();
}

// FIXME from here

// load object from buffer
ZmRef<AnyObject> DB::load(const fbs::Record *record) // cacheLock held
{
  auto prevRN = record->prevRN();
  auto seqLenOp = record->seqLenOp();
  switch (SeqLenOp::op(seqLenOp)) {
    case Op::Delete:
    case Op::Purge:
      return nullptr;
  }
  ZmRef<AnyObject> object;
  if (prevRN != nullRN()) object = cacheDel_(prevRN);
  auto data = Zfb::Load::bytes(record->data());
  if (!object) {
    object = m_handler.loadFn(this, data.data(), data.length());
  } else {
    object = m_handler.updateFn(object, data.data(), data.length());
  }
  object->load(record->rn(), prevRN, seqLenOp);
  return object;
}

// save object to buffer
void DB::save(Zfb::Builder &fbb, AnyObject *object)
{
  m_handler.saveFn(fbb, object->ptr_());
}

bool DB::recover()
{
  ZeError e;
  ZiDir::Path subName;
  ZtBitWindow<1> subDirs;
  // main directory
  {
    ZiDir dir;
    if (dir.open(m_path) != Zi::OK) {
      if (ZiFile::mkdir(m_path, &e) != Zi::OK) {
	ZeLOG(Fatal, ZtString{} << m_path << ": " << e);
	return false;
      }
      return true;
    }
    while (dir.read(subName) == Zi::OK) {
#ifdef _WIN32
      ZtString subName_{subName};
#else
      const auto &subName_ = subName;
#endif
      try {
	if (!ZtREGEX("^[0-9a-f]{5}$").m(subName_)) continue;
      } catch (const ZtRegexError &e) {
	ZeLOG(Error, ZtString{} << e);
	continue;
      } catch (...) {
	continue;
      }
      ZuBox<unsigned> subIndex;
      subIndex.scan<ZuFmt::Hex<>>(subName_);
      subDirs.set(subIndex);
    }
    dir.close();
  }
  // subdirectories
  subDirs.all([&](unsigned i, bool) {
#ifdef _WIN32
    ZtString subName_;
#else
    auto &subName_ = subName;
#endif
    subName_ = ZuBox<unsigned>{i}.hex<false, ZuFmt::Right<5>>();
    subName = ZiFile::append(m_path, subName_);
    ZiDir::Path fileName;
    ZtBitWindow<1> files;
    {
      ZiDir subDir;
      if (subDir.open(subName, &e) != Zi::OK) {
	ZeLOG(Error, ZtString{} << subName << ": " << e);
	return true;
      }
      while (subDir.read(fileName) == Zi::OK) {
#ifdef _WIN32
	ZtString fileName_{fileName};
#else
	auto &fileName_ = fileName;
#endif
	try {
	  if (!ZtREGEX("^[0-9a-f]{5}\.zdb$").m(fileName_)) continue;
	} catch (const ZtRegexError &e) {
	  ZeLOG(Error, ZtString{} << e);
	  continue;
	} catch (...) {
	  continue;
	}
	ZuBox<unsigned> fileIndex;
	fileIndex.scan<ZuFmt::Hex<>>(fileName_);
	files.set(fileIndex);
      }
      subDir.close();
    }
    // data files
    files.all([&](unsigned j, bool) {
#ifdef _WIN32
      ZtString fileName_;
#else
      auto &fileName_ = fileName;
#endif
      uint64_t id = (static_cast<uint64_t>(i)<<20) | j;
      {
	Guard guard(m_fileLock);
	ZmRef<File> file = openFile_(fileName_, id, false);
	if (!file) return false;
	recover(file);
      }
      return true;
    });
    return true;
  });
  return true;
}

void DB::recover(File *file)
{
  if (!file->allocated()) return;
  if (file->deleted() >= fileRecs()) { delFile(file); return; }
  RN rn = (file->id())<<fileShift();
  ZmRef<IndexBlk> indexBlk;
  int first = file->first();
  if (ZuUnlikely(first < 0)) return;
  int last = file->last();
  if (ZuUnlikely(last < 0)) return; // should never happen
  rn += first;
  for (int j = first; j <= last; j++, rn++) {
    if (!file->exists(j)) continue;
    auto indexBlkID = rn>>indexShift();
    ZmRef<AnyObject> object;
    if (!indexBlk || indexBlk->id != indexBlkID)
      indexBlk = file->readIndexBlk(indexBlkID);
    if (!indexBlk) return; // I/O error on file, logged within readIndexBlk
    object = this->recover(
	FileRec{file, indexBlk, static_cast<unsigned>(rn & indexMask())});
    if (object && m_handler.recoverFn) m_handler.recoverFn(object, buf);
  }
}

void DB::recover(const FileRec &rec)
{
  if (auto buf = read_(rec)) {
    auto record = record_(msg_(buf->hdr()));
    auto rn = record->rn();
    {
      Guard guard(m_pushLock);
      if (m_minRN < rn) m_minRN = rn;
      if (m_nextRN <= rn) m_nextRN = rn + 1;
    }
    object = recover_(record);
    if (object && m_handler.recoverFn) m_handler.recoverFn(object, buf);
  }
}

bool DB::recoverRN(RN rn)
{
  Guard guard(m_pushLock);
  if (rn != m_nextRN) return nullptr;
  m_nextRN = rn + 1;
  if (rn < m_minRN) m_minRN = rn;
}

ZmRef<AnyObject> DB::recover_(const fbs::Record *record)
{
  ZmAssert(invoked());

  RN rn = record->rn();
  RN prevRN = record->prevRN();

  ZmRef<AnyObject> object;
  auto seqLenOp = record->seqLenOp();

  switch (SeqLenOp::op(seqLenOp)) {
    default:
      return nullptr;
    case Op::Put:
      if (SeqLenOp::seqLen(seqLenOp) > 1)
	m_deletes.add(rn, DeleteOp{prevRN,
	  SeqLenOp::mk(SeqLenOp::seqLen(seqLenOp) - 1, Op::Put)});
    case Op::Append: // fall through
      object = load(record);
      break;
    case Op::Delete:
      m_deletes.add(rn, DeleteOp{rn, seqLenOp});
      break;
    case Op::Purge:
      m_deletes.add(rn, DeleteOp{prevRN, seqLenOp});
      break;
  }
  if (object) cache(object);
  return object;
}

// process replication - database
void DB::repRecord(ZmRef<Buf> buf)
{
  auto record = record_(msg_(buf->hdr()));
  ZmRef<AnyObject> object = recover_(record);
  if (object && m_handler.recoverFn) m_handler.recoverFn(object, buf);
  write(ZuMv(buf));
}

void DB::repGap(ZmRef<Buf> buf)
{
  RN rn = gap->rn();
  if (rn != m_nextRN) return;
  m_nextRN = rn + gap->count();
  m_env->invoke([env = m_env, buf = ZuMv(buf)]() mutable {
    env->repSend(ZuMv(buf));
  });
}

void DB::write(ZmRef<Buf> buf)
{
  ZmAssert(invoked());

  m_buffers->addNode(buf);
  if (config().repMode) {
    m_env->invoke([this, env = m_env, buf = ZuMv(buf)]() mutable {
      if (env->repSend(buf))
	fileRun([this, buf = ZuMv(buf)]() {
	  write2(buf);
	  m_buffers->delNode(buf);
	});
      else
	fileRun([this, buf = ZuMv(buf)]() {
	  ZdbRN rn = write2(buf);
	  m_buffers->delNode(buf);
	  if (rn != nullRN())
	    invoke([this, rn]() { if (ack(rn + 1)) vacuum(); });
	});
    });
  } else {
    fileRun([this, buf = ZuMv(buf)]() {
      ZdbRN rn = write2(buf);
      m_buffers->delNode(buf);
      m_env->invoke([this, rn, buf = ZuMv(buf)]() mutable {
	if (!env->repSend(ZuMv(buf)) && rn != nullRN())
	  invoke([this, rn]() { if (ack(rn + 1)) vacuum(); });
      });
    });
  }
}

void File_::reset()
{
  ZmAssert(m_db->fileInvoked());

  m_flags = 0;
  m_allocated = m_deleted = 0;
  m_bitmap.zero();
  memset(&m_superBlk.data[0], 0, sizeof(FileSuperBlk));
}

bool File_::scan()
{
  ZmAssert(m_db->fileInvoked());

  // if file is truncated below minimum header size, reset/rewrite it
  if (size() < sizeof(FileHdr) + sizeof(FileBitmap) + sizeof(FileSuperBlk)) {
    reset();
    return sync_();
  }
  int r;
  ZeError e;
  // header
  {
    FileHdr hdr;
    if (ZuUnlikely((r = pread(0, &hdr, sizeof(FileHdr), &e)) != Zi::OK)) {
      m_db->fileRdError_(static_cast<File *>(this), 0, r, e);
      return false;
    }
    if (hdr.magic != ZdbMagic) return false;
    if (hdr.version != ZdbVersion) return false;
    m_flags = hdr.flags;
    m_allocated = hdr.allocated;
    m_deleted = hdr.deleted;
  }
  // bitmap
  {
#if Zu_BIGENDIAN
    FileBitmap bitmap;
    auto bitmapData = &(bitmap.data[0]);
#else
    auto bitmapData = &(m_bitmap.data[0]);
#endif
    if (ZuUnlikely((r = pread(
	      sizeof(FileHdr),
	      &bitmapData[0], sizeof(FileBitmap), &e)) != Zi::OK)) {
      m_db->fileRdError_(static_cast<File *>(this), sizeof(FileHdr), r, e);
      return false;
    }
#if Zu_BIGENDIAN
    for (unsigned i = 0; i < Bitmap::Words; i++)
      m_bitmap.data[i] = bitmapData[i];
#endif
  }
  // superblock
  {
#if Zu_BIGENDIAN
    FileSuperBlk superBlk;
    auto superData = &(superBlk.data[0]);
#else
    auto superData = &m_superBlk.data[0];
#endif
    if (ZuUnlikely((r = pread(
	      sizeof(FileHdr) + sizeof(FileBitmap),
	      &superData[0], sizeof(FileSuperBlk), &e)) != Zi::OK)) {
      m_db->fileRdError_(static_cast<File *>(this),
	  sizeof(FileHdr) + sizeof(FileBitmap), r, e);
      return false;
    }
#if Zu_BIGENDIAN
    for (unsigned i = 0; i < fileIndices(); i++)
      m_superBlk.data[i] = superData[i];
#endif
  }
  if (m_flags & FileFlags::Clean) {
    m_flags &= ~FileFlags::Clean;
    m_offset = size();
    return true;
  }
  // rebuild count and bitmap from index blocks
  m_allocated = m_deleted = 0;
  m_bitmap.zero();
  bool rewrite = false;
  for (unsigned i = 0; i < fileIndices(); i++)
    if (uint64_t indexBlkOffset = m_superBlk.data[i]) {
      FileIndexBlk indexBlk;
      if (ZuUnlikely((r = pread(indexBlkOffset,
		&indexBlk, sizeof(FileIndexBlk), &e)) != Zi::OK)) {
	m_db->fileRdError_(static_cast<File *>(this), indexBlkOffset, r, e);
	return false;
      }
      bool rewriteIndexBlk = false;
      for (unsigned j = 0; j < indexRecs(); j++)
	if (uint64_t offset = indexBlk.data[j].offset)
	  if (offset == deleted()) {
	    ++m_deleted;
	  } else {
	    offset += indexBlk.data[j].length;
	    FileRecTrlr trlr;
	    if (ZuUnlikely((r = pread(offset,
		      &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
	      m_db->fileRdError_(static_cast<File *>(this), offset, r, e);
	      return false;
	    }
	    if (trlr.rn != ((id()<<fileShift()) | (i<<indexShift()) | j) ||
		trlr.magic != ZdbCommitted) {
	      indexBlk.data[j] = { 0, 0 };
	      rewriteIndexBlk = true;
	      continue;
	    }
	    ++m_allocated;
	    m_bitmap[(i<<indexShift()) | j].set();
	  }
      if (rewriteIndexBlk) {
	rewrite = true;
	if (ZuUnlikely((r = pwrite(indexBlkOffset,
		  &indexBlk, sizeof(FileIndexBlk), &e)) != Zi::OK)) {
	  m_db->fileWrError_(static_cast<File *>(this), indexBlkOffset, e);
	  return false;
	}
      }
    }
  m_offset = size();
  return !rewrite || sync_();
}

bool File_::sync_()
{
  ZmAssert(m_db->fileInvoked());

  int r;
  ZeError e;
  // header
  {
    FileHdr hdr{
      .magic = ZdbMagic,
      .version = ZdbVersion,
      .flags = m_flags,
      .allocated = m_allocated,
      .deleted = m_deleted
    };
    if (ZuUnlikely((r = pwrite(0, &hdr, sizeof(FileHdr), &e)) != Zi::OK)) {
      m_db->fileWrError_(static_cast<File *>(this), sizeof(FileHdr), e);
      m_flags |= FileFlags::IOError;
      return false;
    }
  }
  // bitmap
  {
#if Zu_BIGENDIAN
    FileBitmap bitmap;
    auto bitmapData = &(bitmap.data[0]);
    for (unsigned i = 0; i < Bitmap::Words; i++)
      bitmapData[i] = m_bitmap.data[i];
#else
    auto bitmapData = &(m_bitmap.data[0]);
#endif
    if (ZuUnlikely((r = pwrite(
	      sizeof(FileHdr),
	      &bitmapData[0], sizeof(FileBitmap), &e)) != Zi::OK)) {
      m_db->fileWrError_(static_cast<File *>(this), sizeof(FileHdr), e);
      m_flags |= FileFlags::IOError;
      return false;
    }
  }
  // superblock
  {
#if Zu_BIGENDIAN
    FileSuperBlk superBlk;
    auto superData = &(super.data[0]);
    for (unsigned i = 0; i < fileIndices(); i++)
      superData[i] = m_superBlk.data[i];
#else
    auto superData = &m_superBlk.data[0];
#endif
    if (ZuUnlikely((r = pwrite(
	      sizeof(FileHdr) + sizeof(FileBitmap),
	      &superData[0], sizeof(FileSuperBlk), &e)) != Zi::OK)) {
      m_db->fileWrError_(static_cast<File *>(this),
	  sizeof(FileHdr) + sizeof(FileBitmap), e);
      m_flags |= FileFlags::IOError;
      return false;
    }
  }
  return true;
}

bool File_::sync() // file thread
{
  ZmAssert(m_db->fileInvoked());

  m_flags |= FileFlags::Clean;
  if (!sync_()) goto error;
  {
    ZeError e;
    if (ZiFile::sync(&e) != Zi::OK) {
      m_db->fileWrError_(static_cast<File *>(this), 0, e);
      m_flags |= FileFlags::IOError;
      goto error;
    }
  }
  m_flags &= ~FileFlags::Clean;
  return true;
error:
  m_flags &= ~FileFlags::Clean;
  FileHdr hdr{
    .magic = ZdbMagic,
    .version = ZdbVersion,
    .flags = m_flags,
    .allocated = m_allocated,
    .deleted = m_deleted
  };
  pwrite(0, &hdr, sizeof(FileHdr)); // best effort to clear Clean flag
  m_flags &= ~FileFlags::Clean;
  return false;
}

bool DB::open()
{
  if (!recover()) return false;

  if (config().warmUp) {
    m_env->mx()->run(config().fileTID, ZmFn<>{this, [](DB *self) {
      self->rn2file(m_nextRN, true);
    }});
    m_env->mx()->run(config().sid, ZmFn<>{this, [](DB *self) {
      ZmRef<AnyObject>{self->m_handler.ctorFn(self)};
    }});
  }

  return true;
}

void DB::close()
{
  // index blocks
  while (IndexBlk *blk = static_cast<IndexBlk *>(m_indexBlkLRU.shiftNode())) {
    if (File *file =
	getFile(blk->id>>(fileShift() - indexShift()), false))
      file->writeIndexBlk(blk);
    m_indexBlks->delNode(blk);
  }

  // files
  while (auto file = static_cast<File *>(m_fileLRU.shiftNode())) {
    file->sync();
    m_files->delNode(file);
  }
}

void Env::checkpoint()
{
  allDBs([](DB *db) { db->checkpoint(); });
}

bool DB::checkpoint()
{
  bool ok = true;
  m_indexBlks->all([&ok](ZmRef<IndexBlk> blk) mutable {
    if (File *file = getFile(blk->id>>(fileShift() - indexShift()), false))
      ok &&= file->writeIndexBlk(blk);
  });
  m_files->all([&ok](ZmRef<File> file) mutable {
    ok &&= file->sync_();
  });
  return ok;
}

ZmRef<AnyObject> DB::placeholder()
{
  return m_handler.ctorFn(this);
}

ZmRef<AnyObject> DB::push()
{
  if (ZuUnlikely(!m_env->active())) {
    ZeLOG(Error, ZtString{} <<
	"Zdb inactive application attempted push on DBID " << config().id);
    return nullptr;
  }
  return push_();
}

ZmRef<AnyObject> DB::push_()
{
  RN rn;
  {
    Guard guard(m_lock);
    rn = m_nextRN++;
    if (rn < m_minRN) m_minRN = rn;
  }
  ZmRef<AnyObject> object = m_handler.ctorFn(this);
  object->init(rn);
  return object;
}

ZmRef<AnyObject> DB::push(RN rn)
{
  if (ZuUnlikely(!m_env->active())) {
    ZeLOG(Error, ZtString{} <<
	"Zdb inactive application attempted push on DBID " << config().id);
    return nullptr;
  }
  if (ZuUnlikely(rn == nullRN())) return push_();
  return push_(rn);
}

ZmRef<AnyObject> DB::push_(RN rn)
{
  if (ZuUnlikely(m_nextRN != rn)) return nullptr;
  m_nextRN = rn + 1;
  if (rn < m_minRN) m_minRN = rn;
  ZmRef<AnyObject> object = m_handler.ctorFn(this);
  object->init(rn);
  return object;
}

template <bool UpdateLRU>
ZmRef<AnyObject> DB::get_(RN rn, GenFn fn)
{
  if (ZuUnlikely(rn < m_minRN || rn >= m_nextRN)) {
    fn(nullptr);
    return nullptr;
  }
  return m_cache->find<UpdateLRU>(rn, [this]<typename L>(RN rn, L l) {
    {
      ZmRef<Buf> buf = m_buffers->find(rn);
      if (ZuLikely(buf)) {
	l(load(record_(msg_(buf->hdr()))));
	return;
      }
    }
    fileRun([this, rn, l = ZuMv(l)]() {
      FileRec rec = rn2file(rn, false);
      if (rec) buf = read_(rec);
      if (buf)
	l(load(record_(msg_(buf->hdr()))));
      else
	l(nullptr);
    });
  }, ZuMv(fn)); // write to file is triggered by put(), not eviction
}

ZmRef<AnyObject> DB::get(RN rn, GenFn fn)
{
  return get_<true>(rn, ZuMv(fn));
}
ZmRef<AnyObject> DB::getUpdate(RN rn, GenFn fn)
{
  return get_<false>(rn, ZuMv(fn));
}

// FIXME
void DB::cache(AnyObject *object) // cacheLock held
{
  if (config().cacheMode != ZdbCacheMode::All &&
      m_cache->count_() >= m_cacheSize) {
    auto lru_ = m_lru.shiftNode();
    if (ZuLikely(lru_)) {
      AnyObject *lru = static_cast<AnyObject *>(lru_);
      m_cache->del(lru->rn());
    }
  }
  cache_(object);
}

void DB::cache_(AnyObject *object) // cacheLock held
{
  m_cache->addNode(object);
  if (config().cacheMode != ZdbCacheMode::All) m_lru.pushNode(object);
}

ZmRef<AnyObject> DB::cacheDel_(RN rn) // cacheLock held
{
  if (ZmRef<CacheNode> object = m_cache->del(rn)) {
    if (config().cacheMode != ZdbCacheMode::All) m_lru.delNode(object);
    return object;
  }
  return nullptr;
}

void DB::cacheDel_(AnyObject *object) // cacheLock held
{
  m_cache->delNode(object);
  if (config().cacheMode != ZdbCacheMode::All) m_lru.delNode(object);
}

bool DB::update(AnyObject *object)
{
  Guard guard(m_cacheLock);
  if (ZuUnlikely(object->seqLen() == SeqLenOp::maxSeqLen())) return false;
  cacheDel_(object);
  RN rn = m_nextRN++;
  object->push(rn);
  if (rn < m_minRN) m_minRN = rn;
  return true;
}

bool DB::update(AnyObject *object, RN rn)
{
  if (ZuUnlikely(rn == nullRN())) return update(object);
  Guard guard(m_cacheLock);
  if (ZuUnlikely(object->seqLen() == SeqLenOp::maxSeqLen())) return false;
  if (ZuUnlikely(m_nextRN != rn)) return false;
  m_nextRN = rn + 1;
  if (rn < m_minRN) m_minRN = rn;
  cacheDel_(object);
  object->push(rn);
  return true;
}

ZmRef<AnyObject> DB::update(RN prevRN)
{
  Guard guard(m_cacheLock);
  if (ZuUnlikely(prevRN < m_minRN || prevRN >= m_nextRN)) return nullptr;
  ZmRef<AnyObject> object = get__(prevRN);
  if (ZuUnlikely(!object || object->deleted())) return nullptr;
  if (ZuUnlikely(object->seqLen() == SeqLenOp::maxSeqLen())) return nullptr;
  cacheDel_(object);
  RN rn = m_nextRN++;
  if (rn < m_minRN) m_minRN = rn;
  object->push(rn);
  return object;
}

ZmRef<AnyObject> DB::update(RN prevRN, RN rn)
{
  Guard guard(m_cacheLock);
  if (ZuUnlikely(prevRN < m_minRN || prevRN >= m_nextRN)) return nullptr;
  if (ZuUnlikely(m_nextRN != rn)) return nullptr;
  ZmRef<AnyObject> object = get__(prevRN);
  if (ZuUnlikely(!object || object->deleted())) return nullptr;
  if (ZuUnlikely(object->seqLen() == SeqLenOp::maxSeqLen())) return nullptr;
  m_nextRN = rn + 1;
  if (rn < m_minRN) m_minRN = rn;
  cacheDel_(object);
  object->push(rn);
  return object;
}

// commits push() / update()
void DB::put(AnyObject *object)
{
  ZmRef<Buf> buf;
  {
    Guard guard(m_cacheLock);
    object->commit(Op::Put);
    if (object->seqLen() > 1)
      m_deletes.add(object->rn(),
	  DeleteOp{object->prevRN(),
	    SeqLenOp::mk(object->seqLen() - 1, Op::Put)});
    buf = object->replicate(fbs::Body_Rep);
    m_buffers->addNode(buf);
    object->put(); // resets seqLen to 1
    cache(object);
  }
  m_env->write(ZuMv(buf)); // FIXME
}

// commits appended update()
void DB::append(AnyObject *object)
{
  ZmRef<Buf> buf;
  {
    Guard guard(m_cacheLock);
    object->commit(Op::Append);
    buf = object->replicate(fbs::Body_Rep);
    m_buffers->addNode(buf);
    cache(object);
  }
  m_env->write(ZuMv(buf)); // FIXME
}

// commits delete following push() / update()
void DB::del(AnyObject *object)
{
  if (ZuUnlikely(!object->seqLen())) {
    Guard guard(m_lock);
    del__(object->rn());
    object->commit(Op::Delete);
    return;
  }
  ZmRef<Buf> buf;
  {
    Guard guard(m_cacheLock);
    object->commit(Op::Delete);
    auto rn = object->rn();
    m_deletes.add(rn, DeleteOp{rn, SeqLenOp::mk(object->seqLen(), Op::Delete)});
    buf = object->replicate(fbs::Body_Rep);
    m_buffers->addNode(buf);
  }
  m_env->write(ZuMv(buf)); // FIXME
}

// aborts push() / update()
void DB::abort(AnyObject *object)
{
  Guard guard(m_lock);
  del__(object->rn());
  object->abort();
  if (object->rn() != nullRN()) cache(object);
}

// prepare replication data for sending & writing to disk
ZmRef<Buf> AnyObject::replicate(int type) // cacheLock held
{
  ZdbDEBUG(m_db->env(),
      ZtString{} << "AnyObject::replicate(" << type << ')');
  Zfb::IOBuilder<Buf> fbb;
  Zfb::Offset<Zfb::Vector<uint8_t>> data;
  if (op() != Op::Delete)
    if (this->ptr_()) {
      m_db->save(fbb, this);
      data = Zfb::Save::nest(fbb);
    }
  {
    auto id = Zfb::Save::id(m_db->config().id);
    auto msg = fbs::CreateMsg(fbb, static_cast<fbs::Body>(type),
	fbs::CreateRecord(fbb, &id,
	  m_rn, m_prevRN, m_seqLenOp, data).Union());
    fbb.Finish(msg);
  }
  return saveHdr(fbb, m_db);
}

// forward replication data
ZmRef<Buf> DB::replicateFwd(const Buf *rxBuf)
{
  ZmRef<Buf> buf = new Buf{this};
  auto data = buf->ensure(rxBuf->length);
  if (!data) return nullptr;
  memcpy(data, rxBuf->data(), rxBuf->length);
  return buf;
}

// prepare recovery data for sending
ZmRef<Buf> DB::recovery(RN rn)
{
  Guard guard(m_cacheLock);
  // recover from outbound replication buffer cache
  if (ZmRef<Buf> txBuf = m_buffers->find(rn)) {
    auto record = record_(msg_(txBuf->hdr()));
    auto repData = Zfb::Load::bytes(record->data());
    Zfb::IOBuilder<Buf> fbb;
    Zfb::Offset<Zfb::Vector<uint8_t>> data;
    if (repData) {
      auto ptr = Zfb::Save::extend(fbb, repData.length(), data);
      if (!data.IsNull() && ptr)
	memcpy(ptr, repData.data(), repData.length());
    }
    auto id = Zfb::Save::id(config().id);
    auto msg = fbs::CreateMsg(fbb, fbs::Body_Rec,
	fbs::CreateRecord(fbb, &id,
	  record->rn(), record->prevRN(), record->seqLenOp(), data).Union());
    fbb.Finish(msg);
    return saveHdr(fbb, this);
  }
  // recover from object cache
  if (ZmRef<AnyObject> object = m_cache->find(rn))
    return object->replicate(fbs::Body_Rec);
  // recover from file
  if (FileRec rec = rn2file(rn, false))
    return read_(rec);
  // unallocated | deleted
  return nullptr;
}

// prepare run-length encoded gap for sending
ZmRef<Buf> DB::gap(RN rn, uint64_t count)
{
  Zfb::IOBuilder<Buf> fbb;
  auto id = Zfb::Save::id(config().id);
  auto msg = fbs::CreateMsg(fbb, fbs::Body_Gap,
      fbs::CreateGap(fbb, &id, rn, count).Union());
  fbb.Finish(msg);
  return saveHdr(fbb, this);
}

void DB::purge(RN minRN)
{
  Zfb::IOBuilder<Buf> fbb;
  RN rn;
  {
    Guard guard(m_lock);
    rn = m_nextRN++;
  }
  {
    auto id = Zfb::Save::id(config().id);
    auto msg = fbs::CreateMsg(fbb, fbs::Body_Rep,
	fbs::CreateRecord(fbb, &id,
	  rn, minRN, SeqLenOp::mk(1, Op::Purge), {}).Union());
    fbb.Finish(msg);
  }
  ZmRef<Buf> buf = saveHdr(fbb, this);
  {
    Guard guard(m_lock);
    m_deletes.add(rn, DeleteOp{minRN, SeqLenOp::mk(1, Op::Purge)});
    m_buffers->addNode(buf);
  }
  m_env->write(ZuMv(buf)); // FIXME
}

// FIXME - this dispatches back to the DB file thread for writing
// following repSend, and is called from the DB main thread repRecord()
// among other places, calls are always preceded by a writeCache->add,
// these ops should be made consistent; most likely need a DB main
// thread function to add to writeCache, invoke Env repSend then run (not
// invoke) DB file thread write2 and call that everywhere env->write is
// currently called
void Env::write(ZmRef<Buf> buf) // FIXME - deprecated - replaced by DB::write
{
  if (reinterpret_cast<DB *>(buf->owner)->config().repMode)
    this->repSend(buf);				// send to replica
  m_mx->run(m_cf.writeTID,
      ZmFn<>::mvFn(ZuMv(buf), [](ZmRef<Buf> buf) {
	auto db = reinterpret_cast<DB *>(buf->owner);
	db->write2(ZuMv(buf));
      }));
}

// creates/caches file and index block as needed
FileRec DB::rn2file(RN rn, bool write) // cacheLock held
{
  uint64_t fileID = rn>>fileShift();
  File *file = getFile(fileID, write);
  if (!file) return {};
  if (!write && !file->exists(rn & fileRecMask())) return {};
  uint64_t indexBlkID = rn>>indexShift();
  IndexBlk *indexBlk = getIndexBlk(file, indexBlkID, write);
  if (!indexBlk) return {};
  auto indexOff = static_cast<unsigned>(rn & indexMask());
  return {file, indexBlk, indexOff};
}

File *DB::getFile(uint64_t id, bool create) // cacheLock held
{
  ++m_fileLoads;
  if (File *file = static_cast<File *>(m_files->findPtr(id))) {
    m_fileLRU.pushNode(m_fileLRU.delNode(file));
    return file;
  }
  ++m_fileMisses;
  ZmRef<File> file = openFile(id, create);
  if (ZuUnlikely(!file)) return nullptr;
  auto filePtr = file.ptr();
  // eviction
  if (m_files->count_() >= m_fileCacheSize)
    if (File *lru = static_cast<File *>(m_fileLRU.shiftNode())) {
      // FIXME - move sync/evict to write thread
      lru->sync();
      m_files->delNode(lru);
    }
  m_files->addNode(ZuMv(file));
  m_fileLRU.pushNode(filePtr);
  if (id > m_lastFile) m_lastFile = id;
  return filePtr;
}

ZmRef<File> DB::openFile(uint64_t id, bool create)
{
  Guard guard(m_fileLock);
  return openFile_(fileName(dirName(id), id), id, create);
}

ZmRef<File> DB::openFile_(
    const ZiFile::Path &name, uint64_t id, bool create) // file thread
{
  ZmRef<File> file = new File{this, id};
  if (file->open(name, ZiFile::GC, 0666) == Zi::OK) {
    if (!file->scan()) return nullptr;
    return file;
  }
  if (!create) return nullptr;
  ZiFile::mkdir(dirName(id)); // pre-emptive idempotent
  ZeError e;
  auto fileSize = sizeof(FileHdr) + sizeof(FileBitmap) + sizeof(FileSuperBlk);
  if (file->open(
	name, ZiFile::Create | ZiFile::GC, 0666, fileSize, &e) != Zi::OK) {
    ZeLOG(Fatal, ZtString{} <<
	"Zdb could not open or create \"" << name << "\": " << e);
    return nullptr;
  }
  file->sync_();
  return file;
}

void DB::delFile(File *file) // file thread
{
  bool lastFile;
  uint64_t id = file->id();
  // FIXME - move evict/close/remove to write thread
  if (m_files->delNode(file)) m_fileLRU.delNode(file);
  lastFile = id == m_lastFile;
  if (ZuUnlikely(lastFile)) getFile(id + 1, true);
  file->close();
  ZiFile::remove(fileName(id));
}

IndexBlk *DB::getIndexBlk(File *file, uint64_t id, bool create) // cacheLock held
{
  ++m_indexBlkLoads;
  if (IndexBlk *indexBlk = static_cast<IndexBlk *>(m_indexBlks->findPtr(id))) {
    m_indexBlkLRU.pushNode(m_indexBlkLRU.delNode(indexBlk));
    return indexBlk;
  }
  ++m_indexBlkMisses;
  ZmRef<IndexBlk> indexBlk =
    create ? file->writeIndexBlk(id) : file->readIndexBlk(id);
  if (ZuUnlikely(!indexBlk)) return nullptr;
  auto indexBlkPtr = indexBlk.ptr();
  // eviction
  if (m_indexBlks->count_() >= m_indexBlkCacheSize)
    if (IndexBlk *lru = static_cast<IndexBlk *>(m_indexBlkLRU.shiftNode())) {
      if (File *file =
	  getFile(lru->id>>(fileShift() - indexShift()), false))
	file->writeIndexBlk(lru);
      m_indexBlks->delNode(lru); // FIXME - move write/evict to write thread
    }
  m_indexBlks->addNode(ZuMv(indexBlk));
  m_indexBlkLRU.pushNode(indexBlkPtr);
  return indexBlkPtr;
}

ZmRef<IndexBlk> File_::readIndexBlk(uint64_t id) // cacheLock held
{
  // FIXME - use file lock
  ZmRef<IndexBlk> indexBlk;
  auto offset = m_superBlk.data[id & indexMask()];
  if (offset) {
    indexBlk = new IndexBlk{id, offset};
    if (!readIndexBlk(indexBlk)) return nullptr;
    return indexBlk;
  }
  return nullptr;
}

// FIXME - need a write cache indexed by index block, each cached index block should
// include boolean indicating whether or not write is pending, so that multiple writes are not scheduled for the same block, and writes can be canceled when eviction is reversed - this is unlike the object cache which is immutable - the cached index blocks are inherently mutable
// FIXME - no need for separate write cache for index blocks, just mark them as pending write (which can be canceled) - and write implies post-write eviction by write thread - also use a lock hierarchy (cache container then each individual index block with an individual lock held during write)

ZmRef<IndexBlk> File_::writeIndexBlk(uint64_t id) // cacheLock held
{
  // FIXME - use file lock
  ZmRef<IndexBlk> indexBlk;
  if (indexBlk = readIndexBlk(id)) return indexBlk;
  auto offset = m_offset;
  m_superBlk.data[id & indexMask()] = offset;
  indexBlk = new IndexBlk{id, offset};
  ZuAssert(sizeof(FileIndexBlk) == sizeof(IndexBlk::Blk));
  // shortcut endian conversion when initializing a blank index block
  memset(&indexBlk->blk, 0, sizeof(FileIndexBlk));
  {
    int r;
    ZeError e;
    if (ZuUnlikely((r = pwrite(offset,
	      &indexBlk->blk, sizeof(FileIndexBlk), &e)) != Zi::OK)) {
      m_db->fileWrError_(static_cast<File *>(this), offset, e);
      return nullptr;
    }
  }
  m_offset = offset + sizeof(FileIndexBlk);
  return indexBlk;
}

bool File_::readIndexBlk(IndexBlk *indexBlk) // cacheLock held
{
  int r;
  ZeError e;
#if Zu_BIGENDIAN
  FileIndexBlk fileIndexBlk;
  auto indexData = &(fileIndexBlk.data[0]);
#else
  auto indexData = &indexBlk->blk.data[0];
#endif
  if (ZuUnlikely((r = pread(indexBlk->offset,
	    &indexData[0], sizeof(FileIndexBlk), &e)) != Zi::OK)) {
    m_db->fileRdError_(static_cast<File *>(this), indexBlk->offset, r, e);
    return false;
  }
#if Zu_BIGENDIAN
  for (unsigned i = 0; i < indexRecs(); i++)
    indexBlk->blk.data[i] = { indexData[i].offset, indexData[i].length };
#endif
  return true;
}

bool File_::writeIndexBlk(IndexBlk *indexBlk) // cacheLock held
{
  int r;
  ZeError e;
#if Zu_BIGENDIAN
  FileIndexBlk fileIndexBlk;
  auto indexData = &(fileIndexBlk.data[0]);
  for (unsigned i = 0; i < indexRecs(); i++) {
    const auto &index = indexBlk->blk.data[i];
    indexData[i] = { index.offset, index.length };
  }
#else
  auto indexData = &indexBlk->blk.data[0];
#endif
  if (ZuUnlikely((r = pwrite(indexBlk->offset,
	    &indexData[0], sizeof(FileIndexBlk), &e)) != Zi::OK)) {
    m_db->fileWrError_(static_cast<File *>(this), indexBlk->offset, e);
    return false;
  }
  return true;
}

// read individual record from disk
ZmRef<Buf> DB::read_(const FileRec &rec) // cacheLock held
{
  const auto &index = rec.index();
  if (ZuUnlikely(!index.offset || index.offset == deleted())) {
    ZeLOG(Error, ZtString{} << "Zdb internal error on DBID " << config().id <<
	" bitmap inconsistent with index for RN " << rec.rn());
    return nullptr;
  }
  Zfb::IOBuilder<Buf> fbb;
  Zfb::Offset<Zfb::Vector<uint8_t>> data;
  uint8_t *ptr = nullptr;
  if (index.length) {
    ptr = Zfb::Save::extend(fbb, index.length, data);
    if (data.IsNull() || !ptr) return nullptr;
  }
  FileRecTrlr trlr;
  {
    File *file = rec.file();
    int r;
    ZeError e;
    if (index.length) {
      // read record
      if (ZuUnlikely((r = file->pread(index.offset,
		ptr, index.length, &e)) != Zi::OK)) {
	fileRdError_(file, index.offset, r, e);
	return nullptr;
      }
    }
    // read trailer
    if (ZuUnlikely((r = file->pread(index.offset + index.length,
	      &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
      fileRdError_(file, index.offset + index.length, r, e);
      return nullptr;
    }
  }
  uint32_t magic = trlr.magic;
  if (magic != ZdbCommitted) return nullptr;
  auto id = Zfb::Save::id(config().id);
  auto msg = fbs::CreateMsg(fbb, fbs::Body_Rec,
      fbs::CreateRecord(fbb,
	&id, trlr.rn, trlr.prevRN, trlr.seqLenOp, data).Union());
  fbb.Finish(msg);
  return saveHdr(fbb, this);
}

// write individual record to disk
// - updates the file bitmap, index block and appends the record on disk
ZdbRN DB::write2(const Buf *buf)
{
  auto record = record_(msg_(buf->hdr()));
  if (ZuUnlikely(!record)) return nullRN();
  RN rn = record->rn();
  auto data = Zfb::Load::bytes(record->data());
  {
    FileRec rec = rn2file(rn, true);
    if (!rec) return nullRN();
    auto &index = rec.index();
    File *file = rec.file();
    // FIXME - move below into File member function
    file->alloc(rn & fileRecMask());
    // FIXME
    index.offset = file->append(data.length() + sizeof(FileRecTrlr));
    index.length = data.length();
    FileRecTrlr trlr{
      .rn = rn,
      .prevRN = record->prevRN(),
      .seqLenOp = record->seqLenOp(),
      .magic = ZdbCommitted
    };
    int r;
    ZeError e;
    if (data) {
      // write record
      if (ZuUnlikely((r = file->pwrite(index.offset,
		data.data(), data.length(), &e)) != Zi::OK)) {
	fileWrError_(file, index.offset, e);
	index.offset = 0;
	index.length = 0;
	return nullRN();
      }
    }
    // write trailer
    if (ZuUnlikely((r = file->pwrite(index.offset + index.length,
	      &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
      fileWrError_(file, index.offset + index.length, e);
      index.offset = 0;
      index.length = 0;
      return nullRN();
    }
  }
  return rn;
}

bool DB::ack(RN rn)
{
  if (ZuUnlikely(rn <= m_minRN)) return false;
  if (ZuUnlikely(rn > m_nextRN)) rn = m_nextRN;
  if (m_vacuumRN == nullRN()) {
    auto deleteRN = m_deletes.minimumKey();
    if (ZuUnlikely(deleteRN == nullRN())) return false;
    if (rn <= deleteRN) return false;
    m_vacuumRN = rn;
    return true;
  }
  if (rn > m_vacuumRN) m_vacuumRN = rn;
  return false;
}

// vacuuming is performed in small batches, yielding between each batch

void DB::vacuum()
{
  if (ZuUnlikely(m_vacuumRN == nullRN())) return;
  // FIXME - need to accumulate a batch in ZmAlloc'd buffer, then iterate through it calling del_() , to avoid contending with m_deletes addition
  auto i = m_deletes.iterator();
  unsigned j = 0;
  ZuPair<int, RN> outcome;
  while (auto node = i.iterate()) {
    if (node->key() >= m_vacuumRN) break;
    outcome = del_(node->val(), config().vacuumBatch - j);
    if (outcome.p<0>() < 0) goto again1;
    i.del(node);
    if ((j += outcome.p<0>()) >= config().vacuumBatch) goto again2;
  }
  m_vacuumRN = nullRN();
  return;

again1:
  if (outcome.p<1>() != nullRN()) // split long sequence
    m_deletes.add(outcome.p<1>(),
	DeleteOp{outcome.p<1>(), SeqLenOp::mk(-outcome.p<0>(), Op::Delete)});

again2:
  run([this]() { vacuum(); });
}

// del_() returns a {int, RN} pair:
// 0, nullRN() - nothing to do
// +ve, nullRN() - all done (work was within batch size), continue
// -ve, nullRN() - work exceeded batch size, some work done, re-attempt
// -ve, rn - work exceeded batch size, need to split sequence at rn
//   (remaining sequence length is encoded in the negative return code)
ZuPair<int, RN> DB::del_(
    const DeleteOp &deleteOp, unsigned maxBatchSize)
{
  RN rn = deleteOp.rn;

  switch (SeqLenOp::op(deleteOp.seqLenOp)) {
    default:
      return {0, nullRN()};
    case Op::Put:
      break;
    case Op::Delete:
      break;
    case Op::Purge: {
      RN minRN = m_minRN;
      unsigned i = 0;
      if (minRN < rn) {
	do {
	  del__(minRN++);
	} while (++i < maxBatchSize && minRN < rn);
	m_minRN = minRN;
      }
      if (i >= maxBatchSize) return {-1, nullRN()}; // re-attempt
      return {static_cast<int>(i), nullRN()};
    }
  }

  auto seqLen = SeqLenOp::seqLen(deleteOp.seqLenOp);

  if (ZuUnlikely(!seqLen)) return {0, nullRN()};

  auto batchSize = seqLen;
  if (batchSize > maxBatchSize) batchSize = maxBatchSize;

  auto batch = ZuAlloc(RN, batchSize);
  if (!batch) return {0, nullRN()};

  // fill the batch
  unsigned i = 0;
  do {
    batch[i++] = rn;
    rn = del_prevRN(rn);
  } while (i < batchSize && rn != nullRN());

  // sequence is longer than the batch size, need to split it
  if (rn != nullRN())
    return {-static_cast<int>(seqLen - batchSize), rn};

  // delete the oldest batched RNs in reverse order (oldest first)
  for (unsigned j = i; j-- > 0; ) del__(batch[j]);

  // entire sequence was deleted
  return {static_cast<int>(i), nullRN()};
}

// obtain prevRN for a record pending deletion
RN DB::del_prevRN(RN rn)
{
  if (ZuUnlikely(rn < m_minRN || rn >= m_nextRN)) return nullRN();
  if (ZmRef<AnyObject> object = cacheDel_(rn))
    return object->prevRN();
  FileRec rec = rn2file(rn, false);
  if (!rec) return nullRN();
  const auto &index = rec.index();
  if (ZuUnlikely(!index.offset || index.offset == deleted())) {
    ZeLOG(Error, ZtString{} << "Zdb internal error on DBID " << config().id <<
	" bitmap inconsistent with index for RN " << rec.rn());
    return nullRN();
  }
  auto offset = index.offset + index.length;
  File *file = rec.file();
  FileRecTrlr trlr;
  int r;
  ZeError e;
  if (ZuUnlikely((r = file->pread(offset,
	    &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
    fileRdError_(file, offset, r, e);
    return nullRN();
  }
  uint32_t magic = trlr.magic;
  if (magic != ZdbCommitted) return nullRN();
  return trlr.prevRN;
}

// delete individual record
void DB::del__(RN rn)
{
  FileRec rec = rn2file(rn, false);
  if (!rec) return;
  rec.index().offset = deleted();
  if (rec.file()->del(rn & fileRecMask()))
    delFile(rec.file());
}

// transition to standalone, trigger vacuuming
void DB::standalone()
{
  if (ack(maxRN())) vacuum();
}

// disk read error
void DB::fileRdError_(
    File *file, ZiFile::Offset off, int r, ZeError e)
{
  if (r < 0) {
    ZeLOG(Error, ZtString{} <<
	"Zdb pread() failed on \"" << fileName(file->id()) <<
	"\" at offset " << ZuBoxed(off) <<  ": " << e);
  } else {
    ZeLOG(Error, ZtString{} <<
	"Zdb pread() truncated on \"" << fileName(file->id()) <<
	"\" at offset " << ZuBoxed(off));
  }
}

// disk write error
void DB::fileWrError_(File *file, ZiFile::Offset off, ZeError e)
{
  ZeLOG(Error, ZtString{} <<
      "Zdb pwrite() failed on \"" << fileName(file->id()) <<
      "\" at offset " << ZuBoxed(off) <<  ": " << e);
}

} // namespace Zdb_
