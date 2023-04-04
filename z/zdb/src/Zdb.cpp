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

#include <assert.h>
#include <errno.h>

namespace Zdb_ {

void Env::init(EnvCf config, ZiMultiplex *mx, EnvHandler handler)
{
  if (!ZmEngine<Env>::lock(
	ZmEngineState::Stopped, [this, &config, mx, &handler]() mutable {
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
      while (auto dbCf_ = i.iterate()) {
	auto &dbCf = const_cast<DBCf &>(dbCf_->val());
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
	if (!dbCf.vacuumBatch) dbCf.vacuumBatch = config.vacuumBatch;
      }
    }

    m_cf = ZuMv(config);
    m_mx = mx;

    m_handler = ZuMv(handler);

    return true;
  }))
    throw ZtString{} << "ZdbEnv::init called out of order";
}

DB *Env::initDB_(ZuID id, DBHandler handler)
{
  DB *db = nullptr;
  if (!ZmEngine<Env>::lock(ZmEngineState::Stopped,
	[this, &db, id, handler = ZuMv(handler)]() {
    if (state() != HostState::Initialized) return false;
    auto cf = m_cf.dbCfs.find(id);
    if (!cf) m_cf.dbCfs.addNode(cf = new DBCfs::Node{id});
    if (m_dbs.findPtr(id)) return false;
    db = new DBs::Node{this, &(cf->val())};
    db->init(ZuMv(handler));
    m_dbs.addNode(db);
    return true;
  }))
    throw ZtString{} << "ZdbEnv::initDB called out of order";
  return db;
}

void Env::final()
{
  if (!ZmEngine<Env>::lock(ZmEngineState::Stopped, [this]() {
    if (state() != HostState::Instantiated) return false;
    all_([](DB *db) { db->final(); });
    m_handler = {};
    return true;
  }))
    throw ZtString{} << "ZdbEnv::final called out of order";
}

void Env::wake()
{
  run([this]() { stopped(); });
}

void Env::start_()
{
  ZmAssert(invoked());

  using namespace HostState;

  if (state() != Initialized) {
    ZeLOG(Fatal, "Env::start_() called out of order");
    started(false);
    return;
  }

  ZeLOG(Info, "Zdb starting");

  // initialize hosts, connections
  // finalize connections, hosts
  m_hostIndex.clean();
  m_hosts = new Hosts{};
  {
    unsigned dbCount = m_dbs.count_();
    auto i = m_cf.hostCfs.readIterator();
    while (auto node = i.iterate()) {
      auto host = new Hosts::Node{this, &(node->data()), dbCount};
      m_hosts->addNode(host);
      m_hostIndex.addNode(host);
    }
  }
  m_self = m_hosts->findPtr(m_cf.hostID);
  if (!m_self) {
    ZeLOG(Fatal, (ZtString{} <<
      "Zdb own host ID " << m_cf.hostID << " not in hosts table"));
    started(false);
    return;
  }
  m_self->state(Initialized);

  // open and recover all databases
  {
    auto i = m_cf.dbCfs.readIterator();
    while (auto cf = i.iterate())
      if (!m_dbs.findPtr(cf->key()))
	m_dbs.addNode(new DBs::Node{this, &(cf->val())});
  }
  {
    ZmAtomic<unsigned> ok = true;
    allSync([&ok](DB *db) { return [&ok, db]() {
      if (ZuLikely(ok.load_())) ok &= db->open();
    }; });
    if (!ok) {
      allSync([](DB *db) { return [db]() { db->close(); }; });
      started(false);
      return;
    }
  }

  // refresh db state vector, begin election
  envStateRefresh();
  m_self->state(Stopped);
  repStop();
  m_self->state(Electing);

  if (!(m_nPeers = m_hosts->count_() - 1)) { // standalone
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

  using namespace HostState;

  state(Stopping);
  repStop();
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

  allSync([](DB *db) { return [db]() { db->close(); }; });

  stopped(true);
}

bool Env::disconnectAll()
{
  ZmAssert(invoked());

  bool disconnected = false;
  auto i = m_cxns.readIterator();
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

  using namespace HostState;

  if (state() != Electing) return;

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
    if (!appActive) up_(oldMaster);
  } else {
    if (appActive) down_();
  }

  m_self->state(won ? Active : Inactive);
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

  if (!m_self) {
badorder:
    ZeLOG(Fatal, "Env::deactivate called out of order");
    return;
  }

  using namespace HostState;

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
  if (!appActive) up_(nullptr);
}

void Env::up_(Host *oldMaster)
{
  ZeLOG(Info, "Zdb ACTIVE");
  if (ZtString cmd = m_self->config().up) {
    if (oldMaster) cmd << ' ' << oldMaster->config().ip;
    ZeLOG(Info, ZtString{} << "Zdb invoking \"" << cmd << '\"');
    ::system(cmd);
  }
  m_handler.upFn(this, oldMaster);
}

void Env::down_()
{
  ZeLOG(Info, "Zdb INACTIVE");
  if (ZuString cmd = m_self->config().down) {
    ZeLOG(Info, ZtString{} << "Zdb invoking \"" << cmd << '\"');
    ::system(cmd);
  }
  m_handler.downFn(this);
}

ZvTelemetry::ZdbEnvFn Env::telFn()
{
  return ZvTelemetry::ZdbEnvFn{ZmMkRef(this), [](
      Env *dbEnv,
      ZvTelemetry::BuildZdbEnvFn envFn,
      ZvTelemetry::BuildZdbHostFn hostFn,
      ZvTelemetry::BuildZdbFn dbFn,
      bool update) {
    dbEnv->invoke([dbEnv,
	envFn = ZuMv(envFn),
	hostFn = ZuMv(hostFn),
	dbFn = ZuMv(dbFn), update]() {
      {
	ZvTelemetry::IOBuilder fbb;
	envFn(fbb, dbEnv->telemetry(fbb, update));
	dbEnv->allHosts([&hostFn, &fbb, update](const Host *host) {
	  hostFn(fbb, host->telemetry(fbb, update));
	});
      }
      dbEnv->all([dbFn = ZuMv(dbFn), update](const DB *db) {
	return [dbFn, update, db]() {
	  ZvTelemetry::IOBuilder fbb;
	  dbFn(fbb, db->telemetry(fbb, update));
	};
      });
    });
  }};
}

Zfb::Offset<ZvTelemetry::fbs::ZdbEnv>
Env::telemetry(ZvTelemetry::IOBuilder &fbb_, bool update) const
{
  using namespace Zfb;
  using namespace Zfb::Save;
  Zfb::Offset<String> thread, fileThread;
  if (!update) {
    thread = str(fbb_, m_cf.thread);
    fileThread = str(fbb_, m_cf.fileThread);
  }
  ZvTelemetry::fbs::ZdbEnvBuilder fbb{fbb_};
  if (!update) {
    fbb.add_thread(thread);
    fbb.add_fileThread(fileThread);
    { auto v = id(m_self->id()); fbb.add_self(&v); }
  }
  { auto v = id(m_master ? m_master->id() : ZuID{}); fbb.add_master(&v); }
  { auto v = id(m_prev ? m_prev->id() : ZuID{}); fbb.add_prev(&v); }
  { auto v = id(m_next ? m_next->id() : ZuID{}); fbb.add_next(&v); }
  fbb.add_nCxns(m_cxns.count_());
  if (!update) {
    fbb.add_heartbeatFreq(m_cf.heartbeatFreq);
    fbb.add_heartbeatTimeout(m_cf.heartbeatTimeout);
    fbb.add_reconnectFreq(m_cf.reconnectFreq);
    fbb.add_electionTimeout(m_cf.electionTimeout);
    fbb.add_nDBs(m_dbs.count_());
    fbb.add_nHosts(m_hosts->count_());
    fbb.add_nPeers(m_nPeers);
  }
  auto state = this->state();
  fbb.add_state(static_cast<ZvTelemetry::fbs::ZdbHostState>(state));
  fbb.add_active(state == HostState::Active);
  fbb.add_recovering(m_recovering);
  fbb.add_replicating(Host::replicating(m_next));
  return fbb.Finish();
}

Zfb::Offset<ZvTelemetry::fbs::ZdbHost>
Host::telemetry(ZvTelemetry::IOBuilder &fbb_, bool update) const
{
  using namespace Zfb;
  using namespace Zfb::Save;
  ZvTelemetry::fbs::ZdbHostBuilder fbb{fbb_};
  if (!update) {
    { auto v = Zfb::Save::ip(config().ip); fbb.add_ip(&v); }
    { auto v = Zfb::Save::id(config().id); fbb.add_id(&v); }
    fbb.add_priority(config().priority);
    fbb.add_port(config().port);
  }
  fbb.add_state(static_cast<ZvTelemetry::fbs::ZdbHostState>(m_state));
  fbb.add_voted(m_voted);
  return fbb.Finish();
}

Host::Host(Env *env, const HostCf *cf, unsigned dbCount) :
  m_env{env},
  m_cf{cf},
  m_mx{env->mx()},
  m_envState{dbCount}
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

Cxn_::Cxn_(Env *env, Host *host, const ZiCxnInfo &ci) :
  ZiConnection{env->mx(), ci},
  m_env{env},
  m_host{host}
{
}

void Cxn_::connected(ZiIOContext &io)
{
  if (!m_env->running()) { io.disconnect(); return; }

  m_env->run([self = ZmMkRef(this)]() mutable {
    auto env = self->env();
    env->connected(ZuMv(self));
  });

  m_env->run([self = ZmMkRef(this)]() { self->hbTimeout(); },
      ZmTimeNow(static_cast<int>(m_env->config().heartbeatTimeout)),
      ZmScheduler::Defer, &m_hbTimer);

  msgRead(io);
}

void Env::connected(ZmRef<Cxn> cxn)
{
  ZmAssert(invoked());

  if (!cxn->up()) return;

  if (Host *host = cxn->host()) associate(cxn, host);

  hbSend_(cxn);

  m_cxns.addNode(ZuMv(cxn));
}

void Env::associate(Cxn *cxn, ZuID hostID)
{
  ZmAssert(invoked());

  Host *host = m_hosts->find(hostID);

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
      ZmTimeNow(static_cast<int>(m_env->config().reconnectFreq)),
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

  mx()->del(&m_hbTimer);

  m_env->run([self = ZmMkRef(this)]() mutable {
    auto env = self->env();
    env->disconnected(ZuMv(self));
  });
}

void Env::disconnected(ZmRef<Cxn> cxn)
{
  ZmAssert(invoked());

  m_cxns.delNode(cxn);

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

  envStateRefresh();

  m_master = nullptr;
  m_nPeers = 0;

  {
    auto i = m_hostIndex.readIterator();

    ZdbDEBUG(this, ZtString{} << "setMaster()\n" << 
	" self=" << ZuPrintPtr{m_self} << '\n' <<
	" prev=" << ZuPrintPtr{m_prev} << '\n' <<
	" next=" << ZuPrintPtr{m_next} << '\n' <<
	" recovering=" << m_recovering <<
	" replicating=" << Host::replicating(m_next));

    while (Host *host = i.iterate()) {
      ZdbDEBUG(this, ZtString{} <<
	  " host=" << ZuPrintPtr{host} << '\n' <<
	  " master=" << ZuPrintPtr{m_master});

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

  if (m_next) {
    m_standalone = false;
    repStart();
  } else {
    m_standalone = true;
    all_file([](DB *db) { return [db]() { db->ack(maxRN()); }; });
  }
}

void Env::setNext()
{
  ZmAssert(invoked());

  Host *next = nullptr;

  {
    auto i = m_hostIndex.readIterator();

    ZdbDEBUG(this, ZtString{} << "setNext()\n" <<
	" self=" << ZuPrintPtr{m_self} << '\n' <<
	" master=" << ZuPrintPtr{m_master} << '\n' <<
	" prev=" << ZuPrintPtr{m_prev} << '\n' <<
	" next=" << ZuPrintPtr{m_next} << '\n' <<
	" recovering=" << m_recovering <<
	" replicating=" << Host::replicating(m_next));

    while (Host *host = i.iterate()) {
      if (host != m_self && host != m_prev && host->voted() &&
	  m_self->cmp(host) >= 0 && (!next || host->cmp(next) > 0))
	next = host;

      ZdbDEBUG(this, ZtString{} <<
	  " host=" << ZuPrintPtr{host} << '\n' <<
	  " next=" << *next);
    }
  }

  setNext(next);
}

void Env::repStart()
{
  ZmAssert(invoked());

  ZeLOG(Info, ZtString{} <<
	"Zdb host " << m_next->id() << " is next in line");

  envStateRefresh();

  ZdbDEBUG(this, ZtString{} << "repStart()\n" <<
      " self=" << ZuPrintPtr{m_self} << '\n' <<
      " master=" << ZuPrintPtr{m_master} << '\n' <<
      " prev=" << ZuPrintPtr{m_prev} << '\n' <<
      " next=" << ZuPrintPtr{m_next} << '\n' <<
      " recovering=" << m_recovering <<
      " replicating=" << Host::replicating(m_next));

  if (m_self->envState().cmp(m_next->envState()) < 0 || m_recovering) return;

  // ZeLOG(Info, "repStart() initiating recovery");
  m_recover = m_next->envState();
  m_recoverEnd = m_self->envState();
  if (ZmRef<Cxn> cxn = m_next->cxn()) {
    auto i = m_recover.readIterator();
    while (auto state = i.iterate()) {
      ZuID id = state->template p<0>();
      if (auto endState = m_recoverEnd.find(id))
	if (auto db = m_dbs.findPtr(id)) {
	  ++m_recovering;
	  db->run([db, cxn,
	      rn = state->p<1>(), endRN = endState->p<1>()]() mutable {
	    db->recSend(ZuMv(cxn), rn, endRN);
	  });
	}
    }
  }
}

void DB::recSend(ZmRef<Cxn> cxn, RN rn, RN endRN, RN gapRN)
{
  ZmAssert(invoked());

  if (!cxn->up()) return;
  if (auto buf = repBuf(rn)) {
    recSend_(ZuMv(cxn), rn, endRN, gapRN, ZuMv(buf));
    return;
  }
  fileRun([this, cxn = ZuMv(cxn), rn, endRN, gapRN]() mutable {
    recSend_file(ZuMv(cxn), rn, endRN, gapRN);
  });
}

void DB::recSend_file(ZmRef<Cxn> cxn, RN rn, RN endRN, RN gapRN)
{
  ZmAssert(fileInvoked());

  if (!cxn->up()) return;
  if (FileRec rec = rn2file<false>(rn))
    if (auto buf = read(rec)) {
      recSend_(ZuMv(cxn), rn, endRN, gapRN, ZuMv(buf));
      return;
    }
  if (gapRN != nullRN()) gapRN = rn;
  if (++rn >= endRN) { // trailing gap
    cxn->send(repGap(gapRN, rn - gapRN));
    m_env->invoke([env = m_env]() { env->recEnd(); });
    return;
  }
  run([this, cxn, rn, endRN, gapRN]() mutable {
    recSend(ZuMv(cxn), rn, endRN, gapRN);
  });
}

void DB::recSend_(ZmRef<Cxn> cxn, RN rn, RN endRN, RN gapRN, ZmRef<Buf> buf)
{
  if (gapRN != nullRN()) cxn->send(repGap(gapRN, rn - gapRN));
  cxn->send(ZuMv(buf));
  if (++rn < endRN)
    run([this, cxn, rn, endRN]() mutable { recSend(ZuMv(cxn), rn, endRN); });
  else
    m_env->invoke([env = m_env]() { env->recEnd(); });
}

void Env::recEnd()
{
  if (m_recovering) --m_recovering;
}

// prepare replication buffer for sending
ZmRef<Buf> DB::repBuf(RN rn)
{
  ZmAssert(invoked());

  // recover from outbound replication buffer cache
  if (auto buf = findBuf(rn)) {
    auto record = record_(msg_(buf->hdr()));
    auto repData = Zfb::Load::bytes(record->data());
    IOBuilder fbb;
    Zfb::Offset<Zfb::Vector<uint8_t>> data;
    if (repData) {
      uint8_t *ptr;
      data = Zfb::Save::pvector_(fbb, repData.length(), ptr);
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
  // recover from object cache (without falling through to reading from disk)
  if (auto object = m_objCache.findSync<false>(rn))
    return object->replicate(fbs::Body_Rec);
  return nullptr;
}

void Env::repStop()
{
  ZmAssert(invoked());

  m_master = nullptr;
  m_prev = nullptr;
  m_next = nullptr;
  m_recovering = false;
  {
    auto i = m_hostIndex.readIterator();
    while (Host *host = i.iterate()) host->voted(false);
  }
  m_self->voted(true);
  m_nPeers = 1;
}

void Cxn_::msgRead(ZiIOContext &io)
{
  recv<
    [](Cxn_ *, const ZiIOContext &, const Buf *buf) -> int {
      return loadHdr(buf);
    },
    [](Cxn_ *cxn, const ZiIOContext &, ZmRef<Buf> buf) -> int {
      return cxn->msgRead2(ZuMv(buf));
    }>(io);
}
int Cxn_::msgRead2(ZmRef<Buf> buf)
{
  return verifyHdr(ZuMv(buf), [this](const Hdr *hdr, ZmRef<Buf> buf) -> int {
    auto msg = Zdb_::msg(hdr);
    if (ZuUnlikely(!msg)) return -1;

    auto length = static_cast<uint32_t>(hdr->length);

    switch (static_cast<int>(msg->body_type())) {
      case fbs::Body_HB:
      case fbs::Body_Gap:
      case fbs::Body_Rep:
      case fbs::Body_Rec:
	if (ZuLikely(buf->length))
	  m_env->run([cxn = ZmMkRef(this), buf = ZuMv(buf)]() mutable {
	    cxn->msgRead3(ZuMv(buf));
	  });
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

  if (!up()) return;

  auto msg = Zdb_::msg(buf->hdr());
  if (!msg) return;
  switch (static_cast<int>(msg->body_type())) {
    case fbs::Body_HB:
      hbRcvd(hb_(msg));
      break;
    case fbs::Body_Rep:
    case fbs::Body_Rec:
      repRecRcvd(ZuMv(buf));
      break;
    case fbs::Body_Gap:
      repGapRcvd(ZuMv(buf));
      break;
  }
}

void Cxn_::hbRcvd(const fbs::Heartbeat *hb)
{
  if (!m_host) m_env->associate(
      static_cast<Cxn *>(this), Zfb::Load::id(hb->host()));

  if (!m_host) { disconnect(); return; }

  m_env->hbRcvd(m_host, hb);
}

// process received heartbeat
void Env::hbRcvd(Host *host, const fbs::Heartbeat *hb)
{
  ZdbDEBUG(this, ZtString{} << "hbDataRcvd()\n" << 
	" host=" << ZuPrintPtr{host} << '\n' <<
	" self=" << ZuPrintPtr{m_self} << '\n' <<
	" master=" << ZuPrintPtr{m_master} << '\n' <<
	" prev=" << ZuPrintPtr{m_prev} << '\n' <<
	" next=" << ZuPrintPtr{m_next} << '\n' <<
	" recovering=" << m_recovering <<
	" replicating=" << Host::replicating(m_next));

  host->state(hb->state());
  host->envState().load(hb->envState());

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
	    reactivate(host);
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
      db->fileRun([db, rn]() { db->ack(rn); });
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

// send replication message to next-in-line
bool Env::replicate(ZmRef<Buf> buf)
{
  if (m_next)
    if (ZmRef<Cxn> cxn = m_next->cxn()) {
      cxn->send(ZuMv(buf));
      return true;
    }
  return false;
}

// broadcast heartbeat
void Env::hbSend()
{
  ZmAssert(invoked());

  hbSend_();

  run([this]() { hbSend(); },
    m_hbSendTime += (time_t)m_cf.heartbeatFreq,
    ZmScheduler::Defer, &m_hbSendTimer);
}

// send heartbeat (broadcast)
void Env::hbSend_()
{
  ZmAssert(invoked());

  envStateRefresh();
  auto i = m_cxns.readIterator();
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
  IOBuilder fbb;
  {
    const auto &envState = self->envState();
    auto id = Zfb::Save::id(self->id());
    auto msg = fbs::CreateMsg(fbb, fbs::Body_HB, 
	fbs::CreateHeartbeat(fbb, &id,
	  m_env->state(), envState.save(fbb)).Union());
    fbb.Finish(msg);
  }

  send(saveHdr(fbb, this));

  ZdbDEBUG(m_env, ZtString{} << "hbSend()"
      "  self[ID:" << self->id() << " S:" << m_env->state() <<
      " N:" << self->envState().count_() << "] " << self->envState());
}

// refresh db state vector
void Env::envStateRefresh()
{
  ZmAssert(invoked());

  EnvState &envState = m_self->envState();
  all_([&envState](DB *db) {
    envState.update(db->config().id, db->nextRN());
  });
}

// process received replicated record
void Cxn_::repRecRcvd(ZmRef<Buf> buf)
{
  ZmAssert(m_env->invoked());

  if (!m_host) return;
  auto record = record_(msg_(buf->hdr()));
  auto id = Zfb::Load::id(record->db());
  DB *db = m_env->db(id);
  if (ZuUnlikely(!db)) return;
  buf->owner = db;
  ZdbDEBUG(m_env, ZtString{} <<
      "replicated(host=" << m_host->id() << ", " <<
      Record_Print{record} << ')');
  m_env->replicated(m_host, id, record->rn() + 1);
  db->invoke([buf = ZuMv(buf)]() mutable {
    auto db = buf->db();
    db->repRecRcvd(ZuMv(buf));
  });
}

// process received replicated gap
void Cxn_::repGapRcvd(ZmRef<Buf> buf)
{
  ZmAssert(m_env->invoked());

  if (!m_host) return;
  auto gap = gap_(msg_(buf->hdr()));
  auto id = Zfb::Load::id(gap->db());
  DB *db = m_env->db(id);
  if (ZuUnlikely(!db)) return;
  buf->owner = db;
  ZdbDEBUG(m_env, ZtString{} <<
      "replicated(host=" << m_host->id() << ", " <<
      Gap_Print{gap} << ')');
  m_env->replicated(m_host, db->config().id, gap->rn() + gap->count());
  db->invoke([buf = ZuMv(buf)]() mutable {
    auto db = buf->db();
    db->repGapRcvd(ZuMv(buf));
  });
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

DB::DB(Env *env, DBCf *cf) :
  m_env{env}, m_mx{env->mx()}, m_cf{cf},
  m_path{ZiFile::append(env->config().path, cf->id)},
  m_repBufs{new RepBufs{}}
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
Zfb::Offset<ZvTelemetry::fbs::Zdb>
DB::telemetry(ZvTelemetry::IOBuilder &fbb_, bool update) const
{
  using namespace Zfb;
  using namespace Zfb::Save;
  Zfb::Offset<String> path, name, thread, fileThread;
  if (!update) {
    path = str(fbb_, m_path);
    name = str(fbb_, config().id);
    thread = str(fbb_, config().thread);
    fileThread = str(fbb_, config().fileThread);
  }
  ZvTelemetry::fbs::ZdbBuilder fbb{fbb_};
  if (!update) {
    fbb.add_path(path);
    fbb.add_name(name);
    fbb.add_thread(thread);
    fbb.add_fileThread(fileThread);
  }
  fbb.add_minRN(m_minRN.load_());
  fbb.add_nextRN(m_nextRN.load_());
  {
    ObjCache::Stats stats;
    m_objCache.stats(stats);
    fbb.add_objCacheLoads(stats.loads);
    fbb.add_objCacheMisses(stats.misses);
    if (!update) fbb.add_objCacheSize(stats.size);
  }
  {
    FileCache::Stats stats;
    m_files.stats(stats);
    fbb.add_fileCacheLoads(stats.loads);
    fbb.add_fileCacheMisses(stats.misses);
    if (!update) fbb.add_fileCacheSize(stats.size);
  }
  {
    IndexBlkCache::Stats stats;
    m_indexBlks.stats(stats);
    fbb.add_indexBlkCacheLoads(stats.loads);
    fbb.add_indexBlkCacheMisses(stats.misses);
    if (!update) fbb.add_indexBlkCacheSize(stats.size);
  }
  if (!update) {
    fbb.add_cacheMode(
	static_cast<ZvTelemetry::fbs::ZdbCacheMode>(config().cacheMode));
    fbb.add_warmUp(config().warmUp);
  }
  return fbb.Finish();
}

// load object from buffer
ZmRef<AnyObject> DB::load(const fbs::Record *record)
{
  auto prevRN = record->prevRN();
  auto seqLenOp = record->seqLenOp();
  switch (SeqLenOp::op(seqLenOp)) {
    case Op::Delete:
    case Op::Purge:
      return nullptr;
  }
  ZmRef<AnyObject> object;
  if (prevRN != nullRN()) object = m_objCache.del(prevRN);
  auto data = Zfb::Load::bytes(record->data());
  if (!object) {
    if (auto fn = m_handler.loadFn)
      object = fn(this, data.data(), data.length());
  } else {
    if (auto fn = m_handler.updateFn)
      object = fn(object, data.data(), data.length());
  }
  if (object) object->init(record->rn(), prevRN, seqLenOp);
  return object;
}

// save object to buffer
Zfb::Offset<void> DB::save(Zfb::Builder &fbb, AnyObject_ *object)
{
  return m_handler.saveFn(fbb, object->ptr_());
}

bool DB::recover()
{
  ZmAssert(fileInvoked());

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
  return subDirs.all([&](unsigned i, bool) {
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
    return files.all([&](unsigned j, bool) {
#ifdef _WIN32
      ZtString fileName_;
#else
      auto &fileName_ = fileName;
#endif
      uint64_t id = (static_cast<uint64_t>(i)<<20) | j;
      if (ZmRef<File> file = openFile_<false>(fileName_, id))
	return recover(file);
      return false;
    });
  });
}

bool DB::recover(File *file)
{
  ZmAssert(fileInvoked());

  if (!file->allocated()) return true;
  if (file->deleted() >= fileRecs()) { delFile(file); return true; }
  RN rn = (file->id())<<fileShift();
  ZmRef<IndexBlk> blk;
  int first = file->first();
  if (ZuUnlikely(first < 0)) return true;
  int last = file->last();
  if (ZuUnlikely(last < 0)) return false; // file corrupt
  rn += first;
  for (int j = first; j <= last; j++, rn++) {
    if (!file->exists(j)) continue;
    auto blkID = rn>>indexShift();
    if (!blk || blk->id != blkID) blk = file->readIndexBlk(blkID);
    if (!blk) return false; // I/O error on file
    if (auto buf = read(
	FileRec{file, blk, static_cast<unsigned>(rn & indexMask())}))
      invoke([buf = ZuMv(buf)]() mutable {
	auto db = buf->db();
	db->recover(ZuMv(buf));
      });
  }
  return true;
}

// recover buffer from file
void DB::recover(ZmRef<Buf> buf)
{
  ZmAssert(invoked());

  auto record = record_(msg_(buf->hdr()));
  RN rn = record->rn();
  m_minRN.maximum(rn);
  m_nextRN.minimum(rn + 1);
  recover(record);
}

// recover record originating from inbound replication or file recovery
void DB::recover(const fbs::Record *record)
{
  ZmAssert(invoked());

  RN rn = record->rn();
  RN prevRN = record->prevRN();

  auto seqLenOp = record->seqLenOp();

  switch (SeqLenOp::op(seqLenOp)) {
    default:
      return;
    case Op::Put:
      if (SeqLenOp::seqLen(seqLenOp) > 1) {
	m_deletes.add(rn, DeleteOp{prevRN,
	  SeqLenOp::mk(SeqLenOp::seqLen(seqLenOp) - 1, Op::Put)});
	if (auto fn = m_handler.deleteFn) fn(prevRN);
      }
      if (auto object = load(record)) {
	if (auto fn = m_handler.recoverFn) fn(object);
	m_objCache.add(ZuMv(object));
      }
      break;
    case Op::Append:
      if (auto object = load(record)) {
	if (auto fn = m_handler.recoverFn) fn(object);
	m_objCache.add(ZuMv(object));
      }
      break;
    case Op::Delete:
      m_deletes.add(rn, DeleteOp{rn, seqLenOp});
      if (auto fn = m_handler.deleteFn) fn(rn);
      m_objCache.del(prevRN);
      break;
    case Op::Purge:
      m_deletes.add(rn, DeleteOp{prevRN, seqLenOp});
      break;
  }
}

// process inbound replication - record
void DB::repRecRcvd(ZmRef<Buf> buf)
{
  ZmAssert(invoked());

  recover(record_(msg_(buf->hdr())));
  write(ZuMv(buf));
}

// process inbound replication - gap
void DB::repGapRcvd(ZmRef<Buf> buf)
{
  ZmAssert(invoked());

  auto gap = gap_(msg_(buf->hdr()));
  RN rn = gap->rn();
  if (m_nextRN.cmpXch(rn + gap->count(), rn) != rn) return;
  m_env->invoke([env = m_env, buf = ZuMv(buf)]() mutable {
    env->replicate(ZuMv(buf));
  });
}

// outbound replication / persistency
void DB::write(ZmRef<Buf> buf)
{
  ZmAssert(invoked());
  ZmAssert(buf->db() == this);

  m_repBufs->addNode(buf);
  if (config().repMode) { // replicate then disk write (faster)
    m_env->invoke([buf = ZuMv(buf)]() mutable {
      auto db = buf->db();
      auto env = db->env();
      if (env->replicate(buf))
	// replicated - peer will ack
	db->fileRun([buf = ZuMv(buf)]() {
	  auto db = buf->db();
	  db->write2(buf); // return value ignored
	});
      else
	// standalone - ack on successful disk write
	db->fileRun([buf = ZuMv(buf)]() {
	  auto db = buf->db();
	  if (db->write2(buf)) {
	    auto rn = record_(msg_(buf->hdr()))->rn();
	    db->ack(rn + 1);
	  }
	});
    });
  } else { // disk write then replicate (slower but potentially more durable)
    fileRun([buf = ZuMv(buf)]() {
      auto db = buf->db();
      auto env = db->env();
      if (db->write2(buf))
	env->invoke([buf = ZuMv(buf)]() mutable {
	  auto db = buf->db();
	  auto env = db->env();
	  if (!env->replicate(ZuMv(buf))) { // standalone - ack disk write
	    auto rn = record_(msg_(buf->hdr()))->rn();
	    db->fileInvoke([db, rn]() { db->ack(rn + 1); });
	  }
	});
      else
	env->invoke([buf = ZuMv(buf)]() mutable {
	  auto db = buf->db();
	  auto env = db->env();
	  env->replicate(ZuMv(buf)); // return value ignored
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
  ZmAssert(invoked());

  if (m_open) return true;

  bool ok = false;
  ZmBlock<>{}([this, &ok](auto wake) {
    fileInvoke([this, &ok, wake = ZuMv(wake)]() {
      ok = recover();
      wake();
    });
  });
  if (!ok) return false;

  if (config().warmUp) {
    if (auto fn = m_handler.ctorFn)
      run([this, fn]() { ZmRef<AnyObject>{fn(this)}; });
    fileRun([this, rn = m_nextRN.load_()]() { rn2file<true>(rn); });
  }

  m_open = true;
  return true;
}

void DB::close()
{
  ZmAssert(invoked());

  if (!m_open) return;

  ZmBlock<>{}([this](auto wake) {
    fileInvoke([this, wake = ZuMv(wake)]() {
      // index blocks
      m_indexBlks.all<true>([this](ZmRef<IndexBlk> blk) {
	auto fileID = (blk->id)>>(fileShift() - indexShift());
	if (File *file = getFile<true>(fileID))
	  file->writeIndexBlk(blk);
      });

      // files
      m_files.all<true>([](ZmRef<File> file) { file->sync(); });

      wake();
    });
  });

  m_open = false;
}

void Env::checkpoint()
{
  ZmAssert(invoked());

  all_file([](DB *db) { return [db]() { db->checkpoint(); }; });
}

bool DB::checkpoint()
{
  ZmAssert(fileInvoked());

  bool ok = true;
  m_indexBlks.all([this, &ok](ZmRef<IndexBlk> blk) mutable {
    auto fileID = (blk->id)>>(fileShift() - indexShift());
    if (File *file = getFile<true>(fileID))
      ok = ok && file->writeIndexBlk(blk);
  });
  m_files.all([&ok](ZmRef<File> file) mutable {
    ok = ok && file->sync_();
  });
  return ok;
}

ZmRef<AnyObject> DB::placeholder()
{
  if (auto fn = m_handler.ctorFn) return fn(this);
  return nullptr;
}

ZmRef<AnyObject> DB::push_(RN rn)
{
  auto fn = m_handler.ctorFn;
  if (ZuUnlikely(!fn)) return nullptr;
  ZmRef<AnyObject> object = fn(this);
  if (!object) return nullptr;
  object->push(rn);
  m_minRN.maximum(rn);
  m_nextRN = rn + 1;
  return object;
}

ZmRef<AnyObject> DB::push()
{
  return push_(m_nextRN.load_());
}

ZmRef<AnyObject> DB::push(RN rn)
{
  RN nextRN = m_nextRN.load_();
  if (ZuUnlikely(rn != nullRN() && nextRN > rn)) return nullptr;
  return push_(nextRN);
}

bool DB::update_(AnyObject *object, RN rn)
{
  if (ZuUnlikely(object->seqLen() == SeqLenOp::maxSeqLen())) return false;
  if (!object->update(rn)) return false;
  m_minRN.maximum(rn);
  m_nextRN = rn + 1;
  return true;
}

bool DB::update(AnyObject *object)
{
  return update_(object, m_nextRN.load_());
}

bool DB::update(AnyObject *object, RN rn)
{
  RN nextRN = m_nextRN.load_();
  if (ZuUnlikely(rn != nullRN() && nextRN > rn)) return false;
  return update_(object, nextRN);
}

// commits push() / update()
void DB::put(AnyObject *object_)
{
  auto object = m_objCache.delNode(object_);
  object->commit(Op::Put);
  if (object->seqLen() > 1)
    m_deletes.add(object->rn(),
	DeleteOp{object->prevRN(),
	  SeqLenOp::mk(object->seqLen() - 1, Op::Put)});
  write(object->replicate(fbs::Body_Rep));
  object->put(); // resets seqLen to 1
  m_objCache.add(ZuMv(object));
}

// commits appended update()
void DB::append(AnyObject *object_)
{
  auto object = m_objCache.delNode(object_);
  object->commit(Op::Append);
  write(object->replicate(fbs::Body_Rep));
  m_objCache.add(ZuMv(object));
}

// commits delete following push() / update()
void DB::del(AnyObject *object_)
{
  auto object = m_objCache.delNode(object_);
  if (ZuUnlikely(!object->seqLen())) {
    fileRun([this, rn = object->rn()]() { del_write(rn); });
    object->commit(Op::Delete);
    return;
  }
  object->commit(Op::Delete);
  RN rn = object->rn();
  m_deletes.add(rn, DeleteOp{rn, SeqLenOp::mk(object->seqLen(), Op::Delete)});
  write(object->replicate(fbs::Body_Rep));
}

// aborts push() / update()
void DB::abort(AnyObject *object)
{
  object->abort();
}

// prepare replication data for sending & writing to disk
ZmRef<Buf> AnyObject_::replicate(int type)
{
  ZmAssert(committed());
  ZmAssert(m_rn != nullRN());

  ZdbDEBUG(m_db->env(),
      ZtString{} << "AnyObject_::replicate(" << type << ')');
  IOBuilder fbb;
  Zfb::Offset<Zfb::Vector<uint8_t>> data;
  if (op() != Op::Delete && this->ptr_())
    data = Zfb::Save::nest(fbb, [this](Zfb::Builder &fbb) {
      return m_db->save(fbb, this);
    });
  {
    auto id = Zfb::Save::id(m_db->config().id);
    auto msg = fbs::CreateMsg(fbb, static_cast<fbs::Body>(type),
	fbs::CreateRecord(fbb, &id,
	  m_rn, m_prevRN, m_seqLenOp, data).Union());
    fbb.Finish(msg);
  }
  return saveHdr(fbb, m_db);
}

// prepare run-length encoded gap for sending
ZmRef<Buf> DB::repGap(RN rn, uint64_t count)
{
  IOBuilder fbb;
  auto id = Zfb::Save::id(config().id);
  auto msg = fbs::CreateMsg(fbb, fbs::Body_Gap,
      fbs::CreateGap(fbb, &id, rn, count).Union());
  fbb.Finish(msg);
  return saveHdr(fbb, this);
}

void DB::purge(RN minRN)
{
  IOBuilder fbb;
  RN rn = m_nextRN++;
  {
    auto id = Zfb::Save::id(config().id);
    auto msg = fbs::CreateMsg(fbb, fbs::Body_Rep,
	fbs::CreateRecord(fbb, &id,
	  rn, minRN, SeqLenOp::mk(1, Op::Purge), {}).Union());
    fbb.Finish(msg);
  }
  m_deletes.add(rn, DeleteOp{minRN, SeqLenOp::mk(1, Op::Purge)});
  write(saveHdr(fbb, this));
}

template <bool Create>
ZmRef<File> DB::openFile_(const ZiFile::Path &name, uint64_t id)
{
  ZmAssert(fileInvoked());

  ZmRef<File> file = new File{this, id};
  if (file->open(name, ZiFile::GC, 0666) == Zi::OK) {
    if (!file->scan()) return nullptr;
    return file;
  }
  if constexpr (!Create) return nullptr;
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

template <bool Create>
ZmRef<File> DB::openFile(uint64_t id)
{
  ZmAssert(fileInvoked());

  return openFile_<Create>(fileName(dirName(id), id), id);
}

template <bool Create>
ZmRef<File> DB::getFile(uint64_t id)
{
  ZmAssert(fileInvoked());

  return m_files.findSync(id, [this](uint64_t id) -> ZmRef<File> {
    ZmRef<File> file = openFile<Create>(id);
    if (ZuUnlikely(!file)) return nullptr;
    if (id > m_lastFile) m_lastFile = id;
    return file;
  }, [](ZmRef<File> file) { file->sync(); });
}

template <bool Create>
ZmRef<IndexBlk> DB::getIndexBlk(File *file, uint64_t id)
{
  ZmAssert(fileInvoked());

  return m_indexBlks.findSync(id, [file](uint64_t id) -> ZmRef<IndexBlk> {
    if constexpr (Create)
      return file->writeIndexBlk(id);
    else
      return file->readIndexBlk(id);
  }, [this](ZmRef<IndexBlk> indexBlk) {
    auto fileID = (indexBlk->id)>>(fileShift() - indexShift());
    if (ZmRef<File> file = getFile<Create>(fileID))
      file->writeIndexBlk(indexBlk);
  });
}

// creates/caches file and index block as needed
template <bool Write>
FileRec DB::rn2file(RN rn)
{
  ZmAssert(fileInvoked());

  uint64_t fileID = rn>>fileShift();
  File *file = getFile<Write>(fileID);
  if (!file) return {};
  if constexpr (!Write) if (!file->exists(rn & fileRecMask())) return {};
  uint64_t indexBlkID = rn>>indexShift();
  IndexBlk *indexBlk = getIndexBlk<Write>(file, indexBlkID);
  if (!indexBlk) return {};
  auto indexOff = static_cast<unsigned>(rn & indexMask());
  return {file, indexBlk, indexOff};
}

void DB::delFile(File *file)
{
  ZmAssert(fileInvoked());

  bool lastFile;
  uint64_t id = file->id();
  m_files.delNode(file);
  lastFile = id == m_lastFile;
  if (ZuUnlikely(lastFile)) getFile<true>(id + 1);
  file->close();
  ZiFile::remove(fileName(id));
}

ZmRef<IndexBlk> File_::readIndexBlk(uint64_t id)
{
  ZmRef<IndexBlk> indexBlk;
  auto offset = m_superBlk.data[id & indexMask()];
  if (offset) {
    indexBlk = new IndexBlk{id, offset};
    if (!readIndexBlk(indexBlk)) return nullptr;
    return indexBlk;
  }
  return nullptr;
}

ZmRef<IndexBlk> File_::writeIndexBlk(uint64_t id)
{
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

bool File_::readIndexBlk(IndexBlk *indexBlk)
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

bool File_::writeIndexBlk(IndexBlk *indexBlk)
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

// read individual record from disk into buffer
ZmRef<Buf> DB::read(const FileRec &rec)
{
  ZmAssert(fileInvoked());

  const auto &index = rec.index();
  if (ZuUnlikely(!index.offset || index.offset == deleted())) {
    ZeLOG(Error, ZtString{} << "Zdb internal error on DB " << config().id <<
	" bitmap inconsistent with index for RN " << rec.rn());
    return nullptr;
  }
  IOBuilder fbb;
  Zfb::Offset<Zfb::Vector<uint8_t>> data;
  uint8_t *ptr = nullptr;
  if (index.length) {
    data = Zfb::Save::pvector_(fbb, index.length, ptr);
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
bool DB::write2(Buf *buf)
{
  ZmAssert(fileInvoked());

  ZuGuard guard([this, buf]() { m_repBufs->delNode(buf); });

  auto record = record_(msg_(buf->hdr()));
  RN rn = record->rn();
  auto data = Zfb::Load::bytes(record->data());
  {
    FileRec rec = rn2file<true>(rn);
    if (!rec) return false;
    auto &index = rec.index();
    File *file = rec.file();
    file->alloc(rn & fileRecMask());
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
	return false;
      }
    }
    // write trailer
    if (ZuUnlikely((r = file->pwrite(index.offset + index.length,
	      &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
      fileWrError_(file, index.offset + index.length, e);
      index.offset = 0;
      index.length = 0;
      return false;
    }
  }
  return true;
}

void DB::ack(RN rn)
{
  ZmAssert(fileInvoked());

  if (ZuUnlikely(rn <= m_minRN.load_())) return;
  if (ZuUnlikely(rn > m_nextRN.load_())) rn = m_nextRN;
  if (m_vacuumRN != nullRN()) {
    if (rn > m_vacuumRN) m_vacuumRN = rn;
    return;
  }
  auto startRN = m_deletes.minimumKey();
  if (startRN == nullRN() || rn < startRN) return;
  m_vacuumRN = rn;
  vacuum();
}

void DB::vacuum()
{
  ZmAssert(fileInvoked());

  auto i = m_deletes.iterator();
  unsigned j = 0;
  unsigned n = config().vacuumBatch;
  ZuPair<int, RN> outcome;
  while (auto node = i.iterate()) {
    if (node->key() >= m_vacuumRN) break;
    outcome = del_(node->val(), n - j);
    if (outcome.p<0>() < 0) goto again1;
    i.del(node);
    if ((j += outcome.p<0>()) >= n) goto again2;
  }
  m_vacuumRN = nullRN();
  return;

again1:
  if (outcome.p<1>() != nullRN()) // split long sequence
    m_deletes.add(outcome.p<1>(),
	DeleteOp{outcome.p<1>(), SeqLenOp::mk(-outcome.p<0>(), Op::Delete)});

again2:
  fileRun([this]() { vacuum(); });
}

// del_() returns a {int, RN} pair:
// 0, nullRN() - nothing to do
// +ve, nullRN() - all done (work was within batch size), continue
// -ve, nullRN() - work exceeded batch size, some work done, re-attempt
// -ve, rn - work exceeded batch size, need to split sequence at rn
//   (remaining sequence length is encoded in the negative return code)
ZuPair<int, RN> DB::del_(const DeleteOp &deleteOp, unsigned maxBatchSize)
{
  ZmAssert(fileInvoked());

  RN rn = deleteOp.rn;

  switch (SeqLenOp::op(deleteOp.seqLenOp)) {
    default:
      return {0, nullRN()};
    case Op::Put:
      break;
    case Op::Delete:
      break;
    case Op::Purge: {
      RN minRN = m_minRN.load_();
      unsigned i = 0;
      if (minRN < rn) {
	do {
	  del_write(minRN++);
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

  auto batch = ZmAlloc(RN, batchSize);
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
  for (unsigned j = i; j-- > 0; ) del_write(batch[j]);

  // entire sequence was deleted
  return {static_cast<int>(i), nullRN()};
}

// obtain prevRN for a record pending deletion
RN DB::del_prevRN(RN rn)
{
  ZmAssert(fileInvoked());

  if (auto object = m_objCache.del(rn)) {
    if (object->committed()) return object->prevRN();
  }
  FileRec rec = rn2file<false>(rn);
  if (!rec) return nullRN();
  const auto &index = rec.index();
  if (ZuUnlikely(!index.offset || index.offset == deleted())) {
    ZeLOG(Error, ZtString{} << "Zdb internal error on DB " << config().id <<
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
void DB::del_write(RN rn)
{
  ZmAssert(fileInvoked());

  FileRec rec = rn2file<false>(rn);
  if (!rec) return;
  rec.index().offset = deleted();
  if (rec.file()->del(rn & fileRecMask()))
    delFile(rec.file());
}

// disk read error
void DB::fileRdError_(File *file, ZiFile::Offset off, int r, ZeError e)
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
