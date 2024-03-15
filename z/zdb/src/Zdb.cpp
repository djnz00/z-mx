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
//   first-ranked is leader
//   second-ranked is leader's next
//   third-ranked is second-ranked's next
//   etc.

// a new next is selected and recovery/replication restarts when
// * an election ends
// * a new host heartbeats for first time after election completes
// * an existing host disconnects

// a new leader is selected (the local instance may activate/deactivate) when:
// * an election ends
// * a new host heartbeats for first time after election completes
//   - possible deactivation of local instance only -
//   - if self is leader and the new host < this one, we just heartbeat it
// * an existing host disconnects (if that is leader, a new election begins)

// if replicating from primary to DR and a down secondary comes back up,
// then primary's m_next will be DR and DR's m_next will be secondary

// if leader and not replicating, then no host is a replica, so leader runs
// as standalone until peers have recovered

#include <zlib/Zdb.hpp>

#include <zlib/ZtBitWindow.hpp>

#include <zlib/ZiDir.hpp>
#include <zlib/ZiModule.hpp>

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
      throw ZeEVENT(Fatal, ([thread = config.thread](auto &s) {
	s << "ZdbEnv thread misconfigured: " << thread; }));

    {
      ZiModule module_;
      auto path = config.storeCf->get<true>("module");
      auto preload = config.storeCf->getBool("preload", false);
      ZtString e; // dlerror() returns a string
      if (module_.load(path, preload ? ZiModule::Pre : 0, &e) < 0)
	throw ZeEVENT(Fatal, ([path = ZtString{path}, e](auto &s) {
	  s << "failed to load \"" << path << "\": " << e; }));
      ZdbStoreFn storeFn =
	reinterpret_cast<ZdbStoreFn>(module_.resolve(ZdbStoreFnSym, &e));
      if (!storeFn) {
	module_.unload();
	throw ZeEVENT(Fatal, ([path = ZtString{path}, e](auto &s) {
	  s << "failed to resolve \"" ZdbStoreFnSym "\" in \""
	    << path << "\": " << e; }));
      }
      m_store = (*storeFn)();
      auto result = m_store->init(
	  config.storeCf, [](Event error) { ZeLogEvent(ZuMv(error)); });
      if (result.contains<Event>()) throw ZuMv(result).v<Event>();
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
	    throw ZeEVENT(Fatal,
		([id = dbCf.id, thread = dbCf.thread](auto &s) {
		  s << "Zdb " << id
		    << " thread misconfigured: " << thread; }));
	}
      }
    }

    m_cf = ZuMv(config);
    m_mx = mx;

    m_handler = ZuMv(handler);

    return true;
  }))
    throw ZeEVENT(Fatal, "ZdbEnv::init called out of order");
}

ZmRef<DB> Env::initDB_(ZuID id, DBHandler handler)
{
  ZmRef<DB> db;
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
    throw ZeEVENT(Fatal, "ZdbEnv::initDB called out of order");
  return db;
}

void Env::opened(DB *db, RN rn, UN un, SN sn)
{
  db->recoveredRN(rn);
  db->recoveredUN(un);
  recoveredSN(sn);
}

void Env::final()
{
  if (!ZmEngine<Env>::lock(ZmEngineState::Stopped, [this]() {
    if (state() != HostState::Instantiated) return false;
    all_([](DB *db) { db->final(); });
    m_handler = {};
    return true;
  }))
    throw ZeEVENT(Fatal, "ZdbEnv::final called out of order");
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
    allSync([this, &ok](DB *db) { return [this, &ok, db]() {
      if (ZuLikely(ok.load_())) ok &= db->open(m_store);
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
  while (auto cxn = i.iterate())
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
  ZeLOG(Info, ([ip = m_self->ip(), port = m_self->port()](auto &s) {
    s << "Zdb listening on (" << ip << ':' << port << ')';
  }));
}

void Env::listenFailed(bool transient)
{
  bool retry = transient && running();
  if (retry) run([this]() { listen(); }, ZmTimeNow((int)m_cf.reconnectFreq));
  ZeLOG(Warning, ([ip = m_self->ip(), port = m_self->port(), retry](auto &s) {
    s << "Zdb listen failed on (" << ip << ':' << port << ')';
    if (retry) s << " - retrying...";
  }));
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

  if (won = m_leader == m_self) {
    m_appActive = true;
    m_prev = nullptr;
    if (!m_nPeers)
      ZeLOG(Warning, "Zdb activating standalone");
    else
      hbSend_(); // announce new leader
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
    ZeLOG(Info, ([cmd](auto &s) { s << "Zdb invoking \"" << cmd << '\"'; }));
    ::system(cmd);
  }
  m_handler.upFn(this, oldMaster);
}

void Env::down_()
{
  ZeLOG(Info, "Zdb INACTIVE");
  if (ZtString cmd = m_self->config().down) {
    ZeLOG(Info, ([cmd](auto &s) { s << "Zdb invoking \"" << cmd << '\"'; }));
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
  Zfb::Offset<String> thread;
  if (!update) {
    thread = str(fbb_, m_cf.thread);
  }
  ZvTelemetry::fbs::ZdbEnvBuilder fbb{fbb_};
  if (!update) {
    fbb.add_thread(thread);
    { auto v = id(m_self->id()); fbb.add_self(&v); }
  }
  { auto v = id(m_leader ? m_leader->id() : ZuID{}); fbb.add_leader(&v); }
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

  ZeLOG(Info,
      ([id = this->id(), ip = config().ip, port = config().port](auto &s) {
	s << "Zdb connecting to host " << id <<
	  " (" << ip << ':' << port << ')';
      }));

  m_mx->connect(
      ZiConnectFn::Member<&Host::connected>::fn(this),
      ZiFailFn::Member<&Host::connectFailed>::fn(this),
      ZiIP{}, 0, config().ip, config().port);
}

void Host::connectFailed(bool transient)
{
  bool retry = transient && m_env->running();
  if (retry) reconnect();
  ZeLOG(Warning,
      ([id = this->id(),
	ip = config().ip,
	port = config().port,
	retry](auto &s) {
    s << "Zdb failed to connect to host " << id <<
      " (" << ip << ':' << port << ')';
    if (retry) s << " - retrying...";
  }));
}

ZiConnection *Host::connected(const ZiCxnInfo &ci)
{
  ZeLOG(Info,
      ([id = this->id(),
	remoteIP = ci.remoteIP, remotePort = ci.remotePort,
	localIP = ci.localIP, localPort = ci.localPort](auto &s) {
    s << "Zdb connected to host " << id << " (" <<
      remoteIP << ':' << remotePort << "): " <<
      localIP << ':' << localPort;
  }));

  if (!m_env->running()) return nullptr;

  return new Cxn{m_env, this, ci};
}

ZiConnection *Env::accepted(const ZiCxnInfo &ci)
{
  ZeLOG(Info,
      ([remoteIP = ci.remoteIP, remotePort = ci.remotePort,
	localIP = ci.localIP, localPort = ci.localPort](auto &s) {
    s << "Zdb accepted cxn on (" <<
      remoteIP << ':' << remotePort << "): " <<
      localIP << ':' << localPort;
  }));

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
    ZeLOG(Error, ([hostID](auto &s) { s <<
	"Zdb cannot associate incoming cxn: host ID " << hostID <<
	" not found"; }));
    return;
  }

  if (host == m_self) {
    ZeLOG(Error, ([hostID](auto &s) { s <<
	"Zdb cannot associate incoming cxn: host ID " << hostID <<
	" is same as self"; }));
    return;
  }

  if (cxn->host() == host) return;

  associate(cxn, host);
}

void Env::associate(Cxn *cxn, Host *host)
{
  ZmAssert(invoked());

  ZeLOG(Info, ([hostID = host->id()](auto &s) {
    s << "Zdb host " << hostID << " CONNECTED";
  }));

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
  ZeLOG(Info,
      ([id = m_host ? m_host->id() : ZuID{"unknown"},
	ip = info().remoteIP, port = info().remotePort](auto &s) {
    s << "Zdb heartbeat timeout on host " <<
      id << " (" << ip << ':' << port << ')';
  }));

  disconnect();
}

void Cxn_::disconnected()
{
  ZeLOG(Info,
      ([id = m_host ? m_host->id() : ZuID{"unknown"},
	ip = info().remoteIP, port = info().remotePort](auto &s) {
    s << "Zdb disconnected from host " <<
      id << " (" << ip << ':' << port << ')';
  }));

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

  ZeLOG(Info, ([id = host->id()](auto &s) {
    s << "Zdb host " << id << " DISCONNECTED";
  }));

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

  if (host == m_leader) {
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

  Host *oldMaster = m_leader;

  envStateRefresh();

  m_leader = nullptr;
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
	  " leader=" << ZuPrintPtr{m_leader});

      if (host->voted()) {
	if (host != m_self) ++m_nPeers;
	if (!m_leader) { m_leader = host; continue; }
	int diff = host->cmp(m_leader);
	if (ZuCmp<int>::null(diff)) {
	  m_leader = nullptr;
	  break;
	} else if (diff > 0)
	  m_leader = host;
      }
    }
  }

  if (m_leader)
    ZeLOG(Info, ([id = m_leader->id()](auto &s) {
      s << "Zdb host " << id << " is leader";
    }));
  else
    ZeLOG(Error, "Zdb leader election failed - hosts inconsistent");

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
    // all([](DB *db) { return [db]() { db->ack(maxRN()); }; });
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
	" leader=" << ZuPrintPtr{m_leader} << '\n' <<
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

  ZeLOG(Info, ([id = m_next->id()](auto &s) {
    s << "Zdb host " << id << " is next in line";
  }));

  envStateRefresh();

  ZdbDEBUG(this, ZtString{} << "repStart()\n" <<
      " self=" << ZuPrintPtr{m_self} << '\n' <<
      " leader=" << ZuPrintPtr{m_leader} << '\n' <<
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
	      un = state->p<1>(), endUN = endState->p<1>()]() mutable {
	    db->recSend(ZuMv(cxn), un, endUN);
	  });
	}
    }
  }
}

void DB::recSend(ZmRef<Cxn> cxn, UN un, UN endUN)
{
  ZmAssert(invoked());

  if (!cxn->up()) return;

  if (auto buf = repBuf(un))
    recSend_(ZuMv(cxn), un, endUN, ZuMv(buf));
  else
    recSendGet(ZuMv(cxn), un, endUN);
}

void DB::recSendGet(ZmRef<Cxn> cxn, UN un, UN endUN)
{
  ZmAssert(invoked());

  if (!cxn->up()) return;

  using namespace Table_;

  m_table->recover(un,
      [cxn = ZuMv(cxn), endUN](DB *db, UN un, GetResult result) mutable {
    if (ZuLikely(result.contains<RecoverData>())) {
      const auto &data = result.v<RecoverData>();
      if (ZmRef<AnyObject> object =
	m_handler.importFn(this, data.import_)) {
	object->init(data.rn, un, data.sn, data.vn);
	run([this, cxn = ZuMv(cxn), un, endUN, o = ZuMv(o)]() mutable {
	  if (auto buf = object->replicate(fbs::Body_Rec))
	    recSend_(ZuMv(cxn), un, endUN, ZuMv(buf));
	  else
	    recNext(ZuMv(cxn), un, endUN);
	});
      }
      return;
    }
    if (ZuUnlikely(result.contains<Event>())) {
      ZeLogEvent(ZuMv(result).v<Event>());
      ZeLOG(Error, ([id = db->id(), un](auto &s) {
	s << "Zdb recovery of " << db->id() << '/' << un << " failed";
      }));
    }
    run([this, cxn = ZuMv(cxn), un, endUN]() mutable {
      recNext(ZuMv(cxn), un, endUN);
    });
  });
}

void DB::recSend_(ZmRef<Cxn> cxn, UN un, UN endUN, ZmRef<Buf> buf)
{
  cxn->send(ZuMv(buf));
  recNext(ZuMv(cxn), un, endUN);
}

void DB::recNext(ZmRef<Cxn> cxn, UN un, UN endUN)
  if (++un < endUN)
    run([this, cxn = ZuMv(cxn), un, endUN]() mutable {
      recSend(ZuMv(cxn), un, endUN);
    });
  else
    m_env->invoke([env = m_env]() { env->recEnd(); });
}

void Env::recEnd()
{
  if (m_recovering) --m_recovering;
}

// prepare replication buffer for sending
ZmRef<Buf> DB::repBuf(UN un)
{
  ZmAssert(invoked());

  // recover from outbound replication buffer cache
  if (auto buf = findBufUN(un)) {
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
    auto msg = fbs::CreateMsg(fbb, fbs::Body_Rec,
	fbs::CreateRecord(fbb, record->id(), record->rn(), record->un(),
	  record->sn(), record->vn(), data).Union());
    fbb.Finish(msg);
    return saveHdr(fbb, this);
  }
  // recover from object cache (without falling through to reading from disk)
  if (auto object = m_updCache.findval(un))
    return object->replicate(fbs::Body_Rec);
  return nullptr;
}

// prepare replication data
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
	  m_un, m_rn, m_prevRN, m_seqLenOp, data).Union());
    fbb.Finish(msg);
  }
  return saveHdr(fbb, m_db);
}

void Env::repStop()
{
  ZmAssert(invoked());

  m_leader = nullptr;
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
    [](const ZiIOContext &, const Buf *buf) -> int {
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
	" leader=" << ZuPrintPtr{m_leader} << '\n' <<
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

  // check for duplicate leader (dual active)
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
#if 0
    if (auto db = m_dbs.findPtr(id))
      db->invoke([db, rn]() { db->ack(rn); });
#endif
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
  while (auto cxn = i.iterate()) cxn->hbSend();
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
      " UN:" << self->un() <<
      " N:" << self->envState().count_() << "] " << self->envState());
}

// refresh db state vector
void Env::envStateRefresh()
{
  ZmAssert(invoked());

  EnvState &envState = m_self->envState();
  envState.updateUN(m_un);
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
  m_env->replicated(m_host, id, record->un(), record->sn());
  db->invoke([buf = ZuMv(buf)]() mutable {
    auto db = buf->db();
    db->repRecRcvd(ZuMv(buf));
  });
}

void Env::replicated(Host *host, ZuID dbID, UN un, SN sn)
{
  ZmAssert(invoked());

  bool updated =
    host->envState().updateSN(sn + 1) || host->envState().update(dbID, un + 1);
  if ((active() || host == m_next) && !updated) return;
  if (!m_prev) {
    m_prev = host;
    ZeLOG(Info, ([id = m_prev->id()](auto &s) {
      s << "Zdb host " << id << " is previous in line";
    }));
  }
}

DB::DB(Env *env, DBCf *cf) :
  m_env{env}, m_mx{env->mx()}, m_cf{cf},
  m_path{ZiFile::append(env->config().path, cf->id)},
  m_updCache{new UpdCache{}},
  m_repBufs{new RepBufs{}},
  m_updBufs{new UpdBufs{}}
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
  Zfb::Offset<String> path, name, thread;
  if (!update) {
    path = str(fbb_, m_path);
    name = str(fbb_, config().id);
    thread = str(fbb_, config().thread);
  }
  ZvTelemetry::fbs::ZdbBuilder fbb{fbb_};
  if (!update) {
    fbb.add_path(path);
    fbb.add_name(name);
    fbb.add_thread(thread);
  }
  fbb.add_minRN(m_minRN.load_());
  fbb.add_nextRN(m_nextRN.load_());
  {
    Cache::Stats stats;
    m_cache.stats(stats);
    fbb.add_cacheLoads(stats.loads);
    fbb.add_cacheMisses(stats.misses);
    if (!update) fbb.add_cacheSize(stats.size);
  }
  if (!update) {
    fbb.add_cacheMode(
	static_cast<ZvTelemetry::fbs::ZdbCacheMode>(config().cacheMode));
    fbb.add_warmup(config().warmup);
  }
  return fbb.Finish();
}

// load object from buffer, bypassing cache
ZmRef<AnyObject> DB::load_(const fbs::Record *record)
{
  auto data = Zfb::Load::bytes(record->data());
  if (!data) return nullptr;
  ZmRef<AnyObject> object;
  if (auto fn = m_handler.loadFn)
    object = fn(this, data.data(), data.length());
  if (object)
    object->init(record->rn(), record->un(), record->sn(), record->vn());
  return object;
}

// load object from buffer, updating cache
ZmRef<AnyObject> DB::load(const fbs::Record *record)
{
  auto data = Zfb::Load::bytes(record->data());
  if (!data) {
    m_cache.del(record->rn());
    return nullptr;
  }
  ZmRef<AnyObject> object;
  if (object = m_cache.find(record->rn())) {
    if (auto fn = m_handler.updateFn)
      object = fn(object, data.data(), data.length());
  } else {
    if (auto fn = m_handler.loadFn)
      object = fn(this, data.data(), data.length());
  }
  if (object)
    object->init(record->rn(), record->un(), record->sn(), record->vn());
  return object;
}

// save object to buffer
Zfb::Offset<void> DB::save(Zfb::Builder &fbb, AnyObject_ *object)
{
  return m_handler.saveFn(fbb, object->ptr_());
}

// recover record originating from inbound replication or data store recovery
void DB::recover(const fbs::Record *record)
{
  ZmAssert(invoked());

  if (auto object = load(record)) {
    if (auto fn = m_handler.scanFn) fn(object);
  } else {
    if (auto fn = m_handler.deleteFn) fn(record->rn());
  }
}

// process inbound replication - record
void DB::repRecRcvd(ZmRef<Buf> buf)
{
  ZmAssert(invoked());

  recover(record_(msg_(buf->hdr())));
  write(ZuMv(buf));
}

// low-level internal write to data store - l(DB *, RN, UN, bool ok)
template <typename L>
void DB::write_(ZmRef<Buf> buf, L l) {
  // FIXME - if running as a secondary, elide this (it will conflict with
  // the primary) 
  //
  // BUT we will need to modify replication:
  // 1] primary sends a followup once the update is committed to the
  //    data store
  // 2] secondaries remove the update from the repBufs only when the
  //    primary confirms the commit
  // 3] when secondaries activate, they begin write_() on all pending repBufs
  //
  auto record = record_(msg_(buf->hdr()));
  auto object = load_(record);
  auto commitFn = [buf = ZuMv(buf), l = ZuMv(l)](
      DB *db, RN rn, UN un, CommitResult result) {
    auto ptr = buf.ptr();
    m_updBufs->del(VBuf_UNAxor(buf)); // FIXME - this and the next line are run on inbound replication of commit confirm from the primary by the secondaries
    m_repBufs->delNode(ptr);
    bool ok = true;
    if (ZuUnlikely(result.contains<Event>())) {
      ZeLogEvent(ZuMv(result).v<Event>());
      ok = false;
    }
    invoke([l = ZuMv(l), db, rn, un, ok]() { l(db, rn, un, ok); });
  };
  if (!object)
    m_table->del(record->rn(), record->un(), record->sn(), record->vn(),
	commitFn);
  else if (object->vn())
    m_table->update(object->rn(), object->un(), object->sn(), object->vn(),
	m_handler.exportFn, commitFn);
  else
    m_table->push(object->rn(), object->un(), object->sn(),
	m_handler.exportFn, commitFn);
}

// outbound replication + persistency
void DB::write(ZmRef<Buf> buf)
{
  ZmAssert(invoked());
  ZmAssert(buf->db() == this);

  m_repBufs->addNode(buf);
  m_updBufs->add(buf);
  if (config().repMode) { // replicate then store (faster)
    m_env->invoke([buf = ZuMv(buf)]() mutable {
      auto db = buf->db();
      auto env = db->env();
      if (env->replicate(buf))
	// replicated - peer will ack
	write_(ZuMv(buf), [](DB *, RN, UN, bool){});
      else
	// standalone - ack on successful data store
	write_(ZuMv(buf), [](DB *db, RN, UN un, bool) { /* db->ack(un + 1) */ });
    });
  } else { // store then replicate (slower but potentially more durable)
    write_(buf, [](DB *db, RN rn, UN un, bool ok) {
      if (ok)
	env->invoke([buf = ZuMv(buf)]() mutable {
	  auto db = buf->db();
	  auto env = db->env();
	  if (!env->replicate(ZuMv(buf))) { // standalone - ack write
	    auto un = record_(msg_(buf->hdr()))->un();
	    // db->invoke([db, un]() { db->ack(un + 1); });
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

// Env::open()
// - iterates over DBs, calling open(store)
// - each DB calls env->opened(this, rn, un) on success

bool DB::open(Store *store)
{
  ZmAssert(invoked());

  if (m_open) return true;

  Store::OpenResult result;
  ZmBlock<>{}([this, &result](auto wake) {
    Store::ScanFn scanFn;
    if (m_handler.scanFn)
      scanFn = [](
	  DB *db, RN rn, UN un, SN sn, VN vn, const ZtField::Import &import_) {
	auto object = db->m_handler.importFn(this, import_);
	object->init(rn, un, sn, vn);
	db->m_handler.scanFn(object);
      };
    store->open(this, id(), fields(),
	[&result, wake = ZuMv(wake)](Store::OpenResult result_) {
      result = ZuMv(result_);
      wake();
    }, scanFn);
  });
  if (!result.contains<Store::OpenData>()) return false;
  const auto &data = result.v<Store::OpenData>();
  m_table = data.table;
  m_env->opened(this, data.rn, data.un, data.sn);

  if (config().warmup) {
    if (auto fn = m_handler.ctorFn)
      run([this, fn]() { ZmRef<AnyObject>{fn(this)}; });
  }

  m_open = true;
  return true;
}

void DB::close()
{
  ZmAssert(invoked());

  if (!m_open) return;

  ZmBlock<>{}([this](auto wake) {
    m_table->close([wake = ZuMv(wake)]() { wake(); });
  });

  m_open = false;
  m_table = nullptr;
}

ZmRef<AnyObject> DB::placeholder()
{
  if (auto fn = m_handler.ctorFn) return fn(this);
  return nullptr;
}

bool AnyObject_::push_()
{
  if (m_state != ObjState::Undefined) return false;
  m_state = ObjState::Push;
  m_rn = m_db->nextRN();
  m_un = m_db->nextUN();
  return true;
}
bool AnyObject_::update_()
{
  if (m_state != ObjState::Committed) return false;
  m_state = ObjState::Update;
  m_origUN = m_un;
  m_un = m_db->nextUN();
  return true;
}
bool AnyObject_::del_()
{
  if (m_state != ObjState::Committed) return false;
  m_state = ObjState::Delete;
  m_origUN = m_un;
  m_un = m_db->nextUN();
  return true;
}

bool AnyObject_::put_()
{
  switch (m_state) {
    default: return false;
    case ObjState::Push:
      if (ZuUnlikely(!m_db->allocRN(m_rn))) { abort_(); return false; }
      break;
    case ObjState::Update: break;
    case ObjState::Delete: break;
  }
  if (ZuUnlikely(!m_db->allocUN(m_un))) { abort_(); return false; }
  m_sn = m_db->env()->allocSN();
  switch (m_state) {
    case ObjState::Push:
      m_state = ObjState::Committed;
      break;
    case ObjState::Update:
      m_state = ObjState::Committed;
      ++m_vn;
      break;
    case ObjState::Delete:
      m_state = ObjState::Deleted;
      ++m_vn;
      break;
  }
  return true;
}

bool AnyObject_::abort_()
{
  switch (m_state) {
    default: return false;
    case ObjState::Push:
      m_state = ObjState::Undefined;
      m_rn = nullRN();
      m_un = nullUN();
      break;
    case ObjState::Update:
    case ObjState::Delete:
      m_state = ObjState::Committed;
      m_un = m_origUN;
      break;
  }
  return true;
}

ZmRef<AnyObject> DB::push_();
{
  auto fn = m_handler.ctorFn;
  if (ZuUnlikely(!fn)) return nullptr;
  ZmRef<AnyObject> object = fn(this);
  if (!object) return nullptr;
  if (!object->push_()) return nullptr;
  return object;
}

bool DB::update_(AnyObject *object, UN un)
{
  if (!object->update_(un)) return false;
  return true;
}

bool DB::del_(AnyObject *object, UN un)
{
  if (!object->del_(un)) return false;
  return true;
}

// commits push/update/del
bool DB::put(ZmRef<AnyObject> object)
{
  UN origUN = object->origUN();
  int origState = object->state();
  if (!object->put_()) return false;
  write(object->replicate(fbs::Body_Rep));
  switch (origState) {
    case ObjState::Push:
      m_cache.add(object);
      m_updCache->add(object->un(), object);
      break;
    case ObjState::Update:
      m_updCache->add(object->un(), object);
      m_updCache->del(origUN);
      break;
    case ObjState::Delete:
      m_cache.delNode(object);
      m_updCache->del(origUN);
      break;
  }
}

// aborts push() or update()
void DB::abort(ZmRef<AnyObject> object)
{
  if (!object->abort_()) return;
  // FIXME?
}

#if 0
// ack UN
void DB::ack(UN un)
{
  ZmAssert(invoked());

  if (ZuUnlikely(un > m_nextUN)) un = m_nextUN;
}
#endif

} // namespace Zdb_
