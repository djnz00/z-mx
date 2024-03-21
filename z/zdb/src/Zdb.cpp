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

// FIXME - replace all ZmAssert with ZeAssert

#include <zlib/Zdb.hpp>

#include <zlib/ZtBitWindow.hpp>

#include <zlib/ZiDir.hpp>
#include <zlib/ZiModule.hpp>

#include <assert.h>
#include <errno.h>

namespace Zdb_ {

void Env::init(
    EnvCf config,
    ZiMultiplex *mx,
    EnvHandler handler,
    Store *store)
{
  if (!ZmEngine<Env>::lock(ZmEngineState::Stopped,
	[this, &config, mx, &handler, store]() mutable {
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
    {
      if (store)
	m_store = store;
      else {
	if (!m_cf.storeCf)
	  throw ZeEVENT(Fatal, ([](auto &s) {
	    s << "no data store configured"; }));
	ZiModule module_;
	auto path = m_cf.storeCf->get<true>("module");
	auto preload = m_cf.storeCf->getBool("preload", false);
	ZtString e; // dlerror() returns a string
	if (module_.load(path, preload ? ZiModule::Pre : 0, &e) < 0)
	  throw ZeEVENT(Fatal, ([path = ZtString{path}, e](auto &s) {
	    s << "failed to load \"" << path << "\": " << e; }));
	auto storeFn =
	  reinterpret_cast<StoreFn>(module_.resolve(ZdbStoreFnSym, &e));
	if (!storeFn) {
	  module_.unload();
	  throw ZeEVENT(Fatal, ([path = ZtString{path}, e](auto &s) {
	    s << "failed to resolve \"" ZdbStoreFnSym "\" in \""
	      << path << "\": " << e; }));
	}
	m_store = (*storeFn)();
      }
      if (!m_store)
	throw ZeEVENT(Fatal, ([](auto &s) { s << "null data store"; }));
      Store_::InitResult result = m_store->init(
	  m_cf.storeCf, [](Event error) { ZeLogEvent(ZuMv(error)); });
      if (result.contains<Event>()) throw ZuMv(result).v<Event>();
      m_repStore = result.v<Store_::InitData>().replicated;
    }

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
    if (!m_self)
      throw ZeEVENT(Fatal, ([id = m_cf.hostID](auto &s) {
	s << "Zdb own host ID " << id << " not in hosts table"; }));
    m_self->state(HostState::Initialized);

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

void Env::final()
{
  if (!ZmEngine<Env>::lock(ZmEngineState::Stopped, [this]() {
    if (state() != HostState::Initialized) return false;
    all_([](DB *db) { db->final(); });
    m_handler = {};
    if (m_store) {
      m_store->final();
      m_store = nullptr;
    }
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
    ZeLOG(Fatal, "Env::start_ called out of order");
    started(false);
    return;
  }

  ZeLOG(Info, "Zdb starting");

  // open and recover all databases
  {
    auto i = m_cf.dbCfs.readIterator();
    while (auto cf = i.iterate())
      if (!m_dbs.findPtr(cf->key()))
	m_dbs.addNode(new DBs::Node{this, &(cf->val())});
  }
  {
    ZmAtomic<unsigned> ok = true;
    auto i = m_dbs.readIterator();
    ZmBlock<>{}(m_dbs.count_(), [this, &ok, &i](unsigned, auto wake) {
      if (auto db = i.iterate())
	db->invoke([this, db, &ok, wake = ZuMv(wake)]() mutable {
	  db->open(m_store,
	      [db, &ok, wake = ZuMv(wake)](Store_::OpenResult result) mutable {
	    ok &= db->opened(ZuMv(result));
	    wake();
	  });
	});
    });
    if (!ok) {
      allSync([](DB *db) { return [db]() { db->close(); }; });
      started(false);
      return;
    }
  }

  // refresh db state vector, begin election
  envStateRefresh();
  repStop();
  m_self->state(Electing);

  if (!(m_nPeers = m_hosts->count_() - 1)) { // standalone
    holdElection();
    return;
  }

  run([this]() { hbSend(); },
      m_hbSendTime = ZmTimeNow(), &m_hbSendTimer);
  run([this]() { holdElection(); },
      ZmTimeNow(static_cast<int>(m_cf.electionTimeout)), &m_electTimer);

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

  state(HostState::Stopping);
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

  state(HostState::Initialized);

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
	s << "Zdb connecting to host " << id
	  << " (" << ip << ':' << port << ')';
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
    s << "Zdb failed to connect to host " << id
      << " (" << ip << ':' << port << ')';
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
    s << "Zdb heartbeat timeout on host "
      << id << " (" << ip << ':' << port << ')';
  }));

  disconnect();
}

void Cxn_::disconnected()
{
  ZeLOG(Info,
      ([id = m_host ? m_host->id() : ZuID{"unknown"},
	ip = info().remoteIP, port = info().remotePort](auto &s) {
    s << "Zdb disconnected from host "
      << id << " (" << ip << ':' << port << ')';
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

      if (next) {
	ZdbDEBUG(this, ZtString{} <<
	    " host=" << ZuPrintPtr{host} << '\n' <<
	    " next=" << *next);
      } else {
	ZdbDEBUG(this, ZtString{} <<
	    " host=" << ZuPrintPtr{host} << '\n' <<
	    " next=(null)");
      }
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

  if (m_self->envState().cmp(m_next->envState()) < 0 ||
      m_recovering ||			// already recovering
      m_repStore)			// back-end data store is replicated
    return;

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
	  auto un = state->p<1>();
	  auto endUN = endState->p<1>();
	  if (endUN <= un) continue;
	  db->run([db, cxn, un, endUN]() mutable {
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
      [cxn = ZuMv(cxn), endUN](DB *db, UN un, RecoverResult result) mutable {
    if (ZuLikely(result.contains<RecoverData>())) {
      const auto &data = result.v<RecoverData>();
      if (ZmRef<AnyObject> object =
	db->m_handler.importFn(db, data.import_)) {
	object->init(data.rn, un, data.sn, data.vn);
	db->run(
	    [db, cxn = ZuMv(cxn), un, endUN, object = ZuMv(object)]() mutable {
	  if (auto buf = object->replicate(fbs::Body_Recovery))
	    db->recSend_(ZuMv(cxn), un, endUN, ZuMv(buf));
	  else
	    db->recNext(ZuMv(cxn), un, endUN);
	});
      }
      return;
    }
    if (ZuUnlikely(result.contains<Event>())) {
      ZeLogEvent(ZuMv(result).v<Event>());
      ZeLOG(Error, ([id = db->id(), un](auto &s) {
	s << "Zdb recovery of " << id << '/' << un << " failed";
      }));
    }
    db->run([db, cxn = ZuMv(cxn), un, endUN]() mutable {
      db->recNext(ZuMv(cxn), un, endUN);
    });
  });
}

void DB::recSend_(ZmRef<Cxn> cxn, UN un, UN endUN, ZmRef<Buf> buf)
{
  cxn->send(ZuMv(buf));
  recNext(ZuMv(cxn), un, endUN);
}

void DB::recNext(ZmRef<Cxn> cxn, UN un, UN endUN)
{
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
    auto msg = fbs::CreateMsg(fbb, fbs::Body_Recovery,
	fbs::CreateRecord(fbb, record->db(), record->rn(), record->un(),
	  record->sn(), record->vn(), data).Union());
    fbb.Finish(msg);
    return saveHdr(fbb, this);
  }
  // recover from object cache (without falling through to reading from disk)
  if (auto object = m_cacheUN->findVal(un))
    return object->replicate(fbs::Body_Recovery);
  return nullptr;
}

// send commit to replica
void DB::repSendCommit(UN un)
{
  IOBuilder fbb;
  {
    auto id = Zfb::Save::id(config().id);
    auto msg = fbs::CreateMsg(fbb, fbs::Body_Commit,
	fbs::CreateCommit(fbb, &id, un).Union());
    fbb.Finish(msg);
  }
  m_env->replicate(saveHdr(fbb, this));
}

// prepare replication data
ZmRef<Buf> AnyObject_::replicate(int type)
{
  ZmAssert(state() == ObjState::Committed || state() == ObjState::Deleted);
  ZmAssert(m_rn != nullRN());

  ZdbDEBUG(m_db->env(), ZtString{} << "AnyObject_::replicate(" << type << ')');
  IOBuilder fbb;
  Zfb::Offset<Zfb::Vector<uint8_t>> data;
  if (state() != ObjState::Deleted && this->ptr_())
    data = Zfb::Save::nest(fbb, [this](Zfb::Builder &fbb) {
      return m_db->save(fbb, this);
    });
  {
    auto id = Zfb::Save::id(m_db->config().id);
    auto sn = Zfb::Save::uint128(m_sn);
    auto msg = fbs::CreateMsg(fbb, static_cast<fbs::Body>(type),
	fbs::CreateRecord(fbb, &id, m_rn, m_un, &sn, m_vn, data).Union());
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
      case fbs::Body_Heartbeat:
      case fbs::Body_Replication:
      case fbs::Body_Recovery:
      case fbs::Body_Commit:
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
    case fbs::Body_Heartbeat:
      hbRcvd(hb_(msg));
      break;
    case fbs::Body_Replication:
    case fbs::Body_Recovery:
      repRecordRcvd(ZuMv(buf));
      break;
    case fbs::Body_Commit:
      repCommitRcvd(ZuMv(buf));
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
    auto msg = fbs::CreateMsg(fbb, fbs::Body_Heartbeat, 
	fbs::CreateHeartbeat(fbb, &id,
	  m_env->state(), envState.save(fbb)).Union());
    fbb.Finish(msg);
  }

  send(saveHdr(fbb, this));

  ZdbDEBUG(m_env, ZtString{} << "hbSend()"
      "  self[ID:" << self->id() << " S:" << m_env->state() <<
      " SN:" << self->envState().sn <<
      " N:" << self->envState().count_() << "] " << self->envState());
}

// refresh db state vector
void Env::envStateRefresh()
{
  ZmAssert(invoked());

  EnvState &envState = m_self->envState();
  envState.updateSN(m_nextSN);
  all_([&envState](DB *db) {
    envState.update(db->config().id, db->nextUN());
  });
}

// process received replicated record
void Cxn_::repRecordRcvd(ZmRef<Buf> buf)
{
  ZmAssert(m_env->invoked());

  if (!m_host) return;
  if (m_env->repStore()) return; // back-end data store is replicated
  auto record = record_(msg_(buf->hdr()));
  auto id = Zfb::Load::id(record->db());
  DB *db = m_env->db(id);
  if (ZuUnlikely(!db)) return;
  buf->owner = db;
  ZdbDEBUG(m_env, ZtString{} <<
      "repRecordRcvd(host=" << m_host->id() << ", " <<
      Record_Print{record} << ')');
  m_env->replicated(
      m_host, id, record->un(), Zfb::Load::uint128(record->sn()));
  db->invoke([buf = ZuMv(buf)]() mutable {
    auto db = buf->db();
    db->repRecordRcvd(ZuMv(buf));
  });
}

// process received replication commit
void Cxn_::repCommitRcvd(ZmRef<Buf> buf)
{
  ZmAssert(m_env->invoked());

  if (!m_host) return;
  auto commit = commit_(msg_(buf->hdr()));
  auto id = Zfb::Load::id(commit->db());
  DB *db = m_env->db(id);
  if (ZuUnlikely(!db)) return;
  // buf->owner = db;
  ZdbDEBUG(m_env, ZtString{} <<
      "repCommitRcvd(host=" << m_host->id() << ", " << commit->un() << ')');
  db->invoke([db, un = commit->un()]() mutable { db->repCommitRcvd(un); });
}

void Env::replicated(Host *host, ZuID dbID, UN un, SN sn)
{
  ZmAssert(invoked());

  bool updated = host->envState().updateSN(sn + 1);
  updated = host->envState().update(dbID, un + 1) || updated;
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
  m_cacheUN{new CacheUN{}},
  m_repBufs{new RepBufs{}},
  m_repBufsUN{new RepBufsUN{}}
{
}

DB::~DB()
{
  // close(); // must be called while running
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
    name = str(fbb_, config().id);
    thread = str(fbb_, config().thread);
  }
  ZvTelemetry::fbs::ZdbBuilder fbb{fbb_};
  if (!update) {
    fbb.add_name(name);
    fbb.add_thread(thread);
  }
  fbb.add_nextRN(m_nextRN);
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
    object->init(
	record->rn(), record->un(),
	Zfb::Load::uint128(record->sn()), record->vn());
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
    object->init(
	record->rn(), record->un(),
	Zfb::Load::uint128(record->sn()), record->vn());
  return object;
}

// save object to buffer
Zfb::Offset<void> DB::save(Zfb::Builder &fbb, AnyObject_ *object)
{
  return m_handler.saveFn(fbb, object->ptr_());
}

// process inbound replication - record
void DB::repRecordRcvd(ZmRef<Buf> buf)
{
  ZmAssert(invoked());

  write(buf);
  recover(record_(msg_(buf->hdr())));
}

// process inbound replication - committed
void DB::repCommitRcvd(UN un)
{
  ZmAssert(invoked());

  repSendCommit(un);
  evictRepBuf(un);
}

// recover record
void DB::recover(const fbs::Record *record)
{
  ZmAssert(invoked());

  m_env->recoveredSN(Zfb::Load::uint128(record->sn()));
  recoveredRN(record->rn());
  recoveredUN(record->un());
  if (auto object = load(record)) {
    if (auto fn = m_handler.scanFn) fn(object);
  } else {
    if (auto fn = m_handler.deleteFn) fn(record->rn());
  }
}

// outbound replication + persistency
void DB::write(ZmRef<Buf> buf)
{
  ZmAssert(invoked());
  ZmAssert(buf->db() == this);

  m_repBufs->addNode(buf);
  m_repBufsUN->add(buf);
  m_env->invoke([buf = ZuMv(buf)]() mutable {
    auto db = buf->db();
    auto env = db->env();
    if (ZuLikely(env->active()) || !env->repStore()) {
      // leader, or follower without replicated data store - will
      // evict buf when write to data store is committed
      env->replicate(buf);
      db->commit(ZuMv(buf));
    } else {
      // follower with replicated data store - will evict buf
      // when leader subsequently sends commit, unless message is recovery
      env->replicate(ZuMv(buf));
      auto msg = msg_(buf->hdr());
      if (msg->body_type() == fbs::Body_Recovery)
	db->invoke([db, un = record_(msg)->un()]() { db->evictRepBuf(un); });
    }
  });
}

// low-level internal write to data store
void DB::commit(ZmRef<Buf> buf)
{
  auto msg = msg_(buf->hdr());
  bool recovery = msg->body_type() == fbs::Body_Recovery;
  auto record = record_(msg);
  auto object = load_(record);
  const void *ptr = object ? object->ptr_() : nullptr;

  using namespace Table_;

  CommitFn commitFn;
  if (recovery)
    commitFn = {ZuMv(object),
      [](AnyObject *, DB *db, UN un, CommitResult result) {
	db->invoke([db, un, result = ZuMv(result)]() mutable {
	  db->committed(un, result);
	});
      }};
  else
    commitFn = {ZuMv(object),
      [](AnyObject *, DB *db, UN un, CommitResult result) {
	db->invoke([db, un, result = ZuMv(result)]() mutable {
	  db->committed(un, result);
	  db->repSendCommit(un);
	});
      }};
  auto sn = Zfb::Load::uint128(record->sn());
  if (!ptr)
    m_table->del(record->rn(), record->un(), sn, record->vn(), ZuMv(commitFn));
  else if (record->vn())
    m_table->update(record->rn(), record->un(), sn, record->vn(),
	ptr, m_handler.exportUpdateFn, ZuMv(commitFn));
  else
    m_table->push(record->rn(), record->un(), sn,
	ptr, m_handler.exportFn, ZuMv(commitFn));
}

// process data store commit
void DB::committed(UN un, Table_::CommitResult &result)
{
  ZmAssert(invoked());

  if (ZuUnlikely(result.contains<Event>())) {
    ZeLogEvent(ZuMv(result).v<Event>());
    run([this, un]() {
      if (auto buf = findBufUN(un)) commit(ZuMv(buf));
    }, ZmTimeNow(m_env->config().retryFreq));
    return;
  }
  evictRepBuf(un);
}

// evict buffer from replication buffer queue
void DB::evictRepBuf(UN un)
{
  if (auto buf = m_repBufsUN->delVal(un)) m_repBufs->delNode(buf.ptr());
}

// Env::open() iterates over DBs, calling open(store)
// - each DB calls env->opened(this, rn, un) on success
template <typename L>
void DB::open(Store *store, L l)
{
  ZmAssert(invoked());

  if (m_open) return;

  using namespace Store_;

  Store_::ScanFn scanFn;
  if (m_handler.scanFn)
    scanFn = [](DB *db, ScanData data) {
      auto object = db->m_handler.importFn(db, data.import_);
      object->init(data.rn, data.un, data.sn, data.vn);
      db->m_handler.scanFn(object);
    };
  store->open(this, id(), fields(),
      [l = ZuMv(l)](DB *db, OpenResult result) mutable {
    db->invoke([l = ZuMv(l), result = ZuMv(result)]() mutable {
      l(ZuMv(result));
    });
  }, scanFn);
}

bool DB::opened(Store_::OpenResult result)
{
  ZmAssert(invoked());
  ZmAssert(!m_open);

  using namespace Store_;

  if (!result.contains<OpenData>()) {
    if (result.contains<Event>())
      ZeLogEvent(ZuMv(result).v<Event>());
    return false;
  }
  const auto &data = result.v<OpenData>();
  m_table = data.table;
  m_env->recoveredSN(data.sn);
  recoveredRN(data.rn);
  recoveredUN(data.un);

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

  m_table->close();

  m_open = false;
  m_table = nullptr;
}

ZmRef<AnyObject> DB::placeholder()
{
  if (auto fn = m_handler.ctorFn) return fn(this);
  return nullptr;
}

bool AnyObject_::push_(RN rn, UN un)
{
  if (m_state != ObjState::Undefined) return false;
  m_state = ObjState::Push;
  m_rn = rn;
  m_un = un;
  return true;
}
bool AnyObject_::update_(UN un)
{
  if (m_state != ObjState::Committed) return false;
  m_state = ObjState::Update;
  m_origUN = m_un;
  m_un = un;
  return true;
}
bool AnyObject_::del_(UN un)
{
  if (m_state != ObjState::Committed) return false;
  m_state = ObjState::Delete;
  m_origUN = m_un;
  m_un = un;
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

ZmRef<AnyObject> DB::push_(RN rn, UN un)
{
  auto fn = m_handler.ctorFn;
  if (ZuUnlikely(!fn)) return nullptr;
  ZmRef<AnyObject> object = fn(this);
  if (!object) return nullptr;
  if (!object->push_(rn, un)) return nullptr;
  return object;
}

bool DB::update_(AnyObject *object, UN un)
{
  m_cacheUN->del(object->un());
  return object->update_(un);
}

bool DB::del_(AnyObject *object, UN un)
{
  m_cacheUN->del(object->un());
  return object->del_(un);
}

  // commit push/update/delete - causes replication/write
bool DB::put(AnyObject *object)
{
  int origState = object->state();
  if (!object->put_()) return false;
  switch (origState) {
    case ObjState::Push:
      if (m_writeCache) {
	m_cache.add(object);
	m_cacheUN->add(object->un(), object);
      }
      break;
    case ObjState::Update:
      if (m_writeCache)
	m_cacheUN->add(object->un(), object);
      break;
    case ObjState::Delete:
      m_cache.delNode(object);
      break;
  }
  write(object->replicate(fbs::Body_Replication));
  return true;
}

// aborts push() or update()
bool DB::abort(AnyObject *object)
{
  return object->abort_();
}

} // namespace Zdb_
