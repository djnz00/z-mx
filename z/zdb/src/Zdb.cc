//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Z Database

// Notes on replication and failover:

// voted (connected, associated and heartbeated) hosts are sorted in
// priority order (i.e. SN then priority):
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

#include <zlib/Zdb.hh>

#include <zlib/ZtBitWindow.hh>

#include <zlib/ZiDir.hh>
#include <zlib/ZiModule.hh>

#include <assert.h>
#include <errno.h>

namespace Zdb_ {

void DB::init(
  DBCf config,
  ZiMultiplex *mx,
  DBHandler handler,
  ZmRef<Store> store)
{
  if (!ZmEngine<DB>::lock(ZmEngineState::Stopped,
	[this, &config, mx, &handler, store = ZuMv(store)]() mutable {
    if (state() != HostState::Instantiated) return false;

    static auto invalidSID = [](ZiMultiplex *mx, unsigned sid) -> bool {
      return !sid ||
	  sid > mx->params().nThreads() ||
	  sid == mx->rxThread() ||
	  sid == mx->txThread();
    };

    config.sid = mx->sid(config.thread);
    if (invalidSID(mx, config.sid))
      throw ZeEVENT(Fatal, ([thread = config.thread](auto &s) {
	s << "Zdb thread misconfigured: " << thread; }));

    {
      auto i = config.tableCfs.readIterator();
      while (auto tableCf_ = i.iterate()) {
	auto &tableCf = const_cast<TableCf &>(tableCf_->val());
	if (!tableCf.threads)
	  tableCf.sids.push(config.sid);
	else {
	  tableCf.sids.size(tableCf.threads.length());
	  tableCf.threads.all([mx, &tableCf](const ZtString &thread) {
	    auto sid = mx->sid(thread);
	    if (invalidSID(mx, sid))
	      throw ZeEVENT(Fatal,
		  ([id = tableCf.id, thread = ZtString{thread}](auto &s) {
		    s << "Zdb " << id
		      << " thread misconfigured: " << thread; }));
	    tableCf.sids.push(sid);
	  });
	}
      }
    }

    m_cf = ZuMv(config);
    m_mx = mx;
    m_handler = ZuMv(handler);
    {
      if (!m_cf.storeCf)
	throw ZeEVENT(Fatal, ([](auto &s) {
	  s << "no data store configured"; }));
      if (store)
	m_store = ZuMv(store);
      else {
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
      InitResult result = m_store->init(
	  m_cf.storeCf, m_mx,
	  FailFn::Member<&DB::storeFailed>::fn(this));
      if (result.is<Event>()) throw ZuMv(result).p<Event>();
      m_repStore = result.p<InitData>().replicated;
    }

    m_hostIndex.clean();
    m_hosts = new Hosts{};
    bool standalone = false;
    {
      unsigned tblCount = m_tables.count_();
      auto i = m_cf.hostCfs.readIterator();
      while (auto node = i.iterate()) {
	auto host = new Hosts::Node{this, &(node->data()), tblCount};
	if (host->standalone()) standalone = true;
	m_hosts->addNode(host);
	m_hostIndex.addNode(host);
      }
    }
    if (standalone && m_hosts->count_() > 1)
      throw ZeEVENT(Fatal, ([id = m_cf.hostID](auto &s) {
	s << "Zdb multiple hosts defined but one or more is standalone"; }));

    m_self = m_hosts->findPtr(m_cf.hostID);
    if (!m_self)
      throw ZeEVENT(Fatal, ([id = m_cf.hostID](auto &s) {
	s << "Zdb own host ID " << id << " not in hosts table"; }));
    state(HostState::Initialized);

    return true;
  }))
    throw ZeEVENT(Fatal, "Zdb::init called out of order");
}

ZmRef<AnyTable> DB::initTable_(
  ZtString id, ZmFn<AnyTable *(DB *, TableCf *)> ctorFn)
{
  ZmRef<AnyTable> table;
  if (!ZmEngine<DB>::lock(ZmEngineState::Stopped,
	[this, &table, id, ctorFn = ZuMv(ctorFn)]() {
    if (state() != HostState::Initialized) return false;
    if (m_tables.findVal(id)) return false;
    auto cf = m_cf.tableCfs.find(id);
    if (!cf) m_cf.tableCfs.addNode(cf = new TableCfs::Node{ZuMv(id)});
    table = ctorFn(this, &(cf->val()));
    m_tables.add(table);
    return true;
  }))
    throw ZeEVENT(Fatal, "Zdb::initTable called out of order");
  return table;
}

void DB::final()
{
  ZdbDEBUG(this, ([hostID = m_cf.hostID, state = this->state()](auto &s) {
    s << hostID << " state=" << HostState::name(state);
  }));

  if (!ZmEngine<DB>::lock(ZmEngineState::Stopped, [this]() {
    if (state() != HostState::Initialized) return false;
    // reset recovery
    m_recovering = 0; m_recover.reset(); m_recoverEnd.reset();
    // reset replication (clearing m_self also sets state to Instantiated)
    m_self = m_leader = m_prev = m_next = nullptr;
    m_selfID = m_leaderID = m_prevID = m_nextID = {};
    m_nPeers = 0; m_standalone = false;
    m_cxns.clean(); m_hostIndex.clean(); m_hosts->clean(); m_hosts = {};
    // reset tables
    m_nextSN = 0;
    m_tables.clean();
    // reset handler
    m_handler = {};
    // reset backing data store
    if (m_store) {
      m_store->final();
      m_store = nullptr;
    }
    return true;
  }))
    throw ZeEVENT(Fatal, "Zdb::final called out of order");
}

void DB::wake()
{
  run([this]() { stopped(); });	// polling stopped(), may call stop_()
}

void DB::start_()
{
  ZdbDEBUG(this, ([hostID = m_cf.hostID, state = this->state()](auto &s) {
    s << hostID << " state=" << HostState::name(state);
  }));

  ZmAssert(invoked());

  using namespace HostState;

  if (state() != Initialized) {
    ZeLOG(Fatal, "DB::start_ called out of order");
    started(false);
    return;
  }

  ZeLOG(Info, "Zdb starting");

  // start backing data store
  m_store->start([this](StartResult result) {
    if (ZuUnlikely(result.is<Event>())) {
      ZeLogEvent(ZuMv(result).p<Event>());
      ZeLOG(Fatal, ([](auto &s) {
	s << "Zdb data store start failed";
      }));
      run([this]() { started(false); });
      return;
    }
    run([this]() { start_1(); });
  });
}

void DB::start_1()
{
  ZdbDEBUG(this, "opening all tables");

  // open and recover all tables
  all([](AnyTable *table, ZmFn<void(bool)> done) {
    table->open([done = ZuMv(done)](bool ok) mutable { done(ok); });
  }, [](DB *db, bool ok) {
    ok ? db->start_2() : db->started(false);
  });
}

void DB::start_2()
{
  ZdbDEBUG(this, ([hostID = m_cf.hostID, state = this->state()](auto &s) {
    s << hostID << " state=" << HostState::name(state);
  }));

  ZmAssert(invoked());

  using namespace HostState;

  // refresh table state vector, begin election
  dbStateRefresh();
  repStop();
  state(Electing);

  if (!(m_nPeers = m_hosts->count_() - 1)) { // standalone
    holdElection();
    return;
  }

  run([this]() { hbSend(); },
      m_hbSendTime = Zm::now(), &m_hbSendTimer);
  run([this]() { holdElection(); },
      Zm::now(int(m_cf.electionTimeout)), &m_electTimer);

  listen();

  {
    auto i = m_hostIndex.readIterator<ZmRBTreeLess>(Host::IndexAxor(*m_self));
    while (Host *host = i.iterate()) host->connect();
  }
}

void DB::stop_()
{
  ZdbDEBUG(this, ([hostID = m_cf.hostID, state = this->state()](auto &s) {
    s << hostID << " state=" << HostState::name(state);
  }));

  ZmAssert(invoked());

  using namespace HostState;

  switch (state()) {
    case Active:
    case Inactive:
      break;
    case Electing:	// holdElection will resume stop_1() at completion
      return;
    default:
      ZeLOG(Fatal, "DB::stop_ called out of order");
      stopped(false);
      return;
  }

  ZeLOG(Info, "Zdb stopping");

  stop_1();
}

void DB::stop_1()
{
  ZdbDEBUG(this, ([hostID = m_cf.hostID, state = this->state()](auto &s) {
    s << hostID << " state=" << HostState::name(state);
  }));

  ZmAssert(invoked());

  // re-check state, stop_1() is resumed via holdElection()

  using namespace HostState;

  switch (state()) {
    case Active:
    case Inactive:
      break;
    default:
      return;
  }

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

void DB::stop_2()
{
  ZdbDEBUG(this, ([hostID = m_cf.hostID, state = this->state()](auto &s) {
    s << hostID << " state=" << HostState::name(state);
  }));

  ZmAssert(invoked());

  // close all tables
  all([](AnyTable *table, ZmFn<void(bool)> done) {
    table->close([done = ZuMv(done)]() mutable { done(true); });
  }, [](DB *db, bool) {
    db->stop_3();
  });
}

void DB::stop_3()
{
  ZdbDEBUG(this, ([hostID = m_cf.hostID, state = this->state()](auto &s) {
    s << hostID << " state=" << HostState::name(state);
  }));

  ZmAssert(invoked());

  state(HostState::Initialized);

  // stop backing data store
  m_store->stop([this](StopResult result) {
    if (ZuUnlikely(result.is<Event>())) {
      ZeLogEvent(ZuMv(result).p<Event>());
      ZeLOG(Fatal, ([](auto &s) {
	s << "Zdb data store stop failed";
      }));
    }
    run([this]() { stopped(true); });
  });
}

bool DB::disconnectAll()
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

void DB::listen()
{
  ZmAssert(invoked());

  if (!m_self->standalone())
    m_mx->listen(
	ZiListenFn::Member<&DB::listening>::fn(this),
	ZiFailFn::Member<&DB::listenFailed>::fn(this),
	ZiConnectFn::Member<&DB::accepted>::fn(this),
	m_self->ip(), m_self->port(), m_cf.nAccepts);
}

void DB::listening(const ZiListenInfo &)
{
  ZeLOG(Info, ([ip = m_self->ip(), port = m_self->port()](auto &s) {
    s << "Zdb listening on (" << ip << ':' << port << ')';
  }));
}

void DB::listenFailed(bool transient)
{
  bool retry = transient && running();
  if (retry) run([this]() { listen(); }, Zm::now((int)m_cf.reconnectFreq));
  ZeLOG(Warning, ([ip = m_self->ip(), port = m_self->port(), retry](auto &s) {
    s << "Zdb listen failed on (" << ip << ':' << port << ')';
    if (retry) s << " - retrying...";
  }));
}

void DB::stopListening()
{
  if (!m_self->standalone()) {
    ZeLOG(Info, "Zdb stop listening");
    m_mx->stopListening(m_self->ip(), m_self->port());
  }
}

void DB::holdElection()
{
  ZdbDEBUG(this, ([hostID = m_cf.hostID, state = this->state()](auto &s) {
    s << hostID << " state=" << HostState::name(state);
  }));

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
    if (appActive) down_(false);
  }

  state(won ? Active : Inactive);
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

void DB::fail()
{
  ZmAssert(invoked());

  if (!m_self) {
    ZeLOG(Fatal, "DB::fail called out of order");
    return;
  }

  deactivate(true);
}

void DB::deactivate(bool failed)
{
  ZmAssert(invoked());

  if (!m_self) {
badorder:
    ZeLOG(Fatal, "DB::deactivate called out of order");
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

  if (appActive) down_(failed);

  state(Inactive);
  setNext();
}

void Host::reactivate()
{
  m_db->reactivate(static_cast<Host *>(this));
}

void DB::reactivate(Host *host)
{
  ZmAssert(invoked());

  if (ZmRef<Cxn> cxn = host->cxn()) cxn->hbSend();

  bool appActive = m_appActive;
  m_appActive = true;
  if (!appActive) up_(nullptr);
}

void DB::up_(Host *oldMaster)
{
  ZeLOG(Info, "Zdb ACTIVE");
  m_handler.upFn(this, oldMaster);
}

void DB::down_(bool failed)
{
  ZeLOG(Info, "Zdb INACTIVE");
  m_handler.downFn(this, failed);
}

void DB::all(AllFn fn, AllDoneFn doneFn)
{
  ZmAssert(invoked());

  if (ZuUnlikely(m_allCount)) {
    ZeLOG(Fatal, ([](auto &s) {
      s << "Zdb - multiple overlapping calls to all()";
    }));
    doneFn(this, false);
    return;
  }
  auto i = m_tables.readIterator();
  m_allCount = m_allNotOK = m_tables.count_();
  if (ZuUnlikely(!m_allCount)) {
    ZeLOG(Fatal, ([](auto &s) { s << "Zdb - no tables"; }));
    doneFn(this, false);
    return;
  }
  m_allFn = ZuMv(fn);
  m_allDoneFn = ZuMv(doneFn);
  while (auto table = i.iterateVal().ptr())
    table->invoke(0, [table]() {
      auto db = table->db();
      db->m_allFn(table, ZmFn<void(bool)>{db, [](DB *db, bool ok) {
	db->invoke([db, ok]() { db->allDone(ok); });
      }});
    });
}

void DB::allDone(bool ok)
{
  ZmAssert(invoked());

  if (ZuUnlikely(!m_allCount)) return;
  if (ok) --m_allNotOK;
  if (!--m_allCount) {
    m_allDoneFn(this, !m_allNotOK);
    m_allFn = AllFn{};
    m_allDoneFn = AllDoneFn{};
    m_allCount = m_allNotOK = 0;
  }
}

Zfb::Offset<void>
DB::telemetry(Zfb::Builder &fbb_, bool update) const
{
  using namespace Zfb::Save;

  Zfb::Offset<Zfb::String> thread;
  if (!update) {
    thread = str(fbb_, m_cf.thread);
  }
  Ztel::fbs::DBBuilder fbb{fbb_};
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
    fbb.add_nTables(m_tables.count_());
    fbb.add_nHosts(m_hosts->count_());
    fbb.add_nPeers(m_nPeers);
  }
  auto state = this->state();
  fbb.add_state(state);
  fbb.add_active(state == HostState::Active);
  fbb.add_recovering(m_recovering);
  fbb.add_replicating(Host::replicating(m_next));
  return fbb.Finish().Union();
}

Zfb::Offset<void>
Host::telemetry(Zfb::Builder &fbb_, bool update) const
{
  using namespace Zfb::Save;

  Ztel::fbs::DBHostBuilder fbb{fbb_};
  if (!update) {
    { auto v = Zfb::Save::ip(config().ip); fbb.add_ip(&v); }
    { auto v = Zfb::Save::id(config().id); fbb.add_id(&v); }
    fbb.add_priority(config().priority);
    fbb.add_port(config().port);
  }
  fbb.add_state(static_cast<Ztel::fbs::DBHostState>(m_state));
  fbb.add_voted(m_voted);
  return fbb.Finish().Union();
}

Host::Host(DB *db, const HostCf *cf, unsigned tblCount) :
  m_db{db},
  m_cf{cf},
  m_mx{db->mx()},
  m_dbState{tblCount}
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
  bool retry = transient && m_db->running();
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
    s << "Zdb connected to host " << id << " ("
      << remoteIP << ':' << remotePort << "): "
      << localIP << ':' << localPort;
  }));

  if (!m_db->running()) return nullptr;

  return new Cxn{m_db, this, ci};
}

ZiConnection *DB::accepted(const ZiCxnInfo &ci)
{
  ZeLOG(Info,
      ([remoteIP = ci.remoteIP, remotePort = ci.remotePort,
	localIP = ci.localIP, localPort = ci.localPort](auto &s) {
    s << "Zdb accepted cxn on ("
      << remoteIP << ':' << remotePort << "): "
      << localIP << ':' << localPort;
  }));

  if (!running()) return nullptr;

  return new Cxn{this, nullptr, ci};
}

Cxn_::Cxn_(DB *db, Host *host, const ZiCxnInfo &ci) :
  ZiConnection{db->mx(), ci},
  m_db{db},
  m_host{host}
{
}

void Cxn_::connected(ZiIOContext &io)
{
  if (!m_db->running()) { io.disconnect(); return; }

  m_db->run([self = ZmMkRef(this)]() mutable {
    auto db = self->db();
    db->connected(ZuMv(self));
  });

  m_db->run([self = ZmMkRef(this)]() { self->hbTimeout(); },
      Zm::now(int(m_db->config().heartbeatTimeout)),
      ZmScheduler::Defer, &m_hbTimer);

  msgRead(io);
}

void DB::connected(ZmRef<Cxn> cxn)
{
  ZmAssert(invoked());

  if (!cxn->up()) return;

  if (Host *host = cxn->host()) associate(cxn, host);

  hbSend_(cxn);

  m_cxns.addNode(ZuMv(cxn));
}

void DB::associate(Cxn *cxn, ZuID hostID)
{
  ZmAssert(invoked());

  Host *host = m_hosts->find(hostID);

  if (!host) {
    ZeLOG(Error, ([hostID](auto &s) {
      s << "Zdb cannot associate incoming cxn: host ID "
	<< hostID << " not found";
    }));
    return;
  }

  if (host == m_self) {
    ZeLOG(Error, ([hostID](auto &s) {
      s << "Zdb cannot associate incoming cxn: host ID "
	<< hostID << " is same as self";
    }));
    return;
  }

  if (cxn->host() == host) return;

  associate(cxn, host);
}

void DB::associate(Cxn *cxn, Host *host)
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
  ZmAssert(m_db->invoked());

  if (ZuUnlikely(m_cxn && m_cxn.ptr() != cxn)) {
    m_cxn->host(nullptr);
    m_cxn->disconnect();
  }
  m_cxn = cxn;
}

void Host::reconnect()
{
  m_db->run([this]() { connect(); },
      Zm::now(int(m_db->config().reconnectFreq)),
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

  m_db->run([self = ZmMkRef(this)]() mutable {
    auto db = self->db();
    db->disconnected(ZuMv(self));
  });
}

void DB::disconnected(ZmRef<Cxn> cxn)
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

Host *DB::setMaster()
{
  ZmAssert(invoked());

  Host *oldMaster = m_leader;

  dbStateRefresh();

  m_leader = nullptr;
  m_nPeers = 0;

  {
    auto i = m_hostIndex.readIterator();

    ZdbDEBUG(this, ZtString{} << "setMaster()\n"
      << " self=" << ZuPrintPtr{m_self} << '\n'
      << " prev=" << ZuPrintPtr{m_prev} << '\n'
      << " next=" << ZuPrintPtr{m_next} << '\n'
      << " recovering=" << m_recovering
      << " replicating=" << Host::replicating(m_next));

    while (Host *host = i.iterate()) {
      ZdbDEBUG(this, ZtString{}
	<< " host=" << ZuPrintPtr{host} << '\n'
	<< " leader=" << ZuPrintPtr{m_leader});

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

  if (m_leader) {
    ZeLOG(Info, ([id = m_leader->id()](auto &s) {
      s << "Zdb host " << id << " is leader";
    }));
  } else
    ZeLOG(Fatal, "Zdb leader election failed");

  return oldMaster;
}

void DB::setNext(Host *host)
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

void DB::setNext()
{
  ZmAssert(invoked());

  Host *next = nullptr;

  {
    auto i = m_hostIndex.readIterator();

    ZdbDEBUG(this, ZtString{} << "setNext()\n"
      << " self=" << ZuPrintPtr{m_self} << '\n'
      << " leader=" << ZuPrintPtr{m_leader} << '\n'
      << " prev=" << ZuPrintPtr{m_prev} << '\n'
      << " next=" << ZuPrintPtr{m_next} << '\n'
      << " recovering=" << m_recovering
      << " replicating=" << Host::replicating(m_next));

    while (Host *host = i.iterate()) {
      if (host != m_self && host != m_prev && host->voted() &&
	  m_self->cmp(host) >= 0 && (!next || host->cmp(next) > 0))
	next = host;

      ZdbDEBUG(this, ZtString{}
	<< " host=" << ZuPrintPtr{host} << '\n'
	<< " next=" << ZuPrintPtr{next});
    }
  }

  setNext(next);
}

void DB::repStart()
{
  ZmAssert(invoked());

  ZeLOG(Info, ([id = m_next->id()](auto &s) {
    s << "Zdb host " << id << " is next in line";
  }));

  dbStateRefresh();

  ZdbDEBUG(this, ZtString{} << "repStart()\n"
    << " self=" << ZuPrintPtr{m_self} << '\n'
    << " leader=" << ZuPrintPtr{m_leader} << '\n'
    << " prev=" << ZuPrintPtr{m_prev} << '\n'
    << " next=" << ZuPrintPtr{m_next} << '\n'
    << " recovering=" << m_recovering
    << " replicating=" << Host::replicating(m_next));

  if (m_self->dbState().cmp(m_next->dbState()) < 0 ||
      m_recovering ||			// already recovering
      m_repStore)			// backing data store is replicated
    return;

  // ZeLOG(Info, "repStart() initiating recovery");

  m_recover = m_next->dbState();
  m_recoverEnd = m_self->dbState();
  if (ZmRef<Cxn> cxn = m_next->cxn()) {
    auto i = m_recover.readIterator();
    while (auto state = i.iterate()) {
      auto key = state->p<0>();
      if (auto endState = m_recoverEnd.find(key))
	if (auto table = m_tables.findVal(key.p<0>())) {
	  ++m_recovering;
	  auto shard = key.p<1>();
	  auto un = state->p<1>();
	  auto endUN = endState->p<1>();
	  if (endUN <= un) continue;
	  table->run(shard, [table, cxn, shard, un, endUN]() mutable {
	    table->recSend(ZuMv(cxn), shard, un, endUN);
	  });
	}
    }
  }
}

// send recovery record
void AnyTable::recSend(ZmRef<Cxn> cxn, Shard shard, UN un, UN endUN)
{
  ZmAssert(invoked(shard));

  if (!m_open) return;

  if (!cxn->up()) return;

  if (auto buf = mkBuf(shard, un)) {
    recSend_(ZuMv(cxn), shard, un, endUN, ZuMv(buf));
    return;
  }

  m_storeTbl->recover(shard, un, [
    this, cxn = ZuMv(cxn), shard, un, endUN
  ](RowResult result) mutable {
    if (ZuLikely(result.is<RowData>())) {
      ZmRef<const IOBuf> buf = result.p<RowData>().buf;
      run(shard, [
	this, cxn = ZuMv(cxn), shard, un, endUN, buf = ZuMv(buf)
      ]() mutable {
	recSend_(ZuMv(cxn), shard, un, endUN, ZuMv(buf));
      });
      return;
    }
    if (ZuUnlikely(result.is<Event>())) {
      ZeLogEvent(ZuMv(result).p<Event>());
      ZeLOG(Error, ([id = this->id(), shard, un](auto &s) {
	s << "Zdb recovery of " << id << '/' << shard << '/' << un << " failed";
      }));
    }
    // missing is not an error, skip over updated/deleted records
    run(shard, [this, cxn = ZuMv(cxn), shard, un, endUN]() mutable {
      recNext(ZuMv(cxn), shard, un, endUN);
    });
  });
}

void AnyTable::recSend_(
  ZmRef<Cxn> cxn, Shard shard, UN un, UN endUN, ZmRef<const IOBuf> buf)
{
  cxn->send(ZuMv(buf));
  recNext(ZuMv(cxn), shard, un, endUN);
}

void AnyTable::recNext(ZmRef<Cxn> cxn, Shard shard, UN un, UN endUN)
{
  if (++un < endUN)
    run(shard, [this, cxn = ZuMv(cxn), shard, un, endUN]() mutable {
      recSend(ZuMv(cxn), shard, un, endUN);
    });
  else
    m_db->invoke([db = m_db]() { db->recEnd(); });
}

void DB::recEnd()
{
  if (m_recovering) --m_recovering;
}

// build replication buffer
// - first looks in buffer cache for a buffer to copy
// - falls back to object cache
ZmRef<const IOBuf> AnyTable::mkBuf(Shard shard, UN un)
{
  ZmAssert(invoked(shard));

  // build from outbound replication buffer cache
  if (auto buf = findBufUN(shard, un)) {
    auto record = record_(msg_(buf->hdr()));
    auto repData = Zfb::Load::bytes(record->data());
    Zfb::IOBuilder fbb{allocBuf()};
    Zfb::Offset<Zfb::Vector<uint8_t>> data;
    if (repData) {
      uint8_t *ptr;
      data = Zfb::Save::pvector_(fbb, repData.length(), ptr);
      if (!data.IsNull() && ptr)
	memcpy(ptr, repData.data(), repData.length());
    }
    ZmAssert(record->shard() == shard);
    auto msg = fbs::CreateMsg(fbb, fbs::Body::Recovery,
	fbs::CreateRecord(
	  fbb, Zfb::Save::str(fbb, Zfb::Load::str(record->table())),
	  record->un(), record->sn(), record->vn(), shard, data).Union());
    fbb.Finish(msg);
    return saveHdr(fbb, this).constRef();
  }
  // build from object cache (without falling through to reading from disk)
  if (auto object = findUN(shard, un))
    return object->replicate(int(fbs::Body::Recovery));
  return nullptr;
}

// send commit to replica
void AnyTable::commitSend(Shard shard, UN un)
{
  Zfb::IOBuilder fbb{allocBuf()};
  {
    auto id = Zfb::Save::str(fbb, config().id);
    auto msg = fbs::CreateMsg(
      fbb, fbs::Body::Commit, fbs::CreateCommit(fbb, id, un, shard).Union());
    fbb.Finish(msg);
  }
  m_db->replicate(saveHdr(fbb, this).constRef());
}

// prepare replication data
ZmRef<const IOBuf> AnyObject::replicate(int type)
{
  ZmAssert(state() == ObjState::Committed || state() == ObjState::Deleted);

  ZdbDEBUG(m_table->db(), ZtString{}
    << "AnyObject::replicate(" << type << ')');

  Zfb::IOBuilder fbb{m_table->allocBuf()};
  auto data = Zfb::Save::nest(fbb, [this](Zfb::Builder &fbb) {
    if (!m_vn) return m_table->objSave(fbb, ptr_());
    if (m_vn > 0) return m_table->objSaveUpd(fbb, ptr_());
    return m_table->objSaveDel(fbb, ptr_());
  });
  {
    auto id = Zfb::Save::str(fbb, m_table->config().id);
    auto sn = Zfb::Save::uint128(m_sn);
    auto msg = fbs::CreateMsg(fbb, static_cast<fbs::Body>(type),
	fbs::CreateRecord(fbb, id, m_un, &sn, m_vn, m_shard, data).Union());
    fbb.Finish(msg);
  }
  return saveHdr(fbb, m_table).constRef();
}

void DB::repStop()
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
    [](const ZiIOContext &, ZiIOBuf *buf) -> int {
      return loadHdr(buf);
    },
    [](Cxn_ *cxn, const ZiIOContext &, ZmRef<ZiIOBuf> buf) -> int {
      return cxn->msgRead2(ZuMv(buf));
    }>(io);
}
int Cxn_::msgRead2(ZmRef<IOBuf> buf)
{
  return verifyHdr(ZuMv(buf), [this](const Hdr *hdr, ZmRef<IOBuf> buf) -> int {
    auto msg = Zdb_::msg(hdr);
    if (ZuUnlikely(!msg)) return -1;

    auto length = static_cast<uint32_t>(hdr->length);

    switch (msg->body_type()) {
      case fbs::Body::Heartbeat:
      case fbs::Body::Replication:
      case fbs::Body::Recovery:
      case fbs::Body::Commit:
	if (ZuLikely(buf->length))
	  m_db->run([cxn = ZmMkRef(this), buf = ZuMv(buf)]() mutable {
	    cxn->msgRead3(ZuMv(buf));
	  });
	break;
      default:
	break;
    }

    m_db->run([this]() { hbTimeout(); },
	Zm::now(int(m_db->config().heartbeatTimeout)),
	ZmScheduler::Defer, &m_hbTimer);

    return length;
  });
}
void Cxn_::msgRead3(ZmRef<IOBuf> buf)
{
  ZmAssert(m_db->invoked());

  if (!up()) return;

  auto msg = Zdb_::msg(buf->hdr());
  if (!msg) return;
  switch (msg->body_type()) {
    case fbs::Body::Heartbeat:
      hbRcvd(hb(msg));
      break;
    case fbs::Body::Replication:
    case fbs::Body::Recovery:
      repRecordRcvd(ZuMv(buf).constRef());
      break;
    case fbs::Body::Commit:
      repCommitRcvd(ZuMv(buf).constRef());
      break;
    default:
      break;
  }
}

void Cxn_::hbRcvd(const fbs::Heartbeat *hb)
{
  if (!m_host) m_db->associate(
      static_cast<Cxn *>(this), Zfb::Load::id(hb->host()));

  if (!m_host) { disconnect(); return; }

  m_db->hbRcvd(m_host, hb);
}

// process received heartbeat
void DB::hbRcvd(Host *host, const fbs::Heartbeat *hb)
{
  ZdbDEBUG(this, ZtString{} << "hbDataRcvd()\n"
    << " host=" << ZuPrintPtr{host} << '\n'
    << " self=" << ZuPrintPtr{m_self} << '\n'
    << " leader=" << ZuPrintPtr{m_leader} << '\n'
    << " prev=" << ZuPrintPtr{m_prev} << '\n'
    << " next=" << ZuPrintPtr{m_next} << '\n'
    << " recovering=" << m_recovering
    << " replicating=" << Host::replicating(m_next));

  host->state(hb->state());
  host->dbState().load(hb->dbState());

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
	    deactivate(false);
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
void DB::vote(Host *host)
{
  host->voted(true);
  dbStateRefresh();
  if (host != m_next && host != m_prev &&
      m_self->cmp(host) >= 0 && (!m_next || host->cmp(m_next) > 0))
    setNext(host);
}

// send replication message to next-in-line
bool DB::replicate(ZmRef<const IOBuf> buf)
{
  if (m_next)
    if (ZmRef<Cxn> cxn = m_next->cxn()) {
      cxn->send(ZuMv(buf));
      return true;
    }
  return false;
}

// broadcast heartbeat
void DB::hbSend()
{
  ZmAssert(invoked());

  hbSend_();

  run([this]() { hbSend(); },
    m_hbSendTime += (time_t)m_cf.heartbeatFreq,
    ZmScheduler::Defer, &m_hbSendTimer);
}

// send heartbeat (broadcast)
void DB::hbSend_()
{
  ZmAssert(invoked());

  dbStateRefresh();
  auto i = m_cxns.readIterator();
  while (auto cxn = i.iterate()) cxn->hbSend();
}

// send heartbeat (directed)
void DB::hbSend_(Cxn *cxn)
{
  ZmAssert(invoked());

  dbStateRefresh();
  cxn->hbSend();
}

// send heartbeat on a specific connection
void Cxn_::hbSend()
{
  ZmAssert(m_db->invoked());

  Host *self = m_db->self();
  Zfb::IOBuilder fbb{new ZiIOBufAlloc<HBBufSize>{}};
  {
    const auto &dbState = self->dbState();
    auto id = Zfb::Save::id(self->id());
    auto msg = fbs::CreateMsg(fbb, fbs::Body::Heartbeat, 
	fbs::CreateHeartbeat(fbb, &id,
	  m_db->state(), dbState.save(fbb)).Union());
    fbb.Finish(msg);
  }

  send(saveHdr(fbb, this).constRef());

  ZdbDEBUG(m_db, ZtString{}
    << "hbSend() self{id=" << self->id()
    << ", state=" << m_db->state()
    << ", dbState=" << self->dbState() << '}');
}

// refresh table state vector
void DB::dbStateRefresh()
{
  ZmAssert(invoked());

  DBState &dbState = m_self->dbState();
  dbState.updateSN(m_nextSN);
  all_([&dbState](AnyTable *table) {
    for (Shard i = 0, n = table->config().nShards; i < n; i++)
      dbState.update(ZuFwdTuple(table->config().id, i), table->nextUN(i));
  });
}

// process received replicated record
void Cxn_::repRecordRcvd(ZmRef<const IOBuf> buf)
{
  ZmAssert(m_db->invoked());

  if (!m_host) return;
  if (m_db->repStore()) return; // backing data store is replicated
  auto record = Zdb_::record(msg_(buf->hdr())); // caller verified msg
  if (!record) return;
  ZuString id = Zfb::Load::str(record->table());
  AnyTable *table = m_db->table(id);
  if (ZuUnlikely(!table)) return;

  ZdbDEBUG(m_db, (ZtString{}
    << "repRecordRcvd(host=" << m_host->id() << ", "
    << Record_Print{record, table}));

  auto shard = record->shard();

  m_db->replicated(
    m_host, id, shard, record->un(), Zfb::Load::uint128(record->sn()));
  table->invoke(shard, [table, shard, buf = ZuMv(buf)]() mutable {
    table->repRecordRcvd(shard, ZuMv(buf));
  });
}

// process received replication commit
void Cxn_::repCommitRcvd(ZmRef<const IOBuf> buf)
{
  ZmAssert(m_db->invoked());

  if (!m_host) return;
  auto commit = Zdb_::commit(msg_(buf->hdr())); // caller verified msg
  auto id = Zfb::Load::str(commit->table());
  AnyTable *table = m_db->table(id);
  if (ZuUnlikely(!table)) return;

  ZdbDEBUG(m_db, ZtString{}
    << "repCommitRcvd(host=" << m_host->id() << ", " << commit->un() << ')');

  auto shard = commit->shard();
  table->invoke(shard, [table, shard, un = commit->un()]() mutable {
    table->repCommitRcvd(shard, un);
  });
}

void DB::replicated(Host *host, ZuString tblID, Shard shard, UN un, SN sn)
{
  ZmAssert(invoked());

  bool updated = host->dbState().updateSN(sn + 1);
  updated = host->dbState().update(ZuFwdTuple(tblID, shard), un + 1) || updated;
  if ((active() || host == m_next) && !updated) return;
  if (!m_prev) {
    m_prev = host;
    ZeLOG(Info, ([id = m_prev->id()](auto &s) {
      s << "Zdb host " << id << " is previous in line";
    }));
  }
}

AnyTable::AnyTable(DB *db, TableCf *cf, IOBufAllocFn fn) :
  m_db{db}, m_cf{cf}, m_mx{db->mx()},
  m_bufAllocFn{ZuMv(fn)}
{
  unsigned n = cf->nShards;
  m_nextUN.length(n);
  m_cacheUN.length(n);
  m_bufCacheUN.length(n);
  ZmIDString cacheID = "Zdb.CacheUN."; cacheID << cf->id;
  ZmIDString bufCacheID = "Zdb.BufCacheUN."; bufCacheID << cf->id;
  for (unsigned i = 0; i < n; i++) {
    m_nextUN[i] = 0;
    m_cacheUN[i] = new CacheUN{cacheID};
    m_bufCacheUN[i] = new BufCacheUN{bufCacheID};
  }
}

AnyTable::~AnyTable()
{
  // close(); // must be called while running
}

// telemetry
Zfb::Offset<void>
AnyTable::telemetry(Zfb::Builder &fbb_, bool update) const
{
  using namespace Zfb::Save;

  Zfb::Offset<Zfb::String> path, name;
  Zfb::Offset<Zfb::Vector<Zfb::Offset<Zfb::String>>> thread;
  if (!update) {
    name = str(fbb_, config().id);
    thread = strVecIter(
      fbb_, config().threads.length(),
      [this](unsigned i) -> const ZtString & {
	return config().threads[i];
      });
  }
  unsigned cacheSize = 0;
  uint64_t cacheLoads = 0, cacheMisses = 0, cacheEvictions = 0;
  for (unsigned i = 0, n = config().nShards; i < n; i++) {
    ZmCacheStats stats;
    this->cacheStats(i, stats);
    cacheSize += stats.size;
    cacheLoads += stats.loads;
    cacheMisses += stats.misses;
    cacheEvictions += stats.evictions;
  }
  Ztel::fbs::DBTableBuilder fbb{fbb_};
  if (!update) {
    fbb.add_name(name);
    fbb.add_shards(config().nShards);
    fbb.add_thread(thread);
  }
  fbb.add_count(m_count.load_());
  fbb.add_cacheLoads(cacheLoads);
  fbb.add_cacheMisses(cacheMisses);
  fbb.add_cacheEvictions(cacheEvictions);
  if (!update) {
    fbb.add_cacheSize(cacheSize);
    fbb.add_cacheMode(static_cast<Ztel::fbs::DBCacheMode>(config().cacheMode));
  }
  return fbb.Finish().Union();
}

// process inbound replication - record
void AnyTable::repRecordRcvd(Shard shard, ZmRef<const IOBuf> buf)
{
  ZmAssert(invoked(shard));

  if (!m_open) return;

  recover(shard, record_(msg_(buf->hdr())));
  write(shard, buf, false);
}

// process inbound replication - committed
void AnyTable::repCommitRcvd(Shard shard, UN un)
{
  ZmAssert(invoked(shard));

  if (!m_open) return;

  commitSend(shard, un);
  evictBuf(shard, un);
}

// recover record
void AnyTable::recover(Shard shard, const fbs::Record *record)
{
  m_db->recoveredSN(Zfb::Load::uint128(record->sn()));
  recoveredUN(shard, record->un());
  objRecover(record);
}

// outbound replication + persistency
void AnyTable::write(Shard shard, ZmRef<const IOBuf> buf, bool active)
{
  ZmAssert(invoked(shard));

  cacheBuf(shard, buf);
  auto db = this->db();
  if (ZuLikely(active) || !db->repStore()) {
    // leader, or follower without replicated data store - will
    // evict buf when write to data store is committed
    db->invoke([db, buf]() mutable { db->replicate(buf); });
    store(shard, ZuMv(buf));
  } else {
    // follower with replicated data store - will evict buf
    // when leader subsequently sends commit, unless message is recovery
    auto msg = msg_(buf->hdr());
    auto un = record_(msg)->un();
    bool recovery = msg->body_type() == fbs::Body::Recovery;
    db->invoke([db, buf = ZuMv(buf)]() mutable { db->replicate(buf); });
    if (recovery) invoke(shard, [this, shard, un]() { evictBuf(shard, un); });
  }
}

// low-level internal write to backing data store
void AnyTable::store(Shard shard, ZmRef<const IOBuf> buf)
{
  ZmAssert(invoked(shard));

  if (ZuUnlikely(!m_open)) return; // table is closing

  store_(shard, ZuMv(buf));
}
void AnyTable::store_(Shard shard, ZmRef<const IOBuf> buf)
{
  m_storeTbl->write(ZuMv(buf), CommitFn{this,
    [](AnyTable *this_, ZmRef<const IOBuf> buf, CommitResult result) {
      this_->committed(ZuMv(buf), ZuMv(result));
    }});
}
void AnyTable::committed(ZmRef<const IOBuf> buf, CommitResult result)
{
  auto msg = msg_(buf->hdr());
  auto record = record_(msg);
  auto shard = record->shard();
  auto un = record->un();
  if (ZuUnlikely(result.is<Event>())) {
    ZeLogEvent(ZuMv(result).p<Event>());
    ZeLOG(Fatal, ([id = this->id(), shard, un](auto &s) {
      s << "Zdb store of " << id << '/' << shard << '/' << un << " failed";
    }));
    auto db = this->db();
    db->run([db]() { db->fail(); }); // trigger failover
    return;
  }
  bool recovery = msg->body_type() == fbs::Body::Recovery;
  run(shard, [this, shard, un, recovery]() {
    evictBuf(shard, un);
    if (!recovery) commitSend(shard, un);
  });
}

// cache buffer
void AnyTable::cacheBuf(Shard shard, ZmRef<const IOBuf> buf)
{
  cacheBufUN(shard, buf.mutablePtr());
  cacheBuf_(shard, ZuMv(buf));
}

// evict buffer
void AnyTable::evictBuf(Shard shard, UN un)
{
  if (auto buf = evictBufUN(shard, un))
    evictBuf_(shard, static_cast<IOBuf *>(buf));
}

// DB::open() iterates over tables, calling open()
// - each store table open calls table->opened(OpenResult) on success
template <typename L>
void AnyTable::open(L l)
{
  /* ZeLOG(Debug,
    ([hostID = db()->config().hostID, open = unsigned(m_open)](auto &s) {
      s << hostID << " m_open=" << open;
    })); */

  ZmAssert(invoked(0));
  ZmAssert(!m_open);

  if (m_open) {
    l(true);
    return;
  }

  db()->store()->open(
    id(), config().nShards,
    objFields(), objKeyFields(), objSchema(), m_bufAllocFn,
    [this, l = ZuMv(l)](OpenResult result) mutable {
      invoke(0, [this, l = ZuMv(l), result = ZuMv(result)]() mutable {
	l(opened(ZuMv(result)));
      });
    });
}

bool AnyTable::opened(OpenResult result)
{
  ZdbDEBUG(m_db, ([
    hostID = m_db->config().hostID, open = unsigned(m_open)
  ](auto &s) {
    s << hostID << " m_open=" << open;
  }));

  ZmAssert(invoked(0));
  ZmAssert(!m_open);

  if (m_open) return true;

  if (!result.is<OpenData>()) {
    if (result.is<Event>())
      ZeLogEvent(ZuMv(result).p<Event>());
    return false;
  }

  const auto &data = result.p<OpenData>();
  m_storeTbl = data.storeTbl;
  m_count = data.count;
  m_db->recoveredSN(data.sn);
  for (unsigned i = 0, n = config().nShards; i < n; i++)
    recoveredUN(i, data.un[i]);

  m_open = 1;
  return true;
}

template <typename L>
void AnyTable::close(L l)
{
  /* ZeLOG(Debug,
    ([hostID = db()->config().hostID, open = unsigned(m_open)](auto &s) {
      s << hostID << " m_open=" << open;
    })); */

  ZmAssert(invoked(0));

  // ensure idempotence

  if (!m_open) {
    l();
    return;
  }

  if (!m_storeTbl) {
    l();
    m_open = 0;
    return;
  }

  m_storeTbl->close([this, l = ZuMv(l)]() mutable {
    invoke(0, [this, l = ZuMv(l)]() mutable {
      m_storeTbl = nullptr;
      l();
      m_open = 0;
    });
  });
}

bool AnyObject::insert_(UN un)
{
  if (m_state != ObjState::Undefined) return false;
  m_state = ObjState::Insert;
  m_un = un;
  return true;
}
bool AnyObject::update_(UN un)
{
  if (m_state != ObjState::Committed) return false;
  m_state = ObjState::Update;
  m_origUN = m_un;
  m_un = un;
  return true;
}
bool AnyObject::del_(UN un)
{
  if (m_state != ObjState::Committed) return false;
  m_state = ObjState::Delete;
  m_origUN = m_un;
  m_un = un;
  return true;
}

bool AnyObject::commit_()
{
  switch (m_state) {
    default: return false;
    case ObjState::Insert:
    case ObjState::Update:
    case ObjState::Delete: break;
  }
  if (ZuUnlikely(!m_table->allocUN(m_shard, m_un))) {
    abort_();
    return false;
  }
  m_sn = m_table->db()->allocSN();
  switch (m_state) {
    case ObjState::Insert:
      m_state = ObjState::Committed;
      break;
    case ObjState::Update:
      m_state = ObjState::Committed;
      m_origUN = nullUN();
      ++m_vn;
      break;
    case ObjState::Delete:
      m_state = ObjState::Deleted;
      m_origUN = nullUN();
      m_vn = -m_vn - 1;
      break;
  }
  return true;
}

bool AnyObject::abort_()
{
  switch (m_state) {
    default: return false;
    case ObjState::Insert:
      m_state = ObjState::Undefined;
      m_un = nullUN();
      break;
    case ObjState::Update:
    case ObjState::Delete:
      m_state = ObjState::Committed;
      m_un = m_origUN;
      m_origUN = nullUN();
      break;
  }
  return true;
}

} // namespace Zdb_
