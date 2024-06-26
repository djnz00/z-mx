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
    Store *store)
{
  if (!ZmEngine<DB>::lock(ZmEngineState::Stopped,
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
	s << "ZdbDB thread misconfigured: " << thread; }));
    if (!config.writeThread)
      config.writeSID = config.sid;
    else {
      config.writeSID = mx->sid(config.writeThread);
      if (invalidSID(config.writeSID))
	throw ZeEVENT(Fatal, ([writeThread = config.writeThread](auto &s) {
	  s << "ZdbDB write thread misconfigured: " << writeThread; }));
    }

    {
      auto i = config.tableCfs.readIterator();
      while (auto tableCf_ = i.iterate()) {
	auto &tableCf = const_cast<TableCf &>(tableCf_->val());
	if (!tableCf.thread)
	  tableCf.sid = config.sid;
	else {
	  tableCf.sid = mx->sid(tableCf.thread);
	  if (invalidSID(tableCf.sid))
	    throw ZeEVENT(Fatal,
		([id = tableCf.id, thread = tableCf.thread](auto &s) {
		  s << "Zdb " << id
		    << " thread misconfigured: " << thread; }));
	}
	if (!tableCf.writeThread) {
	  if (tableCf.thread)
	    tableCf.writeSID = tableCf.sid;
	  else
	    tableCf.writeSID = config.writeSID;
	} else {
	  tableCf.writeSID = mx->sid(tableCf.writeThread);
	  if (invalidSID(tableCf.writeSID) ||
	      (tableCf.sid != config.sid && tableCf.writeSID == config.sid))
	    throw ZeEVENT(Fatal,
		([id = tableCf.id, writeThread = tableCf.writeThread](auto &s) {
		  s << "Zdb " << id
		    << " write thread misconfigured: " << writeThread; }));
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
      if (result.is<Event>()) throw ZuMv(result).p<Event>();
      m_repStore = result.p<Store_::InitData>().replicated;
    }

    m_hostIndex.clean();
    m_hosts = new Hosts{};
    {
      unsigned dbCount = m_tables.count_();
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
    state(HostState::Initialized);

    return true;
  }))
    throw ZeEVENT(Fatal, "ZdbDB::init called out of order");
}

ZmRef<AnyTable> DB::initTable_(ZuID id, ZmFn<DB *, TableCf *> ctorFn)
{
  ZmRef<AnyTable> table;
  if (!ZmEngine<DB>::lock(ZmEngineState::Stopped,
	[this, &table, id, ctorFn = ZuMv(ctorFn)]() {
    if (state() != HostState::Initialized) return false;
    auto cf = m_cf.tableCfs.find(id);
    if (!cf) m_cf.tableCfs.addNode(cf = new TableCfs::Node{id});
    if (m_tables.findVal(id)) return false;
    table = reinterpret_cast<AnyTable *>(ctorFn(this, &(cf->val())));
    m_tables.add(table);
    return true;
  }))
    throw ZeEVENT(Fatal, "ZdbDB::initTable called out of order");
  return table;
}

void DB::final()
{
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
    throw ZeEVENT(Fatal, "ZdbDB::final called out of order");
}

void DB::wake()
{
  run([this]() { stopped(); });
}

void DB::start_()
{
  ZmAssert(invoked());

  using namespace HostState;

  if (state() != Initialized) {
    ZeLOG(Fatal, "DB::start_ called out of order");
    started(false);
    return;
  }

  ZeLOG(Info, "Zdb starting");

  // open and recover all tables
  {
    ZmAtomic<unsigned> ok = true;
    auto i = m_tables.readIterator();
    ZmBlock<>{}(m_tables.count_(), [&ok, &i](unsigned, auto wake) mutable {
      if (auto table = i.iterateVal())
	table->invoke([table, &ok, wake = ZuMv(wake)]() mutable {
	  table->open(table->db()->m_store,
	    [table, &ok, wake = ZuMv(wake)](Store_::OpenResult result) mutable {
	      ok &= table->opened(ZuMv(result));
	      wake();
	    });
	  });
    });
    if (!ok) {
      allSync([](AnyTable *table) { return [table]() { table->close(); }; });
      started(false);
      return;
    }
  }

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
  ZmAssert(invoked());

  using namespace HostState;

  switch (state()) {
    case Active:
    case Inactive:
      break;
    case Electing:	// holdElection will resume stop_() at completion
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

void DB::stop_2()
{
  ZmAssert(invoked());

  allSync([](AnyTable *table) { return [table]() { table->close(); }; });

  state(HostState::Initialized);

  stopped(true);
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
  ZeLOG(Info, "Zdb stop listening");
  m_mx->stopListening(m_self->ip(), m_self->port());
}

void DB::holdElection()
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

void DB::deactivate()
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

  if (appActive) down_();

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
  if (ZtString cmd = m_self->config().up) {
    if (oldMaster) cmd << ' ' << oldMaster->config().ip;
    ZeLOG(Info, ([cmd](auto &s) { s << "Zdb invoking \"" << cmd << '\"'; }));
    ::system(cmd);
  }
  m_handler.upFn(this, oldMaster);
}

void DB::down_()
{
  ZeLOG(Info, "Zdb INACTIVE");
  if (ZtString cmd = m_self->config().down) {
    ZeLOG(Info, ([cmd](auto &s) { s << "Zdb invoking \"" << cmd << '\"'; }));
    ::system(cmd);
  }
  m_handler.downFn(this);
}

ZvTelemetry::DBFn DB::telFn()
{
  return ZvTelemetry::DBFn{ZmMkRef(this), [](
      DB *db,
      ZvTelemetry::BuildDBFn dbFn,
      ZvTelemetry::BuildDBHostFn hostFn,
      ZvTelemetry::BuildDBTableFn tableFn,
      bool update) {
    db->invoke([db,
	dbFn = ZuMv(dbFn),
	hostFn = ZuMv(hostFn),
	tableFn = ZuMv(tableFn), update]() {
      {
	ZvTelemetry::IOBuilder fbb;
	dbFn(fbb, db->telemetry(fbb, update));
	db->allHosts([&hostFn, &fbb, update](const Host *host) {
	  hostFn(fbb, host->telemetry(fbb, update));
	});
      }
      db->all([tableFn = ZuMv(tableFn), update](const AnyTable *table) {
	return [tableFn, update, table]() {
	  ZvTelemetry::IOBuilder fbb;
	  tableFn(fbb, table->telemetry(fbb, update));
	};
      });
    });
  }};
}

Zfb::Offset<ZvTelemetry::fbs::DB>
DB::telemetry(ZvTelemetry::IOBuilder &fbb_, bool update) const
{
  using namespace Zfb;
  using namespace Zfb::Save;

  Zfb::Offset<String> thread, writeThread;
  if (!update) {
    thread = str(fbb_, m_cf.thread);
    writeThread = str(fbb_, m_cf.writeThread);
  }
  ZvTelemetry::fbs::DBBuilder fbb{fbb_};
  if (!update) {
    fbb.add_thread(thread);
    fbb.add_thread(writeThread);
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
  return fbb.Finish();
}

Zfb::Offset<ZvTelemetry::fbs::DBHost>
Host::telemetry(ZvTelemetry::IOBuilder &fbb_, bool update) const
{
  using namespace Zfb;
  using namespace Zfb::Save;

  ZvTelemetry::fbs::DBHostBuilder fbb{fbb_};
  if (!update) {
    { auto v = Zfb::Save::ip(config().ip); fbb.add_ip(&v); }
    { auto v = Zfb::Save::id(config().id); fbb.add_id(&v); }
    fbb.add_priority(config().priority);
    fbb.add_port(config().port);
  }
  fbb.add_state(static_cast<ZvTelemetry::fbs::DBHostState>(m_state));
  fbb.add_voted(m_voted);
  return fbb.Finish();
}

Host::Host(DB *db, const HostCf *cf, unsigned dbCount) :
  m_db{db},
  m_cf{cf},
  m_mx{db->mx()},
  m_dbState{dbCount}
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
    s << "Zdb connected to host " << id << " (" <<
      remoteIP << ':' << remotePort << "): " <<
      localIP << ':' << localPort;
  }));

  if (!m_db->running()) return nullptr;

  return new Cxn{m_db, this, ci};
}

ZiConnection *DB::accepted(const ZiCxnInfo &ci)
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

void DB::repStart()
{
  ZmAssert(invoked());

  ZeLOG(Info, ([id = m_next->id()](auto &s) {
    s << "Zdb host " << id << " is next in line";
  }));

  dbStateRefresh();

  ZdbDEBUG(this, ZtString{} << "repStart()\n" <<
      " self=" << ZuPrintPtr{m_self} << '\n' <<
      " leader=" << ZuPrintPtr{m_leader} << '\n' <<
      " prev=" << ZuPrintPtr{m_prev} << '\n' <<
      " next=" << ZuPrintPtr{m_next} << '\n' <<
      " recovering=" << m_recovering <<
      " replicating=" << Host::replicating(m_next));

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
      ZuID id = state->template p<0>();
      if (auto endState = m_recoverEnd.find(id))
	if (auto table = m_tables.findVal(id)) {
	  ++m_recovering;
	  auto un = state->p<1>();
	  auto endUN = endState->p<1>();
	  if (endUN <= un) continue;
	  table->run([table, cxn, un, endUN]() mutable {
	    table->recSend(ZuMv(cxn), un, endUN);
	  });
	}
    }
  }
}

// send recovery record
void AnyTable::recSend(ZmRef<Cxn> cxn, UN un, UN endUN) {
  if (!m_open) return;

  ZmAssert(invoked());

  if (!cxn->up()) return;

  if (auto buf = mkBuf(un)) {
    recSend_(ZuMv(cxn), un, endUN, ZuMv(buf));
    return;
  }

  using namespace StoreTbl_;

  m_storeTbl->recover(un,
    [this, cxn = ZuMv(cxn), un, endUN](RowResult result) mutable {
    if (ZuLikely(result.is<RowData>())) {
      ZmRef<const AnyBuf> buf = result.p<RowData>().buf;
      run([this, cxn = ZuMv(cxn), un, endUN, buf = ZuMv(buf)]() mutable {
	recSend_(ZuMv(cxn), un, endUN, ZuMv(buf));
      });
      return;
    }
    if (ZuUnlikely(result.is<Event>())) {
      ZeLogEvent(ZuMv(result).p<Event>());
      ZeLOG(Error, ([id = this->id(), un](auto &s) {
	s << "Zdb recovery of " << id << '/' << un << " failed";
      }));
    }
    // missing is not an error, skip over updated/deleted records
    run([this, cxn = ZuMv(cxn), un, endUN]() mutable {
      recNext(ZuMv(cxn), un, endUN);
    });
  });
}

void AnyTable::recSend_(
  ZmRef<Cxn> cxn, UN un, UN endUN, ZmRef<const AnyBuf> buf)
{
  cxn->send(ZuMv(buf));
  recNext(ZuMv(cxn), un, endUN);
}

void AnyTable::recNext(ZmRef<Cxn> cxn, UN un, UN endUN)
{
  if (++un < endUN)
    run([this, cxn = ZuMv(cxn), un, endUN]() mutable {
      recSend(ZuMv(cxn), un, endUN);
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
ZmRef<const AnyBuf> AnyTable::mkBuf(UN un)
{
  ZmAssert(invoked());

  // build from outbound replication buffer cache
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
    auto msg = fbs::CreateMsg(fbb, fbs::Body::Recovery,
	fbs::CreateRecord(fbb, record->table(), record->un(),
	  record->sn(), record->vn(), data).Union());
    fbb.Finish(msg);
    return saveHdr(fbb, this).constRef();
  }
  // build from object cache (without falling through to reading from disk)
  if (auto object = findUN(un))
    return object->replicate(int(fbs::Body::Recovery));
  return nullptr;
}

// send commit to replica
void AnyTable::commitSend(UN un)
{
  IOBuilder fbb;
  {
    auto id = Zfb::Save::id(config().id);
    auto msg = fbs::CreateMsg(
      fbb, fbs::Body::Commit, fbs::CreateCommit(fbb, &id, un).Union());
    fbb.Finish(msg);
  }
  m_db->replicate(saveHdr(fbb, this).constRef());
}

// prepare replication data
ZmRef<const AnyBuf> AnyObject::replicate(int type)
{
  ZmAssert(state() == ObjState::Committed || state() == ObjState::Deleted);

  ZdbDEBUG(m_table->db(),
      ZtString{} << "AnyObject::replicate(" << type << ')');

  IOBuilder fbb;
  auto data = Zfb::Save::nest(fbb, [this](Zfb::Builder &fbb) {
    if (!m_vn) return m_table->objSave(fbb, ptr_());
    if (m_vn > 0) return m_table->objSaveUpd(fbb, ptr_());
    return m_table->objSaveDel(fbb, ptr_());
  });
  {
    auto id = Zfb::Save::id(m_table->config().id);
    auto sn = Zfb::Save::uint128(m_sn);
    auto msg = fbs::CreateMsg(fbb, static_cast<fbs::Body>(type),
	fbs::CreateRecord(fbb, &id, m_un, &sn, m_vn, data).Union());
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
    [](const ZiIOContext &, Buf *buf) -> int {
      return loadHdr(buf);
    },
    [](Cxn_ *cxn, const ZiIOContext &, ZmRef<AnyBuf> buf) -> int {
      return cxn->msgRead2(ZuMv(buf));
    }>(io);
}
int Cxn_::msgRead2(ZmRef<AnyBuf> buf)
{
  return verifyHdr(ZuMv(buf), [this](const Hdr *hdr, ZmRef<AnyBuf> buf) -> int {
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
void Cxn_::msgRead3(ZmRef<AnyBuf> buf)
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
  ZdbDEBUG(this, ZtString{} << "hbDataRcvd()\n" << 
	" host=" << ZuPrintPtr{host} << '\n' <<
	" self=" << ZuPrintPtr{m_self} << '\n' <<
	" leader=" << ZuPrintPtr{m_leader} << '\n' <<
	" prev=" << ZuPrintPtr{m_prev} << '\n' <<
	" next=" << ZuPrintPtr{m_next} << '\n' <<
	" recovering=" << m_recovering <<
	" replicating=" << Host::replicating(m_next));

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
void DB::vote(Host *host)
{
  host->voted(true);
  dbStateRefresh();
  if (host != m_next && host != m_prev &&
      m_self->cmp(host) >= 0 && (!m_next || host->cmp(m_next) > 0))
    setNext(host);
}

// send replication message to next-in-line
bool DB::replicate(ZmRef<const AnyBuf> buf)
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
  IOBuilder fbb;
  {
    const auto &dbState = self->dbState();
    auto id = Zfb::Save::id(self->id());
    auto msg = fbs::CreateMsg(fbb, fbs::Body::Heartbeat, 
	fbs::CreateHeartbeat(fbb, &id,
	  m_db->state(), dbState.save(fbb)).Union());
    fbb.Finish(msg);
  }

  send(saveHdr(fbb, this).constRef());

  ZdbDEBUG(m_db, ZtString{} << "hbSend()"
      "  self[ID:" << self->id() << " S:" << m_db->state() <<
      " SN:" << self->dbState().sn <<
      " N:" << self->dbState().count_() << "] " << self->dbState());
}

// refresh table state vector
void DB::dbStateRefresh()
{
  ZmAssert(invoked());

  DBState &dbState = m_self->dbState();
  dbState.updateSN(m_nextSN);
  all_([&dbState](AnyTable *table) {
    dbState.update(table->config().id, table->nextUN());
  });
}

// process received replicated record
void Cxn_::repRecordRcvd(ZmRef<const AnyBuf> buf)
{
  ZmAssert(m_db->invoked());

  if (!m_host) return;
  if (m_db->repStore()) return; // backing data store is replicated
  auto record = Zdb_::record(msg_(buf->hdr())); // caller verified msg
  if (!record) return;
  auto id = Zfb::Load::id(record->table());
  AnyTable *table = m_db->table(id);
  if (ZuUnlikely(!table)) return;
  ZdbDEBUG(m_db, (ZtString{} <<
      "repRecordRcvd(host=" << m_host->id() << ", " <<
      Record_Print{record, table}));
  m_db->replicated(
      m_host, id, record->un(), Zfb::Load::uint128(record->sn()));
  table->invoke([table, buf = ZuMv(buf)]() mutable {
    table->repRecordRcvd(ZuMv(buf));
  });
}

// process received replication commit
void Cxn_::repCommitRcvd(ZmRef<const AnyBuf> buf)
{
  ZmAssert(m_db->invoked());

  if (!m_host) return;
  auto commit = Zdb_::commit(msg_(buf->hdr())); // caller verified msg
  auto id = Zfb::Load::id(commit->table());
  AnyTable *table = m_db->table(id);
  if (ZuUnlikely(!table)) return;
  ZdbDEBUG(m_db, ZtString{} <<
      "repCommitRcvd(host=" << m_host->id() << ", " << commit->un() << ')');
  table->invoke([table, un = commit->un()]() mutable {
    table->repCommitRcvd(un);
  });
}

void DB::replicated(Host *host, ZuID dbID, UN un, SN sn)
{
  ZmAssert(invoked());

  bool updated = host->dbState().updateSN(sn + 1);
  updated = host->dbState().update(dbID, un + 1) || updated;
  if ((active() || host == m_next) && !updated) return;
  if (!m_prev) {
    m_prev = host;
    ZeLOG(Info, ([id = m_prev->id()](auto &s) {
      s << "Zdb host " << id << " is previous in line";
    }));
  }
}

AnyTable::AnyTable(DB *db, TableCf *cf) :
  m_db{db}, m_mx{db->mx()}, m_cf{cf},
  m_storeDLQ{
    ZmXRingParams{}.initial(StoreDLQ_BlkSize).increment(StoreDLQ_BlkSize)},
  m_cacheUN{new CacheUN{}},
  m_bufCacheUN{new BufCacheUN{}}
{
}

AnyTable::~AnyTable()
{
  // close(); // must be called while running
}

// telemetry
Zfb::Offset<ZvTelemetry::fbs::DBTable>
AnyTable::telemetry(ZvTelemetry::IOBuilder &fbb_, bool update) const
{
  using namespace Zfb;
  using namespace Zfb::Save;

  Zfb::Offset<String> path, name, thread, writeThread;
  if (!update) {
    name = str(fbb_, config().id);
    thread = str(fbb_, config().thread);
    writeThread = str(fbb_, config().writeThread);
  }
  ZvTelemetry::fbs::DBTableBuilder fbb{fbb_};
  if (!update) {
    fbb.add_name(name);
    fbb.add_thread(thread);
    fbb.add_writeThread(writeThread);
  }
  fbb.add_count(m_count.load_());
  {
    ZmCacheStats stats;
    cacheStats(stats);
    fbb.add_cacheLoads(stats.loads);
    fbb.add_cacheMisses(stats.misses);
    if (!update) fbb.add_cacheSize(stats.size);
  }
  if (!update) {
    fbb.add_cacheMode(
	static_cast<ZvTelemetry::fbs::CacheMode>(config().cacheMode));
    fbb.add_warmup(config().warmup);
  }
  return fbb.Finish();
}

// process inbound replication - record
void AnyTable::repRecordRcvd(ZmRef<const AnyBuf> buf)
{
  if (!m_open) return;

  ZmAssert(invoked());

  recover(record_(msg_(buf->hdr())));
  write(buf);
}

// process inbound replication - committed
void AnyTable::repCommitRcvd(UN un)
{
  if (!m_open) return;

  ZmAssert(invoked());

  commitSend(un);
  evictBuf(un);
}

// recover record
void AnyTable::recover(const fbs::Record *record)
{
  m_db->recoveredSN(Zfb::Load::uint128(record->sn()));
  recoveredUN(record->un());
  objRecover(record);
}

// outbound replication + persistency
void AnyTable::write(ZmRef<const AnyBuf> buf)
{
  ZmAssert(invoked());

  cacheBuf(buf);
  m_db->invoke([this, buf = ZuMv(buf)]() mutable {
    auto db = this->db();
    if (ZuLikely(db->active()) || !db->repStore()) {
      // leader, or follower without replicated data store - will
      // evict buf when write to data store is committed
      db->replicate(buf);
      writeRun([this, buf = ZuMv(buf)]() mutable { store(ZuMv(buf)); });
    } else {
      // follower with replicated data store - will evict buf
      // when leader subsequently sends commit, unless message is recovery
      auto msg = msg_(buf->hdr());
      auto un = record_(msg)->un();
      db->replicate(ZuMv(buf));
      if (msg->body_type() == fbs::Body::Recovery)
	invoke([this, un]() { evictBuf(un); });
    }
  });
}

// low-level internal write to backing data store
void AnyTable::store(ZmRef<const AnyBuf> buf)
{
  if (ZuUnlikely(!m_open)) return; // table is closing

  ZmAssert(writeInvoked());

  // DLQ draining in progress - just push onto the queue
  if (m_storeDLQ.count_()) {
    m_storeDLQ.push(ZuMv(buf));
    return;
  }

  store_(ZuMv(buf));
}
void AnyTable::retryStore_()
{
  if (!m_storeDLQ.count_()) return;
  store_(m_storeDLQ.shift());
}
void AnyTable::store_(ZmRef<const AnyBuf> buf)
{
  using namespace StoreTbl_;
  using namespace Zfb::Load;

  CommitFn commitFn = [this](ZmRef<const AnyBuf> buf, CommitResult result) {
    if (ZuUnlikely(result.is<Event>())) {
      ZeLogEvent(ZuMv(result).p<Event>());
      auto un = record_(msg_(buf->hdr()))->un();
      ZeLOG(Error, ([id = this->id(), un](auto &s) {
	s << "Zdb store of " << id << '/' << un << " failed";
      }));
      m_storeDLQ.unshift(ZuMv(buf)); // unshift, not push
      run([this]() { retryStore_(); }, Zm::now(db()->config().retryFreq));
      return;
    }
    {
      auto msg = msg_(buf->hdr());
      bool recovery = msg->body_type() == fbs::Body::Recovery;
      invoke([this, un = record_(msg)->un(), recovery]() {
	evictBuf(un);
	if (!recovery) commitSend(un);
      });
    }
    if (m_storeDLQ.count_()) writeRun([this]() { retryStore_(); });
  };

  m_storeTbl->write(ZuMv(buf), ZuMv(commitFn));
}

// cache buffer
void AnyTable::cacheBuf(ZmRef<const AnyBuf> buf)
{
  cacheBufUN(buf.mutablePtr());
  cacheBuf_(ZuMv(buf));
}

// evict buffer
void AnyTable::evictBuf(UN un)
{
  if (auto buf = evictBufUN(un))
    evictBuf_(static_cast<AnyBuf *>(buf));
}

// DB::open() iterates over tables, calling open()
// - each store table open calls table->opened(Store_::OpenResult) on success
template <typename L>
void AnyTable::open(Store *store, L l)
{
  ZmAssert(invoked());

  if (m_open) return;

  using namespace Store_;

  store->open(
    id(), objFields(), objKeyFields(), objSchema(),
    {this, [](AnyTable *table, MaxData data) {
      table->invoke([table, data = ZuMv(data)]() mutable {
	table->loadMaxima(ZuMv(data));
      });
    }},
    [this, l = ZuMv(l)](OpenResult result) mutable {
      invoke([l = ZuMv(l), result = ZuMv(result)]() mutable {
	l(ZuMv(result));
      });
    });
}

bool AnyTable::opened(Store_::OpenResult result)
{
  ZmAssert(invoked());
  ZmAssert(!m_open);

  using namespace Store_;

  if (!result.is<OpenData>()) {
    if (result.is<Event>())
      ZeLogEvent(ZuMv(result).p<Event>());
    return false;
  }
  const auto &data = result.p<OpenData>();
  m_storeTbl = data.storeTbl;
  m_count = data.count;
  m_db->recoveredSN(data.sn);
  recoveredUN(data.un);

  if (config().warmup) run([this]() { warmup(); });

  m_open = 1;
  return true;
}

void AnyTable::close()
{
  ZmAssert(invoked());

  if (!m_open) return;
  m_open = 0;

  ZmBlock<>{}([this](auto wake) {
    writeInvoke([this, wake = ZuMv(wake)]() mutable {
      m_storeTbl->close();
      wake();
    });
  });

  m_storeTbl = nullptr;
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
  if (ZuUnlikely(!m_table->allocUN(m_un))) { abort_(); return false; }
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
      ++m_vn;
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
