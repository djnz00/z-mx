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

using namespace Zdb_;

void ZdbEnv::init(ZdbEnvCf config, ZiMultiplex *mx, ZdbEnvHandler handler)
{
  Guard guard(m_lock);

  if (state() != ZdbHostState::Instantiated)
    throw ZtString{} << "ZdbEnv::init called out of order";

  config.dbTID = mx->tid(config.dbThread);
  if (!config.dbTID ||
      config.dbTID > mx->params().nThreads() ||
      config.dbTID == mx->rxThread() ||
      config.dbTID == mx->txThread())
    throw ZtString{} <<
      "Zdb dbThread misconfigured: " << config.dbThread;

  config.writeTID = mx->tid(config.writeThread);
  if (!config.writeTID ||
      config.writeTID > mx->params().nThreads() ||
      config.writeTID == mx->rxThread() ||
      config.writeTID == mx->txThread())
    throw ZtString{} <<
      "Zdb writeThread misconfigured: " << config.writeThread;

  m_cf = ZuMv(config);
  m_mx = mx;

  m_handler = ZuMv(handler);

  m_cxns = new CxnHash{};

  {
    auto i = m_cf.hostCfs.readIterator();
    while (auto node = i.iterate())
      m_hosts.addNode(new ZdbHosts::Node{this, &(node->data())});
  }
  m_self = m_hosts.findPtr(m_cf.hostID);
  if (!m_self)
    throw ZtString{} <<
      "Zdb own host ID " << m_cf.hostID << " not in hosts table";

  state(ZdbHostState::Initialized);
  guard.unlock();
  m_stateCond.broadcast();
}

void ZdbEnv::final()
{
  Guard guard(m_lock);

  if (state() != ZdbHostState::Initialized) {
    ZeLOG(Fatal, "ZdbEnv::final called out of order");
    return;
  }

  state(ZdbHostState::Instantiated);

  allDBs_([](Zdb *db) { db->final(); return true; });

  m_activeFn = ZmFn<>{};
  m_inactiveFn = ZmFn<>{};

  guard.unlock();
  m_stateCond.broadcast();
}

Zdb *ZdbEnv::db(ZuID id)
{
  ReadGuard guard(m_lock);
  return m_dbs.findPtr(id);
}

Zdb *ZdbEnv::db_(ZuID id, ZdbHandler handler)
{
  Guard guard(m_lock);
  if (state() != ZdbHostState::Initialized) {
    ZeLOG(Fatal, ZtString{} <<
	"ZdbEnv::add called out of order for DB " << id);
    return nullptr;
  }
  if (auto db = m_dbs.findPtr(id)) return db;
  auto cf = m_cf.dbCfs.find(id);
  if (!cf) m_cf.dbCfs.addNode(cf = new ZdbCfs::Node{id});
  auto db = new Zdbs::Node{this, &(cf->val())};
  db->Zdb::init(ZuMv(handler));
  m_dbs.addNode(db);
  return db;
}

bool ZdbEnv::spawn(ZmFn<> fn)
{
  if (!m_mx || !m_mx->running()) return false;
  m_mx->run(m_cf.dbTID, ZuMv(fn));
  return true;
}

void ZdbEnv::wake()
{
  if (!m_mx || !m_mx->running()) return false;
  m_mx->run(m_cf.dbTID, ZmFn<>{this, [](ZdbEnv *self) {
    self->stopped();
  }});
}

void ZdbEnv::checkpoint()
{
  if (!m_mx || !mx->running()) {
    ZeLOG(Fatal, "ZdbEnv::checkpoint called out of order");
    return;
  }
  m_mx->run(m_cf.dbTID, ZmFn<>{this, [](ZdbEnv *self) {
    self->allDBs_([](Zdb *db) { db->checkpoint(); return true; });
  }});
}

void ZdbEnv::start_()
{
  if (state() != ZdbHostState::Initialized) {
    ZeLOG(Fatal, "ZdbEnv::start_() called out of order");
    started(false);
    return;
  }

  ZeLOG(Info, "Zdb starting");

  if (!allDBs([](Zdb *db) { return db->open(); })) {
    allDBs([](Zdb *db) { db->close(); return true; });
    started(false);
    return;
  }

  dbStateRefresh();
  state(Stopped);
  stopReplication();
  state(Electing);

  if (m_nPeers = m_hosts.count_() - 1) {
    dbStateRefresh();
    m_mx->add(m_cf.dbTID, ZmFn<>::Member<&ZdbEnv::hbSend>::fn(this),
	m_hbSendTime = ZmTimeNow(), &m_hbSendTimer);
    m_mx->add(m_cf.dbTID, ZmFn<>::Member<&ZdbEnv::holdElection>::fn(this),
	ZmTimeNow((int)m_cf.electionTimeout), &m_electTimer);
  }
  if (m_hosts.count_() == 1) {
    holdElection();
    return;
  }

  listen();

  {
    // FIXME - iterate over hash, filtering on priority
    auto i = m_hosts.readIterator<ZmRBTreeLess>(m_cf.hostID);
    while (ZdbHost *host = i.iterate()) host->connect();
  }
}

void ZdbEnv::stop_()
{
  using namespace ZdbHostState;

  switch (state()) {
    case Active:
    case Inactive:
      break;
    case Activating:
    case Deactivating:
      return;
    default:
      ZeLOG(Fatal, "ZdbEnv::stop_ called out of order");
      return;
  }

  ZeLOG(Info, "Zdb stopping");

  state(Stopping);
  stopReplication();
  m_mx->del(&m_hbSendTimer);
  m_mx->del(&m_electTimer);

  // cancel reconnects
  {
    // FIXME - iterate over hash, filtering on priority
    auto i = m_hosts.readIterator<ZmRBTreeLess>(m_cf.hostID);
    while (ZdbHost *host = i.iterate()) host->cancelConnect();
  }

  stopListening();

  // close all connections (and wait for them to be disconnected)
  if (disconnectAll()) {
    Guard guard(m_lock);
    while (m_nPeers > 0) m_stateCond.wait();
    m_nPeers = 0; // paranoia
  }

  allDBs_([](Zdb *db) { db->close(); return true; });

  stopped(true);
}

bool ZdbEnv::disconnectAll()
{
  m_lock.lock();
  unsigned i = 0, n = m_cxns->count_();
  auto cxns = ZuAlloc(ZmRef<Cxn>, n);
  {
    ZmRef<Cxn> cxn;
    auto j = m_cxns->readIterator();
    while (i < n && (cxn = j.iterateKey()))
      if (cxn->up()) new (&cxns[i++]) ZmRef<Cxn>{ZuMv(cxn)};
  }
  m_lock.unlock();
  for (unsigned j = 0; j < i; j++) {
    cxns[j]->disconnect();
    cxns[j].~ZmRef<Cxn>();
  }
  return i;
}

void ZdbEnv::listen()
{
  m_mx->listen(
      ZiListenFn::Member<&ZdbEnv::listening>::fn(this),
      ZiFailFn::Member<&ZdbEnv::listenFailed>::fn(this),
      ZiConnectFn::Member<&ZdbEnv::accepted>::fn(this),
      m_self->ip(), m_self->port(), m_cf.nAccepts);
}

void ZdbEnv::listening(const ZiListenInfo &)
{
  ZeLOG(Info, ZtString{} << "Zdb listening on (" <<
      m_self->ip() << ':' << m_self->port() << ')');
}

void ZdbEnv::listenFailed(bool transient)
{
  ZtString warning;
  warning << "Zdb listen failed on (" <<
      m_self->ip() << ':' << m_self->port() << ')';
  if (transient && running()) {
    warning << " - retrying...";
    m_mx->add(m_cf.dbTID, ZmFn<>::Member<&ZdbEnv::listen>::fn(this),
      ZmTimeNow((int)m_cf.reconnectFreq));
  }
  ZeLOG(Warning, ZuMv(warning));
}

void ZdbEnv::stopListening()
{
  ZeLOG(Info, "Zdb stop listening");
  m_mx->stopListening(m_self->ip(), m_self->port());
}

void ZdbEnv::holdElection()
{
  bool won, appActive;
  ZdbHost *oldMaster;

  m_mx->del(&m_electTimer);

  if (state() != ZdbHostState::Electing) return;

  appActive = m_appActive;
  oldMaster = setMaster();
  if (won = m_master == m_self) {
    state(ZdbHostState::Activating);
    m_appActive = true;
    m_prev = 0;
    if (!m_nPeers)
      ZeLOG(Warning, "Zdb activating standalone");
    else
      hbSend_(); // announce new master
  } else {
    state(ZdbHostState::Deactivating);
    m_appActive = false;
  }

  if (won) {
    if (!appActive) {
      ZeLOG(Info, "Zdb ACTIVE");
      if (ZtString cmd = m_self->config().up) {
	if (oldMaster) cmd << ' ' << oldMaster->config().ip;
	ZeLOG(Info, ZtString{} << "Zdb invoking \"" << cmd << '\"');
	::system(cmd);
      }
      m_activeFn();
    }
  } else {
    if (appActive) {
      ZeLOG(Info, "Zdb INACTIVE");
      if (ZuString cmd = m_self->config().down) {
	ZeLOG(Info, ZtString{} << "Zdb invoking \"" << cmd << '\"');
	::system(cmd);
      }
      m_inactiveFn();
    }
  }

  if (state() == ZdbHostState::Activating) {
    state(won ? ZdbHostState::Active : ZdbHostState::Inactive);
    setNext();
  }

  if (Engine::state() == ZmEngineState::Stopping)
    m_mx->add(m_cf.dbTID, ZmFn<>::Member<&ZdbEnv::stop_1>::fn(this),
}

void ZdbEnv::deactivate()
{
  bool appActive;

  {
    using namespace ZdbHostState;

    Guard guard(m_lock);

retry:
    switch (state()) {
      case Instantiated:
      case Initialized:
      case Stopped:
      case Stopping:
	ZeLOG(Fatal, "ZdbEnv::inactive called out of order");
	return;
      case Deactivating:
      case Inactive:
	return;
      case Activating:
	do { m_stateCond.wait(); } while (state() == Activating);
	goto retry;
      default:
	break;
    }
    state(Deactivating);
    appActive = m_appActive;
    m_self->voted(false);
    setMaster();
    m_self->voted(true);
    m_appActive = false;
    guard.unlock();
    m_stateCond.broadcast();
  }

  if (appActive) {
    ZeLOG(Info, "Zdb INACTIVE");
    if (ZuString cmd = m_self->config().down) {
      ZeLOG(Info, ZtString{} << "Zdb invoking \"" << cmd << '\"');
      ::system(cmd);
    }
    m_inactiveFn();
  }

  {
    using namespace ZdbHostState;
    Guard guard(m_lock);
    state(Inactive);
    setNext();
    guard.unlock();
    m_stateCond.broadcast();
  }
}

void ZdbEnv::telemetry(Telemetry &data) const
{
  data.heartbeatFreq = m_cf.heartbeatFreq;
  data.heartbeatTimeout = m_cf.heartbeatTimeout;
  data.reconnectFreq = m_cf.reconnectFreq;
  data.electionTimeout = m_cf.electionTimeout;
  data.writeThread = m_cf.writeTID;

  ReadGuard guard(m_lock);
  data.nCxns = m_cxns ? m_cxns->count_() : 0;
  data.self = m_self->id();
  data.master = m_master ? m_master->id() : ZuID{};
  data.prev = m_prev ? m_prev->id() : ZuID{};
  data.next = m_next ? m_next->id() : ZuID{};
  data.nHosts = m_hosts.count_();
  data.nPeers = m_nPeers;
  data.nDBs = m_dbs.count_();
  {
    using namespace ZdbHostState;
    int state = this->state();
    data.state = state;
    data.active = state == Activating || state == Active;
  }
  data.recovering = m_recovering;
  data.replicating = !!m_nextCxn;
}

void ZdbHost::telemetry(Telemetry &data) const
{
  data.ip = m_cf->ip;
  data.id = m_cf->id;
  data.priority = m_cf->priority;
  data.port = m_cf->port;
  data.state = m_state;
  data.voted = m_voted;
}

void ZdbHost::reactivate()
{
  m_env->reactivate(static_cast<ZdbHost *>(this));
}

void ZdbEnv::reactivate(ZdbHost *host)
{
  if (ZmRef<Cxn> cxn = host->cxn()) cxn->hbSend();
  ZeLOG(Info, "Zdb dual active detected, remaining master");
  if (ZtString cmd = m_self->config().up) {
    cmd << ' ' << host->config().ip;
    ZeLOG(Info, ZtString{} << "Zdb invoking \'" << cmd << '\'');
    ::system(cmd);
  }
}

ZdbHost::ZdbHost(ZdbEnv *env, const ZdbHostCf *cf) :
  m_env{env},
  m_cf{cf},
  m_mx{env->mx()},
  m_dbState{env->dbCount()}
{
}

void ZdbHost::connect()
{
  if (!m_env->running() || m_cxn) return;

  ZeLOG(Info, ZtString{} << "Zdb connecting to host " << id() <<
      " (" << m_cf->ip << ':' << m_cf->port << ')');

  m_mx->connect(
      ZiConnectFn::Member<&ZdbHost::connected>::fn(this),
      ZiFailFn::Member<&ZdbHost::connectFailed>::fn(this),
      ZiIP(), 0, m_cf->ip, m_cf->port);
}

void ZdbHost::connectFailed(bool transient)
{
  ZtString warning;
  warning << "Zdb failed to connect to host " << id() <<
      " (" << m_cf->ip << ':' << m_cf->port << ')';
  if (transient && m_env->running()) {
    warning << " - retrying...";
    reconnect();
  }
  ZeLOG(Warning, ZuMv(warning));
}

ZiConnection *ZdbHost::connected(const ZiCxnInfo &ci)
{
  ZeLOG(Info, ZtString{} <<
      "Zdb connected to host " << id() <<
      " (" << ci.remoteIP << ':' << ci.remotePort << "): " <<
      ci.localIP << ':' << ci.localPort);

  if (!m_env->running()) return nullptr;

  return new Cxn(m_env, static_cast<ZdbHost *>(this), ci);
}

ZiConnection *ZdbEnv::accepted(const ZiCxnInfo &ci)
{
  ZeLOG(Info, ZtString{} << "Zdb accepted cxn on (" <<
      ci.localIP << ':' << ci.localPort << "): " <<
      ci.remoteIP << ':' << ci.remotePort);

  if (!running()) return nullptr;

  return new Cxn(this, nullptr, ci);
}

namespace Zdb_ {

Cxn::Cxn(ZdbEnv *env, ZdbHost *host, const ZiCxnInfo &ci) :
  ZiConnection(env->mx(), ci),
  m_env(env),
  m_host(host)
{
}

void Cxn::connected(ZiIOContext &io)
{
  if (!m_env->running()) { io.disconnect(); return; }

  m_env->connected(this);

  mx()->add(ZmFn<>::Member<&Cxn::hbTimeout>::fn(this),
      ZmTimeNow((int)m_env->config().heartbeatTimeout),
      ZmScheduler::Defer, &m_hbTimer);

  msgRead(io);
}

} // Zdb_

void ZdbEnv::connected(Cxn *cxn)
{
  m_cxns->add(cxn);

  Guard guard(m_lock);

  if (ZdbHost *host = cxn->host()) associate(cxn, host);

  hbSend_(cxn);
}

void ZdbEnv::associate(Cxn *cxn, ZuID hostID)
{
  Guard guard(m_lock);

  ZdbHost *host = m_hosts.find(hostID);

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

void ZdbEnv::associate(Cxn *cxn, ZdbHost *host)
{
  ZeLOG(Info, ZtString{} << "Zdb host " << host->id() << " CONNECTED");

  cxn->host(host);

  host->associate(cxn);

  host->voted(false);
}

void ZdbHost::associate(Cxn *cxn)
{
  Guard guard(m_lock);

  if (ZuUnlikely(m_cxn && m_cxn.ptr() != cxn)) {
    m_cxn->host(nullptr);
    m_cxn->mx()->add(ZmFn<>::Member<&ZiConnection::disconnect>::fn(m_cxn));
  }
  m_cxn = cxn;
}

void ZdbHost::reconnect()
{
  m_mx->add(m_cf.dbTID, ZmFn<>::Member<&ZdbHost::reconnect2>::fn(this),
      ZmTimeNow((int)m_env->config().reconnectFreq),
      ZmScheduler::Defer, &m_connectTimer);
}

void ZdbHost::reconnect2()
{
  connect();
}

void ZdbHost::cancelConnect()
{
  m_mx->del(&m_connectTimer);
}

namespace Zdb_ {

void Cxn::hbTimeout()
{
  ZeLOG(Info, ZtString{} << "Zdb heartbeat timeout on host " <<
      ZuBoxed(m_host ? (int)m_host->id() : -1) << " (" <<
      info().remoteIP << ':' << info().remotePort << ')');
  disconnect();
}

void Cxn::disconnected()
{
  ZeLOG(Info, ZtString{} << "Zdb disconnected from host " <<
      ZuBoxed(m_host ? (int)m_host->id() : -1) << " (" <<
      info().remoteIP << ':' << info().remotePort << ')');
  mx()->del(&m_hbTimer);
  m_env->disconnected(this);
}

} // Zdb_

void ZdbEnv::disconnected(Cxn *cxn)
{
  delete m_cxns->del(cxn);

  if (cxn == m_nextCxn) m_nextCxn = 0;

  ZdbHost *host = cxn->host();

  if (!host || host->cxn() != cxn) return;

  {
    Guard guard(m_lock);

    if (state() == ZdbHostState::Stopping && --m_nPeers <= 0) {
      guard.unlock();
      m_stateCond.broadcast();
    }
  }

  host->disconnected();
  ZeLOG(Info, ZtString{} << "Zdb host " << host->id() << " DISCONNECTED");

  {
    using namespace ZdbHostState;

    Guard guard(m_lock);

    host->state(Instantiated);
    host->voted(false);

    switch (state()) {
      case Activating:
      case Active:
      case Deactivating:
      case Inactive:
	break;
      default:
	goto ret;
    }

    if (host == m_prev) m_prev = 0;

    if (host == m_master) {
  retry:
      switch (state()) {
	case Deactivating:
	  do { m_stateCond.wait(); } while (state() == Deactivating);
	  goto retry;
	case Inactive:
	  state(Electing);
	  guard.unlock();
	  m_stateCond.broadcast();
	  holdElection();
	  break;
      }
      goto ret;
    }

    if (host == m_next) setNext();
  }

ret:
  if (running() && host->id() < m_cf.hostID) host->reconnect();
}

void ZdbHost::disconnected()
{
  m_cxn = nullptr;
}

ZdbHost *ZdbEnv::setMaster()
{
  ZdbHost *oldMaster = m_master;
  dbStateRefresh();
  m_master = nullptr;
  m_nPeers = 0;
  {
    auto i = m_hosts.readIterator();

    ZdbDEBUG(this, ZtString{} << "setMaster()\n" << 
	" self=" << m_self << '\n' <<
	" prev=" << m_prev << '\n' <<
	" next=" << m_next << '\n' <<
	" recovering=" << m_recovering << " replicating=" << !!m_nextCxn);
    while (ZdbHost *host = i.iterate()) {
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

void ZdbEnv::setNext(ZdbHost *host)
{
  m_next = host;
  m_recovering = false;
  m_nextCxn = 0;
  if (m_next) {
    m_mx->run(m_cf.writeTID,
	ZmFn<>{this, [](ZdbEnv *env) { env->replicating(); }});
    startReplication();
  } else {
    m_mx->run(m_cf.writeTID,
	ZmFn<>{this, [](ZdbEnv *env) { env->standalone(); }});
  }
}

void ZdbEnv::replicating()
{
  m_standalone = false;
}

void ZdbEnv::standalone()
{
  m_standalone = true;
  allDBs([](Zdb *db) { db->standalone(); return true; });
}

void ZdbEnv::setNext()
{
  m_next = nullptr;
  {
    auto i = m_hosts.readIterator();
    ZdbDEBUG(this, ZtString{} << "setNext()\n" <<
	" self=" << m_self << '\n' <<
	" master=" << m_master << '\n' <<
	" prev=" << m_prev << '\n' <<
	" next=" << m_next << '\n' <<
	" recovering=" << m_recovering << " replicating=" << !!m_nextCxn);
    while (ZdbHost *host = i.iterate()) {
      if (host != m_self && host != m_prev && host->voted() &&
	  m_self->cmp(host) >= 0 && (!m_next || host->cmp(m_next) > 0))
	m_next = host;
      ZdbDEBUG(this, ZtString{} <<
	  " host=" << host << '\n' <<
	  " next=" << m_next);
    }
  }
  m_recovering = false;
  m_nextCxn = 0;
  if (m_next) startReplication();
}

void ZdbEnv::startReplication()
{
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
    m_mx->run(m_mx->txThread(), ZmFn<>::Member<&ZdbEnv::recSend>::fn(this));
  }
}

void ZdbEnv::stopReplication()
{
  m_master = nullptr;
  m_prev = nullptr;
  m_next = nullptr;
  m_recovering = false;
  m_nextCxn = nullptr;
  {
    auto i = m_hosts.readIterator();
    while (ZdbHost *host = i.iterate()) host->voted(false);
  }
  m_self->voted(true);
  m_nPeers = 1;
}

namespace Zdb_ {

void Cxn::msgRead(ZiIOContext &io)
{
  ZiRx::recv<Buf>(io,
      [](const ZiIOContext &, const Buf *buf) -> int {
	return loadHdr(buf);
      },
      [](const ZiIOContext &io, const Buf *buf, unsigned) -> int {
	return static_cast<Cxn *>(io.cxn)->msgRead2(buf);
      });
}
int Cxn::msgRead2(const Buf *buf)
{
  return verifyHdr(buf, [this](const Hdr *hdr, const Buf *buf) -> int {
    // using namespace Zfb;
    auto msg = Zdb_::msg(hdr);
    if (ZuUnlikely(!msg)) return -1;
    bool ok = false;
    switch (static_cast<int>(msg->body_type())) {
      case fbs::Body_HB:
	ok = hbRcvd(hb_(msg));
	break;
      case fbs::Body_Rep:
      case fbs::Body_Rec: {
	auto record = record_(msg);
	if (auto db = repRcvd_(record))
	  ok = repRcvd(db, record, buf);
      } break;
      case fbs::Body_Gap: {
	auto gap = gap_(msg);
	if (auto db = repRcvd_(gap))
	  ok = repRcvd(db, gap, buf);
      } break;
      default:
	ZeLOG(Error, ZtString{} <<
	    "Zdb received unknown message from host " <<
	    ZuBoxed(m_host ? (int)m_host->id() : -1)); // FIXME
	break;
    }
    if (!ok) return -1;
    mx()->add(ZmFn<>::Member<&Cxn::hbTimeout>::fn(this),
	ZmTimeNow((int)m_env->config().heartbeatTimeout), &m_hbTimer);
    return static_cast<unsigned>(hdr->length);
  });
}

bool Cxn::hbRcvd(const fbs::Heartbeat *hb)
{
  if (!m_host) m_env->associate(this, hb->hostID());

  if (!m_host) return false;

  m_env->hbRcvd(m_host, hb);
  return true;
}

} // Zdb_

// process received heartbeat
void ZdbEnv::hbRcvd(ZdbHost *host, const fbs::Heartbeat *hb)
{
  {
    Guard guard(m_lock);

    ZdbDEBUG(this, ZtString{} << "hbDataRcvd()\n" << 
	  " host=" << host << '\n' <<
	  " self=" << m_self << '\n' <<
	  " master=" << m_master << '\n' <<
	  " prev=" << m_prev << '\n' <<
	  " next=" << m_next << '\n' <<
	  " recovering=" << m_recovering <<
	  " replicating=" << !!m_nextCxn);

    host->state(hb->state());
    auto &dbState = host->dbState();
    Zfb::Load::all(hb->dbState(),
	[&dbState](unsigned, const fbs::DBState *state) {
      auto id = Zfb::Load::id(&(state->db()));
      ZdbRN rn = state->rn();
      dbState.update(id, rn);
    });

    using namespace ZdbHostState;

    int state = this->state();

    switch (state) {
      case Electing:
	if (!host->voted()) {
	  host->voted(true);
	  if (--m_nPeers <= 0) {
	    guard.unlock();
	    m_mx->add(m_cf.dbTID, ZmFn<>::Member<&ZdbEnv::holdElection>::fn(this));
	  }
	}
	return;
      case Activating:
      case Active:
      case Deactivating:
      case Inactive:
	break;
      default:
	return;
    }

    // check for duplicate master (dual active)
    switch (state) {
      case Activating:
      case Active:
	switch (host->state()) {
	  case Activating:
	  case Active:
	    vote(host);
	    if (host->cmp(m_self) > 0)
	      m_mx->add(m_cf.dbTID, ZmFn<>::Member<&ZdbEnv::deactivate>::fn(this));
	    else
	      m_mx->add(m_cf.dbTID, ZmFn<>::Member<&ZdbHost::reactivate>::fn(host));
	    return;
	}
    }

    // check for new host joining after election
    if (!host->voted()) {
      ++m_nPeers;
      vote(host);
    }
  }

  // trigger DB vacuuming outside of the env lock scope
  Zfb::Load::all(hb->dbState(),
      [this](unsigned, const fbs::DBState *state) {
    auto id = Zfb::Load::id(&(state->db()));
    ZdbRN rn = state->rn();
    if (auto db = m_dbs.findPtr(id))
      if (db->ack(rn)) db->vacuum();
  });
}

// check if new host should be our next in line
void ZdbEnv::vote(ZdbHost *host)
{
  host->voted(true);
  dbStateRefresh();
  if (host != m_next && host != m_prev &&
      m_self->cmp(host) >= 0 && (!m_next || host->cmp(m_next) > 0))
    setNext(host);
}

// send recovery message to next-in-line (continues repeatedly until completed)
void ZdbEnv::recSend()
{
  Guard guard(m_lock);
  if (!m_self) {
    ZeLOG(Fatal, "ZdbEnv::recSend called out of order");
    return;
  }
  if (!m_recovering) return;
  ZmRef<Cxn> cxn = m_nextCxn;
  if (!cxn) return;
  ZdbRN gap = ZdbNullRN;
  {
    auto i = m_recover.readIterator();
    while (auto state = i.iterate()) {
      ZuID id = state->template p<0>();
      if (auto endState = m_recoverEnd.find(id))
	if (auto db = m_dbs.findPtr(id)) {
	  auto &recRN = const_cast<DBState::T *>(state)->template p<1>();
	  auto endRN = endState->template p<1>();
	  while (recRN < endRN) {
	    ZdbRN rn = recRN++;
	    ZmRef<Buf> buf = db->recovery(rn);
	    if (buf) {
	      if (gap != ZdbNullRN) {
		cxn->repSend(db->gap(gap, rn - gap));
		// gap = ZdbNullRN;
	      }
	      cxn->repSend(ZuMv(buf));
	      return;
	    }
	    if (gap == ZdbNullRN) gap = rn;
	  }
	}
    }
  }
  m_recovering = false;
}

// send replication message to next-in-line
void ZdbEnv::repSend(ZmRef<Buf> buf)
{
  if (ZmRef<Cxn> cxn = m_nextCxn) cxn->repSend(ZuMv(buf));
}

namespace Zdb_ {

// send replication message (directed)
void Cxn::repSend(ZmRef<Buf> buf)
{
  if (recovery_(msg_(buf->hdr())))
    ZiTx::send(this, ZuMv(buf), [](ZmRef<Buf> buf) {
      auto db = reinterpret_cast<Zdb *>(buf->owner);
      auto env = db->env();
      auto mx = env->mx();
      mx->run(mx->txThread(), ZmFn<>::Member<&ZdbEnv::recSend>::fn(env));
    });
  else
    ZiTx::send(this, ZuMv(buf));
}

} // Zdb_

// broadcast heartbeat
void ZdbEnv::hbSend()
{
  Guard guard(m_lock);
  hbSend_();
  m_mx->add(m_cf.dbTID, ZmFn<>::Member<&ZdbEnv::hbSend>::fn(this),
    m_hbSendTime += (time_t)m_cf.heartbeatFreq,
    ZmScheduler::Defer, &m_hbSendTimer);
}

// send heartbeat (broadcast)
void ZdbEnv::hbSend_()
{
  if (!m_self) {
    ZeLOG(Fatal, "ZdbEnv::hbSend_ called out of order");
    return;
  }
  dbStateRefresh();
  ZmRef<Cxn> cxn;
  unsigned i = 0, n = m_cxns->count_();
  auto cxns = ZuAlloc(ZmRef<Cxn>, n);
  {
    ZmRef<Cxn> cxn;
    auto j = m_cxns->readIterator();
    while (i < n && (cxn = j.iterateKey()))
      if (cxn->up()) new (&cxns[i++]) ZmRef<Cxn>{ZuMv(cxn)};
  }
  m_lock.unlock();
  for (unsigned j = 0; j < i; j++) {
    cxns[j]->hbSend();
    cxns[j].~ZmRef<Cxn>();
  }
}

// send heartbeat (directed)
void ZdbEnv::hbSend_(Cxn *cxn)
{
  if (!m_self) {
    ZeLOG(Fatal, "ZdbEnv::hbSend_ called out of order");
    return;
  }
  dbStateRefresh();
  cxn->hbSend();
}

namespace Zdb_ {

// send heartbeat on a specific connection
void Cxn::hbSend()
{
  ZdbHost *self = m_env->self();
  if (ZuUnlikely(!self)) {
    ZeLOG(Fatal, "Cxn::hbSend called out of order");
    return;
  }
  Zfb::IOBuilder<Buf> fbb;
  {
    const auto &dbState = self->dbState();
    auto msg = fbs::CreateMsg(fbb, fbs::Body_HB, 
	fbs::CreateHeartbeat(fbb,
	  self->id(), m_env->state(), dbState.save(fbb)).Union());
    fbb.Finish(msg);
  }
  ZiTx::send(this, saveHdr(fbb));
  ZdbDEBUG(m_env, ZtString{} << "hbSend()"
      "  self[ID:" << self->id() << " S:" << m_env->state() <<
      " N:" << self->dbState().count_() << "] " << self->dbState());
}

} // Zdb_

// refresh db state vector
void ZdbEnv::dbStateRefresh()
{
  if (!m_self) {
    ZeLOG(Fatal, "ZdbEnv::dbStateRefresh called out of order");
    return;
  }
  DBState &dbState = m_self->dbState();
  allDBs_([&dbState](const Zdb *db) {
    dbState.update(db->config().id, db->nextRN());
    return true;
  });
}

namespace Zdb_ {

// process received replicated record
bool Cxn::repRcvd(Zdb *db, const fbs::Record *record, const Buf *buf)
{
  m_env->replicated(db, m_host, record);
  db->replicated(record, buf);
  return true;
}

// process received replicated gap
bool Cxn::repRcvd(Zdb *db, const fbs::Gap *gap, const Buf *buf)
{
  m_env->replicated(db, m_host, gap);
  db->replicated(gap, buf);
  return true;
}

} // Zdb_

// process replication - host state
void ZdbEnv::replicated(Zdb *db, ZdbHost *host, const fbs::Record *record)
{
  auto id = db->config().id;
  ZdbRN rn = record->rn() + 1;
  ZdbDEBUG(this, ZtString{} << "replicated(" << id << ", " <<
      host << ", Record{" << record->rn() << "})");
  replicated_(host, id, rn);
}

void ZdbEnv::replicated(Zdb *db, ZdbHost *host, const fbs::Gap *gap)
{
  auto id = db->config().id;
  ZdbRN rn = gap->rn() + gap->count();
  ZdbDEBUG(this, ZtString{} << "replicated(" << id << ", " <<
      host << ", Gap{" << gap->rn() << ", " << gap->count() << "})");
  replicated_(host, id, rn);
}

void ZdbEnv::replicated_(ZdbHost *host, ZuID id, ZdbRN rn)
{
  Guard guard(m_lock);
  bool updated = host->dbState().update(id, rn);
  if ((active() || host == m_next) && !updated) return;
  if (!m_prev) {
    m_prev = host;
    ZeLOG(Info,
	ZtString{} << "Zdb host " << m_prev->id() << " is previous in line");
  }
}

// process replication - database
void Zdb::replicated(const fbs::Record *record, const Buf *rxBuf)
{
  if (auto buf = replicateFwd(rxBuf)) {
    ZmRef<ZdbAnyObject> object = recover_(record, buf);
    if (object && m_handler.recoverFn) m_handler.recoverFn(object, buf);
    if (buf) m_env->write(ZuMv(buf));
  }
}

void Zdb::replicated(const fbs::Gap *gap, const Buf *rxBuf)
{
  ZdbRN rn = gap->rn();
  ZmRef<Buf> buf = replicateFwd(rxBuf);
  {
    Guard guard(m_pushLock);
    if (rn != m_nextRN) return;
    m_nextRN = rn + gap->count();
  }
  if (buf) m_env->write(ZuMv(buf));
}

Zdb::Zdb(ZdbEnv *env, ZdbCf *cf) :
  m_env{env}, m_cf{cf},
  m_path{ZiFile::append(env->config().path, cf->id)},
  m_cache{new Cache{}}, m_cacheSize{m_cache->size()},
  m_writeCache{new BufCache{}},
  m_files{new FileCache{}},
  m_fileCacheSize{m_files->size()},
  m_indexBlks{new IndexBlkCache{}},
  m_indexBlkCacheSize{m_indexBlks->size()}
{
}

Zdb::~Zdb()
{
  close();
  m_lru.clean();
  m_fileLRU.clean();
}

void Zdb::init(ZdbHandler handler)
{
  m_handler = ZuMv(handler);
}

void Zdb::final()
{
  m_handler = ZdbHandler{};
}

// telemetry
void Zdb::telemetry(Telemetry &data) const
{
  data.path = m_path;
  data.name = m_cf->id;
  data.cacheMode = m_cf->cacheMode;
  data.warmUp = m_cf->warmUp;
  {
    ReadGuard guard(m_pushLock);
    data.minRN = m_minRN;
    data.nextRN = m_nextRN;
  }
  {
    ReadGuard guard(m_lock);
    data.cacheSize = m_cacheSize;
    data.cacheLoads = m_cacheLoads;
    data.cacheMisses = m_cacheMisses;
    data.fileCacheSize = m_fileCacheSize;
    data.indexBlkCacheSize = m_indexBlkCacheSize;
    data.fileLoads = m_fileLoads;
    data.fileMisses = m_fileMisses;
    data.indexBlkLoads = m_indexBlkLoads;
    data.indexBlkMisses = m_indexBlkMisses;
  }
}

// load object from buffer
ZmRef<ZdbAnyObject> Zdb::load(const fbs::Record *record) // cacheLock held
{
  auto prevRN = record->prevRN();
  auto seqLenOp = record->seqLenOp();
  switch (SeqLenOp::op(seqLenOp)) {
    case Op::Delete:
    case Op::Purge:
      return nullptr;
  }
  ZmRef<ZdbAnyObject> object;
  if (prevRN != ZdbNullRN) object = cacheDel_(prevRN);
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
void Zdb::save(Zfb::Builder &fbb, ZdbAnyObject *object)
{
  m_handler.saveFn(fbb, object->ptr_());
}

bool Zdb::recover()
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

void Zdb::recover(File *file) // fileLock held
{
  if (!file->allocated()) return;
  if (file->deleted() >= fileRecs()) { delFile(file); return; }
  ZdbRN rn = (file->id())<<fileShift();
  ZmRef<IndexBlk> indexBlk;
  int first = file->first();
  if (ZuUnlikely(first < 0)) return;
  int last = file->last();
  if (ZuUnlikely(last < 0)) return; // should never happen
  rn += first;
  for (int j = first; j <= last; j++, rn++) {
    if (!file->exists(j)) continue;
    auto indexBlkID = rn>>indexShift();
    ZmRef<ZdbAnyObject> object;
    if (!indexBlk || indexBlk->id != indexBlkID)
      indexBlk = file->readIndexBlk(indexBlkID);
    if (!indexBlk) return; // I/O error on file, logged within readIndexBlk
    object = this->recover(
	FileRec{file, indexBlk, static_cast<unsigned>(rn & indexMask())});
    if (object && m_handler.recoverFn) m_handler.recoverFn(object, buf);
  }
}

void Zdb::recover(const FileRec &rec) // cacheLock held
{
  if (auto buf = read_(rec)) {
    auto record = record_(msg_(buf->hdr()));
    auto rn = record->rn();
    {
      Guard guard(m_pushLock);
      if (m_minRN < rn)m_minRN = rn;
      if (m_nextRN <= rn) m_nextRN = rn + 1;
    }
    object = recover_(record, {});
    if (object && m_handler.recoverFn) m_handler.recoverFn(object, buf);
  }
}

bool Zdb::recoverRN(ZdbRN rn)
{
  Guard guard(m_pushLock);
  if (rn != m_nextRN) return nullptr;
  m_nextRN = rn + 1;
  if (rn < m_minRN) m_minRN = rn;
}

ZmRef<ZdbAnyObject> Zdb::recover_(
    const fbs::Record *record, ZmRef<Buf> buf)
{
  ZdbRN rn = record->rn();
  ZdbRN prevRN = record->prevRN();

  ZmRef<ZdbAnyObject> object;
  auto seqLenOp = record->seqLenOp();

  Guard guard(m_cacheLock);

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
  if (buf) m_writeCache->addNode(buf);
  if (object) cache(object);
  return object;
}

namespace Zdb_ {

void File::reset() // fileLock held
{
  // FIXME use file lock
  m_flags = 0;
  m_allocated = m_deleted = 0;
  m_bitmap.zero();
  memset(&m_superBlk.data[0], 0, sizeof(FileSuperBlk));
}

bool File::scan() // fileLock held
{
  // FIXME use file lock
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
  m_allocated = 0;
  m_deleted = 0;
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
	  if (offset == ZdbDeleted) {
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

bool File::sync_() // fileLock held
{
  // FIXME - use file lock
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

bool File::sync() // fileLock held
{
  // FIXME use file lock
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

} // Zdb_

bool Zdb::open()
{
  if (!recover()) return false;

  if (m_cf->warmUp) {
    Guard guard(m_lock);
    rn2file(m_nextRN, true);
    ZmRef<ZdbAnyObject>{m_handler.ctorFn(this)};
  }

  return true;
}

void Zdb::close()
{
  Guard guard(m_cacheLock);
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

void Zdb::checkpoint()
{
  m_env->mx()->run(
      m_env->config().writeTID,
      ZmFn<>{this, [](Zdb *db) { db->checkpoint_(); }});
}

bool Zdb::checkpoint_()
{
  Guard guard(m_cacheLock);
  bool ok = true;
  {
    auto i = m_indexBlkLRU.readIterator();
    while (IndexBlk *blk = static_cast<IndexBlk *>(i.iterateNode()))
      if (File *file =
	  getFile(blk->id>>(fileShift() - indexShift()), false))
	ok &= file->writeIndexBlk(blk);
  }
  {
    auto i = m_fileLRU.readIterator();
    while (File *file = static_cast<File *>(i.iterateNode()))
      ok &= file->sync_();
  }
  return ok;
}

ZmRef<ZdbAnyObject> Zdb::placeholder()
{
  return m_handler.ctorFn(this);
}

ZmRef<ZdbAnyObject> Zdb::push()
{
  if (ZuUnlikely(!m_env->active())) {
    ZeLOG(Error, ZtString{} <<
	"Zdb inactive application attempted push on DBID " << m_cf->id);
    return nullptr;
  }
  return push_();
}

ZmRef<ZdbAnyObject> Zdb::push_()
{
  ZdbRN rn;
  {
    Guard guard(m_lock);
    rn = m_nextRN++;
    if (rn < m_minRN) m_minRN = rn;
  }
  ZmRef<ZdbAnyObject> object = m_handler.ctorFn(this);
  object->init(rn);
  return object;
}

ZmRef<ZdbAnyObject> Zdb::push(ZdbRN rn)
{
  if (ZuUnlikely(!m_env->active())) {
    ZeLOG(Error, ZtString{} <<
	"Zdb inactive application attempted push on DBID " << m_cf->id);
    return nullptr;
  }
  if (ZuUnlikely(rn == ZdbNullRN)) return push_();
  return push_(rn);
}

ZmRef<ZdbAnyObject> Zdb::push_(ZdbRN rn)
{
  {
    Guard guard(m_lock);
    if (ZuUnlikely(m_nextRN != rn)) return nullptr;
    m_nextRN = rn + 1;
    if (rn < m_minRN) m_minRN = rn;
  }
  ZmRef<ZdbAnyObject> object = m_handler.ctorFn(this);
  object->init(rn);
  return object;
}

ZmRef<ZdbAnyObject> Zdb::get(ZdbRN rn)
{
  ZmRef<ZdbAnyObject> object;
  {
    Guard guard(m_cacheLock);
    if (ZuUnlikely(rn < m_minRN || rn >= m_nextRN)) return nullptr;
    ++m_cacheLoads;
    if (ZuLikely(object = m_cache->find(rn))) {
      if (m_cf->cacheMode != ZdbCacheMode::All)
	m_lru.pushNode(m_lru.delNode(object));
      return object;
    }
    ++m_cacheMisses;
    {
      ZmRef<Buf> buf = m_writeCache->find(rn);
      if (!buf) {
	FileRec rec = rn2file(rn, false);
	if (rec) buf = read_(rec);
      }
      if (buf) object = load(record_(msg_(buf->hdr())));
    }
    if (ZuUnlikely(!object)) return nullptr;
    cache(object);
  }
  return object;
}

ZmRef<ZdbAnyObject> Zdb::getUpdate(ZdbRN rn)
{
  Guard guard(m_lock);
  // FIXME - locking
  if (ZuUnlikely(rn < m_minRN || rn >= m_nextRN)) return nullptr;
  return get__(rn);
}

ZmRef<ZdbAnyObject> Zdb::get__(ZdbRN rn) // cacheLock held
{
  ZmRef<ZdbAnyObject> object;
  ++m_cacheLoads;
  if (ZuLikely(object = m_cache->find(rn))) return object;
  ++m_cacheMisses;
  {
    // ++m_writeCacheLoads;
    ZmRef<Buf> buf = m_writeCache->find(rn);
    if (!buf) {
      // ++m_writeCacheMisses;
      FileRec rec = rn2file(rn, false);
      if (rec) buf = read_(rec);
    }
    if (buf) object = load(record_(msg_(buf->hdr())));
  }
  return object;
}

void Zdb::cache(ZdbAnyObject *object) // cacheLock held
{
  if (m_cf->cacheMode != ZdbCacheMode::All &&
      m_cache->count_() >= m_cacheSize) {
    auto lru_ = m_lru.shiftNode();
    if (ZuLikely(lru_)) {
      ZdbAnyObject *lru = static_cast<ZdbAnyObject *>(lru_);
      m_cache->del(lru->rn());
    }
  }
  cache_(object);
}

void Zdb::cache_(ZdbAnyObject *object) // cacheLock held
{
  m_cache->addNode(object);
  if (m_cf->cacheMode != ZdbCacheMode::All) m_lru.pushNode(object);
}

ZmRef<ZdbAnyObject> Zdb::cacheDel_(ZdbRN rn) // cacheLock held
{
  if (ZmRef<CacheNode> object = m_cache->del(rn)) {
    if (m_cf->cacheMode != ZdbCacheMode::All) m_lru.delNode(object);
    return object;
  }
  return nullptr;
}

void Zdb::cacheDel_(ZdbAnyObject *object) // cacheLock held
{
  m_cache->delNode(object);
  if (m_cf->cacheMode != ZdbCacheMode::All) m_lru.delNode(object);
}

bool Zdb::update(ZdbAnyObject *object)
{
  Guard guard(m_cacheLock);
  if (ZuUnlikely(object->seqLen() == SeqLenOp::maxSeqLen())) return false;
  cacheDel_(object);
  // FIXME - locking
  ZdbRN rn = m_nextRN++;
  object->push(rn);
  if (rn < m_minRN) m_minRN = rn;
  return true;
}

bool Zdb::update(ZdbAnyObject *object, ZdbRN rn)
{
  if (ZuUnlikely(rn == ZdbNullRN)) return update(object);
  Guard guard(m_cacheLock);
  if (ZuUnlikely(object->seqLen() == SeqLenOp::maxSeqLen())) return false;
  // FIXME - locking
  if (ZuUnlikely(m_nextRN != rn)) return false;
  m_nextRN = rn + 1;
  if (rn < m_minRN) m_minRN = rn;
  cacheDel_(object);
  object->push(rn);
  return true;
}

ZmRef<ZdbAnyObject> Zdb::update(ZdbRN prevRN)
{
  Guard guard(m_cacheLock);
  if (ZuUnlikely(prevRN < m_minRN || prevRN >= m_nextRN)) return nullptr;
  ZmRef<ZdbAnyObject> object = get__(prevRN);
  if (ZuUnlikely(!object || object->deleted())) return nullptr;
  if (ZuUnlikely(object->seqLen() == SeqLenOp::maxSeqLen())) return nullptr;
  cacheDel_(object);
  // FIXME - locking
  ZdbRN rn = m_nextRN++;
  if (rn < m_minRN) m_minRN = rn;
  object->push(rn);
  return object;
}

ZmRef<ZdbAnyObject> Zdb::update(ZdbRN prevRN, ZdbRN rn)
{
  Guard guard(m_cacheLock);
  if (ZuUnlikely(prevRN < m_minRN || prevRN >= m_nextRN)) return nullptr;
  if (ZuUnlikely(m_nextRN != rn)) return nullptr;
  ZmRef<ZdbAnyObject> object = get__(prevRN);
  if (ZuUnlikely(!object || object->deleted())) return nullptr;
  if (ZuUnlikely(object->seqLen() == SeqLenOp::maxSeqLen())) return nullptr;
  // FIXME - locking
  m_nextRN = rn + 1;
  if (rn < m_minRN) m_minRN = rn;
  cacheDel_(object);
  object->push(rn);
  return object;
}

// commits push() / update()
void Zdb::put(ZdbAnyObject *object)
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
    m_writeCache->addNode(buf);
    object->put(); // resets seqLen to 1
    cache(object);
  }
  m_env->write(ZuMv(buf));
}

// commits appended update()
void Zdb::append(ZdbAnyObject *object)
{
  ZmRef<Buf> buf;
  {
    Guard guard(m_cacheLock);
    object->commit(Op::Append);
    buf = object->replicate(fbs::Body_Rep);
    m_writeCache->addNode(buf);
    cache(object);
  }
  m_env->write(ZuMv(buf));
}

// commits delete following push() / update()
void Zdb::del(ZdbAnyObject *object)
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
    m_writeCache->addNode(buf);
  }
  m_env->write(ZuMv(buf));
}

// aborts push() / update()
void Zdb::abort(ZdbAnyObject *object)
{
  Guard guard(m_lock);
  del__(object->rn());
  object->abort();
  if (object->rn() != ZdbNullRN) cache(object);
}

// prepare replication data for sending & writing to disk
ZmRef<Buf> ZdbAnyObject::replicate(int type) // cacheLock held
{
  ZdbDEBUG(m_db->env(),
      ZtString{} << "ZdbAnyObject::replicate(" << type << ')');
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
ZmRef<Buf> Zdb::replicateFwd(const Buf *rxBuf)
{
  ZmRef<Buf> buf = new Buf{this};
  auto data = buf->ensure(rxBuf->length);
  if (!data) return nullptr;
  memcpy(data, rxBuf->data(), rxBuf->length);
  return buf;
}

// prepare recovery data for sending
ZmRef<Buf> Zdb::recovery(ZdbRN rn)
{
  Guard guard(m_cacheLock);
  // recover from outbound replication buffer cache
  if (ZmRef<Buf> txBuf = m_writeCache->find(rn)) {
    auto record = record_(msg_(txBuf->hdr()));
    auto repData = Zfb::Load::bytes(record->data());
    Zfb::IOBuilder<Buf> fbb;
    Zfb::Offset<Zfb::Vector<uint8_t>> data;
    if (repData) {
      auto ptr = Zfb::Save::extend(fbb, repData.length(), data);
      if (!data.IsNull() && ptr)
	memcpy(ptr, repData.data(), repData.length());
    }
    auto id = Zfb::Save::id(m_cf->id);
    auto msg = fbs::CreateMsg(fbb, fbs::Body_Rec,
	fbs::CreateRecord(fbb, &id,
	  record->rn(), record->prevRN(), record->seqLenOp(), data).Union());
    fbb.Finish(msg);
    return saveHdr(fbb, this);
  }
  // recover from object cache
  if (ZmRef<ZdbAnyObject> object = m_cache->find(rn))
    return object->replicate(fbs::Body_Rec);
  // recover from file
  if (FileRec rec = rn2file(rn, false))
    return read_(rec);
  // unallocated | deleted
  return nullptr;
}

// prepare run-length encoded gap for sending
ZmRef<Buf> Zdb::gap(ZdbRN rn, uint64_t count)
{
  Zfb::IOBuilder<Buf> fbb;
  auto id = Zfb::Save::id(m_cf->id);
  auto msg = fbs::CreateMsg(fbb, fbs::Body_Gap,
      fbs::CreateGap(fbb, &id, rn, count).Union());
  fbb.Finish(msg);
  return saveHdr(fbb, this);
}

void Zdb::purge(ZdbRN minRN)
{
  Zfb::IOBuilder<Buf> fbb;
  ZdbRN rn;
  {
    Guard guard(m_lock);
    rn = m_nextRN++;
  }
  {
    auto id = Zfb::Save::id(m_cf->id);
    auto msg = fbs::CreateMsg(fbb, fbs::Body_Rep,
	fbs::CreateRecord(fbb, &id,
	  rn, minRN, SeqLenOp::mk(1, Op::Purge), {}).Union());
    fbb.Finish(msg);
  }
  ZmRef<Buf> buf = saveHdr(fbb, this);
  {
    Guard guard(m_lock);
    m_deletes.add(rn, DeleteOp{minRN, SeqLenOp::mk(1, Op::Purge)});
    m_writeCache->addNode(buf);
  }
  m_env->write(ZuMv(buf));
}

void ZdbEnv::write(ZmRef<Buf> buf)
{
  if (reinterpret_cast<Zdb *>(buf->owner)->config().repMode)
    this->repSend(buf);				// send to replica
  m_mx->run(m_cf.writeTID,
      ZmFn<>::mvFn(ZuMv(buf), [](ZmRef<Buf> buf) {
	auto db = reinterpret_cast<Zdb *>(buf->owner);
	db->write2(ZuMv(buf));
      }));
}

void Zdb::write2(ZmRef<Buf> buf)
{
  if (!m_cf->repMode) m_env->repSend(buf);	// send to replica
  bool vacuum;
  {
    Guard guard(m_lock);
    vacuum = write_(buf);
    m_writeCache->delNode(buf);
  }
  if (vacuum) this->vacuum();
  // if (m_handler.logFn) m_handler.logFn(buf);
}

// creates/caches file and index block as needed
FileRec Zdb::rn2file(ZdbRN rn, bool write) // cacheLock held
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

File *Zdb::getFile(uint64_t id, bool create) // cacheLock held
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
      lru->sync();
      m_files->delNode(lru);
    }
  m_files->addNode(ZuMv(file));
  m_fileLRU.pushNode(filePtr);
  if (id > m_lastFile) m_lastFile = id;
  return filePtr;
}

ZmRef<File> Zdb::openFile(uint64_t id, bool create)
{
  Guard guard(m_fileLock);
  return openFile_(fileName(dirName(id), id), id, create);
}

ZmRef<File> Zdb::openFile_(
    const ZiFile::Path &name, uint64_t id, bool create) // fileLock held
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

void Zdb::delFile(File *file) // fileLock held
{
  bool lastFile;
  uint64_t id = file->id();
  if (m_files->delNode(file)) m_fileLRU.delNode(file);
  lastFile = id == m_lastFile;
  if (ZuUnlikely(lastFile)) getFile(id + 1, true); // FIXME - locking
  file->close();
  ZiFile::remove(fileName(id));
}

IndexBlk *Zdb::getIndexBlk(File *file, uint64_t id, bool create) // cacheLock held
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
      m_indexBlks->delNode(lru);
    }
  m_indexBlks->addNode(ZuMv(indexBlk));
  m_indexBlkLRU.pushNode(indexBlkPtr);
  return indexBlkPtr;
}

namespace Zdb_ {

ZmRef<IndexBlk> File::readIndexBlk(uint64_t id) // cacheLock held
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

ZmRef<IndexBlk> File::writeIndexBlk(uint64_t id) // cacheLock held
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

bool File::readIndexBlk(IndexBlk *indexBlk) // cacheLock held
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

bool File::writeIndexBlk(IndexBlk *indexBlk) // cacheLock held
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

} // Zdb_

// read individual record from disk
ZmRef<Buf> Zdb::read_(const FileRec &rec) // cacheLock held
{
  const auto &index = rec.index();
  if (ZuUnlikely(!index.offset || index.offset == ZdbDeleted)) {
    ZeLOG(Error, ZtString{} << "Zdb internal error on DBID " << m_cf->id <<
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
  auto id = Zfb::Save::id(m_cf->id);
  auto msg = fbs::CreateMsg(fbb, fbs::Body_Rec,
      fbs::CreateRecord(fbb,
	&id, trlr.rn, trlr.prevRN, trlr.seqLenOp, data).Union());
  fbb.Finish(msg);
  return saveHdr(fbb, this);
}

// write individual record to disk
// - updates the file bitmap, index block and appends the record on disk
bool Zdb::write_(const Buf *buf) // cacheLock held
{
  auto record = record_(msg_(buf->hdr()));
  if (ZuUnlikely(!record)) return false;
  ZdbRN rn = record->rn();
  auto data = Zfb::Load::bytes(record->data());
  {
    FileRec rec = rn2file(rn, true);
    if (!rec) return false;
    auto &index = rec.index();
    File *file = rec.file();
    // FIXME - move below into File member function, use file lock
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
  return m_env->isStandalone() && ack_(rn + 1);
}

bool Zdb::ack(ZdbRN rn)
{
  // FIXME - locking
  Guard guard(m_lock);
  return ack_(rn);
}

  // FIXME - locking
bool Zdb::ack_(ZdbRN rn) // locked
{
  // FIXME - locking
  if (ZuUnlikely(rn <= m_minRN)) return false;
  if (ZuUnlikely(rn > m_nextRN)) rn = m_nextRN;
  if (m_vacuumRN == ZdbNullRN) {
    auto deleteRN = m_deletes.minimumKey();
    if (ZuUnlikely(deleteRN == ZdbNullRN)) return false;
    if (rn <= deleteRN) return false;
    m_vacuumRN = rn;
    return true;
  }
  if (rn > m_vacuumRN) m_vacuumRN = rn;
  return false;
}

// vacuuming is performed in small batches, yielding between each batch

void Zdb::vacuum()
{
  m_env->mx()->run(m_env->config().writeTID,
      ZmFn<>{this, [](Zdb *db) { db->vacuum_(); }});
}

void Zdb::vacuum_()
{
  {
    // FIXME - locking
    Guard guard(m_lock);
    if (ZuUnlikely(m_vacuumRN == ZdbNullRN)) return;
    auto i = m_deletes.iterator(); // FIXME - do not iterate, use minimum
    unsigned j = 0;
    ZuPair<int, ZdbRN> outcome;
    while (auto node = i.iterate()) { // FIXME - do not iterate, use minimum
      if (node->key() >= m_vacuumRN) break;
      outcome = del_(node->val(), m_cf->vacuumBatch - j);
      if (outcome.p<0>() < 0) goto again1;
      i.del(node);
      if ((j += outcome.p<0>()) >= m_cf->vacuumBatch) goto again2;
    }
    m_vacuumRN = ZdbNullRN;
    return;

again1:
    if (outcome.p<1>() != ZdbNullRN) // split long sequence
      m_deletes.add(outcome.p<1>(),
	  DeleteOp{outcome.p<1>(), SeqLenOp::mk(-outcome.p<0>(), Op::Delete)});
  }

again2:
  vacuum();
}

// del_() returns a {int, ZdbRN} pair:
// 0, ZdbNullRN - nothing to do
// +ve, ZdbNullRN - all done (work was within batch size), continue
// -ve, ZdbNullRN - work exceeded batch size, some work done, re-attempt
// -ve, rn - work exceeded batch size, need to split sequence at rn
//   (remaining sequence length is encoded in the negative return code)
ZuPair<int, ZdbRN> Zdb::del_(
    const DeleteOp &deleteOp, unsigned maxBatchSize)
{
  ZdbRN rn = deleteOp.rn;

  switch (SeqLenOp::op(deleteOp.seqLenOp)) {
    default:
      return {0, ZdbNullRN};
    case Op::Put:
      break;
    case Op::Delete:
      break;
    case Op::Purge: {
      ZdbRN minRN = m_minRN;
      unsigned i = 0;
      if (minRN < rn) {
	do {
	  del__(minRN++);
	} while (++i < maxBatchSize && minRN < rn);
	m_minRN = minRN;
      }
      if (i >= maxBatchSize) return {-1, ZdbNullRN}; // re-attempt
      return {static_cast<int>(i), ZdbNullRN};
    }
  }

  auto seqLen = SeqLenOp::seqLen(deleteOp.seqLenOp);

  if (ZuUnlikely(!seqLen)) return {0, ZdbNullRN};

  auto batchSize = seqLen;
  if (batchSize > maxBatchSize) batchSize = maxBatchSize;

  auto batch = ZuAlloc(ZdbRN, batchSize);
  if (!batch) return {0, ZdbNullRN};

  // fill the batch
  unsigned i = 0;
  do {
    batch[i++] = rn;
    rn = del_prevRN(rn);
  } while (i < batchSize && rn != ZdbNullRN);

  // sequence is longer than the batch size, need to split it
  if (rn != ZdbNullRN)
    return {-static_cast<int>(seqLen - batchSize), rn};

  // delete the oldest batched RNs in reverse order (oldest first)
  for (unsigned j = i; j-- > 0; ) del__(batch[j]);

  // entire sequence was deleted
  return {static_cast<int>(i), ZdbNullRN};
}

// obtain prevRN for a record pending deletion
ZdbRN Zdb::del_prevRN(ZdbRN rn)
{
  if (ZuUnlikely(rn < m_minRN || rn >= m_nextRN)) return ZdbNullRN;
  if (ZmRef<ZdbAnyObject> object = cacheDel_(rn))
    return object->prevRN();
  FileRec rec = rn2file(rn, false);
  if (!rec) return ZdbNullRN;
  const auto &index = rec.index();
  if (ZuUnlikely(!index.offset || index.offset == ZdbDeleted)) {
    ZeLOG(Error, ZtString{} << "Zdb internal error on DBID " << m_cf->id <<
	" bitmap inconsistent with index for RN " << rec.rn());
    return ZdbNullRN;
  }
  auto offset = index.offset + index.length;
  File *file = rec.file();
  FileRecTrlr trlr;
  int r;
  ZeError e;
  if (ZuUnlikely((r = file->pread(offset,
	    &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
    fileRdError_(file, offset, r, e);
    return ZdbNullRN;
  }
  uint32_t magic = trlr.magic;
  if (magic != ZdbCommitted) return ZdbNullRN;
  return trlr.prevRN;
}

// delete individual record
void Zdb::del__(ZdbRN rn)
{
  FileRec rec = rn2file(rn, false);
  if (!rec) return;
  rec.index().offset = ZdbDeleted;
  if (rec.file()->del(rn & fileRecMask()))
    delFile(rec.file());
}

// transition to standalone, trigger vacuuming
void Zdb::standalone()
{
  if (ack(ZdbMaxRN)) vacuum();
}

// disk read error
void Zdb::fileRdError_(
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
void Zdb::fileWrError_(File *file, ZiFile::Offset off, ZeError e)
{
  ZeLOG(Error, ZtString{} <<
      "Zdb pwrite() failed on \"" << fileName(file->id()) <<
      "\" at offset " << ZuBoxed(off) <<  ": " << e);
}
