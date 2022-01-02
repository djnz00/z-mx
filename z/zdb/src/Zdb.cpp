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

#include <zlib/ZmAlloc.hpp>

#include <zlib/ZtBitWindow.hpp>

#include <zlib/ZiDir.hpp>
#include <zlib/ZiRx.hpp>
#include <zlib/ZiTx.hpp>

#include <assert.h>
#include <errno.h>

ZdbEnv::ZdbEnv() :
  m_mx(0), m_stateCond(m_lock),
  m_appActive(false), m_self(0), m_master(0), m_prev(0), m_next(0),
  m_nextCxn(0), m_recovering(false), m_nPeers(0)
{
}

ZdbEnv::~ZdbEnv()
{
}

void ZdbEnv::init(ZdbEnvConfig config, ZiMultiplex *mx,
    ZmFn<> activeFn, ZmFn<> inactiveFn)
{
  Guard guard(m_lock);

  if (state() != ZdbHostState::Instantiated)
    throw ZtString{} << "ZdbEnv::init called out of order";

  config.writeTID = mx->tid(config.writeThread);
  if (!config.writeTID ||
      config.writeTID > mx->params().nThreads() ||
      config.writeTID == mx->rxThread() ||
      config.writeTID == mx->txThread())
    throw ZtString{} <<
      "Zdb writeThread misconfigured: " << config.writeThread;

  m_cf = ZuMv(config);
  m_mx = mx;
  m_cxns = new CxnHash(m_cf.cxnHash);
  m_activeFn = activeFn;
  m_inactiveFn = inactiveFn;

  {
    auto i = m_cf.hostCfs.readIterator();
    while (auto node = i.iterate())
      m_hosts.add(new ZdbHosts::Node{this, node});
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

void ZdbEnv::db(ZuID id)
{
  ReadGuard guard(m_lock);
  return m_dbs.findPtr(id);
}

void ZdbEnv::db_(ZuID id, ZdbHandler handler)
{
  Guard guard(m_lock);
  if (state() != ZdbHostState::Initialized) {
    ZeLOG(Fatal, ZtString{} <<
	"ZdbEnv::add called out of order for DB " << name);
    return;
  }
  if (auto db = m_dbs.findPtr(id)) return db;
  auto cf = m_cf.dbCfs.find(id);
  if (!cf) m_cf.dbCfs.addNode(cf = new ZdbCfs::Node{id});
  auto db = new Zdbs::Node{this, cf, ZuMv(handler)};
  m_dbs.addNode(db);
  return db;
}

bool ZdbEnv::open()
{
  Guard guard(m_lock);
  if (state() != ZdbHostState::Initialized) {
    ZeLOG(Fatal, "ZdbEnv::open called out of order");
    return false;
  }
  if (!allDBs([&openFailed](Zdb *db) {
    if (!db->open()) return false;
    return true;
  })) {
    allDBs([](Zdb *db) { db->close(); return true; });
    return false;
  }
  dbStateRefresh_();
  state(ZdbHostState::Stopped);
  guard.unlock();
  m_stateCond.broadcast();
  return true;
}

void ZdbEnv::close()
{
  Guard guard(m_lock);
  if (state() != ZdbHostState::Stopped) {
    ZeLOG(Fatal, "ZdbEnv::close called out of order");
    return;
  }
  {
    unsigned i, n = m_dbs.length();
    for (i = 0; i < n; i++) if (ZdbAny *db = m_dbs[i]) db->close();
  }
  state(ZdbHostState::Initialized);
  guard.unlock();
  m_stateCond.broadcast();
}

void ZdbEnv::checkpoint()
{
  Guard guard(m_lock);
  switch (state()) {
    case ZdbHostState::Instantiated:
    case ZdbHostState::Initialized:
      ZeLOG(Fatal, "ZdbEnv::checkpoint called out of order");
      return;
  }
  {
    unsigned i, n = m_dbs.length();
    for (i = 0; i < n; i++) if (ZdbAny *db = m_dbs[i]) db->checkpoint();
  }
}

void ZdbEnv::start()
{
  {
    using namespace ZdbHostState;

    Guard guard(m_lock);

retry:
    switch (state()) {
      case Instantiated:
      case Initialized:
	ZeLOG(Fatal, "ZdbEnv::start called out of order");
	return;
      case Stopped:
	break;
      case Stopping:
	do { m_stateCond.wait(); } while (state() == Stopping);
	goto retry;
      default:
	return;
    }

    state(Electing);
    stopReplication();
    if (m_nPeers = m_hosts.count() - 1) {
      dbStateRefresh_();
      m_mx->add(ZmFn<>::Member<&ZdbEnv::hbSend>::fn(this),
	  m_hbSendTime = ZmTimeNow(), &m_hbSendTimer);
      m_mx->add(ZmFn<>::Member<&ZdbEnv::holdElection>::fn(this),
	  ZmTimeNow((int)m_cf.electionTimeout), &m_electTimer);
    }
    guard.unlock();
    m_stateCond.broadcast();
  }

  ZeLOG(Info, "Zdb starting");

  if (m_hosts.count() == 1) {
    holdElection();
    return;
  }

  listen();

  {
    auto i = m_hosts.readIterator<ZmRBTreeLess>(m_cf.hostID);
    while (ZdbHost *host = i.iterateKey()) host->connect();
  }
}

void ZdbEnv::stop()
{
  ZeLOG(Info, "Zdb stopping");

  {
    using namespace ZdbHostState;

    Guard guard(m_lock);

retry:
    switch (state()) {
      case Instantiated:
      case Initialized:
	ZeLOG(Fatal, "ZdbEnv::stop called out of order");
	return;
      case Stopped:
	return;
      case Activating:
	do { m_stateCond.wait(); } while (state() == Activating);
	goto retry;
      case Deactivating:
	do { m_stateCond.wait(); } while (state() == Deactivating);
	goto retry;
      case Stopping:
	do { m_stateCond.wait(); } while (state() == Stopping);
	goto retry;
      default:
	break;
    }

    state(Stopping);
    stopReplication();
    guard.unlock();
    m_mx->del(&m_hbSendTimer);
    m_mx->del(&m_electTimer);
    m_stateCond.broadcast();
  }

  // cancel reconnects
  {
    auto i = m_hosts.readIterator<ZmRBTreeLess>(m_cf.hostID);
    while (ZdbHost *host = i.iterateKey()) host->cancelConnect();
  }

  stopListening();

  // close all connections (and wait for them to be disconnected)
  if (disconnectAll()) {
    Guard guard(m_lock);
    while (m_nPeers > 0) m_stateCond.wait();
    m_nPeers = 0; // paranoia
  }

  // final clean up
  {
    Guard guard(m_lock);

    state(ZdbHostState::Stopped);
    guard.unlock();
    m_stateCond.broadcast();
  }
}

bool ZdbEnv::disconnectAll()
{
  m_lock.lock();
  unsigned n = m_cxns->count_();
  ZtArray<ZmRef<Zdb_Cxn> > cxns(n);
  unsigned i = 0;
  {
    ZmRef<Zdb_Cxn> cxn;
    CxnHash::ReadIterator j(*m_cxns);
    while (i < n && (cxn = j.iterateKey())) if (cxn->up()) ++i, cxns.push(cxn);
  }
  m_lock.unlock();
  for (unsigned j = 0; j < i; j++)
    cxns[j]->disconnect();
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
    m_mx->add(ZmFn<>::Member<&ZdbEnv::listen>::fn(this),
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

  {
    Guard guard(m_lock);
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
	hbSend_(0); // announce new master
    } else {
      state(ZdbHostState::Deactivating);
      m_appActive = false;
    }
    guard.unlock();
    m_stateCond.broadcast();
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

  {
    Guard guard(m_lock);
    state(won ? ZdbHostState::Active : ZdbHostState::Inactive);
    setNext();
    guard.unlock();
    m_stateCond.broadcast();
  }
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
  data.master = m_master ? m_master->id() : 0;
  data.prev = m_prev ? m_prev->id() : 0;
  data.next = m_next ? m_next->id() : 0;
  data.nHosts = m_hosts.count();
  data.nPeers = m_nPeers;
  data.nDBs = m_dbs.length();
  {
    using namespace ZdbHostState;
    int state = this->state();
    data.state = state;
    data.active = state == Activating || state == Active;
  }
  data.recovering = m_recovering;
  data.replicating = !!m_nextCxn;
}

void ZdbHost_::telemetry(Telemetry &data) const
{
  data.ip = m_cf->ip;
  data.id = m_cf->id;
  data.priority = m_cf->priority;
  data.port = m_cf->port;
  data.state = m_state;
  data.voted = m_voted;
}

void ZdbHost_::reactivate()
{
  m_env->reactivate(this);
}

void ZdbEnv::reactivate(ZdbHost *host)
{
  if (ZmRef<Zdb_Cxn> cxn = host->cxn()) cxn->hbSend();
  ZeLOG(Info, "Zdb dual active detected, remaining master");
  if (ZtString cmd = m_self->config().up) {
    cmd << ' ' << host->config().ip;
    ZeLOG(Info, ZtString{} << "Zdb invoking \'" << cmd << '\'');
    ::system(cmd);
  }
}

ZdbHost_::ZdbHost(ZdbEnv *env, const ZdbHostConfig *cf) :
  m_env{env},
  m_cf{cf},
  m_mx{env->mx()},
  m_dbState{env->dbCount()}
{
}

void ZdbHost_::connect()
{
  if (!m_env->running() || m_cxn) return;

  ZeLOG(Info, ZtString{} << "Zdb connecting to host " << id() <<
      " (" << m_cf->ip << ':' << m_cf->port << ')');

  m_mx->connect(
      ZiConnectFn::Member<&ZdbHost_::connected>::fn(this),
      ZiFailFn::Member<&ZdbHost_::connectFailed>::fn(this),
      ZiIP(), 0, m_cf->ip, m_cf->port);
}

void ZdbHost_::connectFailed(bool transient)
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

ZiConnection *ZdbHost_::connected(const ZiCxnInfo &ci)
{
  ZeLOG(Info, ZtString{} <<
      "Zdb connected to host " << id() <<
      " (" << ci.remoteIP << ':' << ci.remotePort << "): " <<
      ci.localIP << ':' << ci.localPort);

  if (!m_env->running()) return 0;

  return new Zdb_Cxn(m_env, this, ci);
}

ZiConnection *ZdbEnv::accepted(const ZiCxnInfo &ci)
{
  ZeLOG(Info, ZtString{} << "Zdb accepted cxn on (" <<
      ci.localIP << ':' << ci.localPort << "): " <<
      ci.remoteIP << ':' << ci.remotePort);

  if (!running()) return 0;

  return new Zdb_Cxn(this, 0, ci);
}

Zdb_Cxn::Zdb_Cxn(ZdbEnv *env, ZdbHost *host, const ZiCxnInfo &ci) :
  ZiConnection(env->mx(), ci),
  m_env(env),
  m_host(host)
{
  // memset(&m_hbSendHdr, 0, sizeof(Zdb_Msg_Hdr));
}

void Zdb_Cxn::connected(ZiIOContext &io)
{
  if (!m_env->running()) { io.disconnect(); return; }

  m_env->connected(this);

  mx()->add(ZmFn<>::Member<&Zdb_Cxn::hbTimeout>::fn(this),
      ZmTimeNow((int)m_env->config().heartbeatTimeout),
      ZmScheduler::Defer, &m_hbTimer);

  msgRead(io);
}

void ZdbEnv::connected(Zdb_Cxn *cxn)
{
  m_cxns->add(cxn);

  Guard guard(m_lock);

  if (ZdbHost *host = cxn->host()) associate(cxn, host);

  hbSend_(cxn);
}

void ZdbEnv::associate(Zdb_Cxn *cxn, int hostID)
{
  Guard guard(m_lock);

  ZdbHost *host = m_hosts.findKey(hostID);

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

void ZdbEnv::associate(Zdb_Cxn *cxn, ZdbHost *host)
{
  ZeLOG(Info, ZtString{} << "Zdb host " << host->id() << " CONNECTED");

  cxn->host(host);

  host->associate(cxn);

  host->voted(false);
}

void ZdbHost_::associate(Zdb_Cxn *cxn)
{
  Guard guard(m_lock);

  if (ZuUnlikely(m_cxn && m_cxn.ptr() != cxn)) {
    m_cxn->host(0);
    m_cxn->mx()->add(ZmFn<>::Member<&ZiConnection::disconnect>::fn(m_cxn));
  }
  m_cxn = cxn;
}

void ZdbHost_::reconnect()
{
  m_mx->add(ZmFn<>::Member<&ZdbHost_::reconnect2>::fn(this),
      ZmTimeNow((int)m_env->config().reconnectFreq),
      ZmScheduler::Defer, &m_connectTimer);
}

void ZdbHost_::reconnect2()
{
  connect();
}

void ZdbHost_::cancelConnect()
{
  m_mx->del(&m_connectTimer);
}

void Zdb_Cxn::hbTimeout()
{
  ZeLOG(Info, ZtString{} << "Zdb heartbeat timeout on host " <<
      ZuBoxed(m_host ? (int)m_host->id() : -1) << " (" <<
      info().remoteIP << ':' << info().remotePort << ')');
  disconnect();
}

void Zdb_Cxn::disconnected()
{
  ZeLOG(Info, ZtString{} << "Zdb disconnected from host " <<
      ZuBoxed(m_host ? (int)m_host->id() : -1) << " (" <<
      info().remoteIP << ':' << info().remotePort << ')');
  mx()->del(&m_hbTimer);
  m_env->disconnected(this);
}

void ZdbEnv::disconnected(Zdb_Cxn *cxn)
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

void ZdbHost_::disconnected()
{
  m_cxn = nullptr;
}

ZdbHost *ZdbEnv::setMaster()
{
  ZdbHost *oldMaster = m_master;
  dbStateRefresh_();
  m_master = nullptr;
  m_nPeers = 0;
  {
    auto i = m_hosts.readIterator();
    ZmRef<ZdbHost> host;

    ZdbDEBUG(this, ZtString{} << "setMaster()\n" << 
	" self:" << m_self << '\n' <<
	" prev:" << m_prev << '\n' <<
	" next:" << m_next << '\n' <<
	" recovering:" << m_recovering << " replicating:" << !!m_nextCxn);
    while (host = i.iterateKey()) {
      ZdbDEBUG(this, ZtString{} <<
	  " host:" << *host << '\n' <<
	  " master:" << m_master);
      if (host->voted()) {
	if (host != m_self) ++m_nPeers;
	if (!m_master) { m_master = host; continue; }
	int diff = host->cmp(m_master);
	if (ZuCmp<int>::null(diff)) {
	  m_master = nullptr;
	  break;
	} else if (cmp > 0)
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
  if (m_next) startReplication();
}

void ZdbEnv::setNext()
{
  m_next = 0;
  {
    auto i = m_hosts.readIterator();
    ZdbDEBUG(this, ZtString{} << "setNext()\n" <<
	" self:" << m_self << '\n' <<
	" master:" << m_master << '\n' <<
	" prev:" << m_prev << '\n' <<
	" next:" << m_next << '\n' <<
	" recovering:" << m_recovering << " replicating:" << !!m_nextCxn);
    while (ZdbHost *host = i.iterateKey()) {
      if (host != m_self && host != m_prev && host->voted() &&
	  m_self->cmp(host) >= 0 && (!m_next || host->cmp(m_next) > 0))
	m_next = host;
      ZdbDEBUG(this, ZtString{} <<
	  " host:" << host << '\n' <<
	  " next:" << m_next);
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
  dbStateRefresh_();		// must be called after m_nextCxn assignment
  ZdbDEBUG(this, ZtString{} << "startReplication()\n" <<
      " self:" << m_self << '\n' <<
      " master:" << m_master << '\n' <<
      " prev:" << m_prev << '\n' <<
      " next:" << m_next << '\n' <<
      " recovering:" << m_recovering << " replicating:" << !!m_nextCxn);
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
  m_nextCxn = 0;
  {
    auto i = m_hosts.readIterator();
    while (ZdbHost *host = i.iterateKey()) host->voted(false);
  }
  m_self->voted(true);
  m_nPeers = 1;
}

void Zdb_Cxn::msgRead(ZiIOContext &io)
{
  ZiRx::recv<ZdbRxBuf>(io,
      ZdbNet::loadHdr,
      [this](const ZmRef<ZdbRxBuf> &buf) -> int {
	using namespace Zfb;
	using namespace ZdbNet;
	auto hdr = reinterpret_cast<const Hdr *>(buf->data());
	{
	  Verifier verifier(hdr->data(), hdr->length);
	  if (!fbs::VerifyMsgBuffer(verifier)) return -1;
	}
	auto msg = fbs::GetMsg(hdr->data());
	bool ok = false;
	switch (static_cast<int>(msg->body_type())) {
	  case fbs::Body_HB:
	    ok = hbRcvd(static_cast<const fbs::Heartbeat *>(msg->body()));
	    break;
	  case fbs::Body_Rep:
	    ok = repRcvd(buf, static_cast<const fbs::Object *>(msg->body()));
	    break;
	  case fbs::Body_Rec:
	    ok = recRcvd(static_cast<const fbs::Object *>(msg->body()));
	    break;
	  default:
	    ZeLOG(Error, ZtString{} <<
		"Zdb received garbled message from host " <<
		ZuBoxed(m_host ? (int)m_host->id() : -1));
	    break;
	}
	if (!ok) return -1;
	mx()->add(ZmFn<>::Member<&Zdb_Cxn::hbTimeout>::fn(this),
	    ZmTimeNow((int)m_env->config().heartbeatTimeout), &m_hbTimer);
	return buf->length;
      });
}

void Zdb_Cxn::hbRcvd(const ZdbNet::fbs::Heartbeat *hb)
{
  unsigned dbCount = m_env->dbCount();
  auto dbState = hb->dbState();

  if (dbCount != dbState->size()) {
    ZeLOG(Fatal, ZtString{} <<
	"Zdb inconsistent remote configuration detected (local dbCount " <<
	dbCount << " != host " << hb->hostID() <<
	" dbCount " << dbState->size() << ')');
    io.disconnect();
    return;
  }

  if (!m_host) m_env->associate(this, hb->hostID());

  if (!m_host) {
    io.disconnect();
    return;
  }

  m_env->hbDataRcvd(m_host, hb);
}

// process received heartbeat
void ZdbEnv::hbDataRcvd(ZdbHost *host, const ZdbNet::fbs::Heartbeat *hb)
{
  Guard guard(m_lock);

  ZdbDEBUG(this, ZtString{} << "hbDataRcvd()\n" << 
	" host:" << host << '\n' <<
	" self:" << m_self << '\n' <<
	" master:" << m_master << '\n' <<
	" prev:" << m_prev << '\n' <<
	" next:" << m_next << '\n' <<
	" recovering:" << m_recovering <<
	" replicating:" << !!m_nextCxn);

  host->state(hb->state());
  auto &dbState = host->dbState();
  Zfb::Load::all(hb->dbState(), [&dbState](unsigned i, ZdbRN rn) {
    dbState[i] = rn;
  });

  using namespace ZdbHostState;

  int state = this->state();

  switch (state) {
    case Electing:
      if (!host->voted()) {
	host->voted(true);
	if (--m_nPeers <= 0) {
	  guard.unlock();
	  m_mx->add(ZmFn<>::Member<&ZdbEnv::holdElection>::fn(this));
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
	    m_mx->add(ZmFn<>::Member<&ZdbEnv::deactivate>::fn(this));
	  else
	    m_mx->add(ZmFn<>::Member<&ZdbHost_::reactivate>::fn(ZmMkRef(host)));
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
void ZdbEnv::vote(ZdbHost *host)
{
  host->voted(true);
  dbStateRefresh_();
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
  ZmRef<Zdb_Cxn> cxn = m_nextCxn;
  if (!cxn) return;
  unsigned i, n = m_dbs.length();
  if (n != m_recover.length() || n != m_recoverEnd.length()) {
    ZeLOG(Fatal, ZtString{} <<
	"ZdbEnv::recSend encountered inconsistent dbCount (local dbCount " <<
	n << " != one of " <<
	m_recover.length() << ", "  << m_recoverEnd.length() << ')');
    return;
  }
  for (i = 0; i < n; i++)
    if (Zdb *db = m_dbs[i])
      if (m_recover[i] < m_recoverEnd[i]) {
	ZmRef<ZdbAnyObject> object;
	ZdbRN rn = m_recover[i]++;
	ZmRef<ZdbBuf> buf; // FIXME - optimize this to re-use buffer on a cache miss
	if (object = db->get_(rn)) { // FIXME - should not call get_() here
	  buf = object->replicate(Zdb_Msg::Rec);
	  cxn->repSend(ZuMv(buf));
	}
	return;
      }
  m_recovering = false;
}

// send replication message to next-in-line
void ZdbEnv::repSend(ZmRef<ZdbBuf> buf)
{
  if (ZmRef<Zdb_Cxn> cxn = m_nextCxn)
    cxn->send(ZiIOFn::Member<&ZdbBuf::send>::fn(ZuMv(buf)));
}

// send replication message (directed)
void Zdb_Cxn::repSend(ZmRef<ZdbBuf> buf)
{
  this->send(ZiIOFn::Member<&ZdbBuf::send>::fn(ZuMv(buf)));
}

// prepare replication data for sending & writing to disk
ZmRef<ZdbBuf> ZdbAnyObject::replicate(int type)
{
  ZdbDEBUG(m_db->env(), ZtString{} << "ZdbAnyObject::replicate(" <<
      type << ')');
  Zfb::IOBuilder<ZdbBuf> fbb;
  Zfb::Offset<Zfb::Vector<uint8_t>> data = 0;
  if (!deleted)
    if (auto ptr = this->ptr_()) {
      db->save(fbb, this);
      data = Zfb::Save::nest(fbb);
    }
  using namespace ZdbNet;
  {
    auto id = Zfb::Save::id(db->id());
    auto msg = fbs::CreateMsg(fbb, type,
	fbs::CreateObject(fbb, &id, rn, prevRN, deleted, data));
    fbb.Finish(msg);
  }
  ZdbNet::saveHdr(fbb);
  ZmRef<ZdbBuf> buf = fbb.buf();
  buf->owner = db;
  return buf;
}

// forward replication data
ZmRef<ZdbBuf> Zdb::replicateFwd(const ZmRef<ZdbRxBuf> &rxBuf)
{
  ZdbDEBUG(m_db->env(), ZtString{} << "ZdbAnyObject::replicateFwd(" <<
      type << ')');
  ZmRef<ZdbBuf> buf = new ZdbBuf{this};
  auto data = buf->ensure(rxBuf->length);
  if (!data) return nullptr;
  memcpy(data, rxBuf->data(), rxBuf->length);
  buf->owner = this;
  return buf;
}

// send replication message
void ZdbBuf::send(ZiIOContext &io)
{
  io.init(ZiIOFn::Member<&ZdbBuf::sent>::fn(
	io.fn.mvObject<ZdbBuf>()), data(), length(), 0);
}
void ZdbBuf::sent(ZiIOContext &io)
{
  if ((io.offset += io.length) < io.size) return;
  // if continuing recovery, send next recovery msg
  if (ZuUnlikely(recovery())) {
    ZiMultiplex *mx = io.cxn->mx();
    mx->run(mx->txThread(), ZmFn<>::Member<&ZdbEnv::recSend>::fn(db->env()));
  }
  io.complete();
}

// broadcast heartbeat
void ZdbEnv::hbSend()
{
  Guard guard(m_lock);
  hbSend_(nullptr);
  m_mx->add(ZmFn<>::Member<&ZdbEnv::hbSend>::fn(this),
    m_hbSendTime += (time_t)m_cf.heartbeatFreq,
    ZmScheduler::Defer, &m_hbSendTimer);
}

// send heartbeat (either directed, or broadcast if cxn_ is 0)
void ZdbEnv::hbSend_(Zdb_Cxn *cxn_)
{
  if (!m_self) {
    ZeLOG(Fatal, "ZdbEnv::hbSend_ called out of order");
    return;
  }
  dbStateRefresh_();
  if (cxn_) {
    cxn_->hbSend();
    return;
  }
  ZmRef<Zdb_Cxn> cxn;
  unsigned i = 0, n = m_cxns->count_();
  ZtArray<ZmRef<Zdb_Cxn> > cxns(n);
  {
    auto j = m_cxns->readIterator();
    while (i < n && (cxn = j.iterateKey())) if (cxn->up()) ++i, cxns.push(cxn);
  }
  for (unsigned j = 0; j < i; j++) cxns[j]->hbSend();
}

// send heartbeat on a specific connection
void Zdb_Cxn::hbSend()
{
  this->ZiConnection::send(ZiIOFn::Member<&Zdb_Cxn::hbSend_>::fn(this));
}
void Zdb_Cxn::hbSend_(ZiIOContext &io)
{
  ZdbHost *self = m_env->self();
  if (ZuUnlikely(!self)) {
    ZeLOG(Fatal, "Zdb_Cxn::hbSend called out of order");
    io.complete();
    return;
  }
  Zfb::IOBuilder<ZdbBuf> fbb;
  using namespace ZdbNet;
  {
    const auto &dbState = m_env->dbState();
    auto msg = fbs::CreateMsg(fbb,
	fbs::Hdr_HB, 
	fbs::CreateHeartbeat(fbb, self->id(), m_env->state(),
	  fbb.CreateVector(dbState.data(), dbState.length())));
    fbb.Finish(msg);
  }
  ZdbNet::saveHdr(fbb);
  io.init(ZiIOFn::Member<&Zdb_Cxn::hbSent>::fn(this),
      &m_hbSendHdr, sizeof(Zdb_Msg_Hdr), 0);
  ZdbDEBUG(m_env, ZtString{} << "hbSend()"
      "  self[ID:" << self->id() << " S:" << m_env->state() <<
      " N:" << self->dbState().length() << "] " << self->dbState());
}
void Zdb_Cxn::hbSent(ZiIOContext &io)
{
  if ((io.offset += io.length) < io.size) return;
  ZdbHost *self = m_env->self();
  if (ZuUnlikely(!self)) {
    ZeLOG(Fatal, "Zdb_Cxn::hbSend called out of order");
    io.complete();
    return;
  }
  io.init(ZiIOFn::Member<&Zdb_Cxn::hbSent2>::fn(this),
      self->dbState().data(), self->dbState().length() * sizeof(ZdbRN), 0);
}
void Zdb_Cxn::hbSent2(ZiIOContext &io)
{
  if ((io.offset += io.length) < io.size) return;
  io.complete();
}

// refresh db state vector (locked)
void ZdbEnv::dbStateRefresh()
{
  Guard guard(m_lock);
  if (!m_self) {
    ZeLOG(Fatal, "ZdbEnv::dbStateRefresh called out of order");
    return;
  }
  dbStateRefresh_();
}

// refresh db state vector (unlocked)
void ZdbEnv::dbStateRefresh_()
{
  if (!m_self) {
    ZeLOG(Fatal, "ZdbEnv::dbStateRefresh_ called out of order");
    return;
  }
  Zdb_DBState &dbState = m_self->dbState();
  allDBs_([&dbState](const Zdb *db) {
    if (const Zdb_DBState::Node *node = dbState.find(db->id()))
      const_cast<Zdb_DBState::Node *>(node)->val() = db->nextRN();
    else
      dbState.add(db->id(), db->nextRN());
    return true;
  });
}}

// process received replication header
bool Zdb_Cxn::repRcvd(const ZmRef<ZdbRxBuf> &rxBuf, const fbs::Object *fbo)
{
  if (!m_host) {
    ZeLOG(Fatal, "Zdb received replication message before heartbeat");
    return false;
  }

  auto dbID = Zfb::Load::id(fbo->db())
  ZdbAny *db = m_env->db_(dbID, ZdbHandler{});

  if (!db) {
    ZeLOG(Fatal, ZtString{} <<
	"Zdb unknown remote DBID " << dbID << " received");
    return false;
  }

  m_env->repDataRcvd(db, m_host, fbo);
  db->replicated(rxBuf, fbo);
  return true;
}

// process received replication data
void ZdbEnv::repDataRcvd(Zdb *db, ZdbHost *host, const fbs::Object *fbo)
{
  auto id = db->id();
  auto data = Zfb::Load::bytes(fbo->data());
  ZdbDEBUG(this, ZtHexDump(ZtString{} << "ID:" << id <<
	" RN:" << fbo->rn() << " FROM:" << host,
	data.data(), data.length()));
  {
    Guard guard(m_lock);
    Zdb_DBState &dbState = host->dbState();
    ZdbRN newRN = fbo->rn(), oldRN;
    const Zdb_DBState::Node *node;
    if (node = dbState.find(id))
      oldRN = node->val();
    else
      node = dbState.add(id, oldRN = 0);
    if ((active() || host == m_next) && newRN < oldRN) return;
    if (newRN >= oldRN)
      const_cast<Zdb_DBState::Node *>(node)->val() = newRN + 1;
    if (!m_prev) {
      m_prev = host;
      ZeLOG(Info,
	  ZtString{} << "Zdb host " << m_prev->id() << " is previous in line");
    }
  }
}

// load object from buf
ZmRef<ZdbAnyObject> Zdb::load(const fbs::Object *fbo)
{
  using namespace ZdbNet;
  auto rn = fbo->rn();
  auto prevRN = fbo->prevRN();
  auto data = Zfb::Load::bytes(fbo->data());
  ZmRef<ZdbAnyObject> object;
  if (prevRN == ZdbNullRN) {	// added
    object = m_handler.loadFn(this, rn, data.data(), data.length());
  } else if (data) {		// updated
    {
      Guard guard(m_lock);
      if (object = cacheDel_(prevRN)) {
	object->prevRN = prevRN;
	object->rn = rn;
      }
    }
    if (object)
      object = m_handler.updateFn(object, data.data(), data.length());
    else {
      object = m_handler.loadFn(this, rn, data.data(), data.length());
      if (object) object->prevRN = prevRN;
    }
  } else {			// deleted
    Guard guard(m_lock);
    if (object = cacheDel_(prevRN)) {
      object->prevRN = prevRN;
      object->rn = rn;
    } else {
      object = new ZdbAnyObject(this, rn);
      object->prevRN = prevRN;
    }
  }
  object->deleted = fbo->deleted();
  return object;
}

void Zdb::save(Zfb::Builder &fbb, ZdbAnyObject *object)
{
  m_handler.saveFn(fbb, object->ptr_());
}

// process replicated record
void Zdb::replicated(const ZmRef<ZdbRxBuf> &rxBuf, const fbs::Object *fbo)
{
  ZmRef<ZdbAnyObject> object;
  ZmRef<ZdbBuf> buf;
  {
    Guard guard(m_lock);
    object = load(fbo);
    if (!object) return;
    if (m_nextRN <= rn) m_nextRN = rn + 1;
    buf = replicateFwd(rxBuf);
    m_bufCache->add(buf);
    cache(ZuMv(object));
  }
  if (object->deleted) {
    if (m_handler.deletedFn) m_handler.deletedFn(object, buf);
  } else if (object->prevRN == ZdbNullRN) {
    if (m_handler.addedFn) m_handler.addedFn(object, buf);
  } else {
    if (m_handler.updatedFn) m_handler.updatedFn(object, buf);
  }
  m_env->write(ZuMv(buf));
}

Zdb::Zdb(ZdbEnv *env, ZdbCf *cf, ZdbHandler handler)
  m_env{env}, m_cf{cf}, m_handler{ZuMv(handler)},
  m_path{ZiFile::append(env->config().path, cf->id)},
  m_cache{new Cache{}}, m_cacheSize{m_cache->size()},
  m_bufCache{new BufCache{}},
  m_files{new FileCache{}},
  m_indexBlks{new IndexBlkCache{}},
  m_fileCacheSize{m_files->size()},
  m_indexBlkCacheSize{m_indexBlks->size()}
{
}

Zdb::~Zdb()
{
  close();
  m_lru.clean();
  m_fileLRU.clean();
}

void Zdb::init(ZdbConfig *cf)
{
  m_cf = cf;
}

void Zdb::final()
{
  m_handler = ZdbHandler{};
}

bool Zdb::recover()
{
  Guard guard(m_lock);

  ZeError e;
  ZiDir::Path subName;
  ZtBitWindow<1> subDirs;
  // main directory
  {
    ZiDir dir;
    if (dir.open(m_cf->path) != Zi::OK) {
      if (ZiFile::mkdir(m_cf->path, &e) != Zi::OK) {
	ZeLOG(Fatal, ZtString{} << m_cf->path << ": " << e);
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
    subName_ = ZuBox<unsigned>(i).hex<ZuFmt::Right<5>>();
    subName = ZiFile::append(m_cf->path, subName_);
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
	fileIndex.scan(ZuFmt::Hex<>(), fileName_);
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
      ZmRef<Zdb_File> file = openFile(id, false);
      if (!file) return false;
      recover(file);
      return true;
    });
    return true;
  });
  return true;
}

void Zdb::recover(Zdb_File *file)
{
  if (!file->allocated()) return;
  if (file->deleted() >= file->allocated()) { delFile(file); return; }
  ZdbRN rn = (file->id())<<fileShift();
  ZmRef<IndexBlk> indexBlk;
  int first = file->first();
  if (ZuUnlikely(first < 0)) return;
  int last = file->last();
  if (ZuUnlikely(last < 0)) return; // should never happen
  rn += first;
  for (int j = first; j <= last; j++, rn++) {
    auto indexBlkID = rn>>indexShift();
    if (!indexBlk || indexBlk->id() != indexBlkID)
      indexBlk = getIndexBlk(file, indexBlkID, false, false);
    if (!indexBlk) return; // I/O error on file, logged within getIndexBlk
    if (!this->recover(FileRec{file, indexBlk, rn & indexMask()})) continue;
    if (rn <= m_minRN) m_minRN = rn;
    if (m_nextRN <= rn) m_nextRN = rn + 1;
    if (m_fileRN <= rn) m_fileRN = rn + 1;
  }
}

bool Zdb::recover(const Zdb_FileRec &rec)
{
  ZmRef<ZdbBuf> buf = read_(rec);
  if (!buf) return false;
  ZmRef<ZdbAnyObject> object;
  {
    Guard guard(m_lock);
    object = load(buf->fbo());
    if (!object) return false;
    if (m_nextRN <= rn) m_nextRN = rn + 1;
    cache(ZuMv(object));
  }
  if (object->deleted) {
    if (m_handler.deletedFn) m_handler.deletedFn(object, buf);
  } else if (object->prevRN == ZdbNullRN) {
    if (m_handler.addedFn) m_handler.addedFn(object, buf);
  } else {
    if (m_handler.updatedFn) m_handler.updatedFn(object, buf);
  }
  return true;
}

namespace Zdb_ {

void File_::init()
{
  m_flags = 0;
  m_count = 0;
  m_bitmap.zero();
  memset(&m_superBlk[0], 0, sizeof(FileSuperBlk));
}

// write record:
//
// [allocate/cache new index block, update super] if new index block needed
//
// 0] [append new index block]
// 1] append record
// 2] append record trailer
//
// cache/update index block
// cache/update bitmaps in file, prev file
// cache/update allocated, deleted in file hdr, prev file hdr
//
// index blocks are written on index block eviction
//
// file hdr, bitmap, superblock and all cached index blocks are
// written on file sync/eviction
// - on close, index blocks are both written and evicted
//
// recovery (uses file cache)
// - bitmap and allocated/deleted counts are rebuilt from records
// - bitmap and deleted counts for prev files are updated
//   (and Clean flag is cleared if bitmap was not up to date)
// - note that prev file and current file may be same or different
//
// note: read-only cache load - do not clear Clean flag on open,
// only on subsequent push/put/del

bool File_::scan()
{
  if (size() < sizeof(FileHdr) + sizeof(FileBitmap) + sizeof(FileSuperBlk)) {
    init();
    return sync_();
  }
  int r;
  ZeError e;
  // header
  {
    FileHdr hdr;
    if (ZuUnlikely((r = pread(0, &hdr, sizeof(FileHdr), &e)) != Zi::OK)) {
      m_db->fileRdError_(this, 0, e);
      m_flags = FileFlags::IOError;
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
      m_db->fileRdError_(this, sizeof(FileHdr), e);
      m_flags |= FileFlags::IOError;
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
      m_db->fileRdError_(this, sizeof(FileHdr) + sizeof(FileBitmap), e);
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
    if (m_super[i]) {
      FileIndexBlk indexBlk;
      if (ZuUnlikely((r = file->pread(m_super[i],
		&indexBlk, sizeof(FileIndexBlk), &e)) != Zi::OK)) {
	fileRdError_(file, m_super[i], e);
	return false;
      }
      bool rewriteIndexBlk = false;
      for (unsigned j = 0; j < indexRecs(); j++)
	if (uint64_t offset = indexBlk.data[j].offset) {
	  int64_t length = indexBlk.data[j].length;
	  if (length < 0) length = -length;
	  offset += length;
	  FileRecTrlr trlr;
	  if (ZuUnlikely((r = pread(offset,
		    &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
	    m_db->fileRdError_(this, offset, e);
	    m_flags |= FileFlags::IOError;
	    return false;
	  }
	  if (trlr.rn != ((id<<fileShift()) | (i<<indexShift()) | j) ||
	      (trlr.magic != ZdbCommitted && trlr.magic != ZdbDeleted)) {
	    indexBlk.data[j] = { 0, 0 };
	    rewriteIndexBlk = true;
	    continue;
	  }
	  if (trlr.magic == ZdbCommitted && length) {
	    ++m_allocated;
	    m_bitmap[(i<<indexShift()) | j].set();
	  } else {
	    ++m_deleted;
	    m_bitmap[(i<<indexShift()) | j].clr();
	  }
	}
      if (rewriteIndexBlk) {
	rewrite = true;
	if (ZuUnlikely((r = file->pwrite(m_super[i],
		  &indexBlk, sizeof(FileIndexBlk), &e)) != Zi::OK)) {
	  fileWrError_(file, m_super[i], e);
	  return false;
	}
      }
    }
  m_offset = size();
  return !rewrite || sync_();
}

bool File_::sync_()
{
  // header
  {
    FileHdr hdr{
      .magic = ZdbMagic,
      .version = ZdbVersion,
      .flags = m_flags,
      .count = m_count
    };
    if (ZuUnlikely((r = pwrite(0, &hdr, sizeof(FileHdr), &e)) != Zi::OK)) {
      m_db->fileWrError_(this, sizeof(FileHdr), e);
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
      m_db->fileWrError_(this, sizeof(FileHdr), e);
      m_flags |= FileFlags::IOError;
      return false;
    }
  }
  // superblock
  {
#if Zu_BIGENDIAN
    FileSuperBlk superBlk;
    auto superData = &(super.data[0]);
    for (unsigned i = 0; i < fileIndices(); i++) superData[i] = m_super[i];
#else
    auto superData = &m_super[0];
#endif
    if (ZuUnlikely((r = pwrite(
	      sizeof(FileHdr) + sizeof(FileBitmap),
	      &superData[0], sizeof(FileSuperBlk), &e)) != Zi::OK)) {
      m_db->fileWrError_(this, sizeof(FileHdr) + sizeof(FileBitmap), e);
      m_flags |= FileFlags::IOError;
      return false;
    }
  }
  return true;
}

bool File_::sync()
{
  if (!sync_()) goto error;
  {
    int r;
    ZeError e;
    if (ZiFile::sync(&e) != Zi::OK) {
      m_db->fileWrError_(this, 0, e);
      m_flags |= FileFlags::IOError;
      goto error;
    }
  }
  m_flags |= FileFlags::Clean;
  return true;
error:
  FileHdr hdr{
    .magic = ZdbMagic,
    .version = ZdbVersion,
    .flags = m_flags,
    .count = m_count
  };
  pwrite(0, &hdr, sizeof(FileHdr)); // best effort to clear Clean flag
  m_flags &= ~FileFlags::Clean;
  return false;
}

bool Zdb::open()
{
  if (!recover()) return false;

  if (m_cf->warmUp) {
    Guard guard(m_lock);
    rn2file(m_nextRN, true);
    ZmRef<ZdbAnyObject>{m_handler.ctorFn(this, m_nextRN)};
  }

  return true;
}

void Zdb::close()
{
  FSGuard guard(m_fsLock);
  m_files->clean();
  m_indexBlks->clean();
}

void Zdb::checkpoint()
{
  m_env->mx()->run(
      m_env->config().writeTID,
      ZmFn<>{this, [](ZdbAny *db) { db->checkpoint_(); }});
}

void Zdb::checkpoint_()
{
  FSGuard guard(m_fsLock);
  auto i = m_files->readIterator();
  while (Zdb_File *file = i.iterate())
    file->checkpoint();
}

ZmRef<ZdbAnyObject> Zdb::placeholder()
{
  return m_handler.ctorFn(this, ZdbNullRN);
}

ZdbRN Zdb::pushRN()
{
  Guard guard(m_lock);
  return m_nextRN++;
}

ZmRef<ZdbAnyObject> Zdb::push()
{
  if (ZuUnlikely(!m_env->active())) {
    ZeLOG(Error, ZtString{} <<
	"Zdb inactive application attempted push on DBID " << m_id);
    return nullptr;
  }
  return push_();
}

ZmRef<ZdbAnyObject> Zdb::push_()
{
  return m_handler.ctorFn(this, pushRN());
}

ZmRef<ZdbAnyPOD> Zdb::push(ZdbRN rn)
{
  if (ZuUnlikely(!m_env->active())) {
    ZeLOG(Error, ZtString{} <<
	"Zdb inactive application attempted push on DBID " << m_id);
    return nullptr;
  }
  if (ZuUnlikely(rn == ZdbNullRN)) return push_();
  return push_(rn);
}

ZmRef<ZdbAnyObject> Zdb::push_(ZdbRN rn)
{
  {
    Guard guard(m_lock);
    if (ZuLikely(m_nextRN <= rn))
      m_nextRN = rn + 1;
    else if (ZuLikely(exists__(rn)))
      return nullptr;
  }
  return m_handler.ctorFn(this, rn);
}

ZmRef<ZdbAnyObject> Zdb::get(ZdbRN rn)
{
  ZmRef<ZdbAnyObject> object;
  Guard guard(m_lock);
  if (ZuUnlikely(rn >= m_nextRN)) return nullptr;
  ++m_cacheLoads;
  if (ZuLikely(object = m_cache->find(rn))) {
    if (object->deleted) return nullptr;
    if (m_cacheMode != ZdbCacheMode::FullCache)
      m_lru.push(m_lru.del(object.ptr()));
    return object;
  }
  ++m_cacheMisses;
  {
    ZmRef<ZdbBuf> buf = m_bufCache->find(rn);
    if (!buf) {
      Zdb_FileRec rec = rn2file(rn, false);
      if (rec) buf = read_(rec);
    }
    if (buf) object = load(buf->fbo());
  }
  if (ZuUnlikely(!object)) return nullptr;
  cache(object);
  if (object->deleted) return nullptr;
  return object;
}

ZmRef<ZdbAnyObject> Zdb::get_(ZdbRN rn)
{
  Guard guard(m_lock);
  if (ZuUnlikely(rn >= m_nextRN)) return nullptr;
  return get__(rn);
}

ZmRef<ZdbAnyObject> Zdb::get__(ZdbRN rn)
{
  ZmRef<ZdbAnyObject> object;
  ++m_cacheLoads;
  if (ZuLikely(object = m_cache->find(rn))) return object;
  ++m_cacheMisses;
  {
    ZmRef<ZdbBuf> buf = m_bufCache->find(rn);
    if (!buf) {
      Zdb_FileRec rec = rn2file(rn, false);
      if (rec) buf = read_(rec);
    }
    if (buf) object = load(buf->fbo());
  }
  return object;
}

bool Zdb::exists__(ZdbRN rn)
{
  if (ZuLikely(m_cache->find(rn))) return true;
  if (ZuLikely(m_bufCache->find(rn))) return true;
  ZmRef<Zdb_File> file = getFile(rn>>fileShift(), false, true);
  if (!file) return false;
  return file->exists(rn & fileRecMask());
}

void Zdb::cache(ZdbAnyObject *object)
{
  if (m_cacheMode != ZdbCacheMode::FullCache &&
      m_cache->count_() >= m_cacheSize) {
    auto lru_ = m_lru.shiftNode();
    if (ZuLikely(lru_)) {
      ZdbAnyObject *lru = static_cast<ZdbAnyObject *>(lru_);
      if (lru->pinned)
	m_lru.unshift(lru);
      else
	m_cache->del(lru->rn);
    }
  }
  cache_(object);
}

void Zdb::cache_(ZdbAnyObject *object)
{
  m_cache->add(object);
  if (m_cacheMode != ZdbCacheMode::FullCache) m_lru.push(object);
}

ZmRef<ZdbAnyObject> Zdb::cacheDel_(ZdbRN rn)
{
  if (ZmRef<Zdb_CacheNode> object = m_cache->del(rn)) {
    if (m_cacheMode != ZdbCacheMode::FullCache) m_lru.del(object.ptr());
    return object;
  }
  return nullptr;
}

void Zdb::cacheDel_(ZdbAnyObject *object)
{
  m_cache->delNode(object);
  if (m_cacheMode != ZdbCacheMode::FullCache) m_lru.del(object);
}

void Zdb::abort(ZdbAnyObject *object) // aborts a push() or update()
{
  Guard guard(m_lock);
  object->rn = object->origRN;
  object->origRN = ZdbNullRN;
  if (object->rn != ZdbNullRN) {
    object->deleted = false;
    cache(object);
  }
}

void Zdb::put(ZdbAnyObject *object) // commits a push
{
  ZmRef<ZdbBuf> buf = object->replicate(Zdb_Msg::Rep);
  {
    Guard guard(m_lock);
    m_bufCache->add(buf);
    cache(object);
  }
  m_env->write(ZuMv(buf));
}

void Zdb::update(ZdbAnyObject *object)
{
  Guard guard(m_lock);
  cacheDel_(object);
  object->origRN = object->rn;
  object->rn = m_nextRN++;
}

// FIXME - re-check locking generally, including locks held during
// handler callbacks

bool Zdb::update(ZdbAnyObject *object, ZdbRN rn)
{
  if (ZuUnlikely(rn == ZdbNullRN)) return update(object);
  Guard guard(m_lock);
  if (ZuLikely(m_nextRN <= rn))
    m_nextRN = rn + 1;
  else if (ZuLikely(exists__(rn)))
    return false;
  cacheDel_(object);
  object->origRN = object->rn;
  object->rn = rn;
  return true;
}

ZmRef<ZdbAnyObject> Zdb::update_(ZdbRN prevRN)
{
  Guard guard(m_lock);
  ZmRef<ZdbAnyObject> object = get__(prevRN);
  if (ZuUnlikely(!object || object->deleted)) return nullptr;
  cacheDel_(object);
  object->origRN = object->rn;
  object->rn = m_nextRN++;
  return object;
}

ZmRef<ZdbAnyObject> Zdb::update_(ZdbRN prevRN, ZdbRN rn)
{
  Guard guard(m_lock);
  if (ZuLikely(m_nextRN <= rn))
    m_nextRN = rn + 1;
  else if (ZuLikely(exists__(rn)))
    return nullptr;
  ZmRef<ZdbAnyObject> object = get__(prevRN);
  if (ZuUnlikely(!object || object->deleted)) return nullptr;
  cacheDel_(object);
  object->origRN = object->rn;
  object->rn = rn;
  return object;
}

// commits an update
void Zdb::putUpdate(ZdbAnyObject *object)
{
  ZmRef<ZdbBuf> buf = object->replicate(Zdb_Msg::Rep);
  {
    Guard guard(m_lock);
    object->prevRN = object->origRN;
    object->origRN = ZdbNullRN;
    m_bufCache->add(buf);
    cache(object);
  }
  m_env->write(ZuMv(buf));
}

// FIXME from here

void Zdb::purge(ZdbRN minRN)
{
  ZdbRN rn;
  {
    ReadGuard guard(m_lock);
    rn = m_minRN;
  }
  while (rn < minRN) {
    Guard guard(m_lock);
    if (rn >= m_nextRN) return;
    // FIXME - attempt cacheDel_(rn), then fall back to bufCache, etc.
    if (ZmRef<ZdbAnyObject> object = get__(rn)) {
      cacheDel_(object);
      object->deleted = true;
      m_minRN = rn + 1;
      guard.unlock();
      // FIXME - tombstone? prevRN?
      m_env->write(object->replicate(Zdb_Msg::Rep));
    }
    ++rn;
  }
}

void Zdb::telemetry(Telemetry &data) const
{
  data.path = m_cf->path;
  data.name = m_cf->name;
  data.id = m_id;
  data.cacheMode = m_cacheMode;
  data.warmUp = m_cf->warmUp;
  {
    ReadGuard guard(m_lock);
    data.minRN = m_minRN;
    data.nextRN = m_nextRN;
    data.fileRN = m_fileRN;
    data.cacheSize = m_cacheSize;
    data.cacheLoads = m_cacheLoads;
    data.cacheMisses = m_cacheMisses;
  }
  {
    FSReadGuard guard(m_fsLock);
    data.fileCacheSize = m_fileCacheSize;
    data.indexBlkCacheSize = m_indexBlkCacheSize;
    data.fileLoads = m_fileLoads;
    data.fileMisses = m_fileMisses;
    data.indexBlkLoads = m_indexBlkLoads;
    data.indexBlkMisses = m_indexBlkMisses;
  }
}

void ZdbEnv::write(ZmRef<ZdbBuf> buf)
{
  if (!buf->object->db->config().repMode)
    this->repSend(buf);				// send to replica
  m_mx->run(m_cf.writeTID,
      ZmFn<>::mvFn(ZuMv(buf), [](ZmRef<ZdbBuf> buf) {
	Zdb *db = buf->db();
	db->write2(ZuMv(buf));
      }));
}

void Zdb::write2(ZmRef<ZdbBuf> buf)
{
  if (!m_cf.repMode) m_env->repSend(buf);	// send to replica
  {
    Guard guard(m_lock);
    m_bufCache->delNode(buf);
    write_(buf);
  }
  if (m_handler.logFn) m_handler.logFn(buf);
}

Zdb_FileRec Zdb::rn2file(ZdbRN rn, bool write)
{
  uint64_t fileID = rn>>fileShift();
  ZmRef<Zdb_File> file = getFile(fileID, write, true);
  if (!file) return {};
  if (!file->exists(rn & fileRecMask())) return {};
  uint64_t indexBlkID = rn>>indexShift();
  ZmRef<IndexBlk> indexBlk = getIndexBlk(file, indexBlkID, write, true);
  if (!indexBlk) return {};
  auto indexOff = rn & indexMask()
  if (!write && !indexBlk->blk.data[indexOff].offset) return {};
  return {ZuMv(file), ZuMv(indexBlk), indexOff};
}

ZmRef<Zdb_File> Zdb::getFile(uint64_t id, bool create, bool cache)
{
  FSGuard guard(m_fsLock);
  ++m_fileLoads;
  ZmRef<Zdb_File> file;
  if (file = m_files->find(id)) {
    m_fileLRU.push(m_fileLRU.del(file.ptr()));
    return file;
  }
  ++m_fileMisses;
  file = openFile(id, create);
  if (ZuUnlikely(!file)) return nullptr;
  if (ZuLikely(cache)) {
    // eviction
    if (m_files->count_() >= m_fileCacheSize)
      if (Zdb_File *lru = static_cast<Zdb_File *>(m_fileLRU.shiftNode())) {
	lru->sync();
	m_files->del(lru->id());
      }
    m_files->add(file);
    m_fileLRU.push(file.ptr());
  }
  if (id > m_lastFile) m_lastFile = id;
  return file;
}

ZmRef<Zdb_File> Zdb::openFile(uint64_t id, bool create)
{
  ZiFile::Path name = dirName(id);
  if (create) ZiFile::mkdir(name); // pre-emptive idempotent
  name = fileName(name, id);
  ZmRef<Zdb_File> file = new Zdb_File{this, id};
  if (file->open(name, ZiFile::GC, 0666, m_fileSize, 0) == Zi::OK) {
    if (!file->scan()) return nullptr;
    return file;
  }
  if (!create) return nullptr;
  ZeError e;
  if (file->open(name, ZiFile::Create | ZiFile::GC,
	0666, m_fileSize, &e) != Zi::OK) {
    ZeLOG(Fatal, ZtString{} <<
	"Zdb could not open or create \"" << name << "\": " << e);
    return nullptr; 
  }
  file->sync_();
  return file;
}

void Zdb::delFile(Zdb_File *file)
{
  bool lastFile;
  uint64_t id = file->id();
  {
    FSGuard guard(m_fsLock);
    if (m_files->del(id)) m_fileLRU.del(file);
    lastFile = id == m_lastFile;
  }
  if (ZuUnlikely(lastFile)) getFile(id + 1, true, true);
  file->close();
  ZiFile::remove(fileName(id));
}

ZmRef<IndexBlk> Zdb::getIndexBlk(
    Zdb_File *file, uint64_t id, bool create, bool cache)
{
  FSGuard guard(m_fsLock);
  ++m_indexBlkLoads;
  ZmRef<IndexBlk> indexBlk;
  if (indexBlk = m_indexBlks->find(id)) {
    m_indexBlkLRU.push(m_indexBlkLRU.del(indexBlk.ptr()));
    return indexBlk;
  }
  ++m_indexBlkMisses;
  indexBlk = create ? file->writeIndexBlk(id) : file->readIndexBlk(id);
  if (ZuUnlikely(!indexBlk)) return nullptr;
  if (ZuLikely(cache)) {
    // eviction
    if (m_indexBlks->count_() >= m_indexBlkCacheSize)
      if (IndexBlk *lru = static_cast<IndexBlk *>(m_indexBlksLRU.shiftNode())) {
	if (ZmRef<Zdb_File> file =
	    getFile(lru->id>>(fileShift() - indexShift()), false, false))
	  file->writeIndexBlk(lru);
	m_indexBlks->del(lru->id());
      }
    m_indexBlks->add(indexBlk);
    m_indexBlksLRU.push(indexBlk.ptr());
  }
  if (id > m_lastIndexBlk) m_lastIndexBlk = id;
  return indexBlk;
}

ZmRef<IndexBlk> File_::readIndexBlk(uint64_t id)
{
  ZmRef<IndexBlk> indexBlk;
  auto offset = m_superBlk[id & indexMask()];
  if (offset) {
    indexBlk = new IndexBlk{id, offset};
    if (!readIndexBlk_(indexBlk)) return nullptr;
    return indexBlk;
  }
  return nullptr;
}

ZmRef<IndexBlk> File_::writeIndexBlk(uint64_t id)
{
  ZmRef<IndexBlk> indexBlk;
  if (indexBlk = readIndexBlk(id)) return indexBlk;
  offset = m_offset;
  m_superBlk[id & indexMask()] = offset;
  indexBlk = new IndexBlk{id, offset};
  ZuAssert(sizeof(FileIndexBlk) == sizeof(IndexBlk__));
  // shortcut endian conversion when initializing a blank index block
  memset(&indexBlk->blk, 0, sizeof(FileIndexBlk));
  {
    int r;
    ZeError e;
    if (ZuUnlikely((r = pwrite(indexBlk->offset,
	      &indexBlk->blk, sizeof(FileIndexBlk), &e)) != Zi::OK)) {
      m_db->fileWrError_(this, indexBlk->offset, e);
      return nullptr;
    }
  }
  m_offset = offset + sizeof(FileIndexBlk);
  return indexBlk;
}

bool File_::readIndexBlk_(IndexBlk *indexBlk)
{
  int r;
  ZeError e;
#if Zu_BIGENDIAN
  FileIndexBlk indexBlk;
  auto indexData = &(indexBlk.data[0]);
#else
  auto indexData = &m_indexBlk.blk.data[0];
#endif
  if (ZuUnlikely((r = pread(indexBlk->offset,
	    &indexData[0], sizeof(FileIndexBlk), &e)) != Zi::OK)) {
    m_db->fileRdError_(this, indexBlk->offset, e);
    return false;
  }
#if Zu_BIGENDIAN
  for (unsigned i = 0; i < indexRecs(); i++)
    m_indexBlk.data[i] = { indexData[i].offset, indexData[i].length };
#endif
  return true;
}

bool File_::writeIndexBlk_(IndexBlk *indexBlk)
{
  int r;
  ZeError e;
#if Zu_BIGENDIAN
  FileIndexBlk indexBlk;
  auto indexData = &(indexBlk.data[0]);
  for (unsigned i = 0; i < indexRecs(); i++) {
    const auto &index = m_indexBlk.blk.data[i];
    indexData[i] = { index.offset, index.length };
  }
#else
  auto indexData = &m_indexBlk.blk.data[0];
#endif
  if (ZuUnlikely((r = pwrite(indexBlk->offset,
	    &indexData[0], sizeof(FileIndexBlk), &e)) != Zi::OK)) {
    m_db->fileWrError_(this, indexBlk->offset, e);
    return false;
  }
  return true;
}

ZmRef<ZdbBuf> Zdb::read_(const Zdb_FileRec &rec)
{
  const auto &index = rec.index();
  Zfb::IOBuilder<ZdbBuf> fbb;
  Offset<Vector<uint8_t>> data;
  uint8_t *ptr = nullptr;
  if (index.length) {
    ptr = Zfb::Save::extend(fbb, index.length, data);
    if (!data || !ptr) return nullptr;
  }
  FileRecTrlr trlr;
  {
    int r;
    ZeError e;
    if (index.length) {
      // read record
      if (ZuUnlikely((r = rec.file()->pread(index.offset,
		ptr, index.length, &e)) != Zi::OK)) {
	fileRdError_(rec.file(), index.offset, e);
	return nullptr;
      }
    }
    // read trailer
    if (ZuUnlikely((r = rec.file()->pread(index.offset + index.length,
	      &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
      fileRdError_(rec.file(), index.offset + index.length, e);
      return nullptr;
    }
  }
  if (trlr.magic != ZdbCommitted && trlr.magic != ZdbDeleted)
    return nullptr;
  auto id = Zfb::Save::id(m_id);
  auto msg = fbs::CreateMsg(fbb, Zdb_Msg::Rec,
      fbs::CreateObject(fbb,
	&id, trlr.rn, trlr.prevRN, trlr.magic == ZdbDeleted, data));
  fbb.Finish(msg);
  ZdbNet::saveHdr(fbb);
  ZmRef<ZdbBuf> buf = fbb.buf();
  buf->owner = this;
  return buf;
}

void Zdb::write_(const ZdbBuf *buf)
{
  auto fbo = buf->fbo();
  if (!fbo) return;
  ZdbRN rn = fbo->rn(), prevRN = fbo->prevRN();
  auto data = Zfb::Load::bytes(fbo->data());
  {
    Zdb_FileRec rec = rn2file(rn, true);
    if (!rec) return;
    auto &index = rec.index();
    index.offset = rec.file()->append(data.length() + sizeof(FileRecTrlr));
    rec.file()->alloc(rn & fileRecMask());
    index.length = data.length();
    FileRecTrlr trlr{
      .rn = rn,
      .prevRN = prevRN,
      .magic = !data ? ZdbDeleted : ZdbCommitted
    };
    int r;
    ZeError e;
    if (data) {
      // write record
      if (ZuUnlikely((r = rec.file()->pwrite(index.offset,
		data.data(), data.length(), &e)) != Zi::OK)) {
	fileWrError_(rec.file(), index.offset, e);
	index.offset = 0;
	index.length = 0;
	return;
      }
    }
    if (data || !rec.file()->del(rn & fileRecMask())) {
      // write trailer
      if (ZuUnlikely((r = rec.file()->pwrite(index.offset + index.length,
		&trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
	fileWrError_(rec.file(), index.offset + index.length, e);
	index.offset = 0;
	index.length = 0;
	return;
      }
    } else {
      delFile(rec.file());
    }
  }
  if (data) return;
  // delete - delete chain of previous records
  while (prevRN != ZdbNullRN) {
    rn = prevRN;
    prevRN = ZdbNullRN;
    if (ZmRef<ZdbAnyObject> object = cacheDel_(rn))
      prevRN = object->prevRN;
    Zdb_FileRec rec = rn2file(rn, false);
    if (!rec) return;
    const auto &index = rec.index();
    if (!index.offset) return;
    auto offset = index.offset + index.length;
    FileRecTrlr trlr;
    int r;
    ZeError e;
    if (prevRN == ZdbNullRN) {
      if (ZuUnlikely((r = rec.file()->pread(offset
		&trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
	fileRdError_(rec.file(), offset, e);
	return;
      }
      prevRN = trlr.prevRN;
    } else {
      trlr.rn = rn;
      trlr.prevRN = prevRN;
    }
    if (rec.file()->del(rn & fileRecMask())) {
      delFile(rec.file());
    } else {
      trlr.magic = ZdbDeleted;
      if (ZuUnlikely((r = rec.file()->pwrite(offset,
		&trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
	fileWrError_(rec.file(), offset, e);
      }
    }
  }
}

void Zdb::fileRdError_(
    Zdb_File *file, ZiFile::Offset off, int r, ZeError e)
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

void Zdb::fileWrError_(Zdb_File *file, ZiFile::Offset off, ZeError e)
{
  ZeLOG(Error, ZtString{} <<
      "Zdb pwrite() failed on \"" << fileName(file->id()) <<
      "\" at offset " << ZuBoxed(off) <<  ": " << e);
}
