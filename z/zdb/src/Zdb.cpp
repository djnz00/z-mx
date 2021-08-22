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

#include <zlib/ZiDir.hpp>

#include <assert.h>
#include <errno.h>

#include <zlib/ZtBitWindow.hpp>

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

  m_config = ZuMv(config);
  m_dbs.length(m_config.dbCfs.length());
  m_mx = mx;
  m_cxns = new CxnHash(m_config.cxnHash);
  m_activeFn = activeFn;
  m_inactiveFn = inactiveFn;

  unsigned n = m_config.hostCfs.length();
  for (unsigned i = 0; i < n; i++)
    m_hosts.add(ZmRef<ZdbHost>(new ZdbHost(this, &m_config.hostCfs[i])));
  m_self = m_hosts.findKey(m_config.hostID).ptr();
  if (!m_self)
    throw ZtString{} <<
      "Zdb own host ID " << m_config.hostID << " not in hosts table";

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
  {
    unsigned i, n = m_dbs.length();
    for (i = 0; i < n; i++) if (ZdbAny *db = m_dbs[i]) db->final();
  }
  m_activeFn = ZmFn<>();
  m_inactiveFn = ZmFn<>();
  guard.unlock();
  m_stateCond.broadcast();
}

void ZdbEnv::add(ZdbAny *db, ZuString name)
{
  Guard guard(m_lock);
  if (state() != ZdbHostState::Initialized) {
    ZeLOG(Fatal, ZtString{} <<
	"ZdbEnv::add called out of order for DB " << name);
    return;
  }
  unsigned i, n = m_config.dbCfs.length();
  for (i = 0; i < n; i++)
    if (name == m_config.dbCfs[i].name) {
      db->init(&m_config.dbCfs[i], i);
      m_dbs[i] = db;
      return;
    }
  ZeLOG(Fatal, ZtString{} <<
      "ZdbEnv::add called with invalid DB " << name);
}

bool ZdbEnv::open()
{
  Guard guard(m_lock);
  if (state() != ZdbHostState::Initialized) {
    ZeLOG(Fatal, "ZdbEnv::open called out of order");
    return false;
  }
  {
    unsigned i, n = m_dbs.length();
    for (i = 0; i < n; i++)
      if (ZdbAny *db = m_dbs[i])
	if (!db->open()) {
	  for (unsigned j = 0; j < i; j++)
	    if (ZdbAny *db_ = m_dbs[j])
	      db_->close();
	  return false;
	}
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
	  ZmTimeNow((int)m_config.electionTimeout), &m_electTimer);
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
    auto i = m_hosts.readIterator<ZmRBTreeLess>(m_config.hostID);
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
    auto i = m_hosts.readIterator<ZmRBTreeLess>(m_config.hostID);
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
      m_self->ip(), m_self->port(), m_config.nAccepts);
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
      ZmTimeNow((int)m_config.reconnectFreq));
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
  data.heartbeatFreq = m_config.heartbeatFreq;
  data.heartbeatTimeout = m_config.heartbeatTimeout;
  data.reconnectFreq = m_config.reconnectFreq;
  data.electionTimeout = m_config.electionTimeout;
  data.writeThread = m_config.writeTID;

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

void ZdbHost::telemetry(Telemetry &data) const
{
  data.ip = m_config->ip;
  data.id = m_config->id;
  data.priority = m_config->priority;
  data.port = m_config->port;
  data.state = m_state;
  data.voted = m_voted;
}

void ZdbHost::reactivate()
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

ZdbHost::ZdbHost(ZdbEnv *env, const ZdbHostConfig *config) :
  m_env(env),
  m_config(config),
  m_mx(env->mx()),
  m_state(ZdbHostState::Instantiated),
  m_voted(false)
{
  unsigned n = env->dbCount();
  m_dbState.length(env->dbCount());
  for (unsigned i = 0; i < n; i++) m_dbState[i] = 0;
}

void ZdbHost::connect()
{
  if (!m_env->running() || m_cxn) return;

  ZeLOG(Info, ZtString{} << "Zdb connecting to host " << id() <<
      " (" << m_config->ip << ':' << m_config->port << ')');

  m_mx->connect(
      ZiConnectFn::Member<&ZdbHost::connected>::fn(this),
      ZiFailFn::Member<&ZdbHost::connectFailed>::fn(this),
      ZiIP(), 0, m_config->ip, m_config->port);
}

void ZdbHost::connectFailed(bool transient)
{
  ZtString warning;
  warning << "Zdb failed to connect to host " << id() <<
      " (" << m_config->ip << ':' << m_config->port << ')';
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
  m_mx(env->mx()),
  m_host(host)
{
  memset(&m_hbSendHdr, 0, sizeof(Zdb_Msg_Hdr));
}

void Zdb_Cxn::connected(ZiIOContext &io)
{
  if (!m_env->running()) { io.disconnect(); return; }

  m_env->connected(this);

  m_mx->add(ZmFn<>::Member<&Zdb_Cxn::hbTimeout>::fn(this),
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

void ZdbHost::associate(Zdb_Cxn *cxn)
{
  Guard guard(m_lock);

  if (ZuUnlikely(m_cxn && m_cxn.ptr() != cxn)) {
    m_cxn->host(0);
    m_cxn->m_mx->add(ZmFn<>::Member<&ZiConnection::disconnect>::fn(m_cxn));
  }
  m_cxn = cxn;
}

void ZdbHost::reconnect()
{
  m_mx->add(ZmFn<>::Member<&ZdbHost::reconnect2>::fn(this),
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
  m_mx->del(&m_hbTimer);
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
  if (running() && host->id() < m_config.hostID) host->reconnect();
}

void ZdbHost::disconnected()
{
  m_cxn = nullptr;
}

ZdbHost *ZdbEnv::setMaster()
{
  ZdbHost *oldMaster = m_master;
  dbStateRefresh_();
  m_master = 0;
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
      if (host->voted()) {
	if (host != m_self) ++m_nPeers;
	if (!m_master || host->cmp(m_master) > 0) m_master = host;
      }
      ZdbDEBUG(this, ZtString{} <<
	  " host:" << *host << '\n' <<
	  " master:" << m_master);
    }
  }
  ZeLOG(Info, ZtString{} << "Zdb host " << m_master->id() << " is master");
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
	  host->cmp(m_self) < 0 && (!m_next || host->cmp(m_next) > 0))
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
  if (m_next->dbState().cmp(m_self->dbState()) < 0 && !m_recovering) {
    // ZeLOG(Info, "startReplication() initiating recovery");
    m_recovering = true;
    m_recover = m_next->dbState();
    m_recoverEnd = m_self->dbState();
    m_mx->run(m_mx->txThread(), ZmFn<>::Member<&ZdbEnv::recSend>::fn(this));
  }
}

void ZdbEnv::stopReplication()
{
  m_master = 0;
  m_prev = 0;
  m_next = 0;
  m_recovering = 0;
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
  io.init(ZiIOFn::Member<&Zdb_Cxn::msgRcvd>::fn(this),
      &m_recvHdr, sizeof(Zdb_Msg_Hdr), 0);
}

void Zdb_Cxn::msgRcvd(ZiIOContext &io)
{
  if ((io.offset += io.length) < io.size) return;

  switch (m_recvHdr.type) {
    case Zdb_Msg::HB:	hbRcvd(io); break;
    case Zdb_Msg::Rep:	repRcvd(io); break;
    case Zdb_Msg::Rec:	repRcvd(io); break;
    default:
      ZeLOG(Error, ZtString{} <<
	  "Zdb received garbled message from host " <<
	  ZuBoxed(m_host ? (int)m_host->id() : -1));
      io.disconnect();
      return;
  }

  m_mx->add(ZmFn<>::Member<&Zdb_Cxn::hbTimeout>::fn(this),
      ZmTimeNow((int)m_env->config().heartbeatTimeout), &m_hbTimer);
}

void Zdb_Cxn::hbRcvd(ZiIOContext &io)
{
  const Zdb_Msg_HB &hb = m_recvHdr.u.hb;
  unsigned dbCount = m_env->dbCount();

  if (dbCount != hb.dbCount) {
    ZeLOG(Fatal, ZtString{} <<
	"Zdb inconsistent remote configuration detected (local dbCount " <<
	dbCount << " != host " << hb.hostID << " dbCount " << hb.dbCount <<
	')');
    io.disconnect();
    return;
  }

  if (!m_host) m_env->associate(this, hb.hostID);

  if (!m_host) {
    io.disconnect();
    return;
  }

  hbDataRead(io);
}

// read heartbeat data
void Zdb_Cxn::hbDataRead(ZiIOContext &io)
{
  const Zdb_Msg_HB &hb = m_recvHdr.u.hb;

  m_recvData.length(hb.dbCount * sizeof(ZdbRN));

  io.init(ZiIOFn::Member<&Zdb_Cxn::hbDataRcvd>::fn(this),
      m_recvData.data(), m_recvData.length(), 0);
}

// process received heartbeat (connection level)
void Zdb_Cxn::hbDataRcvd(ZiIOContext &io)
{
  if ((io.offset += io.length) < io.size) return;

  m_env->hbDataRcvd(m_host, m_recvHdr.u.hb, (ZdbRN *)m_recvData.data());

  msgRead(io);
}

// process received heartbeat
void ZdbEnv::hbDataRcvd(ZdbHost *host, const Zdb_Msg_HB &hb, ZdbRN *dbState)
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

  host->state(hb.state);
  memcpy((void *)(host->dbState().data()), dbState,
      m_dbs.length() * sizeof(ZdbRN));

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
	    m_mx->add(ZmFn<>::Member<&ZdbHost::reactivate>::fn(ZmMkRef(host)));
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
  if (host != m_next && host != m_prev && host->cmp(m_self) < 0 &&
      (!m_next || host->cmp(m_next) > 0))
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
    if (ZdbAny *db = m_dbs[i])
      if (m_recover[i] < m_recoverEnd[i]) {
	ZmRef<ZdbAnyPOD> pod;
	ZdbRN rn = m_recover[i]++;
	if (pod = db->get__(rn)) {
	  if (pod->committed()) {
	    pod->range(ZdbRange{0, db->dataSize()});
	    cxn->repSend(
		ZuMv(pod), Zdb_Msg::Rec,
		ZdbOp::Add, db->config().compress);
	  } else {
	    pod->del();
	    cxn->repSend(ZuMv(pod), Zdb_Msg::Rec, ZdbOp::Del, false);
	  }
	} else {
	  db->alloc(pod);
	  pod->init(rn, ZdbRange{}, ZdbDeleted);
	  cxn->repSend(ZuMv(pod), Zdb_Msg::Rec, ZdbOp::Del, false);
	}
	return;
      }
  m_recovering = 0;
}

// send replication message to next-in-line
void ZdbEnv::repSend(ZmRef<ZdbAnyPOD> pod, int type, int op, bool compress)
{
  if (ZmRef<Zdb_Cxn> cxn = m_nextCxn)
    cxn->repSend(ZuMv(pod), type, op, compress);
}
void ZdbEnv::repSend(ZmRef<ZdbAnyPOD> pod)
{
  if (ZmRef<Zdb_Cxn> cxn = m_nextCxn)
    cxn->send(ZiIOFn::Member<&ZdbAnyPOD::send>::fn(ZuMv(pod)));
}

// send replication message (directed)
void Zdb_Cxn::repSend(
    ZmRef<ZdbAnyPOD> pod, int type, int op, bool compress)
{
  pod->replicate(type, op, compress);
  this->send(ZiIOFn::Member<&ZdbAnyPOD::send>::fn(ZuMv(pod)));
}
void Zdb_Cxn::repSend(ZmRef<ZdbAnyPOD> pod)
{
  this->send(ZiIOFn::Member<&ZdbAnyPOD::send>::fn(ZuMv(pod)));
}

// prepare replication data for sending & writing to disk
void ZdbAnyPOD::replicate(int type, int op, bool compress)
{
  ZdbDEBUG(m_db->env(), ZtString{} << "ZdbAnyPOD::replicate(" <<
      type << ", " << this->range() << ", " << ZdbOp::name(op) << ", " <<
      (int)compress << ')');
  m_hdr.type = type;
  Zdb_Msg_Rep &rep = m_hdr.u.rep;
  rep.db = m_db->id();
  rep.rn = rn();
  rep.prevRN = prevRN();
  // rep.range is already set
  rep.op = op;
  if (compress) {
    ZdbRange range = this->range();
    if (range) {
      m_compressed = this->compress();
      if (ZuUnlikely(!m_compressed)) goto uncompressed;
      int n = m_compressed->compress(
	  this->ptr<const char>() + range.off(), range.len());
      if (ZuUnlikely(n < 0)) goto uncompressed;
      rep.clen = n;
      return;
    }
  }

uncompressed:
  m_compressed = nullptr;
  rep.clen = 0;
}

// send replication message
void ZdbAnyPOD::send(ZiIOContext &io)
{
  io.init(ZiIOFn::Member<&ZdbAnyPOD::sent>::fn(
	io.fn.mvObject<ZdbAnyPOD>()), &m_hdr, sizeof(Zdb_Msg_Hdr), 0);
}
void ZdbAnyPOD::sent(ZiIOContext &io)
{
  if ((io.offset += io.length) < io.size) return;
  Zdb_Msg_Rep &rep = m_hdr.u.rep;
  ZdbRange range{rep.range};
  if (m_compressed)
    io.init(ZiIOFn::Member<&ZdbAnyPOD::sent2>::fn(
	  io.fn.mvObject<ZdbAnyPOD>()), m_compressed->ptr(), rep.clen, 0);
  else if (range)
    io.init(ZiIOFn::Member<&ZdbAnyPOD::sent2>::fn(
	  io.fn.mvObject<ZdbAnyPOD>()),
	this->ptr<const char>() + range.off(), range.len(), 0);
  else
    sent3(io);
}
void ZdbAnyPOD::sent2(ZiIOContext &io)
{
  if ((io.offset += io.length) < io.size) return;
  sent3(io);
}
void ZdbAnyPOD::sent3(ZiIOContext &io)
{
  if (ZuUnlikely(m_hdr.type == Zdb_Msg::Rec)) {
    ZiMultiplex *mx = io.cxn->mx();
    ZdbEnv *env = m_db->env();
    mx->run(mx->txThread(), ZmFn<>::Member<&ZdbEnv::recSend>::fn(env));
  }
  io.complete();
}

int ZdbAnyPOD_Cmpr::compress(const char *src, unsigned srcSize)
{
  return LZ4_compress_fast(src, ptr<char>(), srcSize, this->size(), 1);
}

// broadcast heartbeat
void ZdbEnv::hbSend()
{
  Guard guard(m_lock);
  hbSend_(0);
  m_mx->add(ZmFn<>::Member<&ZdbEnv::hbSend>::fn(this),
    m_hbSendTime += (time_t)m_config.heartbeatFreq,
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
  m_hbSendHdr.type = Zdb_Msg::HB;
  Zdb_Msg_HB &hb = m_hbSendHdr.u.hb;
  hb.hostID = self->id();
  hb.state = m_env->state();
  hb.dbCount = self->dbState().length();
  io.init(ZiIOFn::Member<&Zdb_Cxn::hbSent>::fn(this),
      &m_hbSendHdr, sizeof(Zdb_Msg_Hdr), 0);
  ZdbDEBUG(m_env, ZtString{} << "hbSend()"
      "  self[ID:" << hb.hostID << " S:" << hb.state <<
      " N:" << hb.dbCount << "] " << self->dbState());
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
  unsigned i, n = m_dbs.length();
  for (i = 0; i < n; i++) {
    ZdbAny *db = m_dbs[i];
    dbState[i] = db ? db->nextRN() : (ZdbRN)0;
  }
}

// process received replication header
void Zdb_Cxn::repRcvd(ZiIOContext &io)
{
  if (!m_host) {
    ZeLOG(Fatal, "Zdb received replication message before heartbeat");
    io.disconnect();
    return;
  }

  const Zdb_Msg_Rep &rep = m_recvHdr.u.rep;
  ZdbAny *db = m_env->db(rep.db);

  if (!db) {
    ZeLOG(Fatal, ZtString{} <<
	"Zdb unknown remote DBID " << rep.db << " received");
    io.disconnect();
    return;
  }

  repDataRead(io);
}

// read replication data
void Zdb_Cxn::repDataRead(ZiIOContext &io)
{
  const Zdb_Msg_Rep &rep = m_recvHdr.u.rep;
  ZdbAny *db = m_env->db(rep.db);
  if (ZuUnlikely(!db)) {
    ZeLOG(Fatal, "Zdb_Cxn::repDataRead internal error");
    return;
  }
  ZdbRange range{rep.range};
  if (!range) {
    m_env->repDataRcvd(m_host, this, rep, nullptr);
    msgRead(io);
  } else {
    m_recvData2.length(rep.clen ? (unsigned)rep.clen : (unsigned)range.len());
    io.init(ZiIOFn::Member<&Zdb_Cxn::repDataRcvd>::fn(this),
	m_recvData2.data(), m_recvData2.length(), 0);
  }
}

// pre-process received replication data, decompress as needed
void Zdb_Cxn::repDataRcvd(ZiIOContext &io)
{
  if (!m_host || m_host->cxn().ptr() != this) { io.disconnect(); return; }

  if ((io.offset += io.length) < io.size) return;

  Zdb_Msg_Rep &rep = m_recvHdr.u.rep;

  if (rep.clen) {
    ZdbAny *db = m_env->db(rep.db);
    m_recvData.length(db->recSize());
    int n = LZ4_decompress_safe(
	m_recvData2.data(), m_recvData.data(), rep.clen, db->recSize());
    if (ZuUnlikely(n < 0)) {
      ZeLOG(Fatal, ZtHexDump(ZtString{} << 
	    "decompress failed with rcode " << n << " (RN: " << rep.rn <<
	    ") RecSize: " << db->recSize() << " CLen " << rep.clen <<
	    "Data:\n", m_recvData.data(), db->recSize()));
      msgRead(io);
      return;
    }
    m_env->repDataRcvd(m_host, this, rep, (void *)m_recvData.data());
  } else {
    m_env->repDataRcvd(m_host, this, rep, (void *)m_recvData2.data());
  }
  msgRead(io);
}

// process received replication data
void ZdbEnv::repDataRcvd(
    ZdbHost *host, Zdb_Cxn *cxn, const Zdb_Msg_Rep &rep, void *ptr)
{
  ZdbRange range{rep.range};
  ZdbDEBUG(this, ZtHexDump(ZtString{} << "DBID:" << rep.db <<
	" RN:" << rep.rn << " R:" << range << " FROM:" << host,
	ptr, range.len()));
  ZdbAny *db = this->db(rep.db);
  if (ZuUnlikely(!db)) {
    ZeLOG(Error, ZtString{} <<
	  "Zdb bad incoming replication data from host " << host->id() <<
	  " - unknown DBID " << rep.db);
    return;
  }
  {
    Guard guard(m_lock);
    Zdb_DBState &dbState = host->dbState();
    if (ZuUnlikely(rep.db >= (ZdbID)dbState.length())) {
      ZeLOG(Fatal, ZtString{} <<
	  "ZdbEnv::repDataRcvd encountered inconsistent DBID "
	  "(ID " << rep.db << " >= " << dbState.length() << ')');
      return;
    }
    if ((active() || host == m_next) && rep.rn < dbState[rep.db]) return;
    if (rep.rn >= dbState[rep.db]) dbState[rep.db] = rep.rn + 1;
    if (!m_prev) {
      m_prev = host;
      ZeLOG(Info, ZtString{} <<
	  "Zdb host " << m_prev->id() << " is previous in line");
    }
  }
  ZmRef<ZdbAnyPOD> pod =
    db->replicated(rep.rn, rep.prevRN, ptr, range, rep.op);
  if (pod)
    repSend(ZuMv(pod), Zdb_Msg::Rep, rep.op, db->config().compress);
}

// process replicated record
ZmRef<ZdbAnyPOD> ZdbAny::replicated(
    ZdbRN rn, ZdbRN prevRN, void *ptr, ZdbRange range, int op)
{
  ZmRef<ZdbAnyPOD> pod = replicated_(rn, prevRN, range, op);
  if (!pod) return nullptr;
  replicate(pod, ptr, op);
  m_env->write(pod, Zdb_Msg::Rep, op, m_config->compress);
  return pod;
}

ZmRef<ZdbAnyPOD> ZdbAny::replicated_(
    ZdbRN rn, ZdbRN prevRN, ZdbRange range, int op)
{
  ZmRef<ZdbAnyPOD> pod;
  alloc(pod);
  if (ZuUnlikely(!pod)) return nullptr;
  Guard guard(m_lock);
  if (m_nextRN <= rn) m_nextRN = rn + 1;
  if (op != ZdbOp::Del) {
    if (prevRN != rn && (range.off() || range.len() < m_dataSize)) {
      ZmRef<ZdbAnyPOD> prev = m_cache->find(prevRN);
      if (ZuUnlikely(!prev) && prevRN < m_nextRN) {
	Zdb_FileRec rec = rn2file(prevRN, false);
	if (rec) prev = read_(rec);
      }
      if (prev && prev->magic())
	memcpy(pod->ptr(), prev->ptr(), m_dataSize);
    }
    pod->update(rn, prevRN, range, ZdbCommitted);
  } else
    pod->update(rn, prevRN, ZdbRange{}, ZdbDeleted);
  pod->pin();
  cache(pod);
  return pod;
}

void ZdbAny::replicate(ZdbAnyPOD *pod, void *ptr, int op)
{
  ZdbRange range = pod->range();
#ifdef ZdbRep_DEBUG
  ZmAssert((!range || (range.off() + range.len()) <= pod->size()));
#endif
  if (range) memcpy((char *)pod->ptr() + range.off(), ptr, range.len());
  m_handler.addFn(pod, op, false);
}

ZdbAny::ZdbAny(ZdbEnv *env, ZuString name, uint32_t version, int cacheMode,
    ZdbHandler handler, unsigned recSize, unsigned dataSize) :
  m_env(env), m_version(version), m_cacheMode(cacheMode),
  m_handler(ZuMv(handler)), m_recSize(recSize), m_dataSize(dataSize)
{
  if (!m_recSize || !m_dataSize) {
    ZeLOG(Fatal, ZtString{} <<
	"Zdb misconfiguration for DB " << name << " - record/data size is 0");
    return;
  }
  m_env->add(this, name);
  if (!m_config) {
    ZeLOG(Fatal, ZtString{} <<
	"Zdb misconfiguration for DB " << name << " - ZdbEnv::add() failed");
    return;
  }
  m_fileSize = ((uint64_t)m_recSize)<<ZdbFileShift;
}

ZdbAny::~ZdbAny()
{
  close();
  m_lru.clean();
  m_filesLRU.clean();
}

void ZdbAny::init(ZdbConfig *config, ZdbID id)
{
  m_config = config;
  m_id = id;
  m_cache = new Zdb_Cache{}; // (m_config->cache);
  m_cacheSize = m_cache->size();
  m_files = new FileHash{}; // (m_config->fileHash);
  m_filesMax = m_files->size();
}

void ZdbAny::final()
{
  m_handler = ZdbHandler{};
}

#pragma pack(push, 4)
namespace {
  struct Schema {
    uint32_t	magic;
    uint32_t	version;
    uint32_t	fileSize;
    uint32_t	recSize;
    uint32_t	dataSize;
  };
}
#pragma pack(pop)

bool ZdbAny::recover()
{
  ZeError e;
  ZiDir::Path subName;
  ZtBitWindow<1> subDirs;
  {
    ZiDir dir;
    if (dir.open(m_config->path) != Zi::OK) {
      if (ZiFile::mkdir(m_config->path, &e) != Zi::OK) {
	ZeLOG(Fatal, ZtString{} << m_config->path << ": " << e);
	return false;
      }
      {
	Schema f{
	  ZdbSchema, m_version, m_fileSize, m_recSize, m_dataSize};
	ZiFile::Path sName = ZiFile::append(m_config->path, "schema");
	ZiFile sFile;
	if (sFile.open(sName, ZiFile::Create | ZiFile::GC,
	      0666, sizeof(Schema), &e) != Zi::OK) {
	  ZeLOG(Fatal, ZtString{} << sName << ": " << e);
	  return false;
	}
	int r;
	if (ZuUnlikely((r = sFile.write(&f, sizeof(Schema), &e)) != Zi::OK)) {
	  ZeLOG(Fatal, ZtString{} <<
	      "Zdb write() failed on \"" << sName << "\": " << e);
	  return false;
	}
      }
      return true;
    }
    {
      Schema p{ZdbSchema, m_version, m_fileSize, m_recSize, m_dataSize};
      Schema f;
      ZiFile::Path sName = ZiFile::append(m_config->path, "schema");
      ZiFile sFile;
      if (sFile.open(sName, ZiFile::GC, 0666, sizeof(Schema), &e) != Zi::OK) {
	ZeLOG(Fatal, ZtString{} << sName << ": " << e);
	return false;
      }
      int r;
      if (ZuUnlikely((r = sFile.read(
		&f, sizeof(Schema), &e)) < (int)sizeof(Schema))) {
	ZeLOG(Fatal, ZtString{} <<
	    "Zdb read() failed on \"" << sName << "\": " << e);
	return false;
      }
      if (memcmp(&p, &f, sizeof(Schema))) {
	auto magicFmt = ZuFmt::Alt<ZuFmt::Right<10> >();
	ZeLOG(Fatal, ZtString{} <<
	    "Zdb \"" << m_config->path << "\": "
	      "program/filesystem inconsistent"
	    " magic:" <<
	      ZuBoxed(p.magic).hex(magicFmt) << '/' <<
		ZuBoxed(f.magic).hex(magicFmt) <<
	    " version:" <<
	      ZuBoxed(p.version) << '/' << ZuBoxed(f.version) <<
	    " fileSize:" <<
	      ZuBoxed(p.fileSize) << '/' << ZuBoxed(f.fileSize) <<
	    " recSize:" <<
	      ZuBoxed(p.recSize) << '/' << ZuBoxed(f.recSize) <<
	    " dataSize:" <<
	      ZuBoxed(p.dataSize) << '/' << ZuBoxed(f.dataSize));
	return false;
      }
    }
    while (dir.read(subName) == Zi::OK) {
#ifdef _WIN32
      ZtString subName_{subName};
#else
      auto &subName_ = subName;
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
      subIndex.scan(ZuFmt::Hex<>(), subName_);
      subDirs.set(subIndex);
    }
    dir.close();
  }
  subDirs.all([&](unsigned i, bool) -> uintptr_t {
#ifdef _WIN32
    ZtString subName_;
#else
    auto &subName_ = subName;
#endif
    subName_ = ZuBox<unsigned>(i).hex(ZuFmt::Right<5>());
    subName = ZiFile::append(m_config->path, subName_);
    ZiDir::Path fileName;
    ZtBitWindow<1> files;
    {
      ZiDir subDir;
      if (subDir.open(subName, &e) != Zi::OK) {
	ZeLOG(Error, ZtString{} << subName << ": " << e);
	return 0;
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
    files.all([&](unsigned j, bool) -> uintptr_t {
#ifdef _WIN32
      ZtString fileName_;
#else
      auto &fileName_ = fileName;
#endif
      fileName_ = ZuBox<unsigned>(j).hex(ZuFmt::Right<5>());
      fileName_ << ".zdb";
      unsigned index = (((unsigned)i)<<20U) | ((unsigned)j);
      fileName = ZiFile::append(subName, fileName_);
      ZmRef<Zdb_File> file = new Zdb_File(index);
      if (file->open(
	    fileName, ZiFile::GC, 0666, m_fileSize, &e) != Zi::OK) {
	ZeLOG(Error, ZtString{} << fileName << ": " << e);
	return 0;
      }
      recover(file);
      return 0;
    });
    return 0;
  });
  return true;
}

void ZdbAny::recover(Zdb_File *file)
{
  ZdbRN rn = ((ZdbRN)(file->index()))<<ZdbFileShift;
  for (unsigned j = 0; j < ZdbFileRecs; j++, rn++) {
    ZmRef<ZdbAnyPOD> pod = read_(Zdb_FileRec(file, j));
    if (!pod || !pod->magic()) return;
    if (rn != pod->rn()) {
      ZeLOG(Error, ZtString{} <<
	  "Zdb recovered corrupt record from \"" <<
	  fileName(file->index()) <<
	  "\" at offset " << (j * m_recSize) << ' ' <<
	  ZuBoxed(rn) << " != " << ZuBoxed(pod->rn()));
      continue;
    }
    switch (pod->magic()) {
      case ZdbCommitted:
	if (rn < m_minRN) m_minRN = rn;
	this->recover(ZuMv(pod), ZdbOp::Add);
	break;
      case ZdbDeleted:
	if (rn < m_minRN) m_minRN = rn;
	this->recover(ZuMv(pod), ZdbOp::Del);
	file->del(j);
	break;
      case ZdbAllocated:
	file->del(j);
	break;
      default:
	return;
    }
    if (m_nextRN <= rn) m_nextRN = rn + 1;
    if (m_fileRN <= rn) m_fileRN = rn + 1;
  }
}

void ZdbAny::recover(ZmRef<ZdbAnyPOD> pod, int op)
{
  ZdbRN prevRN = pod->prevRN();
  if (pod->rn() != prevRN) m_cache->del(prevRN);
  m_handler.addFn(pod, op, true);
  cache(ZuMv(pod));
}

void ZdbAny::scan(Zdb_File *file)
{
  unsigned magicOffset =
    m_recSize - sizeof(ZdbTrailer) + offsetof(ZdbTrailer, magic);
  for (unsigned j = 0; j < ZdbFileRecs; j++) {
    ZiFile::Offset off = (ZiFile::Offset)j * m_recSize + magicOffset;
    uint32_t magic;
    int r;
    ZeError e;
    if (ZuUnlikely((r = file->pread(off, &magic, 4, &e)) < 4)) {
      fileRdError_(file, off, r, e);
      return;
    }
    switch (magic) {
      case ZdbCommitted:
	break;
      case ZdbAllocated:
      case ZdbDeleted:
	file->del(j);
	break;
      default:
	return;
    }
  }
}

bool ZdbAny::open()
{
  if (!recover()) return false;

  ZmRef<ZdbAnyPOD> pod;
  for (unsigned i = 0, n = m_config->preAlloc; i < n; i++)
    alloc(pod);

  return true;
}

void ZdbAny::close()
{
  FSGuard guard(m_fsLock);
  m_files->clean();
}

void ZdbAny::checkpoint()
{
  m_env->mx()->run(
      m_env->config().writeTID,
      ZmFn<>{this, [](ZdbAny *db) { db->checkpoint_(); }});
}

void ZdbAny::checkpoint_()
{
  FSGuard guard(m_fsLock);
  auto i = m_files->readIterator();
  while (Zdb_File *file = i.iterate())
    file->checkpoint();
}

ZmRef<ZdbAnyPOD> ZdbAny::placeholder()
{
  ZmRef<ZdbAnyPOD> pod;
  alloc(pod);
  if (ZuUnlikely(!pod)) return nullptr;
  pod->placeholder();
  return pod;
}

ZmRef<ZdbAnyPOD> ZdbAny::push()
{
  if (ZuUnlikely(!m_env->active())) {
    ZeLOG(Error, ZtString{} <<
	"Zdb inactive application attempted push on DBID " << m_id);
    return nullptr;
  }
  return push_();
}

ZmRef<ZdbAnyPOD> ZdbAny::push_()
{
  ZmRef<ZdbAnyPOD> pod;
  alloc(pod);
  if (ZuUnlikely(!pod)) return nullptr;
  Guard guard(m_lock);
  ZdbRN rn = m_nextRN++;
  pod->init(rn, ZdbRange{0, m_dataSize});
  return pod;
}

ZmRef<ZdbAnyPOD> ZdbAny::push(ZdbRN rn)
{
  if (ZuUnlikely(!m_env->active())) {
    ZeLOG(Error, ZtString{} <<
	"Zdb inactive application attempted push on DBID " << m_id);
    return nullptr;
  }
  if (ZuUnlikely(rn == ZdbNullRN)) return push_();
  return push_(rn);
}

ZmRef<ZdbAnyPOD> ZdbAny::push_(ZdbRN rn)
{
  ZmRef<ZdbAnyPOD> pod;
  alloc(pod);
  if (ZuUnlikely(!pod)) return nullptr;
  {
    Guard guard(m_lock);
    if (ZuLikely(m_nextRN <= rn))
      m_nextRN = rn + 1;
    else {
      ZmRef<ZdbAnyPOD> pod_ = get__(rn);
      if (ZuUnlikely(pod_ && pod_->committed())) return nullptr;
    }
  }
  pod->init(rn, ZdbRange{0, m_dataSize});
  return pod;
}

ZdbRN ZdbAny::pushRN()
{
  Guard guard(m_lock);
  return m_nextRN++;
}

ZmRef<ZdbAnyPOD> ZdbAny::get(ZdbRN rn)
{
  ZmRef<ZdbAnyPOD> pod;
  Guard guard(m_lock);
  if (ZuUnlikely(rn >= m_nextRN)) return nullptr;
  ++m_cacheLoads;
  if (ZuLikely(pod = m_cache->find(rn))) {
    if (!pod->committed()) return nullptr;
    if (m_cacheMode != ZdbCacheMode::FullCache)
      m_lru.push(m_lru.del(pod.ptr()));
    return pod;
  }
  ++m_cacheMisses;
  {
    Zdb_FileRec rec = rn2file(rn, false);
    if (rec) pod = read_(rec);
  }
  if (ZuUnlikely(!pod || !pod->committed())) return nullptr;
  cache(pod);
  return pod;
}

ZmRef<ZdbAnyPOD> ZdbAny::get_(ZdbRN rn)
{
  Guard guard(m_lock);
  if (ZuUnlikely(rn >= m_nextRN)) return nullptr;
  ZmRef<ZdbAnyPOD> pod = get__(rn);
  if (ZuUnlikely(!pod || !pod->committed())) return nullptr;
  return pod;
}

ZmRef<ZdbAnyPOD> ZdbAny::get__(ZdbRN rn)
{
  ZmRef<ZdbAnyPOD> pod;
  ++m_cacheLoads;
  if (ZuLikely(pod = m_cache->find(rn))) return pod;
  ++m_cacheMisses;
  {
    Zdb_FileRec rec = rn2file(rn, false);
    if (rec) pod = read_(rec);
  }
  return pod;
}

void ZdbAny::cache(ZdbAnyPOD *pod)
{
  if (m_cacheMode != ZdbCacheMode::FullCache &&
      m_cache->count_() >= m_cacheSize) {
    auto lru_ = m_lru.shiftNode();
    if (ZuLikely(lru_)) {
      ZdbAnyPOD *lru = static_cast<ZdbAnyPOD *>(lru_);
      if (lru->pinned()) {
	m_lru.push(ZuMv(lru_));
	cache_(pod);
	m_cacheSize = m_cache->size();
	return;
      }
      m_cache->del(lru->rn());
    }
  }
  cache_(pod);
}

void ZdbAny::cache_(ZdbAnyPOD *pod)
{
  m_cache->add(pod);
  if (m_cacheMode != ZdbCacheMode::FullCache) m_lru.push(pod);
}

void ZdbAny::cacheDel_(ZdbRN rn)
{
  if (ZmRef<Zdb_CacheNode> pod = m_cache->del(rn))
    if (m_cacheMode != ZdbCacheMode::FullCache) m_lru.del(pod.ptr());
}

void ZdbAny::abort(ZdbAnyPOD *pod) // aborts a push()
{
  ZmAssert(!pod->committed());
  pod->del();
  m_env->write(pod, Zdb_Msg::Rep, ZdbOp::Del, false);
}

void ZdbAny::put(ZdbAnyPOD *pod) // commits a push
{
  ZmAssert(!pod->committed());
  pod->commit();
  {
    Guard guard(m_lock);
    pod->pin();
    cache(pod);
  }
  m_env->write(pod, Zdb_Msg::Rep, ZdbOp::Add, m_config->compress);
}

ZmRef<ZdbAnyPOD> ZdbAny::update(ZdbAnyPOD *prev)
{
  ZmRef<ZdbAnyPOD> pod;
  alloc(pod);
  if (ZuUnlikely(!pod)) return nullptr;
  ZdbRN rn;
  {
    Guard guard(m_lock);
    rn = m_nextRN++;
  }
  memcpy(pod->ptr(), prev->ptr(), m_dataSize);
  ZdbRN prevRN = prev->rn();
  if (ZuUnlikely(prevRN == ZdbNullRN)) prevRN = rn;
  pod->update(rn, prevRN, ZdbRange{0, m_dataSize});
  return pod;
}

ZmRef<ZdbAnyPOD> ZdbAny::update(ZdbAnyPOD *prev, ZdbRN rn)
{
  if (ZuUnlikely(rn == ZdbNullRN)) return update(prev);
  ZmRef<ZdbAnyPOD> pod;
  alloc(pod);
  if (ZuUnlikely(!pod)) return nullptr;
  {
    Guard guard(m_lock);
    if (ZuLikely(m_nextRN <= rn))
      m_nextRN = rn + 1;
    else {
      ZmRef<ZdbAnyPOD> pod_ = get__(rn);
      if (ZuUnlikely(pod_ && pod_->committed())) return nullptr;
    }
  }
  memcpy(pod->ptr(), prev->ptr(), m_dataSize);
  ZdbRN prevRN = prev->rn();
  if (ZuUnlikely(prevRN == ZdbNullRN)) prevRN = rn;
  pod->update(rn, prevRN, ZdbRange{0, m_dataSize});
  return pod;
}

ZmRef<ZdbAnyPOD> ZdbAny::update_(ZdbRN prevRN)
{
  ZmRef<ZdbAnyPOD> pod;
  alloc(pod);
  if (ZuUnlikely(!pod)) return nullptr;
  ZdbRN rn;
  {
    Guard guard(m_lock);
    rn = m_nextRN++;
  }
  if (ZuUnlikely(prevRN == ZdbNullRN)) prevRN = rn;
  pod->update(rn, prevRN, ZdbRange{0, m_dataSize});
  return pod;
}

ZmRef<ZdbAnyPOD> ZdbAny::update_(ZdbRN prevRN, ZdbRN rn)
{
  if (ZuUnlikely(rn == ZdbNullRN)) return update_(prevRN);
  ZmRef<ZdbAnyPOD> pod;
  alloc(pod);
  if (ZuUnlikely(!pod)) return nullptr;
  {
    Guard guard(m_lock);
    if (m_nextRN > rn) return nullptr;
    m_nextRN = rn + 1;
  }
  if (ZuUnlikely(prevRN == ZdbNullRN)) prevRN = rn;
  pod->update(rn, prevRN, ZdbRange{0, m_dataSize});
  return pod;
}

// commits an update - if replace, previous versions are deleted
void ZdbAny::putUpdate(ZdbAnyPOD *pod, bool replace)
{
  ZmAssert(!pod->committed());
  pod->commit();
  {
    Guard guard(m_lock);
    cacheDel_(pod->prevRN());
    pod->pin();
    cache(pod);
  }
  m_env->write(pod, Zdb_Msg::Rep,
      replace ? ZdbOp::Upd : ZdbOp::Add, m_config->compress);
}

void ZdbAny::del(ZdbAnyPOD *pod)
{
  ZmAssert(pod->committed());
  ZdbRN rn;
  {
    Guard guard(m_lock);
    cacheDel_(pod->rn());
    rn = m_nextRN++;
  }
  pod->update(rn, pod->rn(), ZdbRange{}, ZdbDeleted);
  m_env->write(pod, Zdb_Msg::Rep, ZdbOp::Del, false);
}

void ZdbAny::purge(ZdbRN minRN)
{
  ZdbRN rn;
  {
    ReadGuard guard(m_lock);
    rn = m_minRN;
  }
  while (rn < minRN) {
    Guard guard(m_lock);
    if (rn >= m_nextRN) return;
    if (ZmRef<ZdbAnyPOD> pod = get__(rn)) {
      cacheDel_(rn);
      m_minRN = rn;
      guard.unlock();
      pod->del();
      m_env->write(ZuMv(pod), Zdb_Msg::Rep, ZdbOp::Del, false);
    }
    ++rn;
  }
}

void ZdbAny::telemetry(Telemetry &data) const
{
  data.path = m_config->path;
  data.name = m_config->name;
  data.fileSize = m_fileSize;
  data.id = m_id;
  data.preAlloc = m_config->preAlloc;
  data.recSize = m_recSize;
  data.compress = m_config->compress;
  data.cacheMode = m_cacheMode;
  {
    ReadGuard guard(m_lock);
    data.minRN = m_minRN;
    data.nextRN = m_nextRN;
    data.fileRN = m_fileRN;
    data.cacheLoads = m_cacheLoads;
    data.cacheMisses = m_cacheMisses;
    data.fileRecs = ZdbFileRecs;
    data.cacheSize = m_cacheSize;
    data.filesMax = m_filesMax;
  }
  {
    FSReadGuard guard(m_fsLock);
    data.fileLoads = m_fileLoads;
    data.fileMisses = m_fileMisses;
  }
}

void ZdbEnv::write(ZmRef<ZdbAnyPOD> pod, int type, int op, bool compress)
{
  pod->replicate(type, op, compress);
  {
    const ZdbConfig &config = pod->db()->config();
    if (config.repMode) repSend(pod);
  }
  m_mx->run(m_config.writeTID,
      ZmFn<>::mvFn(ZuMv(pod), [](ZmRef<ZdbAnyPOD> pod) {
	ZdbAny *db = pod->db();
	db->write(ZuMv(pod));
      }));
}

void ZdbAny::write(ZmRef<ZdbAnyPOD> pod)
{
  if (!m_config->repMode) m_env->repSend(pod);
  int op = pod->write();
  {
    Guard guard(m_lock);
    pod->unpin();
  }
  m_handler.writeFn(pod, op);
}

int ZdbAnyPOD::write()
{
  Zdb_Msg_Rep &rep = m_hdr.u.rep;
  int op = rep.op;
  m_db->write_(rn(), prevRN(), ptr(), op);
  return op;
}

Zdb_FileRec ZdbAny::rn2file(ZdbRN rn, bool write)
{
  unsigned index = rn>>ZdbFileShift;
  unsigned offRN = rn & ZdbFileMask;
  ZmRef<Zdb_File> file = getFile(index, write);
  if (!file) return Zdb_FileRec();
  return Zdb_FileRec(ZuMv(file), offRN);
}

ZmRef<Zdb_File> ZdbAny::getFile(unsigned index, bool create)
{
  FSGuard guard(m_fsLock);
  ++m_fileLoads;
  ZmRef<Zdb_File> file;
  if (file = m_files->find(index)) {
    m_filesLRU.push(m_filesLRU.del(file.ptr()));
    return file;
  }
  ++m_fileMisses;
  file = openFile(index, create);
  if (ZuUnlikely(!file)) return nullptr;
  if (m_files->count_() >= m_filesMax)
    if (Zdb_File *lru = static_cast<Zdb_File *>(m_filesLRU.shiftNode()))
      m_files->del(lru->index());
  m_files->add(file);
  m_filesLRU.push(file.ptr());
  if (index > m_lastFile) m_lastFile = index;
  return file;
}

ZmRef<Zdb_File> ZdbAny::openFile(unsigned index, bool create)
{
  ZiFile::Path name = dirName(index);
  if (create) ZiFile::mkdir(name); // pre-emptive idempotent
  name = fileName(name, index);
  ZmRef<Zdb_File> file = new Zdb_File(index);
  if (file->open(name, ZiFile::GC, 0666, m_fileSize, 0) == Zi::OK) {
    scan(file);
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
  return file;
}

void ZdbAny::delFile(Zdb_File *file)
{
  bool lastFile;
  unsigned index = file->index();
  {
    FSGuard guard(m_fsLock);
    if (m_files->del(index))
      m_filesLRU.del(file);
    lastFile = index == m_lastFile;
  }
  if (ZuUnlikely(lastFile)) getFile(index + 1, true);
  file->close();
  ZiFile::remove(fileName(index));
}

ZmRef<ZdbAnyPOD> ZdbAny::read_(const Zdb_FileRec &rec)
{
  ZmRef<ZdbAnyPOD> pod;
  alloc(pod);
  ZiFile::Offset off = (ZiFile::Offset)rec.offRN() * m_recSize;
  int r;
  ZeError e;
  if (ZuUnlikely((r = rec.file()->pread(
	    off, pod->ptr(), m_recSize, &e)) < (int)m_recSize)) {
    fileRdError_(rec.file(), off, r, e);
    return nullptr;
  }
  return pod;
}

void ZdbAny::write_(ZdbRN rn, ZdbRN prevRN, const void *ptr, int op)
{
  int r;
  ZeError e;
  Zdb_FileRec rec;
  unsigned trailerOffset = m_recSize - sizeof(ZdbTrailer);

  {
    ZdbRN gapRN = m_fileRN;
    if (m_fileRN <= rn) m_fileRN = rn + 1;
    {
      ZdbRN minGapRN = (rn & ~((ZdbRN)ZdbFileMask));
      if (gapRN < minGapRN) gapRN = minGapRN;
    }
    while (gapRN < rn) {
      rec = rn2file(gapRN, true);
      if (!rec) return; // error is logged by getFile/openFile
      ZdbTrailer trailer{gapRN, gapRN, ZdbDeleted};
      if (rec.file()->del(rec.offRN())) {
	delFile(rec.file());
	gapRN = ((ZdbRN)(rec.file()->index() + 1))<<ZdbFileShift;
      } else {
	ZiFile::Offset off =
	  (ZiFile::Offset)rec.offRN() * m_recSize + trailerOffset;
	if (ZuUnlikely((r = rec.file()->pwrite(
		  off, &trailer, sizeof(ZdbTrailer), &e)) != Zi::OK))
	  fileWrError_(rec.file(), off, e);
	++gapRN;
      }
    }
  }

  if (!(rec = rn2file(rn, true))) return;
    // any error is logged by getFile/openFile

  if (op == ZdbOp::Del && rec.file()->del(rec.offRN()))
    delFile(rec.file());
  else {
    ZiFile::Offset off = (ZiFile::Offset)rec.offRN() * m_recSize;
    if (ZuUnlikely((r = rec.file()->pwrite(off, ptr, m_recSize, &e)) != Zi::OK))
      fileWrError_(rec.file(), off, e);
  }

  if (op == ZdbOp::Add) return;

  ZdbTrailer trailer;
  uint32_t magicDeleted = ZdbDeleted;
  unsigned magicOffset = trailerOffset + offsetof(ZdbTrailer, magic);

  while (prevRN != rn) {
    rn = prevRN;

    if (!(rec = rn2file(rn, false))) return;

    {
      ZiFile::Offset off =
	(ZiFile::Offset)rec.offRN() * m_recSize + trailerOffset;
      r = rec.file()->pread(off, &trailer, sizeof(ZdbTrailer), &e);
      if (ZuUnlikely(r < (int)sizeof(ZdbTrailer)))
	break;
      if (trailer.magic != ZdbCommitted) break;
      prevRN = trailer.prevRN;
    }

    if (rec.file()->del(rec.offRN()))
      delFile(rec.file());
    else {
      ZiFile::Offset off =
	(ZiFile::Offset)rec.offRN() * m_recSize + magicOffset;
      r = rec.file()->pwrite(off, &magicDeleted, 4, &e);
      if (ZuUnlikely(r != Zi::OK)) {
	fileWrError_(rec.file(), off, e);
	break;
      }
    }
  }
}

void ZdbAny::fileRdError_(
    Zdb_File *file, ZiFile::Offset off, int r, ZeError e)
{
  if (r < 0) {
    ZeLOG(Error, ZtString{} <<
	"Zdb pread() failed on \"" << fileName(file->index()) <<
	"\" at offset " << ZuBoxed(off) <<  ": " << e);
  } else {
    ZeLOG(Error, ZtString{} <<
	"Zdb pread() truncated on \"" << fileName(file->index()) <<
	"\" at offset " << ZuBoxed(off));
  }
}

void ZdbAny::fileWrError_(Zdb_File *file, ZiFile::Offset off, ZeError e)
{
  ZeLOG(Error, ZtString{} <<
      "Zdb pwrite() failed on \"" << fileName(file->index()) <<
      "\" at offset " << ZuBoxed(off) <<  ": " << e);
}
