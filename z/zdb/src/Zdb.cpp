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

ZdbEnv::ZdbEnv() :
  m_mx{0}, m_stateCond{m_lock},
  m_appActive{false}, m_self{0}, m_master{0}, m_prev{0}, m_next{0},
  m_nextCxn{0}, m_recovering{false},
  m_recover{4}, m_recoverEnd{4},
  m_nPeers{0}
{
}

ZdbEnv::~ZdbEnv()
{
}

void ZdbEnv::init(ZdbEnvCf config, ZiMultiplex *mx,
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
  m_cxns = new CxnHash{};
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
  if (!allDBs([](Zdb *db) {
    if (!db->open()) return false;
    return true;
  })) {
    allDBs([](Zdb *db) { db->close(); return true; });
    return false;
  }
  dbStateRefresh();
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
  allDBs_([](Zdb *db) { db->close(); });
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
  allDBs_([](Zdb *db) { db->checkpoint(); });
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
    if (m_nPeers = m_hosts.count_() - 1) {
      dbStateRefresh();
      m_mx->add(ZmFn<>::Member<&ZdbEnv::hbSend>::fn(this),
	  m_hbSendTime = ZmTimeNow(), &m_hbSendTimer);
      m_mx->add(ZmFn<>::Member<&ZdbEnv::holdElection>::fn(this),
	  ZmTimeNow((int)m_cf.electionTimeout), &m_electTimer);
    }
    guard.unlock();
    m_stateCond.broadcast();
  }

  ZeLOG(Info, "Zdb starting");

  if (m_hosts.count_() == 1) {
    holdElection();
    return;
  }

  listen();

  {
    auto i = m_hosts.readIterator<ZmRBTreeLess>(m_cf.hostID);
    while (ZdbHost *host = i.iterate()) host->connect();
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
    while (ZdbHost *host = i.iterate()) host->cancelConnect();
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
  ZtArray<ZmRef<Cxn> > cxns(n);
  unsigned i = 0;
  {
    ZmRef<Cxn> cxn;
    auto j = m_cxns->readIterator();
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

ZdbHost_::ZdbHost_(ZdbEnv *env, const ZdbHostCf *cf) :
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
  // memset(&m_hbSendHdr, 0, sizeof(Zdb_Msg_Hdr));
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

void ZdbEnv::associate(Cxn *cxn, int hostID)
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

void ZdbHost_::associate(Cxn *cxn)
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

void ZdbHost_::disconnected()
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
	" self:" << m_self << '\n' <<
	" prev:" << m_prev << '\n' <<
	" next:" << m_next << '\n' <<
	" recovering:" << m_recovering << " replicating:" << !!m_nextCxn);
    while (ZdbHost *host = i.iterate()) {
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
  allDBs([](Zdb *db) {
    db->standalone();
    return true;
  });
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
    while (ZdbHost *host = i.iterate()) {
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
  dbStateRefresh();		// must be called after m_nextCxn assignment
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
    while (ZdbHost *host = i.iterate()) host->voted(false);
  }
  m_self->voted(true);
  m_nPeers = 1;
}

namespace Zdb_ {

void Cxn::msgRead(ZiIOContext &io)
{
  using namespace Zdb_;
  ZiRx::recv<Buf>(io,
      loadHdr,
      [this](const ZmRef<Buf> &buf) -> int {
	return verifyHdr(buf, [this](
	      const Hdr *hdr, const ZmRef<Buf> &buf) -> int {
	  using namespace Zfb;
	  using namespace Zdb_;
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
		  "Zdb received garbled message from host " <<
		  ZuBoxed(m_host ? (int)m_host->id() : -1));
	      break;
	  }
	  if (!ok) return -1;
	  mx()->add(ZmFn<>::Member<&Cxn::hbTimeout>::fn(this),
	      ZmTimeNow((int)m_env->config().heartbeatTimeout), &m_hbTimer);
	  return static_cast<unsigned>(hdr->length);
	});
      });
}

bool Cxn::hbRcvd(const Zdb_::fbs::Heartbeat *hb)
{
  if (!m_host) m_env->associate(this, hb->hostID());

  if (!m_host) return false;

  m_env->hbRcvd(m_host, hb);
  return true;
}

} // Zdb_

// process received heartbeat
void ZdbEnv::hbRcvd(ZdbHost *host, const Zdb_::fbs::Heartbeat *hb)
{
  using namespace Zdb_;

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

  guard.unlock();

  // trigger DB vacuuming outside of the env lock scope
  Zfb::Load::all(hb->dbState(),
      [this](unsigned, const fbs::DBState *state) {
    auto id = Zfb::Load::id(&(state->db()));
    ZdbRN rn = state->rn();
    if (auto db = m_dbs.findPtr(id))
      if (db->ack(rn))
	m_mx->run(m_cf.writeTID,
	    ZmFn<>{db, [](Zdb *db) { db->vacuum(); }});
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
  using namespace Zdb_;
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
  hbSend_(nullptr);
  m_mx->add(ZmFn<>::Member<&ZdbEnv::hbSend>::fn(this),
    m_hbSendTime += (time_t)m_cf.heartbeatFreq,
    ZmScheduler::Defer, &m_hbSendTimer);
}

// send heartbeat (either directed, or broadcast if cxn_ is 0)
void ZdbEnv::hbSend_(Cxn *cxn_)
{
  if (!m_self) {
    ZeLOG(Fatal, "ZdbEnv::hbSend_ called out of order");
    return;
  }
  dbStateRefresh();
  if (cxn_) {
    cxn_->hbSend();
    return;
  }
  ZmRef<Cxn> cxn;
  unsigned i = 0, n = m_cxns->count_();
  ZtArray<ZmRef<Cxn> > cxns(n);
  {
    auto j = m_cxns->readIterator();
    while (i < n && (cxn = j.iterateKey())) if (cxn->up()) ++i, cxns.push(cxn);
  }
  for (unsigned j = 0; j < i; j++) cxns[j]->hbSend();
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
  using namespace Zdb_;
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
    dbState.update(db->id(), db->nextRN());
    return true;
  });
}

namespace Zdb_ {

// process received replicated record
bool Cxn::repRcvd(Zdb *db, const fbs::Record *record, const ZmRef<Buf> &buf)
{
  m_env->replicated(db, m_host, record);
  db->replicated(record, buf);
  return true;
}

// process received replicated gap
bool Cxn::repRcvd(Zdb *db, const fbs::Gap *gap, const ZmRef<Buf> &buf)
{
  m_env->replicated(db, m_host, gap);
  db->replicated(gap, buf);
  return true;
}

} // Zdb_

// process replication - host state
void ZdbEnv::replicated(Zdb *db, ZdbHost *host, const Zdb_::fbs::Record *record)
{
  using namespace Zdb_;
  auto id = db->id();
  ZdbRN rn = record->rn() + 1;
  ZdbDEBUG(this, ZtString{} << "replicated(" << id << ", " <<
      host << ", Record{" << record->rn() << "})");
  replicated_(host, id, rn);
}

void ZdbEnv::replicated(Zdb *db, ZdbHost *host, const Zdb_::fbs::Gap *gap)
{
  using namespace Zdb_;
  auto id = db->id();
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
void Zdb::replicated(const Zdb_::fbs::Record *record, const ZmRef<Buf> &rxBuf)
{
  using namespace Zdb_;
  if (auto buf = replicateFwd(rxBuf)) recover_(record, ZuMv(buf));
}

// process replication - database
void Zdb::replicated(const Zdb_::fbs::Gap *gap, const ZmRef<Buf> &rxBuf)
{
  ZdbRN rn = gap->rn();
  ZmRef<Buf> buf = replicateFwd(rxBuf);
  {
    Guard guard(m_lock);
    if (rn != m_nextRN) return;
    m_nextRN = rn + gap->count();
  }
  m_env->write(ZuMv(buf));
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
  data.name = m_id;
  data.cacheMode = m_cf->cacheMode;
  data.warmUp = m_cf->warmUp;
  {
    ReadGuard guard(m_lock);
    data.minRN = m_minRN;
    data.nextRN = m_nextRN;
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
ZmRef<ZdbAnyObject> Zdb::load(const Zdb_::fbs::Record *record)
{
  using namespace Zdb_;
  auto prevRN = record->prevRN();
  int op = record->op();
  if (op == Op::Delete || op == Op::Purge) return nullptr;
  ZmRef<ZdbAnyObject> object;
  Guard guard(m_lock);
  if (prevRN != ZdbNullRN) object = cacheDel_(prevRN);
  auto data = Zfb::Load::bytes(record->data());
  if (!object) {
    object = m_handler.loadFn(this, data.data(), data.length());
  } else {
    object = m_handler.updateFn(object, data.data(), data.length());
  }
  object->load(record->rn(), prevRN, record->seqLenOp());
  return object;
}

// save object to buffer
void Zdb::save(Zfb::Builder &fbb, ZdbAnyObject *object)
{
  m_handler.saveFn(fbb, object->ptr_());
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
      ZmRef<File> file = openFile(id, false);
      if (!file) return false;
      recover(file);
      return true;
    });
    return true;
  });
  return true;
}

void Zdb::recover(File *file)
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
    auto indexBlkID = rn>>indexShift();
    if (!indexBlk || indexBlk->id() != indexBlkID)
      indexBlk = getIndexBlk(file, indexBlkID, false, false);
    if (!indexBlk) return; // I/O error on file, logged within getIndexBlk
    this->recover(FileRec{file, indexBlk, rn & indexMask()});
  }
}

void Zdb::recover(const FileRec &rec)
{
  using namespace Zdb_;
  if (auto buf = read_(rec)) {
    auto record = record_(msg_(buf->hdr()));
    auto rn = record->rn();
    if (m_nextRN <= rn) m_nextRN = rn + 1;
    recover_(record, {});
  }
}

void Zdb::recover_(const Zdb_::fbs::Record *record, ZmRef<Buf> buf)
{
  auto data = Zfb::Load::bytes(record->data());
  ZdbRN rn = record->rn();
  ZdbRN prevRN = record->prevRN();
  ZmRef<ZdbAnyObject> object;
  {
    Guard guard(m_lock);
    if (rn != m_nextRN) return;
    m_nextRN = rn + 1;
    if (rn < m_minRN) m_minRN = rn;
    auto seqLenOp = record->seqLenOp();
    switch (SeqLenOp::op(seqLenOp)) {
      default:
	return;
      case Op::Put:
	if (prevRN != ZdbNullRN)
	  m_deletes.add(rn, DeleteOp{prevRN, seqLenOp});
	break;
      case Op::Append:
	break;
      case Op::Delete:
	m_deletes.add(rn, DeleteOp{rn, seqLenOp});
	return;
      case Op::Purge:
	m_deletes.add(rn, DeleteOp{prevRN, seqLenOp});
	return;
    }
    if (!(object = load(record))) return;
    if (buf) m_writeCache->add(buf);
    cache(object);
  }
  if (object && m_handler.recoverFn) m_handler.recoverFn(object, buf);
  if (buf) m_env->write(ZuMv(buf));
}

namespace Zdb_ {

void File_::init()
{
  m_flags = m_db->config().append ? FileFlags::Append : 0;
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
    if (!!(m_flags & Append) != m_db->config().append) {
      ZeLOG(Error, ZtString{} <<
	  "Zdb file inconsistent append flag in \"" << m_db->fileName(m_id) <<
	  "\"");
      return false;
    }
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
	if (uint64_t offset = indexBlk.data[j].offset)
	  if (offset == ZdbDeleted) {
	    ++m_deleted;
	  } else {
	    offset += indexBlk.data[j].length;
	    FileRecTrlr trlr;
	    if (ZuUnlikely((r = pread(offset,
		      &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
	      m_db->fileRdError_(this, offset, e);
	      m_flags |= FileFlags::IOError;
	      return false;
	    }
	    if (trlr.rn != ((id<<fileShift()) | (i<<indexShift()) | j) ||
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
  m_flags |= FileFlags::Clean;
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
  m_flags &= ~FileFlags::Clean;
  return true;
error:
  m_flags &= ~FileFlags::Clean;
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

} // Zdb_

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
  Guard guard(m_lock);
  // index blocks
  while (IndexBlk *lru = static_cast<IndexBlk *>(m_indexBlkLRU.shiftNode())) {
    if (ZmRef<File> file =
	getFile(lru->id>>(fileShift() - indexShift()), false, false))
      file->writeIndexBlk(lru);
    m_indexBlks->delNode(lru);
  }
  // files
  while (File *file = static_cast<File *>(m_fileLRU.shiftNode())) {
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

void Zdb::checkpoint_()
{
  Guard guard(m_lock);
  {
    auto i = m_indexBlkLRU->readIterator();
    while (IndexBlk *blk = static_cast<IndexBlk *>(i.iterate()))
      if (ZmRef<File> file =
	  getFile(blk->id>>(fileShift() - indexShift()), false, false))
	file->writeIndexBlk(blk);
  }
  {
    auto i = m_fileLRU->readIterator();
    while (File *file = static_cast<File *>(i.iterate()))
      file->sync_();
  }
}

ZmRef<ZdbAnyObject> Zdb::placeholder()
{
  return m_handler.ctorFn(this, ZdbNullRN);
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
  ZdbRN rn;
  {
    Guard guard(m_lock);
    rn = m_nextRN++;
    if (rn < m_minRN) m_minRN = rn;
  }
  return m_handler.ctorFn(this, rn);
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
  Guard guard(m_lock);
  if (ZuUnlikely(rn < m_minRN || rn >= m_nextRN)) return nullptr;
  ++m_cacheLoads;
  if (ZuLikely(object = m_cache->find(rn))) {
    if (m_cacheMode != ZdbCacheMode::FullCache)
      m_lru.push(m_lru.del(object.ptr()));
    return object;
  }
  ++m_cacheMisses;
  {
    ZmRef<Buf> buf = m_writeCache->find(rn);
    if (!buf) {
      FileRec rec = rn2file(rn, false);
      if (rec) buf = read_(rec);
    }
    if (buf) object = load(record_(msg_(buf.hdr())));
  }
  if (ZuUnlikely(!object)) return nullptr;
  cache(object);
  return object;
}

ZmRef<ZdbAnyObject> Zdb::get_(ZdbRN rn)
{
  Guard guard(m_lock);
  if (ZuUnlikely(rn < m_minRN || rn >= m_nextRN)) return nullptr;
  return get__(rn);
}

ZmRef<ZdbAnyObject> Zdb::get__(ZdbRN rn)
{
  ZmRef<ZdbAnyObject> object;
  ++m_cacheLoads;
  if (ZuLikely(object = m_cache->find(rn))) return object;
  ++m_cacheMisses;
  {
    ZmRef<Buf> buf = m_writeCache->find(rn);
    if (!buf) {
      FileRec rec = rn2file(rn, false);
      if (rec) buf = read_(rec);
    }
    if (buf) object = load(record_(msg_(buf.hdr())));
  }
  return object;
}

bool Zdb::exists(ZdbRN rn)
{
  if (ZuLikely(m_cache->find(rn))) return true;
  if (ZuLikely(m_writeCache->find(rn))) return true;
  ZmRef<File> file = getFile(rn>>fileShift(), false, true);
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

bool Zdb::update(ZdbAnyObject *object)
{
  Guard guard(m_lock);
  if (ZuUnlikely(object->seqLen() == SeqLenOp::maxSeqLen())) return false;
  cacheDel_(object);
  ZdbRN rn = m_nextRN++;
  object->pushRN(rn);
  if (rn < m_minRN) m_minRN = rn;
  return true;
}

bool Zdb::update(ZdbAnyObject *object, ZdbRN rn)
{
  if (ZuUnlikely(rn == ZdbNullRN)) return update(object);
  Guard guard(m_lock);
  if (ZuUnlikely(object->seqLen() == SeqLenOp::maxSeqLen())) return false;
  if (ZuUnlikely(m_nextRN != rn)) return false;
  m_nextRN = rn + 1;
  if (rn < m_minRN) m_minRN = rn;
  cacheDel_(object);
  object->pushRN(rn);
  return true;
}

ZmRef<ZdbAnyObject> Zdb::update_(ZdbRN prevRN)
{
  Guard guard(m_lock);
  ZmRef<ZdbAnyObject> object = get__(prevRN);
  if (ZuUnlikely(!object || object->deleted)) return nullptr;
  if (ZuUnlikely(object->seqLen() == SeqLenOp::maxSeqLen())) return nullptr;
  cacheDel_(object);
  ZdbRN rn = m_nextRN++;
  if (rn < m_minRN) m_minRN = rn;
  object->pushRN(rn);
  return object;
}

ZmRef<ZdbAnyObject> Zdb::update_(ZdbRN prevRN, ZdbRN rn)
{
  Guard guard(m_lock);
  if (ZuUnlikely(m_nextRN != rn)) return nullptr;
  m_nextRN = rn + 1;
  if (rn < m_minRN) m_minRN = rn;
  ZmRef<ZdbAnyObject> object = get__(prevRN);
  if (ZuUnlikely(!object || object->deleted)) return nullptr;
  if (ZuUnlikely(object->seqLen() == SeqLenOp::maxSeqLen())) return nullptr;
  cacheDel_(object);
  object->pushRN(rn);
  return object;
}

// commits push() / update()
void Zdb::put(ZdbAnyObject *object)
{
  auto seqLen = object->seqLen();
  if (ZuUnlikely(object->deleted() && !seqLen)) {
    Guard guard(m_lock);
    del__(object->rn());
    object->abortRN();
    return;
  }
  ZmRef<Buf> buf = object->replicate(Zdb_Msg::Rep);
  {
    Guard guard(m_lock);
    object->putRN();
    ++seqLen;
    if (object->deleted()) {
      auto rn = object->rn();
      if (!m_cf->append) seqLen = 2;
      m_deletes.add(rn, DeleteOp{rn, SeqLenOp::mk(seqLen, Op::Delete)});
    } else if (!m_cf->append && seqLen > 1) {
      m_deletes.add(object->rn(),
	  DeleteOp{object->prevRN(), SeqLenOp::mk(1, Op::Put)});
    }
    m_writeCache->add(buf);
    if (!object->deleted()) cache(object);
  }
  m_env->write(ZuMv(buf));
}

// aborts push() / update()
void Zdb::abort(ZdbAnyObject *object)
{
  Guard guard(m_lock);
  del__(object->rn());
  object->abortRN();
  object->undel();
  if (object->rn() != ZdbNullRN) cache(object);
}

// prepare replication data for sending & writing to disk
ZmRef<Buf> ZdbAnyObject::replicate(int type)
{
  ZdbDEBUG(m_db->env(),
      ZtString{} << "ZdbAnyObject::replicate(" << type << ')');
  Zfb::IOBuilder<Buf> fbb;
  Zfb::Offset<Zfb::Vector<uint8_t>> data;
  if (!m_deleted)
    if (auto ptr = this->ptr_()) {
      m_db->save(fbb, this);
      data = Zfb::Save::nest(fbb);
    }
  using namespace Zdb_;
  {
    auto id = Zfb::Save::id(m_db->id());
    unsigned op =
      m_deleted ? Op::Delete : m_db->config().append ? Op::Append : Op::Put;
    auto msg = fbs::CreateMsg(fbb, type,
	fbs::CreateRecord(fbb, &id,
	  m_rn, m_prevRN, SeqLenOp::mk(m_seqLen, op), data));
    fbb.Finish(msg);
  }
  return saveHdr(fbb, m_db);
}

// forward replication data
ZmRef<Buf> Zdb::replicateFwd(const ZmRef<Buf> &rxBuf)
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
  using namespace Zdb_;
  Guard guard(m_lock);
  // recover from outbound replication buffer cache
  if (ZmRef<Buf> txBuf = m_writeCache->find(rn)) {
    auto record = record_(msg_(txBuf->hdr()));
    auto repData = Zfb::Load::bytes(record->data());
    Zfb::IOBuilder<Buf> fbb;
    Offset<Vector<uint8_t>> data;
    if (repData) {
      auto ptr = Zfb::Save::extend(fbb, repData.length(), data);
      if (data && ptr) memcpy(ptr, repData.data(), repData.length());
    }
    auto id = Zfb::Save::id(m_id);
    auto msg = fbs::CreateMsg(fbb, fbs::Body_Rec,
	fbs::CreateRecord(fbb, &id,
	  record->rn(), record->prevRN(), record->seqLenOp(), data).Union());
    fbb.Finish(msg);
    return saveHdr(fbb, this);
  }
  // recover from object cache
  if (ZmRef<ZdbAnyObject> object = m_cache->find(rn))
    return object->replicate(Zdb_Msg::Rec);
  // recover from file
  if (FileRec rec = rn2file(rn, false))
    return read_(rec);
  // unallocated | deleted
  return nullptr;
}

// prepare run-length encoded gap for sending
ZmRef<Buf> Zdb::gap(ZdbRN rn, uint64_t count)
{
  using namespace Zdb_;
  Zfb::IOBuilder<Buf> fbb;
  auto id = Zfb::Save::id(m_id);
  auto msg = fbs::CreateMsg(fbb, fbs::Body_Gap,
      fbs::CreateGap(fbb, rn, count).Union());
  fbb.Finish(msg);
  return saveHdr(fbb, this);
}

void Zdb::purge(ZdbRN minRN)
{
  using namespace Zdb_;
  Zfb::IOBuilder<Buf> fbb;
  {
    auto id = Zfb::Save::id(m_id);
    auto msg = fbs::CreateMsg(fbb, type,
	fbs::CreateRecord(fbb, &id,
	  m_nextRN++, minRN, SeqLenOp::mk(1, Op::Purge), {}));
    fbb.Finish(msg);
  }
  ZmRef<Buf> buf = saveHdr(fbb, this);
  {
    Guard guard(m_lock);
    m_deletes.add(rn, DeleteOp{minRN, SeqLenOp::mk(1, Op::Purge)});
    m_writeCache->add(buf);
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
  using namespace Zdb_;
  if (!m_cf.repMode) m_env->repSend(buf);	// send to replica
  {
    Guard guard(m_lock);
    write_(buf);
    m_writeCache->delNode(buf);
  }
  if (m_handler.logFn) m_handler.logFn(buf);
}

FileRec Zdb::rn2file(ZdbRN rn, bool write)
{
  uint64_t fileID = rn>>fileShift();
  ZmRef<File> file = getFile(fileID, write, true);
  if (!file) return {};
  if (!file->exists(rn & fileRecMask())) return {};
  uint64_t indexBlkID = rn>>indexShift();
  ZmRef<IndexBlk> indexBlk = getIndexBlk(file, indexBlkID, write, true);
  if (!indexBlk) return {};
  auto indexOff = rn & indexMask()
  if (!write && !indexBlk->blk.data[indexOff].offset) return {};
  return {ZuMv(file), ZuMv(indexBlk), indexOff};
}

ZmRef<File> Zdb::getFile(uint64_t id, bool create, bool cache)
{
  ++m_fileLoads;
  ZmRef<File> file;
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
      if (File *lru = static_cast<File *>(m_fileLRU.shiftNode())) {
	lru->sync();
	m_files->delNode(lru);
      }
    m_files->add(file);
    m_fileLRU.push(file.ptr());
  }
  if (id > m_lastFile) m_lastFile = id;
  return file;
}

ZmRef<File> Zdb::openFile(uint64_t id, bool create)
{
  ZiFile::Path name = dirName(id);
  if (create) ZiFile::mkdir(name); // pre-emptive idempotent
  name = fileName(name, id);
  ZmRef<File> file = new File{this, id};
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

void Zdb::delFile(File *file)
{
  bool lastFile;
  uint64_t id = file->id();
  if (m_files->delNode(file)) m_fileLRU.del(file);
  lastFile = id == m_lastFile;
  if (ZuUnlikely(lastFile)) getFile(id + 1, true, true);
  file->close();
  ZiFile::remove(fileName(id));
}

ZmRef<IndexBlk> Zdb::getIndexBlk(
    File *file, uint64_t id, bool create, bool cache)
{
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
      if (IndexBlk *lru = static_cast<IndexBlk *>(m_indexBlkLRU.shiftNode())) {
	if (ZmRef<File> file =
	    getFile(lru->id>>(fileShift() - indexShift()), false, false))
	  file->writeIndexBlk(lru);
	m_indexBlks->delNode(lru);
      }
    m_indexBlks->add(indexBlk);
    m_indexBlkLRU.push(indexBlk.ptr());
  }
  if (id > m_lastIndexBlk) m_lastIndexBlk = id;
  return indexBlk;
}

namespace Zdb_ {

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

} // Zdb_

// read individual record from disk
ZmRef<Buf> Zdb::read_(const FileRec &rec)
{
  using namespace Zdb_;
  const auto &index = rec.index();
  if (!index.offset) return nullptr;
  Zfb::IOBuilder<Buf> fbb;
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
  uint32_t magic = trlr.magic;
  int recType;
  if (magic != ZdbCommitted) return nullptr;
  SeqLen seqLenOp = trlr.seqLenOp;
  auto id = Zfb::Save::id(m_id);
  auto msg = fbs::CreateMsg(fbb, fbs::Body_Rec,
      fbs::CreateRecord(fbb,
	&id, trlr.rn, trlr.prevRN, trlr.seqLenOp, data).Union());
  fbb.Finish(msg);
  return saveHdr(fbb, this);
}

// write individual record to disk
void Zdb::write_(const Buf *buf)
{
  using namespace Zdb_;
  auto record = record_(msg_(buf->hdr()));
  if (ZuUnlikely(!record)) return;
  ZdbRN rn = record->rn();
  auto data = Zfb::Load::bytes(record->data());
  {
    FileRec rec = rn2file(rn, true);
    if (!rec) return;
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
	return;
      }
    }
    // write trailer
    if (ZuUnlikely((r = file->pwrite(index.offset + index.length,
	      &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
      fileWrError_(file, index.offset + index.length, e);
      index.offset = 0;
      index.length = 0;
      return;
    }
  }
  if (m_env->isStandalone()) if (ack(rn + 1)) vacuum();
}

bool Zdb::ack(ZdbRN rn)
{
  Guard guard(m_lock);
  if (ZuUnlikely(rn <= m_minRN)) return false;
  if (ZuUnlikely(rn > m_nextRN)) rn = m_nextRN; // should never happen
  if (m_vacuumRN == ZdbNullRN) {
    auto deleteRN = m_deletes.minimumKey();
    if (ZuUnlikely(deleteRN == ZdbNullRN) return false;
    if (rn <= deleteRN) return false;
    m_vacuumRN = rn;
    return true;
  }
  if (rn > m_vacuumRN) m_vacuumRN = rn;
  return false;
}

void Zdb::vacuum()
{
  Guard guard(m_lock);
  if (ZuUnlikely(m_vacuumRN == ZdbNullRN)) return;
  auto i = m_deletes.iterator();
  while (auto node = i.iterate()) {
    if (node->key() >= m_vacuumRN) break;
    const auto &val = node->val()
    del_(val.rn, val.seqLenOp);
    i.del();
  }
  m_vacuumRN = ZdbNullRN;
}

void Zdb::standalone()
{
  Guard guard(m_lock);
  auto i = m_deletes.iterator();
  while (auto node = i.iterate()) {
    const auto &val = node->val()
    del_(val.rn, val.seqLenOp);
    i.del();
  }
  m_vacuumRN = ZdbNullRN;
}

void Zdb::del_(ZdbRN rn, SeqLen seqLenOp)
{
  switch (SeqLenOp::op(seqLenOp)) {
    default:
      return;
    case Op::Put:
      break;
    case Op::Delete:
      break;
    case Op::Purge: {
      ZdbRN minRN = m_minRN;
      if (minRN < rn) {
	do { del__(minRN++); } while (minRN < rn);
	m_minRN = minRN;
      }
    } return;
  }
  auto seqLen = SeqLenOp::seqLen(seqLenOp);
  auto seq = ZuAlloc(ZdbRN, seqLen);
  if (!seq) return;
  auto i = seqLen;
  do {
    seq[--i] = rn;
    rn = prevRN_(rn);
  } while (i > 0 && rn != ZdbNullRN);
  do {
    del__(seq[i++]);
  } while (i < seqLen);
}

ZdbRN Zdb::prevRN_(ZdbRN rn)
{
  if (ZuUnlikely(rn < m_minRN || rn >= m_nextRN)) return ZdbNullRN;
  if (ZmRef<ZdbAnyObject> object = cacheDel_(rn))
    return object->prevRN();
  FileRec rec = rn2file(rn, false);
  if (!rec) return ZdbNullRN;
  const auto &index = rec.index();
  if (!index.offset) return ZdbNullRN;
  auto offset = index.offset + index.length;
  File *file = rec.file();
  FileRecTrlr trlr;
  int r;
  ZeError e;
  if (ZuUnlikely((r = file->pread(offset
	    &trlr, sizeof(FileRecTrlr), &e)) != Zi::OK)) {
    fileRdError_(file, offset, e);
    return ZdbNullRN;
  }
  uint32_t magic = trlr.magic;
  if (magic != ZdbCommitted) return ZdbNullRN;
  return trlr.prevRN;
}

void Zdb::del__(ZdbRN rn)
{
  FileRec rec = rn2file(rn, false);
  if (!rec) return;
  rec.file()->del(rn & fileRecMask());
  rec.index().offset = 0;
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
