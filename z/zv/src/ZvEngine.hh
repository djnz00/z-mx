//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// ZvEngine - connectivity framework

#ifndef ZvEngine_HH
#define ZvEngine_HH

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <zlib/ZuLambdaTraits.hh>

#include <zlib/ZmRWLock.hh>
#include <zlib/ZmFn.hh>
#include <zlib/ZmPolymorph.hh>
#include <zlib/ZmEngine.hh>
#include <zlib/ZmTime.hh>

#include <zlib/Zfb.hh>

#include <zlib/ZvCf.hh>
#include <zlib/ZvIOQueue.hh>
#include <zlib/ZvMxParams.hh>
#include <zlib/ZvThreadParams.hh>

#include <zlib/zv_engine_state_fbs.h>
#include <zlib/zv_link_state_fbs.h>
#include <zlib/zv_queue_type_fbs.h>

class ZvEngine;

namespace ZvEngineState {
  namespace fbs = Ztel::fbs;
  ZfbEnumMatch(EngineState, ZmEngineState,
      Stopped, Starting, Running, Stopping, StartPending, StopPending);
}

namespace ZvLinkState {
  namespace fbs = Ztel::fbs;
  ZfbEnumValues(LinkState, 
    Down,
    Disabled,
    Deleted,
    Connecting,
    Up,
    ReconnectPending,
    Reconnecting,
    Failed,
    Disconnecting,
    ConnectPending,
    DisconnectPending)
}

namespace ZvQueueType {
  namespace fbs = Ztel::fbs;
  ZfbEnumValues(QueueType, Thread, IPC, Rx, Tx);
}

class ZvAPI ZvAnyTx : public ZmPolymorph {
  ZvAnyTx(const ZvAnyTx &);	//prevent mis-use
  ZvAnyTx &operator =(const ZvAnyTx &);

protected:
  ZvAnyTx(ZuID id);
  
  void init(ZvEngine *engine);

public:
  using Mx = ZiMultiplex;

  static ZuID IDAxor(const ZvAnyTx *tx) { return tx->id(); }

  ZvEngine *engine() const { return m_engine; }
  Mx *mx() const { return m_mx; }
  ZuID id() const { return m_id; }

  template <typename T = uintptr_t>
  T appData() const { return static_cast<T>(m_appData); }
  template <typename T>
  void appData(T v) { m_appData = static_cast<uintptr_t>(v); }

private:
  ZuID			m_id;
  ZvEngine		*m_engine = nullptr;
  Mx			*m_mx = nullptr;
  uintptr_t		m_appData = 0;
};

class ZvAPI ZvAnyTxPool : public ZvAnyTx {
  ZvAnyTxPool(const ZvAnyTxPool &) = delete;
  ZvAnyTxPool &operator =(const ZvAnyTxPool &) = delete;

protected:
  ZvAnyTxPool(ZuID id) : ZvAnyTx(id) { }

public:
  virtual ZvIOQueue *txQueue() = 0;
};

class ZvAPI ZvAnyLink : public ZvAnyTx {
  ZvAnyLink(const ZvAnyLink &) = delete;
  ZvAnyLink &operator =(const ZvAnyLink &) = delete;

friend ZvEngine;

  using StateLock = ZmPRWLock;
  using StateGuard = ZmGuard<StateLock>;
  using StateReadGuard = ZmReadGuard<StateLock>;

protected:
  ZvAnyLink(ZuID id);

public:
  int state() const { return m_state; }
  unsigned reconnects() const { return m_reconnects.load_(); }

  struct Telemetry {
    ZuID	id;
    ZuID	engineID;
    uint64_t	rxSeqNo = 0;
    uint64_t	txSeqNo = 0;
    uint32_t	reconnects = 0;
    int8_t	state = 0;
  };
  void telemetry(Telemetry &data) const;

  void up() { up_(true); }
  void down() { down_(true); }

  virtual void update(const ZvCf *cf) = 0;
  virtual void reset(ZvSeqNo rxSeqNo, ZvSeqNo txSeqNo) = 0;

  ZvSeqNo rxSeqNo() const {
    if (const ZvIOQueue *queue = rxQueue()) return queue->head();
    return 0;
  }
  ZvSeqNo txSeqNo() const {
    if (const ZvIOQueue *queue = txQueue()) return queue->tail();
    return 0;
  }

  virtual ZvIOQueue *rxQueue() = 0;
  virtual const ZvIOQueue *rxQueue() const = 0;
  virtual ZvIOQueue *txQueue() = 0;
  virtual const ZvIOQueue *txQueue() const = 0;

protected:
  virtual void connect() = 0;
  virtual void disconnect() = 0;

  void connected();
  void disconnected();
  void reconnecting();	// transition direct from Up to Connecting
  void reconnect(bool immediate);

  virtual ZuTime reconnInterval(unsigned) { return ZuTime{1}; }

private:
  void up_(bool enable);
  void down_(bool disable);

  void reconnect_();

  void deleted_();	// called from ZvEngine::delLink

private:
  ZmScheduler::Timer	m_reconnTimer;

  StateLock		m_stateLock;
    ZmAtomic<int>	  m_state = ZvLinkState::Down;
    ZmAtomic<unsigned>	  m_reconnects;
    bool		  m_enabled = true;
};

// Callbacks to the application from the engine implementation
struct ZvAPI ZvEngineApp {
  virtual ZmRef<ZvAnyLink> createLink(ZuID) = 0;
};

// Note: When event/flow steering, referenced objects must remain
// in scope until eventually called by queued member functions; however
// this is only a routine problem for transient objects such as messages.
// It should not be necessary to frequently adjust the reference count of
// semi-persistent objects such as links and engines, since most of the time
// they remain in scope. Furthermore, atomic operations on reference
// counts are somewhat expensive and should be minimized.
//
// Accordingly, when enqueuing work involving links/engines, 'this'
// can be directly captured as a raw pointer, without reference counting.
// Corresponding code that deletes links or engines needs to take extra
// care not to destroy them while they could remain referenced by outstanding
// work; to assure this, perform teardown as follows:
// 1] De-index the link/engine and disable it so it will not be further used
// 2] Initialize a temporary semaphore
// 3] Enqueue a function that posts the semaphore onto each of the threads
//    that could potentially do work involving the link/engine being deleted;
//    each semaphore post will be executed after all the work ahead of it
// 4] Wait for the semaphore to be posted as many times as there are threads
// 5] Destroy the link/engine, safe in the knowledge that no outstanding work
//    involving it can remain enqueued or in progress on any of the threads

struct ZvQueueTelemetry {
  ZuID		id;		// primary key - same as Link id for Rx/Tx
  uint64_t	seqNo = 0;	// 0 for Thread, IPC
  uint64_t	count = 0;	// dynamic - may not equal in - out
  uint64_t	inCount = 0;	// dynamic (*)
  uint64_t	inBytes = 0;	// dynamic
  uint64_t	outCount = 0;	// dynamic (*)
  uint64_t	outBytes = 0;	// dynamic
  uint32_t	size = 0;	// 0 for Rx, Tx
  uint32_t	full = 0;	// dynamic - how many times queue overflowed
  int8_t	type = -1;	// primary key - QueueType
};

struct ZvEngineMgr {
  using QueueFn = ZmFn<void(ZvQueueTelemetry &)>;

  // Engine Management
  virtual void addEngine(ZvEngine *) { }
  virtual void delEngine(ZvEngine *) { }
  virtual void updEngine(ZvEngine *) { }

  // Link Management
  virtual void updLink(ZvAnyLink *) { }

  // Queue Management
  virtual void addQueue(unsigned type, ZuID id, QueueFn queueFn) { }
  virtual void delQueue(unsigned type, ZuID id) { }
};

class ZvAPI ZvEngine : public ZmPolymorph, public ZmEngine<ZvEngine> {
  ZvEngine(const ZvEngine &);	//prevent mis-use
  ZvEngine &operator =(const ZvEngine &);

friend ZmEngine<ZvEngine>;
friend ZvAnyLink;

  using Lock = ZmPRWLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  using StateLock = ZmPRWLock;
  using StateGuard = ZmGuard<StateLock>;
  using StateReadGuard = ZmReadGuard<StateLock>;

public:
  static ZuID IDAxor(const ZvEngine *e) { return e->id(); }

  using Mx = ZiMultiplex;
  using Mgr = ZvEngineMgr;
  using App = ZvEngineApp;
  using QueueFn = ZvEngineMgr::QueueFn;

  ZvEngine() { }

  bool init(Mgr *mgr, App *app, Mx *mx, const ZvCf *cf) {
    return ZmEngine<ZvEngine>::lock(
	ZmEngineState::Stopped, [this, mgr, app, mx, cf]() {
      m_mgr = mgr;
      m_app = app;
      m_id = cf->get("id", true);
      m_mx = mx;
      if (ZuCSpan s = cf->get("rxThread"))
	m_rxThread = mx->sid(s);
      else
	m_rxThread = mx->rxThread();
      if (ZuCSpan s = cf->get("txThread"))
	m_txThread = mx->sid(s);
      else
	m_txThread = mx->txThread();
      return true;
    });
  }
  bool final();

  Mgr *mgr() const { return m_mgr; }
  ZvEngineApp *app() const { return m_app; }
  ZuID id() const { return m_id; }
  Mx *mx() const { return m_mx; }
  unsigned rxThread() const { return m_rxThread; }
  unsigned txThread() const { return m_txThread; }

  template <typename ...Args> void rxRun(Args &&...args)
    { m_mx->run(m_rxThread, ZuFwd<Args>(args)...); }
  template <typename ...Args> void rxPush(Args &&...args)
    { m_mx->push(m_rxThread, ZuFwd<Args>(args)...); }
  template <typename ...Args> void rxInvoke(Args &&...args)
    { m_mx->invoke(m_rxThread, ZuFwd<Args>(args)...); }
  template <typename ...Args> void txRun(Args &&...args)
    { m_mx->run(m_txThread, ZuFwd<Args>(args)...); }
  template <typename ...Args> void txInvoke(Args &&...args)
    { m_mx->invoke(m_txThread, ZuFwd<Args>(args)...); }

  void mgrAddEngine() { mgr()->addEngine(this); }
  void mgrDelEngine() { mgr()->delEngine(this); }

  ZmRef<ZvAnyLink> appCreateLink(ZuID id) {
    return app()->createLink(id);
  }
  void mgrUpdLink(ZvAnyLink *link) { mgr()->updLink(link); }

  // Note: ZvIOQueues are contained in Link and TxPool
  void mgrAddQueue(unsigned type, ZuID id, QueueFn queueFn)
    { mgr()->addQueue(type, id, ZuMv(queueFn)); }
  void mgrDelQueue(unsigned type, ZuID id)
    { mgr()->delQueue(type, id); }

  // generic O.S. error logging
  auto osError(const char *op, int result, ZeError e) {
    return [id = m_id, op, result, e](ZeLogBuf &s) {
      s << id << " - " << op << " - " << Zi::ioResult(result) << " - " << e;
    };
  }

  struct Telemetry {
    ZuID	id;		// primary key
    ZuID	type;
    ZuID	mxID;
    uint16_t	down = 0;
    uint16_t	disabled = 0;
    uint16_t	transient = 0;
    uint16_t	up = 0;
    uint16_t	reconn = 0;
    uint16_t	failed = 0;
    uint16_t	nLinks = 0;
    uint16_t	rxThread = 0;
    uint16_t	txThread = 0;
    int8_t	state = -1;
  };

  void telemetry(Telemetry &data) const;

private:
  using TxPools_ =
    ZmRBTree<ZmRef<ZvAnyTxPool>,
      ZmRBTreeKey<ZvAnyTxPool::IDAxor,
	ZmRBTreeUnique<true>>>;
  struct TxPools : public TxPools_ { using TxPools_::TxPools_; };
  using Links_ =
    ZmRBTree<ZmRef<ZvAnyLink>,
      ZmRBTreeKey<ZvAnyLink::IDAxor,
	ZmRBTreeUnique<true>>>;
  struct Links : public Links_ { using Links_::Links_; };

public:
  ZmRef<ZvAnyTxPool> txPool(ZuID id) {
    ReadGuard guard(m_lock);
    return m_txPools.findVal(id);
  }
  template <typename TxPool>
  ZmRef<ZvAnyTxPool> updateTxPool(ZuID id, const ZvCf *cf) {
    Guard guard(m_lock);
    ZmRef<TxPool> pool;
    if (pool = m_txPools.findVal(id)) {
      guard.unlock();
      pool->update(cf);
      return pool;
    }
    pool = new TxPool(*this, id);
    m_txPools.add(pool);
    guard.unlock();
    pool->update(cf);
    mgrAddQueue(ZvQueueType::Tx, id, [pool](ZvQueueTelemetry &data) {
      const ZvIOQueue *queue = pool->txQueue();
      data.id = pool->id();
      data.seqNo = queue->head();
      data.count = queue->count_();
      queue->stats(data.inCount, data.inBytes, data.outCount, data.outBytes);
      data.size = data.full = 0;
      data.type = ZvQueueType::Tx;
    });
    return pool;
  }
  ZmRef<ZvAnyTxPool> delTxPool(ZuID id) {
    Guard guard(m_lock);
    ZmRef<ZvAnyTxPool> txPool;
    if (txPool = m_txPools.delVal(id)) {
      guard.unlock();
      mgrDelQueue(ZvQueueType::Tx, id);
    }
    return txPool;
  }

  ZmRef<ZvAnyLink> link(ZuID id) {
    ReadGuard guard(m_lock);
    return m_links.findVal(id);
  }
  ZmRef<ZvAnyLink> updateLink(ZuID id, const ZvCf *cf) {
    Guard guard(m_lock);
    ZmRef<ZvAnyLink> link;
    if (link = m_links.findVal(id)) {
      guard.unlock();
      link->update(cf);
      mgrUpdLink(link);
      return link;
    }
    link = appCreateLink(id);
    link->init(this);
    m_links.add(link);
    guard.unlock();
    linkState(link, -1, link->state());
    link->update(cf);
    mgrUpdLink(link);
    mgrAddQueue(ZvQueueType::Rx, id, [link](ZvQueueTelemetry &data) {
      const ZvIOQueue *queue = link->rxQueue();
      data.id = link->id();
      data.seqNo = queue->head();
      data.count = queue->count_();
      queue->stats(data.inCount, data.inBytes, data.outCount, data.outBytes);
      data.size = data.full = 0;
      data.type = ZvQueueType::Rx;
    });
    mgrAddQueue(ZvQueueType::Tx, id, [link](ZvQueueTelemetry &data) {
      const ZvIOQueue *queue = link->txQueue();
      data.id = link->id();
      data.seqNo = queue->head();
      data.count = queue->count_();
      queue->stats(data.inCount, data.inBytes, data.outCount, data.outBytes);
      data.size = data.full = 0;
      data.type = ZvQueueType::Tx;
    });
    return link;
  }
  ZmRef<ZvAnyLink> delLink(ZuID id) {
    Guard guard(m_lock);
    ZmRef<ZvAnyLink> link;
    if (link = m_links.delVal(id)) {
      guard.unlock();
      mgrDelQueue(ZvQueueType::Rx, id);
      mgrDelQueue(ZvQueueType::Tx, id);
      link->deleted_();	// calls linkState(), mgrUpdLink()
    }
    return link;
  }
  unsigned nLinks() const {
    ReadGuard guard(m_lock);
    return m_links.count_();
  }
  template <typename Link, typename L> bool allLinks(L l) {
    ReadGuard guard(m_lock);
    auto i = m_links.readIterator();
    while (ZvAnyLink *link = i.iterateVal())
      if (!l(static_cast<Link *>(link))) return false;
    return true;
  }

private:
  void linkState(ZvAnyLink *, int prev, int next);

  void start_();
  void stop_();
  void stateChanged() { mgr()->updEngine(this); }

private:
  ZuID				m_id;
  Mgr				*m_mgr = 0;
  App				*m_app = 0;
  Mx				*m_mx;
  unsigned			m_rxThread = 0;
  unsigned			m_txThread = 0;

  Lock				m_lock;
    TxPools			  m_txPools;	// from csv
    Links			  m_links;	// from csv

  StateLock			m_stateLock;
    unsigned			  m_down = 0;		// #links down
    unsigned			  m_disabled = 0;	// #links disabled
    unsigned			  m_transient = 0;	// #links transient
    unsigned			  m_up = 0;		// #links up
    unsigned			  m_reconn = 0;		// #links reconnecting
    unsigned			  m_failed = 0;		// #links failed
};

template <typename Impl, typename Base>
class ZvTx : public Base {
  using Tx = ZvIOQueueTx<Impl>;

public:
  using Mx = ZiMultiplex;
  using Gap = ZvIOQueue::Gap;

  ZvTx(ZuID id) : Base{id} { }
  
  void init(ZvEngine *engine) { Base::init(engine); }

  auto impl() const { return static_cast<const Impl *>(this); }
  auto impl() { return static_cast<Impl *>(this); }

  template <typename L> void txRun(L &&l)
    { this->engine()->txRun(ZmFn<>{impl()->tx(), ZuFwd<L>(l)}); }
  template <typename L> void txRun(L &&l) const
    { this->engine()->txRun(ZmFn<>{impl()->tx(), ZuFwd<L>(l)}); }
  template <typename L> void txInvoke(L &&l)
    { this->engine()->txInvoke(impl()->tx(), ZuFwd<L>(l)); }
  template <typename L> void txInvoke(L &&l) const
    { this->engine()->txInvoke(impl()->tx(), ZuFwd<L>(l)); }

  void scheduleSend() { txInvoke([](Tx *tx) { tx->send(); }); }
  void rescheduleSend() { txRun([](Tx *tx) { tx->send(); }); }
  void idleSend() { }

  void scheduleResend() { txInvoke([](Tx *tx) { tx->resend(); }); }
  void rescheduleResend() { txRun([](Tx *tx) { tx->resend(); }); }
  void idleResend() { }

  void scheduleArchive() { rescheduleArchive(); }
  void rescheduleArchive() { txRun([](Tx *tx) { tx->archive(); }); }
  void idleArchive() { }
};

// CRTP - implementation must conform to the following interface:
#if 0
struct TxPoolImpl : public ZvTxPool<TxPoolImpl> {
  ZuTime reconnInterval(unsigned reconnects); // optional - defaults to 1sec
};
#endif

template <typename Impl> class ZvTxPool :
  public ZvTx<Impl, ZvAnyTxPool>,
  public ZvIOQueueTxPool<Impl> {

  using Base = ZvTx<Impl, ZvAnyTxPool>;

public:
  using Tx_ = ZmPQTx<Impl, ZvIOQueue, ZmNoLock>;
  using Tx = ZvIOQueueTxPool<Impl>;

  ZvTxPool(ZuID id) : Base{id} { }

  const ZvIOQueue *txQueue() const { return ZvIOQueueTxPool<Impl>::txQueue(); }
  ZvIOQueue *txQueue() { return ZvIOQueueTxPool<Impl>::txQueue(); }

  const Tx *tx() const { return static_cast<const Tx *>(this); }
  Tx *tx() { return static_cast<Tx *>(this); }

  void send(ZmRef<ZvIOMsg> msg) {
    msg->owner(tx());
    this->engine()->txInvoke(ZuMv(msg), [](ZmRef<ZvIOMsg> msg) {
      msg->owner<Tx *>()->send(ZuMv(msg));
    });
  }
  template <typename L>
  void abort(ZvSeqNo seqNo, L l) {
    this->txInvoke([seqNo, l = ZuMv(l)](Tx *tx) mutable {
      l(tx->abort(seqNo));
    });
  }

private:
  // prevent direct call from Impl - must be called via txRun/txInvoke
  using Tx_::start;		// Tx - start
  using Tx_::stop;		// Tx - stop
  // using Tx::send;		// Tx - send (from app)
  // using Tx::abort;		// Tx - abort (from app)
  using Tx::unload;		// Tx - unload all messages (for reload)
  using Tx::txReset;		// Tx - reset sequence numbers
  using Tx::join;		// Tx - join pool
  using Tx::leave;		// Tx - leave pool
  // should not be called from Impl at all
  using Tx_::ackd;		// handled by ZvIOQueueTxPool
  using Tx_::resend;		// handled by ZvTx
  using Tx_::archive;		// handled by ZvTx
  using Tx_::archived;		// handled by ZvIOQueueTxPool
  using Tx::ready;		// handled by ZvIOQueueTxPool
  using Tx::unready;		// handled by ZvIOQueueTxPool
  using Tx::ready_;		// internal to ZvIOQueueTxPool
  using Tx::unready_;		// internal to ZvIOQueueTxPool
};

// CRTP - implementation must conform to the following interface:
// (Note: can be derived from TxPoolImpl above)
#if 0
struct Link : public ZvLink<Link> {
  ZuTime reconnInterval(unsigned reconnects); // optional - defaults to 1sec

  // Rx
  void process(ZvIOMsg *);
  ZuTime reReqInterval(); // resend request interval
  void request(const ZvIOQueue::Gap &prev, const ZvIOQueue::Gap &now);
  void reRequest(const ZvIOQueue::Gap &now);

  // Tx
  void loaded_(ZvIOMsg *msg);
  void unloaded_(ZvIOMsg *msg);

  bool send_(ZvIOMsg *msg, bool more); // true on success
  bool resend_(ZvIOMsg *msg, bool more); // true on success
  void aborted_(ZvIOMsg *msg);

  bool sendGap_(const ZvIOQueue::Gap &gap, bool more); // true on success
  bool resendGap_(const ZvIOQueue::Gap &gap, bool more); // true on success
};
#endif

template <typename Impl> class ZvLink :
  public ZvTx<Impl, ZvAnyLink>,
  public ZvIOQueueRx<Impl>, public ZvIOQueueTx<Impl> {

  using Base = ZvTx<Impl, ZvAnyLink>;

public:
  using Rx_ = ZmPQRx<Impl, ZvIOQueue, ZmNoLock>;
  using Rx = ZvIOQueueRx<Impl>;
  using Tx_ = ZmPQTx<Impl, ZvIOQueue, ZmNoLock>;
  using Tx = ZvIOQueueTx<Impl>;

  auto impl() const { return static_cast<const Impl *>(this); }
  auto impl() { return static_cast<Impl *>(this); }

  const Rx *rx() const { return static_cast<const Rx *>(this); }
  Rx *rx() { return static_cast<Rx *>(this); }
  const Tx *tx() const { return static_cast<const Tx *>(this); }
  Tx *tx() { return static_cast<Tx *>(this); }

  ZvLink(ZuID id) : Base{id} { }

  void init(ZvEngine *engine) { Base::init(engine); }

  void scheduleDequeue() { rescheduleDequeue(); }
  void rescheduleDequeue() { rxRun([](Rx *rx) { rx->dequeue(); }); }
  void idleDequeue() { }

  using RRLock = ZmPLock;
  using RRGuard = ZmGuard<RRLock>;

  void scheduleReRequest() {
    RRGuard guard(m_rrLock);
    if (ZuLikely(!m_rrTime)) scheduleReRequest_(guard);
  }
  void rescheduleReRequest() {
    RRGuard guard(m_rrLock);
    scheduleReRequest_(guard);
  }
  void scheduleReRequest_(RRGuard &guard) {
    ZuTime interval = impl()->reReqInterval();
    if (!interval) return;
    m_rrTime = Zm::now();
    ZuTime rrTime = (m_rrTime += interval);
    guard.unlock();
    rxRun([](Rx *rx) { rx->reRequest(); }, rrTime, &m_rrTimer);
  }
  void cancelReRequest() {
    this->mx()->del(&m_rrTimer);
    {
      RRGuard guard(m_rrLock);
      m_rrTime = ZuTime();
    }
  }

  const ZvIOQueue *rxQueue() const { return ZvIOQueueRx<Impl>::rxQueue(); }
  ZvIOQueue *rxQueue() { return ZvIOQueueRx<Impl>::rxQueue(); }
  const ZvIOQueue *txQueue() const { return ZvIOQueueTx<Impl>::txQueue(); }
  ZvIOQueue *txQueue() { return ZvIOQueueTx<Impl>::txQueue(); }

  using ZvAnyLink::rxSeqNo;
  using ZvAnyLink::txSeqNo;

  template <typename L, typename ...Args>
  void rxRun(L &&l, Args &&...args)
    { this->engine()->rxRun(ZmFn<>{rx(), ZuFwd<L>(l)}, ZuFwd<Args>(args)...); }
  template <typename L, typename ...Args>
  void rxRun(L &&l, Args &&...args) const
    { this->engine()->rxRun(ZmFn<>{rx(), ZuFwd<L>(l)}, ZuFwd<Args>(args)...); }
  template <typename L> void rxPush(L &&l)
    { this->engine()->rxPush(ZmFn<>{rx(), ZuFwd<L>(l)}); }
  template <typename L> void rxPush(L &&l) const
    { this->engine()->rxPush(ZmFn<>{rx(), ZuFwd<L>(l)}); }
  template <typename L> void rxInvoke(L &&l)
    { this->engine()->rxInvoke(rx(), ZuFwd<L>(l)); }
  template <typename L> void rxInvoke(L &&l) const
    { this->engine()->rxInvoke(rx(), ZuFwd<L>(l)); }

  template <auto Rcvd>
  void received(ZmRef<ZvIOMsg> msg) {
    msg->owner(rx());
    this->engine()->rxInvoke(ZuMv(msg), [](ZmRef<ZvIOMsg> msg) {
      Rx *rx = msg->owner<Rx *>();
      rx->received(ZuMv(msg));
      ZuInvoke<Rcvd, ZuTypeList<Rx *>>(rx);
    });
  }
  void received(ZmRef<ZvIOMsg> msg) {
    msg->owner(rx());
    this->engine()->rxInvoke(ZuMv(msg), [](ZmRef<ZvIOMsg> msg) {
      Rx *rx = msg->owner<Rx *>();
      rx->received(ZuMv(msg));
    });
  }

  void send(ZmRef<ZvIOMsg> msg) {
    // permit callers to use code of the form send(mkMsg(...))
    // (i.e. without needing to explicitly check mkMsg() success/failure)
    if (ZuUnlikely(!msg)) return;
    msg->owner(tx());
    this->engine()->txInvoke(ZuMv(msg), [](ZmRef<ZvIOMsg> msg) {
      msg->owner<Tx *>()->send(ZuMv(msg));
    });
  }
  template <typename L>
  void abort(ZvSeqNo seqNo, L l) {
    this->txInvoke([seqNo, l = ZuMv(l)](Tx *tx) mutable {
      l(tx->abort(seqNo));
    });
  }
  void archived(ZvSeqNo seqNo) {
    this->txInvoke([seqNo](Tx *tx) { tx->archived(seqNo); });
  }

private:
  // prevent direct call from Impl - must be called via rx/tx Run/Invoke
  using Rx_::rxReset;		// Rx - reset sequence numbers
  using Rx_::startQueuing;	// Rx - start queuing
  using Rx_::stopQueuing;	// Rx - stop queuing (start processing)
  // using Rx_::received;	// Rx - received (from network)
  using Tx_::start;		// Tx - start
  using Tx_::stop;		// Tx - stop
  using Tx_::ackd;		// Tx - ackd (due to received message)
  using Tx_::archived;		// Tx - archived (following call to archive_)
  // using Tx::send;		// Tx - send (from app)
  // using Tx::abort;		// Tx - abort (from app)
  using Tx::unload;		// Tx - unload all messages (for reload)
  using Tx::txReset;		// Tx - reset sequence numbers
  using Tx::join;		// Tx - join pool
  using Tx::leave;		// Tx - leave pool
  using Tx::ready;		// Tx - inform pool(s) of readiness
  using Tx::unready;		// Tx - inform pool(s) not ready
  // should not be called from Impl at all
  using Rx_::dequeue;		// handled by ZvLink
  using Rx_::reRequest;		// handled by ZvLink
  using Tx_::resend;		// handled by ZvTx
  using Tx_::archive;		// handled by ZvTx
  using Tx::ready_;		// internal to ZvIOQueueTx
  using Tx::unready_;		// internal to ZvIOQueueTx

  ZmScheduler::Timer	m_rrTimer;

  RRLock		m_rrLock;
    ZuTime		  m_rrTime;
};

#endif /* ZvEngine_HH */
