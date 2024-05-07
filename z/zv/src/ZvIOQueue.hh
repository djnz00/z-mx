//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// concrete generic I/O queue based on ZmPQueue skip lists, used by ZvEngine
//
// Key / SeqNo - uint64
// Link ID - ZuID (union of 8-byte string with uint64)

#ifndef ZvIOQueue_HH
#define ZvIOQueue_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <zlib/ZuArrayN.hh>
#include <zlib/ZuRef.hh>
#include <zlib/ZuID.hh>

#include <zlib/ZmPQueue.hh>
#include <zlib/ZmFn.hh>
#include <zlib/ZmRBTree.hh>

#include <zlib/ZiIOBuf.hh>
#include <zlib/ZiIP.hh>
#include <zlib/ZiMultiplex.hh>

#include <zlib/ZvSeqNo.hh>
#include <zlib/ZvMsgID.hh>

struct ZvIOQItem : public ZmPolymorph {
  ZvIOQItem(ZmRef<ZiAnyIOBuf> buf) :
      m_buf(ZuMv(buf)) { }
  ZvIOQItem(ZmRef<ZiAnyIOBuf> buf, ZvMsgID id) :
      m_buf(ZuMv(buf)), m_id(id) { }

  template <typename T = ZiAnyIOBuf>
  const T *buf() const { return m_buf.ptr<T>(); }
  template <typename T = ZiAnyIOBuf>
  T *buf() { return m_buf.ptr<T>(); }

  template <typename T = void *> T owner() const {
    return reinterpret_cast<T>(m_owner);
  }
  template <typename T> void owner(T v) { m_owner = v; }

  const ZvMsgID id() const { return m_id; }

  void load(const ZvMsgID &id) { m_id = id; }
  void unload() { m_id = ZvMsgID{}; }

  unsigned skip() const { return m_skip <= 0 ? 1 : m_skip; }
  void skip(unsigned n) { m_skip = n; }

  bool noQueue() const { return m_skip < 0; }
  void noQueue(int b) { m_skip = -b; }

private:
  ZmRef<ZiAnyIOBuf>	m_buf;
  void			*m_owner = nullptr;
  ZvMsgID		m_id;
  int32_t		m_skip = 0;	// -ve - no queuing; +ve - gap fill
};

class ZvIOQFn {
public:
  using Key = ZvSeqNo;
  static Key KeyAxor(const ZvIOQItem &item) { return item.id().seqNo; }

  ZvIOQFn(ZvIOQItem &item) : m_item(item) { }

  Key key() const { return KeyAxor(m_item); }
  unsigned length() const { return m_item.skip(); }
  unsigned clipHead(unsigned) { return length(); }
  unsigned clipTail(unsigned) { return length(); }
  void write(const ZvIOQFn &) { }
  unsigned bytes() const { return m_item.buf()->length; }

private:
  ZvIOQItem	&m_item;
};

inline constexpr const char *ZvIOMsg_HeapID() { return "ZvIOMsg"; };
using ZvIOQueue_ =
  ZmPQueue<ZvIOQItem,
    ZmPQueueNode<ZvIOQItem,
      ZmPQueueFn<ZvIOQFn,
	ZmPQueueHeapID<ZvIOMsg_HeapID>>>>;
using ZvIOMsg = ZvIOQueue_::Node;
using ZvIOQGap = ZvIOQueue_::Gap;
struct ZvIOQueue : public ZmObject, public ZvIOQueue_ {
  using ZvIOQueue_::ZvIOQueue_;
};

// ZvIOQueueRx - receive queue

// CRTP - application must conform to the following interface:
#if 0
struct Impl : public ZvIOQueueRx<Impl> {
  void process(ZvIOMsg *);		// rx process

  void scheduleDequeue();
  void rescheduleDequeue();
  void idleDequeue();

  void scheduleReRequest();
  void cancelReRequest();

  void request(const ZvIOQueue::Gap &prev, const ZvIOQueue::Gap &now);
  void reRequest(const ZvIOQueue::Gap &now);
};
#endif

template <class Impl, class Lock_ = ZmNoLock>
class ZvIOQueueRx : public ZmPQRx<Impl, ZvIOQueue, Lock_> {
  using Rx = ZmPQRx<Impl, ZvIOQueue, Lock_>;

public:
  using Lock = Lock_;
  using Guard = ZmGuard<Lock>;

  ZvIOQueueRx() : m_queue{new ZvIOQueue{ZvSeqNo{}}} { }
  
  auto impl() const { return static_cast<const Impl *>(this); }
  auto impl() { return static_cast<Impl *>(this); }

  const ZvIOQueue *rxQueue() const { return m_queue; }
  ZvIOQueue *rxQueue() { return m_queue; }

  void rxInit(ZvSeqNo seqNo) {
    if (seqNo > m_queue->head()) m_queue->head(seqNo);
  }

private:
  ZmRef<ZvIOQueue>	m_queue;
};

// ZvIOQueueTx - transmit queue
// ZvIOQueueTxPool - transmit fan-out queue

#define ZvIOQueueMaxPools 8	// max #pools a tx queue can be a member of

template <class Impl, class Lock> class ZvIOQueueTxPool;

// CRTP - application must conform to the following interface:
#if 0
struct Impl : public ZvIOQueueTx<Impl> {
  void archive_(ZvIOMsg *);			// tx archive (persistent)
  ZmRef<ZvIOMsg> retrieve_(ZvSeqNo, ZvSeqNo);	// tx retrieve

  void scheduleSend();
  void rescheduleSend();
  void idleSend();

  void scheduleResend();
  void rescheduleResend();
  void idleResend();

  void scheduleArchive();
  void rescheduleArchive();
  void idleArchive();

  void loaded_(ZvIOMsg *msg);		// may adjust readiness
  void unloaded_(ZvIOMsg *msg);		// ''

  // send_() must perform one of
  // 1] usual case (successful send): persist seqNo, return true
  // 2] stale message: abort message, return true
  // 3] transient failure (throttling, I/O, etc.): return false
  bool send_(ZvIOMsg *msg, bool more);
  // resend_() must perform one of
  // 1] usual case (successful resend): return true
  // 2] transient failure (throttling, I/O, etc.): return false
  bool resend_(ZvIOMsg *msg, bool more);

  // sendGap_() and resendGap_() return true, or false on transient failure
  bool sendGap_(const ZvIOQueue::Gap &gap, bool more);
  bool resendGap_(const ZvIOQueue::Gap &gap, bool more);
};

struct Impl : public ZvIOQueueTxPool<Impl> {
  // Note: below member functions have same signature as above
  void scheduleSend();
  void rescheduleSend();
  void idleSend();

  void scheduleResend();
  void rescheduleResend();
  void idleResend();

  void scheduleArchive();
  void rescheduleArchive();
  void idleArchive();

  void aborted_(ZvIOMsg *msg);

  void loaded_(ZvIOMsg *msg);		// may adjust readiness
  void unloaded_(ZvIOMsg *msg);		// ''
};
#endif

template <class Impl, class Lock_ = ZmNoLock>
class ZvIOQueueTx : public ZmPQTx<Impl, ZvIOQueue, Lock_> {
  using Tx = ZmPQTx<Impl, ZvIOQueue, Lock_>;
  using Pool = ZvIOQueueTxPool<Impl, Lock_>;
  using Pools = ZuArrayN<Pool *, ZvIOQueueMaxPools>;

public:
  using Lock = Lock_;
  using Guard = ZmGuard<Lock>;

protected:
  const Lock &lock() const { return m_lock; }
  Lock &lock() { return m_lock; }

public:
  ZvIOQueueTx() : m_queue{new ZvIOQueue{ZvSeqNo{}}} { }

  auto impl() const { return static_cast<const Impl *>(this); }
  auto impl() { return static_cast<Impl *>(this); }

  const ZvSeqNo txSeqNo() const { return m_seqNo; }

  const ZvIOQueue *txQueue() const { return m_queue; }
  ZvIOQueue *txQueue() { return m_queue; }

  void txInit(ZvSeqNo seqNo) {
    if (seqNo > m_seqNo) m_queue->head(m_seqNo = seqNo);
  }

  void send() { Tx::send(); }
  void send(ZmRef<ZvIOMsg> msg) {
    if (ZuUnlikely(msg->noQueue())) {
      Guard guard(m_lock);
      if (ZuUnlikely(!m_ready)) {
	guard.unlock();
	impl()->aborted_(ZuMv(msg));
	return;
      }
    }
    msg->load(ZvMsgID{impl()->id(), m_seqNo++});
    impl()->loaded_(msg);
    Tx::send(ZuMv(msg));
  }
  bool abort(ZvSeqNo seqNo) {
    ZmRef<ZvIOMsg> msg = Tx::abort(seqNo);
    if (msg) {
      impl()->aborted_(msg);
      impl()->unloaded_(msg);
      msg->unload();
      return true;
    }
    return false;
  }

  // unload all messages from queue
  void unload(ZmFn<ZvIOMsg *> fn) {
    while (ZmRef<ZvIOMsg> msg = m_queue->shift()) {
      impl()->unloaded_(msg);
      msg->unload();
      fn(msg);
    }
  }

  void ackd(ZvSeqNo seqNo) {
    if (m_seqNo < seqNo) m_seqNo = seqNo;
    Tx::ackd(seqNo);
  }

  void txReset(ZvSeqNo seqNo = ZvSeqNo{}) {
    Tx::txReset(m_seqNo = seqNo);
  }

  // fails silently if ZvIOQueueMaxPools exceeded
  void join(Pool *g) {
    Guard guard(m_lock);
    m_pools << g;
  }
  void leave(Pool *g) {
    Guard guard(m_lock);
    unsigned i, n = m_pools.length();
    for (i = 0; i < n; i++)
      if (m_pools[i] == g) {
	m_pools.splice(i, 1);
	return;
      }
  }

  void ready() { // ready to send immediately
    Guard guard(m_lock);
    ready_(ZmTime(0, 1));
  }
  void ready(ZmTime next) { // ready to send at time next
    Guard guard(m_lock);
    ready_(next);
  }
  void unready() { // not ready to send
    Guard guard(m_lock);
    unready_();
  }
protected:
  void ready_(ZmTime next);
  void unready_();

private:

  ZvSeqNo		m_seqNo;
  ZmRef<ZvIOQueue>	m_queue;

  Lock			m_lock;
    Pools		  m_pools;
    unsigned		  m_poolOffset = 0;
    ZmTime		  m_ready;
};

template <class Impl, class Lock_ = ZmNoLock>
class ZvIOQueueTxPool : public ZvIOQueueTx<Impl, Lock_> {
  using Gap = ZvIOQueue::Gap;
  using Tx = ZvIOQueueTx<Impl, Lock_>;

  using Lock = Lock_;
  using Guard = ZmGuard<Lock>;

  static const char *Queues_HeapID() { return "ZvIOQueueTxPool.Queues"; }
  using Queues =
    ZmRBTreeKV<ZmTime, ZmRef<Tx>,
      ZmRBTreeHeapID<Queues_HeapID>>;

public:
  void loaded_(ZvIOMsg *) { }   // may be overridden by Impl
  void unloaded_(ZvIOMsg *) { } // ''

  bool send_(ZvIOMsg *msg, bool more) {
    if (ZmRef<Tx> next = next_()) {
      next->send(msg);
      sent_(msg);
      return true;
    }
    return false;
  }
  bool resend_(ZvIOMsg *, bool) { return true; } // unused
  void aborted_(ZvIOMsg *) { } // unused

  bool sendGap_(const Gap &, bool) { return true; } // unused
  bool resendGap_(const Gap &, bool) { return true; } // unused

  void sent_(ZvIOMsg *msg) { Tx::ackd(msg->id().seqNo + 1); }
  void archive_(ZvIOMsg *msg) { Tx::archived(msg->id().seqNo + 1); }
  ZmRef<ZvIOMsg> retrieve_(ZvSeqNo, ZvSeqNo) { // unused
    return nullptr;
  }

  ZmRef<Tx> next_() {
    Guard guard(this->lock());
    return m_queues.minimumVal();
  }

  void ready_(Tx *queue, ZmTime prev, ZmTime next) {
    Guard guard(this->lock());
    typename Queues::Node *node = 0;
    if (!prev || !(node = m_queues.del(prev, queue))) {
      if (node) delete node;
      m_queues.add(next, queue);
      if (m_queues.count() == 1) {
	Tx::ready_(next);
	guard.unlock();
	Tx::start();
	return;
      }
    } else {
      node->key() = next;
      m_queues.add(node);
    }
    Tx::ready_(m_queues.minimumKey());
  }

  void unready_(Tx *queue, ZmTime prev) {
    Guard guard(this->lock());
    typename Queues::Node *node = 0;
    if (!prev || !(node = m_queues.del(prev, queue))) return;
    delete node;
    if (!m_queues.count()) Tx::unready_();
  }

  Queues	m_queues;	// guarded by Tx::lock()
};

template <class Impl, class Lock_>
void ZvIOQueueTx<Impl, Lock_>::ready_(ZmTime next)
{
  unsigned i, n = m_pools.length();
  unsigned o = (m_poolOffset = (m_poolOffset + 1) % n);
  for (i = 0; i < n; i++)
    m_pools[(i + o) % n]->ready_(this, m_ready, next);
  m_ready = next;
}

template <class Impl, class Lock_>
void ZvIOQueueTx<Impl, Lock_>::unready_()
{
  unsigned i, n = m_pools.length();
  unsigned o = (m_poolOffset = (m_poolOffset + 1) % n);
  for (i = 0; i < n; i++)
    m_pools[(i + o) % n]->unready_(this, m_ready);
  m_ready = ZmTime();
}

#endif /* ZvIOQueue_HH */
