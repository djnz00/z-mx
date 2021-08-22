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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// concrete generic I/O queue based on ZmPQueue skip lists, used by ZvEngine
//
// Key / SeqNo - uint64
// Link ID - ZuID (union of 8-byte string with uint64)

#ifndef ZvIOQueue_HPP
#define ZvIOQueue_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

#include <zlib/ZuArrayN.hpp>
#include <zlib/ZuPOD.hpp>
#include <zlib/ZuRef.hpp>
#include <zlib/ZuID.hpp>

#include <zlib/ZmPQueue.hpp>
#include <zlib/ZmFn.hpp>
#include <zlib/ZmRBTree.hpp>

#include <zlib/ZiIP.hpp>
#include <zlib/ZiIOBuf.hpp>
#include <zlib/ZiMultiplex.hpp>

#include <zlib/ZvSeqNo.hpp>
#include <zlib/ZvMsgID.hpp>

struct ZvIOQItem {
  ZuInline ZvIOQItem(ZmRef<ZiAnyIOBuf> buf) :
      m_buf(ZuMv(buf)) { }
  ZuInline ZvIOQItem(ZmRef<ZiAnyIOBuf> buf, ZvMsgID id) :
      m_buf(ZuMv(buf)), m_id(id) { }

  template <typename T = ZiAnyIOBuf>
  ZuInline const T *buf() const { return m_buf.ptr<T>(); }
  template <typename T = ZiAnyIOBuf>
  ZuInline T *buf() { return m_buf.ptr<T>(); }

  template <typename T = void *> ZuInline T owner() const { return (T)m_owner; }
  template <typename T> ZuInline void owner(T v) { m_owner = (void *)v; }

  ZuInline const ZvMsgID id() const { return m_id; }

  ZuInline void load(const ZvMsgID &id) { m_id = id; }
  ZuInline void unload() { m_id = ZvMsgID{}; }

  ZuInline unsigned skip() const { return m_skip <= 0 ? 1 : m_skip; }
  ZuInline void skip(unsigned n) { m_skip = n; }

  ZuInline bool noQueue() const { return m_skip < 0; }
  ZuInline void noQueue(int b) { m_skip = -b; }

private:
  ZmRef<ZiAnyIOBuf>	m_buf;
  void			*m_owner = 0;
  ZvMsgID		m_id;
  int32_t		m_skip = 0;	// -ve - no queuing; +ve - gap fill
};

class ZvIOQFn {
public:
  using Key = ZvSeqNo;
  ZuInline ZvIOQFn(ZvIOQItem &item) : m_item(item) { }
  ZuInline ZvSeqNo key() const { return m_item.id().seqNo; }
  ZuInline unsigned length() const { return m_item.skip(); }
  ZuInline unsigned clipHead(unsigned n) { return length(); }
  ZuInline unsigned clipTail(unsigned n) { return length(); }
  ZuInline void write(const ZvIOQFn &) { }
  ZuInline unsigned bytes() const { return m_item.buf()->length; }

private:
  ZvIOQItem	&m_item;
};

struct ZvIOMsg_HeapID {
  static constexpr const char *id() { return "ZvIOMsg"; }
};
using ZvIOQueue_ =
  ZmPQueue<ZvIOQItem,
    ZmPQueueNodeDerive<true,
      ZmPQueueObject<ZmPolymorph,
	ZmPQueueFn<ZvIOQFn,
	  ZmPQueueLock<ZmNoLock,
	    ZmPQueueHeapID<ZvIOMsg_HeapID> > > > > >;
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

  ZuInline ZvIOQueueRx() : m_queue(new ZvIOQueue(ZvSeqNo{})) { }
  
  ZuInline const Impl *impl() const { return static_cast<const Impl *>(this); }
  ZuInline Impl *impl() { return static_cast<Impl *>(this); }

  ZuInline const ZvIOQueue *rxQueue() const { return m_queue; }
  ZuInline ZvIOQueue *rxQueue() { return m_queue; }

  ZuInline void rxInit(ZvSeqNo seqNo) {
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
  ZvIOQueueTx() : m_queue(new ZvIOQueue(ZvSeqNo{})) { }

  const Impl *impl() const { return static_cast<const Impl *>(this); }
  Impl *impl() { return static_cast<Impl *>(this); }

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
  struct Queues_HeapID {
    static constexpr const char *id() { return "ZvIOQueueTxPool.Queues"; }
  };

  using Gap = ZvIOQueue::Gap;
  using Tx = ZvIOQueueTx<Impl, Lock_>;

  using Lock = Lock_;
  using Guard = ZmGuard<Lock>;

  using Queues =
    ZmRBTree<ZmTime,
      ZmRBTreeVal<ZmRef<Tx>,
	ZmRBTreeObject<ZuNull,
	  ZmRBTreeLock<ZmNoLock,
	    ZmRBTreeHeapID<Queues_HeapID> > > > >;

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

#endif /* ZvIOQueue_HPP */
