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

// FIFO ring buffer with broadcast fan-out to multiple readers (up to 64)

// Note: this is broadcast, not conventional SPMC - every consumer processes
// every message i.e. every message is processed as many times as there are
// consumers; with normal SPMC, each message is processed once by a single
// consumer selected from the pool of consumers, typically according to
// consumer availability/readiness/priority

#ifndef ZmBxRing_HPP
#define ZmBxRing_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZmPlatform.hpp>
#include <zlib/ZmAssert.hpp>
#include <zlib/ZmBitmap.hpp>
#include <zlib/ZmTopology.hpp>
#include <zlib/ZmLock.hpp>
#include <zlib/ZmGuard.hpp>
#include <zlib/ZmAtomic.hpp>
#include <zlib/ZmTime.hpp>
#include <zlib/ZmRing.hpp>

// uses NTP (named template parameters):
//
// ZmBxRing<ZmFn<> >			// ring of functions

// NTP defaults
struct ZmBxRing_Defaults { };

#define ZmBxRingAlign(x) (((x) + 8 + 15) & ~15)

template <typename T_, class NTP = ZmBxRing_Defaults>
class ZmBxRing : public ZmRing_ {
  ZmBxRing(const ZmBxRing &) = delete;
  ZmBxRing &operator =(const ZmBxRing &) = delete;

public:
  enum { CacheLineSize = Zm::CacheLineSize };

  enum { // open() flags
    Read	= 0x00000001,
    Write	= 0x00000002,
    Shadow	= 0x00000004
  };

  using T = T_;

  enum { Size = ZmBxRingAlign(sizeof(T)) };

  template <typename ...Args>
  ZmBxRing(ZmRingParams params = ZmRingParams(0), Args &&... args) :
      ZmRing_(params),
      m_flags(0), m_id(-1), m_ctrl(0), m_data(0),
      m_tail(0), m_size(0), m_full(0) { }

  ~ZmBxRing() { close(); }

private:
  struct Ctrl {
    ZmAtomic<uint32_t>		head;
    uint32_t			pad_1;
    ZmAtomic<uint64_t>		inCount;
    ZmAtomic<uint64_t>		inBytes;
    char			pad_2[CacheLineSize - 24];

    ZmAtomic<uint32_t>		tail;
    uint32_t			pad_3;
    ZmAtomic<uint64_t>		outCount;
    ZmAtomic<uint64_t>		outBytes;
    char			pad_4[CacheLineSize - 24];

    ZmAtomic<uint32_t>		rdrCount; // reader count
    ZmAtomic<uint64_t>		rdrMask;  // active readers
    ZmAtomic<uint64_t>		attMask;  // readers pending attach
    ZmAtomic<uint64_t>		attSeqNo; // attach/detach seqNo
  };

  ZuInline const Ctrl *ctrl() const {
    return static_cast<const Ctrl *>(m_ctrl);
  }
  ZuInline Ctrl *ctrl() {
    return static_cast<Ctrl *>(m_ctrl);
  }

  ZuInline const ZmAtomic<uint32_t> &head() const { return ctrl()->head; }
  ZuInline ZmAtomic<uint32_t> &head() { return ctrl()->head; }

  ZuInline const ZmAtomic<uint32_t> &tail() const { return ctrl()->tail; }
  ZuInline ZmAtomic<uint32_t> &tail() { return ctrl()->tail; }

  ZuInline ZmAtomic<uint32_t> &rdrCount() { return ctrl()->rdrCount; }
  ZuInline ZmAtomic<uint64_t> &rdrMask() { return ctrl()->rdrMask; }
  ZuInline ZmAtomic<uint64_t> &attMask() { return ctrl()->attMask; }
  ZuInline ZmAtomic<uint64_t> &attSeqNo() { return ctrl()->attSeqNo; }

  ZuInline const ZmAtomic<uint64_t> &inCount() const
    { return ctrl()->inCount; }
  ZuInline ZmAtomic<uint64_t> &inCount() { return ctrl()->inCount; }
  ZuInline const ZmAtomic<uint64_t> &inBytes() const
    { return ctrl()->inBytes; }
  ZuInline ZmAtomic<uint64_t> &inBytes() { return ctrl()->inBytes; }
  ZuInline const ZmAtomic<uint64_t> &outCount() const
    { return ctrl()->outCount; }
  ZuInline ZmAtomic<uint64_t> &outCount() { return ctrl()->outCount; }
  ZuInline const ZmAtomic<uint64_t> &outBytes() const
    { return ctrl()->outBytes; }
  ZuInline ZmAtomic<uint64_t> &outBytes() { return ctrl()->outBytes; }
 
public:
  ZuInline bool operator !() const { return !m_ctrl; }
  ZuOpBool;

  ZuInline uint8_t *data() const { return reinterpret_cast<uint8_t *>(m_data); }

  ZuInline unsigned full() const { return m_full; }

  int open(unsigned flags) {
    if (m_ctrl) return OK;
    if (!m_params.size()) return Error;
    flags &= (Read | Write);
    m_size = ((m_params.size() + Size - 1) / Size) * Size;
    if (m_flags & Shadow) {
      m_flags |= flags;
      goto ret;
    }
    m_flags = flags;
    if (!m_params.ll() && ZmRing_::open() != OK) return Error;
    if (!m_params.cpuset())
      m_ctrl = hwloc_alloc(ZmTopology::hwloc(), sizeof(Ctrl));
    else
      m_ctrl = hwloc_alloc_membind(
	  ZmTopology::hwloc(), sizeof(Ctrl),
	  m_params.cpuset(), HWLOC_MEMBIND_BIND, HWLOC_MEMBIND_MIGRATE);
    if (!m_ctrl) { if (!m_params.ll()) ZmRing_::close(); return Error; }
    memset(m_ctrl, 0, sizeof(Ctrl));
    if (!m_params.cpuset())
      m_data = hwloc_alloc(ZmTopology::hwloc(), size());
    else
      m_data = hwloc_alloc_membind(
	  ZmTopology::hwloc(), size(),
	  m_params.cpuset(), HWLOC_MEMBIND_BIND, HWLOC_MEMBIND_MIGRATE);
    if (!m_data) {
      hwloc_free(ZmTopology::hwloc(), m_ctrl, sizeof(Ctrl));
      m_ctrl = 0;
      if (!m_params.ll()) ZmRing_::close();
      return Error;
    }
  ret:
    if (flags & Read) ++rdrCount();
    return OK;
  }

  int shadow(const ZmBxRing &ring) {
    if (m_ctrl || !ring.m_ctrl) return Error;
    m_params = ring.m_params;
    m_flags = Read | Shadow;
    m_id = -1;
    m_ctrl = ring.m_ctrl;
    m_data = ring.m_data;
    m_tail = 0;
    m_full = 0;
    return OK;
  }

  void close() {
    if (!m_ctrl) return;
    if (m_flags & Read) {
      if (m_id >= 0) detach();
      --rdrCount();
    }
    if (m_flags & Shadow) return;
    hwloc_free(ZmTopology::hwloc(), m_ctrl, sizeof(Ctrl));
    hwloc_free(ZmTopology::hwloc(), m_data, size());
    m_ctrl = m_data = 0;
    if (!m_params.ll()) ZmRing_::close();
  }

  int reset() {
    if (!m_ctrl) return Error;
    if ((m_flags & Read) && m_id >= 0) detach();
    if (ZuUnlikely(rdrMask())) return NotReady;
    memset(m_ctrl, 0, sizeof(Ctrl));
    m_full = 0;
    return OK;
  }

  unsigned ctrlSize() const { return sizeof(Ctrl); }
  unsigned size() const { return m_size; }

  unsigned length() {
    uint32_t head = this->head().load_() & ~Mask;
    uint32_t tail = this->tail().load_() & ~Mask;
    if (head == tail) return 0;
    if ((head ^ tail) == Wrapped) return size();
    head &= ~Wrapped;
    tail &= ~Wrapped;
    if (head > tail) return head - tail;
    return size() - (tail - head);
  }

  // writer

  ZuInline void *push() { return push_<1>(); }
  ZuInline void *tryPush() { return push_<0>(); }
  template <bool Wait> void *push_() {
    ZmAssert(m_ctrl);
    ZmAssert(m_flags & Write);

  retry:
    uint64_t rdrMask = this->rdrMask().load_();
    if (!rdrMask) return 0; // no readers

    uint32_t head = this->head().load_();
    if (ZuUnlikely(head & EndOfFile_)) return 0; // EOF
    head &= ~Mask;
    uint32_t tail = this->tail() & ~Mask; // acquire
    if (ZuUnlikely((head ^ tail) == Wrapped)) {
      ++m_full;
      if constexpr (!Wait) return 0;
      if (ZuUnlikely(!m_params.ll()))
	if (this->ZmRing_wait(Tail, this->tail(), tail) != OK) return 0;
      goto retry;
    }

    uint8_t *ptr = &(data())[head & ~Wrapped];
    *reinterpret_cast<uint64_t *>(ptr) = rdrMask;
    return (void *)&ptr[8];
  }
  void push2() {
    ZmAssert(m_ctrl);
    ZmAssert(m_flags & Write);

    uint32_t head = this->head().load_();
    head += Size;
    if ((head & ~(Wrapped | Mask)) >= size()) head = (head ^ Wrapped) - size();

    if (ZuUnlikely(!m_params.ll())) {
      if (ZuUnlikely(this->head().xch(head & ~Waiting) & Waiting))
	this->ZmRing_wake(Head, this->head(), rdrCount().load_());
    } else
      this->head() = head; // release

    this->inCount().store_(this->inCount().load_() + 1);
    this->inBytes().store_(this->inBytes().load_() + Size);
  }

  void eof(bool b = true) {
    ZmAssert(m_ctrl);
    ZmAssert(m_flags & Write);

    uint32_t head = this->head().load_();
    if (b)
      head |= EndOfFile_;
    else
      head &= ~EndOfFile_;

    if (ZuUnlikely(!m_params.ll())) {
      if (ZuUnlikely(this->head().xch(head & ~Waiting) & Waiting))
	this->ZmRing_wake(Head, this->head(), rdrCount().load_());
    } else
      this->head() = head; // release
  }

  // can be called by writers after push() returns 0; returns
  // NotReady (no readers), EndOfFile,
  // or amount of space remaining in ring buffer (>= 0)
  int writeStatus() {
    ZmAssert(m_ctrl);
    ZmAssert(m_flags & Write);

    if (ZuUnlikely(!rdrMask())) return NotReady;
    uint32_t head = this->head().load_();
    if (ZuUnlikely(head & EndOfFile_)) return EndOfFile;
    head &= ~(Wrapped | Mask);
    uint32_t tail = this->tail() & ~(Wrapped | Mask);
    if (head < tail) return tail - head;
    return size() - (head - tail);
  }

  // reader

  ZuInline int id() { return m_id; } // -ve if not attached

  int attach() {
    ZmAssert(m_ctrl);
    ZmAssert(m_flags & Read);

    if (m_id >= 0) return Error;

    // allocate an ID for this reader
    {
      uint64_t attMask;
      unsigned id;
      do {
	attMask = this->attMask().load_();
	for (id = 0; id < 64; id++)
	  if (!(attMask & (1ULL<<id))) break;
	if (id == 64) return Error;
      } while (
	  this->attMask().cmpXch(attMask | (1ULL<<id), attMask) != attMask);

      m_id = id;
    }

    ++(this->attSeqNo());

    rdrMask() |= (1ULL<<m_id); // notifies the writer about an attach

    // skip any trailing messages not intended for us, since other readers
    // may be concurrently advancing the ring's tail; this must be
    // re-attempted as long as the head keeps moving and the writer remains
    // unaware of our attach
    uint32_t tail = this->tail().load_() & ~Mask;
    uint32_t head = this->head() & ~Mask, head_; // acquire
    do {
      while (tail != head) {
	uint8_t *ptr = &(data())[tail & ~Wrapped];
	if (*reinterpret_cast<uint64_t *>(ptr) & (1ULL<<m_id))
	  goto done; // writer aware
	tail += Size;
	if ((tail & ~Wrapped) >= size()) tail = (tail ^ Wrapped) - size();
      }
      head_ = head;
      head = this->head() & ~Mask; // acquire
    } while (head != head_);

  done:
    m_tail = tail;

    ++(this->attSeqNo());

    return OK;
  }

  int detach() {
    ZmAssert(m_ctrl);
    ZmAssert(m_flags & Read);

    if (m_id < 0) return Error;

    ++(this->attSeqNo());

    rdrMask() &= ~(1ULL<<m_id); // notifies the writer about a detach

    // drain any trailing messages that are waiting to be read by us,
    // advancing ring's tail as needed; this must be
    // re-attempted as long as the head keeps moving and the writer remains
    // unaware of our detach
    uint32_t tail = m_tail;
    uint32_t head = this->head() & ~Mask, head_; // acquire
    do {
      while (tail != head) {
	uint8_t *ptr = &(data())[tail & ~Wrapped];
	if (!(*reinterpret_cast<uint64_t *>(ptr) & (1ULL<<m_id)))
	  goto done; // writer aware
	tail += Size;
	if ((tail & ~Wrapped) >= size()) tail = (tail ^ Wrapped) - size();
	if (*reinterpret_cast<ZmAtomic<uint64_t> *>(ptr) &= ~(1ULL<<m_id))
	  continue;

	if (ZuUnlikely(!m_params.ll())) {
	  if (ZuUnlikely(this->tail().xch(tail) & Waiting))
	    this->ZmRing_wake(Tail, this->tail(), 1);
	} else
	  this->tail() = tail; // release
      }
      head_ = head;
      head = this->head() & ~Mask; // acquire
    } while (head != head_);
  done:
    m_tail = tail;

    ++(this->attSeqNo());

    attMask() &= ~(1ULL<<m_id);
    m_id = -1;

    return OK;
  }

  T *shift() {
    ZmAssert(m_ctrl);
    ZmAssert(m_flags & Read);
    ZmAssert(m_id >= 0);

    uint32_t tail = m_tail;
    uint32_t head;
  retry:
    head = this->head(); // acquire
    if (tail == (head & ~Mask)) {
      if (ZuUnlikely(head & EndOfFile_)) return 0;
      if (ZuUnlikely(!m_params.ll()))
	if (this->ZmRing_wait(Head, this->head(), head) != OK) return 0;
      goto retry;
    }

    uint8_t *ptr = &(data())[tail & ~Wrapped];
    return reinterpret_cast<T *>(&ptr[8]);
  }
  void shift2() {
    ZmAssert(m_ctrl);
    ZmAssert(m_flags & Read);
    ZmAssert(m_id >= 0);

    uint32_t tail = m_tail;
    uint8_t *ptr = &(data())[tail & ~Wrapped];
    tail += Size;
    if ((tail & ~Wrapped) >= size()) tail = (tail ^ Wrapped) - size();
    m_tail = tail;
    if (*reinterpret_cast<ZmAtomic<uint64_t> *>(ptr) &= ~(1ULL<<m_id)) return;
    if (ZuUnlikely(!m_params.ll())) {
      if (ZuUnlikely(this->tail().xch(tail) & Waiting))
	this->ZmRing_wake(Tail, this->tail(), 1);
    } else
      this->tail() = tail; // release

    this->outCount().store_(this->outCount().load_() + 1);
    this->outBytes().store_(this->outBytes().load_() + Size);
  }

  // can be called by readers after shift() returns 0; returns
  // EndOfFile (< 0), or amount of data remaining in ring buffer (>= 0)
  int readStatus() const {
    ZmAssert(m_ctrl);
    ZmAssert(m_flags & Read);

    uint32_t head = const_cast<ZmBxRing *>(this)->head();
    if (ZuUnlikely(head & EndOfFile_)) return EndOfFile;
    head &= ~Mask;
    uint32_t tail = m_tail;
    if ((head ^ tail) == Wrapped) return size();
    head &= ~Wrapped;
    tail &= ~Wrapped;
    if (head >= tail) return head - tail;
    return size() - (tail - head);
  }

  unsigned count_() const {
    int i = readStatus();
    if (i < 0) return 0;
    return i / Size;
  }

  void stats(
      uint64_t &inCount, uint64_t &inBytes, 
      uint64_t &outCount, uint64_t &outBytes) const {
    ZmAssert(m_ctrl);

    inCount = this->inCount().load_();
    inBytes = this->inBytes().load_();
    outCount = this->outCount().load_();
    outBytes = this->outBytes().load_();
  }

private:
  uint32_t		m_flags;
  int			m_id;
  void			*m_ctrl;
  void			*m_data;
  uint32_t		m_tail;
  uint32_t		m_size;
  uint32_t		m_full;
};

#endif /* ZmBxRing_HPP */
