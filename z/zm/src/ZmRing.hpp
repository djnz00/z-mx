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

// FIFO ring buffer of fixed-size items with fan-in (MPSC)

#ifndef ZmRing_HPP
#define ZmRing_HPP

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
#include <zlib/ZmBackTrace.hpp>
#include <zlib/ZmRingUtil.hpp>

// ring buffer parameters

struct ZmRingError {
  int code;
  ZmRingError(int code_) : code(code_) { }
  template <typename S> void print(S &s) const {
    switch (code) {
      case ZmRingUtil::OK:		s << "OK"; break;
      case ZmRingUtil::EndOfFile:	s << "EndOfFile"; break;
      case ZmRingUtil::Error:	s << "Error"; break;
      case ZmRingUtil::NotReady:	s << "NotReady"; break;
      default:			s << "Unknown"; break;
    }
  }
  friend ZuPrintFn ZuPrintType(ZmRingError *);
};

// uses NTP (named template parameters):
//
// ZmRing<ZmFn<> >			// ring of functions

// NTP defaults
struct ZmRing_Defaults { };

#define ZmRingAlign(x) (((x) + 8 + 15) & ~15)

namespace ZmRing_ {

template <typename> class Params;
class ParamData {
  template <typename> friend Params;
public:
  ParamData() = default;
  ParamData(const ParamData &) = default; 
  ParamData &operator =(const ParamData &) = default;
  ParamData(ParamData &&) = default; 
  ParamData &operator =(ParamData &&) = default;

  ParamData(unsigned size) : m_size{size} { }

  unsigned size() const { return m_size; }
  bool ll() const { return m_ll; }
  const ZmBitmap &cpuset() const { return m_cpuset; }

private:
  unsigned	m_size = 0;
  bool		m_ll = false;
  ZmBitmap	m_cpuset;
};
template <typename Derived> class Params :
    public ZmRingUtil_::Params<Derived>, public ParamData {
  ZuInline Derived &&derived() { return ZuMv(*static_cast<Derived *>(this)); }
public:
  Derived &&size(unsigned n) { m_size = n; return derived(); }
  Derived &&ll(bool b) { m_ll = b; return derived(); }
  Derived &&cpuset(ZmBitmap b) { m_cpuset = ZuMv(b); return derived(); }
};

} // ZmRing_

struct ZmRingParams : public ZmRing_::Params<ZmRingParams> { };

template <typename T_, class NTP = ZmRing_Defaults>
class ZmRing : public ZmRingUtil {
  ZmRing &operator =(const ZmRing &);	// prevent mis-use

  using ParamData = ZmRing_::ParamData;
  template <typename Derived> using Params = ZmRing_::Params<Derived>;

public:
  enum { CacheLineSize = Zm::CacheLineSize };

  enum { // open() flags
    Read	= 0x00000001,
    Write	= 0x00000002,
    Shadow	= 0x00000004
  };

  using T = T_;

  enum { Size = ZmRingAlign(sizeof(T)) };

  ZmRing() = default;
  template <typename Derived, typename ...Args>
  ZmRing(Params<Derived> params, Args &&... args) :
      ZmRingUtil{ZuMv(params)},
      m_params{ZuMv(params)} { } // yes, moved twice

  ZmRing(const ZmRing &ring) :
      ZmRing_(ring.m_params), m_flags(Shadow),
      m_ctrl(ring.m_ctrl), m_data(ring.m_data) { }

  ~ZmRing() { close(); }

  const ParamData &params() const { return m_params; }

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

  ZuInline uint8_t *data() const {
    return reinterpret_cast<uint8_t *>(m_data);
  }

  ZuInline unsigned full() const { return m_full; }

  int open(unsigned flags) {
    if (m_ctrl) return OK;
    if (!params().size()) return Error;
    flags &= (Read | Write);
    m_size = ((params().size() + Size - 1) / Size) * Size;
    if (m_flags & Shadow) {
      m_flags |= flags;
      return OK;
    }
    m_flags = flags;
    if (!params().ll() && ZmRingUtil::open() != OK) return Error;
    if (!params().cpuset())
      m_ctrl = hwloc_alloc(ZmTopology::hwloc(), sizeof(Ctrl));
    else
      m_ctrl = hwloc_alloc_membind(
	  ZmTopology::hwloc(), sizeof(Ctrl),
	  params().cpuset(), HWLOC_MEMBIND_BIND, HWLOC_MEMBIND_MIGRATE);
    if (!m_ctrl) {
      if (!params().ll()) ZmRingUtil::close();
      return Error;
    }
    memset(m_ctrl, 0, sizeof(Ctrl));
    if (!params().cpuset())
      m_data = hwloc_alloc(ZmTopology::hwloc(), size());
    else
      m_data = hwloc_alloc_membind(
	  ZmTopology::hwloc(), size(),
	  params().cpuset(), HWLOC_MEMBIND_BIND, HWLOC_MEMBIND_MIGRATE);
    if (!m_data) {
      hwloc_free(ZmTopology::hwloc(), m_ctrl, sizeof(Ctrl));
      m_ctrl = 0;
      if (!params().ll()) ZmRingUtil::close();
      return Error;
    }
    return OK;
  }

  void close() {
    if (!m_ctrl) return;
    if (m_flags & Shadow) return;
    hwloc_free(ZmTopology::hwloc(), m_ctrl, sizeof(Ctrl));
    hwloc_free(ZmTopology::hwloc(), m_data, size());
    m_ctrl = m_data = 0;
    if (!params().ll()) ZmRingUtil::close();
  }

  int reset() {
    if (!m_ctrl) return Error;
    memset(m_ctrl, 0, sizeof(Ctrl));
    memset(m_data, 0, size());
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
    uint32_t head_ = this->head().load_();
    if (ZuUnlikely(head_ & EndOfFile_)) return nullptr;
    uint32_t head = head_ & ~Mask;
    uint32_t tail = this->tail(); // acquire
    uint32_t tail_ = tail & ~Mask;
    if (ZuUnlikely((head ^ tail_) == Wrapped)) {
      ++m_full;
      if constexpr (!Wait) return nullptr;
      if (ZuUnlikely(!params().ll()))
	if (this->ZmRing_wait(Tail, this->tail(), tail) != OK) return nullptr;
      goto retry;
    }

    head += Size;
    if ((head & ~Wrapped) >= size()) head = (head ^ Wrapped) - size();

    if (ZuUnlikely(this->head().cmpXch(head, head_) != head_))
      goto retry;

    auto ptr = reinterpret_cast<ZmAtomic<uint32_t> *>(
	&(data())[head_ & ~(Wrapped | Mask)]);
    return (void *)&ptr[2];
  }
  void push2(void *ptr_) {
    ZmAssert(m_ctrl);
    ZmAssert(m_flags & Write);

    auto ptr = &(reinterpret_cast<ZmAtomic<uint32_t> *>(ptr_))[-2];

    if (ZuUnlikely(!params().ll())) {
      if (ZuUnlikely(ptr->xch(Ready) & Waiting))
	this->ZmRing_wake(Head, *ptr, 1);
    } else
      *ptr = Ready; // release

    this->inCount().store_(this->inCount().load_() + 1);
    this->inBytes().store_(this->inBytes().load_() + Size);
  }

  void eof(bool b = true) {
    ZmAssert(m_ctrl);
    ZmAssert(m_flags & Write);

    uint32_t head = this->head().load_();
    auto ptr = reinterpret_cast<ZmAtomic<uint32_t> *>(
	&(data())[head & ~(Wrapped | Mask)]);
    if (!b) {
      *ptr = 0; // release
      this->head() = head & ~EndOfFile_; // release
      return;
    }
    if (ZuUnlikely(!params().ll())) {
      if (ZuUnlikely(ptr->xch(EndOfFile_) & Waiting))
	this->ZmRing_wake(Head, *ptr, 1);
    } else
      *ptr = EndOfFile_; // release
    this->head() = head | EndOfFile_; // release
  }

  // can be called by writers after push() returns 0; returns
  // NotReady (no readers), EndOfFile,
  // or amount of space remaining in ring buffer (>= 0)
  int writeStatus() {
    ZmAssert(m_ctrl);
    ZmAssert(m_flags & Write);

    uint32_t head = this->head().load_();
    if (ZuUnlikely(head & EndOfFile_)) return EndOfFile;
    head &= ~Mask;
    uint32_t tail = this->tail() & ~Mask;
    if ((head ^ tail) == Wrapped) return 0;
    head &= ~Wrapped;
    tail &= ~Wrapped;
    if (head < tail) return tail - head;
    return size() - (head - tail);
  }

  // reader

  T *shift() {
    ZmAssert(m_ctrl);
    ZmAssert(m_flags & Read);

    uint32_t tail = this->tail().load_() & ~Mask;
  retry:
    auto ptr = reinterpret_cast<ZmAtomic<uint32_t> *>(
	&(data())[tail & ~Wrapped]);
    uint32_t header = *ptr; // acquire
    if (!(header & ~Waiting)) {
      if (ZuUnlikely(!params().ll()))
	if (this->ZmRing_wait(Head, *ptr, header) != OK) return nullptr;
      goto retry;
    }

    if (ZuUnlikely(header & EndOfFile_)) return nullptr;
    ptr->store_(0);
    return (T *)&ptr[2];
  }
  void shift2() {
    ZmAssert(m_ctrl);
    ZmAssert(m_flags & Read);

    uint32_t tail = this->tail().load_() & ~Mask;
    tail += Size;
    if ((tail & ~Wrapped) >= size()) tail = (tail ^ Wrapped) - size();

    if (ZuUnlikely(!params().ll())) {
      if (ZuUnlikely(this->tail().xch(tail & ~Waiting) & Waiting))
	this->ZmRing_wake(Tail, this->tail(), 1);
    } else
      this->tail() = tail; // release

    this->outCount().store_(this->outCount().load_() + 1);
    this->outBytes().store_(this->outBytes().load_() + Size);
  }

  // can be called by a reader after shift() returns 0; returns
  // EndOfFile (< 0), or amount of data remaining in ring buffer (>= 0)
  int readStatus() const {
    ZmAssert(m_ctrl);
    ZmAssert(m_flags & Read);

    uint32_t tail = this->tail().load_() & ~Mask;
    {
      auto ptr = reinterpret_cast<ZmAtomic<uint32_t> *>(
	&(const_cast<ZmRing *>(this)->data())[tail & ~Wrapped]);
      if (ZuUnlikely(ptr->load_() & EndOfFile_)) return EndOfFile;
    }
    uint32_t head = const_cast<ZmRing *>(this)->head() & ~Mask; // acquire
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
  ParamData		m_params;
  uint32_t		m_flags = 0;
  void			*m_ctrl = nullptr;
  void			*m_data = nullptr;
  uint32_t		m_size = 0;
  uint32_t		m_full = 0;
};

#endif /* ZmRing_HPP */
