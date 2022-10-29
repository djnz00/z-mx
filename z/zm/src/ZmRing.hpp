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

#ifdef ZmRing_FUNCTEST
#define ZmRing_bp(x) (bp_##x.reached(#x))
#else
#ifdef ZmRing_STRESSTEST
#define ZmRing_bp(x) ZmPlatform::yield()
#else
#define ZmRing_bp(x) (void{})
#endif
#endif

// ring buffer parameters

// uses NTP (named template parameters):
//
// ZmRing<ZmFn<> >			// ring of functions

namespace ZmRing_ {

template <typename T>
struct DefaultSizeAxor {
  static constexpr unsigned get(const void *) { return sizeof(T); }
};
template <>
struct DefaultSizeAxor<void> {
  static unsigned get(const void *) { return 0; }
};

// NTP defaults
struct Defaults {
  using T = void;					// variable-sized
  using SizeAxor = DefaultSizeAxor<void>;
  enum { MW = 0 };
  enum { MR = 0 };
};

} // ZmRing_

// fixed-size message type
template <typename T_, typename NTP = ZmRing_::Defaults>
struct ZmRingT : public NTP {
  using T = T_;
  using SizeAxor = ZmRing_::DefaultSizeAxor<T>;
};
template <typename NTP>
struct ZmRingT<ZuNull, NTP> : public NTP {
  using T = void;
  using SizeAxor = ZmRing_::DefaultSizeAxor<T>;
};

// variable-sized message type
template <typename SizeAxor_, typename NTP = ZmRing_::Defaults>
struct ZmRingSizeAxor : public NTP {
  using SizeAxor = SizeAxor_;
};

// multiple producers
template <bool MW_, typename NTP = ZmRing_::Defaults>
struct ZmRingMW : public NTP {
  enum { MW = MW_ };
};

// multiple consumers
template <bool MR_, typename NTP = ZmRing_::Defaults>
struct ZmRingMR : public NTP {
  enum { MR = MR_ };
};

namespace ZmRing_ {

template <typename> class Params;
class ParamData {
  template <typename> friend class Params;
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
  unsigned spin() const { return m_spin; }
  unsigned timeout() const { return m_timeout; } // milliseconds

private:
  unsigned	m_size = 0;
  bool		m_ll = false;
  ZmBitmap	m_cpuset;
  unsigned	m_spin = 1000;
  unsigned	m_timeout = 1;
};
template <typename Derived> class Params : public ParamData {
  ZuInline Derived &&derived() { return ZuMv(*static_cast<Derived *>(this)); }
public:
  Derived &&size(unsigned n) { m_size = n; return derived(); }
  Derived &&ll(bool b) { m_ll = b; return derived(); }
  Derived &&cpuset(ZmBitmap b) { m_cpuset = ZuMv(b); return derived(); }
  Derived &&spin(unsigned n) { m_spin = n; return derived(); }
  Derived &&timeout(unsigned n) { m_timeout = n; return derived(); }
};

} // ZmRing_

struct ZmRingParams : public ZmRing_::Params<ZmRingParams> { };

namespace ZmRingErrorCode {
  enum { OK = 0, EndOfFile = -1, Error = -2, NotReady = -3 };
  inline const char *name(int i) {
    static const char *names[] = { "OK", "EndOfFile", "Error", "NotReady" };
    if (i > 0) i = 0;
    i = -i;
    if (i > (sizeof(names) / sizeof(names[0]))) return "Unknown";
    return names[i];
  }
}

struct ZmRingError {
  int code;
  ZmRingError(int code_) : code{code_} { }
  template <typename S> void print(S &s) const {
    using namespace ZmRingErrorCode;
    switch (code) {
      case OK:		s << "OK"; break;
      case EndOfFile:	s << "EndOfFile"; break;
      case Error:	s << "Error"; break;
      case NotReady:	s << "NotReady"; break;
      default:		s << "Unknown"; break;
    }
  }
  friend ZuPrintFn ZuPrintType(ZmRingError *);
};

namespace ZmRing_ {

class ZmAPI Blocker {
public:
  int open();
  int close();

  // block until woken or timeout while addr == val
  int wait(
#ifdef _WIN32
      unsigned index,
#endif
      ZmAtomic<uint32_t> &addr, uint32_t val,
      unsigned timeout, unsigned spin);
  // wake up waiters on addr (up to n waiters are woken)
  int wake(
#ifdef _WIN32
      unsigned index,
#endif
      ZmAtomic<uint32_t> &addr);

#ifndef _WIN32
#define ZmRing_Blocker_wait(i, a, v, t, s) wait(a, v, t, s)
#define ZmRing_Blocker_wake(i, a) wake(a)
#else
#define ZmRing_Blocker_wait(i, a, v, t, s) wait(i, a, v, t, s)
#define ZmRing_Blocker_wake(i, a) wake(i, a)
#endif

protected:
  ParamData		m_params;
#ifdef _WIN32
  ZmCondition<ZmNoLock>	m_cond[2];
#endif
};

template <bool MR> struct Ctrl_ {
  ZmAtomic<uint32_t>		head;
  uint32_t			pad_1;
  ZmAtomic<uint64_t>		inCount;
  ZmAtomic<uint64_t>		inBytes;
  char				pad_2[Zm::CacheLineSize - 24];

  ZmAtomic<uint32_t>		tail;
  uint32_t			pad_3;
  ZmAtomic<uint64_t>		outCount;
  ZmAtomic<uint64_t>		outBytes;
  char				pad_4[Zm::CacheLineSize - 24];
};

template <> struct Ctrl_<true> : public Ctrl_<false> {
  ZmAtomic<uint32_t>		rdrCount; // reader count
  uint32_t			pad_5;
  ZmAtomic<uint64_t>		rdrMask;  // active readers
  ZmAtomic<uint64_t>		attMask;  // readers pending attach
  ZmAtomic<uint64_t>		attSeqNo; // attach/detach seqNo
};

class ZmAPI CtrlBlk {
public:
  CtrlBlk() = default;
  CtrlBlk(const CtrlBlk &blk) : m_addr(blk.m_addr) { }

  bool open(unsigned size, const ZmBitmap &cpuset);
  void close(unsigned size);

  ZuInline const void *addr() const { return m_addr; }
  ZuInline void *addr() { return m_addr; }

protected:
  void			*m_addr = nullptr;
};

template <typename CtrlBlk_, typename Ctrl__, bool MR> class CtrlFn_ {
protected:
  using CtrlBlk = CtrlBlk_;
  using Ctrl = Ctrl__;

  CtrlFn_() = default;
  CtrlFn_(const CtrlFn_ &ring) : m_ctrl{ring.m_ctrl} { }

  ZuInline const Ctrl *ctrl() const {
    return static_cast<const Ctrl *>(m_ctrl.addr());
  }
  ZuInline Ctrl *ctrl() {
    return static_cast<Ctrl *>(m_ctrl.addr());
  }

  bool openCtrl(const ZmBitmap &cpuset) {
    return m_ctrl.open(sizeof(Ctrl), cpuset);
  }
  void closeCtrl() {
    m_ctrl.close(sizeof(Ctrl));
  }
  constexpr unsigned ctrlSize() const { return sizeof(Ctrl); }

  ZuInline const ZmAtomic<uint32_t> &head() const { return ctrl()->head; }
  ZuInline ZmAtomic<uint32_t> &head() { return ctrl()->head; }

  ZuInline const ZmAtomic<uint32_t> &tail() const { return ctrl()->tail; }
  ZuInline ZmAtomic<uint32_t> &tail() { return ctrl()->tail; }

  ZuInline const ZmAtomic<uint32_t> &inCount() const { return ctrl()->inCount; }
  ZuInline ZmAtomic<uint32_t> &inCount() { return ctrl()->inCount; }
  ZuInline const ZmAtomic<uint32_t> &inBytes() const { return ctrl()->inBytes; }
  ZuInline ZmAtomic<uint32_t> &inBytes() { return ctrl()->inBytes; }
  ZuInline const ZmAtomic<uint32_t> &outCount() const
    { return ctrl()->outCount; }
  ZuInline ZmAtomic<uint32_t> &outCount() { return ctrl()->outCount; }
  ZuInline const ZmAtomic<uint32_t> &outBytes() const
    { return ctrl()->outBytes; }
  ZuInline ZmAtomic<uint32_t> &outBytes() { return ctrl()->outBytes; }

  ZmAtomic<uint32_t> &rdrCount();	// unused
  ZmAtomic<uint64_t> &rdrMask();	// ''
  ZmAtomic<uint64_t> &attMask();	// ''
  ZmAtomic<uint64_t> &attSeqNo();	// ''

private:
  CtrlBlk	m_ctrl;
};
template <typename CtrlBlk_, typename Ctrl__>
class CtrlFn_<CtrlBlk_, Ctrl__, true> :
    public CtrlFn_<CtrlBlk_, Ctrl__, false> {
protected:
  using CtrlBlk = CtrlBlk_;
  using Ctrl = Ctrl__;

private:
  using Base =CtrlFn_<CtrlBlk, Ctrl, false>;

protected:
  using Base::ctrl;

  CtrlFn_() = default;
  CtrlFn_(const CtrlFn_ &ring) : Base{ring} { }

  ZuInline ZmAtomic<uint32_t> &rdrCount() { return ctrl()->rdrCount; }
  ZuInline ZmAtomic<uint64_t> &rdrMask() { return ctrl()->rdrMask; }
  ZuInline ZmAtomic<uint64_t> &attMask() { return ctrl()->attMask; }
  ZuInline ZmAtomic<uint64_t> &attSeqNo() { return ctrl()->attSeqNo; }
};
template <typename CtrlBlk, bool MR>
using CtrlFn = CtrlFn_<CtrlBlk, Ctrl_<MR>, MR>;

template <bool MW, bool MR> class MsgFn {
  constexpr static unsigned align(unsigned n) {
    return (((n) + 8 + 15) & ~15);
  }
};
template <> class MsgFn<false, false> {
  constexpr static unsigned align(unsigned n) {
    return (((n) + 15) & ~15);
  }
};

class ZmAPI DataBlk {
public:
  DataBlk() = default;
  DataBlk(const DataBlk &blk) : m_addr(blk.m_addr) { }

  bool open(unsigned size, const ZmBitmap &cpuset);
  void close(unsigned size);

  ZuInline const void *addr() const { return m_addr; }
  ZuInline void *addr() { return m_addr; }

private:
  void			*m_addr = nullptr;
};

class ZmAPI MirrorDataBlk {
public:
  bool open(unsigned size, const ZmBitmap &cpuset);
  void close(unsigned size);

  ZuInline const void *addr() const { return m_addr; }
  ZuInline void *addr() { return m_addr; }

private:
#ifndef _WIN32
  using Handle = int;
  constexpr static Handle nullHandle() { return -1; }
#else
  using Handle = HANDLE;
  constexpr static Handle nullHandle() { return INVALID_HANDLE_VALUE; }
#endif

  Handle		m_handle = nullHandle();
  void			*m_addr = nullptr;
};

template <typename DataBlk_> class DataFn_ {
protected:
  using DataBlk = DataBlk_;

  DataFn_() = default;
  DataFn_(const DataFn_ &ring) : m_data{ring.m_data} { }

  bool openData(unsigned size, const ZmBitmap &cpuset) {
    return m_data.open(size, cpuset);
  }
  void closeData(unsigned size) {
    m_data.close(size);
  }

  ZuInline const uint8_t *data() const {
    return reinterpret_cast<const uint8_t *>(m_data.addr());
  }
  ZuInline uint8_t *data() {
    return reinterpret_cast<uint8_t *>(m_data.addr());
  }
  ZuInline unsigned dataSize() { return m_data.size(); }

private:
  DataBlk		m_data;
};

template <
  typename DataBlk_, typename MirrorDataBlk_,
  typename T, bool MW, bool MR>
class DataFn : public DataFn_<DataBlk_> {
protected:
  using DataBlk = DataBlk_;

private:
  using Base = DataFn_<DataBlk>;

protected:
  enum { Size = MsgFn<MW, MR>::align(sizeof(T)) };

  DataFn() = default;
  DataFn(const DataFn &ring) : Base{ring} { }

  constexpr static unsigned sizeAlign(unsigned n) {
    return ((n + Size - 1) / Size) * Size;
  }

  DataBlk		m_data;
};
template <typename DataBlk_, typename MirrorDataBlk_, bool MW, bool MR>
class DataFn<DataBlk_, MirrorDataBlk_, void, MW, MR> :
    public DataFn_<MirrorDataBlk_> {
protected:
  using DataBlk = MirrorDataBlk_;

private:
  using Base = DataFn_<DataBlk>;

protected:
  enum { Size = 0 };

  DataFn() = default;
  DataFn(const DataFn &ring) : Base{ring} { }

  constexpr static unsigned sizeAlign(unsigned n) {
    return MsgFn<MW, MR>::align(n);
  }

  DataBlk		m_data;
};

template <typename Ring, bool MR>
class ReadFn {
public:
  void close_() { }

  int rdrID() const;			// unused
  void rdrID(int);			// ''

  uint32_t rdrTail() const;		// ''
  void rdrTail(uint32_t);		// ''

  int attach();				// ''
  int detach();				// ''
};

template <typename Ring>
class ReadFn<Ring, true> {
private:
  Ring *ring() { return static_cast<Ring *>(this); }
  const Ring *ring() const { return static_cast<const Ring *>(this); }

public:
  void close_();

  int rdrID() const { return m_rdrID; }
  void rdrID(int v) { m_rdrID = v; }

  uint32_t rdrTail() const { return m_rdrTail; }
  void rdrTail(uint32_t v) { m_rdrTail = v; }

  int attach();
  int detach();

private:
  int		m_rdrID = -1;
  uint32_t	m_rdrTail = 0;
};

} // ZmRing_

template <typename NTP = ZmRing_::Defaults>
class ZmRing :
    public ZmRing_::Blocker,
    public ZmRing_::MsgFn<NTP::MW, NTP::MR>,
    public ZmRing_::CtrlFn<ZmRing_::CtrlBlk, NTP::MR>,
    public ZmRing_::DataFn<
      ZmRing_::DataBlk, ZmRing_::MirrorDataBlk,
      typename NTP::T, NTP::MW, NTP::MR>,
    public ZmRing_::ReadFn<ZmRing<NTP>, NTP::MR> {
  ZmRing &operator =(const ZmRing &);	// prevent mis-use

protected:
  using Blocker = ZmRing_::Blocker;
  using CtrlBlk = ZmRing_::CtrlBlk;
  using DataBlk = ZmRing_::DataBlk;
  using MirrorDataBlk = ZmRing_::MirrorDataBlk;

  using T = typename NTP::T;
  enum { MW = NTP::MW };
  enum { MR = NTP::MR };

private:
  using MsgFn = ZmRing_::MsgFn<MW, MR>;
  using CtrlFn = ZmRing_::CtrlFn<CtrlBlk, MR>;
  using DataFn = ZmRing_::DataFn<DataBlk, MirrorDataBlk, T, MW, MR>;

  using ParamData = ZmRing_::ParamData;
  template <typename Derived> using Params = ZmRing_::Params<Derived>;

protected:
  enum { // flags
    Ready	= 0x10000000,		// hdr
    EndOfFile_	= 0x20000000,		// hdr, head
    Waiting	= 0x40000000,		// hdr, tail
    Wrapped	= 0x80000000,		// head, tail
    Mask	= Waiting | EndOfFile_,	// does NOT include Wrapped or Ready
    RdrMask	= 0x0fffffff		// hdr
  };
  enum { MaxRdrs = 60 };
#ifdef Zu_BIGENDIAN
  enum { HdrOffset = -2 };
#else
  enum { HdrOffset = -1 };
#endif

  enum { Head = 0, Tail }; // Blocker index parameter

public:
  using SizeAxor = typename NTP::SizeAxor;
  enum { V = ZuConversion<void, T>::Is };

  enum { // open() flags
    Read	= 0x00000001,
    Write	= 0x00000002,
    Shadow	= 0x00000004
  };

  ZmRing() = default;
  template <typename Derived, typename ...Args>
  ZmRing(Params<Derived> params, Args &&... args) :
      m_params{ZuMv(params)} { }

  ZmRing(const ZmRing &ring) :
      m_params{ring.m_params}, m_flags{Shadow},
      CtrlFn{ring}, DataFn{ring} { }

  ~ZmRing() { close(); }

  const ParamData &params() const { return m_params; }

private:
  using Blocker::wait;
  using Blocker::wake;

  using MsgFn::align;

  using Ctrl = CtrlFn::Ctrl;
  using CtrlFn::openCtrl;
  using CtrlFn::closeCtrl;
  using CtrlFn::ctrl;
  using CtrlFn::ctrlSize;
  using CtrlFn::head;
  using CtrlFn::tail;
  using CtrlFn::inCount;
  using CtrlFn::inBytes;
  using CtrlFn::outCount;
  using CtrlFn::outBytes;
  using CtrlFn::rdrCount;
  using CtrlFn::rdrMask;
  using CtrlFn::attMask;
  using CtrlFn::attSeqNo;

  enum { Size = DataFn::Size };
  using DataFn::openData;
  using DataFn::closeData;
  using DataFn::data;
  using DataFn::dataSize;

  using ReadFn = ZmRing_::ReadFn<ZmRing, MR>;
  using ReadFn::close_;
  using ReadFn::attach;
  using ReadFn::detach;
  using ReadFn::rdrID;
  using ReadFn::rdrTail;
friend ReadFn;

public:
  // how many times push() was delayed by this ring buffer being full
  ZuInline unsigned full() const { return m_full; }

  int open(unsigned flags) {
    using namespace ZmRingErrorCode;
    flags &= (Read | Write);
    if (m_flags & Shadow) {
      m_flags = (m_flags & ~(Read | Write)) | flags;
      return OK;
    }
    if (ctrl()) return OK;
    if (!params().size()) return Error;
    m_size = params().size();
    if constexpr (Size) m_size = ((m_size + Size - 1) / Size) * Size;
    m_flags = flags;
    if (!params().ll() && Blocker::open() != OK) return Error;
    if (!openCtrl(params().cpuset())) {
      if (!params().ll()) Blocker::close();
      return Error;
    }
    if (!openData(m_size, params().cpuset())) {
      closeCtrl();
      if (!params().ll()) Blocker::close();
      return Error;
    }
    return OK;
  }

  void close() {
    if (!ctrl()) return;
    close_();
    if (m_flags & Shadow) return;
    closeCtrl();
    closeData(m_size);
    m_size = 0;
    if (!params().ll()) Blocker::close();
  }

  int reset() {
    using namespace ZmRingErrorCode;
    if (!ctrl()) return Error;
    memset(ctrl(), 0, sizeof(Ctrl));
    memset(data(), 0, m_size);
    m_full = 0;
    return OK;
  }

  constexpr static unsigned ctrlSize() { return sizeof(Ctrl); }
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

private:
  template <bool V_ = V>
  ZuInline ZuIfT<!V_, void *> push() {
    return push_<1>();
  }
  template <bool V_ = V>
  ZuInline ZuIfT<V_, void *> push(unsigned size) {
    return push_<1>(size);
  }
  template <bool V_ = V>
  ZuInline ZuIfT<!V_, void *> tryPush() {
    return push_<0>();
  }
  template <bool V_ = V>
  ZuInline ZuIfT<V_, void *> tryPush(unsigned size) {
    return push_<0>(size);
  }

  template <bool V_ = V>
  ZuIfT<!V_, bool> push_full(uint32_t head, uint32_t tail) {
    return ZuUnlikely((head ^ tail) == Wrapped);
  }
  template <bool V_ = V>
  ZuIfT<V_, bool> push_full(uint32_t head, uint32_t tail, unsigned size) {
    if (ZuUnlikely(head == tail)) return false;
    return ZuUnlikely(head < tail ?
	(head + size >= tail) :
	(head + size >= tail + this->size()));
  }

#define ZmRing_push_assert() \
    ZmAssert(ctrl()); \
    ZmAssert(m_flags & Write)

#define ZmRing_push_align_size() \
  size = align(size); \
    ZmAssert(size < this->size())

#define ZmRing_push_get_head_tail() \
    uint32_t head = this->head().load_(); \
    if (ZuUnlikely(head & EndOfFile_)) return nullptr; \
    uint32_t tail = this->tail() /* acquire */

#define ZmRing_push_get_rdrMask() \
    uint64_t rdrMask = this->rdrMask().load_(); \
    if (!rdrMask) return nullptr /* no readers */

#define ZmRing_push_full() push_full(head & ~Mask, tail & ~Mask)

#define ZmRing_push_full_v() push_full(head & ~Mask, tail & ~Mask, size)

#define ZmRing_push_retry() \
    do { \
      ++m_full; \
      if constexpr (!Wait) return nullptr; \
      if (ZuUnlikely(!params().ll())) \
	if (ZmRing_Blocker_wait( \
	      Tail, this->tail(), tail, \
	      params().timeout(), params().spin()) != OK) return nullptr; \
      goto retry; \
    } while (0)

#define ZmRing_advance_head_(size_) \
    auto head_ = head; \
    head += size_; \
    if (ZuUnlikely((head & ~(Wrapped | Mask)) >= this->size())) \
      head = (head ^ Wrapped) - this->size()

#define ZmRing_advance_head(size_) \
    ZmRing_advance_head_(size_); \
    this->head() = head /* release */

#define ZmRing_advance_head_mw(size_) \
    ZmRing_advance_head_(size_); \
    if (ZuUnlikely(this->head().cmpXch(head, head_) != head_)) goto retry

#define ZmRing_push_return() \
    return &(data())[head & ~(Wrapped | Mask)]

#define ZmRing_push_return_swmr() \
    uint8_t *ptr = &(data())[head & ~(Wrapped | Mask)]; \
    *reinterpret_cast<uint64_t *>(ptr) = rdrMask; \
    return &ptr[8]

#define ZmRing_push_return_mwsr() \
    uint8_t *ptr = &(data())[head & ~(Wrapped | Mask)]; \
    *reinterpret_cast<uint64_t *>(ptr) = 0; \
    return &ptr[8]

#define ZmRing_push_return_mwmr() ZmRing_push_return_swmr()

#define ZmRing_push2_get_head() \
    uint32_t head = this->head().load_();

#define ZmRing_push2_wake_readers() \
    if (ZuUnlikely(!params().ll())) { \
      if (ZuUnlikely(this->head().xch(head & ~Waiting) & Waiting)) \
	ZmRing_Blocker_wake(Head, this->head()); \
    } else \
      this->head() = head /* release */

#define ZmRing_push2_get_hdrPtr() \
    auto hdrPtr = &(reinterpret_cast<ZmAtomic<uint32_t> *>( \
	  &(data())[head & ~(Wrapped | Mask)]))[HdrOffset]

#define ZmRing_push2_wake_readers_mwsr() \
    ZmRing_push2_get_hdrPtr(); \
    if (ZuUnlikely(!m_params.ll())) { \
      if (ZuUnlikely(hdrPtr->xch(Ready) & Waiting)) \
	this->ZmRing_Blocker_wake(Head, *hdrPtr); \
    } else \
      *hdrPtr = Ready /* release */

#define ZmRing_push2_wake_readers_mwmr() \
    ZmRing_push2_get_hdrPtr(); \
    auto hdr = hdrPtr->load_(); \
    if (ZuUnlikely(!m_params.ll())) { \
      if (ZuUnlikely(hdrPtr->xch((hdr & RdrMask) | Ready) & Waiting)) \
	this->ZmRing_Blocker_wake(Head, *hdrPtr); \
    } else \
      *hdrPtr = (hdr & RdrMask) | Ready /* release */

#define ZmRing_push2_update_stats(size_) \
    inCount().store_(inCount().load_() + 1); \
    inBytes().store_(inBytes().load_() + size_)

  // fixed-size SWSR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && !MR_ && !V_, void *> push_() {
    using namespace ZmRingErrorCode;
    ZmRing_push_assert();
  retry:
    ZmRing_push_get_head_tail();
    if (ZmRing_push_full()) ZmRing_push_retry();
    ZmRing_push_return();
  }
  template <bool MW_ = MW, bool MR_ = MR>
  ZuIfT<!MW_ && !MR_, void *> push2() {
    ZmRing_push_assert();
    ZmRing_push2_get_head();
    ZmRing_advance_head(Size);
    ZmRing_push2_wake_readers();
    ZmRing_push2_update_stats(Size);
  }
  // variable-size SWSR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && !MR_ && V_, void *> push_(unsigned size) {
    using namespace ZmRingErrorCode;
    ZmRing_push_assert();
    ZmRing_push_align_size();
  retry:
    ZmRing_push_get_head_tail();
    if (ZmRing_push_full_v()) ZmRing_push_retry();
    ZmRing_push_return();
  }
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && !MR_ && V_, void *> push2(unsigned size) {
    ZmRing_push_assert();
    ZmRing_push2_get_head();
    ZmRing_advance_head(size);
    ZmRing_push2_wake_readers();
    ZmRing_push2_update_stats(size);
  }
  // fixed-size SWMR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && MR_ && !V_, void *> push_() {
    using namespace ZmRingErrorCode;
    ZmRing_push_assert();
  retry:
    ZmRing_push_get_rdrMask();
    ZmRing_push_get_head_tail();
    if (ZmRing_push_full()) ZmRing_push_retry();
    ZmRing_push_return_swmr();
  }
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && !MR_ && !V_, void *> push2() {
    ZmRing_push_assert();
    ZmRing_push2_get_head();
    ZmRing_advance_head(Size);
    ZmRing_push2_wake_readers();
    ZmRing_push2_update_stats(Size);
  }
  // variable-size SWMR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && MR_ && V_, void *> push_(unsigned size) {
    using namespace ZmRingErrorCode;
    ZmRing_push_assert();
    ZmRing_push_align_size();
  retry:
    ZmRing_push_get_rdrMask();
    ZmRing_push_get_head_tail();
    if (ZmRing_push_full_v()) ZmRing_push_retry();
    ZmRing_push_return_swmr();
  }
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && MR_ && V_, void *> push2(unsigned size) {
    ZmRing_push_assert();
    ZmRing_push_align_size();
    ZmRing_push2_get_head();
    ZmRing_advance_head(size);
    ZmRing_push2_wake_readers();
    ZmRing_push2_update_stats(size);
  }
  // fixed-size MWSR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && !MR_ && !V_, void *> push_() {
    using namespace ZmRingErrorCode;
    ZmRing_push_assert();
  retry:
    ZmRing_push_get_head_tail();
    if (ZmRing_push_full()) ZmRing_push_retry();
    ZmRing_advance_head_mw(Size);
    ZmRing_push_return_mwsr();
  }
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && !MR_ && !V_, void *> push2(void *ptr) {
    ZmRing_push_assert();
    ZmRing_push2_wake_readers_mwsr();
    ZmRing_push2_update_stats(Size);
  }
  // variable-size MWSR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && !MR_ && V_, void *> push_(unsigned size) {
    using namespace ZmRingErrorCode;
    ZmRing_push_assert();
    ZmRing_push_align_size();
  retry:
    ZmRing_push_get_head_tail();
    if (ZmRing_push_full_v()) ZmRing_push_retry();
    ZmRing_advance_head_mw(size);
    ZmRing_push_return_mwsr();
  }
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && !MR_ && V_, void *> push2(void *ptr, unsigned size) {
    ZmRing_push_assert();
    ZmRing_push2_wake_readers_mwsr();
    ZmRing_push2_update_stats(size);
  }
  // fixed-size MWMR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && MR_ && !V_, void *> push_() {
    using namespace ZmRingErrorCode;
    ZmRing_push_assert();
  retry:
    ZmRing_push_get_head_tail();
    if (ZmRing_push_full()) ZmRing_push_retry();
    ZmRing_advance_head_mw(Size);
    ZmRing_push_return_mwmr();
  }
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && MR_ && !V_, void *> push2(void *ptr) {
    ZmRing_push_assert();
    ZmRing_push2_wake_readers_mwmr();
    ZmRing_push2_update_stats(Size);
  }
  // variable-size MWMR - push2() is same as fixed-size
  template <bool Wait, bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && MR_ && V_, void *> push_(unsigned size) {
    using namespace ZmRingErrorCode;
    ZmRing_push_assert();
    ZmRing_push_align_size();
  retry:
    ZmRing_push_get_head_tail();
    if (ZmRing_push_full_v()) ZmRing_push_retry();
    ZmRing_advance_head_mw(size);
    ZmRing_push_return_mwmr();
  }
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && MR_ && V_, void *> push2(void *ptr, unsigned size) {
    ZmRing_push_assert();
    ZmRing_push2_wake_readers_mwmr();
    ZmRing_push2_update_stats(size);
  }

#define ZmRing_eof_assert() ZmRing_push_assert()

#define ZmRing_eof(rdrCount) \
  uint32_t head = this->head().load_(); \
  if (!eof) { \
    head &= ~EndOfFile_; \
  } else { \
    head |= EndOfFile_; \
  } \
  ZmRing_push2_wake_readers()

#define ZmRing_eof_mw(hdrValue) \
    uint32_t head = this->head().load_(); \
    auto hdrPtr = &(reinterpret_cast<ZmAtomic<uint32_t> *>( \
	  &(data())[head & ~(Wrapped | Mask)]))[HdrOffset]; \
    uint32_t hdr = hdrValue; \
    if (!eof) { \
      head &= ~EndOfFile_; \
      hdr |= Ready; \
    } else { \
      head |= EndOfFile_; \
      hdr |= EndOfFile_ | Ready; \
    } \
    if (ZuUnlikely(!m_params.ll())) { \
      if (ZuUnlikely(hdrPtr->xch(hdr) & Waiting)) \
	this->ZmRing_Blocker_wake(Head, *hdrPtr); \
    } else \
      *hdrPtr = hdr; /* release */ \
    this->head() = head /* release */

  // SWSR
  template <bool MW_ = MW, bool MR_ = MR>
  ZuIfT<!MW_ && !MR_> eof(bool eof = true) {
    ZmRing_eof_assert();
    ZmRing_eof(1);
  }
  // SWMR
  template <bool MW_ = MW, bool MR_ = MR>
  ZuIfT<!MW_ && MR_> eof(bool eof = true) {
    ZmRing_eof_assert();
    ZmRing_eof(rdrCount().load_());
  }
  // MWSR
  template <bool MW_ = MW, bool MR_ = MR>
  ZuIfT<MW_ && !MR_> eof(bool eof = true) {
    ZmRing_eof_assert();
    ZmRing_eof_mw(0);
  }
  // MWMR
  template <bool MW_ = MW, bool MR_ = MR>
  ZuIfT<MW_ && MR_> eof(bool eof = true) {
    ZmRing_eof_assert();
    ZmRing_eof_mw(hdrPtr->load_() & RdrMask);
  }

#define ZmRing_writeStatus_preamble() \
    ZmAssert(m_flags & Write); \
    using namespace ZmRingErrorCode; \
    if (ZuUnlikely(!ctrl())) return Error

#define ZmRing_writeStatus() \
    uint32_t head = this->head().load_(); \
    if (ZuUnlikely(head & EndOfFile_)) return EndOfFile; \
    head &= ~(Wrapped | Mask); \
    uint32_t tail = this->tail() & ~(Wrapped | Mask); \
    if (head < tail) return tail - head; \
    return size() - (head - tail)

  // can be called by writers after push() returns 0; returns
  // Error (not open), NotReady (no readers), EndOfFile,
  // or amount of space remaining in ring buffer (>= 0)
  // SR
  template <bool MR_ = MR>
  ZuIfT<!MR_, int> writeStatus() const {
    ZmRing_writeStatus_preamble();
    ZmRing_writeStatus();
  }
  // MR
  template <bool MR_ = MR>
  ZuIfT<MR_, int> writeStatus() const {
    ZmRing_writeStatus_preamble();
    if (ZuUnlikely(!rdrMask())) return NotReady;
    ZmRing_writeStatus();
  }

  // reader

#define ZmRing_shift_assert() \
    ZmAssert(ctrl()); \
    ZmAssert(m_flags & Read)

#define ZmRing_shift_assert_mr() \
    ZmRing_shift_assert(); \
    ZmAssert(rdrID() >= 0)

#define ZmRing_shift_align_size() ZmRing_push_align_size()

#define ZmRing_shift_get_tail() \
    uint32_t tail = this->tail().load_() & ~Mask

#define ZmRing_shift_get_tail_mr() \
    uint32_t tail = rdrTail()

#define ZmRing_shift_get_head() \
    uint32_t head = this->head(); /* acquire */ \
    /**/ZmRing_bp(shift1)

#define ZmRing_shift_get_hdr() \
    auto hdrPtr = &(reinterpret_cast<ZmAtomic<uint32_t> *>( \
	  &(data())[tail & ~Wrapped]))[2 + HdrOffset]; \
    uint32_t hdr = *hdrPtr /* acquire */

#define ZmRing_shift_empty() (tail == (head & ~Mask))

#define ZmRing_shift_empty_mw() (!(hdr & ~Waiting))

#define ZmRing_shift_retry() \
    do { \
      if (ZuUnlikely(head & EndOfFile)) return nullptr; \
      if (ZuUnlikely(!m_params.ll())) \
	if (ZmRing_Blocker_wait( \
	      Head, this->head(), head, \
	      params().timeout(), params().spin()) != OK) return nullptr; \
      goto retry; \
    } while (0)

#define ZmRing_shift_retry_mw() \
    do { \
      if (ZuUnlikely(!m_params.ll())) \
	if (ZmRing_Blocker_wait( \
	      Head, *hdrPtr, hdr, \
	      params().timeout(), params().spin()) != OK) return nullptr; \
      goto retry; \
    } while (0) \

#define ZmRing_shift_return_swsr() \
    return reinterpret_cast<T *>(&(data())[tail & ~Wrapped])

#define ZmRing_shift_return_swmr() \
    return reinterpret_cast<T *>(&(data())[(tail & ~Wrapped) + 8])

#define ZmRing_shift_return_mwsr() \
    if (ZuUnlikely(head & EndOfFile)) return nullptr; \
    hdrPtr->store_(0); \
    ZmRing_shift_return_swmr()

#define ZmRing_shift_return_mwmr() ZmRing_shift_return_mwsr()

#define ZmRing_shift2_move_tail(size_) \
    tail += size_; \
    if (ZuUnlikely((tail & ~Wrapped) >= this->size())) \
      tail = (tail ^ Wrapped) - this->size()

#define ZmRing_shift2_wake_writers() \
    if (ZuUnlikely(!params().ll())) { \
      if (ZuUnlikely(this->tail().xch(tail & ~Waiting) & Waiting)) \
	ZmRing_Blocker_wake(Tail, this->tail()); \
    } else \
      this->tail() = tail /* release */

#define ZmRing_shift2_wake_writers_mr() \
    if (*reinterpret_cast<ZmAtomic<uint64_t> *>( \
	  &(data())[tail & ~Wrapped]) &= ~(1ULL<<rdrID())) return; \
    ZmRing_shift2_wake_writers()

#define ZmRing_shift2_update_stats(size_) \
    this->outCount().store_(this->outCount().load_() + 1); \
    this->outBytes().store_(this->outBytes().load_() + size_)

  // SWSR
  template <bool MW_ = MW, bool MR_ = MR>
  ZuIfT<!MW_ && !MR_, T *> shift() {
    using namespace ZmRingErrorCode;
    ZmRing_shift_assert();
    ZmRing_shift_get_tail();
  retry:
    ZmRing_shift_get_head();
    if (ZmRing_shift_empty()) ZmRing_shift_retry();
    ZmRing_shift_return_swsr();
  }
  // fixed-size SWSR
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && !MR_ && !V_> shift2() {
    ZmRing_shift_assert();
    ZmRing_shift_get_tail();
    ZmRing_shift2_move_tail(Size);
    ZmRing_shift2_wake_writers();
    ZmRing_shift2_update_stats(Size);
  }
  // variable-size SWSR
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && !MR_ && V_> shift2(unsigned size) {
    ZmRing_shift_assert();
    ZmRing_shift_align_size();
    ZmRing_shift_get_tail();
    ZmRing_shift2_move_tail(size);
    ZmRing_shift2_wake_writers();
    ZmRing_shift2_update_stats(size);
  }
  // SWMR
  template <bool MW_ = MW, bool MR_ = MR>
  ZuIfT<!MW_ && MR_, T *> shift() {
    using namespace ZmRingErrorCode;
    ZmRing_shift_assert_mr();
    ZmRing_shift_get_tail_mr();
  retry:
    ZmRing_shift_get_head();
    if (ZmRing_shift_empty()) ZmRing_shift_retry();
    ZmRing_shift_return_swmr();
  }
  // fixed-size SWMR
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && MR_ && !V_> shift2() {
    ZmRing_shift_assert_mr();
    ZmRing_shift_get_tail_mr();
    ZmRing_shift2_move_tail(Size);
    ZmRing_shift2_wake_writers_mr();
    ZmRing_shift2_update_stats(Size);
  }
  // variable-size SWMR
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && MR_ && V_> shift2(unsigned size) {
    ZmRing_shift_assert_mr();
    ZmRing_shift_align_size();
    ZmRing_shift_get_tail_mr();
    ZmRing_shift2_move_tail(size);
    ZmRing_shift2_wake_writers_mr();
    ZmRing_shift2_update_stats(size);
  }
  // MWSR
  template <bool MW_ = MW, bool MR_ = MR>
  ZuIfT<MW_ && !MR_, T *> shift() {
    using namespace ZmRingErrorCode;
    ZmRing_shift_assert();
    ZmRing_shift_get_tail();
  retry:
    ZmRing_shift_get_hdr();
    if (ZmRing_shift_empty_mw()) ZmRing_shift_retry_mw();
    ZmRing_shift_return_mwsr();
  }
  // fixed-size MWSR
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && !MR_ && !V_> shift2() {
    ZmRing_shift_assert();
    ZmRing_shift_get_tail();
    ZmRing_shift2_move_tail(Size);
    ZmRing_shift2_wake_writers();
    ZmRing_shift2_update_stats(Size);
  }
  // variable-size MWSR
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && !MR_ && V_> shift2(unsigned size) {
    ZmRing_shift_assert();
    ZmRing_shift_align_size();
    ZmRing_shift_get_tail();
    ZmRing_shift2_move_tail(size);
    ZmRing_shift2_wake_writers();
    ZmRing_shift2_update_stats(size);
  }
  // MWMR
  template <bool MW_ = MW, bool MR_ = MR>
  ZuIfT<MW_ && MR_, T *> shift() {
    using namespace ZmRingErrorCode;
    ZmRing_shift_assert_mr();
    ZmRing_shift_get_tail_mr();
  retry:
    ZmRing_shift_get_hdr();
    if (ZmRing_shift_empty_mw()) ZmRing_shift_retry_mw();
    ZmRing_shift_return_mwmr();
  }
  // fixed-size MWMR
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && MR_ && !V_> shift2() {
    ZmRing_shift_assert_mr();
    ZmRing_shift_get_tail_mr();
    ZmRing_shift2_move_tail(Size);
    ZmRing_shift2_wake_writers_mr();
    ZmRing_shift2_update_stats(Size);
  }
  // variable-size MWMR
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && MR_ && V_> shift2(unsigned size) {
    ZmRing_shift_assert_mr();
    ZmRing_shift_align_size();
    ZmRing_shift_get_tail_mr();
    ZmRing_shift2_move_tail(size);
    ZmRing_shift2_wake_writers_mr();
    ZmRing_shift2_update_stats(size);
  }

#define ZmRing_readStatus_preamble() \
    ZmAssert(m_flags & Read); \
    using namespace ZmRingErrorCode; \
    if (ZuUnlikely(!ctrl())) return Error

#define ZmRing_readStatus() \
    ZmRing_shift_get_head(); \
    if (ZuUnlikely(head & EndOfFile)) return EndOfFile; \
    head &= ~Mask; \
    if ((head ^ tail) == Wrapped) return size(); \
    head &= ~Wrapped; \
    tail &= ~Wrapped; \
    if (head >= tail) return head - tail; \
    return size() - (tail - head)

  // can be called by a reader after shift() returns 0; returns
  // EndOfFile, or amount of data remaining in ring buffer (>= 0)
  // SR
  template <bool MR_ = MR>
  ZuIfT<!MR_, int> readStatus() const {
    ZmRing_readStatus_preamble();
    ZmRing_shift_get_tail();
    ZmRing_readStatus();
  }
  // MR
  template <bool MR_ = MR>
  ZuIfT<MR_, int> readStatus() const {
    ZmRing_readStatus_preamble();
    ZmRing_shift_get_tail_mr();
    ZmRing_readStatus();
  }

  unsigned count_() const {
    int i = readStatus();
    if (i < 0) return 0;
    return i / Size;
  }

  void stats(
      uint64_t &inCount, uint64_t &inBytes, 
      uint64_t &outCount, uint64_t &outBytes) const {
    ZmAssert(ctrl());

    inCount = this->inCount().load_();
    inBytes = this->inBytes().load_();
    outCount = this->outCount().load_();
    outBytes = this->outBytes().load_();
  }

private:
  ParamData		m_params;
  uint32_t		m_flags = 0;
  uint32_t		m_size = 0;
  uint32_t		m_full = 0;
};

namespace ZmRing_ {

template <typename Ring>
inline void ReadFn<Ring, true>::close_()
{
  if (ring()->m_flags & Ring::Read) {
    if (rdrID() >= 0) detach();
    --ring()->rdrCount();
  }
}

template <typename Ring>
inline int ReadFn<Ring, true>::attach()
{
  enum { Read = Ring::Read };

  ZmAssert(ring()->ctrl());
  ZmAssert(ring()->m_flags & Read);

  using namespace ZmRingErrorCode;

  if (rdrID() >= 0) return Error;

  enum { MaxRdrs = Ring::MaxRdrs };

  // allocate an ID for this reader
  {
    uint64_t attMask;
    unsigned id;
    do {
      attMask = ring()->attMask().load_();
      for (id = 0; id < MaxRdrs; id++)
	if (!(attMask & (1ULL<<id))) break;
      if (id == MaxRdrs) return Error;
    } while (
	ring()->attMask().cmpXch(attMask | (1ULL<<id), attMask) != attMask);
    rdrID(id);
  }

  ++(ring()->attSeqNo());

  /**/ZmRing_bp(attach1);

  enum {
    Wrapped = Ring::Wrapped,
    Mask = Ring::Mask
  };

  // skip any trailing messages not intended for us, since other readers
  // may be concurrently advancing the ring's tail; this must be
  // re-attempted as long as the head keeps moving and the writer remains
  // unaware of our attach
  uint32_t tail = ring()->tail().load_() & ~Mask;
  uint32_t head = ring()->head() & ~Mask, head_; // acquire
  /**/ZmRing_bp(attach2);
  ring()->rdrMask() |= (1ULL<<rdrID()); // notifies the writer about an attach
  /**/ZmRing_bp(attach3);
  auto data = ring()->data();
  auto size = ring()->size();

  using SizeAxor = Ring::SizeAxor;

  do {
    while (tail != head) {
      uint8_t *ptr = &data[tail & ~Wrapped];
      if (*reinterpret_cast<uint64_t *>(ptr) & (1ULL<<rdrID()))
	goto done; // writer aware
      tail += align(SizeAxor::get(&ptr[8]));
      if ((tail & ~Wrapped) >= size) tail = (tail ^ Wrapped) - size;
    }
    head_ = head;
    /**/ZmRing_bp(attach4);
    head = ring()->head() & ~Mask; // acquire
  } while (head != head_);

done:
  rdrTail(tail);

  ++(ring()->attSeqNo());

  return OK;
}

template <typename Ring>
inline int ReadFn<Ring, true>::detach()
{
  enum { Read = Ring::Read };

  ZmAssert(ring()->ctrl());
  ZmAssert(ring()->m_flags & Read);

  using namespace ZmRingErrorCode;

  if (rdrID() < 0) return Error;

  ++(ring()->attSeqNo());

  ring()->rdrMask() &= ~(1ULL<<rdrID()); // notifies the writer about a detach
  /**/ZmRing_bp(detach1);

  enum {
    Waiting = Ring::Waiting,
    Wrapped = Ring::Wrapped,
    Mask = Ring::Mask
  };

  // drain any trailing messages that are waiting to be read by us,
  // advancing ring's tail as needed; this must be
  // re-attempted as long as the head keeps moving and the writer remains
  // unaware of our detach
  uint32_t tail = rdrTail();
  /**/ZmRing_bp(detach2);
  uint32_t head = ring()->head() & ~Mask, head_; // acquire

  auto data = ring()->data();
  auto size = ring()->size();

  using SizeAxor = Ring::SizeAxor;

  enum { Tail = Ring::Tail };

  do {
    while (tail != head) {
      uint8_t *ptr = &data[tail & ~Wrapped];
      if (!(*reinterpret_cast<uint64_t *>(ptr) & (1ULL<<rdrID())))
	goto done; // writer aware
      tail += align(SizeAxor::get(&ptr[8]));
      if ((tail & ~Wrapped) >= size) tail = (tail ^ Wrapped) - size;
      if (*reinterpret_cast<ZmAtomic<uint64_t> *>(ptr) &= ~(1ULL<<rdrID()))
	continue;
      /**/ZmRing_bp(detach3);
      if (ZuUnlikely(!ring()->params().ll())) {
	if (ZuUnlikely(ring()->tail().xch(tail) & Waiting))
	  ring()->ZmRing_Blocker_wake(Tail, ring()->tail());
      } else
	ring()->tail() = tail; // release
    }
    head_ = head;
    /**/ZmRing_bp(detach4);
    head = ring()->head() & ~Mask; // acquire
  } while (head != head_);
done:
  rdrTail(tail);

  // release ID for re-use by future attach
  /**/ZmRing_bp(detach5);

  ++(ring()->attSeqNo());

  ring()->attMask() &= ~(1ULL<<rdrID());
  rdrID(-1);

  return OK;
}

} // ZmRing_

#endif /* ZmRing_HPP */
