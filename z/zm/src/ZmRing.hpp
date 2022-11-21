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

// ring buffer
// - single/multiple writers/producers and readers/consumers
//   - supports SWSR MWSR SWMR MWMR
// - fixed- and variable-sized messages (types)

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
#define ZmRing_bp(self, name) self->bp_##name.reached(#name)
#else
#ifdef ZmRing_STRESSTEST
#define ZmRing_bp(self, name) ZmPlatform::yield()
#else
#define ZmRing_bp(self, name) (void())
#endif
#endif

// ring buffer parameters

// uses NTP (named template parameters):
//
// ZmRingT<T>			// ring of fixed-size T

namespace ZmRing_ {

// NTP defaults
struct Defaults {
  using T = void;					// variable-sized
  constexpr static auto SizeAxor = [](const void *) { return 0; };
  enum { MW = 0 };
  enum { MR = 0 };
  enum { Broadcast = 0 };
};

} // ZmRing_

// fixed-size message type
template <typename T_, typename NTP = ZmRing_::Defaults>
struct ZmRingT : public NTP {
  using T = T_;
  constexpr static auto SizeAxor = [](const void *) { return sizeof(T); };
};
template <typename NTP>
struct ZmRingT<ZuNull, NTP> : public NTP {
  using T = void;
  constexpr static auto SizeAxor = [](const void *) { return 0; };
};

// variable-sized message type
template <auto SizeAxor_, typename NTP = ZmRing_::Defaults>
struct ZmRingSizeAxor : public NTP {
  constexpr static auto SizeAxor = SizeAxor_;
};

// multiple writers (producers)
template <bool MW_, typename NTP = ZmRing_::Defaults>
struct ZmRingMW : public NTP {
  enum { MW = MW_ };
};

// multiple readers (consumers)
template <bool MR_, typename NTP = ZmRing_::Defaults>
struct ZmRingMR : public NTP {
  enum { MR = MR_ };
};

// broadcast
template <bool Broadcast_, typename NTP = ZmRing_::Defaults>
struct ZmRingBroadcast : public NTP {
  enum { Broadcast = Broadcast_ };
  enum { MR = Broadcast_ }; // broadcast implies MR
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
  Params() = default;
  Params(unsigned size) : ParamData{size} { }
  Derived &&size(unsigned n) { m_size = n; return derived(); }
  Derived &&ll(bool b) { m_ll = b; return derived(); }
  Derived &&cpuset(ZmBitmap b) { m_cpuset = ZuMv(b); return derived(); }
  Derived &&spin(unsigned n) { m_spin = n; return derived(); }
  Derived &&timeout(unsigned n) { m_timeout = n; return derived(); }
};

namespace Status {
  enum { OK = 0, EndOfFile = -1, IOError = -2, NotReady = -3 };
  inline const char *name(int i) {
    static const char *names[] = { "OK", "EndOfFile", "IOError", "NotReady" };
    if (i > 0) i = 0;
    i = -i;
    if (i > (sizeof(names) / sizeof(names[0]))) return "Unknown";
    return names[i];
  }
}
using namespace Status;

class ZmAPI Blocker {
public:
  // block until woken or timeout while addr == val
  int wait(
      ZmAtomic<uint32_t> &addr, uint32_t val,
      unsigned timeout, unsigned spin);
  // wake up waiters on addr
  void wake(ZmAtomic<uint32_t> &addr);

protected:
#ifdef _WIN32
  HANDLE		m_sem;
#endif
};

class ZmAPI CtrlMem {
public:
  CtrlMem() = default;
  CtrlMem(const CtrlMem &mem) : m_addr{mem.m_addr} { }

  bool open(unsigned size, const ZmBitmap &cpuset);
  void close(unsigned size);

  ZuInline const void *addr() const { return m_addr; }
  ZuInline void *addr() { return m_addr; }

protected:
  void		*m_addr = nullptr;
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

template <typename CtrlMem_, typename Ctrl__, bool MR> class CtrlMgr_ {
protected:
  using CtrlMem = CtrlMem_;
  using Ctrl = Ctrl__;

  CtrlMgr_() = default;
  CtrlMgr_(const CtrlMgr_ &ring) : m_ctrl{ring.m_ctrl} { }

public:
  ZuInline const Ctrl *ctrl() const {
    return static_cast<const Ctrl *>(m_ctrl.addr());
  }
  ZuInline Ctrl *ctrl() {
    return static_cast<Ctrl *>(m_ctrl.addr());
  }

protected:
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

  ZuInline const ZmAtomic<uint64_t> &inCount() const { return ctrl()->inCount; }
  ZuInline ZmAtomic<uint64_t> &inCount() { return ctrl()->inCount; }
  ZuInline const ZmAtomic<uint64_t> &inBytes() const { return ctrl()->inBytes; }
  ZuInline ZmAtomic<uint64_t> &inBytes() { return ctrl()->inBytes; }
  ZuInline const ZmAtomic<uint64_t> &outCount() const
    { return ctrl()->outCount; }
  ZuInline ZmAtomic<uint64_t> &outCount() { return ctrl()->outCount; }
  ZuInline const ZmAtomic<uint64_t> &outBytes() const
    { return ctrl()->outBytes; }
  ZuInline ZmAtomic<uint64_t> &outBytes() { return ctrl()->outBytes; }

  ZmAtomic<uint32_t> &rdrCount();		// unused
  const ZmAtomic<uint32_t> &rdrCount() const;	// ''
  ZmAtomic<uint64_t> &rdrMask();		// ''
  const ZmAtomic<uint64_t> &rdrMask() const;	// ''
  ZmAtomic<uint64_t> &attMask();		// ''
  const ZmAtomic<uint64_t> &attMask() const;	// ''
  ZmAtomic<uint64_t> &attSeqNo();		// ''
  const ZmAtomic<uint64_t> &attSeqNo() const;	// ''

private:
  CtrlMem	m_ctrl;
};
template <typename CtrlMem_, typename Ctrl__>
class CtrlMgr_<CtrlMem_, Ctrl__, true> :
    public CtrlMgr_<CtrlMem_, Ctrl__, false> {
protected:
  using CtrlMem = CtrlMem_;
  using Ctrl = Ctrl__;

private:
  using Base =CtrlMgr_<CtrlMem, Ctrl, false>;

protected:
  using Base::ctrl;

  CtrlMgr_() = default;
  CtrlMgr_(const CtrlMgr_ &ring) : Base{ring} { }

  ZuInline ZmAtomic<uint32_t> &rdrCount() { return ctrl()->rdrCount; }
  ZuInline const ZmAtomic<uint32_t> &rdrCount() const
    { return ctrl()->rdrCount; }
  ZuInline ZmAtomic<uint64_t> &rdrMask() { return ctrl()->rdrMask; }
  ZuInline const ZmAtomic<uint64_t> &rdrMask() const
    { return ctrl()->rdrMask; }
  ZuInline ZmAtomic<uint64_t> &attMask() { return ctrl()->attMask; }
  ZuInline const ZmAtomic<uint64_t> &attMask() const
    { return ctrl()->attMask; }
  ZuInline ZmAtomic<uint64_t> &attSeqNo() { return ctrl()->attSeqNo; }
  ZuInline const ZmAtomic<uint64_t> &attSeqNo() const
    { return ctrl()->attSeqNo; }
};
template <typename CtrlMem, bool MR>
using CtrlMgr = CtrlMgr_<CtrlMem, Ctrl_<MR>, MR>;

template <bool MW, bool MR> struct AlignFn {
  constexpr static unsigned align(unsigned n) {
    return (((n) + 8 + 15) & ~15);
  }
};
template <> struct AlignFn<false, false> {
  constexpr static unsigned align(unsigned n) {
    return (((n) + 15) & ~15);
  }
};

class ZmAPI DataMem {
public:
  DataMem() = default;
  DataMem(const DataMem &mem) : m_addr(mem.m_addr) { }

  bool open(unsigned size, const ZmBitmap &cpuset);
  void close(unsigned size);

  ZuInline const void *addr() const { return m_addr; }
  ZuInline void *addr() { return m_addr; }

private:
  void			*m_addr = nullptr;
};

class ZmAPI MirrorDataMem {
public:
  static unsigned alignSize(unsigned size);

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

template <typename DataMem_> class DataMgr_ {
protected:
  using DataMem = DataMem_;

  DataMgr_() = default;
  DataMgr_(const DataMgr_ &ring) : m_data{ring.m_data} { }

  bool openData(unsigned size, const ZmBitmap &cpuset) {
    return m_data.open(size, cpuset);
  }
  void closeData(unsigned size) {
    m_data.close(size);
  }

public:
  ZuInline const uint8_t *data() const {
    return reinterpret_cast<const uint8_t *>(m_data.addr());
  }
  ZuInline uint8_t *data() {
    return reinterpret_cast<uint8_t *>(m_data.addr());
  }

private:
  DataMem		m_data;
};

template <
  typename DataMem_, typename MirrorDataMem_,
  typename T, bool MW, bool MR>
class DataMgr : public DataMgr_<DataMem_> {
protected:
  using DataMem = DataMem_;

private:
  using Base = DataMgr_<DataMem>;

public:
  enum { MsgSize = AlignFn<MW, MR>::align(sizeof(T)) };

protected:
  DataMgr() = default;
  DataMgr(const DataMgr &ring) : Base{ring} { }

  constexpr static unsigned alignSize(unsigned n) {
    return ((n + MsgSize - 1) / MsgSize) * MsgSize;
  }
};
template <typename DataMem_, typename MirrorDataMem_, bool MW, bool MR>
class DataMgr<DataMem_, MirrorDataMem_, void, MW, MR> :
    public DataMgr_<MirrorDataMem_> {
protected:
  using DataMem = MirrorDataMem_;

private:
  using Base = DataMgr_<DataMem>;

protected:
  enum { MsgSize = 0 };

  DataMgr() = default;
  DataMgr(const DataMgr &ring) : Base{ring} { }

  static unsigned alignSize(unsigned n) {
    return DataMem::alignSize(n);
  }
};

inline constexpr uint64_t EndOfFile_() { return static_cast<uint64_t>(1)<<61; }
inline constexpr uint64_t Waiting()    { return static_cast<uint64_t>(2)<<61; }
inline constexpr uint64_t Wrapped()    { return static_cast<uint64_t>(4)<<61; }
inline constexpr uint64_t Mask()       { return Waiting() | EndOfFile_(); }
inline constexpr uint64_t RdrMask()  { return ~(static_cast<uint64_t>(7)<<61); }
enum { MaxRdrs = 61 };
#ifdef Zu_BIGENDIAN
enum { Flags32Offset = 0 };	// 32bit offset of flags within 64bit header
#else
enum { Flags32Offset = 1 };
#endif
// 32bit versions of flags
inline constexpr uint32_t EndOfFile32() { return EndOfFile_()>>32; }
inline constexpr uint32_t Waiting32()   { return Waiting()>>32; }
inline constexpr uint32_t Wrapped32()   { return Wrapped()>>32; }
inline constexpr uint32_t Mask32()      { return Waiting32() | EndOfFile32(); }

// CRTP - SR ring reader functions
template <typename Ring, bool MR>
class RingRdr {
  Ring *ring() { return static_cast<Ring *>(this); }
  const Ring *ring() const { return static_cast<const Ring *>(this); }

public:
  int attach();				// unused
  int detach();				// ''

  int rdrID() const { return 0; }	// ''

protected:
  void close_() { }

  void rdrID(int);			// ''

  uint32_t rdrTail() const;		// ''
  void rdrTail(uint32_t);		// ''
};
// CRTP - MR ring reader functions
template <typename Ring>
class RingRdr<Ring, true> {
  Ring *ring() { return static_cast<Ring *>(this); }
  const Ring *ring() const { return static_cast<const Ring *>(this); }

public:
  int attach();
  int detach();

  int rdrID() const { return m_rdrID; }

protected:
  void close_();

  void rdrID(int v) { m_rdrID = v; }

  uint32_t rdrTail() const { return m_rdrTail; }
  void rdrTail(uint32_t v) { m_rdrTail = v; }

private:
  int		m_rdrID = -1;
  uint32_t	m_rdrTail = 0;
};

template <typename NTP = Defaults>
class Ring :
    public AlignFn<NTP::MW, NTP::MR>,
    public CtrlMgr<CtrlMem, NTP::MR>,
    public DataMgr<
      DataMem, MirrorDataMem,
      typename NTP::T, NTP::MW, NTP::MR>,
    public RingRdr<Ring<NTP>, NTP::MR> {
public:
  using T = typename NTP::T;
  enum { MW = NTP::MW };
  enum { MR = NTP::MR };
  enum { Broadcast = NTP::Broadcast };

private:
  using AlignFn_ = AlignFn<MW, MR>;
  using CtrlMgr_ = CtrlMgr<CtrlMem, MR>;
  using DataMgr_ = DataMgr<DataMem, MirrorDataMem, T, MW, MR>;
  using RingRdr_ = RingRdr<Ring<NTP>, MR>;
friend RingRdr_;

public:
  constexpr static auto SizeAxor = NTP::SizeAxor;
  enum { V = ZuConversion<void, T>::Same };
  enum { MsgSize = DataMgr_::MsgSize };

  // MR requires a non-default SizeAxor
  ZuAssert(
      (!MR || !ZuConversion<
	decltype([](const void *) { return 0; }),
	decltype(SizeAxor)
      >::Same));

  enum { // open() flags
    Read	= 0x00000001,
    Write	= 0x00000002,
    Shadow	= 0x00000004
  };

  Ring() = default;

  template <typename Derived, typename ...Args>
  Ring(Params<Derived> params, Args &&... args) :
      m_params{static_cast<ParamData &&>(params)} { }

  Ring(const Ring &ring) :
      CtrlMgr_{ring}, DataMgr_{ring},
      m_params{ring.m_params}, m_flags{Shadow}, m_size{ring.m_size} { }

  Ring &operator =(const Ring &ring) {
    ~Ring();
    new (this) Ring{ring};
    return *this;
  }

  ~Ring() { close(); }

  template <typename Derived>
  void init(Params<Derived> params) {
    m_params = static_cast<ParamData &&>(params);
  }

  const ParamData &params() const { return m_params; }

  auto &headBlocker() { return m_headBlocker; }
  const auto &headBlocker() const { return m_headBlocker; }
  auto &tailBlocker() { return m_tailBlocker; }
  const auto &tailBlocker() const { return m_tailBlocker; }

private:
  using AlignFn_::align;

  using Ctrl = typename CtrlMgr_::Ctrl;
  using CtrlMgr_::openCtrl;
  using CtrlMgr_::closeCtrl;
public:
  using CtrlMgr_::ctrl;
private:
  using CtrlMgr_::head;
  using CtrlMgr_::tail;
  using CtrlMgr_::inCount;
  using CtrlMgr_::inBytes;
  using CtrlMgr_::outCount;
  using CtrlMgr_::outBytes;
  using CtrlMgr_::rdrCount;
  using CtrlMgr_::rdrMask;
  using CtrlMgr_::attMask;
  using CtrlMgr_::attSeqNo;

  using DataMgr_::openData;
  using DataMgr_::closeData;
  using DataMgr_::alignSize;
public:
  using DataMgr_::data;

  using RingRdr_::attach;
  using RingRdr_::detach;
  using RingRdr_::rdrID;
private:
  using RingRdr_::close_;
  using RingRdr_::rdrTail;

public:
  // how many times push() was delayed by this ring buffer being full
  ZuInline unsigned full() const { return m_full; }

  int open(unsigned flags) {
    flags &= (Read | Write);
    if (m_flags & Shadow) {
      m_flags = (m_flags & ~(Read | Write)) | flags;
      return OK;
    }
    if (ctrl()) return OK;
    if (!params().size()) return IOError;
    m_size = alignSize(params().size());
    m_flags = flags;
    if (!openCtrl(params().cpuset())) return IOError;
    if (!openData(m_size, params().cpuset())) {
      closeCtrl();
      return IOError;
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
  }

  int reset() {
    if (!ctrl()) return IOError;
    memset(static_cast<void *>(ctrl()), 0, sizeof(Ctrl));
    memset(data(), 0, m_size);
    m_full = 0;
    return OK;
  }

  constexpr static unsigned ctrlSize() { return sizeof(Ctrl); }
  unsigned size() const { return m_size; }

  unsigned length() {
    uint32_t head = this->head().load_() & ~Mask32();
    uint32_t tail = this->tail().load_() & ~Mask32();
    if (head == tail) return 0;
    if ((head ^ tail) == Wrapped32()) return size();
    head &= ~Wrapped32();
    tail &= ~Wrapped32();
    if (head > tail) return head - tail;
    return size() - (tail - head);
  }

  // inspection accessors

  uint32_t head_() const { return this->head().load_(); }
  uint32_t tail_() const {
    if constexpr (MR)
      return rdrTail();
    else
      return this->tail().load_();
  }

  // writer

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

private:
  template <bool V_ = V>
  ZuIfT<!V_, bool> pushFull(uint32_t head, uint32_t tail) {
    head &= ~Mask32();
    tail &= ~Mask32();
    return ZuUnlikely((head ^ tail) == Wrapped32());
  }
  template <bool V_ = V>
  ZuIfT<V_, bool> pushFull(uint32_t head, uint32_t tail, unsigned size) {
    head &= ~Mask32();
    tail &= ~Mask32();
    if (ZuUnlikely(head == tail)) return false;
    if (ZuUnlikely((head ^ tail) == Wrapped32())) return true;
    head &= ~Wrapped32();
    tail &= ~Wrapped32();
    return ZuUnlikely(head > tail ?
	(head + size >= tail + this->size()) :
	(head + size >= tail));
  }

  void writeAssert() {
    ZmAssert(ctrl());
    ZmAssert(m_flags & Write);
  }

  unsigned alignAssert(unsigned size) {
    size = align(size);
    ZmAssert(size < this->size());
    return size;
  }

#define ZmRing_push_get_head_tail() \
    uint32_t head = this->head().load_(); \
    if (ZuUnlikely(head & EndOfFile32())) return nullptr; \
    uint32_t tail = this->tail() /* acquire */

#define ZmRing_push_check_rdrMask() \
    if (!this->rdrMask().load_()) return nullptr /* no readers */

#define ZmRing_push_retry() \
    do { \
      ++m_full; \
      if constexpr (!Wait) return nullptr; \
      if (ZuUnlikely(!params().ll())) \
	if (m_tailBlocker.wait( \
	      this->tail(), tail, \
	      params().timeout(), params().spin()) != OK) return nullptr; \
      goto retry; \
    } while (0)

#define ZmRing_move_head_(msgSize) \
    head += msgSize; \
    if (ZuUnlikely((head & ~(Wrapped32() | Mask32())) >= this->size())) \
      head = (head ^ Wrapped32()) - this->size()

// SWSR - push2() advances head, wakeReaders() updates head
#define ZmRing_move_head_swsr(msgSize) \
    ZmRing_move_head_(msgSize);
// SWMR - push2() advances head, wakeReaders() does not update head
#define ZmRing_move_head_swmr(msgSize) \
    auto head_ = head; \
    ZmRing_move_head_(msgSize); \
    *reinterpret_cast<uint64_t *>( \
	&(data())[head & ~(Wrapped32() | Mask32())]) = 0; /* clear ahead */ \
    this->head() = head /* release */
// MWSR | MWMR - push() advances head, updating it atomically
#define ZmRing_move_head_mwsr(msgSize) \
    auto head_ = head; \
    ZmRing_move_head_(msgSize); \
    if (ZuUnlikely(this->head().cmpXch(head, head_) != head_)) goto retry; \
    *reinterpret_cast<uint64_t *>( \
	&(data())[head & ~(Wrapped32() | Mask32())]) = 0 /* clear ahead */
#define ZmRing_move_head_mwmr(msgSize) ZmRing_move_head_mwsr(msgSize) 

#define ZmRing_push_return_swsr() \
    return &(data())[head & ~(Wrapped32() | Mask32())]
#define ZmRing_push_return_swmr() \
    auto ptr = reinterpret_cast<uint64_t *>( \
	&(data())[head & ~(Wrapped32() | Mask32())]); \
    return &ptr[1]
#define ZmRing_push_return_mwsr() \
    auto ptr = reinterpret_cast<uint64_t *>( \
	&(data())[head_ & ~(Wrapped32() | Mask32())]); \
    return &ptr[1]
#define ZmRing_push_return_mwmr() ZmRing_push_return_mwsr()

#define ZmRing_push2_get_head() \
    uint32_t head = this->head().load_();

#define ZmRing_push2_update_stats(msgSize) \
    inCount().store_(inCount().load_() + 1); \
    inBytes().store_(inBytes().load_() + msgSize)

#define ZmRing_eof_get_head() ZmRing_push2_get_head()

  // SWSR
  template <uint64_t Flags = 0, bool MW_ = MW, bool MR_ = MR>
  ZuIfT<!MW_ && !MR_> wakeReaders(uint32_t head) {
    if (ZuUnlikely(!params().ll())) {
      if (ZuUnlikely(this->head().xch((head & ~Waiting32()) |
	      static_cast<uint32_t>(Flags>>32)) & Waiting32()))
	m_headBlocker.wake(this->head());
    } else
      this->head() = head | static_cast<uint32_t>(Flags>>32); // release
  }
  // !SWSR
  template <uint64_t Flags = 0, bool MW_ = MW, bool MR_ = MR>
  ZuIfT<MW_ || MR_> wakeReaders(uint32_t head) {
    wakeReaders_<Flags>(
	reinterpret_cast<ZmAtomic<uint64_t> *>(
	    &(data())[head & ~(Wrapped32() | Mask32())]));
  }
  template <uint64_t Flags = 0, bool MW_ = MW, bool MR_ = MR>
  ZuIfT<MW_ || MR_> wakeReaders(void *ptr) {
    wakeReaders_<Flags>(
	&reinterpret_cast<ZmAtomic<uint64_t> *>(ptr)[-1]);
  }
  // SWMR | MWMR
  template <uint64_t Flags = 0, bool MR_ = MR>
  ZuIfT<MR_> wakeReaders_(ZmAtomic<uint64_t> *hdrPtr) {
    uint64_t rdrMask = this->rdrMask().load_();
    // FIXME for MR unicast
    if (ZuUnlikely(!m_params.ll())) {
      if (ZuUnlikely(hdrPtr->xch(Flags | rdrMask) & Waiting())) {
	auto &hdrPtr32 =
	  reinterpret_cast<ZmAtomic<uint32_t> *>(hdrPtr)[Flags32Offset];
	m_headBlocker.wake(hdrPtr32);
      }
    } else
      *hdrPtr = Flags | rdrMask; // release
  }
  // MWSR
  template <uint64_t Flags = 0, bool MR_ = MR>
  ZuIfT<!MR_> wakeReaders_(ZmAtomic<uint64_t> *hdrPtr) {
    if (ZuUnlikely(!m_params.ll())) {
      if (ZuUnlikely(hdrPtr->xch(Flags | 1) & Waiting())) {
	auto &hdrPtr32 =
	  reinterpret_cast<ZmAtomic<uint32_t> *>(hdrPtr)[Flags32Offset];
	m_headBlocker.wake(hdrPtr32);
      }
    } else
      *hdrPtr = Flags | 1; // release
  }

  // fixed-size SWSR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && !MR_ && !V_, void *> push_() {
    writeAssert();
  retry:
    ZmRing_push_get_head_tail();
    if (pushFull(head, tail)) ZmRing_push_retry();
    ZmRing_push_return_swsr();
  }
public:
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && !MR_ && !V_> push2() {
    writeAssert();
    ZmRing_push2_get_head();
    ZmRing_move_head_swsr(MsgSize);
    wakeReaders(head);
    ZmRing_push2_update_stats(MsgSize);
  }
private:
  // variable-size SWSR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && !MR_ && V_, void *> push_(unsigned size) {
    writeAssert();
    size = alignAssert(size);
  retry:
    ZmRing_push_get_head_tail();
    if (pushFull(head, tail, size)) ZmRing_push_retry();
    ZmRing_push_return_swsr();
  }
public:
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && !MR_ && V_> push2(unsigned size) {
    writeAssert();
    size = alignAssert(size);
    ZmRing_push2_get_head();
    ZmRing_move_head_swsr(size);
    wakeReaders(head);
    ZmRing_push2_update_stats(size);
  }
private:
  // fixed-size SWMR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && MR_ && !V_, void *> push_() {
    writeAssert();
  retry:
    ZmRing_push_check_rdrMask();
    ZmRing_push_get_head_tail();
    if (pushFull(head, tail)) ZmRing_push_retry();
    ZmRing_push_return_swmr();
  }
public:
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && MR_ && !V_> push2() {
    writeAssert();
    ZmRing_push2_get_head();
    ZmRing_move_head_swmr(MsgSize);
    wakeReaders(head_);
    ZmRing_push2_update_stats(MsgSize);
  }
private:
  // variable-size SWMR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && MR_ && V_, void *> push_(unsigned size) {
    writeAssert();
    size = alignAssert(size);
  retry:
    ZmRing_push_check_rdrMask();
    ZmRing_push_get_head_tail();
    if (pushFull(head, tail, size)) ZmRing_push_retry();
    ZmRing_push_return_swmr();
  }
public:
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && MR_ && V_> push2(unsigned size) {
    writeAssert();
    size = alignAssert(size);
    ZmRing_push2_get_head();
    ZmRing_move_head_swmr(size);
    wakeReaders(head_);
    ZmRing_push2_update_stats(size);
  }
private:
  // fixed-size MWSR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && !MR_ && !V_, void *> push_() {
    writeAssert();
  retry:
    ZmRing_push_get_head_tail();
    if (pushFull(head, tail)) ZmRing_push_retry();
    ZmRing_move_head_mwsr(MsgSize);
    ZmRing_push_return_mwsr();
  }
public:
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && !MR_ && !V_> push2(void *ptr) {
    writeAssert();
    wakeReaders(ptr);
    ZmRing_push2_update_stats(MsgSize);
  }
private:
  // variable-size MWSR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && !MR_ && V_, void *> push_(unsigned size) {
    writeAssert();
    size = alignAssert(size);
  retry:
    ZmRing_push_get_head_tail();
    if (pushFull(head, tail, size)) ZmRing_push_retry();
    ZmRing_move_head_mwsr(size);
    ZmRing_push_return_mwsr();
  }
public:
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && !MR_ && V_> push2(void *ptr, unsigned size) {
    writeAssert();
    size = alignAssert(size);
    wakeReaders(ptr);
    ZmRing_push2_update_stats(size);
  }
private:
  // fixed-size MWMR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && MR_ && !V_, void *> push_() {
    writeAssert();
  retry:
    ZmRing_push_check_rdrMask();
    ZmRing_push_get_head_tail();
    if (pushFull(head, tail)) ZmRing_push_retry();
    ZmRing_move_head_mwmr(MsgSize);
    ZmRing_push_return_mwmr();
  }
public:
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && MR_ && !V_> push2(void *ptr) {
    writeAssert();
    wakeReaders(ptr);
    ZmRing_push2_update_stats(MsgSize);
  }
private:
  // variable-size MWMR - push2() is same as fixed-size
  template <bool Wait, bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && MR_ && V_, void *> push_(unsigned size) {
    writeAssert();
    size = alignAssert(size);
  retry:
    ZmRing_push_check_rdrMask();
    ZmRing_push_get_head_tail();
    if (pushFull(head, tail, size)) ZmRing_push_retry();
    ZmRing_move_head_mwmr(size);
    ZmRing_push_return_mwmr();
  }
public:
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && MR_ && V_> push2(void *ptr, unsigned size) {
    writeAssert();
    size = alignAssert(size);
    wakeReaders(ptr);
    ZmRing_push2_update_stats(size);
  }

  // EOF signalling is complex:
  // for SWSR, readers wait on the head, which is signalled with 32bit flags
  // in all other cases, readers wait on the hdr
  // wakeReaders() updates either the head, or the hdr, accordingly
  // ... however readStatus() only examines the head, not the hdr, and
  // needs to determine EOF, so eof() updates head explicitly for non-SWSR
  void eof(bool eof = true) {
    writeAssert();
    ZmRing_eof_get_head();
    if (!eof) {
      head &= ~EndOfFile32();
      if constexpr (MW || MR) this->head() = head; // see above
      wakeReaders(head);
    } else {
      if constexpr (MW || MR) this->head() = head | EndOfFile32(); // ''
      wakeReaders<EndOfFile_()>(head);
    }
  }

#define ZmRing_writeStatus_preamble() \
    ZmAssert(m_flags & Write); \
    if (ZuUnlikely(!ctrl())) return IOError

#define ZmRing_writeStatus() \
    uint32_t head = this->head().load_(); \
    if (ZuUnlikely(head & EndOfFile32())) return EndOfFile; \
    head &= ~(Wrapped32() | Mask32()); \
    uint32_t tail = this->tail() & ~(Wrapped32() | Mask32()); \
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

public:
  ZuInline T *shift() { return shift_<1>(); }
  ZuInline T *tryShift() { return shift_<0>(); }

private:
  template <bool MR_ = MR>
  ZuIfT<!MR_> readAssert() {
    ZmAssert(ctrl());
    ZmAssert(m_flags & Read);
  }
  template <bool MR_ = MR>
  ZuIfT<MR_> readAssert() {
    readAssert<false>();
    ZmAssert(rdrID() >= 0);
  }

#define ZmRing_shift_get_tail_() \
    this->tail().load_() & ~Mask32()

#define ZmRing_shift_get_tail() \
    uint32_t tail = ZmRing_shift_get_tail_()
#define ZmRing_shift_get_tail_mr() \
    uint32_t tail = rdrTail()

#define ZmRing_shift_get_head() \
    uint32_t head = this->head(); /* acquire */ \
    /**/ZmRing_bp(this, shift1)

#define ZmRing_shift_get_hdr() \
    auto hdrPtr = reinterpret_cast<ZmAtomic<uint64_t> *>( \
	  &(data())[tail & ~Wrapped32()]); \
    uint64_t hdr = *hdrPtr; /* acquire */ \
    /**/ZmRing_bp(this, shift1)

#define ZmRing_shift_empty_swsr() (tail == (head & ~Mask32()))
#define ZmRing_shift_empty_swmr() (!(hdr & ~Waiting()))
#define ZmRing_shift_empty_mwsr() ZmRing_shift_empty_swmr()
#define ZmRing_shift_empty_mwmr() ZmRing_shift_empty_swmr()

#define ZmRing_shift_retry_swsr() \
    do { \
      if (ZuUnlikely(head & EndOfFile32())) return nullptr; \
      if constexpr (!Wait) return nullptr; \
      if (ZuUnlikely(!m_params.ll())) \
	if (m_headBlocker.wait( \
	      this->head(), head, \
	      params().timeout(), params().spin()) != OK) return nullptr; \
      goto retry; \
    } while (0)
#define ZmRing_shift_retry_swmr() \
    do { \
      if (ZuUnlikely(hdr & EndOfFile_())) return nullptr; \
      if constexpr (!Wait) return nullptr; \
      if (ZuUnlikely(!m_params.ll())) { \
	auto &hdrPtr32 = \
	  reinterpret_cast<ZmAtomic<uint32_t> *>(hdrPtr)[Flags32Offset]; \
	if (m_headBlocker.wait( \
	      hdrPtr32, hdr>>32, \
	      params().timeout(), params().spin()) != OK) return nullptr; \
      } \
      goto retry; \
    } while (0)
#define ZmRing_shift_retry_mwsr() ZmRing_shift_retry_swmr()
#define ZmRing_shift_retry_mwmr() ZmRing_shift_retry_swmr()

#define ZmRing_move_tail_(msgSize) \
    tail += msgSize; \
    if (ZuUnlikely((tail & ~Wrapped32()) >= this->size())) \
      tail = (tail ^ Wrapped32()) - this->size()

#define ZmRing_shift_return_swsr() \
    return reinterpret_cast<T *>(&(data())[tail & ~Wrapped32()])
#define ZmRing_shift_return_mwsr() \
    return reinterpret_cast<T *>(&(data())[(tail & ~Wrapped32()) + 8])
#define ZmRing_shift_return_swmr() \
    if (ZuUnlikely(!(hdr & (1ULL<<rdrID())))) { \
      if constexpr (V) { \
	unsigned msgSize = align(SizeAxor(&hdrPtr[1])); \
	std::cout << "\nGOT HERE " << ZuBoxed(hdr).hex() << ' ' << msgSize << '\n' << std::flush; \
	ZmRing_move_tail_(msgSize); \
      } else { \
	ZmRing_move_tail_(MsgSize); \
      } \
      goto retry; \
    } \
    ZmRing_shift_return_mwsr();
#define ZmRing_shift_return_mwmr() ZmRing_shift_return_swmr()

#define ZmRing_move_tail_swsr(msgSize) \
    ZmRing_move_tail_(msgSize)
#define ZmRing_move_tail_swmr(msgSize) \
    auto tail_ = tail; \
    ZmRing_move_tail_(msgSize); \
    rdrTail(tail); \
    if (*reinterpret_cast<ZmAtomic<uint64_t> *>( \
	&(data())[tail_ & ~Wrapped32()]) &= \
	  (RdrMask() & ~(1ULL<<rdrID()))) \
      return
#define ZmRing_move_tail_mwsr(msgSize) \
    *reinterpret_cast<ZmAtomic<uint64_t> *>( \
	&(data())[tail & ~Wrapped32()]) = 0; \
    ZmRing_move_tail_(msgSize)
#define ZmRing_move_tail_mwmr(msgSize) ZmRing_move_tail_swmr(msgSize)

#define ZmRing_shift2_update_stats(msgSize) \
    this->outCount().store_(this->outCount().load_() + 1); \
    this->outBytes().store_(this->outBytes().load_() + msgSize)

  void wakeWriters(uint32_t tail) {
    if (ZuUnlikely(!params().ll())) {
      if (ZuUnlikely(this->tail().xch(tail & ~Waiting32()) & Waiting32()))
	m_tailBlocker.wake(this->tail());
    } else
      this->tail() = tail; // release
  }

  // SWSR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR>
  ZuIfT<!MW_ && !MR_, T *> shift_() {
    readAssert();
    ZmRing_shift_get_tail();
  retry:
    ZmRing_shift_get_head();
    if (ZmRing_shift_empty_swsr()) ZmRing_shift_retry_swsr();
    ZmRing_shift_return_swsr();
  }
public:
  // fixed-size SWSR
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && !MR_ && !V_> shift2() {
    readAssert();
    ZmRing_shift_get_tail();
    ZmRing_move_tail_swsr(MsgSize);
    wakeWriters(tail);
    ZmRing_shift2_update_stats(MsgSize);
  }
  // variable-size SWSR
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && !MR_ && V_> shift2(unsigned size) {
    readAssert();
    size = alignAssert(size);
    ZmRing_shift_get_tail();
    ZmRing_move_tail_swsr(size);
    wakeWriters(tail);
    ZmRing_shift2_update_stats(size);
  }
private:
  // SWMR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR>
  ZuIfT<!MW_ && MR_, T *> shift_() {
    readAssert();
    ZmRing_shift_get_tail_mr();
  retry:
    ZmRing_shift_get_hdr();
    if (ZmRing_shift_empty_swmr()) ZmRing_shift_retry_swmr();
    ZmRing_shift_return_swmr();
  }
public:
  // fixed-size SWMR
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && MR_ && !V_> shift2() {
    readAssert();
    ZmRing_shift_get_tail_mr();
    ZmRing_move_tail_swmr(MsgSize);
    wakeWriters(tail);
    ZmRing_shift2_update_stats(MsgSize);
  }
  // variable-size SWMR
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<!MW_ && MR_ && V_> shift2(unsigned size) {
    readAssert();
    size = alignAssert(size);
    ZmRing_shift_get_tail_mr();
    ZmRing_move_tail_swmr(size);
    wakeWriters(tail);
    ZmRing_shift2_update_stats(size);
  }
private:
  // MWSR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR>
  ZuIfT<MW_ && !MR_, T *> shift_() {
    readAssert();
    ZmRing_shift_get_tail();
  retry:
    ZmRing_shift_get_hdr();
    if (ZmRing_shift_empty_mwsr()) ZmRing_shift_retry_mwsr();
    ZmRing_shift_return_mwsr();
  }
public:
  // fixed-size MWSR
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && !MR_ && !V_> shift2() {
    readAssert();
    ZmRing_shift_get_tail();
    ZmRing_move_tail_mwsr(MsgSize);
    wakeWriters(tail);
    ZmRing_shift2_update_stats(MsgSize);
  }
  // variable-size MWSR
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && !MR_ && V_> shift2(unsigned size) {
    readAssert();
    size = alignAssert(size);
    ZmRing_shift_get_tail();
    ZmRing_move_tail_mwsr(size);
    wakeWriters(tail);
    ZmRing_shift2_update_stats(size);
  }
private:
  // MWMR
  template <bool Wait, bool MW_ = MW, bool MR_ = MR>
  ZuIfT<MW_ && MR_, T *> shift_() {
    readAssert();
    ZmRing_shift_get_tail_mr();
  retry:
    ZmRing_shift_get_hdr();
    if (ZmRing_shift_empty_mwmr()) ZmRing_shift_retry_mwmr();
    ZmRing_shift_return_mwmr();
  }
public:
  // fixed-size MWMR
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && MR_ && !V_> shift2() {
    readAssert();
    ZmRing_shift_get_tail_mr();
    ZmRing_move_tail_mwmr(MsgSize);
    wakeWriters(tail);
    ZmRing_shift2_update_stats(MsgSize);
  }
  // variable-size MWMR
  template <bool MW_ = MW, bool MR_ = MR, bool V_ = V>
  ZuIfT<MW_ && MR_ && V_> shift2(unsigned size) {
    readAssert();
    size = alignAssert(size);
    ZmRing_shift_get_tail_mr();
    ZmRing_move_tail_mwmr(size);
    wakeWriters(tail);
    ZmRing_shift2_update_stats(size);
  }

#define ZmRing_readStatus_preamble() \
    ZmAssert(m_flags & Read); \
    if (ZuUnlikely(!ctrl())) return IOError

#define ZmRing_readStatus() \
    ZmRing_shift_get_head(); \
    bool eof = head & EndOfFile32(); \
    head &= ~Mask32(); \
    if ((head ^ tail) == Wrapped32()) return size(); \
    head &= ~Wrapped32(); \
    tail &= ~Wrapped32(); \
    if (head >= tail) { \
      if (head > tail) return head - tail; \
      if (ZuUnlikely(eof)) return EndOfFile; \
      return 0; \
    } \
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
    if constexpr (!MsgSize)
      return i;
    else
      return i / MsgSize;
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
  Blocker		m_headBlocker, m_tailBlocker;
  uint32_t		m_flags = 0;
  uint32_t		m_size = 0;
  uint32_t		m_full = 0;

#ifdef ZmRing_FUNCTEST
public:
  ZmRing_Breakpoint	bp_attach1;
  ZmRing_Breakpoint	bp_attach2;
  ZmRing_Breakpoint	bp_attach3;
  ZmRing_Breakpoint	bp_attach4;
  ZmRing_Breakpoint	bp_detach1;
  ZmRing_Breakpoint	bp_detach2;
  ZmRing_Breakpoint	bp_detach3;
  ZmRing_Breakpoint	bp_shift1;
#endif
};

template <typename Ring>
inline void RingRdr<Ring, true>::close_()
{
  if (ring()->m_flags & Ring::Read) {
    if (rdrID() >= 0) detach();
    --ring()->rdrCount();
  }
}

template <typename Ring, bool MR>
inline int RingRdr<Ring, MR>::attach()	// unused
{
  /**/ZmRing_bp(ring(), attach1);
  /**/ZmRing_bp(ring(), attach2);
  /**/ZmRing_bp(ring(), attach3);
  /**/ZmRing_bp(ring(), attach4);
  return OK;
}

template <typename Ring>
inline int RingRdr<Ring, true>::attach()
{
  enum { Read = Ring::Read };

  ZmAssert(ring()->ctrl());
  ZmAssert(ring()->m_flags & Read);

  if (rdrID() >= 0) return IOError;

  // allocate an ID for this reader
  {
    uint64_t attMask;
    unsigned id;
    do {
      attMask = ring()->attMask().load_();
      for (id = 0; id < MaxRdrs; id++)
	if (!(attMask & (1ULL<<id))) break;
      if (id == MaxRdrs) return IOError;
    } while (
	ring()->attMask().cmpXch(attMask | (1ULL<<id), attMask) != attMask);
    rdrID(id);
  }

  ++(ring()->attSeqNo());

  /**/ZmRing_bp(ring(), attach1);

  rdrTail(ring()->tail().load_() & ~Mask32());

  /**/ZmRing_bp(ring(), attach2);

  ring()->rdrMask() |= (1ULL<<rdrID()); // notifies the writer about an attach

  /**/ZmRing_bp(ring(), attach3);

  ++(ring()->attSeqNo());

  /**/ZmRing_bp(ring(), attach4);

  return OK;
}

template <typename Ring, bool MR>
inline int RingRdr<Ring, MR>::detach()	// unused
{
  /**/ZmRing_bp(ring(), detach1);
  /**/ZmRing_bp(ring(), detach2);
  /**/ZmRing_bp(ring(), detach3);
  return OK;
}

template <typename Ring>
inline int RingRdr<Ring, true>::detach()
{
  enum { Read = Ring::Read };

  ZmAssert(ring()->ctrl());
  ZmAssert(ring()->m_flags & Read);

  if (rdrID() < 0) return IOError;

  ++(ring()->attSeqNo());

  ring()->rdrMask() &= ~(1ULL<<rdrID()); // notifies the writer about a detach
  /**/ZmRing_bp(ring(), detach1);

  // drain any trailing messages that are waiting to be read by us,
  // advancing ring's tail as needed; this must be
  // re-attempted as long as the head keeps moving and the writer remains
  // unaware of our detach
  uint32_t tail = rdrTail();
  /**/ZmRing_bp(ring(), detach2);
  uint32_t head = ring()->head() & ~Mask32(); // acquire
  uint32_t head_;

  auto data = ring()->data();
  auto size = ring()->size();

  do {
    while (tail != head) {
      auto hdrPtr = reinterpret_cast<ZmAtomic<uint64_t> *>(
	  &data[tail & ~Wrapped32()]);
      if (!(*hdrPtr & (1ULL<<rdrID()))) goto done; // writer aware
      tail += ring()->align(Ring::SizeAxor(&hdrPtr[1]));
      if ((tail & ~Wrapped32()) >= size) tail = (tail ^ Wrapped32()) - size;
      if (*hdrPtr &= ~(1ULL<<rdrID())) continue;
      ring()->wakeWriters(tail);
    }
    head_ = head;
    head = ring()->head() & ~Mask32(); // acquire
  } while (head != head_);
done:
  /**/ZmRing_bp(ring(), detach3);

  rdrTail(tail);

  // release ID for re-use by future attach

  ++(ring()->attSeqNo());

  ring()->attMask() &= ~(1ULL<<rdrID());
  rdrID(-1);

  return OK;
}

} // ZmRing_

struct ZmRingParams : public ZmRing_::Params<ZmRingParams> {
  ZmRingParams() = default;
  ZmRingParams(unsigned size) : ZmRing_::Params<ZmRingParams>{size} { }
};

template <typename NTP = ZmRing_::Defaults> using ZmRing = ZmRing_::Ring<NTP>;

namespace ZmRingStatus {
  using namespace ZmRing_::Status;
};

struct ZmRingError {
  int code;
  ZmRingError(int code_) : code{code_} { }
  template <typename S> void print(S &s) const {
    s << ZmRingStatus::name(code);
  }
  friend ZuPrintFn ZuPrintType(ZmRingError *);
};

#endif /* ZmRing_HPP */
