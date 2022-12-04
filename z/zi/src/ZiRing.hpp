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

// shared memory IPC ring buffer

#ifndef ZiRing_HPP
#define ZiRing_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZiLib_HPP
#include <zlib/ZiLib.hpp>
#endif

#include <zlib/ZmRing.hpp>

#include <zlib/ZiPlatform.hpp>
#include <zlib/ZiFile.hpp>

namespace ZiRing_ {

using namespace ZmRing_;

// ring buffer parameters

struct ParamData : public ZmRing_::ParamData {
  using Base = ZmRing_::ParamData;

  ParamData() = default;
  ParamData(const ParamData &) = default;
  ParamData(ParamData &&) = default;
  template <
    typename Arg0, typename ...Args,
    typename = ZuIsNot<ZmRing_::ParamData, Arg0>>
  ParamData(Arg0 &&arg0, Args &&... args) :
      Base{ZuFwd<Args>(args)...}, name{ZuFwd<Arg0>(arg0)} { }

  Zi::Path	name;
  unsigned	killWait = 1;
  bool		coredump = false;
};

template <typename Derived, typename Data = ParamData>
class Params_ : public ZmRing_::Params_<Derived, Data> {
  using Base = ZmRing_::Params_<Derived, Data>;

  Derived &&derived() { return ZuMv(*static_cast<Derived *>(this)); }

public:
  Params_() = default;
  Params_(const Params_ &) = default;
  Params_(Params_ &&) = default;
  template <
    typename Arg0, typename ...Args,
    typename = ZuIsNot<ZmRing_::ParamData, Arg0>>
  Params_(Arg0 &&arg0, Args &&... args) :
      Base{ZuFwd<Arg0>(arg0), ZuFwd<Args>(args)...} { }

  Derived &&name(ZuString s) { Data::name = s; return derived(); }
  Derived &&killWait(unsigned n) { Data::killWait = n; return derived(); }
  Derived &&coredump(bool b) { Data::coredump = b; return derived(); }
};

class Params : public Params_<Params> {
  using Base = Params_<Params>;

public:
  Params() = default;
  Params(const Params &) = default;
  Params(Params &&) = default;
  template <
    typename Arg0, typename ...Args,
    typename = ZuIsNot<ZmRing_::ParamData, Arg0>>
  Params(Arg0 &&arg0, Args &&... args) :
      Base{ZuFwd<Arg0>(arg0), ZuFwd<Args>(args)...} { }
};

class ZiAPI Blocker {
public:
  using Params = ParamData;

  Blocker();
  ~Blocker();

  bool open(bool head, const Params &);
  void close();

  // block until woken or timeout while addr == val
  int wait(
      ZmAtomic<uint32_t> &addr, uint32_t val,
      const Params &params);
  // wake up waiters on addr
  void wake(ZmAtomic<uint32_t> &addr);

protected:
#ifdef _WIN32
  HANDLE		m_sem;
#endif
};

class ZiAPI CtrlMem {
public:
  using Params = ParamData;

  CtrlMem() = default;
  CtrlMem(const CtrlMem &mem) : m_file{mem.m_file} { }

  bool open(unsigned size, const Params &params);
  void close(unsigned size, const Params &params);

  ZuInline const void *addr() const { return m_file.addr(); }
  ZuInline void *addr() { return m_file.addr(); }

protected:
  ZiFile	m_file;
};

struct Ctrl : public ZmRing_::Ctrl<true> {
  ZmAtomic<uint32_t>		openSize; // opened size && latency
  uint32_t			pad_6;
  ZmAtomic<uint32_t>		writerPID;
  ZmTime			writerTime;
  uint32_t			rdrPID[MaxRdrs];
  ZmTime			rdrTime[MaxRdrs];
};

template <typename CtrlMem_, typename Ctrl_>
class CtrlMgr_ : public ZmRing_::CtrlMgr_<CtrlMem_, Ctrl_, true> {
protected:
  using CtrlMem = CtrlMem_;
  using Params = typename CtrlMem::Params;
  using Ctrl = Ctrl_;

private:
  using Base = ZmRing_::CtrlMgr_<CtrlMem, Ctrl, true>;

protected:
  using Base::ctrl;

  CtrlMgr_() = default;
  CtrlMgr_(const CtrlMgr_ &mgr) : Base{mgr} { }

  // PIDs may be re-used by the OS, so processes are ID'd by PID + start time

  ZuInline ZmAtomic<uint32_t> &openSize() { return ctrl()->openSize; }
  ZuInline const ZmAtomic<uint32_t> &openSize() const
    { return ctrl()->openSize; }
  ZuInline ZmAtomic<uint32_t> &writerPID() { return ctrl()->writerPID; }
  ZuInline const ZmAtomic<uint32_t> &writerPID() const
    { return ctrl()->writerPID; }
  ZuInline ZmTime &writerTime() { return ctrl()->writerTime; }
  ZuInline const ZmTime &writerTime() const { return ctrl()->writerTime; }
  ZuInline uint32_t *rdrPID() { return ctrl()->rdrPID; }
  ZuInline const uint32_t *rdrPID() const { return ctrl()->rdrPID; }
  ZuInline ZmTime *rdrTime() { return ctrl()->rdrTime; }
  ZuInline const ZmTime *rdrTime() const { return ctrl()->rdrTime; }
};
template <typename CtrlMem>
using CtrlMgr = CtrlMgr_<CtrlMem, Ctrl>;

class ZiAPI DataMem {
public:
  using Params = ParamData;

  DataMem() = default;
  DataMem(const DataMem &mem) : m_file{mem.m_file} { }

  bool open(unsigned size, const Params &params);
  void close(unsigned size, const Params &params);

  ZuInline const void *addr() const { return m_file.addr(); }
  ZuInline void *addr() { return m_file.addr(); }

private:
  ZiFile	m_file;
};

class ZiAPI MirrorMem {
public:
  using Params = ParamData;

  MirrorMem() = default;
  MirrorMem(const MirrorMem &mem) : m_file{mem.m_file} { }

  static unsigned alignSize(unsigned size);

  bool open(unsigned size, const Params &params);
  void close(unsigned size, const Params &params);

  ZuInline const void *addr() const { return m_file.addr(); }
  ZuInline void *addr() { return m_file.addr(); }

private:
  ZiFile	m_file;
};

// ring extensions base class for shared memory IPC
class ZiAPI RingExt_ {
public:
  static void getpinfo(uint32_t &pid, ZmTime &start);
  static bool alive(uint32_t pid, ZmTime start);
  static bool kill(uint32_t pid, bool coredump);
};

// generic ring extensions for shared memory IPC - always multi-reader
template <typename Ring, bool>
class RingExt :
    public RingExt_,
    public ZmRing_::RingExt<Ring, true> {
  using Base = ZmRing_::RingExt<Ring, true>;

  Ring *ring() { return static_cast<Ring *>(this); }
  const Ring *ring() const { return static_cast<const Ring *>(this); }

public:
  using Friend = Base; // extend ZmRing friendship to ZmRing_::RingExt

  RingExt() = default;

  RingExt(const RingExt &ring) : Base{ring} { }
  RingExt &operator =(const RingExt &ring) {
    if (this != &ring) {
      this->~RingExt();
      new (this) RingExt{ring};
    }
    return *this;
  }

  RingExt(RingExt &&) = delete;
  RingExt &operator =(RingExt &&) = delete;

  // can be called by writer if ring is full to garbage collect
  // dead readers and any lingering messages intended exclusively for them;
  // returns space freed
  unsigned gc();

  // kills all stalled readers (following a timeout), sleeps, then runs gc()
  unsigned kill();

protected:
  bool open_();
  void close_();

  void attached(unsigned id);
  void detached(unsigned id);
};

template <typename Ring, bool MR>
inline bool RingExt<Ring, MR>::open_()
{
  auto &params = ring()->params();

  if (params.size) {
    uint32_t reqSize =
      static_cast<uint32_t>(params.size) |
      static_cast<uint32_t>(params.ll);
    // check that requested sizes and latency are consistent
    if (uint32_t openSize = ring()->openSize().cmpXch(reqSize, 0))
      if (openSize != reqSize) return false;
  } else {
    uint32_t openSize = ring()->openSize();
    if (!(openSize & ~1)) return false;
    params.size = openSize & ~1;
    params.ll = openSize & 1;
  }

  if (!Base::open_()) return false;

  if (ring()->flags() & Ring::Write) {
    uint32_t pid;
    ZmTime start;
    getpinfo(pid, start);
    uint32_t oldPID = ring()->writerPID().load_();
    if (alive(oldPID, ring()->writerTime()) ||
	ring()->writerPID().cmpXch(pid, oldPID) != oldPID) {
      Base::close_();
      return false;
    }
    ring()->writerTime() = start;
  }

  return true;
}

template <typename Ring, bool MR>
inline void RingExt<Ring, MR>::close_()
{
  if (ring()->flags() & Ring::Write) {
    ring()->writerTime() = ZmTime{}; // writerPID store is a release
    ring()->writerPID() = 0;
  }

  Base::close_();
}

template <typename Ring, bool MR>
inline unsigned RingExt<Ring, MR>::gc()
{
  ZmAssert(ring()->ctrl());
  ZmAssert(ring()->flags() & Ring::Write);

  const auto &params = ring()->params();

  auto rdrPID = ring()->rdrPID();
  auto rdrTime = ring()->rdrTime();

  // GC dead readers

  unsigned freed = 0;
  uint64_t dead;
  unsigned rdrCount;

  // below loop is a probe - as long as any concurrent attach() or
  // detach() overlap with our discovery of dead readers, the results
  // are unreliable and the probe must be re-attempted - after N attempts
  // give up and return 0
  for (unsigned i = 0;; ) {
    uint64_t attSeqNo = ring()->attSeqNo().load_();
    dead = ring()->rdrMask(); // assume all dead
    rdrCount = 0;
    if (dead) {
      for (unsigned id = 0; id < MaxRdrs; id++) {
	if (!(dead & (1ULL<<id))) continue;
	if (alive(rdrPID[id], rdrTime[id])) {
	  dead &= ~(1ULL<<id);
	  ++rdrCount;
	}
      }
    }
    if (attSeqNo == ring()->attSeqNo()) break;
    Zm::yield();
    if (++i == params.spin) return 0;
  }

  auto data = ring()->data();
  auto size = ring()->size();

  uint32_t tail_ = ring()->tail(); // acquire
  uint32_t tail = tail_ & ~Mask32();
  uint32_t head = ring()->head().load_() & ~Mask32();

  while (tail != head) {
    auto hdrPtr = reinterpret_cast<ZmAtomic<uint64_t> *>(
	&data[tail & ~Wrapped32()]);
    auto msgSize = ring()->align(Ring::SizeAxor(&hdrPtr[1]));
    tail += msgSize;
    if ((tail & ~Wrapped32()) >= size) tail = (tail ^ Wrapped32()) - size;
    uint64_t mask = hdrPtr->xchAnd(~dead);
    if (mask && !(mask & ~dead)) {
      freed += msgSize;
      ring()->wakeWriters(tail);
    }
  }

  for (unsigned id = 0; id < MaxRdrs; id++)
    if (dead & (1ULL<<id))
      if (rdrPID[id]) {
	ring()->rdrMask() &= ~(1ULL<<id);
	detached(id);
	++(ring()->attSeqNo());
	ring()->attMask() &= ~(1ULL<<id);
      }
  ring()->rdrCount() = rdrCount;
  return freed;
}

template <typename Ring, bool MR>
inline unsigned RingExt<Ring, MR>::kill()
{
  const auto &params = ring()->params();

  auto data = ring()->data();
  auto rdrPID = ring()->rdrPID();

  uint64_t hdr;
  {
    uint32_t tail = ring()->tail() & ~Mask32();
    if (tail == (ring()->head() & ~Mask32())) return 0;
    auto hdrPtr = reinterpret_cast<ZmAtomic<uint64_t> *>(
	&data[tail & ~Wrapped32()]);
    hdr = *hdrPtr;
  }
  for (unsigned id = 0; id < MaxRdrs; id++)
    if (hdr & (1ULL<<id))
      kill(rdrPID[id], params.coredump);
  Zm::sleep(ZmTime{static_cast<time_t>(params.killWait)});
  return gc();
}

template <typename Ring, bool MR>
inline void RingExt<Ring, MR>::attached(unsigned id)
{
  getpinfo((ring()->rdrPID())[id], (ring()->rdrTime())[id]);
}

template <typename Ring, bool MR>
inline void RingExt<Ring, MR>::detached(unsigned id)
{
  (ring()->rdrPID())[id] = 0;
  (ring()->rdrTime())[id] = ZmTime{};
}

} // ZiRing_

using ZiRingParams = ZiRing_::Params;

template <typename NTP = ZiRing_::Defaults>
using ZiRing = ZmRing_::Ring<
  ZmRingMR<true, NTP>,
  ZiRing_::ParamData,
  ZiRing_::Blocker,
  ZiRing_::CtrlMgr<ZiRing_::CtrlMem>,
  ZmRing_::DataMgr<
    ZiRing_::DataMem, ZiRing_::MirrorMem, typename NTP::T, NTP::MW, true>,
  ZiRing_::RingExt>;

#endif /* ZiRing_HPP */
