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
  ParamData(unsigned size_) : Base{size_} { }
  ParamData(ZuString name_, unsigned size_) : Base{size_}, name{name_} { }

  Zi::Path	name;
  unsigned	killWait = 1;
  bool		coredump = false;
};

template <typename Derived, typename Data = ParamData>
class Params_ : public ZmRing_::Params_<Derived, Data> {
  Derived &&derived() { return ZuMv(*static_cast<Derived *>(this)); }

public:
  Params_() = default;
  Params_(unsigned size) : Data{size} { }
  Params_(ZuString name, unsigned size) : Data{name, size} { }

  Derived &&name(ZuString s) { Data::name = s; return derived(); }
  Derived &&killWait(unsigned n) { Data::killWait = n; return derived(); }
  Derived &&coredump(bool b) { Data::coredump = b; return derived(); }
};

class Params : public Params_<Params> {
public:
  Params() = default;
  Params(unsigned size) : Params_<Params>{size} { }
  Params(ZuString name, unsigned size) : Params_<Params>{name, size} { }
};

class ZiAPI Blocker {
public:
  // block until woken or timeout while addr == val
  int wait(
      ZmAtomic<uint32_t> &addr, uint32_t val,
      const ParamData &params);
  // wake up waiters on addr
  void wake(ZmAtomic<uint32_t> &addr);

protected:
#ifdef _WIN32
  HANDLE		m_sem;
#endif
};

class ZiAPI CtrlMem {
public:
  CtrlMem() = default;
  CtrlMem(const CtrlMem &mem) : m_addr{mem.m_addr} { }

  bool open(unsigned size, const ParamData &params);
  void close(unsigned size, const ParamData &params);

  ZuInline const void *addr() const { return m_addr; }
  ZuInline void *addr() { return m_addr; }

protected:
  void		*m_addr = nullptr;
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
  using Ctrl = Ctrl_;

private:
  using Base = ZmRing_::CtrlMgr_<CtrlMem, Ctrl, true>;

protected:
  using Base::ctrl;

  CtrlMgr_() = default;
  CtrlMgr_(const CtrlMgr_ &ring) : Base{ring} { }

  // PIDs may be re-used by the OS, so processes are ID'd by PID + start time

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
  DataMem() = default;
  DataMem(const DataMem &mem) : m_addr(mem.m_addr) { }

  bool open(unsigned size, const ParamData &params);
  void close(unsigned size, const ParamData &params);

  ZuInline const void *addr() const { return m_addr; }
  ZuInline void *addr() { return m_addr; }

private:
  void		*m_addr = nullptr;
};

class ZiAPI MirrorMem {
public:
  static unsigned alignSize(unsigned size);

  bool open(unsigned size, const ParamData &params);
  void close(unsigned size, const ParamData &params);

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

class ZiAPI RdrMgr_ {
  static void getpinfo(uint32_t &pid, ZmTime &start);
  static bool alive(uint32_t pid, ZmTime start);
  static bool kill(uint32_t pid, bool coredump);
};

template <typename Ring, bool>
class RdrMgr : public RdrMgr_, public ZmRing_::RdrMgr<Ring, true> {
  Ring *ring() { return static_cast<Ring *>(this); }
  const Ring *ring() const { return static_cast<const Ring *>(this); }

public:
  // can be called by writer if ring is full to garbage collect
  // dead readers and any lingering messages intended exclusively for them;
  // returns space freed
  unsigned gc() { return gc_(params()); }

  // kills all stalled readers (following a timeout), sleeps, then runs gc()
  unsigned kill();

protected:
  unsigned gc_(const ParamData &);

  void attached(unsigned id);
  void detached(unsigned id);
};

template <typename Ring, bool MR>
inline unsigned RdrMgr<Ring, MR>::gc_(const ParamData &params)
{
  ZmAssert(ring()->ctrl());
  ZmAssert(ring()->m_flags & Write);

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
    tail += ring()->align(Ring::SizeAxor(&hdrPtr[1]));
    if ((tail & ~Wrapped32()) >= size) tail = (tail ^ Wrapped32()) - size;
    uint64_t mask = hdrPtr->xchAnd(~dead);
    if (mask && !(mask & ~dead)) {
      freed += size_;
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
inline unsigned RdrMgr<Ring, MR>::kill()
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
inline void RdrMgr<Ring, MR>::attached(unsigned id)
{
  getpinfo((ring()->rdrPID())[id], (ring()->rdrTime())[id]);
}

template <typename Ring, bool MR>
inline void RdrMgr<Ring, MR>::detached(unsigned id)
{
  (ring()->rdrPID())[id] = 0;
  (ring()->rdrTime())[id] = {};
}

} // ZiRing_

using ZiRingParams = ZiRing_::Params;

template <typename NTP = ZiRing_::Defaults>
using ZiRing = ZmRing_::Ring<
  ZmRingMR<true, NTP>,
  ZiRing_::ParamData,
  ZiRing_::Blocker,
  ZiRing_::CtrlMgr<CtrlMem>,
  ZmRing_::DataMgr<DataMem, MirrorMem, typename NTP::T, NTP::MW, true>,
  ZiRing_::RdrMgr>;

#endif /* ZiRing_HPP */
