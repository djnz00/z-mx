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

// shared memory SPSC ring buffer IPC

// supports variable length messages
// messages are C/C++ POD types

// two modes are supported: normal and low-latency
// normal uses futexes (Linux) or semaphores (Windows) to wait/wake-up
// low-latency uses 100% CPU - readers spin, no yielding or system calls

// performance varies with message size; for typical sizes (100-10000 bytes):
// normal	- expect .1 -.5  mics latency,	1- 5M msgs/sec
// low-latency	- expect .01-.05 mics latency, 10-50M msgs/sec

// ring buffer size heuristics
// normal	- use  100x average message size
// low-latency	- use 1000x average message size

#ifndef ZiVRing_HPP
#define ZiVRing_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZiLib_HPP
#include <zlib/ZiLib.hpp>
#endif

#include <zlib/ZmPlatform.hpp>
#include <zlib/ZmBitmap.hpp>
#include <zlib/ZmTopology.hpp>
#include <zlib/ZmAtomic.hpp>

#include <zlib/ZePlatform.hpp>

#include <zlib/ZiPlatform.hpp>
#include <zlib/ZiFile.hpp>
#include <zlib/ZiRingUtil.hpp>

#ifdef ZiVRing_FUNCTEST
#define ZiVRing ZiVRingTest
#define ZiVRingAPI
#else
#define ZiVRingAPI ZiAPI
#endif

#ifdef ZiVRing_FUNCTEST
#define ZiVRing_bp(x) (bp_##x.reached(#x))
#else
#ifdef ZiVRing_STRESSTEST
#define ZiVRing_bp(x) Zm::yield()
#else
#define ZiVRing_bp(x) (void{})
#endif
#endif

namespace ZiVRing_ {

template <typename> class Params;
class ParamData {
public:
  unsigned size() const { return m_size; }
  bool ll() const { return m_ll; }
  const ZmBitmap &cpuset() const { return m_cpuset; }
  unsigned killWait() const { return m_killWait; }
  bool coredump() const { return m_coredump; }

private:
  unsigned	m_size = 0;
  bool		m_ll = false;
  ZmBitmap	m_cpuset;
  unsigned	m_killWait = 1;
  bool		m_coredump = false;
};
template <typename Derived> class Params :
    public ZiRingUtil_::Params<Derived>, public ParamData {
  ZuInline Derived &&derived() { return ZuMv(*static_cast<Derived *>(this)); }
public:
  Derived &&size(unsigned n) { m_size = n; return derived(); }
  Derived &&ll(bool b) { m_ll = b; return derived(); }
  Derived &&cpuset(ZmBitmap b) {
    m_cpuset = ZuMv(b);
    return derived();
  }
  Derived &&killWait(unsigned n) { m_killWait = n; return derived(); }
  Derived &&coredump(bool b) { m_coredump = b; return derived(); }
};

} // ZiVRing_

struct ZiVRingParams : public ZiVRing_::Params<ZiVRingParams> { };

class ZiVRingAPI ZiVRing : public ZiRingUtil {
  ZiVRing(const ZiVRing &) = delete;
  ZiVRing &operator =(const ZiVRing &) = delete;
  ZiVRing(ZiVRing &&) = delete;
  ZiVRing &operator =(ZiVRing &&) = delete;

  using ParamData = ZiVRing_::ParamData;
  template <typename Derived> using Params = ZiVRing_::Params<Derived>;

  //               | SPSC | MPSC | SPMC | MPMC |
  //               |------+------+------+------|
  // Fixed-size    |      | H    |      | H    |
  // Variable-size | M    | HSM  | M    | HSM  |

  // H - message has a 64bit header for MP
  // S - size is stored in the header (implies H)
  // M - mirrored buffer

  // Zm - intra-process
  // Zi = inter-process
 
  // consolidate into
  // ZmRingUtil - flags (used for head, tail and header)
  // ZmRing<typename T, bool MP, bool MC>
  // ZiRing<typename T, bool MP, bool MC>
  //
  // T == ZuAny (sentinel type) implies variable-sized messages
  // otherwise fixed-size sizeof(T)
  //
  // !MP implies SP
  // !MC implies SC

  enum { // head+tail flags
    EndOfFile	= 0x20000000,
    Waiting	= 0x40000000,
    Wrapped	= 0x80000000,
    Mask	= Waiting | EndOfFile,	// does NOT include Wrapped
  };

  enum { Head = 0, Tail };

  enum { CacheLineSize = Zm::CacheLineSize };

public:
  template <typename Derived>
  ZiVRing(Params<Derived> params) :
      ZiRingUtil{ZuMv(params)},
      m_params{ZuMv(params)} { } // yes, moved twice
  ~ZiVRing();

  const ParamData &params() const { return m_params; }

  enum { // open() flags
    Create	= 0x00000001,
    Read	= 0x00000002,
    Write	= 0x00000004,
    Shadow	= 0x00000008
  };

#ifndef ZiVRing_FUNCTEST
private:
#endif
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

    ZmAtomic<uint32_t>		openSize; // opened size && latency
    ZmAtomic<uint32_t>		rdrCount; // reader count

    ZmAtomic<uint32_t>		writerPID;
    ZmTime			writerTime;
  };

  ZuInline const Ctrl *ctrl() const {
    return reinterpret_cast<const Ctrl *>(m_ctrl.addr());
  }
  ZuInline Ctrl *ctrl() {
    return reinterpret_cast<Ctrl *>(m_ctrl.addr());
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
 
  ZuInline ZmAtomic<uint32_t> &openSize() { return ctrl()->openSize; }
  ZuInline ZmAtomic<uint32_t> &rdrCount() { return ctrl()->rdrCount; }

  // PIDs may be re-used by the OS, so processes are ID'd by PID + start time

  ZuInline ZmAtomic<uint32_t> &writerPID() { return ctrl()->writerPID; }
  ZuInline ZmTime &writerTime() { return ctrl()->writerTime; }
 
public:
  ZuInline bool operator !() const { return !m_ctrl.addr(); }
  ZuOpBool;

  ZuInline uint8_t *data() const {
    return static_cast<uint8_t *>(m_data.addr());
  }

  ZuInline unsigned full() const { return m_full; }

  int open(unsigned flags, ZeError *e = nullptr);
  int shadow(const ZiVRing &ring, ZeError *e = nullptr);

private:
  bool incRdrCount();

public:
  void close();
  int reset();

  ZuInline unsigned ctrlSize() const { return m_ctrl.mmapLength(); }
  ZuInline unsigned size() const { return m_data.mmapLength(); }

  unsigned length();

  ZuInline unsigned align(unsigned size) {
    return (size + CacheLineSize - 1) & ~(CacheLineSize - 1);
  }

  // writer

  void *push(uint32_t size, bool wait_ = true);
  ZuInline void *tryPush(uint32_t size) { return push(size, false); }
  void push2(uint32_t size);

  void eof(bool b = true);

  // can be called by writers after push returns 0; returns
  // NotReady (no readers), EndOfFile,
  // or amount of space remaining in ring buffer (>= 0)
  int writeStatus();

  // reader

  void *shift();
  void shift2(uint32_t size);

  // can be called by readers after push returns 0; returns
  // EndOfFile (< 0), or amount of data remaining in ring buffer (>= 0)
  int readStatus();

  void stats(
      uint64_t &inCount, uint64_t &inBytes, 
      uint64_t &outCount, uint64_t &outBytes) const;

#ifndef ZiVRing_FUNCTEST
private:
#endif
  ParamData		m_params;

  uint32_t		m_flags = 0;
  ZiFile		m_ctrl;
  ZiFile		m_data;
  uint32_t		m_tail = 0;	// reader only
  uint32_t		m_full = 0;

#ifdef ZiVRing_FUNCTEST
  ZiVRing_Breakpoint	bp_open1;
  ZiVRing_Breakpoint	bp_shift1;
#endif
};

#endif /* ZiVRing_HPP */
