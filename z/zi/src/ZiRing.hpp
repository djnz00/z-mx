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

// shared memory ring buffer IPC with broadcast fan-out to multiple readers
// (up to 64)

// Note: this is broadcast, not conventional SPMC - every consumer processes
// every message i.e. every message is processed as many times as there are
// consumers; with normal SPMC, each message is processed once by a single
// consumer selected from the pool of consumers, typically according to
// consumer availability/readiness/priority

// supports fixed- and variable length messages
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

#ifndef ZiRing_HPP
#define ZiRing_HPP

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

#ifdef ZiRing_FUNCTEST
#define ZiRing ZiRingTest
#define ZiRingAPI
#else
#define ZiRingAPI ZiAPI
#endif

#ifdef ZiRing_FUNCTEST
#define ZiRing_bp(x) (bp_##x.reached(#x))
#else
#ifdef ZiRing_STRESSTEST
#define ZiRing_bp(x) Zm::yield()
#else
#define ZiRing_bp(x) (void)0
#endif
#endif

// ring buffer parameters
class ZiRingParams {
public:
  using Path = Zi::Path;

  ZiRingParams() :
    m_size(0),
    m_ll(false), m_spin(1000),
    m_timeout(1), m_killWait(1), m_coredump(false) { }

  template <typename Name>
  ZiRingParams(const Name &name) :
    m_name(name), m_size(0),
    m_ll(false), m_spin(1000),
    m_timeout(1), m_killWait(1), m_coredump(false) { }
  template <typename Name>
  ZiRingParams(const Name &name, const ZiRingParams &p) :
    m_name(name), m_size(p.m_size),
    m_ll(p.m_ll), m_spin(p.m_spin),
    m_timeout(p.m_timeout),
    m_cpuset(p.m_cpuset),
    m_killWait(p.m_killWait),
    m_coredump(p.m_coredump) { }

  ZiRingParams(const ZiRingParams &) = default;
  ZiRingParams &operator =(const ZiRingParams &) = default;
  ZiRingParams(ZiRingParams &&) = default;
  ZiRingParams &operator =(ZiRingParams &&) = default;

  template <typename Name>
  ZiRingParams &name(const Name &s) { m_name = s; return *this; }
  ZiRingParams &size(unsigned n) { m_size = n; return *this; }
  ZiRingParams &ll(bool b) { m_ll = b; return *this; }
  ZiRingParams &spin(unsigned n) { m_spin = n; return *this; }
  ZiRingParams &timeout(unsigned n) { m_timeout = n; return *this; }
  ZiRingParams &cpuset(const ZmBitmap &b) { m_cpuset = b; return *this; }
  ZiRingParams &killWait(unsigned n) { m_killWait = n; return *this; }
  ZiRingParams &coredump(bool b) { m_coredump = b; return *this; }

  ZuInline const ZtString &name() const { return m_name; }
  ZuInline unsigned size() const { return m_size; }
  ZuInline bool ll() const { return m_ll; }
  ZuInline unsigned spin() const { return m_spin; }
  ZuInline unsigned timeout() const { return m_timeout; }
  ZuInline const ZmBitmap &cpuset() const { return m_cpuset; }
  ZuInline unsigned killWait() const { return m_killWait; }
  ZuInline bool coredump() const { return m_coredump; }

private:
  ZtString	m_name;
  unsigned	m_size;
  bool		m_ll;
  unsigned	m_spin;
  unsigned	m_timeout;
  ZmBitmap	m_cpuset;
  unsigned	m_killWait;
  bool		m_coredump;
};

typedef unsigned (*ZiRingSizeFn)(const void *);

class ZiRingAPI ZiRing {
  ZiRing(const ZiRing &) = delete;
  ZiRing &operator =(const ZiRing &) = delete;
  ZiRing(ZiRing &&) = delete;
  ZiRing &operator =(ZiRing &&) = delete;

  enum { // head+tail flags
    EndOfFile	= 0x20000000,
    Waiting	= 0x40000000,
    Wrapped	= 0x80000000,
    Mask	= EndOfFile | Waiting // does NOT include Wrapped
  };

  enum { Head = 0, Tail };

  int open_(ZeError *e = 0);
  int close_(ZeError *e = 0);

#ifdef linux
#define ZiRing_wait(index, addr, val) wait(addr, val)
#define ZiRing_wake(index, addr, n) wake(addr, n)
  // block until woken or timeout while addr == val
  int wait(ZmAtomic<uint32_t> &addr, uint32_t val);
  // wake up waiters on addr (up to n waiters are woken)
  int wake(ZmAtomic<uint32_t> &addr, int n);
#endif

#ifdef _WIN32
#define ZiRing_wait(index, addr, val) wait(index, addr, val)
#define ZiRing_wake(index, addr, n) wake(index, addr, n)
  // block until woken or timeout while addr == val
  int wait(unsigned index, ZmAtomic<uint32_t> &addr, uint32_t val);
  // wake up waiters on addr (up to n waiters are woken)
  int wake(unsigned index, ZmAtomic<uint32_t> &addr, int n);
#endif

  static void getpinfo(uint32_t &pid, ZmTime &start);
  static bool alive(uint32_t pid, ZmTime start);
  static bool kill(uint32_t pid, bool coredump);

  enum { CacheLineSize = Zm::CacheLineSize };

public:
  ZiRing(ZiRingSizeFn, const ZiRingParams &params);
  ~ZiRing();

  ZuInline const ZiRingParams &params() const { return m_params; }

  enum { // open() flags
    Create	= 0x00000001,
    Read	= 0x00000002,
    Write	= 0x00000004,
    Shadow	= 0x00000008
  };

#ifndef ZiRing_FUNCTEST
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
    ZmAtomic<uint64_t>		rdrMask;  // active readers
    ZmAtomic<uint64_t>		attMask;  // readers pending attach
    ZmAtomic<uint64_t>		attSeqNo; // attach/detach seqNo

    ZmAtomic<uint32_t>		writerPID;
    ZmTime			writerTime;
    uint32_t			rdrPID[64];
    ZmTime			rdrTime[64];
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
  ZuInline ZmAtomic<uint64_t> &rdrMask() { return ctrl()->rdrMask; }
  ZuInline ZmAtomic<uint64_t> &attMask() { return ctrl()->attMask; }
  ZuInline ZmAtomic<uint64_t> &attSeqNo() { return ctrl()->attSeqNo; }

  // PIDs may be re-used by the OS, so processes are ID'd by PID + start time

  ZuInline ZmAtomic<uint32_t> &writerPID() { return ctrl()->writerPID; }
  ZuInline ZmTime &writerTime() { return ctrl()->writerTime; }
  ZuInline uint32_t *rdrPID() { return ctrl()->rdrPID; }
  ZuInline ZmTime *rdrTime() { return ctrl()->rdrTime; }
 
public:
  ZuInline bool operator !() const { return !m_ctrl.addr(); }
  ZuOpBool;

  ZuInline uint8_t *data() const {
    return static_cast<uint8_t *>(m_data.addr());
  }

  ZuInline unsigned full() const { return m_full; }

  int open(unsigned flags, ZeError *e = nullptr);
  int shadow(const ZiRing &ring, ZeError *e = nullptr);

private:
  bool incRdrCount();

public:
  void close();
  int reset();

  ZuInline unsigned ctrlSize() const { return m_ctrl.mmapLength(); }
  ZuInline unsigned size() const { return m_data.mmapLength(); }

  unsigned length();

  ZuInline unsigned align(unsigned size) {
    return (size + 8 + CacheLineSize - 1) & ~(CacheLineSize - 1);
  }

  // writer

  void *push(unsigned size, bool wait_ = true);
  ZuInline void *tryPush(unsigned size) { return push(size, false); }
  void push2();

  void eof(bool b = true);

  // can be called by writer if ring is full to garbage collect
  // dead readers and any lingering messages intended exclusively for them;
  // returns space freed, or -1 if ring is empty and no readers
  int gc();

  // kills all stalled readers (following a timeout), sleeps, then runs gc()
  int kill();

  // can be called by writers after push returns 0; returns
  // NotReady (no readers), EndOfFile,
  // or amount of space remaining in ring buffer (>= 0)
  int writeStatus();

  // reader

  ZuInline int id() { return m_id; } // -ve if not attached

  int attach();
  int detach();

  void *shift();
  void shift2();

  // can be called by readers after push returns 0; returns
  // EndOfFile (< 0), or amount of data remaining in ring buffer (>= 0)
  int readStatus();

  void stats(
      uint64_t &inCount, uint64_t &inBytes, 
      uint64_t &outCount, uint64_t &outBytes) const;

#ifndef ZiRing_FUNCTEST
private:
#endif
  ZiRingSizeFn		m_sizeFn;
  ZiRingParams		m_params;
#ifdef _WIN32
  HANDLE		m_sem[2];
#endif

  uint32_t		m_flags;
  int			m_id;
  ZiFile		m_ctrl;
  ZiFile		m_data;
  uint32_t		m_tail;	// reader only
  uint32_t		m_full;

#ifdef ZiRing_FUNCTEST
  ZiRing_Breakpoint	bp_attach1;
  ZiRing_Breakpoint	bp_attach2;
  ZiRing_Breakpoint	bp_attach3;
  ZiRing_Breakpoint	bp_attach4;
  ZiRing_Breakpoint	bp_detach1;
  ZiRing_Breakpoint	bp_detach2;
  ZiRing_Breakpoint	bp_detach3;
  ZiRing_Breakpoint	bp_detach4;
  ZiRing_Breakpoint	bp_detach5;
  ZiRing_Breakpoint	bp_shift1;
#endif
};

#endif /* ZiRing_HPP */
