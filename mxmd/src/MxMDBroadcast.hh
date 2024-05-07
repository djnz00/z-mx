//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxMD in-memory broadcast (ZiVBxRing wrapper)

#ifndef MxMDBroadcast_HH
#define MxMDBroadcast_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxMDLib_HH
#include <mxmd/MxMDLib.hh>
#endif

#include <zlib/ZmPLock.hh>
#include <zlib/ZmRef.hh>

#include <zlib/ZiVBxRing.hh>

#include <zlib/ZvRingCf.hh>

#include <mxmd/MxMDTypes.hh>
#include <mxmd/MxMDStream.hh>

class MxMDCore;

class MxMDAPI MxMDBroadcast {
public:
  typedef MxMDStream::Hdr Hdr;

  struct Ring : public ZmObject, public ZiVBxRing {
    Ring(ZiVBxRingParams params) :
	ZiVBxRing{[](const void *ptr) -> unsigned {
	  using Hdr = MxMDStream::Hdr;
	  return sizeof(Hdr) + static_cast<const Hdr *>(ptr)->len;
	}, ZuMv(params)} { }
  };

  MxMDBroadcast();
  ~MxMDBroadcast();

  void init(MxMDCore *core);

  const ZiVBxRingParams &params() const { return m_params; }

  bool open(); // returns true if successful, false otherwise
  void close();

  ZmRef<Ring> shadow(ZeError *e = nullptr);
  void close(ZmRef<Ring> ring);

  bool active() { return m_openCount; }

  ZmRef<Ring> ring() { Guard guard(m_lock); return m_ring; }

  // caller must ensure ring is open during Rx/Tx

  // Rx

  int attach() { return m_ring->attach(); }
  int detach() { return m_ring->detach(); }

  int id() { return m_ring->id(); }

  const Hdr *shift() { return m_ring->shift(); }
  void shift2() { m_ring->shift2(); }

  int readStatus() { return m_ring->readStatus(); }

  // Tx

  void *push(unsigned size);
  void *out(void *ptr, unsigned length, unsigned type, int shardID);
  void push2();

  int writeStatus() {
    Guard guard(m_lock);
    if (ZuUnlikely(!m_ring)) return Zi::NotReady;
    return m_ring->writeStatus();
  }

private:
  typedef ZmPLock Lock;
  typedef ZmGuard<Lock> Guard;
  typedef ZmReadGuard<Lock> ReadGuard;

  bool open_(Guard &guard);
  void close_();
  void close__();

  void heartbeat();
  void heartbeat_();
  void eof();

private:
  MxMDCore		*m_core = 0;
  ZvRingParams		m_params;
  Lock			m_lock;
    MxSeqNo		  m_seqNo = 0;
    ZmTime		  m_lastTime;
    ZmScheduler::Timer	  m_hbTimer;
    unsigned		  m_openCount = 0;
    ZmRef<Ring>		  m_ring;
};

#endif /* MxMDBroadcast_HH */
