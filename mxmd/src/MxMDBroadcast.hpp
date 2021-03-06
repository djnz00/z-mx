//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2

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

// MxMD in-memory broadcast (ZiRing wrapper)

#ifndef MxMDBroadcast_HPP
#define MxMDBroadcast_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxMDLib_HPP
#include <mxmd/MxMDLib.hpp>
#endif

#include <zlib/ZmPLock.hpp>
#include <zlib/ZmRef.hpp>

#include <zlib/ZiRing.hpp>

#include <zlib/ZvRingCf.hpp>

#include <mxmd/MxMDTypes.hpp>
#include <mxmd/MxMDStream.hpp>

class MxMDCore;

template <> struct ZiRingTraits<MxMDStream::Hdr> {
  inline static unsigned size(const MxMDStream::Hdr &hdr) {
    return sizeof(MxMDStream::Hdr) + hdr.len;
  }
};

class MxMDAPI MxMDBroadcast {
public:
  typedef MxMDStream::Hdr Hdr;

  typedef ZiRing<Hdr, ZiRingBase<ZmObject> > Ring;

  MxMDBroadcast();
  ~MxMDBroadcast();

  void init(MxMDCore *core);

  inline const ZiRingParams &params() const { return m_params; }

  bool open(); // returns true if successful, false otherwise
  void close();

  ZmRef<Ring> shadow(ZeError *e = 0);
  void close(ZmRef<Ring> ring);

  inline bool active() { return m_openCount; }

  inline ZmRef<Ring> ring() { Guard guard(m_lock); return m_ring; }

  // caller must ensure ring is open during Rx/Tx

  // Rx

  inline int attach() { return m_ring->attach(); }
  inline int detach() { return m_ring->detach(); }

  inline int id() { return m_ring->id(); }

  inline const Hdr *shift() { return m_ring->shift(); }
  inline void shift2() { m_ring->shift2(); }

  inline int readStatus() { return m_ring->readStatus(); }

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

#endif /* MxMDBroadcast_HPP */
