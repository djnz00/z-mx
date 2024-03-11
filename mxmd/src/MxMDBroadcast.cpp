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

// MxMD in-memory broadcast

#include <mxmd/MxMDBroadcast.hpp>

#include <mxmd/MxMDCore.hpp>

MxMDBroadcast::MxMDBroadcast()
{
}

MxMDBroadcast::~MxMDBroadcast()
{
  close__();
}

void MxMDBroadcast::init(MxMDCore *core)
{
  m_core = core;

  if (ZmRef<ZvCf> cf = core->cf()->getCf<false>("broadcast"))
    m_params.init(cf);
  else
    m_params.name("RMD").size(131072); // 131072 is ~100mics at 1Gbit/s
}

bool MxMDBroadcast::open()
{
  Guard guard(m_lock);
  return open_(guard);
}

ZmRef<MxMDBroadcast::Ring> MxMDBroadcast::shadow(ZeError *e)
{
  Guard guard(m_lock);
  if (!open_(guard)) return nullptr;
  ZmRef<Ring> ring = new Ring(m_params);
  if (ring->shadow(*m_ring, e) < 0) { close_(); return nullptr; }
  return ring;
}

void MxMDBroadcast::close(ZmRef<Ring> ring)
{
  Guard guard(m_lock);
  ring->close();
  close_();
}

void MxMDBroadcast::close()
{
  Guard guard(m_lock);
  close_();
}

bool MxMDBroadcast::open_(Guard &guard)
{
  if (m_openCount++) return true;
  m_ring = new Ring(m_params);
  ZeError e;
  if (m_ring->open(Ring::Create | Ring::Read | Ring::Write, &e) < 0) {
    m_openCount = 0;
    m_ring = 0;
    guard.unlock();
    m_core->raise(ZeMkEvent(Error,
      ([name = MxTxtString(m_params.name()), e](
	const ZeEventInfo &, auto &s) {
	  s << '"' << name
	    << "\": failed to open IPC shared memory ring buffer: " << e;
	})));
    return false;
  }
  heartbeat_();
  return true;
}

void MxMDBroadcast::close_()
{
  if (!m_openCount) return;
  if (!--m_openCount) close__();
}

void MxMDBroadcast::close__()
{
  if (ZuLikely(m_core)) {
    m_core->mx()->del(&m_hbTimer);
  }
  if (ZuLikely(m_ring)) {
    m_ring->close();
    m_ring = 0;
  }
}

void MxMDBroadcast::heartbeat()
{
  Guard guard(m_lock);
  heartbeat_();
}

void MxMDBroadcast::heartbeat_()
{
  using namespace MxMDStream;
  if (ZuUnlikely(!m_ring)) return;
  if (void *ptr = m_ring->push(sizeof(Hdr) + sizeof(HeartBeat))) {
    Hdr *hdr = new (ptr) Hdr{
	(uint64_t)m_seqNo++, (uint32_t)0,
	(uint16_t)sizeof(HeartBeat), (uint8_t)HeartBeat::Code};
    new (hdr->body()) HeartBeat{MxDateTime{m_lastTime}};
    m_ring->push2();
  }
  m_lastTime.now();
  m_core->mx()->add(ZmFn<>::Member<&MxMDBroadcast::heartbeat>::fn(this),
      m_lastTime + ZmTime{(time_t)1, 0}, &m_hbTimer);
}

void MxMDBroadcast::eof()
{
  Guard guard(m_lock);
  if (ZuLikely(m_ring)) m_ring->eof();
}

void *MxMDBroadcast::push(unsigned size)
{
  m_lock.lock();
  if (ZuUnlikely(!m_ring)) { m_lock.unlock(); return 0; }
  if (void *ptr = m_ring->push(size)) return ptr;
  int i = m_ring->writeStatus();
  m_lock.unlock();
  if (ZuLikely(i == Zi::NotReady || i == Zi::EndOfFile)) return 0;
  m_core->raise(ZeMkEvent(Error,
    ([name = MxTxtString(m_params.name())](
	const ZeEventInfo &, auto &s) {
      s << '"' << name
	<< "\": IPC shared memory ring buffer overflow";
    })));
  return 0;
}

void *MxMDBroadcast::out(
    void *ptr, unsigned length, unsigned type, int shardID)
{
  ZmTime delta = ZmTimeNow() - m_lastTime;
  uint32_t nsec = delta.sec() * 1000000000 + delta.nsec();
  Hdr *hdr = new (ptr) Hdr{
      (uint64_t)m_seqNo++, nsec,
      (uint16_t)length, (uint8_t)type, (uint8_t)shardID};
  return hdr->body();
}

void MxMDBroadcast::push2()
{
  m_ring->push2();
  m_lock.unlock();
}
