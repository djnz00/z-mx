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

#include <zlib/ZuStringN.hpp>

#include <zlib/ZiVRing.hpp>

ZiVRing::ZiVRing(ZiVRingParams params) : m_params{ZuMv(params)}
{
}

ZiVRing::~ZiVRing()
{
  close();
}

int ZiVRing::open(unsigned flags, ZeError *e)
{
  if (m_ctrl.addr()) goto einval;
  if (!m_params.name()) goto einval;
  m_flags = flags;
  if (!m_params.ll() && open_(e) != Zi::OK) return Zi::IOError;
  {
    unsigned mmapFlags = ZiFile::Shm;
    if (flags & Create) mmapFlags |= ZiFile::Create;
    int r;
    if ((r = m_ctrl.mmap(m_params.name() + ".ctrl",
	    mmapFlags, sizeof(Ctrl), true, 0, 0666, e)) != Zi::OK)
      return r;
    if (m_params.size()) {
      uint32_t reqSize = (uint32_t)m_params.size() | (uint32_t)m_params.ll();
      // check that requested sizes and latency are consistent
      if (uint32_t openSize = this->openSize().cmpXch(reqSize, 0))
	if (openSize != reqSize) {
	  m_ctrl.close();
	  goto einval;
	}
    } else {
      uint32_t openSize = this->openSize();
      if (!(openSize & ~1)) {
	m_ctrl.close();
	goto einval;
      }
      m_params.size(openSize & ~1);
      m_params.ll(openSize & 1);
    }
    if (flags & Write) {
      uint32_t pid;
      ZmTime start;
      getpinfo(pid, start);
      uint32_t oldPID = writerPID().load_();
      if (alive(oldPID, writerTime()) ||
	  writerPID().cmpXch(pid, oldPID) != oldPID) {
	m_ctrl.close();
	if (!m_params.ll()) close_();
	if (e) *e = ZiEADDRINUSE;
	return Zi::IOError;
      }
      writerTime() = start;
    }
    mmapFlags |= ZiFile::ShmDbl;
    if ((r = m_data.mmap(m_params.name() + ".data",
	    mmapFlags, m_params.size(), true, 0, 0666, e)) != Zi::OK) {
      m_ctrl.close();
      if (!m_params.ll()) close_();
      return r;
    }
    if (!!m_params.cpuset())
      hwloc_set_area_membind(
	  ZmTopology::hwloc(), m_data.addr(), (m_data.mmapLength())<<1,
	  m_params.cpuset(), HWLOC_MEMBIND_BIND, HWLOC_MEMBIND_MIGRATE);
    /**/ZiVRing_bp(open1);
    if (flags & Write) {
      this->head() = this->head().load_() & ~EndOfFile;
    }
    if (flags & Read) {
      if (!incRdrCount()) {
	m_ctrl.close();
	if (!m_params.ll()) close_();
	if (e) *e = ZiEADDRINUSE;
	return Zi::IOError;
      }
      m_tail = this->tail().load_() & ~Mask;
    }
  }
  return Zi::OK;

einval:
  if (e) *e = ZiEINVAL;
  return Zi::IOError;
}

int ZiVRing::shadow(const ZiVRing &ring, ZeError *e)
{
  if (m_ctrl.addr() || !ring.m_ctrl.addr()) {
    if (e) *e = ZiEINVAL;
    return Zi::IOError;
  }
  m_params = ring.m_params;
  m_flags = Read | Shadow;
  if (!m_params.ll() && open_(e) != Zi::OK) return Zi::IOError;
  if (m_ctrl.shadow(ring.m_ctrl, e) != Zi::OK) {
    if (!m_params.ll()) close_();
    return Zi::IOError;
  }
  if (m_data.shadow(ring.m_data, e) != Zi::OK) {
    m_ctrl.close();
    if (!m_params.ll()) close_();
    return Zi::IOError;
  }
  if (!incRdrCount()) {
    m_ctrl.close();
    m_data.close();
    if (!m_params.ll()) close_();
    if (e) *e = ZiEADDRINUSE;
    return Zi::IOError;
  }
  m_tail = this->tail().load_() & ~Mask;
  return Zi::OK;
}

bool ZiVRing::incRdrCount()
{
  uint32_t rdrCount;
  do {
    rdrCount = this->rdrCount();
    if (rdrCount >= 1) return false;
  } while (this->rdrCount().cmpXch(rdrCount + 1, rdrCount) != rdrCount);
  return true;
}

void ZiVRing::close()
{
  if (!m_ctrl.addr()) return;
  if (m_flags & Read) {
    --rdrCount();
  }
  if (m_flags & Write) {
    writerTime() = ZmTime(); // writerPID store is a release
    writerPID() = 0;
  }
  m_ctrl.close();
  m_data.close();
  if (!m_params.ll()) close_();
}

int ZiVRing::reset()
{
  if (!m_ctrl.addr()) return Zi::IOError;
  if (ZuUnlikely(this->rdrMask())) return Zi::NotReady;
  memset(m_ctrl.addr(), 0, sizeof(Ctrl));
  m_full = 0;
  return Zi::OK;
}

unsigned ZiVRing::length()
{
  uint32_t head = this->head().load_() & ~Mask;
  uint32_t tail = this->tail().load_() & ~Mask;
  if (head == tail) return 0;
  if ((head ^ tail) == Wrapped) return size();
  head &= ~Wrapped;
  tail &= ~Wrapped;
  if (head > tail) return head - tail;
  return size() - (tail - head);
}

void *ZiVRing::push(uint32_t size, bool wait_)
{
  ZmAssert(m_ctrl.addr());
  ZmAssert(m_flags & Write);

  size = align(size);

  ZmAssert(size < this->size());

retry:
  if (!this->rdrCount().load_()) return nullptr;

  uint32_t head = this->head().load_();
  if (ZuUnlikely(head & EndOfFile)) return nullptr; // EOF
  uint32_t tail = this->tail(); // acquire
  uint32_t head_ = head & ~(Wrapped | Mask);
  uint32_t tail_ = tail & ~(Wrapped | Mask);
  if (ZuLikely(head_ != tail_)) {
    if (ZuUnlikely((head_ < tail_ ?
	(head_ + size >= tail_) :
	(head_ + size >= tail_ + this->size())))) {
      ++m_full;
      if (!wait_) return nullptr;
      if (ZuUnlikely(!m_params.ll()))
	if (ZiIPC_wait(Tail, this->tail(), tail) != Zi::OK) return nullptr;
      goto retry;
    }
  }

  return &(data())[head_];
}

void ZiVRing::push2(uint32_t size_)
{
  ZmAssert(m_ctrl.addr());
  ZmAssert(m_flags & Write);

  uint32_t head = this->head().load_();
  uint8_t *ptr = &(data())[head & ~(Wrapped | Mask)];
  size_ = align(size_);
  head += size_;
  if ((head & ~(Wrapped | Mask)) >= size())
    head = (head ^ Wrapped) - size();

  if (ZuUnlikely(!m_params.ll())) {
    if (ZuUnlikely(this->head().xch(head & ~Waiting) & Waiting))
      ZiIPC_wake(Head, this->head(), rdrCount().load_());
  } else
    this->head() = head; // release

  this->inCount().store_(this->inCount().load_() + 1);
  this->inBytes().store_(this->inBytes().load_() + size_);
}

void ZiVRing::eof(bool b)
{
  ZmAssert(m_ctrl.addr());
  ZmAssert(m_flags & Write);

  uint32_t head = this->head().load_();
  if (b)
    head |= EndOfFile;
  else
    head &= ~EndOfFile;

  if (ZuUnlikely(!m_params.ll())) {
    if (ZuUnlikely(this->head().xch(head & ~Waiting) & Waiting))
      ZiIPC_wake(Head, this->head(), rdrCount().load_());
  } else
    this->head() = head; // release
}

int ZiVRing::writeStatus()
{
  ZmAssert(m_flags & Write);

  if (ZuUnlikely(!m_ctrl.addr())) return Zi::IOError;
  if (ZuUnlikely(!rdrCount())) return Zi::NotReady;
  uint32_t head = this->head().load_();
  if (ZuUnlikely(head & EndOfFile)) return Zi::EndOfFile;
  head &= ~(Wrapped | Mask);
  uint32_t tail = this->tail() & ~(Wrapped | Mask);
  if (head < tail) return tail - head;
  return size() - (head - tail);
}

void *ZiVRing::shift()
{
  ZmAssert(m_ctrl.addr());
  ZmAssert(m_flags & Read);

  uint32_t tail = m_tail;
  uint32_t head;
retry:
  head = this->head(); // acquire
  /**/ZiVRing_bp(shift1);
  if (tail == (head & ~Mask)) {
    if (ZuUnlikely(head & EndOfFile)) return nullptr;
    if (ZuUnlikely(!m_params.ll()))
      if (ZiIPC_wait(Head, this->head(), head) != Zi::OK) return nullptr;
    goto retry;
  }

  return &(data())[tail & ~Wrapped];
}

void ZiVRing::shift2(uint32_t size_)
{
  ZmAssert(m_ctrl.addr());
  ZmAssert(m_flags & Read);

  uint32_t tail = m_tail;
  uint8_t *ptr = &(data())[tail & ~Wrapped];
  size_ = align(size__);
  tail += size_;
  if ((tail & ~Wrapped) >= size()) tail = (tail ^ Wrapped) - size();
  m_tail = tail;
  if (ZuUnlikely(!m_params.ll())) {
    if (ZuUnlikely(this->tail().xch(tail) & Waiting))
      ZiIPC_wake(Tail, this->tail(), 1);
  } else
    this->tail() = tail; // release

  this->outCount().store_(this->outCount().load_() + 1);
  this->outBytes().store_(this->outBytes().load_() + size_);
}

// can be called by readers after push returns 0; returns
// EndOfFile (< 0), or amount of data remaining in ring buffer (>= 0)
int ZiVRing::readStatus()
{
  ZmAssert(m_flags & Read);

  if (ZuUnlikely(!m_ctrl.addr())) return Zi::IOError;
  uint32_t head = this->head();
  uint32_t tail = m_tail;
  bool eof = head & EndOfFile;
  head &= ~Mask;
  if (ZuUnlikely(eof && tail == head)) return Zi::EndOfFile;
  if ((head ^ tail) == Wrapped) return size();
  head &= ~Wrapped;
  tail &= ~Wrapped;
  if (head >= tail) return head - tail;
  return size() - (tail - head);
}

void ZiVRing::stats(
    uint64_t &inCount, uint64_t &inBytes, 
    uint64_t &outCount, uint64_t &outBytes) const
{
  ZmAssert(m_ctrl);

  inCount = this->inCount().load_();
  inBytes = this->inBytes().load_();
  outCount = this->outCount().load_();
  outBytes = this->outBytes().load_();
}
