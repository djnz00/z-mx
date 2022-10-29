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

// shared memory SPSC intra-process ring buffer

#include <zlib/ZuStringN.hpp>

#include <zlib/ZmVRing.hpp>

ZmVRing::ZmVRing(ZmVRingParams params) : m_params{ZuMv(params)}
{
}

ZmVRing::ZmVRing(const ZmVRing &ring) :
    ZmRingUtil{ring.m_params}, m_params{ring.m_params},
    m_flags{Shadow}, m_ctrl{ring.m_ctrl}, m_data{ring.m_data}
{
}

ZmVRing::~ZmVRing()
{
  close();
}

int ZmVRing::open(unsigned flags)
{
  using namespace ZmRingErrorCode;
  if (m_ctrl) return Error;
  if (!m_params.name()) return Error;
  m_flags = flags;
  if (!m_params.ll() && !ZmRingUtil::open()) return Error;
  if (!params().cpuset())
    m_ctrl = hwloc_alloc(ZmTopology::hwloc(), sizeof(Ctrl));
  else
    m_ctrl = hwloc_alloc_membind(
	ZmTopology::hwloc(), sizeof(Ctrl),
	params().cpuset(), HWLOC_MEMBIND_BIND, HWLOC_MEMBIND_MIGRATE);
  if (!m_ctrl) {
    if (!m_params.ll()) ZmRingUtil::close();
    return Error;
  }
  memset(m_ctrl, 0, sizeof(Ctrl));
  if (!m_data.open(m_params.size())) {
    closeCtrl();
    if (!m_params.ll()) ZmRingUtil::close();
    return Error;
  }
  if (!!m_params.cpuset())
    hwloc_set_area_membind(
	ZmTopology::hwloc(), m_data.addr(), (m_data.size())<<1,
	m_params.cpuset(), HWLOC_MEMBIND_BIND, HWLOC_MEMBIND_MIGRATE);
  /**/ZmVRing_bp(open1);
  return OK;
}

void ZmVRing::close()
{
  if (!m_ctrl) return;
  if (m_flags & Shadow) return;
  closeCtrl();
  m_data.close();
  if (!m_params.ll()) ZmRingUtil::close();
}

void ZmVRing::closeCtrl()
{
  hwloc_free(ZmTopology::hwloc(), m_ctrl, sizeof(Ctrl));
  m_ctrl = nullptr;
}

int ZmVRing::reset()
{
  using namespace ZmRingErrorCode;
  if (!m_ctrl) return Error;
  memset(m_ctrl, 0, sizeof(Ctrl));
  memset(m_data, 0, size());
  m_full = 0;
  return OK;
}

unsigned ZmVRing::length()
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

void *ZmVRing::push(uint32_t size, bool wait_)
{
  ZmAssert(m_ctrl);
  ZmAssert(m_flags & Write);

  size = align(size);

  ZmAssert(size < this->size());

retry:
  uint32_t head = this->head().load_();
  if (ZuUnlikely(head & EndOfFile)) return nullptr; // EOF
  uint32_t head_ = head & ~(Wrapped | Mask);
  uint32_t tail = this->tail(); // acquire
  uint32_t tail_ = tail & ~(Wrapped | Mask);
  if (ZuLikely(head_ != tail_)) {
    if (ZuUnlikely((head_ < tail_ ?
	(head_ + size >= tail_) :
	(head_ + size >= tail_ + this->size())))) {
      ++m_full;
      if (!wait_) return nullptr;
      if (ZuUnlikely(!m_params.ll()))
	if (ZmRingUtil::wait(Tail, this->tail(), tail) != OK) return nullptr;
      goto retry;
    }
  }

  // MPSC - need to handle multiple overlapping concurrent pushes,
  // and have the reader wait until the next message is stable

  head_ += size;
  if ((head_ & ~Wrapped) >= size()) head_ = (head_ ^ Wrapped) - size();

  if (ZuUnlikely(this->head().cmpXch(head_, head) != head))
    goto retry;

  return &(data())[head_];
}

void ZmVRing::push2(uint32_t size_)
{
  ZmAssert(m_ctrl);
  ZmAssert(m_flags & Write);

  uint32_t head = this->head().load_();
  uint8_t *ptr = &(data())[head & ~(Wrapped | Mask)];
  size_ = align(size_);
  head += size_;
  if ((head & ~(Wrapped | Mask)) >= size())
    head = (head ^ Wrapped) - size();

  if (ZuUnlikely(!m_params.ll())) {
    if (ZuUnlikely(this->head().xch(head & ~Waiting) & Waiting))
      ZmRingUtil::wake(Head, this->head(), 1);
  } else
    this->head() = head; // release

  this->inCount().store_(this->inCount().load_() + 1);
  this->inBytes().store_(this->inBytes().load_() + size_);
}

void ZmVRing::eof(bool b)
{
  ZmAssert(m_ctrl);
  ZmAssert(m_flags & Write);

  uint32_t head = this->head().load_();
  if (b)
    head |= EndOfFile;
  else
    head &= ~EndOfFile;

  if (ZuUnlikely(!m_params.ll())) {
    if (ZuUnlikely(this->head().xch(head & ~Waiting) & Waiting))
      ZmRingUtil::wake(Head, this->head(), 1);
  } else
    this->head() = head; // release
}

int ZmVRing::writeStatus()
{
  ZmAssert(m_flags & Write);

  if (ZuUnlikely(!m_ctrl)) return IOError;
  uint32_t head = this->head().load_();
  if (ZuUnlikely(head & EndOfFile)) return EndOfFile;
  head &= ~(Wrapped | Mask);
  uint32_t tail = this->tail() & ~(Wrapped | Mask);
  if (head < tail) return tail - head;
  return size() - (head - tail);
}

void *ZmVRing::shift()
{
  ZmAssert(m_ctrl);
  ZmAssert(m_flags & Read);

  uint32_t tail = m_tail;
  uint32_t head;
retry:
  head = this->head(); // acquire
  /**/ZmVRing_bp(shift1);
  if (tail == (head & ~Mask)) {
    if (ZuUnlikely(head & EndOfFile)) return nullptr;
    if (ZuUnlikely(!m_params.ll()))
      if (ZmRingUtil::wait(Head, this->head(), head) != OK) return nullptr;
    goto retry;
  }

  return &(data())[tail & ~Wrapped];
}

void ZmVRing::shift2(uint32_t size_)
{
  ZmAssert(m_ctrl);
  ZmAssert(m_flags & Read);

  uint32_t tail = m_tail;
  uint8_t *ptr = &(data())[tail & ~Wrapped];
  size_ = align(size__);
  tail += size_;
  if ((tail & ~Wrapped) >= size()) tail = (tail ^ Wrapped) - size();
  m_tail = tail;
  if (ZuUnlikely(!m_params.ll())) {
    if (ZuUnlikely(this->tail().xch(tail) & Waiting))
      ZmRingUtil::wake(Tail, this->tail(), 1);
  } else
    this->tail() = tail; // release

  this->outCount().store_(this->outCount().load_() + 1);
  this->outBytes().store_(this->outBytes().load_() + size_);
}

// can be called by readers after push returns 0; returns
// EndOfFile (< 0), or amount of data remaining in ring buffer (>= 0)
int ZmVRing::readStatus()
{
  ZmAssert(m_flags & Read);

  using namespace ZmRingErrorCode;
  if (ZuUnlikely(!m_ctrl)) return IOError;
  uint32_t head = this->head();
  uint32_t tail = m_tail;
  bool eof = head & EndOfFile;
  head &= ~Mask;
  if (ZuUnlikely(eof && tail == head)) return EndOfFile;
  if ((head ^ tail) == Wrapped) return size();
  head &= ~Wrapped;
  tail &= ~Wrapped;
  if (head >= tail) return head - tail;
  return size() - (tail - head);
}

void ZmVRing::stats(
    uint64_t &inCount, uint64_t &inBytes, 
    uint64_t &outCount, uint64_t &outBytes) const
{
  ZmAssert(m_ctrl);

  inCount = this->inCount().load_();
  inBytes = this->inBytes().load_();
  outCount = this->outCount().load_();
  outBytes = this->outBytes().load_();
}
