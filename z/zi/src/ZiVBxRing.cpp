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

// shared memory ring buffer IPC with fan-out to multiple readers (up to 64)

#include <zlib/ZuStringN.hpp>

#include <zlib/ZiVBxRing.hpp>

ZiVBxRing::~ZiVBxRing()
{
  close();
}

int ZiVBxRing::open(unsigned flags, ZeError *e)
{
  if (m_ctrl.addr()) goto einval;
  if (!m_params.name()) goto einval;
  m_flags = flags;
  if (!m_params.ll() && ZiRingUtil::open(e) != Zi::OK) return Zi::IOError;
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
	if (!m_params.ll()) ZiRingUtil::close();
	if (e) *e = ZiEADDRINUSE;
	return Zi::IOError;
      }
      writerTime() = start;
    }
    mmapFlags |= ZiFile::ShmDbl;
    if ((r = m_data.mmap(m_params.name() + ".data",
	    mmapFlags, m_params.size(), true, 0, 0666, e)) != Zi::OK) {
      m_ctrl.close();
      if (!m_params.ll()) ZiRingUtil::close();
      return r;
    }
    if (!!m_params.cpuset())
      hwloc_set_area_membind(
	  ZmTopology::hwloc(), m_data.addr(), (m_data.mmapLength())<<1,
	  m_params.cpuset(), HWLOC_MEMBIND_BIND, HWLOC_MEMBIND_MIGRATE);
    if (flags & Write) {
      gc();
      this->head() = this->head().load_() & ~EndOfFile;
    }
    if (flags & Read) {
      if (!incRdrCount()) {
	m_ctrl.close();
	if (!m_params.ll()) ZiRingUtil::close();
	if (e) *e = ZiEADDRINUSE;
	return Zi::IOError;
      }
    }
  }
  return Zi::OK;

einval:
  if (e) *e = ZiEINVAL;
  return Zi::IOError;
}

int ZiVBxRing::shadow(const ZiVBxRing &ring, ZeError *e)
{
  if (m_ctrl.addr() || !ring.m_ctrl.addr()) {
    if (e) *e = ZiEINVAL;
    return Zi::IOError;
  }
  m_params = ring.m_params;
  m_flags = Read | Shadow;
  if (!m_params.ll() && ZiRingUtil::open(e) != Zi::OK) return Zi::IOError;
  if (m_ctrl.shadow(ring.m_ctrl, e) != Zi::OK) {
    if (!m_params.ll()) ZiRingUtil::close();
    return Zi::IOError;
  }
  if (m_data.shadow(ring.m_data, e) != Zi::OK) {
    m_ctrl.close();
    if (!m_params.ll()) ZiRingUtil::close();
    return Zi::IOError;
  }
  if (!incRdrCount()) {
    m_ctrl.close();
    m_data.close();
    if (!m_params.ll()) ZiRingUtil::close();
    if (e) *e = ZiEADDRINUSE;
    return Zi::IOError;
  }
  m_id = -1;
  m_tail = 0;
  return Zi::OK;
}

bool ZiVBxRing::incRdrCount()
{
  uint32_t rdrCount;
  do {
    rdrCount = this->rdrCount();
    if (rdrCount >= 64) return false;
  } while (this->rdrCount().cmpXch(rdrCount + 1, rdrCount) != rdrCount);
  return true;
}

void ZiVBxRing::close()
{
  if (!m_ctrl.addr()) return;
  if (m_flags & Read) {
    if (m_id >= 0) detach();
    --rdrCount();
  }
  if (m_flags & Write) {
    writerTime() = ZmTime(); // writerPID store is a release
    writerPID() = 0;
  }
  m_ctrl.close();
  m_data.close();
  if (!m_params.ll()) ZiRingUtil::close();
}

int ZiVBxRing::reset()
{
  if (!m_ctrl.addr()) return Zi::IOError;
  if ((m_flags & Read) && m_id >= 0) detach();
  if (ZuUnlikely(this->rdrMask())) return Zi::NotReady;
  memset(m_ctrl.addr(), 0, sizeof(Ctrl));
  m_full = 0;
  return Zi::OK;
}

unsigned ZiVBxRing::length()
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

void *ZiVBxRing::push(unsigned size, bool wait_)
{
  ZmAssert(m_ctrl.addr());
  ZmAssert(m_flags & Write);

  size = align(size);

  ZmAssert(size < this->size());

retry:
  uint64_t rdrMask = this->rdrMask().load_();
  if (!rdrMask) return nullptr; // no readers

  uint32_t head = this->head().load_();
  if (ZuUnlikely(head & EndOfFile)) return nullptr; // EOF
  uint32_t tail = this->tail(); // acquire
  uint32_t head_ = head & ~(Wrapped | Mask);
  uint32_t tail_ = tail & ~(Wrapped | Mask);
  if (ZuLikely(head_ != tail_)) {
    if (ZuUnlikely((head_ < tail_ ?
	(head_ + size >= tail_) :
	(head_ + size >= tail_ + this->size())))) {
      int j = gc();
      if (ZuUnlikely(j < 0)) return nullptr;
      if (ZuUnlikely(j > 0)) goto retry;
      ++m_full;
      if (!wait_) return nullptr;
      if (ZuUnlikely(!m_params.ll()))
	if (ZiRingUtil::wait(Tail, this->tail(), tail) != Zi::OK) return nullptr;
      goto retry;
    }
  }

  uint8_t *ptr = &(data())[head_];
  *reinterpret_cast<uint64_t *>(ptr) = rdrMask;
  return &ptr[8];
}

void ZiVBxRing::push2()
{
  ZmAssert(m_ctrl.addr());
  ZmAssert(m_flags & Write);

  uint32_t head = this->head().load_();
  uint8_t *ptr = &(data())[head & ~(Wrapped | Mask)];
  uint32_t size_ = align(m_sizeFn(&ptr[8]));
  head += size_;
  if ((head & ~(Wrapped | Mask)) >= size())
    head = (head ^ Wrapped) - size();

  if (ZuUnlikely(!m_params.ll())) {
    if (ZuUnlikely(this->head().xch(head & ~Waiting) & Waiting))
      ZiRingUtil::wake(Head, this->head(), rdrCount().load_());
  } else
    this->head() = head; // release

  this->inCount().store_(this->inCount().load_() + 1);
  this->inBytes().store_(this->inBytes().load_() + size_);
}

void ZiVBxRing::eof(bool b)
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
      ZiRingUtil::wake(Head, this->head(), rdrCount().load_());
  } else
    this->head() = head; // release
}

int ZiVBxRing::gc()
{
  ZmAssert(m_ctrl.addr());
  ZmAssert(m_flags & Write);

  // GC dead readers

  unsigned freed = 0;
  uint64_t dead;
  unsigned rdrCount;

  // below loop is a probe - as long as any concurrent attach() or
  // detach() overlap with our discovery of dead readers, the results
  // are unreliable and the probe must be re-attempted - after N attempts
  // give up and return 0
  for (unsigned i = 0;; ) {
    uint64_t attSeqNo = this->attSeqNo().load_();
    dead = rdrMask(); // assume all dead
    rdrCount = 0;
    if (dead) {
      for (unsigned id = 0; id < 64; id++) {
	if (!(dead & (1ULL<<id))) continue;
	if (alive(rdrPID()[id], rdrTime()[id])) {
	  dead &= ~(1ULL<<id);
	  ++rdrCount;
	}
      }
    }
    if (attSeqNo == this->attSeqNo()) break;
    Zm::yield();
    if (++i == m_params.spin()) return 0;
  }

  uint32_t tail_ = this->tail(); // acquire
  uint32_t tail = tail_ & ~Mask;
  uint32_t head = this->head().load_() & ~Mask;
  while (tail != head) {
    uint8_t *ptr = &(data())[tail & ~Wrapped];
    uint32_t size_ = align(m_sizeFn(&ptr[8]));
    tail += size_;
    if (ZuUnlikely((tail & ~Wrapped) >= size()))
      tail = (tail ^ Wrapped) - size();
    uint64_t mask =
      (*reinterpret_cast<ZmAtomic<uint64_t> *>(ptr)).xchAnd(~dead);
    if (mask && !(mask & ~dead)) {
      freed += size_;
      if (ZuUnlikely(!m_params.ll())) {
	if (ZuUnlikely(
	      this->tail().xch(tail | (tail_ & (Mask & ~Waiting))) & Waiting))
	  ZiRingUtil::wake(Tail, this->tail(), 1);
      } else
	this->tail() = tail | (tail_ & Mask); // release
    }
  }

  for (unsigned id = 0; id < 64; id++)
    if (dead & (1ULL<<id))
      if (rdrPID()[id]) {
	this->rdrMask() &= ~(1ULL<<id);
	rdrPID()[id] = 0, rdrTime()[id] = ZmTime();
      }
  this->rdrCount() = rdrCount;
  if (!(attMask() &= ~dead)) return -1; // no readers left
  return freed;
}

int ZiVBxRing::kill()
{
  uint64_t targets;
  {
    uint32_t tail = this->tail() & ~Mask;
    if (tail == (this->head() & ~Mask)) return 0;
    uint8_t *ptr = &(data())[tail & ~Wrapped];
    targets = *reinterpret_cast<ZmAtomic<uint64_t> *>(ptr);
  }
  for (unsigned id = 0; id < 64; id++)
    if (targets & (1ULL<<id))
      kill(rdrPID()[id], m_params.coredump());
  Zm::sleep(ZmTime((time_t)m_params.killWait()));
  return gc();
}

int ZiVBxRing::writeStatus()
{
  ZmAssert(m_flags & Write);

  if (ZuUnlikely(!m_ctrl.addr())) return Zi::IOError;
  if (ZuUnlikely(!rdrMask())) return Zi::NotReady;
  uint32_t head = this->head().load_();
  if (ZuUnlikely(head & EndOfFile)) return Zi::EndOfFile;
  head &= ~(Wrapped | Mask);
  uint32_t tail = this->tail() & ~(Wrapped | Mask);
  if (head < tail) return tail - head;
  return size() - (head - tail);
}

int ZiVBxRing::attach()
{
  ZmAssert(m_ctrl.addr());
  ZmAssert(m_flags & Read);

  if (m_id >= 0) return Zi::IOError;

  // allocate an ID for this reader
  {
    uint64_t attMask;
    unsigned id;
    do {
      attMask = this->attMask().load_();
      for (id = 0; id < 64; id++)
	if (!(attMask & (1ULL<<id))) break;
      if (id == 64) return Zi::IOError;
    } while (
	this->attMask().cmpXch(attMask | (1ULL<<id), attMask) != attMask);

    m_id = id;
  }

  ++(this->attSeqNo());

  getpinfo(rdrPID()[m_id], rdrTime()[m_id]);

  /**/ZiVBxRing_bp(attach1);

  // skip any trailing messages not intended for us, since other readers
  // may be concurrently advancing the ring's tail; this must be
  // re-attempted as long as the head keeps moving and the writer remains
  // unaware of our attach
  uint32_t tail = this->tail().load_() & ~Mask;
  uint32_t head = this->head() & ~Mask, head_; // acquire
  /**/ZiVBxRing_bp(attach2);
  rdrMask() |= (1ULL<<m_id); // notifies the writer about an attach
  /**/ZiVBxRing_bp(attach3);
  do {
    while (tail != head) {
      uint8_t *ptr = &(data())[tail & ~Wrapped];
      if (*reinterpret_cast<uint64_t *>(ptr) & (1ULL<<m_id))
	goto done; // writer aware
      tail += align(m_sizeFn(&ptr[8]));
      if ((tail & ~Wrapped) >= size()) tail = (tail ^ Wrapped) - size();
    }
    head_ = head;
    /**/ZiVBxRing_bp(attach4);
    head = this->head() & ~Mask; // acquire
  } while (head != head_);

done:
  m_tail = tail;

  ++(this->attSeqNo());

  return Zi::OK;
}

int ZiVBxRing::detach()
{
  ZmAssert(m_ctrl.addr());
  ZmAssert(m_flags & Read);

  if (m_id < 0) return Zi::IOError;

  ++(this->attSeqNo());

  rdrMask() &= ~(1ULL<<m_id); // notifies the writer about a detach
  /**/ZiVBxRing_bp(detach1);

  // drain any trailing messages that are waiting to be read by us,
  // advancing ring's tail as needed; this must be
  // re-attempted as long as the head keeps moving and the writer remains
  // unaware of our detach
  uint32_t tail = m_tail;
  /**/ZiVBxRing_bp(detach2);
  uint32_t head = this->head() & ~Mask, head_; // acquire
  do {
    while (tail != head) {
      uint8_t *ptr = &(data())[tail & ~Wrapped];
      if (!(*reinterpret_cast<uint64_t *>(ptr) & (1ULL<<m_id)))
	goto done; // writer aware
      tail += align(m_sizeFn(&ptr[8]));
      if ((tail & ~Wrapped) >= size()) tail = (tail ^ Wrapped) - size();
      if (*reinterpret_cast<ZmAtomic<uint64_t> *>(ptr) &= ~(1ULL<<m_id))
	continue;
      /**/ZiVBxRing_bp(detach3);
      if (ZuUnlikely(!m_params.ll())) {
	if (ZuUnlikely(this->tail().xch(tail) & Waiting))
	  ZiRingUtil::wake(Tail, this->tail(), 1);
      } else
	this->tail() = tail; // release
    }
    head_ = head;
    /**/ZiVBxRing_bp(detach4);
    head = this->head() & ~Mask; // acquire
  } while (head != head_);
done:
  m_tail = tail;

  // release ID for re-use by future attach
  /**/ZiVBxRing_bp(detach5);
  rdrPID()[m_id] = 0, rdrTime()[m_id] = ZmTime();

  ++(this->attSeqNo());

  attMask() &= ~(1ULL<<m_id);
  m_id = -1;

  return Zi::OK;
}

void *ZiVBxRing::shift()
{
  ZmAssert(m_ctrl.addr());
  ZmAssert(m_flags & Read);
  ZmAssert(m_id >= 0);

  uint32_t tail = m_tail;
  uint32_t head;
retry:
  head = this->head(); // acquire
  /**/ZiVBxRing_bp(shift1);
  if (tail == (head & ~Mask)) {
    if (ZuUnlikely(head & EndOfFile)) return nullptr;
    if (ZuUnlikely(!m_params.ll()))
      if (ZiRingUtil::wait(Head, this->head(), head) != Zi::OK) return nullptr;
    goto retry;
  }

  uint8_t *ptr = &(data())[tail & ~Wrapped];
  return &ptr[8];
}

void ZiVBxRing::shift2()
{
  ZmAssert(m_ctrl.addr());
  ZmAssert(m_flags & Read);
  ZmAssert(m_id >= 0);

  uint32_t tail = m_tail;
  uint8_t *ptr = &(data())[tail & ~Wrapped];
  uint32_t size_ = align(m_sizeFn(&ptr[8]));
  tail += size_;
  if ((tail & ~Wrapped) >= size()) tail = (tail ^ Wrapped) - size();
  m_tail = tail;
  if (*reinterpret_cast<ZmAtomic<uint64_t> *>(ptr) &= ~(1ULL<<m_id)) return;
  if (ZuUnlikely(!m_params.ll())) {
    if (ZuUnlikely(this->tail().xch(tail) & Waiting))
      ZiRingUtil::wake(Tail, this->tail(), 1);
  } else
    this->tail() = tail; // release

  this->outCount().store_(this->outCount().load_() + 1);
  this->outBytes().store_(this->outBytes().load_() + size_);
}

// can be called by readers after push returns 0; returns
// EndOfFile (< 0), or amount of data remaining in ring buffer (>= 0)
int ZiVBxRing::readStatus()
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

void ZiVBxRing::stats(
    uint64_t &inCount, uint64_t &inBytes, 
    uint64_t &outCount, uint64_t &outBytes) const
{
  ZmAssert(m_ctrl);

  inCount = this->inCount().load_();
  inBytes = this->inBytes().load_();
  outCount = this->outCount().load_();
  outBytes = this->outBytes().load_();
}
