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

// Data Series Buffer

#ifndef ZdfBuf_HPP
#define ZdfBuf_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdfLib_HPP
#include <zlib/ZdfLib.hpp>
#endif

#include <zlib/ZuByteSwap.hpp>

#include <zlib/ZmHeap.hpp>
#include <zlib/ZmList.hpp>
#include <zlib/ZmNoLock.hpp>
#include <zlib/ZmHash.hpp>

#include <zlib/ZtArray.hpp>

namespace Zdf {

#pragma pack(push, 1)
struct Hdr {
  using UInt64 = ZuLittleEndian<uint64_t>;
  using Int64 = ZuLittleEndian<int64_t>;

  UInt64	offset_ = 0;
  UInt64	cle_ = 0;	// count/length/exponent
  Int64		last = 0;	// last value in buffer

private:
  constexpr static uint64_t countMask() { return 0xfffffffULL; }
  constexpr static uint64_t lengthMask() { return countMask(); }
  constexpr static unsigned lengthShift() { return 28U; }
public:
  constexpr static uint64_t lengthMax() { return countMask(); }
private:
  constexpr static uint64_t expMask() { return 0x1fULL; }
  constexpr static unsigned expShift() { return 56U; }

  uint64_t cle() const { return cle_; }

public:
  // offset (as a value count) of the first value in this buffer
  uint64_t offset() const { return offset_; }
  // count of values in this buffer
  unsigned count() const { return cle() & countMask(); }
  // length of this buffer in bytes
  unsigned length() const { return (cle()>>lengthShift()) & lengthMask(); }
  // negative decimal exponent
  unsigned exponent() const { return cle()>>expShift(); }

  void offset(uint64_t v) { offset_ = v; }
  void cle(uint64_t count, uint64_t length, uint64_t exponent) {
    cle_ = count | (length<<lengthShift()) | (exponent<<expShift());
  }
};
#pragma pack(pop)

struct BufLRUNode_ {
  BufLRUNode_() = delete;
  BufLRUNode_(const BufLRUNode_ &) = delete;
  BufLRUNode_ &operator =(const BufLRUNode_ &) = delete;
  BufLRUNode_(BufLRUNode_ &&) = delete;
  BufLRUNode_ &operator =(BufLRUNode_ &&) = delete;
  ~BufLRUNode_() = default;

  BufLRUNode_(void *mgr_, unsigned seriesID_, unsigned blkIndex_) :
      mgr(mgr_), seriesID(seriesID_), blkIndex(blkIndex_) { }

  void		*mgr;
  unsigned	seriesID;
  unsigned	blkIndex;
};
struct BufLRU_HeapID {
  static constexpr const char *id() { return "ZdfSeries.BufLRU"; }
};
using BufLRU =
  ZmList<BufLRUNode_,
    ZmListObject<ZuShadow,
      ZmListNodeDerive<true,
	ZmListHeapID<ZuNull,
	  ZmListLock<ZmNoLock> > > > >;
using BufLRUNode = BufLRU::Node;

// TCP over Ethernet maximum payload is 1460 (without Jumbo frames)
enum { BufSize = 1460 };

template <typename Heap>
class Buf_ : public ZmPolymorph, public BufLRUNode, public Heap {
  using PinLock = ZmPLock;
  using PinGuard = ZmGuard<PinLock>;
  using PinReadGuard = ZmReadGuard<PinLock>;

public:
  enum { Size = BufSize };

  template <typename ...Args>
  Buf_(Args &&... args) : BufLRUNode{ZuFwd<Args>(args)...} { }

  void pin() {
    PinGuard guard(m_pinLock);
    ++m_pinned;
  }
  void unpin() {
    PinGuard guard(m_pinLock);
    if (ZuLikely(m_pinned)) --m_pinned;
  }
  template <typename L>
  void pinned(L l) const {
    PinReadGuard guard(m_pinLock);
    l(m_pinned);
  }

  // used for asynchronous saves
  template <typename L>
  void save(L l) {
    PinGuard guard(m_pinLock);
    if (!m_saves++) l();
  }
  template <typename L>
  void save_(L l) {
    PinGuard guard(m_pinLock);
    if (ZuLikely(m_saves)) {
      if (m_pinned > m_saves)
	m_pinned -= m_saves;
      else
	m_pinned = 0;
      m_saves = 0;
      l();
    }
  }

  uint8_t *data() { return &m_data[0]; }
  const uint8_t *data() const { return &m_data[0]; }

  const Hdr *hdr() const {
    return reinterpret_cast<const Hdr *>(data());
  }
  Hdr *hdr() { return reinterpret_cast<Hdr *>(data()); }

  template <typename Reader>
  Reader reader() {
    auto start = data() + sizeof(Hdr);
    return Reader{start, start + hdr()->length()};
  }

  template <typename Writer>
  Writer writer() {
    auto start = data() + sizeof(Hdr);
    auto end = data() + Size;
    return Writer{start, end};
  }
  template <typename Writer>
  void sync(const Writer &writer, unsigned exponent, int64_t last) {
    auto hdr = this->hdr();
    auto start = data() + sizeof(Hdr);
    hdr->cle(writer.count(), writer.pos() - start, exponent);
    hdr->last = last;
  }

  unsigned space() const {
    auto start = data() + sizeof(Hdr) + hdr()->length();
    auto end = data() + Size;
    if (start >= end) return 0;
    return end - start;
  }

private:
  PinLock		m_pinLock;
    unsigned		  m_pinned = 0;
    unsigned		  m_saves = 0;

  uint8_t		m_data[Size];
};
struct Buf_HeapID {
  static constexpr const char *id() { return "ZdfSeries.Buf"; }
};
using Buf = Buf_<ZmHeap<Buf_HeapID, sizeof(Buf_<ZuNull>)>>;

using BufUnloadFn = ZmFn<Buf *>;

class ZdfAPI BufMgr {
public:
  void init(unsigned maxBufs);
  void final();

  unsigned alloc(BufUnloadFn unloadFn);
  void free(unsigned seriesID);

  virtual void shift() {
    if (m_lru.count() >= m_maxBufs) {
      auto lru_ = m_lru.shiftNode();
      if (ZuLikely(lru_)) {
	Buf *lru = static_cast<Buf *>(lru_);
	lru->pinned([this, lru_ = ZuMv(lru_), lru](unsigned pinned) {
	  if (pinned) {
	    m_lru.pushNode(ZuMv(lru_));
	    m_maxBufs = m_lru.count() + 1;
	  } else
	    (m_unloadFn[lru->seriesID])(lru);
	});
      }
    }
  }
  virtual void push(BufLRUNode *node) { m_lru.pushNode(node); }
  virtual void use(BufLRUNode *node) { m_lru.pushNode(m_lru.delNode(node)); }
  virtual void del(BufLRUNode *node) { m_lru.delNode(node); }

  virtual void purge(unsigned seriesID, unsigned blkIndex); // caller unloads

private:
  BufLRU		m_lru;
  ZtArray<BufUnloadFn>	m_unloadFn;	// allocates seriesID
  unsigned		m_maxBufs = 0;
};

} // namespace Zdf

#endif /* ZdfBuf_HPP */
