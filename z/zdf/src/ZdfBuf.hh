//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Series - buffer

#ifndef ZdfBuf_HH
#define ZdfBuf_HH

#ifndef ZdfLib_HH
#include <zlib/ZdfLib.hh>
#endif

#include <zlib/ZuByteSwap.hh>

#include <zlib/ZmHeap.hh>
#include <zlib/ZmList.hh>
#include <zlib/ZmNoLock.hh>
#include <zlib/ZmHash.hh>

#include <zlib/ZtArray.hh>

namespace Zdf {

#pragma pack(push, 1)
struct Hdr {
  using UInt64 = ZuLittleEndian<uint64_t>;
  using Int64 = ZuLittleEndian<int64_t>;

  UInt64	offset_ = 0;
  UInt64	cle_ = 0;	// count/length/ndp
  Int64		last = 0;	// last value in buffer

private:
  static constexpr uint64_t countMask() { return 0xfffffffULL; }
  static constexpr uint64_t lengthMask() { return countMask(); }
  static constexpr unsigned lengthShift() { return 28U; }
public:
  static constexpr uint64_t lengthMax() { return countMask(); }
private:
  static constexpr uint64_t ndpMask() { return 0x1fULL; }
  static constexpr unsigned ndpShift() { return 56U; }

  uint64_t cle() const { return cle_; }

public:
  // offset (as a value count) of the first value in this buffer
  uint64_t offset() const { return offset_; }
  // count of values in this buffer
  unsigned count() const { return cle() & countMask(); }
  // length of this buffer in bytes
  unsigned length() const { return (cle()>>lengthShift()) & lengthMask(); }
  // ndp of values in this buffer
  unsigned ndp() const { return cle()>>ndpShift(); }

  void offset(uint64_t v) { offset_ = v; }
  void cle(uint64_t count, uint64_t length, uint64_t ndp) {
    cle_ = count | (length<<lengthShift()) | (ndp<<ndpShift());
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
    ZmListNode<BufLRUNode_,
      ZmListShadow<true>>>;
using BufLRUNode = BufLRU::Node;

// TCP over Ethernet maximum payload is 1460 (without Jumbo frames)
enum { BufSize = 1460 };

template <typename Heap>
class Buf_ : public Heap, public ZmPolymorph, public BufLRUNode {
  using PinLock = ZmPLock;
  using PinGuard = ZmGuard<PinLock>;
  using PinReadGuard = ZmReadGuard<PinLock>;

public:
  enum { Size = BufSize };

  using BufLRUNode::BufLRUNode;

  // cache pinning for asynchronous saves
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
  void sync(const Writer &writer, unsigned ndp, int64_t last) {
    auto hdr = this->hdr();
    auto start = data() + sizeof(Hdr);
    hdr->cle(writer.count(), writer.pos() - start, ndp);
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
inline constexpr const char *Buf_HeapID() { return "ZdfSeries.Buf"; }
using Buf = Buf_<ZmHeap<Buf_HeapID, sizeof(Buf_<ZuNull>)>>;

using BufUnloadFn = ZmFn<void(Buf *)>;

class ZdfAPI BufMgr {
public:
  void init(unsigned maxBufs);
  void final();

  unsigned alloc(BufUnloadFn unloadFn);	// registers series, allocates seriesID
  void free(unsigned seriesID);		// frees buffers, unloadFn is not called

  virtual void shift() {
    if (m_lru.count_() >= m_maxBufs) {
      auto lru_ = m_lru.shift();
      if (ZuLikely(lru_)) {
	Buf *lru = static_cast<Buf *>(lru_);
	lru->pinned([this, lru_ = ZuMv(lru_), lru](unsigned pinned) {
	  if (pinned) {
	    m_lru.pushNode(ZuMv(lru_));
	    m_maxBufs = m_lru.count_() + 1;
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
  ZtArray<BufUnloadFn>	m_unloadFn;	// callbacks for each series
  unsigned		m_maxBufs = 0;
};

} // namespace Zdf

#endif /* ZdfBuf_HH */
