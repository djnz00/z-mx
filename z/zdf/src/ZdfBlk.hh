//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Series Block

#ifndef ZdfBlk_HH
#define ZdfBlk_HH

#ifndef ZdfLib_HH
#include <zlib/ZdfLib.hh>
#endif

#include <zlib/ZdfTypes.hh>
#include <zlib/ZdfCompress.hh>
#include <zlib/ZdfSchema.hh>

namespace Zdf {

template <typename> class Series;

class BlkData : public ZdbObject<DB::BlkData> {
public:
  using EvictFn = ZmFn<void(BlkData *)>;

  BlkData(EvictFn evictFn, ZdbTable<DB::BlkData> *tbl, Shard shard) :
    ZdbObject<DB::BlkData>{tbl, shard}, m_evictFn{ZuMv(evictFn)} { }

  void evict() {
    auto fn = ZuMv(m_evictFn);
    fn(this);
    ZdbAnyObject::evict();
  }

private:
  EvictFn	m_evictFn;
};

// all of US equities trades since 2003 is ~350B rows
// 47bits handles 140T rows for a single series, more than enough

union Last {
  int64_t	fixed;
  double	float_;
};

struct Blk {
  uint64_t		ocn = 0;		// offset/count/ndp
  Last			last{.fixed = 0};	// last value in block
  ZmRef<BlkData>	blkData;		// cached data

  static constexpr const uint64_t OffsetMask() { return (uint64_t(1)<<47) - 1; }
  static constexpr const unsigned CountShift() { return 47; }
  static constexpr const uint64_t CountMask() { return uint64_t(0xfff); }
  static constexpr const unsigned NDPShift() { return 59; }
  static constexpr const uint64_t NDPMask() { return uint64_t(0x1f); }

  void init(uint64_t offset, uint64_t count, uint64_t ndp, int64_t last_) {
    ocn = offset | (count<<CountShift()) | (ndp<<NDPShift());
    last.fixed = last_;
  }
  void init(uint64_t offset, uint64_t count, double last_) {
    ocn = offset | (count<<CountShift());
    last.float_ = last_;
  }

  ZuInline uint64_t offset() const { return ocn & OffsetMask(); }
  ZuInline BlkCount count() const { return (ocn>>CountShift()) & CountMask(); }
  ZuInline NDP ndp() const { return (ocn>>NDPShift()) & NDPMask(); }

  ZuInline bool operator !() const { return !count(); }

  ZuInline void offset(uint64_t v) {
    ocn = (ocn & ~OffsetMask()) | v;
  }
  ZuInline void count(uint64_t v) {
    ocn = (ocn & ~(CountMask()<<CountShift())) | (v<<CountShift());
  }
  ZuInline void ndp(uint64_t v) {
    ocn = (ocn & ~(NDPMask()<<NDPShift())) | (v<<NDPShift());
  }
  ZuInline void count_ndp(uint64_t count_, uint64_t ndp_) {
    ocn = (ocn & OffsetMask()) | (count_<<CountShift()) | (ndp_<<NDPShift());
  }

  template <typename Decoder>
  Decoder decoder() const {
    ZeAssert(blkData, (), "blkData not loaded", return {});
    const auto &buf = blkData->data().buf;
    auto start = buf.data();
    return Decoder{start, start + buf.length()};
  }

  template <typename Decoder>
  Encoder<Decoder> encoder(Series<Decoder> *series) {
    ZeAssert(blkData, (), "blkData not instantiated", return {});
    auto &buf = blkData->data().buf;
    auto start = buf.data();
    return {start, start + BlkSize};
  }

  template <typename Encoder>
  void sync(const Encoder &encoder, int64_t last_, NDP ndp) { // fixed
    count_ndp(encoder.offset(), ndp);
    last.fixed = last_;
    ZeAssert(blkData, (), "blkData not loaded", return);
    auto &buf = blkData->data().buf;
    buf.length(encoder.pos() - buf.data());
  }
  template <typename Encoder>
  void sync(const Encoder &encoder, double last_) { // floating
    count(encoder.offset());
    last.float_ = last_;
    ZeAssert(blkData, (), "blkData not loaded", return);
    auto &buf = blkData->data().buf;
    buf.length(encoder.pos() - buf.data());
  }

  unsigned space() const {
    ZeAssert(blkData, (), "blkData not loaded", return 0);
    const auto &buf = blkData->data().buf;
    return BlkSize - buf.length();
  }
};

} // namespace Zdf

#endif /* ZdfBlk_HH */
