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
#include <zlib/ZdfSchema.hh>

namespace Zdf {

class Series;

class BlkData : public ZdbObject<DB::BlkData> {
public:
  BlkData(Series *series) :
    ZdbObject<DB::BlkData>{series->db()->seriesTbl()},
    m_series{series} { }

  void evict();

private:
  Series	*m_series;
};

// all of US equities trades since 2003 is ~350B rows
// 47bits handles 140T rows for a single series, more than enough

using Last = union {
  int64_t	fixed;
  double	float_;
};

struct Blk {
  uint64_t		ocn = 0;	// offset/count/ndp
  Last			last = 0;	// last value in block
  ZmRef<BlkData>	blkData;	// cached data

  constexpr const uint64_t OffsetMask() { return (uint64_t(1)<<47) - 1; }
  constexpr const unsigned CountShift() { return 47; }
  constexpr const uint64_t CountMask() { return uint64_t(0xfff); }
  constexpr const unsigned NDPShift() { return 59; }
  constexpr const uint64_t NDPMask() { return uint64_t(0x1f); }

  void init(uint64_t offset, uint64_t count, uint64_t ndp, int64_t last_) {
    ocn = offset | (count<<CountShift()) | (ndp<<NDPShift());
    last.fixed = last_;
  }
  void init(uint64_t offset, uint64_t count, double last_) {
    ocn = offset | (count<<CountShift());
    last.float_ = last_;
  }

  ZuInline uint64_t offset() const { return ocnl & OffsetMask(); }
  ZuInline unsigned count() const { return (ocnl>>CountShift()) & CountMask(); }
  ZuInline unsigned ndp() const { return (ocnl>>NDPShift()) & NDPMask(); }

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
  Decoder decoder() {
    ZeAssert(blkData, (), "blkData not loaded", return Decoder{});
    const auto &buf = blkData->data().buf;
    auto start = buf.data();
    return Decoder{start, start + buf.length()};
  }

  template <typename Encoder>
  void sync(const Encoder &encoder, unsigned ndp, int64_t last_) {
    count_ndp(encoder.count(), ndp);
    last = last_;
    ZeAssert(blkData, (), "blkData not loaded", return);
    auto &buf = blkData->data().buf;
    buf.length(encoder.pos() - buf.data());
  }

  template <typename Encoder>
  Encoder encoder() {
    if (!blkData) blkData = new BlkData{this};
    blkData->pin();
    const auto &buf = blkData->data().buf;
    auto start = buf.data();
    return Encoder{start, start + BlkSize};
  }

  unsigned space() const {
    ZeAssert(blkData, (), "blkData not loaded", return 0);
    const auto &buf = blkData->data().buf;
    return BlkSize - buf.length();
  }
};

} // namespace Zdf

#endif /* ZdfBlk_HH */
