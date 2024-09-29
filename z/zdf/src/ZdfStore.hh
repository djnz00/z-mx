//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Frame backing data store

#ifndef ZdfStore_HH
#define ZdfStore_HH

#ifndef ZdfLib_HH
#include <zlib/ZdfLib.hh>
#endif

#include <zlib/ZtString.hh>

#include <zlib/Zdb.hh>

#include <zlib/ZdfTypes.hh>
#include <zlib/ZdfSchema.hh>
#include <zlib/ZdfCompress.hh>
#include <zlib/ZdfSeries.hh>
#include <zlib/Zdf.hh>

namespace Zdf {

// data store state
namespace StoreState {
  ZtEnumValues(StoreState, int,
    Uninitialized, Initialized, Opening, Opened, OpenFailed);
}

using OpenFn = ZmFn<void(bool)>;	// (bool ok)

class ZdfAPI Store {
public:
  Store() { }

  static void dbCf(const ZvCf *, ZdbCf &dbCf);	// inject tables into dbCf
  void init(Zdb *);
  void final();

  // convert shard to thread slot ID
  auto sid(Shard shard) const {
    return m_sids[shard & (m_sids.length() - 1)];
  }

  // dataframe threads (may be shared by app workloads)
  template <typename ...Args>
  void run(Shard shard, Args &&...args) const {
    m_mx->run(sid(shard), ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  void invoke(Shard shard, Args &&...args) const {
    m_mx->invoke(sid(shard), ZuFwd<Args>(args)...);
  }
  bool invoked(Shard shard) const { return m_mx->invoked(sid(shard)); }

  void open(OpenFn);	// establishes nextSeriesID
  void close();

  // open data frame
  template <typename O, bool TimeIndex, bool Create>
  void openDF(
    Shard shard, ZtString name,
    ZmFn<void(ZmRef<DataFrame<O, TimeIndex>>)> fn)
  {
    using DataFrame = Zdf::DataFrame<O, TimeIndex>;
    using DFRef = ZmRef<DataFrame>;
    using W = WrapType<O, TimeIndex>;
    using Fields = Zdf::Fields<W>;
    using SeriesRefs = Zdf::SeriesRefs<W>;

    [
      this, shard, name = ZuMv(name), fn = ZuMv(fn),
      seriesRefs = SeriesRefs{}
    ](this auto &&self, auto I, auto series) {
      if constexpr (I >= 0) {
	if (ZuUnlikely(!series)) { fn(DFRef{}); return; }
	seriesRefs.template p<I>() = ZuMv(series);
      }
      enum { J = I + 1 };
      if constexpr (J >= SeriesRefs::N) {
	fn(DFRef{new DataFrame{this, shard, ZuMv(name), ZuMv(seriesRefs)}});
      } else {
	using Field = ZuType<J, Fields>;
	using Decoder = FieldDecoder<Field>;
	auto next = [self = ZuMv(self)](auto series) mutable {
	  ZuMv(self).template operator()(ZuInt<J>{}, ZuMv(series));
	};
	ZtString seriesName{name.length() + strlen(Field::id()) + 2};
	seriesName << name << '/' << Field::id();
	openSeries<Decoder, Create>(
	  shard, ZuMv(seriesName), /* FIXME - epoch */, ZuMv(next));
	// FIXME - obtain epoch from default time value of field
      }
    }(ZuInt<-1>{}, static_cast<void *>(nullptr));
  }

  // open series
  // FIXME - time series epoch
  template <typename Decoder, bool Create>
  void openSeries(
    Shard shard, ZtString name, ZuTime epoch,
    ZmFn<void(ZmRef<Series<Decoder>>)> fn)
  {
    using Series = Zdf::Series<Decoder>;
    using DBSeries = typename Series::DBSeries;
    enum { Fixed = Series::Fixed };

    static auto seriesTbl = [](const Store *this_) {
      if constexpr (Fixed)
	return this_->m_seriesFixedTbl;
      else
	return this_->m_seriesFloatTbl;
    };

    run(shard, [
      this, shard, name = ZuMv(name), fn = ZuMv(fn)
    ]() mutable {
      auto findFn = [
	this, shard, name, epoch, fn = ZuMv(fn)
      ](ZdbObjRef<DBSeries> dbSeries) mutable {
	if (dbSeries) {
	  ZmRef<Series> series = new Series{this, ZuMv(dbSeries)};
	  series->open(ZuMv(fn));
	  return;
	}
	if (!Create) { fn(nullptr); return; }
	dbSeries = new ZdbObject<DBSeries>{seriesTbl(this), shard};
	new (dbSeries->ptr_()) DBSeries{
	  .id = m_nextSeriesID++,
	  .name = ZuMv(name),
	  .epoch = epoch,
	  .blkOffset = 0
	};
	auto insertFn = [
	  this, fn = ZuMv(fn)
	](ZdbObjRef<DBSeries> dbSeries) mutable {
	  if (!dbSeries) { fn(nullptr); return; }
	  dbSeries->commit();
	  ZmRef<Series> series = new Series{this, ZuMv(dbSeries)};
	  series->open(ZuMv(fn));
	};
	seriesTbl(this)->insert(dbSeries, ZuMv(insertFn));
      };
      seriesTbl(this)->template find<1>(shard, ZuMvTuple(name), ZuMv(findFn));
    });
  }

  ZdbTable<DB::SeriesFixed> *seriesFixedTbl() const { return m_seriesFixedTbl; }
  ZdbTable<DB::SeriesFloat> *seriesFloatTbl() const { return m_seriesFloatTbl; }
  ZdbTable<DB::BlkFixed> *blkFixedTbl() const { return m_blkFixedTbl; }
  ZdbTable<DB::BlkFloat> *blkFloatTbl() const { return m_blkFloatTbl; }
  ZdbTable<DB::BlkData> *blkDataTbl() const { return m_blkDataTbl; }

private:
  void open_recoverNextSeriesID_Fixed();
  void open_recoverNextSeriesID_Float();
  void opened(bool ok);

private:
  ZiMultiplex			*m_mx = nullptr;
  StoreState::T			m_state = StoreState::Uninitialized;
  ZdbTblRef<DB::SeriesFixed>	m_seriesFixedTbl;
  ZdbTblRef<DB::SeriesFloat>	m_seriesFloatTbl;
  ZdbTblRef<DB::BlkFixed>	m_blkFixedTbl;
  ZdbTblRef<DB::BlkFloat>	m_blkFloatTbl;
  ZdbTblRef<DB::BlkData>	m_blkDataTbl;
  ZdbTableCf::SIDArray		m_sids;
  ZmAtomic<uint32_t>		m_nextSeriesID = 1;
  OpenFn			m_openFn;
};

template <typename O, bool TimeIndex>
template <typename ...Args>
inline void DataFrame<O, TimeIndex>::run(Args &&...args) const
{
  m_store->run(m_shard, ZuFwd<Args>(args)...);
}
template <typename O, bool TimeIndex>
template <typename ...Args>
inline void DataFrame<O, TimeIndex>::invoke(Args &&...args) const
{
  m_store->invoke(m_shard, ZuFwd<Args>(args)...);
}
template <typename O, bool TimeIndex>
inline bool DataFrame<O, TimeIndex>::invoked() const
{
  return m_store->invoked(m_shard);
}

template <typename Decoder>
template <typename ...Args>
inline void Series<Decoder>::run(Args &&...args) const
{
  m_store->run(m_shard, ZuFwd<Args>(args)...);
}
template <typename Decoder>
template <typename ...Args>
inline void Series<Decoder>::invoke(Args &&...args) const
{
  m_store->invoke(m_shard, ZuFwd<Args>(args)...);
}
template <typename Decoder>
inline bool Series<Decoder>::invoked() const
{
  return m_store->invoked(m_shard);
}

template <typename Decoder>
ZdbTable<DB::BlkData> *Series<Decoder>::blkDataTbl() const
{
  return m_store->blkDataTbl();
}
template <typename Decoder>
auto Series<Decoder>::seriesTbl() const ->
ZuIf<Fixed, ZdbTable<DB::SeriesFixed> *, ZdbTable<DB::SeriesFloat> *>
{
  if constexpr (Fixed)
    return m_store->seriesFixedTbl();
  else
    return m_store->seriesFloatTbl();
}
template <typename Decoder>
auto Series<Decoder>::blkTbl() const ->
ZuIf<Fixed, ZdbTable<DB::BlkFixed> *, ZdbTable<DB::BlkFloat> *>
{
  if constexpr (Fixed)
    return m_store->blkFixedTbl();
  else
    return m_store->blkFloatTbl();
}

} // namespace Zdf

#endif /* ZdfStore_HH */
