//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Frame backing data store

#ifndef ZdfDB_HH
#define ZdfDB_HH

#ifndef ZdfLib_HH
#include <zlib/ZdfLib.hh>
#endif

#include <zlib/ZtString.hh>

#include <zlib/Zdb.hh>

#include <zlib/ZdfTypes.hh>
#include <zlib/ZdfSchema.hh>
#include <zlib/ZdfCompress.hh>
#include <zlib/ZdfSeries.hh>

namespace Zdf {

// DB state
namespace DBState {
  ZtEnumValues(DBState, int,
    Uninitialized = 0, Initialized, Opening, Opened, OpenFailed);
}

using OpenFn = ZmFn<void(bool)>;	// (bool ok)

class ZdfAPI DB {
public:
  void init(ZvCf *, Zdb *);
  void final();

  // convert shard to thread slot ID
  auto sid(Shard shard) const {
    return m_sid[shard & (m_sid.length() - 1)];
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

    ZuLambda{[
      this, shard, name = ZuMv(name), fn = ZuMv(fn),
      seriesRefs = SeriesRefs{}
    ](auto &&self, auto I, auto series) mutable {
      if constexpr (I >= 0) {
	if (ZuUnlikely(!series)) { fn(DFRef{}); return; }
	seriesRefs.template p<I>() = ZuMv(series);
      }
      enum { J = I + 1 };
      if constexpr (J >= Series::N) {
	fn(DFRef{new DataFrame{this, shard, ZuMv(name), ZuMv(seriesRefs)}});
      } else {
	using Field = ZuType<I, Fields>;
	using Decoder = FieldDecoder<Field>;
	auto next = [self = ZuMv(self)](auto series) mutable {
	  ZuMv(self).template operator()(ZuInt<J>{}, ZuMv(series));
	};
	ZtString seriesName{name.length() + strlen(Field::id()) + 2};
	seriesName << name << '/' << Field::id();
	openSeries<Decoder>(shard, ZuMv(seriesName), Create, ZuMv(next));
      }
    }}(ZuInt<-1>{}, static_cast<void *>(nullptr));
  }

  // open series
  template <typename Decoder, bool Create>
  void openSeries(
    Shard shard, ZtString name,
    ZmFn<void(ZmRef<Series<Decoder>>)>)
  {
    using Series = Zdf::Series<Decoder>;
    using DBSeries = typename Series::DBType;
    enum { Fixed = Series::Fixed };

    static auto seriesTbl = [](const DB *this_) {
      if constexpr (Fixed)
	return this_->m_seriesFixedTbl;
      else
	return this_->m_seriesFloatTbl;
    };

    run(shard, [
      this, shard, name = ZuMv(name), fn = ZuMv(fn)
    ]() mutable {
      auto findFn = [
	this, shard, name = ZuMv(name), fn = ZuMv(fn)
      ](ZdbObjRef<DBSeries> dbSeries) mutable {
	if (dbSeries) {
	  ZmRef<Series> series = new Series{this, ZuMv(dbSeries)};
	  series->open(ZuMv(fn));
	  return;
	}
	if (!Create) { fn(nullptr); return; }
	dbSeries = new ZdbObject<DBSeries>{seriesTbl(), shard};
	new (dbSeries->ptr_()) DBType{
	  .id = m_nextSeriesID++,
	  .dfid = 0,
	  .name = ZuMv(name),
	  .epoch = Zm::now()
	};
	auto insertFn = [
	  series = ZuMv(series), fn = ZuMv(fn)
	](ZdbObjRef<DBSeries> dbSeries) mutable {
	  if (!dbSeries) { fn(nullptr); return; }
	  dbSeries->commit();
	  ZmRef<Series> series = new Series{this, ZuMv(dbSeries)};
	  series->open(ZuMv(fn));
	};
	seriesTbl()->insert(dbSeries, ZuMv(insertFn));
      };
      auto key = ZuMvTuple(ZuString{name});
      seriesTbl()->find<1>(shard, key, ZuMv(findFn));
    });
  }

  ZdbTable<DB::SeriesFixed> *seriesFixedTbl() const { return m_seriesFixedTbl; }
  ZdbTable<DB::SeriesFloat> *seriesFloatTbl() const { return m_seriesFloatTbl; }
  ZdbTable<DB::BlkHdrFixed> *blkHdrFixedTbl() const { return m_blkHdrFixedTbl; }
  ZdbTable<DB::BlkHdrFloat> *blkHdrFloatTbl() const { return m_blkHdrFloatTbl; }
  ZdbTable<DB::BlkData> *blkDataTbl() const { return m_blkDataTbl; }

private:
  DBState::T			m_state = DBState::Uninitialized;
  ZdbTblRef<DB::SeriesFixed>	m_seriesFixedTbl;
  ZdbTblRef<DB::SeriesFloat>	m_seriesFloatTbl;
  ZdbTblRef<DB::BlkHdrFixed>	m_blkHdrFixedTbl;
  ZdbTblRef<DB::BlkHdrFloat>	m_blkHdrFloatTbl;
  ZdbTblRef<DB::BlkData>	m_blkDataTbl;
  ZdbTableCf::SIDArray		m_sid;
  ZmAtomic<uint32_t>		m_nextSeriesID = 1;
};

} // namespace Zdf

#endif /* ZdfDB_HH */
