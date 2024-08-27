//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Frame

// Data d;
// ...
// DataFrame df{Data::fields(), "d"};
// ...
// auto w = df.writer();
// ZuTime time{ZuTime::Now};
// w.write(&d);
// ...
// AnyReader index, reader;
// ...
// df.find(index, 0, df.nsecs(time));	// index time to offset
// df.seek(reader, N, index.offset());	// seek reader to offset
// ...
// ZuFixed nsecs, value;
// index.read(nsecs);
// ZuTime then = df.time(nsecs);
// reader.read(value);

#ifndef Zdf_HH
#define Zdf_HH

#ifndef ZdfLib_HH
#include <zlib/ZdfLib.hh>
#endif

#include <zlib/ZuPtr.hh>
#include <zlib/ZuUnion.hh>

#include <zlib/ZtArray.hh>
#include <zlib/ZtString.hh>

#include <zlib/ZtField.hh>

#include <zlib/Zfb.hh>
#include <zlib/ZfbField.hh>

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

// typedefs for (de)encoders
using AbsDecoder = ZdfCompress::Decoder;
template <typename Base>
using DeltaDecoder_ = ZdfCompress::DeltaDecoder<Base>;
using DeltaDecoder = DeltaDecoder_<AbsDecoder>;
using Delta2Decoder = DeltaDecoder_<DeltaDecoder>;
using FPEncoder = ZdfCompress::FloatDecoder;

using AbsSeries = Series<AbsDecoder>;
using AbsReader = Reader<AbsDecoder>;
using AbsWriter = Writer<AbsDecoder>;

using DeltaSeries = Series<DeltaDecoder>;
using DeltaReader = Reader<DeltaDecoder>;
using DeltaWriter = Writer<DeltaDecoder>;

using Delta2Series = Series<Delta2Decoder>;
using Delta2Reader = Reader<Delta2Decoder>;
using Delta2Writer = Writer<Delta2Decoder>;

using FloatSeries = Series<FloatDecoder>;
using FloatReader = Reader<FloatDecoder>;
using FloatWriter = Writer<FloatDecoder>;

// Zdf data-frames are comprised of series fields
template <typename Field>
using FieldFilter = ZuTypeIn<ZtFieldProp::Series, typename Field::Props>;

template <typename T>
using Fields = ZuTypeGrep<FieldFilter, ZuFields<T>>;

template <typename T>
auto fields() { return ZtVFields_<Fields<T>>(); }

using OpenFn = ZmFn<void(bool)>;	// (bool ok)
using OpenDFFn = ZmFn<void(ZmRef<DataFrame>)>;

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

  void open(OpenFn);			// establishes nextDFID, nextSeriesID
  void close();

  // open data frame
  void openDF(
    Shard shard, ZtString name, bool create,
    const ZtVFieldArray &fields, bool timeIndex, OpenDFFn);

  // open series
  template <typename Decoder>
  void openSeries(Shard shard, ZtString name, bool create, OpenSeriesFn) {
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
      this, shard, name = ZuMv(name), create, fn = ZuMv(fn)
    ]() mutable {
      auto findFn = [
	this, shard, name = ZuMv(name), create, fn = ZuMv(fn)
      ](ZdbObjRef<DBSeries> dbSeries) mutable {
	if (dbSeries) {
	  ZmRef<Series> series = new Series{this, ZuMv(dbSeries)};
	  series->open(ZuMv(fn));
	  return;
	}
	if (!create) { fn(nullptr); return; }
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

  ZdbTable<DB::DataFrame> *dataFrameTbl() const { return m_dataFrameTbl; }
  ZdbTable<DB::SeriesFixed> *seriesFixedTbl() const { return m_seriesFixedTbl; }
  ZdbTable<DB::SeriesFloat> *seriesFloatTbl() const { return m_seriesFloatTbl; }
  ZdbTable<DB::BlkHdrFixed> *blkHdrFixedTbl() const { return m_blkHdrFixedTbl; }
  ZdbTable<DB::BlkHdrFloat> *blkHdrFloatTbl() const { return m_blkHdrFloatTbl; }
  ZdbTable<DB::BlkData> *blkDataTbl() const { return m_blkDataTbl; }

private:
  DBState::T			m_state = DBState::Uninitialized;
  ZdbTblRef<DB::DataFrame>	m_dataFrameTbl;
  ZdbTblRef<DB::SeriesFixed>	m_seriesFixedTbl;
  ZdbTblRef<DB::SeriesFloat>	m_seriesFloatTbl;
  ZdbTblRef<DB::BlkHdrFixed>	m_blkHdrFixedTbl;
  ZdbTblRef<DB::BlkHdrFloat>	m_blkHdrFloatTbl;
  ZdbTblRef<DB::BlkData>	m_blkDataTbl;
  ZdbTableCf::SIDArray		m_sid;
  ZmAtomic<uint32_t>		m_nextDFID = 1;
  ZmAtomic<uint32_t>		m_nextSeriesID = 1;
};

class ZdfAPI DataFrame : public DB::DataFrame {
  DataFrame() = delete;
  DataFrame(const DataFrame &) = delete;
  DataFrame &operator =(const DataFrame &) = delete;
  DataFrame(DataFrame &&) = delete;
  DataFrame &operator =(DataFrame &&) = delete;

  friend class DB;

public:
  ~DataFrame() = default;

  DataFrame(
    DB *db, const ZtVFieldArray &fields,
    ZuString name, bool timeIndex = false);

  const ZtString &name() const { return m_name; }
  const ZuTime &epoch() const { return m_epoch; }

  class ZdfAPI Writer {
    Writer(const Writer &) = delete;
    Writer &operator =(const Writer &) = delete;
  public:
    Writer() = default;
    Writer(Writer &&) = default;
    Writer &operator =(Writer &&) = default;
    ~Writer() = default;

  friend DataFrame;
    Writer(DataFrame *df) : m_df(df) {
      unsigned n = df->nSeries();
      m_writers.length(n);
      for (unsigned i = 0; i < n; i++) m_writers[i] = df->writer_(i);
    }

  public:
    void write(const void *ptr) {
      using namespace ZtFieldTypeCode;

      unsigned n = m_writers.length();
      if (ZuUnlikely(!n)) return;
      for (unsigned i = 0; i < n; i++) {
	auto field = m_df->field(i);
	if (i || field) {
	  auto code = field->type->code;
	  if (code == Float)
	    m_writers[i]->write(field->get.get<Float>(ptr));
	  else {
	    ZuFixed v;
	    switch (code) {
	      case Int8:    v = {field->get.get<Int8>(ptr), 0}; break;
	      case UInt8:   v = {field->get.get<UInt8>(ptr), 0}; break;
	      case Int16:   v = {field->get.get<Int16>(ptr), 0}; break;
	      case UInt16:  v = {field->get.get<UInt16>(ptr), 0}; break;
	      case Int32:   v = {field->get.get<Int32>(ptr), 0}; break;
	      case UInt32:  v = {field->get.get<UInt32>(ptr), 0}; break;
	      case Int64:   v = {field->get.get<Int64>(ptr), 0}; break;
	      case UInt64:  v = {field->get.get<UInt64>(ptr), 0}; break;
	      case UInt:    v = {field->get.get<UInt>(ptr), 0}; break;
	      case Enum:    v = {field->get.get<Enum>(ptr), 0}; break;
	      case Fixed:   v = field->get.get<Fixed>(ptr); break;
	      case Decimal:
		v = {field->get.get<Decimal>(ptr), field->ndp};
		break;
	      case Time:
		v = {m_df->nsecs(field->get.get<Time>(ptr)), 9};
		break;
	      default:
		v = {0, 0};
		break;
	    }
	    m_writers[i]->write(v);
	  }
	} else {
	  m_writers[i]->write(ZuFixed{m_df->nsecs(Zm::now()), 9});
	}
      }
    }

    void final() {
      m_df = nullptr;
      m_writers.null();
    }

  private:
    DataFrame		*m_df = nullptr;
    ZtArray<WrHandle>	m_writers;
  };
  Writer writer() { return Writer{this}; }

friend Writer;
private:
  WrHandle writer_(unsigned i) {
    auto field = m_fields[i];
    unsigned props = field ? field->props : ZtVFieldProp::Delta();

    if (field->type->code == ZtFieldTypeCode::Float)

    if (props & ZtV
  }
public:
  void seek(AnyReader &r, unsigned i, uint64_t offset = 0) {
    auto field = m_fields[i];
    unsigned props = field ? field->props : ZtVFieldProp::Delta;
    r.seek(m_series[i], props, offset);
  }
  void find(AnyReader &r, unsigned i, const ZuFixed &value) {
    auto field = m_fields[i];
    unsigned props = field ? field->props : ZtVFieldProp::Delta;
    r.find(m_series[i], props, value);
  }

  unsigned nSeries() const { return m_series.length(); }
  const Series *series(unsigned i) const { return m_series[i]; }
  Series *series(unsigned i) { return m_series[i]; }
  const ZtVField *field(unsigned i) const { return m_fields[i]; }

private:
  static constexpr const uint64_t pow10_9() { return 1000000000UL; }
public:
  ZuFixed nsecs(ZuTime t) {
    t -= m_epoch;
    return ZuFixed{static_cast<uint64_t>(t.sec()) * pow10_9() + t.nsec(), 9};
  }
  ZuTime time(const ZuFixed &v) {
    ZuFixedVal n = v.adjust(9);
    uint64_t p = pow10_9();
    return ZuTime{int64_t(n / p), int32_t(n % p)} + m_epoch;
  }

private:
  DB				*m_db = nullptr;
  ZtString			m_name;
  ZtArray<ZmRef<AnySeries>>	m_series;
  ZtArray<const ZtVField *>	m_fields;

  // async open/close series context
  using Callback = ZuUnion<void, OpenFn, CloseFn>;
  ZmPLock			m_lock;
    unsigned			  m_pending = 0;// number pending
    ZuPtr<Event>		  m_error;	// first error encountered
    Callback			  m_callback;	// completion callback
};

} // namespace Zdf

#endif /* Zdf_HH */
