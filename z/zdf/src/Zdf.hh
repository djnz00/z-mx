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

// wrapper for type together with time index flag
template <typename O_, bool TimeIndex_>
struct WrapType {
  using O = O_;
  enum { TimeIndex = TimeIndex_ };
};

// synthetic field returning time now, used if TimeIndex is set
using TimeType = ZtFieldType_Time<ZuTime, ZuTypeList<ZuFieldProp::NDP<9>>>;
template <typename O_>
struct TimeField_ {
  using O = O_;
  using T = ZuTime;
  using Props = ZuTypeList<
    ZuFieldProp::NDP<9>
    ZuFieldProp::Synthetic,
    ZuFieldProp::Series>;
  enum { ReadOnly = 1 };
  static constexpr const char *id() { return "_time"; }
  ZuTime get(const O &) { return Zm::now(); }
  template <typename P> static void set(O &, P &&) { }
};
template <typename O_>
using TimeField = ZtField_Time<TimeField_<O>>;

// Zdf data-frames are comprised of series fields
template <typename Field>
using FieldFilter = ZuTypeIn<ZtFieldProp::Series, typename Field::Props>;
template <typename O, bool TimeIndex>
struct Fields_ {
  using T = ZuTypeGrep<FieldFilter, ZuFields<O>>;
};
template <typename O>
struct Fields_<O, true> {
  using T = Fields_<O, false>::typename T::template Unshift<TimeField<O>>;
};
template <typename W>
using Fields = typename Fields_<typename W::O, W::TimeIndex>::T;

// map a field to its Decoder type
template <typename Field, typename Props = typename Field::Props>
using FieldDecoderFlags_ = ZuUnsigned<
  ((Field::Type::Code == ZtFieldTypeCode::Float) ? 4 : 0) |
  (ZuTypeIn<ZtFieldProp::Delta2, Props>{} ? 2 : 0) |
  (ZuTypeIn<ZtFieldProp::Delta, Props>{} ? 1 : 0)>;
template <typename Field, unsigned = FieldDecoderFlags_<Field>{}>
struct FieldDecoder_;
template <typename Field>
struct FieldDecoder_<Field, 0U> { using T = Decoder; };
template <typename Field>
struct FieldDecoder_<Field, 1U> { using T = DeltaDecoder<>; };
template <typename Field>
struct FieldDecoder_<Field, 2U> { using T = DeltaDecoder<DeltaDecoder<>>; };
template <typename Field>
struct FieldDecoder_<Field, 4U> { using T = FloatDecoder; };
template <typename Field>
using FieldDecoder = typename FieldDecoder_<Field>::T;

// map a field to corresponding Series, RdRef, WrRef
template <typename Field>
using FieldSeriesRef = ZmRef<Series<FieldDecoder<Field>>>;
template <typename Field> using FieldRdRef = RdRef<FieldDecoder<Field>>;
template <typename Field> using FieldWrRef = WrRef<FieldDecoder<Field>>;

// tuples of series and writer references given object type wrapper
template <typename W>
using SeriesRefs = ZuTypeApply<ZuTuple, ZuTypeMap<FieldSeriesRef, Fields<W>>>;
template <typename W>
using WrRefTuple = ZuTypeApply<ZuTuple, ZuTypeMap<FieldWrRef, Fields<W>>>;

template <typename O> class DataFrame;

// data frame writer
template <typename W>
class DFWriter {
  DFWriter() = delete;
  DFWriter(const DFWriter &) = delete;
  DFWriter &operator =(const DFWriter &) = delete;

public:
  using O = typename W::O;
  enum { TimeIndex = W::TimeIndex };
  using DataFrame = Zdf::DataFrame<O, TimeIndex>;

private:
friend DataFrame;

  using Fields = Zdf::Fields<W>;
  using WrRefs = WrRefTuple<W>;

  DFWriter() = default;
  DFWriter(DataFrame *df, WrRefs wrRefs) :
    m_df{df}, m_wrRefs{ZuMv(wrRefs)} { }

public:

  DFWriter(DFWriter &&) = default;
  DFWriter &operator =(DFWriter &&) = default;
  ~DFWriter() = default;

  bool operator !() const { return !m_df; }

  void write(const O &o) {
    using namespace ZtFieldTypeCode;

    ZuUnroll::all<WrRefs::N>([this, &o](auto I) {
      using Field = ZuType<I, Fields>;
      if constexpr (
	  Field::Code == Float ||
	  Field::Code == Fixed)
	m_wrRefs.template p<I>()->write(Field::get(o));
      else if constexpr (
	  Field::Code == Int8 ||
	  Field::Code == UInt8 ||
	  Field::Code == Int16 ||
	  Field::Code == UInt16 ||
	  Field::Code == Int32 ||
	  Field::Code == UInt32 ||
	  Field::Code == Int64 ||
	  Field::Code == UInt64 ||
	  Field::Code == UInt ||
	  Field::Code == Enum)
	m_wrRefs.template p<I>()->write(ZuFixed{Field::get(o), 0});
      else if constexpr (Field::Code == Decimal)
	m_wrRefs.template p<I>()->write(
	  ZuFixed{Field::get(o), ZuFieldProp::GetNDP<Field::Props>{}});
      else if constexpr (Field::Code == Time)
	m_wrRefs.template p<I>()->write(
	  ZuFixed{m_df->nsecs(Field::get(o)), 9});
    });
  }

  struct StopContext : public ZmObject {
    StopFn		fn;

    ~StopContext() { fn(); }
  };
  void stop(StopFn fn) {
    ZmRef<StopContext> context = new StopContext{ZuMv(fn)};
    ZuUnroll::all<WrRefs::N>([this, &context](auto I) {
      m_wrRefs.template p<I>()->stop([context]() { });
    });
  }

  void final() {
    m_df = nullptr;
    m_wrRefs = {};
  }

private:
  DataFrame		*m_df = nullptr;
  WrRefs		m_wrRefs;
};

template <typename O_, bool TimeIndex_ = false>
class DataFrame : public ZmObject {
public:
  using O = O_;
  enum { TimeIndex = TimeIndex_ };
private:
  using W = WrapType<O, TimeIndex>;
public:
  using DFWriter = Zdf::DFWriter<W>;

private:
  friend DB;

  using Fields = Zdf::Fields<W>;
  using SeriesRefs = Zdf::SeriesRefs<W>;
  using WrRefs = WrRefTuple<W>;

  DataFrame(DB *db, Shard shard, ZtString name, SeriesRefs seriesRefs) :
    m_db{db}, m_shard{shard}, m_name{ZuMv(name)},
    m_seriesRefs{ZuMv(seriesRefs)} { }

public:
  ~DataFrame() = default;

  DB *db() const { return m_db; }
  Shard shard() const { return m_shard; }
  const ZtString &name() const { return m_name; }

  void write(ZmFn<void(DFWriter)> fn) {
    ZuLambda{[
      this, fn = ZuMv(fn), wrRefs = WrRefs{}
    ](auto &&self, auto I, auto wrRef) mutable {
      if constexpr (I >= 0) {
	if (ZuUnlikely(!wrRef)) { fn(DFWriter{}); return; }
	wrRefs.template p<I>() = ZuMv(wrRef);
      }
      enum { J = I + 1 };
      if constexpr (J >= Series::N) {
	fn(DFWriter{this, ZuMv(wrRefs)});
	return;
      }
      using Field = ZuType<I, Fields>;
      auto seriesRefs = &m_seriesRefs;
      auto next = [self = ZuMv(self)](auto wrRef) {
	ZuMv(self).template operator()(I, ZuMv(wrRef));
      };
      if constexpr (Field::Code == Float)
	seriesRefs->template p<I>()->write(ZuMv(next));
      else
	seriesRefs->template p<I>()->write(
	  ZuMv(next), ZuFieldProp::GetNDP<Field::Props>{});
    }}(ZuInt<-1>{}, static_cast<void *>(nullptr));
  }

public:
  template <typename Field>
  RdRef seek(Offset offset = 0) {
    using I = ZuTypeIndex<Field, Fields>;
    m_series.template p<I{}>()->
    auto field = m_fields[i];
    unsigned props = field ? field->props : ZtVFieldProp::Delta;
    r.seek(m_series[i], props, offset);
  }
  template <typename Field, typename Value>
  void find(Value value) {
    auto field = m_fields[i];
    unsigned props = field ? field->props : ZtVFieldProp::Delta;
    r.find(m_series[i], props, value);
  }

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
  DB		*m_db;
  Shard		m_shard;
  ZtString	m_name;
  SeriesRefs	m_seriesRefs;
};

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
	return;
      }
      using Field = ZuType<I, Fields>;
      using Decoder = FieldDecoder<Field>;
      auto next = [self = ZuMv(self)](auto series) {
	ZuMv(self).template operator()(I, ZuMv(series));
      };
      ZtString seriesName{name.length() + strlen(Field::id()) + 2};
      seriesName << name << '/' << Field::id();
      openSeries<Decoder>(shard, ZuMv(seriesName), Create, ZuMv(next));
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

#endif /* Zdf_HH */
