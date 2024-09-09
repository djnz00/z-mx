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
// df.find(index, 0, df.nsecs(time));	// index time to offset // FIXME
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

#include <zlib/ZtArray.hh>
#include <zlib/ZtString.hh>

#include <zlib/ZtField.hh>

#include <zlib/ZdfTypes.hh>
#include <zlib/ZdfSchema.hh>
#include <zlib/ZdfCompress.hh>
#include <zlib/ZdfSeries.hh>

namespace Zdf {

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
    ZuFieldProp::NDP<9>,
    ZuFieldProp::Synthetic,
    ZuFieldProp::Series>;
  enum { ReadOnly = 1 };
  static constexpr const char *id() { return "_time"; }
  ZuTime get(const O &) { return Zm::now(); }
  template <typename P> static void set(O &, P &&) { }
};
template <typename O>
using TimeField = ZtField_Time<TimeField_<O>>;

// Zdf data-frames are comprised of series fields
template <typename Field>
using FieldFilter = ZuTypeIn<ZuFieldProp::Series, typename Field::Props>;
template <typename O, bool TimeIndex>
struct Fields_ {
  using T = ZuTypeGrep<FieldFilter, ZuFields<O>>;
};
// if the data-frame is time-indexed, unshift a TimeField onto the field list
template <typename O>
struct Fields_<O, true> {
  using T = Fields_<O, false>::T::template Unshift<TimeField<O>>;
};
template <typename W>
using Fields = typename Fields_<typename W::O, W::TimeIndex>::T;

// map a field to its Decoder type
template <typename Field, typename Props = typename Field::Props>
using FieldDecoderFlags_ = ZuUnsigned<
  ((Field::Type::Code == ZtFieldTypeCode::Float) ? 4 : 0) |
  (ZuTypeIn<ZuFieldProp::Delta2, Props>{} ? 2 : 0) |
  (ZuTypeIn<ZuFieldProp::Delta, Props>{} ? 1 : 0)>;
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
template <typename Field> using FieldSeries = Series<FieldDecoder<Field>>;
template <typename Field> using FieldSeriesRef = ZmRef<FieldSeries<Field>>;
template <typename Field> using FieldWrRef = WrRef<FieldDecoder<Field>>;

// tuples of series and writer references given object type wrapper
template <typename W>
using SeriesRefs = ZuTypeApply<ZuTuple, ZuTypeMap<FieldSeriesRef, Fields<W>>>;
template <typename W>
using WrRefTuple = ZuTypeApply<ZuTuple, ZuTypeMap<FieldWrRef, Fields<W>>>;

template <typename O, bool TimeIndex> class DataFrame;

template <
  typename Field,
  typename Props = typename Field::Props,
  bool HasNDP = ZuFieldProp::HasNDP<Props>{}>
struct GetNDP_ {
  using T = ZuFieldProp::GetNDP<Props>;
};
template <typename Field, typename Props>
struct GetNDP_<Field, Props, false> {
  using T = ZuUnsigned<0>;
};
template <typename Field>
using GetNDP = typename GetNDP_<Field>::T;

// data frame writer
template <typename W>
class DFWriter {
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
      enum { NDP = GetNDP<Field>{} };
      if constexpr (Field::Code == Float)
	m_wrRefs.template p<I>()->write(Field::get(o));
      else if constexpr (Field::Code == Fixed)
	m_wrRefs.template p<I>()->write(Field::get(o).adjust(NDP));
      else if constexpr (
	  Field::Code == Int8 ||
	  Field::Code == UInt8 ||
	  Field::Code == Int16 ||
	  Field::Code == UInt16 ||
	  Field::Code == Int32 ||
	  Field::Code == UInt32 ||
	  Field::Code == Int64 ||
	  Field::Code == UInt64)
	m_wrRefs.template p<I>()->write(Field::get(o));
      else if constexpr (Field::Code == Decimal)
	m_wrRefs.template p<I>()->write(ZuFixed{Field::get(o), NDP});
      else if constexpr (Field::Code == Time)
	m_wrRefs.template p<I>()->write(
	  m_df->template series<I>()->nsecs(Field::get(o)));
    });
  }

  using StopFn = ZmFn<void()>;
private:
  struct StopContext : public ZmObject {
    StopFn		fn;

    ~StopContext() { fn(); }
  };
public:
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

class Store;

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
  friend Store;

  using Fields = Zdf::Fields<W>;
  using SeriesRefs = Zdf::SeriesRefs<W>;
  using WrRefs = WrRefTuple<W>;

  DataFrame(Store *store, Shard shard, ZtString name, SeriesRefs seriesRefs) :
    m_store{store}, m_shard{shard}, m_name{ZuMv(name)},
    m_seriesRefs{ZuMv(seriesRefs)} { }

public:
  ~DataFrame() = default;

  Shard shard() const { return m_shard; }
  const ZtString &name() const { return m_name; }

  // run/invoke on shard
  template <typename ...Args> void run(Args &&...args) const;
  template <typename ...Args> void invoke(Args &&...args) const;
  bool invoked() const;

  template <typename Field>
  auto series() const {
    using I = ZuTypeIndex<Field, Fields>;
    return m_seriesRefs.template p<I>();
  }

  void write(ZmFn<void(DFWriter)> fn) {
    ZuLambda{[
      this, fn = ZuMv(fn), wrRefs = WrRefs{}
    ](auto &&self, auto I, auto wrRef) mutable {
      if constexpr (I >= 0) {
	if (ZuUnlikely(!wrRef)) { fn(DFWriter{}); return; }
	wrRefs.template p<I>() = ZuMv(wrRef);
      }
      enum { J = I + 1 };
      if constexpr (J >= SeriesRefs::N) {
	fn(DFWriter{this, ZuMv(wrRefs)});
      } else {
	using Field = ZuType<J, Fields>;
	auto seriesRefs = &m_seriesRefs;
	auto next = [self = ZuMv(self)](auto wrRef) mutable {
	  ZuMv(self).template operator()(ZuInt<J>{}, ZuMv(wrRef));
	};
	if constexpr (Field::Code == ZtFieldTypeCode::Float)
	  seriesRefs->template p<J>()->write(ZuMv(next));
	else
	  seriesRefs->template p<J>()->write(
	    ZuMv(next), GetNDP<Field>{});
      }
    }}(ZuInt<-1>{}, static_cast<void *>(nullptr));
  }

public:
  template <typename Field>
  auto seek(Offset offset = 0) {
    using I = ZuTypeIndex<Field, Fields>;
    return m_seriesRefs.template p<I{}>()->seek(offset);
  }
  template <typename Field>
  auto find(typename FieldSeries<Field>::Value value) {
    using I = ZuTypeIndex<Field, Fields>;
    return m_seriesRefs.template p<I{}>()->find(value);
  }

private:
  Store		*m_store = nullptr;
  Shard		m_shard;
  ZtString	m_name;
  SeriesRefs	m_seriesRefs;
};

} // namespace Zdf

#endif /* Zdf_HH */
