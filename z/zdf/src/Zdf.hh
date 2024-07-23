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
#include <zlib/ZdfStore.hh>

namespace Zdf {

// typedefs for (de)encoders
using AbsDecoder = ZdfCompress::Decoder;
template <typename Base>
using DeltaDecoder_ = ZdfCompress::DeltaDecoder<Base>;
using DeltaDecoder = DeltaDecoder_<AbsDecoder>;
using Delta2Decoder = DeltaDecoder_<DeltaDecoder>;

using AbsEncoder = ZdfCompress::Encoder;
template <typename Base>
using DeltaEncoder_ = ZdfCompress::DeltaEncoder<Base>;
using DeltaEncoder = DeltaEncoder_<AbsEncoder>;
using Delta2Encoder = DeltaEncoder_<DeltaEncoder>;

// typedefs for reader/writer types
using AbsReader = Reader<Series, AbsDecoder>;
using DeltaReader = Reader<Series, DeltaDecoder>;
using Delta2Reader = Reader<Series, Delta2Decoder>;

using AbsWriter = Writer<Series, AbsEncoder>;
using DeltaWriter = Writer<Series, DeltaEncoder>;
using Delta2Writer = Writer<Series, Delta2Encoder>;

// run-time polymorphic reader
using AnyReader_ = ZuUnion<AbsReader, DeltaReader, Delta2Reader>;
class AnyReader : public AnyReader_ {
public:
  AnyReader() { initFn(); }
  AnyReader(const AnyReader &r) : AnyReader_{r} {
    initFn();
  }
  AnyReader(AnyReader &&r) : AnyReader_{static_cast<AnyReader_ &&>(r)} {
    initFn();
  }
  AnyReader &operator =(const AnyReader &r) {
    if (ZuLikely(this != &r)) {
      AnyReader_::operator =(r);
      initFn();
    }
    return *this;
  }
  AnyReader &operator =(AnyReader &&r) {
    if (ZuLikely(this != &r)) {
      AnyReader_::operator =(static_cast<AnyReader_ &&>(r));
      initFn();
    }
    return *this;
  }
  ~AnyReader() = default;

  void seek(const Series *s, unsigned props, uint64_t offset) {
    if (props & ZtVFieldProp::Delta)
      init_<DeltaReader>(s, offset);
    else if (props & ZtVFieldProp::Delta2)
      init_<Delta2Reader>(s, offset);
    else
      init_<AbsReader>(s, offset);
  }

  // series must monotonically increase

  void find(const Series *s, unsigned props, const ZuFixed &value) {
    if (props & ZtVFieldProp::Delta)
      find_<DeltaReader>(s, value);
    else if (props & ZtVFieldProp::Delta2)
      find_<Delta2Reader>(s, value);
    else
      find_<AbsReader>(s, value);
  }

  bool read(ZuFixed &v) { return m_readFn(this, v); }
  void seekFwd(uint64_t offset) { m_seekFwdFn(this, offset); }
  void seekRev(uint64_t offset) { m_seekRevFn(this, offset); }
  void findFwd(const ZuFixed &value) { m_findFwdFn(this, value); }
  void findRev(const ZuFixed &value) { m_findRevFn(this, value); }
  uint64_t offset() { return m_offsetFn(this); }

  void purge() { dispatch([](auto, auto &&v) { v.purge(); }); }

private:
  template <typename Reader>
  void init_(const Series *s, unsigned offset) {
    new (AnyReader_::new_<Reader>())
      Reader{s->seek<typename Reader::Decoder>(offset)};
    initFn_<Reader>();
  }
  template <typename Reader>
  void find_(const Series *s, const ZuFixed &value) {
    new (AnyReader_::new_<Reader>())
      Reader{s->find<typename Reader::Decoder>(value)};
    initFn_<Reader>();
  }

  void initFn() {
    dispatch([this](auto, auto &&v) { initFn_<ZuDecay<decltype(v)>>(); });
  }
  template <typename Reader>
  void initFn_() {
    m_readFn = [](AnyReader *this_, ZuFixed &v) {
      return this_->ptr_<Index<Reader>{}>()->read(v);
    };
    m_seekFwdFn = [](AnyReader *this_, uint64_t offset) {
      this_->ptr_<Index<Reader>{}>()->seekFwd(offset);
    };
    m_seekRevFn = [](AnyReader *this_, uint64_t offset) {
      this_->ptr_<Index<Reader>{}>()->seekRev(offset);
    };
    m_findFwdFn = [](AnyReader *this_, const ZuFixed &value) {
      this_->ptr_<Index<Reader>{}>()->findFwd(value);
    };
    m_findRevFn = [](AnyReader *this_, const ZuFixed &value) {
      this_->ptr_<Index<Reader>{}>()->findRev(value);
    };
    m_offsetFn = [](const AnyReader *this_) {
      return this_->ptr_<Index<Reader>{}>()->offset();
    };
  }

  typedef bool (*ReadFn)(AnyReader *, ZuFixed &);
  typedef void (*SeekFn)(AnyReader *, uint64_t); 
  typedef void (*FindFn)(AnyReader *, const ZuFixed &); 
  typedef uint64_t (*OffsetFn)(const AnyReader *);

private:
  ReadFn	m_readFn = nullptr;
  SeekFn	m_seekFwdFn = nullptr;
  SeekFn	m_seekRevFn = nullptr;
  FindFn	m_findFwdFn = nullptr;
  FindFn	m_findRevFn = nullptr;
  OffsetFn	m_offsetFn = nullptr;
};

// run-time polymorphic writer
using AnyWriter_ = ZuUnion<AbsWriter, DeltaWriter, Delta2Writer>;
class AnyWriter : public AnyWriter_ {
public:
  AnyWriter(const AnyWriter &r) = delete;
  AnyWriter &operator =(const AnyWriter &r) = delete;

  AnyWriter() { initFn(); }
  AnyWriter(AnyWriter &&r) : AnyWriter_{static_cast<AnyWriter_ &&>(r)} {
    initFn();
  }
  AnyWriter &operator =(AnyWriter &&w) {
    if (ZuLikely(this != &w)) {
      AnyWriter_::operator =(static_cast<AnyWriter_ &&>(w));
      initFn();
    }
    return *this;
  }
  ~AnyWriter() = default;

  void init(Series *s, unsigned props) {
    if (props & ZtVFieldProp::Delta)
      init_<DeltaWriter>(s);
    else if (props & ZtVFieldProp::Delta2)
      init_<Delta2Writer>(s);
    else
      init_<AbsWriter>(s);
  }

  bool write(const ZuFixed &v) { return m_writeFn(this, v); }
  void sync() { m_syncFn(this); }

private:
  template <typename Writer>
  void init_(Series *s) {
    new (AnyWriter_::new_<Writer>())
      Writer{s->writer<typename Writer::Encoder>()};
    initFn_<Writer>();
  }

  void initFn() {
    dispatch([this](auto, auto &&v) { initFn_<ZuDecay<decltype(v)>>(); });
  }
  template <typename Writer>
  void initFn_() {
    m_writeFn = [](AnyWriter *this_, const ZuFixed &v) {
      return this_->ptr_<Index<Writer>{}>()->write(v);
    };
    m_syncFn = [](AnyWriter *this_) {
      this_->ptr_<Index<Writer>{}>()->sync();
    };
  }

  typedef bool (*WriteFn)(AnyWriter *, const ZuFixed &);
  typedef void (*SyncFn)(AnyWriter *);

private:
  WriteFn	m_writeFn = nullptr;
  SyncFn	m_syncFn = nullptr;
};

// Zdf data-frames are comprised of series fields
template <typename Field>
using FieldFilter = ZuTypeIn<ZtFieldProp::Series, typename Field::Props>;

template <typename T>
using Fields = ZuTypeGrep<FieldFilter, ZuFields<T>>;

template <typename T>
auto fields() { return ZtVFields_<Fields<T>>(); }

class ZdfAPI Mgr {
public:
  void init(ZmRef<Zdb> db, ZuString prefix);

private:
  ZmRef<Zdb>		m_db;
  ZmRef<ZdbAnyTable>	m_dataFrame;
  ZmRef<ZdbAnyTable>	m_series;
  ZmRef<ZdbAnyTable>	m_hdr;
  ZmRef<ZdbAnyTable>	m_blk;
};

class ZdfAPI DataFrame {
  DataFrame() = delete;
  DataFrame(const DataFrame &) = delete;
  DataFrame &operator =(const DataFrame &) = delete;
  DataFrame(DataFrame &&) = delete;
  DataFrame &operator =(DataFrame &&) = delete;
public:
  ~DataFrame() = default;

  DataFrame(
    Mgr *mgr, const ZtVFieldArray &fields, ZuString name, bool timeIndex = false);

  const ZtString &name() const { return m_name; }
  const ZuTime &epoch() const { return m_epoch; }

  void open(OpenFn fn);
private:
  void openSeries();
  void openedSeries(OpenResult);
  void openFailed(OpenResult);
public:
  void close(CloseFn fn);
private:
  void closeSeries();
  void closedSeries(CloseResult);
  void closeFailed(CloseResult);

public:
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
      for (unsigned i = 0; i < n; i++) df->writer_(m_writers[i], i);
    }

  public:
    void write(const void *ptr) {
      unsigned n = m_writers.length();
      if (ZuUnlikely(!n)) return;
      ZuFixed v;
      for (unsigned i = 0; i < n; i++) {
	using namespace ZtFieldTypeCode;
	auto field = m_df->field(i);
	if (i || field) {
	  switch (field->type->code) {
	    case Int:     v = {field->get.get<Int>(ptr, i), 0}; break;
	    case UInt:    v = {field->get.get<UInt>(ptr, i), 0}; break;
	    case Enum:    v = {field->get.get<Enum>(ptr, i), 0}; break;
	    case Fixed:   v = field->get.get<Fixed>(ptr, i); break;
	    case Decimal: v = field->get.get<Decimal>(ptr, i); break;
	    case Time:    v = m_df->nsecs(field->get.get<Time>(ptr, i)); break;
	    default:      v = ZuFixed{0, 0}; break;
	  }
	} else
	  v = m_df->nsecs(Zm::now());
	m_writers[i].write(v);
      }
    }

    void sync() {
      unsigned n = m_writers.length();
      for (unsigned i = 0; i < n; i++)
	m_writers[i].sync();
    }

    void final() {
      m_df = nullptr;
      m_writers.null();
    }

  private:
    DataFrame		*m_df = nullptr;
    ZtArray<AnyWriter>	m_writers;
  };
  Writer writer() { return Writer{this}; }

friend Writer;
private:
  void writer_(AnyWriter &w, unsigned i) {
    auto field = m_fields[i];
    unsigned props = field ? field->props : ZtVFieldProp::Delta;
    w.init(m_series[i], props);
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
    return ZuFixed{
      static_cast<uint64_t>(t.sec()) * pow10_9() + t.nsec(), 9 };
  }
  ZuTime time(const ZuFixed &v) {
    uint64_t n = v.adjust(9);
    uint64_t p = pow10_9();
    return ZuTime{int64_t(n / p), int32_t(n % p)} + m_epoch;
  }

private:
  void load(Store_::LoadFn fn);
  bool load_(ZuBytes data);

  void save(Store_::SaveFn fn);
  Zfb::Offset<fbs::DataFrame> save_(Zfb::Builder &);

private:
  Mgr				*m_mgr = nullptr;
  ZtString			m_name;
  ZtArray<ZuPtr<Series>>	m_series;
  ZtArray<const ZtVField *>	m_fields;
  ZmRef<ZdbTable<Hdr>>		m_hdrTable;
  ZmRef<ZdbTable<Blk>>		m_blkTable;
  ZuTime			m_epoch;
  // async open/close series context
  using Callback = ZuUnion<void, OpenFn, CloseFn>;
  ZmPLock			m_lock;
    unsigned			  m_pending = 0;// number pending
    ZuPtr<Event>		  m_error;	// first error encountered
    Callback			  m_callback;	// completion callback
};

} // namespace Zdf

#endif /* Zdf_HH */
