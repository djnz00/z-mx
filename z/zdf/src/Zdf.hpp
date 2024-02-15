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

// Data Frame

#ifndef Zdf_HPP
#define Zdf_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdfLib_HPP
#include <zlib/ZdfLib.hpp>
#endif

#include <zlib/ZuPtr.hpp>
#include <zlib/ZuUnion.hpp>

#include <zlib/ZtArray.hpp>
#include <zlib/ZtString.hpp>

#include <zlib/Zfb.hpp>

#include <zlib/ZtField.hpp>

#include <zlib/ZdfCompress.hpp>
#include <zlib/ZdfSeries.hpp>

#include <zlib/dataframe_fbs.h>

namespace Zdf {

// Data d;
// ...
// DataFrame df{Data::fields(), "d"};
// ...
// auto w = df.writer();
// ZmTime time{ZmTime::Now};
// w.write(&d);
// ...
// AnyReader index, reader;
// ...
// df.find(index, 0, df.nsecs(time));	// index time to offset
// df.seek(reader, N, index.offset());	// seek reader to offset
// ...
// ZuFixed nsecs, value;
// index.read(nsecs);
// ZmTime then = df.time(nsecs);
// reader.read(value);

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

  // series must be monotonically increasing

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

  void purge() { dispatch([](auto &&v) { v.purge(); }); }

private:
  template <typename Reader>
  void init_(const Series *s, unsigned offset) {
    new (AnyReader_::init<Reader>())
      Reader{s->seek<typename Reader::Decoder>(offset)};
    initFn_<Reader>();
  }
  template <typename Reader>
  void find_(const Series *s, const ZuFixed &value) {
    new (AnyReader_::init<Reader>())
      Reader{s->find<typename Reader::Decoder>(value)};
    initFn_<Reader>();
  }

  void initFn() {
    dispatch([this](auto &&v) { initFn_<ZuDecay<decltype(v)>>(); });
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
    new (AnyWriter_::init<Writer>())
      Writer{s->writer<typename Writer::Encoder>()};
    initFn_<Writer>();
  }

  void initFn() {
    dispatch([this](auto &&v) { initFn_<ZuDecay<decltype(v)>>(); });
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

template <typename Field> struct FieldFilter :
    public ZuTypeIn<ZtFieldProp::Series, typename Field::Props> { };

template <typename T>
auto fields() {
  using Fields = ZuTypeGrep<FieldFilter, ZuFieldList<T>>;
  return ZtVFields_<Fields>();
}

class ZdfAPI DataFrame {
  DataFrame() = delete;
  DataFrame(const DataFrame &) = delete;
  DataFrame &operator =(const DataFrame &) = delete;
  DataFrame(DataFrame &&) = delete;
  DataFrame &operator =(DataFrame &&) = delete;
public:
  ~DataFrame() = default;

  DataFrame(ZtVFieldArray fields, ZuString name, bool timeIndex = false);

  const ZtString &name() const { return m_name; }
  const ZmTime &epoch() const { return m_epoch; }

  void init(Mgr *mgr);

  bool open(ZeError *e = nullptr);
  bool close(ZeError *e = nullptr);

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
	auto field = m_df->field(i);
	if (i || field) {
	  switch (field->type) {
	    case ZtFieldType::Int:
	    case ZtFieldType::Enum:
	      v = {field->getFn.int_(ptr), 0};
	      break;
	    case ZtFieldType::Fixed:
	      v = field->getFn.fixed(ptr);
	      break;
	    case ZtFieldType::Time:
	      v = m_df->nsecs(field->getFn.time(ptr));
	      break;
	    default:
	      v = ZuFixed{0, 0};
	      break;
	  }
	} else
	  v = m_df->nsecs(ZmTimeNow());
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
  constexpr static const uint64_t pow10_9() { return 1000000000UL; }
public:
  ZuFixed nsecs(ZmTime t) {
    t -= m_epoch;
    return ZuFixed{
      static_cast<uint64_t>(t.sec()) * pow10_9() + t.nsec(), 9 };
  }
  ZmTime time(const ZuFixed &v) {
    uint64_t n = v.adjust(9);
    uint64_t p = pow10_9();
    return ZmTime{static_cast<time_t>(n / p), static_cast<long>(n % p)} +
      m_epoch;
  }

private:
  bool load(ZeError *e = nullptr);
  bool load_(const uint8_t *buf, unsigned len);

  bool save(ZeError *e = nullptr);
  Zfb::Offset<fbs::DataFrame> save_(Zfb::Builder &);

private:
  ZtString			m_name;
  ZtArray<ZuPtr<Series>>	m_series;
  ZtArray<const ZtVField *>	m_fields;
  Mgr				*m_mgr = nullptr;
  ZmTime			m_epoch;
};

} // namespace Zdf

#endif /* Zdf_HPP */
