//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#ifndef ZdbPQ_HH
#define ZdbPQ_HH

#ifndef ZdbLib_HH
#include <zlib/ZdbPQLib.hh>
#endif

#include <libpq-fe.h>

#include <zlib/ZuArrayN.hh>
#include <zlib/ZuString.hh>
#include <zlib/ZuBytes.hh>
#include <zlib/ZuInt.hh>
#include <zlib/ZuDecimal.hh>
#include <zlib/ZuFixed.hh>
#include <zlib/ZuTime.hh>
#include <zlib/ZuDateTime.hh>
#include <zlib/ZuID.hh>

#include <zlib/ZeAssert.hh>

#include <zlib/ZiIP.hh>

#include <zlib/ZdbStore.hh>

namespace ZdbPQ {

class Store;
class StoreTbl;

using namespace Zdb_;

// Value     C++        flatbuffers     PG SQL        PG send/recv
// -----     ---        -----------     ------        ------------
// String    ZuString   string          text          raw data
// Bytes     ZuBytes    [uint8_t]       bytea         raw data
// Bool      bool       bool            bool          uint8_t
// Int8      int8_t     int8            int1     (*)  int8_t
// UInt8     uint8_t    uint8           uint1    (*)  uint8_t
// Int16     int16_t    int16           int2     (*)  int16_t BE
// UInt16    uint16_t   uint16          uint2    (*)  uint16_t BE
// Int32     int32_t    int32           int4          int32_t BE
// UInt32    uint32_t   uint32          uint4    (*)  uint32_t BE
// Int64     int64_t    int64           int8          int64_t BE
// UInt64    uint64_t   uint64          uint8    (*)  uint64_t BE
// Float     double     double          float8        double BE
// Fixed     ZuFixed    Zfb.Fixed       zdecimal (**) int128_t BE
// Decimal   ZuDecimal  Zfb.Decimal     zdecimal (**) int128_t BE
// Time      ZuTime     Zfb.Time        ztime    (**) int64_t BE, int32_t BE
// DateTime  ZuDateTime Zfb.DateTime    ztime    (**) int64_t BE, int32_t BE
// Int128    int128_t   Zfb.Int128      int16    (*)  int128_t BE
// UInt128   uint128_t  Zfb.UInt128     uint16   (*)  uint128_t BE
// Bitmap    ZtBitmap   Zfb.Bitmap      zbitmap  (**) uint64_t BE, uint64_t[] BE
// IP        ZiIP       Zfb.IP          inet          IPHdr, ZuIP
// ID        ZuID       Zfb.ID          text          raw data
//
// <Type>Vec            [<Type>]        type[]        array (see below)

// Postgres array binary format:
// int32_t BE	number of dimensions
// int32_t BE	flags (0 for no nulls, 1 for possible nulls)
// int32_t BE	oid (OID of elements)
// [{int32_t BE length, int32_t BE lower bound}...] dimensions (lb is 1-based)
// [{int32_t BE length, data...}...] elements (-1 length for null)
// Note that elements are appended without padding/alignment

// (*)   Postgres uint extension https://github.com/djnz00/pguint
// (**)  libz Postgres extension

// --- extended field information

struct XField {
  ZtString			id_;		// snake case field ID
  const reflection::Field	*field;		// flatbuffers reflection field
  unsigned			type;		// Value union discriminator
};
using XFields = ZtArray<XField>;
using XKeyFields = ZtArray<XFields>;

// --- value union (postgres binary send/receive formats)

#pragma pack(push, 1)
// 1-dimensional array - postgres binary send/receive format
struct VecHdr {
  ZuBigEndian<int32_t>	ndim;	// = 1
  ZuBigEndian<int32_t>	flags;	// = 0
  ZuBigEndian<int32_t>	oid;	// OID of elements
  ZuBigEndian<int32_t>	length;	// = N
  ZuBigEndian<int32_t>	lbound;	// = 1 (SQL, sigh)
};
struct VecElem {
  ZuBigEndian<int32_t>	length;	// size of element
};
#pragma pack(pop)
// return total size of an array buffer (fixed-size elements)
inline unsigned vecSize(unsigned n, unsigned size) {
  return sizeof(VecHdr) + n * (sizeof(VecElem) + size);
}
// return total size of an array buffer (variable-size elements)
template <typename L>
inline unsigned vecVarSize(unsigned n, L l) {
  unsigned size = 0;
  for (unsigned i = 0; i < n; i++) size += l(i);
  return sizeof(VecHdr) + n * sizeof(VecElem) + size;
}
// initialize array header
inline void vecInit(ZuArray<uint8_t> &buf, unsigned oid, unsigned n) {
  new (buf.data()) VecHdr{1, 0, int32_t(oid), int32_t(n), 1};
  buf.offset(sizeof(VecHdr));
}
// append array element
template <typename L>
inline void vecAppend(ZuArray<uint8_t> &buf, unsigned length, L l) {
  new (buf.data()) VecElem{length};
  buf.offset(sizeof(VecElem));
  l(buf.data(), length);
  buf.offset(length);
}
// read array header (advances buf)
inline const VecHdr *vecHdr(ZuArray<const uint8_t> &buf) {
  auto hdr = reinterpret_cast<const VecHdr *>(buf.data());
  buf.offset(sizeof(VecHdr));
  return hdr;
}
// validate received array header
inline bool validateVecHdr(const VecHdr *hdr)
{
  ZeAssert(int32_t(hdr->ndim) == 1,
    (i = int32_t(hdr->ndim)), "ndim=" << i, return false);
  ZeAssert(int32_t(hdr->lbound) == 1,
    (i = int32_t(hdr->lbound)), "lbound=" << i, return false);
  return true;
}
// read array element (advances buf)
template <typename L>
inline auto vecElem(ZuArray<const uint8_t> &buf, L l) {
  auto elem = reinterpret_cast<const VecElem *>(buf.data());
  unsigned length = elem->length;
  buf.offset(sizeof(VecElem));
  auto ptr = buf.data();
  buf.offset(length);
  return l(ptr, length);
}

using String = ZuString;
using Bytes = ZuBytes;
struct Bitmap { ZuBytes v; };
#pragma pack(push, 1)
struct Bool { uint8_t v; };
struct Int8 { int8_t v; };
struct UInt8 { uint8_t v; };
struct Int16 { ZuBigEndian<int16_t> v; };
struct UInt16 { ZuBigEndian<uint16_t> v; };
struct Int32 { ZuBigEndian<int32_t> v; };
struct UInt32 { ZuBigEndian<uint32_t> v; };
struct Int64 { ZuBigEndian<int64_t> v; };
struct UInt64 { ZuBigEndian<uint64_t> v; };
struct Float { ZuBigEndian<double> v; };
struct Fixed { ZuBigEndian<int128_t> v; };
struct Decimal { ZuBigEndian<int128_t> v; };
struct Time { ZuBigEndian<int64_t> sec; ZuBigEndian<int32_t> nsec; };
struct DateTime { ZuBigEndian<int64_t> sec; ZuBigEndian<int32_t> nsec; };
struct Int128 { ZuBigEndian<int128_t> v; };
struct UInt128 { ZuBigEndian<uint128_t> v; };
struct IPHdr {
  uint8_t	family = 2;	// AF_INET
  uint8_t	bits = 32;
  uint8_t	is_cidr = 0;
  uint8_t	len = 4;
};
struct IP { IPHdr hdr; ZiIP addr; };
struct ID { ZuID id; };
#pragma pack(pop)
// these must all be distinct types
struct StringVec { ZuBytes v; };
struct BytesVec { ZuBytes v; };
struct Int8Vec { ZuBytes v; };
struct UInt8Vec { ZuBytes v; };
struct Int16Vec { ZuBytes v; };
struct UInt16Vec { ZuBytes v; };
struct Int32Vec { ZuBytes v; };
struct UInt32Vec { ZuBytes v; };
struct Int64Vec { ZuBytes v; };
struct UInt64Vec { ZuBytes v; };
struct Int128Vec { ZuBytes v; };
struct UInt128Vec { ZuBytes v; };
struct FloatVec { ZuBytes v; };
struct FixedVec { ZuBytes v; };
struct DecimalVec { ZuBytes v; };
struct TimeVec { ZuBytes v; };
struct DateTimeVec { ZuBytes v; };

template <typename U> struct Elem_ { using T = void; };
template <> struct Elem_<StringVec> { using T = String; };
template <> struct Elem_<BytesVec> { using T = Bytes; };
template <> struct Elem_<Int8Vec> { using T = Int8; };
template <> struct Elem_<UInt8Vec> { using T = UInt8; };
template <> struct Elem_<Int16Vec> { using T = Int16; };
template <> struct Elem_<UInt16Vec> { using T = UInt16; };
template <> struct Elem_<Int32Vec> { using T = Int32; };
template <> struct Elem_<UInt32Vec> { using T = UInt32; };
template <> struct Elem_<Int64Vec> { using T = Int64; };
template <> struct Elem_<UInt64Vec> { using T = UInt64; };
template <> struct Elem_<Int128Vec> { using T = Int128; };
template <> struct Elem_<UInt128Vec> { using T = UInt128; };
template <> struct Elem_<FloatVec> { using T = Float; };
template <> struct Elem_<FixedVec> { using T = Fixed; };
template <> struct Elem_<DecimalVec> { using T = Decimal; };
template <> struct Elem_<TimeVec> { using T = Time; };
template <> struct Elem_<DateTimeVec> { using T = DateTime; };
template <typename U> using Elem = typename Elem_<U>::T;

using Value_ = ZuUnion<
  void,
  String,
  Bytes,
  Bool,
  Int8,
  UInt8,
  Int16,
  UInt16,
  Int32,
  UInt32,
  Int64,
  UInt64,
  Float,
  Fixed,
  Decimal,
  Time,
  DateTime,
  Int128,
  UInt128,
  Bitmap,
  IP,
  ID,

  // all types after this are vectors, see isVec() below
  StringVec,
  BytesVec,
  Int8Vec,
  UInt8Vec,
  Int16Vec,
  UInt16Vec,
  Int32Vec,
  UInt32Vec,
  Int64Vec,
  UInt64Vec,
  Int128Vec,
  UInt128Vec,
  FloatVec,
  FixedVec,
  DecimalVec,
  TimeVec,
  DateTimeVec>;

enum { VecBase = Value_::Index<StringVec>{} };

// isVec() - is a vector/array
inline constexpr bool isVec(unsigned i) { return i >= VecBase; }
// isVar() - is variable-length
inline constexpr bool isVar(unsigned i) {
  // * Bytes and String are excluded because they can be zero-copied
  //   - PG -> flatbuffers - the PGresult remains in scope
  //   - flatbuffers -> PG - the flatbuffer remains in scope
  // * Bitmap needs copying and byte-swapping (sigh)
  // * All vectors/arrays need copying and transforming into PG binary format
  //   when transforming from flatbuffers to PG (they are zero-copy in
  //   the other direction)
  return i == Value_::Index<Bitmap>{} || isVec(i);
}

struct Value : public Value_ {
  using Value_::Value_;
  using Value_::operator =;
  template <typename ...Args>
  Value(Args &&...args) : Value_{ZuFwd<Args>(args)...} { }

  // Postgres binary format - load from PGresult

  // void
  template <unsigned I, typename T = Value_::Type<I>>
  ZuExact<void, T, bool>
  load(const char *, unsigned) { type_(I); return true; }

  // String - zero-copy - relies on the PGresult remaining in scope
  template <unsigned I, typename T = Value_::Type<I>>
  ZuExact<String, T, bool>
  load(const char *data, unsigned length) {
    new (new_<I, true>()) T{data, length};
    return true;
  }

  // Bytes - zero-copy - relies on the PGresult remaining in scope
  template <unsigned I, typename T = Value_::Type<I>>
  ZuExact<Bytes, T, bool>
  load(const char *data, unsigned length) {
    new (new_<I, true>()) T{reinterpret_cast<const uint8_t *>(data), length};
    return true;
  }

  // Vectors - zero-copy - relies on the PGresult remaining in scope
  template <unsigned I, typename T = Value_::Type<I>>
  ZuIfT<isVar(I), bool>
  load(const char *data, unsigned length) {
    new (new_<I, true>()) T{{reinterpret_cast<const uint8_t *>(data), length}};
    return true;
  }

  // All other types - memcpy
#if defined(__GNUC__) && !defined(__llvm__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
  template <unsigned I, typename T = Value_::Type<I>>
  ZuIfT<
    !ZuIsExact<void, T>{} &&
    !ZuIsExact<String, T>{} &&
    !ZuIsExact<Bytes, T>{} &&
    !isVar(I), bool>
  load(const char *data, unsigned length) {
    if (length != sizeof(T)) return false;
    memcpy(new_<I, true>(), data, length);
    return true;
  }
#if defined(__GNUC__) && !defined(__llvm__)
#pragma GCC diagnostic pop
#endif

  // Postgres binary format - save to params - data<I>(), length<I>()

  // void - return {nullptr, 0}
  template <unsigned I, typename T = Value_::Type<I>>
  ZuExact<void, T, const char *>
  data() const { return nullptr; }
  template <unsigned I, typename T = Value_::Type<I>>
  ZuExact<void, T, unsigned>
  length() const { return 0; }

  // String - return raw string data
  template <unsigned I, typename T = Value_::Type<I>>
  ZuExact<String, T, const char *>
  data() const { return p<T>().data(); }
  template <unsigned I, typename T = Value_::Type<I>>
  ZuExact<String, T, unsigned>
  length() const { return p<T>().length(); }

  // Bytes - return raw byte data
  template <unsigned I, typename T = Value_::Type<I>>
  ZuExact<Bytes, T, const char *>
  data() const {
    return reinterpret_cast<const char *>(p<T>().data());
  }
  template <unsigned I, typename T = Value_::Type<I>>
  ZuExact<Bytes, T, unsigned>
  length() const { return p<T>().length(); }

  // variable-sized - return raw byte data
  template <unsigned I, typename T = Value_::Type<I>>
  ZuIfT<isVar(I), const char *> data() const {
    return reinterpret_cast<const char *>(p<T>().v.data());
  }
  template <unsigned I, typename T = Value_::Type<I>>
  ZuIfT<isVar(I), unsigned> length() const {
    return p<T>().v.length();
  }

  // All other types - return bigendian packed struct
  template <unsigned I, typename T = Value_::Type<I>>
  ZuIfT<
    !ZuIsExact<void, T>{} &&
    !ZuIsExact<String, T>{} &&
    !ZuIsExact<Bytes, T>{} &&
    !isVar(I), const char *>
  data() const { return reinterpret_cast<const char *>(this); }
  template <unsigned I, typename T = Value_::Type<I>>
  ZuIfT<
    !ZuIsExact<void, T>{} &&
    !ZuIsExact<String, T>{} &&
    !ZuIsExact<Bytes, T>{} &&
    !isVar(I), unsigned>
  length() const { return sizeof(T); }

  // print value
  template <unsigned I, typename S>
  ZuIfT<I == Value_::Index<void>{}>
  print_(S &s) const { }

  template <unsigned I, typename S>
  ZuIfT<I == Value_::Index<String>{}>
  print_(S &s) const { s << ZtField_::Print::String{p<I>()}; }

  template <unsigned I, typename S>
  ZuIfT<I == Value_::Index<Bytes>{}>
  print_(S &s) const { s << ZtField_::Print::Bytes{p<I>()}; }

  template <unsigned I, typename S>
  ZuIfT<
    I == Value_::Index<Bool>{} ||
    I == Value_::Index<Int8>{} ||
    I == Value_::Index<UInt8>{} ||
    I == Value_::Index<Int16>{} ||
    I == Value_::Index<UInt16>{} ||
    I == Value_::Index<Int32>{} ||
    I == Value_::Index<UInt32>{} ||
    I == Value_::Index<Int64>{} ||
    I == Value_::Index<UInt64>{} ||
    I == Value_::Index<Int128>{} ||
    I == Value_::Index<UInt128>{} ||
    I == Value_::Index<Float>{}>
  print_(S &s) const { s << ZuBoxed(ZuUnderlying(p<I>().v)); }

  template <unsigned I, typename S>
  ZuIfT<
    I == Value_::Index<Fixed>{} ||
    I == Value_::Index<Decimal>{}>
  print_(S &s) const {
    s << ZuDecimal{ZuDecimal::Unscaled{ZuUnderlying(p<I>().v)}};
  }

  template <unsigned I, typename S>
  ZuIfT<
    I == Value_::Index<Time>{} ||
    I == Value_::Index<DateTime>{}>
  print_(S &s) const { s << ZuTime{p<I>().sec, p<I>().nsec}; }

  template <unsigned I, typename S>
  ZuIfT<I == Value_::Index<Bitmap>{}>
  print_(S &s) const {
    using Word = ZuBigEndian<uint64_t>;
    const auto &data_ = p<I>().v;
    ZuArray<const Word> data{
      reinterpret_cast<const Word *>(&data_[0]),
      data_.length() / sizeof(uint64_t)};
    ZtBitmap b;
    unsigned n = data.length() - 1;
    b.data.length(n);
    for (unsigned i = 0; i < n; i++) b.data[i] = data[i + 1];
    s << b;
  }

  template <unsigned I, typename S>
  ZuIfT<I == Value_::Index<IP>{}>
  print_(S &s) const { s << p<I>().addr; }

  template <unsigned I, typename S>
  ZuIfT<I == Value_::Index<ID>{}>
  print_(S &s) const { s << p<I>().id; }

  template <unsigned I, typename S>
  ZuIfT<I == Value_::Index<StringVec>{}>
  print_(S &s) const {
    auto varBuf = p<I>().v;
    auto hdr = vecHdr(varBuf);
    unsigned n = int32_t(hdr->length);
    s << '[';
    for (unsigned i = 0; i < n; i++)
      if (i) s << ',';
      vecElem(varBuf, [&s](const uint8_t *ptr, unsigned length) {
	s << ZtField_::Print::String{ZuString{ptr, length}};
      });
    s << ']';
  }

  template <unsigned I, typename S>
  ZuIfT<I == Value_::Index<BytesVec>{}>
  print_(S &s) const {
    auto varBuf = p<I>().v;
    auto hdr = vecHdr(varBuf);
    unsigned n = int32_t(hdr->length);
    s << '[';
    for (unsigned i = 0; i < n; i++)
      if (i) s << ',';
      vecElem(varBuf, [&s](const uint8_t *ptr, unsigned length) {
	s << ZtField_::Print::Bytes{ZuBytes{ptr, length}};
      });
    s << ']';
  }

  template <unsigned I, typename S>
  ZuIfT<
    I == Value_::Index<Int8Vec>{} ||
    I == Value_::Index<UInt8Vec>{} ||
    I == Value_::Index<Int16Vec>{} ||
    I == Value_::Index<UInt16Vec>{} ||
    I == Value_::Index<Int32Vec>{} ||
    I == Value_::Index<UInt32Vec>{} ||
    I == Value_::Index<Int64Vec>{} ||
    I == Value_::Index<UInt64Vec>{} ||
    I == Value_::Index<Int128Vec>{} ||
    I == Value_::Index<UInt128Vec>{} ||
    I == Value_::Index<FloatVec>{}>
  print_(S &s) const {
    using Elem = ZdbPQ::Elem<Value_::Type<I>>;
    auto varBuf = p<I>().v;
    auto hdr = vecHdr(varBuf);
    unsigned n = int32_t(hdr->length);
    s << '[';
    for (unsigned i = 0; i < n; i++)
      if (i) s << ',';
      vecElem(varBuf, [&s](const uint8_t *ptr, unsigned) {
	s << ZuBoxed(ZuUnderlying(reinterpret_cast<const Elem *>(ptr)->v));
      });
    s << ']';
  }

  template <unsigned I, typename S>
  ZuIfT<
    I == Value_::Index<FixedVec>{} ||
    I == Value_::Index<DecimalVec>{}>
  print_(S &s) const {
    using Elem = ZdbPQ::Elem<Value_::Type<I>>;
    auto varBuf = p<I>().v;
    auto hdr = vecHdr(varBuf);
    unsigned n = int32_t(hdr->length);
    s << '[';
    for (unsigned i = 0; i < n; i++)
      if (i) s << ',';
      vecElem(varBuf, [&s](const uint8_t *ptr, unsigned) {
	s << ZuDecimal{ZuDecimal::Unscaled{
	  reinterpret_cast<const Elem *>(ptr)->v}};
      });
    s << ']';
  }

  template <unsigned I, typename S>
  ZuIfT<
    I == Value_::Index<TimeVec>{} ||
    I == Value_::Index<DateTimeVec>{}>
  print_(S &s) const {
    using Elem = ZdbPQ::Elem<Value_::Type<I>>;
    auto varBuf = p<I>().v;
    auto hdr = vecHdr(varBuf);
    unsigned n = int32_t(hdr->length);
    s << '[';
    for (unsigned i = 0; i < n; i++)
      if (i) s << ',';
      vecElem(varBuf, [&s](const uint8_t *ptr, unsigned) {
	const auto &v = reinterpret_cast<const Elem *>(ptr)->v;
	s << ZuTime{v.sec, v.nsec};
      });
    s << ']';
  }

  template <typename S>
  void print(S &s) const {
    ZuSwitch::dispatch<Value_::N>(this->type(), [this, &s](auto I) {
      print_<I>(s);
    });
  }
  friend ZuPrintFn ZuPrintType(Value *);
};

// --- vector buffer

// VarBuf contains the entire Postgres array in native binary format
using VarBuf = ZtArray<uint8_t>;
// VarBufPart references the VarBuf fragment for an individual element
using VarBufPart = ZuTuple<unsigned, unsigned>;	// offset, length
// VarBufParts tracks VarBuf fragments for multiple elements
using VarBufParts = ZtArray<VarBufPart>;

// varBufSize() calculates the size of a variable-sized type
template <unsigned Type>
inline ZuIfT<!isVar(Type), unsigned>
varBufSize(const reflection::Field *, const Zfb::Table *) { return 0; }

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Bitmap>{}, unsigned>
varBufSize(const reflection::Field *field, const Zfb::Table *fbo) {
  auto bitmap = fbo->GetPointer<const Zfb::Bitmap *>(field->offset());
  if (!bitmap || !bitmap->data()) return sizeof(uint64_t);
  return (bitmap->data()->size() + 1) * sizeof(uint64_t);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<StringVec>{}, unsigned>
varBufSize(const reflection::Field *field, const Zfb::Table *fbo) {
  auto v = Zfb::GetFieldV<Zfb::Offset<Zfb::String>>(*fbo, *field);
  return vecVarSize(v->size(), [&v](unsigned i) -> unsigned {
    return v->Get(i)->size();
  });
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<BytesVec>{}, unsigned>
varBufSize(const reflection::Field *field, const Zfb::Table *fbo) {
  auto v = Zfb::GetFieldV<Zfb::Offset<Zfb::Vector<uint8_t>>>(*fbo, *field);
  return vecVarSize(v->size(), [&v](unsigned i) -> unsigned {
    return v->Get(i)->size();
  });
}

#define ZdbPQ_IntVarBufSize(width) \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<Int##width##Vec>{}, unsigned> \
varBufSize(const reflection::Field *field, const Zfb::Table *fbo) { \
  return vecSize(Zfb::GetFieldV<int##width##_t>( \
      *fbo, *field)->size(), sizeof(Int##width)); \
} \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<UInt##width##Vec>{}, unsigned> \
varBufSize(const reflection::Field *field, const Zfb::Table *fbo) { \
  using namespace Zfb; \
  return vecSize(Zfb::GetFieldV<uint##width##_t>( \
      *fbo, *field)->size(), sizeof(UInt##width)); \
}

ZdbPQ_IntVarBufSize(8)
ZdbPQ_IntVarBufSize(16)
ZdbPQ_IntVarBufSize(32)
ZdbPQ_IntVarBufSize(64)

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Int128Vec>{}, unsigned>
varBufSize(const reflection::Field *field, const Zfb::Table *fbo) {
  return vecSize(Zfb::GetFieldV<Zfb::Int128 *>(
      *fbo, *field)->size(), sizeof(Int128));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<UInt128Vec>{}, unsigned>
varBufSize(const reflection::Field *field, const Zfb::Table *fbo) {
  return vecSize(Zfb::GetFieldV<Zfb::UInt128 *>(
      *fbo, *field)->size(), sizeof(UInt128));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<FloatVec>{}, unsigned>
varBufSize(const reflection::Field *field, const Zfb::Table *fbo) {
  return vecSize(Zfb::GetFieldV<double>(
      *fbo, *field)->size(), sizeof(Float));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<FixedVec>{}, unsigned>
varBufSize(const reflection::Field *field, const Zfb::Table *fbo) {
  return vecSize(Zfb::GetFieldV<Zfb::Fixed *>(
      *fbo, *field)->size(), sizeof(Fixed));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<DecimalVec>{}, unsigned>
varBufSize(const reflection::Field *field, const Zfb::Table *fbo) {
  return vecSize(Zfb::GetFieldV<Zfb::Decimal *>(
      *fbo, *field)->size(), sizeof(Decimal));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<TimeVec>{}, unsigned>
varBufSize(const reflection::Field *field, const Zfb::Table *fbo) {
  return vecSize(Zfb::GetFieldV<Zfb::Time *>(
      *fbo, *field)->size(), sizeof(Time));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<DateTimeVec>{}, unsigned>
varBufSize(const reflection::Field *field, const Zfb::Table *fbo) {
  return vecSize(Zfb::GetFieldV<Zfb::DateTime *>(
      *fbo, *field)->size(), sizeof(DateTime));
}

// --- postgres OIDs, type names

class OIDs {
  using OIDs_ = ZuArrayN<unsigned, Value::N - 1>;
  using Types = ZmLHashKV<unsigned, int8_t, ZmLHashStatic<6, ZmLHashLocal<>>>;
  using Lookup = ZmLHashKV<ZuString, int8_t, ZmLHashStatic<6, ZmLHashLocal<>>>;

public:
  OIDs();

  const char *name(unsigned i) const {
    if (i < 1 || i >= Value::N) return nullptr;
    return m_names[i - 1];
  }
  unsigned oid(unsigned i) const {
    if (i < 1 || i >= Value::N) return ZuCmp<unsigned>::null();
    return m_oids[i - 1];
  }
  bool match(unsigned oid, unsigned type) const {
    auto i = m_types.readIterator(oid);
    while (auto kv = i.iterate())
      if (kv->p<1>() == type) return true;
    return false;
  }
  unsigned oid(ZuString name) const {
    int8_t i = m_lookup.findVal(name);
    if (ZuCmp<int8_t>::null(i)) return ZuCmp<unsigned>::null();
    ZmAssert(i >= 1 && i < Value::N);
    return m_oids[i - 1];
  }

  void init(int8_t i, unsigned oid) {
    ZmAssert(i >= 1 && i < Value::N);
    m_oids[i - 1] = oid;
    m_types.add(unsigned(oid), i);
    m_lookup.add(m_names[i - 1], i);	// only add resolved names to lookup
  }

private:
  unsigned resolve(PGconn *conn, ZuString name);

  const char	**m_names = nullptr;
  OIDs_		m_oids;
  Types		m_types;
  Lookup	m_lookup;
};

// --- load value from flatbuffer (for sending to postgres)

template <unsigned Type>
inline ZuIfT<isVar(Type)> // unused
loadValue(void *, const reflection::Field *, const Zfb::Table *) { }

template <unsigned Type>
inline ZuIfT<Type == Value::Index<void>{}> // unused
loadValue(void *, const reflection::Field *, const Zfb::Table *) { }

template <unsigned Type>
inline ZuIfT<Type == Value::Index<String>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) String{Zfb::Load::str(Zfb::GetFieldS(*fbo, *field))};
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Bytes>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) Bytes{Zfb::Load::bytes(Zfb::GetFieldV<uint8_t>(*fbo, *field))};
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Bool>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) Bool{Zfb::GetFieldI<bool>(*fbo, *field)};
}

#define ZdbPQ_LoadInt(width) \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<Int##width>{}> \
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) { \
  new (ptr) Int##width{Zfb::GetFieldI<int##width##_t>(*fbo, *field)}; \
} \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<UInt##width>{}> \
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) { \
  new (ptr) UInt##width{Zfb::GetFieldI<uint##width##_t>(*fbo, *field)}; \
}

ZdbPQ_LoadInt(8)
ZdbPQ_LoadInt(16)
ZdbPQ_LoadInt(32)
ZdbPQ_LoadInt(64)

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Float>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) Float{Zfb::GetFieldF<double>(*fbo, *field)};
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Fixed>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  ZuDecimal d =
    Zfb::Load::fixed(fbo->GetPointer<const Zfb::Fixed *>(field->offset()));
  new (ptr) Fixed{d.value};
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Decimal>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  ZuDecimal d =
    Zfb::Load::decimal(fbo->GetPointer<const Zfb::Decimal *>(field->offset()));
  new (ptr) Decimal{d.value};
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Time>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  auto t = fbo->GetPointer<const Zfb::Time *>(field->offset());
  new (ptr) Time{t->sec(), t->nsec()};
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<DateTime>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  auto t =
    Zfb::Load::dateTime(
      fbo->GetPointer<const Zfb::DateTime *>(field->offset())).as_time();
  new (ptr) ZuDateTime{t.sec(), t.nsec()};
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Int128>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) Int128{
    Zfb::Load::int128(fbo->GetPointer<const Zfb::Int128 *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<UInt128>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) UInt128{
    Zfb::Load::uint128(fbo->GetPointer<const Zfb::UInt128 *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<IP>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) IP{
    IPHdr{},
    Zfb::Load::ip(fbo->GetPointer<const Zfb::IP *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ID>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ID{
    Zfb::Load::id(fbo->GetPointer<const Zfb::ID *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<!isVar(Type)> // unused
loadValue(
  void *, VarBuf &, const VarBufPart &,
  const OIDs &, const reflection::Field *, const Zfb::Table *) { }

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Bitmap>{}>
loadValue(
  void *ptr, VarBuf &varBuf_, const VarBufPart &varBufPart,
  const OIDs &, const reflection::Field *field, const Zfb::Table *fbo)
{
  ZuArray<uint8_t> varBuf(&varBuf_[varBufPart.p<0>()], varBufPart.p<1>());
  new (ptr) Bitmap{ZuBytes(varBuf)};
  using Word = ZuBigEndian<uint64_t>;
  ZuArray<Word> data(
    reinterpret_cast<Word *>(&varBuf[0]),
    varBuf.length() / sizeof(uint64_t));
  auto bitmap = fbo->GetPointer<const Zfb::Bitmap *>(field->offset());
  if (!bitmap || !bitmap->data()) { data[0] = 0; return; }
  auto vec = bitmap->data();
  unsigned n = vec->size();
  data[0] = n;
  for (unsigned i = 0; i < n; i++) data[i + 1] = vec->Get(i);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<StringVec>{}>
loadValue(
  void *ptr, VarBuf &varBuf_, const VarBufPart &varBufPart,
  const OIDs &oids, const reflection::Field *field, const Zfb::Table *fbo)
{
  ZuArray<uint8_t> varBuf(&varBuf_[varBufPart.p<0>()], varBufPart.p<1>());
  new (ptr) StringVec{ZuBytes(varBuf)};
  auto v = Zfb::GetFieldV<Zfb::Offset<Zfb::String>>(*fbo, *field);
  unsigned n = v->size();
  vecInit(varBuf, oids.oid(Value::Index<String>{}), n);
  for (unsigned i = 0; i < n; i++) {
    auto s = v->Get(i);
    vecAppend(varBuf, s->size(), [s](uint8_t *ptr, unsigned size) {
      memcpy(ptr, s->Data(), size);
    });
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<BytesVec>{}>
loadValue(
  void *ptr, VarBuf &varBuf_, const VarBufPart &varBufPart,
  const OIDs &oids, const reflection::Field *field, const Zfb::Table *fbo)
{
  ZuArray<uint8_t> varBuf(&varBuf_[varBufPart.p<0>()], varBufPart.p<1>());
  new (ptr) BytesVec{ZuBytes(varBuf)};
  auto v = Zfb::GetFieldV<Zfb::Offset<Zfb::Vector<uint8_t>>>(*fbo, *field);
  unsigned n = v->size();
  vecInit(varBuf, oids.oid(Value::Index<Bytes>{}), n);
  for (unsigned i = 0; i < n; i++) {
    auto b = v->Get(i);
    vecAppend(varBuf, b->size(), [b](uint8_t *ptr, unsigned size) {
      memcpy(ptr, b->Data(), size);
    });
  }
}

#define ZdbPQ_LoadIntVec(width) \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<Int##width##Vec>{}> \
loadValue( \
  void *ptr, VarBuf &varBuf_, const VarBufPart &varBufPart, \
  const OIDs &oids, const reflection::Field *field, const Zfb::Table *fbo) \
{ \
  ZuArray<uint8_t> varBuf(&varBuf_[varBufPart.p<0>()], varBufPart.p<1>()); \
  new (ptr) Int##width##Vec{ZuBytes(varBuf)}; \
  auto v = Zfb::GetFieldV<int##width##_t>(*fbo, *field); \
  unsigned n = v->size(); \
  vecInit(varBuf, oids.oid(Value::Index<Int##width>{}), n); \
  for (unsigned i = 0; i < n; i++) { \
    auto e = v->Get(i); \
    vecAppend(varBuf, sizeof(Int##width), [e](uint8_t *ptr, unsigned) { \
      new (ptr) Int##width{e}; \
    }); \
  } \
} \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<UInt##width##Vec>{}> \
loadValue( \
  void *ptr, VarBuf &varBuf_, const VarBufPart &varBufPart, \
  const OIDs &oids, const reflection::Field *field, const Zfb::Table *fbo) \
{ \
  ZuArray<uint8_t> varBuf(&varBuf_[varBufPart.p<0>()], varBufPart.p<1>()); \
  new (ptr) UInt##width##Vec{ZuBytes(varBuf)}; \
  auto v = Zfb::GetFieldV<uint##width##_t>(*fbo, *field); \
  unsigned n = v->size(); \
  vecInit(varBuf, oids.oid(Value::Index<UInt##width>{}), n); \
  for (unsigned i = 0; i < n; i++) { \
    auto e = v->Get(i); \
    vecAppend(varBuf, sizeof(UInt##width), [e](uint8_t *ptr, unsigned) { \
      new (ptr) UInt##width{e}; \
    }); \
  } \
}

ZdbPQ_LoadIntVec(8)
ZdbPQ_LoadIntVec(16)
ZdbPQ_LoadIntVec(32)
ZdbPQ_LoadIntVec(64)

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Int128Vec>{}>
loadValue(
  void *ptr, VarBuf &varBuf_, const VarBufPart &varBufPart,
  const OIDs &oids, const reflection::Field *field, const Zfb::Table *fbo)
{
  ZuArray<uint8_t> varBuf(&varBuf_[varBufPart.p<0>()], varBufPart.p<1>());
  new (ptr) Int128Vec{ZuBytes(varBuf)};
  auto v = Zfb::GetFieldV<Zfb::Int128 *>(*fbo, *field);
  unsigned n = v->size();
  vecInit(varBuf, oids.oid(Value::Index<Int128>{}), n);
  for (unsigned i = 0; i < n; i++) {
    auto e = v->Get(i);
    vecAppend(varBuf, sizeof(Int128), [e](uint8_t *ptr, unsigned) {
      new (ptr) Int128{Zfb::Load::int128(e)};
    });
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<UInt128Vec>{}>
loadValue(
  void *ptr, VarBuf &varBuf_, const VarBufPart &varBufPart,
  const OIDs &oids, const reflection::Field *field, const Zfb::Table *fbo)
{
  ZuArray<uint8_t> varBuf(&varBuf_[varBufPart.p<0>()], varBufPart.p<1>());
  new (ptr) UInt128Vec{ZuBytes(varBuf)};
  auto v = Zfb::GetFieldV<Zfb::UInt128 *>(*fbo, *field);
  unsigned n = v->size();
  vecInit(varBuf, oids.oid(Value::Index<UInt128>{}), n);
  for (unsigned i = 0; i < n; i++) {
    auto e = v->Get(i);
    vecAppend(varBuf, sizeof(UInt128), [e](uint8_t *ptr, unsigned) {
      new (ptr) UInt128{Zfb::Load::uint128(e)};
    });
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<FloatVec>{}>
loadValue(
  void *ptr, VarBuf &varBuf_, const VarBufPart &varBufPart,
  const OIDs &oids, const reflection::Field *field, const Zfb::Table *fbo)
{
  ZuArray<uint8_t> varBuf(&varBuf_[varBufPart.p<0>()], varBufPart.p<1>());
  new (ptr) FloatVec{ZuBytes(varBuf)};
  auto v = Zfb::GetFieldV<double>(*fbo, *field);
  unsigned n = v->size();
  vecInit(varBuf, oids.oid(Value::Index<Float>{}), n);
  for (unsigned i = 0; i < n; i++) {
    auto e = v->Get(i);
    vecAppend(varBuf, sizeof(double), [e](uint8_t *ptr, unsigned) {
      new (ptr) Float{e};
    });
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<FixedVec>{}>
loadValue(
  void *ptr, VarBuf &varBuf_, const VarBufPart &varBufPart,
  const OIDs &oids, const reflection::Field *field, const Zfb::Table *fbo)
{
  ZuArray<uint8_t> varBuf(&varBuf_[varBufPart.p<0>()], varBufPart.p<1>());
  new (ptr) FixedVec{ZuBytes(varBuf)};
  auto v = Zfb::GetFieldV<Zfb::Fixed *>(*fbo, *field);
  unsigned n = v->size();
  vecInit(varBuf, oids.oid(Value::Index<Fixed>{}), n);
  for (unsigned i = 0; i < n; i++) {
    auto e = v->Get(i);
    vecAppend(varBuf, sizeof(Fixed), [e](uint8_t *ptr, unsigned) {
      new (ptr) Fixed{ZuDecimal{Zfb::Load::fixed(e)}.value};
    });
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<DecimalVec>{}>
loadValue(
  void *ptr, VarBuf &varBuf_, const VarBufPart &varBufPart,
  const OIDs &oids, const reflection::Field *field, const Zfb::Table *fbo)
{
  ZuArray<uint8_t> varBuf(&varBuf_[varBufPart.p<0>()], varBufPart.p<1>());
  new (ptr) DecimalVec{ZuBytes(varBuf)};
  auto v = Zfb::GetFieldV<Zfb::Decimal *>(*fbo, *field);
  unsigned n = v->size();
  vecInit(varBuf, oids.oid(Value::Index<Decimal>{}), n);
  for (unsigned i = 0; i < n; i++) {
    auto e = v->Get(i);
    vecAppend(varBuf, sizeof(Decimal), [e](uint8_t *ptr, unsigned) {
      new (ptr) Decimal{Zfb::Load::decimal(e).value};
    });
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<TimeVec>{}>
loadValue(
  void *ptr, VarBuf &varBuf_, const VarBufPart &varBufPart,
  const OIDs &oids, const reflection::Field *field, const Zfb::Table *fbo)
{
  ZuArray<uint8_t> varBuf(&varBuf_[varBufPart.p<0>()], varBufPart.p<1>());
  new (ptr) TimeVec{ZuBytes(varBuf)};
  auto v = Zfb::GetFieldV<Zfb::Time *>(*fbo, *field);
  unsigned n = v->size();
  vecInit(varBuf, oids.oid(Value::Index<Time>{}), n);
  for (unsigned i = 0; i < n; i++) {
    auto e = v->Get(i);
    vecAppend(varBuf, sizeof(Time), [e](uint8_t *ptr, unsigned) {
      new (ptr) Time{e->sec(), e->nsec()};
    });
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<DateTimeVec>{}>
loadValue(
  void *ptr, VarBuf &varBuf_, const VarBufPart &varBufPart,
  const OIDs &oids, const reflection::Field *field, const Zfb::Table *fbo)
{
  ZuArray<uint8_t> varBuf(&varBuf_[varBufPart.p<0>()], varBufPart.p<1>());
  new (ptr) DateTimeVec{ZuBytes(varBuf)};
  auto v = Zfb::GetFieldV<Zfb::DateTime *>(*fbo, *field);
  unsigned n = v->size();
  vecInit(varBuf, oids.oid(Value::Index<DateTime>{}), n);
  for (unsigned i = 0; i < n; i++) {
    auto e = v->Get(i);
    vecAppend(varBuf, sizeof(DateTime), [e](uint8_t *ptr, unsigned) {
      auto t = Zfb::Load::dateTime(e).as_time();
      new (ptr) DateTime{t.sec(), t.nsec()};
    });
  }
}

// --- save value to flatbuffer (for passing to Zdb/app)

using Offset = Zfb::Offset<void>;

struct Offsets {
  ZmAlloc_<Offset>	data;
  unsigned		in = 0;
  mutable unsigned	out = 0;

  void push(Offset o) { data[in++] = o; }
  Offset shift() const { return data[out++]; }
};

template <unsigned Type>
inline ZuIfT<Type == Value::Index<String>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  offsets.push(Zfb::Save::str(fbb, value.p<Type>()).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Bytes>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  offsets.push(Zfb::Save::bytes(fbb, value.p<Type>()).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Bitmap>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  using Word = ZuBigEndian<uint64_t>;
  const auto &data_ = value.p<Type>().v;
  ZuArray<const Word> data(
    reinterpret_cast<const Word *>(&data_[0]),
    data_.length() / sizeof(uint64_t));
  unsigned n = data.length() - 1;
  offsets.push(
    Zfb::CreateBitmap(fbb, Zfb::Save::pvectorIter<uint64_t>(
	fbb, n, [&data](unsigned i) { return data[i + 1]; })).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<StringVec>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  auto varBuf = value.p<StringVec>().v;
  auto hdr = vecHdr(varBuf);
  if (!validateVecHdr(hdr)) return;
  unsigned n = int32_t(hdr->length);
  offsets.push(
    Zfb::Save::strVecIter(fbb, n, [&varBuf](unsigned) {
      return vecElem(varBuf, [](const uint8_t *ptr, unsigned length) {
	return ZuString{ptr, length};
      });
    }).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<BytesVec>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  auto varBuf = value.p<BytesVec>().v;
  auto hdr = vecHdr(varBuf);
  if (!validateVecHdr(hdr)) return;
  unsigned n = int32_t(hdr->length);
  offsets.push(Zfb::Save::vectorIter<Zfb::Vector<uint8_t>>(fbb, n,
    [&varBuf](Zfb::Builder &fbb, unsigned) {
      return Zfb::Save::bytes(fbb,
	vecElem(varBuf, [](const uint8_t *ptr, unsigned length) {
	  return ZuBytes{ptr, length};
	}));
    }).Union());
}

#define ZdbPQ_SaveIntVec(width) \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<Int##width##Vec>{}> \
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value) \
{ \
  auto varBuf = value.p<Int##width##Vec>().v; \
  auto hdr = vecHdr(varBuf); \
  if (!validateVecHdr(hdr)) return; \
  unsigned n = int32_t(hdr->length); \
  offsets.push(Zfb::Save::pvectorIter<int##width##_t>( \
    fbb, n, [&varBuf](unsigned) { \
      return vecElem(varBuf, [](const uint8_t *ptr, unsigned) { \
	return int##width##_t(reinterpret_cast<const Int##width *>(ptr)->v); \
      }); \
  }).Union()); \
} \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<UInt##width##Vec>{}> \
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value) \
{ \
  auto varBuf = value.p<UInt##width##Vec>().v; \
  auto hdr = vecHdr(varBuf); \
  if (!validateVecHdr(hdr)) return; \
  unsigned n = int32_t(hdr->length); \
  offsets.push(Zfb::Save::pvectorIter<uint##width##_t>( \
    fbb, n, [&varBuf](unsigned) { \
      return vecElem(varBuf, [](const uint8_t *ptr, unsigned) { \
	return uint##width##_t(reinterpret_cast<const UInt##width *>(ptr)->v); \
      }); \
  }).Union()); \
}

ZdbPQ_SaveIntVec(8)
ZdbPQ_SaveIntVec(16)
ZdbPQ_SaveIntVec(32)
ZdbPQ_SaveIntVec(64)

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Int128Vec>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  auto varBuf = value.p<Int128Vec>().v;
  auto hdr = vecHdr(varBuf);
  if (!validateVecHdr(hdr)) return;
  unsigned n = int32_t(hdr->length);
  offsets.push(Zfb::Save::structVecIter<Zfb::Int128>(fbb, n,
    [&varBuf](Zfb::Int128 *ptr, unsigned) {
      *ptr = Zfb::Save::int128(vecElem(varBuf,
	[](const uint8_t *ptr, unsigned) {
	  return int128_t(reinterpret_cast<const Int128 *>(ptr)->v);
	}));
    }).Union());
}
template <unsigned Type>
inline ZuIfT<Type == Value::Index<UInt128Vec>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  auto varBuf = value.p<UInt128Vec>().v;
  auto hdr = vecHdr(varBuf);
  if (!validateVecHdr(hdr)) return;
  unsigned n = int32_t(hdr->length);
  offsets.push(Zfb::Save::structVecIter<Zfb::UInt128>(fbb, n,
    [&varBuf](Zfb::UInt128 *ptr, unsigned) {
      *ptr = Zfb::Save::uint128(vecElem(varBuf,
	[](const uint8_t *ptr, unsigned) {
	  return uint128_t(reinterpret_cast<const UInt128 *>(ptr)->v);
	}));
    }).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<FloatVec>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  auto varBuf = value.p<FloatVec>().v;
  auto hdr = vecHdr(varBuf);
  if (!validateVecHdr(hdr)) return;
  unsigned n = int32_t(hdr->length);
  offsets.push(Zfb::Save::pvectorIter<double>(fbb, n, [&varBuf](unsigned) {
    return vecElem(varBuf, [](const uint8_t *ptr, unsigned) {
      return double(reinterpret_cast<const Float *>(ptr)->v);
    });
  }).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<FixedVec>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  auto varBuf = value.p<FixedVec>().v;
  auto hdr = vecHdr(varBuf);
  if (!validateVecHdr(hdr)) return;
  unsigned n = int32_t(hdr->length);
  offsets.push(Zfb::Save::structVecIter<Zfb::Fixed>(fbb, n,
    [&varBuf](Zfb::Fixed *ptr, unsigned) {
      *ptr = Zfb::Save::fixed(vecElem(varBuf,
	[](const uint8_t *ptr, unsigned) {
	  return ZuFixed{ZuDecimal{ZuDecimal::Unscaled{
	    reinterpret_cast<const Fixed *>(ptr)->v
	  }}};
	}));
    }).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<DecimalVec>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  auto varBuf = value.p<DecimalVec>().v;
  auto hdr = vecHdr(varBuf);
  if (!validateVecHdr(hdr)) return;
  unsigned n = int32_t(hdr->length);
  offsets.push(Zfb::Save::structVecIter<Zfb::Decimal>(fbb, n,
    [&varBuf](Zfb::Decimal *ptr, unsigned) {
      *ptr = Zfb::Save::decimal(vecElem(varBuf,
	[](const uint8_t *ptr, unsigned) {
	  return ZuDecimal{ZuDecimal::Unscaled{
	    reinterpret_cast<const Decimal *>(ptr)->v}};
	}));
    }).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<TimeVec>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  auto varBuf = value.p<TimeVec>().v;
  auto hdr = vecHdr(varBuf);
  if (!validateVecHdr(hdr)) return;
  unsigned n = int32_t(hdr->length);
  offsets.push(Zfb::Save::structVecIter<Zfb::Time>(fbb, n,
    [&varBuf](Zfb::Time *ptr, unsigned) {
      *ptr = Zfb::Save::time(vecElem(varBuf,
	[](const uint8_t *ptr, unsigned) {
	  auto t = reinterpret_cast<const Time *>(ptr);
	  return ZuTime{t->sec, t->nsec};
	}));
    }).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<DateTimeVec>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  auto varBuf = value.p<DateTimeVec>().v;
  auto hdr = vecHdr(varBuf);
  if (!validateVecHdr(hdr)) return;
  unsigned n = int32_t(hdr->length);
  offsets.push(Zfb::Save::structVecIter<Zfb::DateTime>(fbb, n,
    [&varBuf](Zfb::DateTime *ptr, unsigned) {
      *ptr = Zfb::Save::dateTime(vecElem(varBuf,
	[](const uint8_t *ptr, unsigned) {
	  auto t = reinterpret_cast<const DateTime *>(ptr);
	  return ZuDateTime{ZuTime{t->sec, t->nsec}};
	}));
    }).Union());
}

template <unsigned Type>
inline ZuIfT<
  Type != Value::Index<String>{} &&
  Type != Value::Index<Bytes>{} &&
  !isVar(Type)>
saveOffset(Zfb::Builder &, Offsets &, const Value &) { }

template <unsigned Type>
inline ZuIfT<Type == Value::Index<void>{}>
saveValue(
  Zfb::Builder &, const Offsets &,
  const reflection::Field *, const Value &) { }

template <unsigned Type>
inline ZuIfT<
  Type == Value::Index<String>{} ||
  Type == Value::Index<Bytes>{} ||
  isVar(Type)>
saveValue(
  Zfb::Builder &fbb, const Offsets &offsets,
  const reflection::Field *field, const Value &value)
{
  fbb.AddOffset(field->offset(), offsets.shift());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Bool>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  fbb.AddElement<bool>(
    field->offset(), value.p<Type>().v, field->default_integer());
}

#define ZdbPQ_SaveInt(width) \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<Int##width>{}> \
saveValue( \
  Zfb::Builder &fbb, const Offsets &, \
  const reflection::Field *field, const Value &value) \
{ \
  fbb.AddElement<int##width##_t>( \
    field->offset(), value.p<Type>().v, field->default_integer()); \
} \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<UInt##width>{}> \
saveValue( \
  Zfb::Builder &fbb, const Offsets &, \
  const reflection::Field *field, const Value &value) \
{ \
  fbb.AddElement<uint##width##_t>( \
    field->offset(), value.p<Type>().v, field->default_integer()); \
}

ZdbPQ_SaveInt(8)
ZdbPQ_SaveInt(16)
ZdbPQ_SaveInt(32)
ZdbPQ_SaveInt(64)

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Float>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  fbb.AddElement<double>(
    field->offset(), value.p<Type>().v, field->default_real());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Fixed>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::fixed(
    ZuFixed{ZuDecimal{ZuDecimal::Unscaled{value.p<Type>().v}}});
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Decimal>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::decimal(
    ZuDecimal{ZuDecimal::Unscaled{value.p<Type>().v}});
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Time>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  const auto &v_ = value.p<Type>();
  auto v = Zfb::Save::time(ZuTime{v_.sec, v_.nsec});
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<DateTime>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  const auto &v_ = value.p<Type>();
  auto v = Zfb::Save::dateTime(ZuDateTime{ZuTime{v_.sec, v_.nsec}});
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Int128>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::int128(value.p<Type>().v);
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<UInt128>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::uint128(value.p<Type>().v);
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<IP>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::ip(value.p<Type>().addr);
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ID>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::id(value.p<Type>().id);
  fbb.AddStruct(field->offset(), &v);
}

// --- data tuple

using Tuple = ZtArray<Value>;

// load tuple from flatbuffer
// - when called from select_send(), nParams is < fields.length()
void loadTuple(
  Tuple &tuple,
  VarBuf &varBuf,
  VarBufParts &varBufParts,
  const OIDs &oids,
  unsigned nParams,
  const ZtMFields &fields,
  const XFields &xFields,
  const Zfb::Table *fbo)
{
  unsigned j = 0;
  for (unsigned i = 0; i < nParams; i++) {
    auto value = static_cast<Value *>(tuple.push());
    auto type = xFields[i].type;
    if (!isVar(type))
      ZuSwitch::dispatch<Value_::N>(type, [
	value, field = xFields[i].field, fbo
      ](auto I) {
	loadValue<I>(value->new_<I, true>(), field, fbo);
      });
    else {
      auto &varBufPart = varBufParts[j++];
      ZuSwitch::dispatch<Value_::N>(type, [
	value, &varBuf, &varBufPart, &oids, field = xFields[i].field, fbo
      ](auto I) {
	loadValue<I>(
	  value->new_<I, true>(), varBuf, varBufPart, oids, field, fbo);
      });
    }
  }
}

// save tuple to flatbuffer
Offset saveTuple(
  Zfb::Builder &fbb,
  const XFields &xFields,
  ZuArray<const Value> tuple)
{
  unsigned n = xFields.length();
  ZmAssert(tuple.length() == n);
  Offsets offsets{ZmAlloc(Offset, n)};
  for (unsigned i = 0; i < n; i++) {
    auto type = xFields[i].type;
    const auto &value = tuple[i];
    ZuSwitch::dispatch<Value::N>(type,
      [&fbb, &offsets, &value](auto I) {
	saveOffset<I>(fbb, offsets, value);
      });
  }
  auto start = fbb.StartTable();
  for (unsigned i = 0; i < n; i++) {
    auto type = xFields[i].type;
    const auto &value = tuple[i];
    ZuSwitch::dispatch<Value::N>(type,
      [&fbb, &offsets, field = xFields[i].field, &value](auto I) {
	saveValue<I>(fbb, offsets, field, value);
      });
  }
  auto end = fbb.EndTable(start);
  return Offset{end};
}

namespace SendState {
  ZtEnumValues(SendState, int8_t,
    Unsent = 0,	// unsent
    Again,	// send attempted, need to retry
    Sent,	// sent, no server-side flush or sync needed
    Flush,	// sent, PQsendFlushRequest() needed
    Sync);	// sent, PQpipelineSync() needed
}

namespace Work {

struct Open { };	// open table

struct Count {
  unsigned		keyID;
  ZmRef<const AnyBuf>	buf;
  CountFn		countFn;
};

struct Select {
  unsigned		keyID;
  unsigned		limit;
  ZmRef<const AnyBuf>	buf;
  TupleFn		tupleFn;
  unsigned		count = 0;
  bool			selectRow;
  bool			selectNext;
  bool			inclusive;
};

struct Find {
  unsigned		keyID;
  ZmRef<const AnyBuf>	buf;
  RowFn			rowFn;
  bool			found = false;
};

struct Recover {
  UN			un;
  RowFn			rowFn;
  bool			found = false;
};

struct Write {
  ZmRef<const AnyBuf>	buf;
  CommitFn		commitFn;
  bool			mrd = false;	// used by delete only
};

using Query = ZuUnion<Open, Count, Select, Find, Recover, Write>;

struct Start { };			// start data store

struct Stop { };

struct TblTask {
  StoreTbl	*tbl;
  Query		query;
};

using Task = ZuUnion<Start, Stop, TblTask>;

using Queue = ZmList<Task>;

} // Work

// start state for the data store
// - start is a heavy lift, involving several distinct phases
// - care is taken to alert and error out on schema inconsistencies
// - ... while automatically creating new tables and indices as needed
// - OIDs for all vocabulary types are retrieved and populated
// - MRD table is idempotently created
class StartState {
public:
  uint32_t v = 0;	// public for printing/logging

  // phases
  enum {
    PreStart = 0,
    GetOIDs,	// retrieve OIDs
    MkSchema,	// idempotent create schema
    MkTblMRD,	// idempotent create MRD table
    Started	// start complete (possibly failed)
  };

private:
  // flags and masks
  enum {
    Create	= 0x8000,	// used by MkMRD
    Failed	= 0x4000,	// used by all
    TypeMask	= 0x3fff,	// used by GetOIDs
    PhaseShift	= 16
  };

public:
  constexpr void reset() { v = 0; }

  constexpr bool create() const { return v & Create; }
  constexpr bool failed() const { return v & Failed; }
  constexpr unsigned type() const { return v & TypeMask; }
  constexpr unsigned phase() const { return v>>PhaseShift; }

  constexpr void phase(uint32_t p) { v = p<<PhaseShift; }
  constexpr void incType() {
    ZmAssert(type() < TypeMask);
    ++v;
  }

  constexpr void setCreate() { v |= Create; }
  constexpr void clrCreate() { v &= ~Create; }

  constexpr void setFailed() { v |= Failed; }
  constexpr void clrFailed() { v &= ~Failed; }
};

// open state for a table
// - table open is a heavy lift, involving multiple distinct phases
// - some phases iterate over individual keys and fields
// - care is taken to alert and error out on schema inconsistencies
// - ... while automatically creating new tables and indices as needed
// - recover, find, insert, update and delete statements are prepared
// - max UN, SN are recovered
// - explicit state management is used, encapsulated with OpenState
class OpenState {
public:
  uint32_t v = 0;	// public for printing/logging

  // phases
  enum {
    Closed = 0,
    MkTable,	// idempotent create table
    MkIndices,	// idempotent create indices for all keys
    PrepCount,	// prepare count for all keys
    PrepSelectKIX, // Key, Initial, eXclusive - prepare select for all keys
    PrepSelectKNX, // Key, Next,    eXclusive - ''
    PrepSelectKNI, // Key, Next,    Inclusive - ''
    PrepSelectRIX, // Row, Initial, eXclusive - ''
    PrepSelectRNX, // Row, Next,    eXclusive - ''
    PrepSelectRNI, // Row, Next,    Inclusive - ''
    PrepFind,	// prepare recover and find for all keys
    PrepInsert,	// prepare insert query
    PrepUpdate,	// prepare update query
    PrepDelete,	// prepare delete query
    PrepMRD,	// prepare MRD update
    Count,	// query count
    MaxUN,	// query max UN, max SN from main table
    EnsureMRD,	// ensure MRD table has row for this table
    MRD,	// query max UN, max SN from mrd table
    Opened	// open complete (possibly failed)
  };

private:
  // flags and masks
  enum {
    Create	= 0x8000,	// used by MkTable, MkIndices
    Failed	= 0x4000,	// used by all
    FieldMask	= 0x3fff,	// used by MkTable, MkIndices
    KeyShift	= 16,
    KeyMask	= 0x7ff,	// up to 2K keys
    PhaseShift	= 27		// up to 32 phases
  };

  ZuAssert(FieldMask >= Zdb_::maxFields());
  ZuAssert(KeyMask >= Zdb_::maxKeys());

public:
  constexpr void reset() { v = 0; }

  constexpr bool create() const { return v & Create; }
  constexpr bool failed() const { return v & Failed; }
  constexpr unsigned field() const { return v & FieldMask; }
  constexpr unsigned keyID() const { return (v>>KeyShift) & KeyMask; }
  constexpr unsigned phase() const { return v>>PhaseShift; }

  constexpr void phase(uint32_t p) { v = p<<PhaseShift; }
  constexpr void incKey() {
    ZmAssert(keyID() < KeyMask);
    v = (v + (1<<KeyShift)) & ~(Create | Failed | FieldMask);
  }
  constexpr void incField() {
    ZmAssert(field() < FieldMask);
    ++v;
  }

  constexpr void setCreate() { v |= Create; }
  constexpr void clrCreate() { v &= ~Create; }

  constexpr void setFailed() { v |= Failed; }
  constexpr void clrFailed() { v &= ~Failed; }
};

class StoreTbl : public Zdb_::StoreTbl {
friend Store;

public:
  StoreTbl(
    Store *store, ZuID id, ZtMFields fields, ZtMKeyFields keyFields,
    const reflection::Schema *schema);

  Store *store() const { return m_store; }
  auto id() const { return m_id; }

protected:
  ~StoreTbl();

public:
  void open(OpenFn);
  void close(CloseFn);

  void warmup();

  void count(unsigned keyID, ZmRef<const AnyBuf>, CountFn);

  void select(
    bool selectRow, bool selectNext, bool inclusive,
    unsigned keyID, ZmRef<const AnyBuf>,
    unsigned limit, TupleFn);

  void find(unsigned keyID, ZmRef<const AnyBuf>, RowFn);

  void recover(UN, RowFn);

  void write(ZmRef<const AnyBuf>, CommitFn);

private:
  // open orchestration
  void open_enqueue();
  int open_send();
  void open_rcvd(PGresult *);
  void open_failed(ZeMEvent);
  void opened();

  // open phases
  void mkTable();
  int mkTable_send();
  void mkTable_rcvd(PGresult *);

  void mkIndices();
  int mkIndices_send();
  void mkIndices_rcvd(PGresult *);

  void prepCount();
  int prepCount_send();
  void prepCount_rcvd(PGresult *);

  void prepSelect();
  int prepSelect_send();
  void prepSelect_rcvd(PGresult *);

  void prepFind();
  int prepFind_send();
  void prepFind_rcvd(PGresult *);

  void prepInsert();
  int prepInsert_send();
  void prepInsert_rcvd(PGresult *);

  void prepUpdate();
  int prepUpdate_send();
  void prepUpdate_rcvd(PGresult *);

  void prepDelete();
  int prepDelete_send();
  void prepDelete_rcvd(PGresult *);

  void prepMRD();
  int prepMRD_send();
  void prepMRD_rcvd(PGresult *);

  void openCount();
  int openCount_send();
  void openCount_rcvd(PGresult *);

  void maxUN();
  int maxUN_send();
  void maxUN_rcvd(PGresult *);

  void ensureMRD();
  int ensureMRD_send();
  void ensureMRD_rcvd(PGresult *);

  void mrd();
  int mrd_send();
  void mrd_rcvd(PGresult *);

  // principal queries
  int count_send(Work::Count &);
  void count_rcvd(Work::Count &, PGresult *);
  void count_failed(Work::Count &, ZeMEvent);

  int select_send(Work::Select &);
  void select_rcvd(Work::Select &, PGresult *);
  ZmRef<AnyBuf> select_save(ZuArray<const Value> tuple, const XFields &xFields);
  void select_failed(Work::Select &, ZeMEvent);

  int find_send(Work::Find &);
  void find_rcvd(Work::Find &, PGresult *);
  template <bool Recovery>
  void find_rcvd_(RowFn &, bool &found, PGresult *);
  template <bool Recovery>
  ZmRef<AnyBuf> find_save(ZuArray<const Value> tuple);
  void find_failed(Work::Find &, ZeMEvent);
  void find_failed_(RowFn, ZeMEvent);

  int recover_send(Work::Recover &);
  void recover_rcvd(Work::Recover &, PGresult *);
  void recover_failed(Work::Recover &, ZeMEvent);

  int write_send(Work::Write &);
  void write_rcvd(Work::Write &, PGresult *);
  void write_failed(Work::Write &, ZeMEvent);

private:
  using UpdFields = ZtArray<const ZtMField *>;
  using FieldMap = ZmLHashKV<ZtString, unsigned, ZmLHashLocal<>>;

  Store			*m_store = nullptr;
  ZuID			m_id;
  ZtString		m_id_;		// snake case
  ZtMFields		m_fields;	// all fields
  UpdFields		m_updFields;	// update fields
  ZtMKeyFields		m_keyFields;	// fields for each key
  XFields		m_xFields;
  XFields		m_xUpdFields;
  XKeyFields		m_xKeyFields;
  ZtArray<unsigned>	m_keyGroup;	// length of group key
  FieldMap		m_fieldMap;

  OpenState		m_openState;
  OpenFn		m_openFn;	// open callback

  uint64_t		m_count = 0;
  UN			m_maxUN = ZdbNullUN();
  SN			m_maxSN = ZdbNullSN();
};

// --- PostgreSQL data store

inline ZuID StoreTbl_IDAxor(const StoreTbl &storeTbl) { return storeTbl.id(); }
inline constexpr const char *StoreTbls_HeapID() { return "StoreTbls"; }
using StoreTbls =
  ZmHash<StoreTbl,
    ZmHashNode<StoreTbl,
      ZmHashKey<StoreTbl_IDAxor,
	ZmHashLock<ZmPLock,
	  ZmHashHeapID<StoreTbls_HeapID>>>>>;

class Store : public Zdb_::Store {
public:
  InitResult init(ZvCf *, ZiMultiplex *, unsigned sid);
  void final();

  void start(StartFn);
  void stop(StopFn);

  void open(
    ZuID id,
    ZtMFields fields,
    ZtMKeyFields keyFields,
    const reflection::Schema *schema,
    OpenFn openFn);

  bool stopping() const { return m_stopFn; }

  void enqueue(Work::Task task);

  template <typename ...Args> void pqRun(Args &&...args) {
    m_mx->run(m_pqSID, ZuFwd<Args>(args)...);
  }
  template <typename ...Args> void pqInvoke(Args &&...args) {
    m_mx->invoke(m_pqSID, ZuFwd<Args>(args)...);
  }

  template <typename ...Args> void zdbRun(Args &&...args) {
    m_mx->run(m_zdbSID, ZuFwd<Args>(args)...);
  }
  template <typename ...Args> void zdbInvoke(Args &&...args) {
    m_mx->invoke(m_zdbSID, ZuFwd<Args>(args)...);
  }

  const OIDs &oids() const { return m_oids; }

  template <int State, bool MultiRow>
  int sendQuery(const ZtString &query, const Tuple &params);
  int sendPrepare(
    const ZtString &query, const ZtString &id, ZuArray<unsigned> oids);
  template <int State, bool MultiRow>
  int sendPrepared(const ZtString &id, const Tuple &params);

private:
  bool start_();
  void stop_();
  void stop_1();
  void stop_2();

  void wake();
  void wake_();
  void run_();

  void recv();
  void rcvd(Work::Queue::Node *, PGresult *);
  void failed(Work::Queue::Node *, ZeMEvent);

  void send();

  // start orchestration
  void start_enqueue();
  int start_send();
  void start_rcvd(PGresult *);
  void start_failed(bool running, ZeMEvent);
  void started();

  // start phases
  void getOIDs();
  int getOIDs_send();
  void getOIDs_rcvd(PGresult *);

  void mkSchema();
  int mkSchema_send();
  void mkSchema_rcvd(PGresult *);

  void mkTblMRD();
  int mkTblMRD_send();
  void mkTblMRD_rcvd(PGresult *);

  void mkIdxMRD();
  int mkIdxMRD_send();
  void mkIdxMRD_rcvd(PGresult *);

private:
  ZvCf			*m_cf = nullptr;
  ZiMultiplex		*m_mx = nullptr;
  unsigned		m_zdbSID = ZuCmp<unsigned>::null();
  unsigned		m_pqSID = ZuCmp<unsigned>::null();

  ZmRef<StoreTbls>	m_storeTbls;

  PGconn		*m_conn = nullptr;
  int			m_connFD = -1;
#ifndef _WIN32
  int			m_epollFD = -1;
  int			m_wakeFD = -1, m_wakeFD2 = -1;
#else
  HANDLE		m_wakeSem = INVALID_HANDLE_VALUE;
  HANDLE		m_connEvent = INVALID_HANDLE_VALUE;
#endif

  // FIXME - later - telemetry for queue lengths
  // FIXME - later - test transient send failure, pushback, resend
  Work::Queue		m_queue;	// not yet sent
  Work::Queue		m_sent;		// sent, awaiting response

  OIDs			m_oids;

  StartState		m_startState;
  StartFn		m_startFn;
  StopFn		m_stopFn;
};

} // ZdbPQ

// main data store driver entry point
extern "C" {
  ZdbPQExtern Zdb_::Store *ZdbStore();
}

#endif /* ZdbPQ_HH */
