//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#ifndef ZdbPQ_HH
#define ZdbPQ_HH

#ifdef _MSC_VER
#pragma once
#endif

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

#include <zlib/ZiIP.hh>

#include <zlib/ZdbStore.hh>

namespace ZdbPQ {

class Store;
class StoreTbl;

using namespace Zdb_;

// Value    C++        flatbuffers     PG SQL        PG send/recv
// -----    ---        -----------     ------        ------------
// String   ZuString   String          text          raw data
// Bytes    ZuBytes    Vector<uint8_t> bytea         raw data
// Bool     bool       Bool            bool          uint8_t
// Int64    int64_t    Long            int8     (*)  int64_t BE
// UInt64   uint64_t   ULong           uint8    (*)  uint64_t BE
// Enum     int        Byte            int1          int8_t
// Flags    uint64_t   ULong           uint8    (*)  uint64_t BE
// Float    double     Double          float8        double BE
// Fixed    ZuFixed    Zfb.Fixed       zdecimal (**) int128_t BE
// Decimal  ZuDecimal  Zfb.Decimal     zdecimal (**) int128_t BE
// Time     ZuTime     Zfb.Time        ztime    (**) int64_t BE, int32_t BE
// DateTime ZuDateTime Zfb.DateTime    ztime    (**) int64_t BE, int32_t BE
// Int128   int128_t   Zfb.Int128      int16    (**) int128_t BE
// UInt128  uint128_t  Zfb.UInt128     uint16   (**) uint128_t BE
// IP       ZiIP       Zfb.IP          inet          IPHdr, ZuIP
// ID       ZuID       Zfb.ID          text          raw data

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

// --- value union (postgres binary send/receive format)

using String = ZuString;
using Bytes = ZuBytes;
#pragma pack(push, 1)
struct Bool { uint8_t v; };
struct Int64 { ZuBigEndian<int64_t> v; };
struct UInt64 { ZuBigEndian<uint64_t> v; };
struct Enum { int8_t v; };
struct Flags { ZuBigEndian<uint64_t> v; };
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

using Value_ = ZuUnion<
  void,
  String,
  Bytes,
  Bool,
  Int64,
  UInt64,
  Enum,
  Flags,
  Float,
  Fixed,
  Decimal,
  Time,
  DateTime,
  Int128,
  UInt128,
  IP,
  ID>;

struct Value : public Value_ {
  using Value_::Value_;
  using Value_::operator =;
  template <typename ...Args>
  Value(Args &&...args) : Value_{ZuFwd<Args>(args)...} { }

  // invoke() skips void
  template <unsigned I, typename L, typename T = Value_::Type<I>>
  static ZuIfT<!ZuIsExact<void, T>{}>
  invoke_(L l) { l(ZuUnsigned<I>{}); }
  template <unsigned I, typename L, typename T = Value_::Type<I>>
  static ZuIfT<ZuIsExact<void, T>{}>
  invoke_(L l) { }
  template <typename L> static void invoke(L l, unsigned i) {
    ZuSwitch::dispatch<Value_::N>(i, [l = ZuMv(l)](auto I) mutable {
      invoke_<I>(ZuMv(l));
    });
  }
  template <typename L> void invoke(L l) const {
    invoke(ZuMv(l), this->type());
  }

  // Postgres binary format loaders

  // void
  template <unsigned I, typename T = Value_::Type<I>>
  ZuExact<void, T, bool>
  load(const char *, unsigned) { type_(I); return true; }

  // String - zero-copy - relies on the PGresult remaining in scope
  template <unsigned I, typename T = Value_::Type<I>>
  ZuExact<String, T, bool>
  load(const char *data, unsigned length) {
    new (new_<I, true>()) String{data, length};
    return true;
  }

  // Bytes - zero-copy - relies on the PGresult remaining in scope
  template <unsigned I, typename T = Value_::Type<I>>
  ZuExact<Bytes, T, bool>
  load(const char *data, unsigned length) {
    new (new_<I, true>())
      Bytes{reinterpret_cast<const uint8_t *>(data), length};
    return true;
  }

  // All other types - copied
  template <unsigned I, typename T = Value_::Type<I>>
  ZuIfT<
    !ZuIsExact<void, T>{} &&
    !ZuIsExact<String, T>{} &&
    !ZuIsExact<Bytes, T>{}, bool>
  load(const char *data, unsigned length) {
    if (length != sizeof(T)) return false;
    memcpy(new_<I, true>(), data, length);
    return true;
  }

  // Postgres binary format accessors - data<I>(), length<I>()

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
  data() const { return p<String>().data(); }
  template <unsigned I, typename T = Value_::Type<I>>
  ZuExact<String, T, unsigned>
  length() const { return p<String>().length(); }

  // Bytes - return raw byte data
  template <unsigned I, typename T = Value_::Type<I>>
  ZuExact<Bytes, T, const char *>
  data() const {
    return reinterpret_cast<const char *>(p<Bytes>().data());
  }
  template <unsigned I, typename T = Value_::Type<I>>
  ZuExact<Bytes, T, unsigned>
  length() const { return p<Bytes>().length(); }

  // All other types - return bigendian packed struct
  template <unsigned I, typename T = Value_::Type<I>>
  ZuIfT<
    !ZuIsExact<void, T>{} &&
    !ZuIsExact<String, T>{} &&
    !ZuIsExact<Bytes, T>{}, const char *>
  data() const { return reinterpret_cast<const char *>(this); }
  template <unsigned I, typename T = Value_::Type<I>>
  ZuIfT<
    !ZuIsExact<void, T>{} &&
    !ZuIsExact<String, T>{} &&
    !ZuIsExact<Bytes, T>{}, unsigned>
  length() const { return sizeof(T); }

  template <typename S>
  void print(S &s) const {
    invoke([this, &s](auto i) { s << this->p<i>(); });
  }

  friend ZuPrintFn ZuPrintType(Value *);
};

// --- load value from flatbuffer

template <unsigned Type>
inline ZuIfT<Type == Value::Index<String>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) String{Zfb::Load::str(Zfb::GetFieldS(*fbo, *field))};
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Bytes>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) Bytes{Zfb::Load::bytes(Zfb::GetFieldV<uint8_t>(*fbo, *field))};
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Bool>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) Bool{Zfb::GetFieldI<bool>(*fbo, *field)};
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Int64>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  switch (field->type()->base_type()) {
    case reflection::Bool:
      new (ptr) Int64{Zfb::GetFieldI<uint8_t>(*fbo, *field)};
      break;
    case reflection::Byte:
      new (ptr) Int64{Zfb::GetFieldI<int8_t>(*fbo, *field)};
      break;
    case reflection::Short:
      new (ptr) Int64{Zfb::GetFieldI<int16_t>(*fbo, *field)};
      break;
    case reflection::Int:
      new (ptr) Int64{Zfb::GetFieldI<int32_t>(*fbo, *field)};
      break;
    case reflection::Long:
      new (ptr) Int64{Zfb::GetFieldI<int64_t>(*fbo, *field)};
      break;
    default:
      break;
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<UInt64>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  switch (field->type()->base_type()) {
    case reflection::Bool:
      new (ptr) UInt64{Zfb::GetFieldI<uint8_t>(*fbo, *field)};
      break;
    case reflection::UByte:
      new (ptr) UInt64{Zfb::GetFieldI<uint8_t>(*fbo, *field)};
      break;
    case reflection::UShort:
      new (ptr) UInt64{Zfb::GetFieldI<uint16_t>(*fbo, *field)};
      break;
    case reflection::UInt:
      new (ptr) UInt64{Zfb::GetFieldI<uint32_t>(*fbo, *field)};
      break;
    case reflection::ULong:
      new (ptr) UInt64{Zfb::GetFieldI<uint64_t>(*fbo, *field)};
      break;
    default:
      break;
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Enum>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  switch (field->type()->base_type()) {
    case reflection::Bool:
      new (ptr) Enum{int8_t(Zfb::GetFieldI<uint8_t>(*fbo, *field))};
      break;
    case reflection::Byte:
      new (ptr) Enum{Zfb::GetFieldI<int8_t>(*fbo, *field)};
      break;
    case reflection::Short:
      new (ptr) Enum{int8_t(Zfb::GetFieldI<int16_t>(*fbo, *field))};
      break;
    case reflection::Int:
      new (ptr) Enum{int8_t(Zfb::GetFieldI<int32_t>(*fbo, *field))};
      break;
    case reflection::Long:
      new (ptr) Enum{int8_t(Zfb::GetFieldI<int64_t>(*fbo, *field))};
      break;
    default:
      break;
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Flags>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  switch (field->type()->base_type()) {
    case reflection::ULong:
      new (ptr) Flags{Zfb::GetFieldI<uint64_t>(*fbo, *field)};
      break;
    default:
      break;
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Float>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  switch (field->type()->base_type()) {
    case reflection::Float:
      new (ptr) Float{Zfb::GetFieldF<float>(*fbo, *field)};
      break;
    case reflection::Double:
      new (ptr) Float{Zfb::GetFieldF<double>(*fbo, *field)};
      break;
    default:
      break;
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Fixed>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  ZuDecimal d =
    Zfb::Load::fixed(fbo->GetPointer<const Zfb::Fixed *>(field->offset()));
  new (ptr) Fixed{d.value};
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Decimal>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  ZuDecimal d =
    Zfb::Load::decimal(fbo->GetPointer<const Zfb::Decimal *>(field->offset()));
  new (ptr) Decimal{d.value};
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Time>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  auto t = fbo->GetPointer<const Zfb::Time *>(field->offset());
  new (ptr) Time{ t->sec(), t->nsec() };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<DateTime>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  ZuTime t =
    Zfb::Load::dateTime(
      fbo->GetPointer<const Zfb::DateTime *>(field->offset())).as_time();
  new (ptr) ZuDateTime{ t.sec(), t.nsec() };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Int128>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) Int128{
    Zfb::Load::int128(fbo->GetPointer<const Zfb::Int128 *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<UInt128>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) UInt128{
    Zfb::Load::uint128(fbo->GetPointer<const Zfb::UInt128 *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<IP>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) IP{
    IPHdr{},
    Zfb::Load::ip(fbo->GetPointer<const Zfb::IP *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ID>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ID{
    Zfb::Load::id(fbo->GetPointer<const Zfb::ID *>(field->offset()))
  };
}

inline void loadValue(
  Value *value, const XField &xField, const Zfb::Table *fbo)
{
  if (ZuUnlikely(!xField.type))
    new (value) Value{};
  else
    Value::invoke([value, field = xField.field, fbo](auto I) {
      loadValue_<I>(value->new_<I, true>(), field, fbo);
    }, xField.type);
}

// --- save value to flatbuffer

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
saveOffset_(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  offsets.push(Zfb::Save::str(fbb, value.p<Type>()).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Bytes>{}>
saveOffset_(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  offsets.push(Zfb::Save::bytes(fbb, value.p<Type>()).Union());
}

template <unsigned Type>
inline ZuIfT<
  Type != Value::Index<String>{} &&
  Type != Value::Index<Bytes>{}>
saveOffset_(Zfb::Builder &, Offsets &, const Value &) { }

inline void saveOffset(
  Zfb::Builder &fbb, Offsets &offsets,
  const XField &xField, const Value &value)
{
  Value::invoke([&fbb, &offsets, &value](auto I) {
    saveOffset_<I>(fbb, offsets, value);
  }, xField.type);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<String>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &offsets,
  const reflection::Field *field, const Value &value)
{
  fbb.AddOffset(field->offset(), offsets.shift());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Bytes>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &offsets,
  const reflection::Field *field, const Value &value)
{
  fbb.AddOffset(field->offset(), offsets.shift());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Bool>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  fbb.AddElement<uint8_t>(
    field->offset(), value.p<Type>().v, field->default_integer());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Int64>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  switch (field->type()->base_type()) {
    case reflection::Bool:
      fbb.AddElement<uint8_t>(
	field->offset(), value.p<Type>().v, field->default_integer());
      break;
    case reflection::Byte:
      fbb.AddElement<int8_t>(
	field->offset(), value.p<Type>().v, field->default_integer());
      break;
    case reflection::Short:
      fbb.AddElement<int16_t>(
	field->offset(), value.p<Type>().v, field->default_integer());
      break;
    case reflection::Int:
      fbb.AddElement<int32_t>(
	field->offset(), value.p<Type>().v, field->default_integer());
      break;
    case reflection::Long:
      fbb.AddElement<int64_t>(
	field->offset(), value.p<Type>().v, field->default_integer());
      break;
    default:
      break;
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<UInt64>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  switch (field->type()->base_type()) {
    case reflection::Bool:
    case reflection::UByte:
      fbb.AddElement<uint8_t>(
	field->offset(), value.p<Type>().v, field->default_integer());
      break;
    case reflection::UShort:
      fbb.AddElement<uint16_t>(
	field->offset(), value.p<Type>().v, field->default_integer());
      break;
    case reflection::UInt:
      fbb.AddElement<uint32_t>(
	field->offset(), value.p<Type>().v, field->default_integer());
      break;
    case reflection::ULong:
      fbb.AddElement<uint64_t>(
	field->offset(), value.p<Type>().v, field->default_integer());
      break;
    default:
      break;
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Enum>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  switch (field->type()->base_type()) {
    case reflection::Byte:
      fbb.AddElement<int8_t>(
	field->offset(), value.p<Type>().v, field->default_integer());
      break;
    case reflection::Short:
      fbb.AddElement<int16_t>(
	field->offset(), value.p<Type>().v, field->default_integer());
      break;
    case reflection::Int:
      fbb.AddElement<int32_t>(
	field->offset(), value.p<Type>().v, field->default_integer());
      break;
    case reflection::Long:
      fbb.AddElement<int64_t>(
	field->offset(), value.p<Type>().v, field->default_integer());
      break;
    default:
      break;
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Flags>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  switch (field->type()->base_type()) {
    case reflection::ULong:
      fbb.AddElement<uint64_t>(
	field->offset(), value.p<Type>().v, field->default_integer());
      break;
    default:
      break;
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Float>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  switch (field->type()->base_type()) {
    case reflection::Float:
      fbb.AddElement<float>(
	field->offset(), value.p<Type>().v, field->default_real());
      break;
    case reflection::Double:
      fbb.AddElement<double>(
	field->offset(), value.p<Type>().v, field->default_real());
      break;
    default:
      break;
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Fixed>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::fixed(
    ZuFixed{ZuDecimal{ZuDecimal::Unscaled{value.p<Type>().v}}});
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Decimal>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::decimal(
    ZuDecimal{ZuDecimal::Unscaled{value.p<Type>().v}});
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Time>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  const auto &v_ = value.p<Type>();
  auto v = Zfb::Save::time(ZuTime{v_.sec, v_.nsec});
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<DateTime>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  const auto &v_ = value.p<Type>();
  auto v = Zfb::Save::dateTime(ZuDateTime{ZuTime{v_.sec, v_.nsec}});
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Int128>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::int128(value.p<Type>().v);
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<UInt128>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::uint128(value.p<Type>().v);
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<IP>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::ip(value.p<Type>().addr);
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ID>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::id(value.p<Type>().id);
  fbb.AddStruct(field->offset(), &v);
}

inline void saveValue(
  Zfb::Builder &fbb, const Offsets &offsets,
  const XField &xField, const Value &value)
{
  Value::invoke([&fbb, &offsets, field = xField.field, &value](auto I) {
    saveValue_<I>(fbb, offsets, field, value);
  }, xField.type);
}

// --- data tuple

using Tuple = ZtArray<Value>;

// load tuple from flatbuffer
template <typename Filter>
Tuple loadTuple(
  Tuple tuple,
  const ZtMFields &fields,
  const XFields &xFields,
  const Zfb::Table *fbo,
  Filter filter)
{
  unsigned n = fields.length();
  tuple.ensure(tuple.length() + n);
  for (unsigned i = 0; i < n; i++)
    if (!filter(fields[i]))
      loadValue(static_cast<Value *>(tuple.push()), xFields[i], fbo);
  return tuple;
}
Tuple loadTuple(Tuple tuple,
  const ZtMFields &fields, const XFields &xFields, const Zfb::Table *fbo)
{
  return loadTuple(ZuMv(tuple), fields, xFields, fbo,
    [](const ZtMField *) { return false; });
}
Tuple loadUpdTuple(Tuple tuple,
  const ZtMFields &fields, const XFields &xFields, const Zfb::Table *fbo)
{
  return loadTuple(ZuMv(tuple), fields, xFields, fbo,
    [](const ZtMField *field) {
      return !(field->props & ZtMFieldProp::Update);
    });
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
  for (unsigned i = 0; i < n; i++)
    saveOffset(fbb, offsets, xFields[i], tuple[i]);
  auto start = fbb.StartTable();
  for (unsigned i = 0; i < n; i++)
    saveValue(fbb, offsets, xFields[i], tuple[i]);
  auto end = fbb.EndTable(start);
  return Offset{end};
}

// --- postgres OIDs, type names

class OIDs {
  using OIDs_ = ZuArrayN<unsigned, Value::N - 1>;
  using Types = ZmLHashKV<unsigned, int8_t, ZmLHashStatic<4, ZmLHashLocal<>>>;
  using Lookup = ZmLHashKV<ZuString, int8_t, ZmLHashStatic<4, ZmLHashLocal<>>>;

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

namespace SendState {
  ZtEnumValues(SendState,
    Unsent = 0,	// unsent
    Again,	// send attempted, need to retry
    Sent,	// sent, no server-side flush or sync needed
    Flush,	// sent, PQsendFlushRequest() needed
    Sync);	// sent, PQpipelineSync() needed
}

namespace Work {

struct Open { };			// open table

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

using Query = ZuUnion<Open, Find, Recover, Write>;

struct Start { };			// start data store

struct Stop {
  StopFn		stopped;
};

struct TblItem { // FIXME - rename
  StoreTbl	*tbl;	
  Query		query;
};

using Item = ZuUnion<Start, Stop, TblItem>;

using Queue = ZmList<Item>;

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
// - table open is a heavy lift, involving several distinct phases
// - some phases iterate over individual keys and fields
// - care is taken to alert and error out on schema inconsistencies
// - ... while automatically creating new tables and indices as needed
// - recover, find, insert, update and delete statements are prepared
// - max UN, SN are recovered
// - any series maxima are recovered
class OpenState {
public:
  uint32_t v = 0;	// public for printing/logging

  // phases
  enum {
    Closed = 0,
    MkTable,	// idempotent create table
    MkIndices,	// idempotent create indices for all keys
    PrepFind,	// prepare recover and find for all keys
    PrepInsert,	// prepare insert query
    PrepUpdate,	// prepare update query
    PrepDelete,	// prepare delete query
    PrepMRD,	// prepare MRD update
    Count,	// query count
    MaxUN,	// query max UN, max SN from main table
    EnsureMRD,	// ensure MRD table has row for this table
    MRD,	// query max UN, max SN from mrd table
    Maxima,	// query maxima for series keys
    Opened	// open complete (possibly failed)
  };

private:
  // flags and masks
  enum {
    Create	= 0x8000,	// used by MkTable, MkIndices
    Failed	= 0x4000,	// used by all
    FieldMask	= 0x3fff,	// used by MkTable, MkIndices
    KeyShift	= 16,
    KeyMask	= 0xfff,	// up to 4K keys
    PhaseShift	= 28		// up to 16 phases
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
  void open(MaxFn, OpenFn);
  void close(CloseFn);

  void warmup();

  void find(unsigned keyID, ZmRef<const AnyBuf> buf, RowFn rowFn);

  void recover(UN un, RowFn rowFn);

  void write(ZmRef<const AnyBuf> buf, CommitFn commitFn);

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

  void count();
  int count_send();
  void count_rcvd(PGresult *);

  void maxUN();
  int maxUN_send();
  void maxUN_rcvd(PGresult *);

  void ensureMRD();
  int ensureMRD_send();
  void ensureMRD_rcvd(PGresult *);

  void mrd();
  int mrd_send();
  void mrd_rcvd(PGresult *);

  void maxima();
  int maxima_send();
  void maxima_rcvd(PGresult *);
  ZmRef<AnyBuf> maxima_save(ZuArray<const Value> tuple, unsigned keyID);

  // principal queries
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
  using FieldMap = ZmLHashKV<ZtString, unsigned, ZmLHashLocal<>>;

  Store			*m_store = nullptr;
  ZuID			m_id;
  ZtString		m_id_;		// snake case
  ZtMFields		m_fields;
  ZtMKeyFields		m_keyFields;
  XFields		m_xFields;
  XKeyFields		m_xKeyFields;
  FieldMap		m_fieldMap;

  OpenState		m_openState;
  MaxFn			m_maxFn;	// maxima callback
  OpenFn		m_openFn;	// open callback
  ZmRef<AnyBuf>		m_maxBuf;	// used by maxima_save()

  uint64_t		m_count = 0;
  UN			m_maxUN = ZdbNullUN();
  SN			m_maxSN = ZdbNullSN();
};

// --- mock data store

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
    MaxFn maxFn,
    OpenFn openFn);

  bool stopping() const { return !!m_stopFn; }
  void enqueue(Work::Item item);

  template <typename ...Args> void pqRun(Args &&... args) {
    m_mx->run(m_pqSID, ZuFwd<Args>(args)...);
  }
  template <typename ...Args> void pqInvoke(Args &&... args) {
    m_mx->invoke(m_pqSID, ZuFwd<Args>(args)...);
  }

  template <typename ...Args> void zdbRun(Args &&... args) {
    m_mx->run(m_zdbSID, ZuFwd<Args>(args)...);
  }
  template <typename ...Args> void zdbInvoke(Args &&... args) {
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
  void stop_(StopFn);
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
  void start_failed(ZeMEvent);
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

  Work::Queue		m_queue;	// not yet sent
  Work::Queue		m_sent;		// sent, awaiting response

  OIDs			m_oids;

  StartState		m_startState;
  StartFn		m_startFn;
  StopFn		m_stopFn;
};

} // ZdbPQ

extern "C" {
  ZdbPQExtern Zdb_::Store *ZdbStore();
}

#endif /* ZdbPQ_HH */
