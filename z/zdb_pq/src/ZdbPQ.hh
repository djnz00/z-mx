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

// --- flatbuffer field arrays

struct FBField {
  const reflection::Field	*field;
  unsigned			type;	// Value union discriminator
};
using FBFields = ZtArray<FBField>;
using FBKeyFields = ZtArray<FBFields>;

// --- value union (postgres binary send/receive format)

#pragma pack(push, 1)
struct String { ZuString data; };
struct Bytes { ZuBytes data; };
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
  Value *value, const FBField &fbField, const Zfb::Table *fbo)
{
  if (ZuUnlikely(!fbField.type))
    new (value) Value{};
  else
    Value::invoke([value, field = fbField.field, fbo](auto I) {
      loadValue_<I>(value->new_<I, true>(), field, fbo);
    }, fbField.type);
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
  offsets.push(Zfb::Save::str(fbb, value.p<Type>().data).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Bytes>{}>
saveOffset_(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  offsets.push(Zfb::Save::bytes(fbb, value.p<Type>().data).Union());
}

template <unsigned Type>
inline ZuIfT<
  Type != Value::Index<String>{} &&
  Type != Value::Index<Bytes>{}>
saveOffset_(Zfb::Builder &, Offsets &, const Value &) { }

inline void saveOffset(
  Zfb::Builder &fbb, Offsets &offsets,
  const FBField &fbField, const Value &value)
{
  Value::invoke([&fbb, &offsets, &value](auto I) {
    saveOffset_<I>(fbb, offsets, value);
  }, fbField.type);
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
  const FBField &fbField, const Value &value)
{
  Value::invoke([&fbb, &offsets, field = fbField.field, &value](auto I) {
    saveValue_<I>(fbb, offsets, field, value);
  }, fbField.type);
}

// --- data tuple

using Tuple = ZtArray<Value>;

// load tuple from flatbuffer
template <typename Filter>
Tuple loadTuple(
  const ZtMFields &fields,
  const FBFields &fbFields,
  const Zfb::Table *fbo, Filter filter)
{
  unsigned n = fields.length();
  ZmAssert(n == fbFields.length());
  Tuple tuple(n); // not {}
  for (unsigned i = 0; i < n; i++)
    if (filter(fields[i]))
      new (tuple.push()) Value{};
    else
      loadValue(static_cast<Value *>(tuple.push()), fbFields[i], fbo);
  return tuple;
}
Tuple loadTuple(
  const ZtMFields &fields, const FBFields &fbFields, const Zfb::Table *fbo)
{
  return loadTuple(fields, fbFields, fbo, [](const ZtMField *) {
    return false;
  });
}
Tuple loadUpdTuple(
  const ZtMFields &fields, const FBFields &fbFields, const Zfb::Table *fbo)
{
  return loadTuple(fields, fbFields, fbo, [](const ZtMField *field) {
    return (field->type->props & ZtMFieldProp::Update) || (field->keys & 1);
  });
}
Tuple loadDelTuple(
  const ZtMFields &fields, const FBFields &fbFields, const Zfb::Table *fbo)
{
  return loadTuple(fields, fbFields, fbo, [](const ZtMField *field) {
    return (field->keys & 1);
  });
}

Offset saveTuple(
  Zfb::Builder &fbb,
  const ZtMFields &fields,
  const FBFields &fbFields,
  const Tuple &tuple)
{
  unsigned n = fields.length();
  ZmAssert(n == fbFields.length());
  Offsets offsets{ZmAlloc(Offset, n)};
  for (unsigned i = 0; i < n; i++)
    saveOffset(fbb, offsets, fbFields[i], tuple[i]);
  auto start = fbb.StartTable();
  for (unsigned i = 0; i < n; i++)
    saveValue(fbb, offsets, fbFields[i], tuple[i]);
  auto end = fbb.EndTable(start);
  return Offset{end};
}

// --- postgres OIDs, type names

class OIDs {
  using Map = ZmLHashKV<ZuString, int8_t, ZmLHashStatic<4, ZmLHashLocal<>>>;
  using Values = ZuArrayN<unsigned, Value::N - 1>;

public:
  void init(PGconn *);

  int oid(unsigned i) {
    if (i < 1 || i >= Value::N) return -1;
    return m_values[i - 1];
  }
  int oid(ZuString name) {
    int8_t i = m_map.findVal(name);
    if (ZuCmp<int8_t>::null(i) || i < 1 || i >= Value::N) return -1;
    return m_values[i - 1];
  }

private:
  unsigned resolve(PGconn *conn, ZuString name);

  Map		m_map;
  Values	m_values;
};

namespace Work {

struct Foo {
};

struct Stop {
  ZmFn<>		stopped;
};

// DB start sequence
// - MkMRD - create table if not exist

// table open sequence
// - GetTable - check field IDs and OIDs match, if not then bail
// - [MkTable] - create table if missing
// - MkIndex - for each key
//   - idempotent "create index if not exist"
//   - series indices might be different than regular indices
// - PrepFind for each key
// - PrepRecover
// - PrepIns
// - PrepUpd
// - PrepDel
// - Open - get count, max(UN), max(SN)
// - MRD - get UN, SN
// - Max - for each series key
//   ... finally we're in business
//
// GetTable query:
/*
SELECT a.attname AS name, a.atttypid AS oid
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE c.relname = 'foo'
  AND n.nspname = 'public'
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;
 name |  oid
------+--------
 a    | 343471
 */

struct MkMRD { };			// create _mrd table if not exists

struct GetTable { };			// validate table (if it exists)

struct MkTable { };			// create table (if it did not exist)

struct MkIndex {			// create index if not exists
  unsigned		keyID;		// {tableID}_{keyID}
};

struct PrepFind {			// {tableID}_find_{keyID} 
  unsigned		keyID;
};

struct PrepRecover { };			// {tableID}_recover

struct PrepInsert { };			// {tableID}_insert

struct PrepUpdate { };			// {tableID}_update

struct PrepDelete { };			// {tableID}_delete

struct Open { };

struct MRD { };

struct Max {				// {tableID}_max
  unsigned		keyID;
};

struct Find {
  unsigned		keyID;
  ZmRef<const AnyBuf>	buf;
  RowFn			rowFn;
};

struct Recover {
  UN			un;
  RowFn			rowFn;
};

struct Write {
  ZmRef<const AnyBuf>	buf;
  CommitFn		commitFn;
};

using Query = ZuUnion<
  GetTable, MkTable, MkIndex,
  PrepFind, PrepRecover, PrepInsert, PrepUpdate, PrepDelete,
  Open, MRD, Max,
  Find, Recover, Write>;

struct TblItem {
  StoreTbl	*tbl;	
  Query		query;
};

using Item = ZuUnion<Foo, Stop, MkMRD, TblItem>;

using Queue = ZmList<Item>;

} // Work

namespace OpenState {
ZtEnumValues(OpenState,
  PreOpen = 0,
  GetTable,	// validate table columns for consistency
  MkTable,	// skipped if table pre-exists and is consistent
  MkIndex,	// idempotent make indices for all keys
  PrepFind,	// prepare find for all keys
  PrepRecover,	// prepare recover query
  PrepInsert,	// prepare insert query
  PrepUpdate,	// prepare update query
  PrepDelete,	// prepare delete query
  Open,		// query count, max UN, max SN from main table
  MRD,		// query max UN, max SN from _mrd table
  Max,		// query maxima for series keys
  Opened);	// open complete
}

class StoreTbl : public Zdb_::StoreTbl {
public:
  StoreTbl(
    Store *store, ZuID id, ZtMFields fields, ZtMKeyFields keyFields,
    const reflection::Schema *schema);

  auto id() const { return m_id; }

protected:
  ~StoreTbl();

public:
  void open(MaxFn, OpenFn);
  void close(CloseFn);

  bool getTable(PGconn *);
  void getTable2(PGresult *);
  void opened();

  void warmup();

  void drop();

  void maxima(MaxFn maxFn);

  void find(unsigned keyID, ZmRef<const AnyBuf> buf, RowFn rowFn);

  void recover(UN un, RowFn rowFn);

  void write(ZmRef<const AnyBuf> buf, CommitFn commitFn);

private:
  Store			*m_store = nullptr;
  ZuID			m_id;
  ZtMFields		m_fields;
  ZtMKeyFields		m_keyFields;
  FBFields		m_fbFields;
  FBKeyFields		m_fbKeyFields;

  ZmRef<AnyBuf>		m_maxBuf;

  int			m_openState = 0;
  int			m_openKeyID = -1;// sub-state
  MaxFn			m_maxFn;	// maxima callback
  OpenFn		m_openFn;	// open callback
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

  StartResult start();
  void stop();

  void open(
    ZuID id,
    ZtMFields fields,
    ZtMKeyFields keyFields,
    const reflection::Schema *schema,
    MaxFn maxFn,
    OpenFn openFn);

  void enqueue(Work::Item item);

  template <typename ...Args> void zdbRun(Args &&... args) {
    m_mx->run(m_zdbSID, ZuFwd<Args>(args)...);
  }
  template <typename ...Args> void zdbInvoke(Args &&... args) {
    m_mx->invoke(m_zdbSID, ZuFwd<Args>(args)...);
  }

private:
  void start_();
  void stop_(ZmFn<>);
  void stop_1();
  void close_fds();

  void wake();
  void wake_();
  void run_();

  void read();
  void parse(Work::Queue::Node *, PGresult *);

  void write();

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
  Work::Queue		m_pending;	// sent, awaiting response

  OIDs			m_oids;
};

} // ZdbPQ

extern "C" {
  ZdbPQExtern Zdb_::Store *ZdbStore();
}

#endif /* ZdbPQ_HH */
