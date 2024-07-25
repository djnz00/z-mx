//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Zdb in-memory data store

#ifndef ZdbMemStore_HH
#define ZdbMemStore_HH

#ifndef ZdbLib_HH
#include <zlib/ZdbLib.hh>
#endif

#include <zlib/ZuStringN.hh>
#include <zlib/ZuPrint.hh>

#include <zlib/ZmHash.hh>

#include <zlib/ZtJoin.hh>
#include <zlib/ZtCase.hh>

#include <zlib/ZeLog.hh>

#include <zlib/Zfb.hh>
#include <zlib/ZfbField.hh>

#include <zlib/ZdbStore.hh>

namespace ZdbMem {

using namespace Zdb_;

// --- value union

// a distinct ZtArray<uint8_t> type (distinct from ZtBytes)
struct UInt8Vec : public ZtArray<uint8_t> {
  using Base = ZtArray<uint8_t>;
  using Base::Base;
  using Base::operator =;
  template <typename ...Args>
  UInt8Vec(Args &&...args) : Base{ZuFwd<Args>(args)...} { }
};

// alias types for consistent naming with UInt8Vec
using Int8Vec = ZtArray<int8_t>;
using Int16Vec = ZtArray<int16_t>;
using UInt16Vec = ZtArray<uint16_t>;
using Int32Vec = ZtArray<int32_t>;
using UInt32Vec = ZtArray<uint32_t>;
using Int64Vec = ZtArray<int64_t>;
using UInt64Vec = ZtArray<uint64_t>;
using Int128Vec = ZtArray<int128_t>;
using UInt128Vec = ZtArray<uint128_t>;

// all supported types
using Value_ = ZuUnion<
  void,
  ZtString,	// String
  ZtBytes,	// Vector<uint8_t>
  bool,
  int8_t,
  uint8_t,
  int16_t,
  uint16_t,
  int32_t,
  uint32_t,
  int64_t,
  uint64_t,
  double,
  ZuFixed,	// Zfb.Fixed
  ZuDecimal,	// Zfb.Decimal
  ZuTime,	// Zfb.Time
  ZuDateTime,	// Zfb.DateTime
  int128_t,	// Zfb.UInt128
  uint128_t,	// Zfb.Int128
  ZtBitmap,	// Zfb.Bitmap
  ZiIP,		// Zfb.IP
  ZuID,		// Zfb.ID

  // all types after this are vectors, see isVec() below
  ZtArray<ZtString>,
  ZtArray<ZtBytes>,
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
  ZtArray<double>,
  ZtArray<ZuFixed>,
  ZtArray<ZuDecimal>,
  ZtArray<ZuTime>,
  ZtArray<ZuDateTime>>;

enum { VecBase = Value_::Index<ZtArray<ZtString>>{} };

inline constexpr bool isVec(unsigned i) { return i >= VecBase; }

struct Value : public Value_ {
  using Value_::Value_;
  using Value_::operator =;

  template <unsigned I, typename S>
  ZuIfT<I == Value_::Index<void>{}>
  print_(S &s) const { }

  template <unsigned I, typename S>
  ZuIfT<I == Value_::Index<ZtString>{}>
  print_(S &s) const { s << ZtField_::Print::String{p<I>()}; }

  template <unsigned I, typename S>
  ZuIfT<I == Value_::Index<ZtBytes>{}>
  print_(S &s) const { s << ZtField_::Print::Bytes{p<I>()}; }

  template <unsigned I, typename S>
  ZuIfT<
    I == Value_::Index<bool>{} ||
    I == Value_::Index<int8_t>{} ||
    I == Value_::Index<uint8_t>{} ||
    I == Value_::Index<int16_t>{} ||
    I == Value_::Index<uint16_t>{} ||
    I == Value_::Index<int32_t>{} ||
    I == Value_::Index<uint32_t>{} ||
    I == Value_::Index<int64_t>{} ||
    I == Value_::Index<uint64_t>{} ||
    I == Value_::Index<int128_t>{} ||
    I == Value_::Index<uint128_t>{} ||
    I == Value_::Index<double>{}>
  print_(S &s) const { s << ZuBoxed(p<I>()); }

  template <unsigned I, typename S>
  ZuIfT<
    I == Value_::Index<ZuFixed>{} ||
    I == Value_::Index<ZuDecimal>{} ||
    I == Value_::Index<ZuTime>{} ||
    I == Value_::Index<ZtBitmap>{} ||
    I == Value_::Index<ZiIP>{} ||
    I == Value_::Index<ZuID>{}>
  print_(S &s) const { s << p<I>(); }

  template <unsigned I, typename S>
  ZuIfT<I == Value_::Index<ZuDateTime>{}>
  print_(S &s) const {
    auto &fmt = ZmTLS<ZuDateTimeFmt::CSV, (int Value_::*){}>();
    s << p<I>().fmt(fmt);
  }

  template <unsigned I, typename S>
  ZuIfT<I == Value_::Index<ZtArray<ZtString>>{}>
  print_(S &s) const {
    s << '[';
    bool first = true;
    p<I>().all([&s, &first](const ZtString &v) {
      if (!first) s << ','; else first = false;
      s << ZtField_::Print::String{v};
    });
    s << ']';
  }

  template <unsigned I, typename S>
  ZuIfT<I == Value_::Index<ZtArray<ZtBytes>>{}>
  print_(S &s) const {
    s << '[';
    bool first = true;
    p<I>().all([&s, &first](const ZtBytes &v) {
      if (!first) s << ','; else first = false;
      s << ZtField_::Print::Bytes{v};
    });
    s << ']';
  }

  template <unsigned I, typename S>
  ZuIfT<
    I == Value_::Index<ZtArray<int8_t>>{} ||
    I == Value_::Index<UInt8Vec>{} ||
    I == Value_::Index<ZtArray<int16_t>>{} ||
    I == Value_::Index<ZtArray<uint16_t>>{} ||
    I == Value_::Index<ZtArray<int32_t>>{} ||
    I == Value_::Index<ZtArray<uint32_t>>{} ||
    I == Value_::Index<ZtArray<int64_t>>{} ||
    I == Value_::Index<ZtArray<uint64_t>>{} ||
    I == Value_::Index<ZtArray<int128_t>>{} ||
    I == Value_::Index<ZtArray<uint128_t>>{} ||
    I == Value_::Index<ZtArray<double>>{}>
  print_(S &s) const {
    using Elem = typename ZuTraits<Type<I>>::Elem;
    s << '[';
    bool first = true;
    p<I>().all([&s, &first](const Elem &v) {
      if (!first) s << ','; else first = false;
      s << ZuBoxed(v);
    });
    s << ']';
  }

  template <unsigned I, typename S>
  ZuIfT<
    I == Value_::Index<ZtArray<ZuFixed>>{} ||
    I == Value_::Index<ZtArray<ZuDecimal>>{} ||
    I == Value_::Index<ZtArray<ZuTime>>{}>
  print_(S &s) const {
    using Elem = typename ZuTraits<Type<I>>::Elem;
    s << '[';
    bool first = true;
    p<I>().all([&s, &first](const Elem &v) {
      if (!first) s << ','; else first = false;
      s << v;
    });
    s << ']';
  }

  template <unsigned I, typename S>
  ZuIfT<I == Value_::Index<ZtArray<ZuDateTime>>{}>
  print_(S &s) const {
    s << '[';
    bool first = true;
    p<I>().all([&s, &first](const ZuDateTime &v) {
      auto &fmt = ZmTLS<ZuDateTimeFmt::CSV, (int Value_::*){}>();
      if (!first) s << ','; else first = false;
      s << v.fmt(fmt);
    });
    s << ']';
  }

  template <typename S>
  void print(S &s) const {
    ZuSwitch::dispatch<Value_::N>(this->type(), [this, &s](auto I) {
      this->print_<I>(s);
    });
  }
  friend ZuPrintFn ZuPrintType(Value *);
};

// --- extended field information

struct XField {
  const reflection::Field	*field;
  unsigned			type;	// Value union discriminator
};
using XFields = ZtArray<XField>;
using XKeyFields = ZtArray<XFields>;

// resolve Value union discriminator from flatbuffers reflection data
XField xField(
  const Zfb::Vector<Zfb::Offset<reflection::Field>> *fbFields_,
  const ZtVField *field,
  const ZtString &id)
{
  // resolve flatbuffers reflection data for field
  const reflection::Field *fbField = fbFields_->LookupByKey(id);
  if (!fbField) return {nullptr, 0};
  unsigned type = 0;
  auto ftype = field->type;
  switch (fbField->type()->base_type()) {
    case reflection::String:
      if (ftype->code == ZtFieldTypeCode::CString ||
	  ftype->code == ZtFieldTypeCode::String)
	type = Value::Index<ZtString>{};
      break;
    case reflection::Bool:
      if (ftype->code == ZtFieldTypeCode::Bool)
	type = Value::Index<bool>{};
      break;
    case reflection::Byte:
      if (ftype->code == ZtFieldTypeCode::Int8)
	type = Value::Index<int8_t>{};
      break;
    case reflection::UByte:
      if (ftype->code == ZtFieldTypeCode::UInt8)
	type = Value::Index<uint8_t>{};
      break;
    case reflection::Short:
      if (ftype->code == ZtFieldTypeCode::Int16)
	type = Value::Index<int16_t>{};
      break;
    case reflection::UShort:
      if (ftype->code == ZtFieldTypeCode::UInt16)
	type = Value::Index<uint16_t>{};
      break;
    case reflection::Int:
      if (ftype->code == ZtFieldTypeCode::Int32)
	type = Value::Index<int32_t>{};
      break;
    case reflection::UInt:
      if (ftype->code == ZtFieldTypeCode::UInt32)
	type = Value::Index<uint32_t>{};
      break;
    case reflection::Long:
      if (ftype->code == ZtFieldTypeCode::Int64)
	type = Value::Index<int64_t>{};
      break;
    case reflection::ULong:
      if (ftype->code == ZtFieldTypeCode::UInt64)
	type = Value::Index<uint64_t>{};
      break;
    case reflection::Double:
      if (ftype->code == ZtFieldTypeCode::Float)
	type = Value::Index<double>{};
      break;
    case reflection::Obj: {
      switch (ftype->code) {
	case ZtFieldTypeCode::Int128:
	  type = Value::Index<int128_t>{};
	  break;
	case ZtFieldTypeCode::UInt128:
	  type = Value::Index<uint128_t>{};
	  break;
	case ZtFieldTypeCode::Fixed:
	  type = Value::Index<ZuFixed>{};
	  break;
	case ZtFieldTypeCode::Decimal:
	  type = Value::Index<ZuDecimal>{};
	  break;
	case ZtFieldTypeCode::Time:
	  type = Value::Index<ZuTime>{};
	  break;
	case ZtFieldTypeCode::DateTime:
	  type = Value::Index<ZuDateTime>{};
	  break;
	case ZtFieldTypeCode::UDT: {
	  ZuID typeID = ftype->info.udt()->id;
	  if (typeID == ZuID("Bitmap")) {
	    type = Value::Index<ZtBitmap>{};
	    break;
	  }
	  if (typeID == ZuID("IP")) {
	    type = Value::Index<ZiIP>{};
	    break;
	  }
	  if (typeID == ZuID("ID")) {
	    type = Value::Index<ZuID>{};
	    break;
	  }
	}
      }
    } break;
    case reflection::Vector:
      switch (fbField->type()->element()) {
	default: break;
	case reflection::String:
	  if (ftype->code == ZtFieldTypeCode::StringVec)
	    type = Value::Index<ZtArray<ZtString>>{};
	  break;
	case reflection::Byte:
	  if (ftype->code == ZtFieldTypeCode::Int8Vec)
	    type = Value::Index<ZtArray<int8_t>>{};
	  break;
	case reflection::UByte:
	  if (ftype->code == ZtFieldTypeCode::Bytes)
	    type = Value::Index<ZtBytes>{};
	  else if (ftype->code == ZtFieldTypeCode::UInt8Vec)
	    type = Value::Index<UInt8Vec>{};
	  break;
	case reflection::Short:
	  if (ftype->code == ZtFieldTypeCode::Int16Vec)
	    type = Value::Index<ZtArray<int16_t>>{};
	  break;
	case reflection::UShort:
	  if (ftype->code == ZtFieldTypeCode::UInt16Vec)
	    type = Value::Index<ZtArray<uint16_t>>{};
	  break;
	case reflection::Int:
	  if (ftype->code == ZtFieldTypeCode::Int32Vec)
	    type = Value::Index<ZtArray<int32_t>>{};
	  break;
	case reflection::UInt:
	  if (ftype->code == ZtFieldTypeCode::UInt32Vec)
	    type = Value::Index<ZtArray<uint32_t>>{};
	  break;
	case reflection::Long:
	  if (ftype->code == ZtFieldTypeCode::Int64Vec)
	    type = Value::Index<ZtArray<int64_t>>{};
	  break;
	case reflection::ULong:
	  if (ftype->code == ZtFieldTypeCode::UInt64Vec)
	    type = Value::Index<ZtArray<uint64_t>>{};
	  break;
	case reflection::Double:
	  if (ftype->code == ZtFieldTypeCode::FloatVec)
	    type = Value::Index<ZtArray<double>>{};
	  break;
	case reflection::Obj:
	  switch (ftype->code) {
	    case ZtFieldTypeCode::BytesVec:
	      type = Value::Index<ZtArray<ZtBytes>>{};
	      break;
	    case ZtFieldTypeCode::Int128Vec:
	      type = Value::Index<ZtArray<int128_t>>{};
	      break;
	    case ZtFieldTypeCode::UInt128Vec:
	      type = Value::Index<ZtArray<uint128_t>>{};
	      break;
	    case ZtFieldTypeCode::FixedVec:
	      type = Value::Index<ZtArray<ZuFixed>>{};
	      break;
	    case ZtFieldTypeCode::DecimalVec:
	      type = Value::Index<ZtArray<ZuDecimal>>{};
	      break;
	    case ZtFieldTypeCode::TimeVec:
	      type = Value::Index<ZtArray<ZuTime>>{};
	      break;
	    case ZtFieldTypeCode::DateTimeVec:
	      type = Value::Index<ZtArray<ZuDateTime>>{};
	      break;
	  }
	  break;
      }
      break;
    default:
      break;
  }
  return {fbField, type};
}

// --- load value from flatbuffer

template <unsigned Type>
inline ZuIfT<Type == Value::Index<void>{}>
loadValue(void *, const reflection::Field *, const Zfb::Table *) { }

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtString>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZtString{Zfb::Load::str(Zfb::GetFieldS(*fbo, *field))};
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtBytes>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZtBytes{Zfb::Load::bytes(Zfb::GetFieldV<uint8_t>(*fbo, *field))};
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<bool>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  *static_cast<bool *>(ptr) = Zfb::GetFieldI<bool>(*fbo, *field);
}

#define zdbtest_LoadInt(width) \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<int##width##_t>{}> \
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) { \
  *static_cast<int##width##_t *>(ptr) = \
    Zfb::GetFieldI<int##width##_t>(*fbo, *field); \
} \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<uint##width##_t>{}> \
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) { \
  *static_cast<uint##width##_t *>(ptr) = \
  Zfb::GetFieldI<uint##width##_t>(*fbo, *field); \
}

zdbtest_LoadInt(8)
zdbtest_LoadInt(16)
zdbtest_LoadInt(32)
zdbtest_LoadInt(64)

template <unsigned Type>
inline ZuIfT<Type == Value::Index<double>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  *static_cast<double *>(ptr) = Zfb::GetFieldF<double>(*fbo, *field);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuFixed>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZuFixed{
    Zfb::Load::fixed(fbo->GetPointer<const Zfb::Fixed *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuDecimal>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZuDecimal{
    Zfb::Load::decimal(fbo->GetPointer<const Zfb::Decimal *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuTime>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZuTime{
    Zfb::Load::time(fbo->GetPointer<const Zfb::Time *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuDateTime>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZuDateTime{
    Zfb::Load::dateTime(
      fbo->GetPointer<const Zfb::DateTime *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<int128_t>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  *static_cast<int128_t *>(ptr) =
    Zfb::Load::int128(fbo->GetPointer<const Zfb::Int128 *>(field->offset()));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<uint128_t>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  *static_cast<uint128_t *>(ptr) =
    Zfb::Load::uint128(fbo->GetPointer<const Zfb::UInt128 *>(field->offset()));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtBitmap>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZtBitmap{
    Zfb::Load::bitmap<ZtBitmap>(
      fbo->GetPointer<const Zfb::Bitmap *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZiIP>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZiIP{
    Zfb::Load::ip(fbo->GetPointer<const Zfb::IP *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuID>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZuID{
    Zfb::Load::id(fbo->GetPointer<const Zfb::ID *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtArray<ZtString>>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  auto v = Zfb::GetFieldV<Zfb::Offset<Zfb::String>>(*fbo, *field);
  unsigned n = v ? v->size() : 0;
  auto array = new (ptr) ZtArray<ZtString>(n);
  for (unsigned i = 0; i < n; i++) array->push(Zfb::Load::str(v->Get(i)));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtArray<ZtBytes>>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  auto v = Zfb::GetFieldV<Zfb::Offset<Zfb::Vector<uint8_t>>>(*fbo, *field);
  unsigned n = v ? v->size() : 0;
  auto array = new (ptr) ZtArray<ZtBytes>(n);
  for (unsigned i = 0; i < n; i++) array->push(Zfb::Load::bytes(v->Get(i)));
}

#define zdbtest_LoadIntVec(width) \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<Int##width##Vec>{}> \
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) { \
  auto v = Zfb::GetFieldV<int##width##_t>(*fbo, *field); \
  unsigned n = v ? v->size() : 0; \
  auto array = new (ptr) Int##width##Vec(n); \
  for (unsigned i = 0; i < n; i++) array->push(v->Get(i)); \
} \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<UInt##width##Vec>{}> \
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) { \
  auto v = Zfb::GetFieldV<uint##width##_t>(*fbo, *field); \
  unsigned n = v ? v->size() : 0; \
  auto array = new (ptr) UInt##width##Vec(n); \
  for (unsigned i = 0; i < n; i++) array->push(v->Get(i)); \
}

zdbtest_LoadIntVec(8)
zdbtest_LoadIntVec(16)
zdbtest_LoadIntVec(32)
zdbtest_LoadIntVec(64)

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Int128Vec>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  auto v = Zfb::GetFieldV<Zfb::Int128 *>(*fbo, *field);
  unsigned n = v ? v->size() : 0;
  auto array = new (ptr) Int128Vec(n);
  for (unsigned i = 0; i < n; i++) array->push(Zfb::Load::int128(v->Get(i)));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<UInt128Vec>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  auto v = Zfb::GetFieldV<Zfb::UInt128 *>(*fbo, *field);
  unsigned n = v ? v->size() : 0;
  auto array = new (ptr) UInt128Vec(n);
  for (unsigned i = 0; i < n; i++) array->push(Zfb::Load::uint128(v->Get(i)));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtArray<double>>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  auto v = Zfb::GetFieldV<double>(*fbo, *field);
  unsigned n = v ? v->size() : 0;
  auto array = new (ptr) ZtArray<double>(n);
  for (unsigned i = 0; i < n; i++) array->push(v->Get(i));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtArray<ZuFixed>>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  auto v = Zfb::GetFieldV<Zfb::Fixed *>(*fbo, *field);
  unsigned n = v ? v->size() : 0;
  auto array = new (ptr) ZtArray<ZuFixed>(n);
  for (unsigned i = 0; i < n; i++)
    array->push(Zfb::Load::fixed(v->Get(i)));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtArray<ZuDecimal>>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  auto v = Zfb::GetFieldV<Zfb::Decimal *>(*fbo, *field);
  unsigned n = v ? v->size() : 0;
  auto array = new (ptr) ZtArray<ZuDecimal>(n);
  for (unsigned i = 0; i < n; i++)
    array->push(Zfb::Load::decimal(v->Get(i)));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtArray<ZuTime>>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  auto v = Zfb::GetFieldV<Zfb::Time *>(*fbo, *field);
  unsigned n = v ? v->size() : 0;
  auto array = new (ptr) ZtArray<ZuTime>(n);
  for (unsigned i = 0; i < n; i++)
    array->push(Zfb::Load::time(v->Get(i)));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtArray<ZuDateTime>>{}>
loadValue(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  auto v = Zfb::GetFieldV<Zfb::DateTime *>(*fbo, *field);
  unsigned n = v ? v->size() : 0;
  auto array = new (ptr) ZtArray<ZuDateTime>(n);
  for (unsigned i = 0; i < n; i++)
    array->push(Zfb::Load::dateTime(v->Get(i)));
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
inline ZuIfT<Type == Value::Index<ZtString>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  offsets.push(Zfb::Save::str(fbb, value.p<Type>()).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtBytes>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  offsets.push(Zfb::Save::bytes(fbb, value.p<Type>()).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtArray<ZtString>>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  const auto &array = value.p<ZtArray<ZtString>>();
  unsigned n = array.length();
  offsets.push(
    Zfb::Save::strVecIter(fbb, n, [&array](unsigned i) {
      return array[i];
    }).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtArray<ZtBytes>>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  const auto &array = value.p<ZtArray<ZtBytes>>();
  unsigned n = array.length();
  offsets.push(Zfb::Save::vectorIter<Zfb::Vector<uint8_t>>(fbb, n,
    [&array](Zfb::Builder &fbb, unsigned i) {
      return Zfb::Save::bytes(fbb, array[i]);
    }).Union());
}

#define ZdbPQ_SaveIntVec(width) \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<Int##width##Vec>{}> \
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value) \
{ \
  const auto &array = value.p<Int##width##Vec>(); \
  unsigned n = array.length(); \
  offsets.push(Zfb::Save::pvectorIter<int##width##_t>( \
      fbb, n, [&array](unsigned i) { return array[i]; }).Union()); \
} \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<UInt##width##Vec>{}> \
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value) \
{ \
  const auto &array = value.p<UInt##width##Vec>(); \
  unsigned n = array.length(); \
  offsets.push(Zfb::Save::pvectorIter<uint##width##_t>( \
      fbb, n, [&array](unsigned i) { return array[i]; }).Union()); \
}

ZdbPQ_SaveIntVec(8)
ZdbPQ_SaveIntVec(16)
ZdbPQ_SaveIntVec(32)
ZdbPQ_SaveIntVec(64)

template <unsigned Type>
inline ZuIfT<Type == Value::Index<Int128Vec>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  const auto &array = value.p<Int128Vec>();
  unsigned n = array.length();
  offsets.push(Zfb::Save::structVecIter<Zfb::Int128>(fbb, n,
    [&array](Zfb::Int128 *ptr, unsigned i) {
      *ptr = Zfb::Save::int128(array[i]);
    }).Union());
}
template <unsigned Type>
inline ZuIfT<Type == Value::Index<UInt128Vec>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  const auto &array = value.p<UInt128Vec>();
  unsigned n = array.length();
  offsets.push(Zfb::Save::structVecIter<Zfb::UInt128>(fbb, n,
    [&array](Zfb::UInt128 *ptr, unsigned i) {
      *ptr = Zfb::Save::uint128(array[i]);
    }).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtArray<double>>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  const auto &array = value.p<ZtArray<double>>();
  unsigned n = array.length();
  offsets.push(Zfb::Save::pvectorIter<double>(fbb, n, [&array](unsigned i) {
    return array[i];
  }).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtArray<ZuFixed>>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  const auto &array = value.p<ZtArray<ZuFixed>>();
  unsigned n = array.length();
  offsets.push(Zfb::Save::structVecIter<Zfb::Fixed>(fbb, n,
    [&array](Zfb::Fixed *ptr, unsigned i) {
      *ptr = Zfb::Save::fixed(array[i]);
    }).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtArray<ZuDecimal>>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  const auto &array = value.p<ZtArray<ZuDecimal>>();
  unsigned n = array.length();
  offsets.push(Zfb::Save::structVecIter<Zfb::Decimal>(fbb, n,
    [&array](Zfb::Decimal *ptr, unsigned i) {
      *ptr = Zfb::Save::decimal(array[i]);
    }).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtArray<ZuTime>>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  const auto &array = value.p<ZtArray<ZuTime>>();
  unsigned n = array.length();
  offsets.push(Zfb::Save::structVecIter<Zfb::Time>(fbb, n,
    [&array](Zfb::Time *ptr, unsigned i) {
      *ptr = Zfb::Save::time(array[i]);
    }).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtArray<ZuDateTime>>{}>
saveOffset(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  const auto &array = value.p<ZtArray<ZuDateTime>>();
  unsigned n = array.length();
  offsets.push(Zfb::Save::structVecIter<Zfb::DateTime>(fbb, n,
    [&array](Zfb::DateTime *ptr, unsigned i) {
      *ptr = Zfb::Save::dateTime(array[i]);
    }).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtBitmap>{}>
saveOffset(
  Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  offsets.push(Zfb::Save::bitmap(fbb, value.p<Type>()).Union());
}

template <unsigned Type>
inline ZuIfT<
  Type != Value::Index<ZtString>{} &&
  Type != Value::Index<ZtBytes>{} &&
  Type != Value::Index<ZtBitmap>{} &&
  !isVec(Type)>
saveOffset(Zfb::Builder &, Offsets &, const Value &) { }

template <unsigned Type>
inline ZuIfT<Type == Value::Index<void>{}>
saveValue(
  Zfb::Builder &, const Offsets &,
  const reflection::Field *, const Value &) { }

template <unsigned Type>
inline ZuIfT<
  Type == Value::Index<ZtString>{} ||
  Type == Value::Index<ZtBytes>{} ||
  Type == Value::Index<ZtBitmap>{} ||
  isVec(Type)>
saveValue(
  Zfb::Builder &fbb, const Offsets &offsets,
  const reflection::Field *field, const Value &value)
{
  fbb.AddOffset(field->offset(), offsets.shift());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<bool>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  fbb.AddElement<bool>(
    field->offset(), value.p<Type>(), field->default_integer());
}

#define zdbtest_SaveInt(width) \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<int##width##_t>{}> \
saveValue( \
  Zfb::Builder &fbb, const Offsets &, \
  const reflection::Field *field, const Value &value) \
{ \
  fbb.AddElement<int##width##_t>( \
    field->offset(), value.p<Type>(), field->default_integer()); \
} \
template <unsigned Type> \
inline ZuIfT<Type == Value::Index<uint##width##_t>{}> \
saveValue( \
  Zfb::Builder &fbb, const Offsets &, \
  const reflection::Field *field, const Value &value) \
{ \
  fbb.AddElement<uint##width##_t>( \
    field->offset(), value.p<Type>(), field->default_integer()); \
}

zdbtest_SaveInt(8)
zdbtest_SaveInt(16)
zdbtest_SaveInt(32)
zdbtest_SaveInt(64)

template <unsigned Type>
inline ZuIfT<Type == Value::Index<double>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  fbb.AddElement<double>(
    field->offset(), value.p<Type>(), field->default_real());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuFixed>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::fixed(value.p<Type>());
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuDecimal>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::decimal(value.p<Type>());
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuTime>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::time(value.p<Type>());
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuDateTime>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::dateTime(value.p<Type>());
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<int128_t>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::int128(value.p<Type>());
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<uint128_t>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::uint128(value.p<Type>());
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZiIP>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::ip(value.p<Type>());
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuID>{}>
saveValue(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::id(value.p<Type>());
  fbb.AddStruct(field->offset(), &v);
}

// --- data tuple

using Tuple = ZtArray<Value>;

// loadTuple() and saveTuple() rely on tuples being a full row
// of values, i.e. tuples.length() == fields.length() == xFields.length()
// (individual elements of the tuple can be null values)

// load tuple from flatbuffer
// - when called from select(), nParams is < fields.length()
template <typename Filter>
Tuple loadTuple_(
  unsigned nParams,
  const ZtVFieldArray &fields,
  const XFields &xFields,
  const Zfb::Table *fbo,
  Filter filter)
{
  Tuple tuple(nParams); // not {}
  for (unsigned i = 0; i < nParams; i++)
    if (filter(fields[i])) {
      auto value = static_cast<Value *>(tuple.push());
      auto type = xFields[i].type;
      ZuSwitch::dispatch<Value::N>(type,
	[value, field = xFields[i].field, fbo](auto I) {
	  loadValue<I>(value->new_<I, true>(), field, fbo);
	});
    } else
      new (tuple.push()) Value{};
  return tuple;
}
Tuple loadTuple_(
  unsigned nParams,
  const ZtVFieldArray &fields,
  const XFields &xFields,
  const Zfb::Table *fbo)
{
  return loadTuple_(nParams, fields, xFields, fbo,
    [](const ZtVField *) { return true; });
}
Tuple loadTuple(
  const ZtVFieldArray &fields, const XFields &xFields, const Zfb::Table *fbo)
{
  return loadTuple_(fields.length(), fields, xFields, fbo);
}
Tuple loadUpdTuple(
  const ZtVFieldArray &fields, const XFields &xFields, const Zfb::Table *fbo)
{
  return loadTuple_(fields.length(), fields, xFields, fbo,
    [](const ZtVField *field) -> bool {
      return bool(field->props & ZtVFieldProp::Mutable()) || (field->keys & 1);
    });
}
Tuple loadDelTuple(
  const ZtVFieldArray &fields, const XFields &xFields, const Zfb::Table *fbo)
{
  return loadTuple_(fields.length(), fields, xFields, fbo,
    [](const ZtVField *field) -> bool { return (field->keys & 1); });
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

// update tuple
void updTuple(const ZtVFieldArray &fields, Tuple &data, Tuple &&update) {
  ZmAssert(fields.length() == data.length());
  ZmAssert(data.length() == update.length());
  unsigned n = data.length();
  for (unsigned i = 0; i < n; i++)
    if (fields[i]->props & ZtVFieldProp::Mutable()) {
      ZmAssert(update[i].type());
      data[i] = ZuMv(update[i]);
    }
}

// extract key from tuple
Tuple extractKey(
  const ZtVFieldArray &fields,
  const ZtVKeyFieldArray &keyFields,
  unsigned keyID, const Tuple &data)
{
  ZmAssert(fields.length() == data.length());
  Tuple key(keyFields[keyID].length()); // not {}
  unsigned m = fields.length();
  for (unsigned j = 0; j < m; j++)
    if (fields[j]->keys & (uint64_t(1)<<keyID)) key.push(data[j]);
  ZmAssert(key.length() == key.size());
  return key;
}

// --- in-memory row

struct MemRow__ {
  ZdbUN		un;
  ZdbSN		sn;
  ZdbVN		vn;
  Tuple		data;

  static ZdbUN UNAxor(const MemRow__ &row) { return row.un; }
};

// UN index
struct MemRow_ : public ZuObject, public MemRow__ {
  using MemRow__::MemRow__;
  template <typename ...Args>
  MemRow_(Args &&...args) : MemRow__{ZuFwd<Args>(args)...} { }
};
inline constexpr const char *Row_HeapID() { return "MemRow"; }
using IndexUN =
  ZmRBTree<MemRow_,
    ZmRBTreeNode<MemRow_,
      ZmRBTreeKey<MemRow_::UNAxor,
	ZmRBTreeUnique<true,
	  ZmRBTreeHeapID<Row_HeapID>>>>>;
struct MemRow : public IndexUN::Node {
  using Base = IndexUN::Node;
  using Base::Base;
  using MemRow__::data;
};

// key indices
// - override the default comparator to provide in-memory indices
//   that mimic a RDBMS B-Tree ascending/descending indices
inline bool equals_(const Tuple &l, const Tuple &r, unsigned n) {
  for (unsigned i = 0; i < n; i++)
    if (!l[i].equals(r[i])) return false;
  return true;
}
template <typename T = Tuple> struct TupleCmp {
  uint64_t	descending;

  int cmp(const T &l, const T &r) const {
    unsigned ln = l.length();
    unsigned rn = r.length();
    unsigned i, n = ln < rn ? ln : rn;
    for (i = 0; i < n; i++) {
      if (int j = l[i].cmp(r[i])) {
	if (descending & (uint64_t(1)<<i)) j = -j;
	return j;
      }
    }
    return ZuCmp<int>::cmp(ln, rn);
  }
  static bool equals(const T &l, const T &r) {
    unsigned ln = l.length();
    unsigned rn = r.length();
    return equals_(l, r, ln < rn ? ln : rn);
  }
};
inline constexpr const char *MemRowIndex_HeapID() { return "MemRowIndex"; }
using Index =
  ZmRBTreeKV<Tuple, ZmRef<const MemRow>,
    ZmRBTreeCmp<TupleCmp,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<MemRowIndex_HeapID>>>>;

// --- mock storeTbl

class StoreTbl : public Zdb_::StoreTbl {
public:
  StoreTbl(
    ZuID id, ZtVFieldArray fields, ZtVKeyFieldArray keyFields,
    const reflection::Schema *schema, IOBufAllocFn bufAllocFn
  ) :
    m_id{id},
    m_fields{ZuMv(fields)}, m_keyFields{ZuMv(keyFields)},
    m_bufAllocFn{ZuMv(bufAllocFn)}
  {
    // introspect fields and flatbuffers reflection data, building
    // m_xFields[], m_keyGroup[] and m_xKeyFields[]
    for (unsigned i = 0, n = keyFields.length(); i < n; i++) {
      uint64_t descending = 0;
      unsigned m = keyFields[i].length();
      ZmAssert(m < 64);
      for (unsigned j = 0; j < m; j++)
	if (keyFields[i][j]->descend & (uint64_t(1)<<i))
	  descending |= (uint64_t(1)<<j);
      new (m_indices.push()) Index{TupleCmp<>{descending}};
    }
    const reflection::Object *rootTbl = schema->root_table();
    const Zfb::Vector<Zfb::Offset<reflection::Field>> *fbFields_ =
      rootTbl->fields();
    unsigned n = m_fields.length();
    m_xFields.size(n);
    for (unsigned i = 0; i < n; i++)
      ZtCase::camelSnake(m_fields[i]->id,
	[this, fbFields_, i](const ZtString &id) {
	  m_xFields.push(xField(fbFields_, m_fields[i], id));
	});
    n = m_keyFields.length();
    m_xKeyFields.size(n);
    m_keyGroup.length(n);
    for (unsigned i = 0; i < n; i++) {
      unsigned m = m_keyFields[i].length();
      new (m_xKeyFields.push()) XFields{m};
      m_keyGroup[i] = 0;
      for (unsigned j = 0; j < m; j++) {
	if (m_keyFields[i][j]->group & (uint64_t(1)<<i))
	  m_keyGroup[i] = j + 1;
	ZtCase::camelSnake(m_keyFields[i][j]->id,
	  [this, fbFields_, i, j](const ZtString &id) {
	    m_xKeyFields[i].push(xField(fbFields_, m_keyFields[i][j], id));
	  });
      }
    }
  }

  auto id() const { return m_id; }

  bool opened() const { return m_opened; }

  auto count() const { return m_indices[0].count_(); }
  auto maxUN() const { return m_maxUN; }
  auto maxSN() const { return m_maxSN; }

protected:
  ~StoreTbl() = default;

private:
  // load a row from a buffer containing a replication/recovery message
  ZmRef<const MemRow> loadRow(const ZmRef<const IOBuf> &buf) {
    auto record = record_(msg_(buf->hdr()));
    auto sn = Zfb::Load::uint128(record->sn());
    auto data = Zfb::Load::bytes(record->data());
    auto fbo = Zfb::GetAnyRoot(data.data());
    Tuple tuple;
    if (!record->vn())
      tuple = loadTuple(m_fields, m_xFields, fbo);
    else if (record->vn() > 0)
      tuple = loadUpdTuple(m_fields, m_xFields, fbo);
    else
      tuple = loadDelTuple(m_fields, m_xFields, fbo);
    return new MemRow{record->un(), sn, record->vn(), ZuMv(tuple)};
  }

  // save a row to a buffer as a replication/recovery message
  template <bool Recovery>
  ZmRef<IOBuf> saveRow(const ZmRef<const MemRow> &row) {
    Zfb::IOBuilder fbb{m_bufAllocFn()};
    auto data = Zfb::Save::nest(fbb, [this, &row](Zfb::Builder &fbb) {
      return saveTuple(fbb, m_xFields, row->data);
    });
    {
      auto id = Zfb::Save::id(this->id());
      auto sn = Zfb::Save::uint128(row->sn);
      auto msg = fbs::CreateMsg(
	fbb, Recovery ? fbs::Body::Recovery : fbs::Body::Replication,
	fbs::CreateRecord(fbb, &id, row->un, &sn, row->vn, data).Union()
      );
      fbb.Finish(msg);
    }
    return saveHdr(fbb);
  }

public:
  void open() { m_opened = true; }
  void close(CloseFn fn) { m_opened = false; fn(); }

  void warmup() { }

  void count(unsigned keyID, ZmRef<const IOBuf>, CountFn);

  void select(
    bool selectRow, bool selectNext, bool inclusive,
    unsigned keyID, ZmRef<const IOBuf>,
    unsigned limit, TupleFn);

  void find(unsigned keyID, ZmRef<const IOBuf>, RowFn);

  void recover(UN, RowFn);

  void write(ZmRef<const IOBuf>, CommitFn);

private:
  void insert(ZmRef<MemRow>, ZmRef<const IOBuf>, CommitFn);
  void update(ZmRef<MemRow>, ZmRef<const IOBuf>, CommitFn);
  void del(ZmRef<MemRow>, ZmRef<const IOBuf>, CommitFn);

private:
  ZuID			m_id;
  ZtVFieldArray		m_fields;
  ZtVKeyFieldArray		m_keyFields;
  XFields		m_xFields;
  XKeyFields		m_xKeyFields;
  ZtArray<unsigned>	m_keyGroup;	// length of group key, 0 if none
  IndexUN		m_indexUN;
  ZtArray<Index>	m_indices;
  IOBufAllocFn		m_bufAllocFn;

  bool			m_opened = false;

  UN			m_maxUN = ZdbNullUN();
  SN			m_maxSN = ZdbNullSN();
};

// --- in-memory data store

template <typename StoreTbl_>
inline ZuID StoreTbl_IDAxor(const StoreTbl_ &tbl) { return tbl.id(); }
inline constexpr const char *StoreTbls_HeapID() { return "StoreTbls"; }
template <typename StoreTbl_>
using StoreTbls_ =
  ZmHash<StoreTbl_,
    ZmHashNode<StoreTbl_,
      ZmHashKey<StoreTbl_IDAxor<StoreTbl_>,
	ZmHashLock<ZmPLock,
	  ZmHashHeapID<StoreTbls_HeapID>>>>>;

template <typename StoreTbl_>
class Store_ : public Zdb_::Store {
  using StoreTbl = StoreTbl_;
  using StoreTbls = StoreTbls_<StoreTbl>;
  using StoreTblNode = typename StoreTbls::Node;

public:
  InitResult init(ZvCf *cf, ZiMultiplex *mx, FailFn failFn) {
    if (!m_storeTbls) m_storeTbls = new StoreTbls{};
    m_mx = mx;
    m_failFn = ZuMv(failFn);
    return {InitData{.replicated = false}};

    try {
      const ZtString &tid = cf->get<true>("thread");
      auto sid = m_mx->sid(tid);
      if (!sid ||
	  sid > m_mx->params().nThreads() ||
	  sid == m_mx->rxThread() ||
	  sid == m_mx->txThread())
	return {ZeVEVENT(Fatal, ([tid = ZtString{tid}](auto &s, const auto &) {
	  s << "Store::init() failed: invalid thread configuration \""
	    << tid << '"';
	}))};
      m_sid = sid;
    } catch (const ZvError &e_) {
      ZtString e;
      e << e_;
      return {ZeVEVENT(Fatal, ([e = ZuMv(e)](auto &s, const auto &) {
	s << "Store::init() failed: invalid configuration: " << e;
      }))};
    }
  }
  void final() {
    m_failFn = FailFn{};
    if (!m_preserve) {
      m_storeTbls->clean();
      m_storeTbls = nullptr;
    }
  }

  void fail(ZeVEvent e) { m_failFn(ZuMv(e)); } // simulate async store failure

  void preserve() { m_preserve = true; }

  void open(
      ZuID id,
      ZtVFieldArray fields,
      ZtVKeyFieldArray keyFields,
      const reflection::Schema *schema,
      IOBufAllocFn bufAllocFn,
      OpenFn openFn) {
    StoreTblNode *storeTbl = m_storeTbls->find(id);
    if (storeTbl && storeTbl->opened()) {
      openFn(OpenResult{ZeVEVENT(Error, ([id](auto &s, const auto &) {
	s << "open(" << id << ") failed - already open";
      }))});
      return;
    }
    if (!storeTbl) {
      storeTbl = new StoreTblNode{
	id, ZuMv(fields), ZuMv(keyFields), schema, ZuMv(bufAllocFn)};
      m_storeTbls->addNode(storeTbl);
    }
    storeTbl->open();
    openFn(OpenResult{OpenData{
      .storeTbl = storeTbl,
      .count = storeTbl->count(),
      .un = storeTbl->maxUN(),
      .sn = storeTbl->maxSN()
    }});
  }

  template <typename ...Args> void run(Args &&...args) {
    m_mx->run(m_sid, ZuFwd<Args>(args)...);
  }
  template <typename ...Args> void invoke(Args &&...args) {
    m_mx->invoke(m_sid, ZuFwd<Args>(args)...);
  }

private:
  ZmRef<StoreTbls>	m_storeTbls;
  ZiMultiplex		*m_mx = nullptr;
  unsigned		m_sid = ZuCmp<unsigned>::null();
  FailFn		m_failFn;
  bool			m_preserve = false;
};

using Store = Store_<StoreTbl>;

} // ZdbMem

// main data store driver entry point
extern "C" {
  ZdbExtern Zdb_::Store *ZdbStore();
}

#endif /* ZdbMemStore_HH */
