//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#ifndef ZdbMockStore_HH
#define ZdbMockStore_HH

#include <zlib/ZuStringN.hh>
#include <zlib/ZuPrint.hh>

#include <zlib/ZmHash.hh>

#include <zlib/ZtJoin.hh>
#include <zlib/ZtCase.hh>

#include <zlib/ZeLog.hh>

#include <zlib/Zfb.hh>
#include <zlib/ZfbField.hh>

#include <zlib/ZdbStore.hh>

namespace zdbtest {

using namespace Zdb_;

ZmXRing<ZmFn<>, ZmXRingLock<ZmPLock>> work, callbacks;

bool deferWork = false, deferCallbacks = false;

void performWork() { while (auto fn = work.shift()) fn(); }
void performCallbacks() { while (auto fn = callbacks.shift()) fn(); }

// --- value union

// a distinct int type for Enum
struct Enum : public ZuBox<int> {
  using Base = ZuBox<int>;
  using Base::Base;
  using Base::operator =;
  template <typename ...Args>
  Enum(Args &&...args) : Base{ZuFwd<Args>(args)...} { }
};

// a distinct uint64_t type for Flags
struct Flags : public ZuBox<uint64_t> {
  using Base = ZuBox<uint64_t>;
  using Base::Base;
  using Base::operator =;
  template <typename ...Args>
  Flags(Args &&...args) :
      Base{ZuFwd<Args>(args)...} { }
};

// all supported types
using Value_ = ZuUnion<
  void,
  ZtString,	// String
  ZtBytes,	// Vector<uint8_t>
  bool,
  int64_t,
  uint64_t,
  Enum,		// int
  Flags,	// uint64_t
  double,
  ZuFixed,	// Zfb.Fixed
  ZuDecimal,	// Zfb.Decimal
  ZuTime,	// Zfb.Time
  ZuDateTime,	// Zfb.DateTime
  int128_t,	// Zfb.UInt128
  uint128_t,	// Zfb.Int128
  ZiIP,		// Zfb.IP
  ZuID>;	// Zfb.ID

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

// --- extended field information

struct XField {
  const reflection::Field	*field;
  unsigned			type;	// Value union discriminator
};
using XFields = ZtArray<XField>;
using XKeyFields = ZtArray<XFields>;

// resolve Value union discriminator from field metadata
XField xField(
  const Zfb::Vector<Zfb::Offset<reflection::Field>> *fbFields_,
  const ZtMField *field,
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
    case reflection::Short:
    case reflection::Int:
    case reflection::Long:
      if (ftype->code == ZtFieldTypeCode::Int) {
	type = Value::Index<int64_t>{};
      } else if (ftype->code == ZtFieldTypeCode::Enum) {
	type = Value::Index<Enum>{};
      }
      break;
    case reflection::UByte:
    case reflection::UShort:
    case reflection::UInt:
    case reflection::ULong:
      if (ftype->code == ZtFieldTypeCode::UInt) {
	type = Value::Index<uint64_t>{};
      } else if (ftype->code == ZtFieldTypeCode::Flags) {
	type = Value::Index<Flags>{};
      }
      break;
    case reflection::Float:
    case reflection::Double:
      if (ftype->code == ZtFieldTypeCode::Float)
	type = Value::Index<double>{};
      break;
    case reflection::Obj: {
      switch (ftype->code) {
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
	  auto ftindex = std::type_index{*(ftype->info.udt()->info)};
	  if (ftindex == std::type_index{typeid(int128_t)}) {
	    type = Value::Index<int128_t>{};
	    break;
	  }
	  if (ftindex == std::type_index{typeid(uint128_t)}) {
	    type = Value::Index<uint128_t>{};
	    break;
	  }
	  if (ftindex == std::type_index{typeid(ZiIP)}) {
	    type = Value::Index<ZiIP>{};
	    break;
	  }
	  if (ftindex == std::type_index{typeid(ZuID)}) {
	    type = Value::Index<ZuID>{};
	    break;
	  }
	}
      }
    } break;
    default:
      break;
  }
  return {fbField, type};
}

// --- load value from flatbuffer

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtString>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZtString{Zfb::Load::str(Zfb::GetFieldS(*fbo, *field))};
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtBytes>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZtBytes{Zfb::Load::bytes(Zfb::GetFieldV<uint8_t>(*fbo, *field))};
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<bool>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  *static_cast<bool *>(ptr) = Zfb::GetFieldI<bool>(*fbo, *field);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<int64_t>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  switch (field->type()->base_type()) {
    case reflection::Bool:
      *static_cast<int64_t *>(ptr) = Zfb::GetFieldI<uint8_t>(*fbo, *field);
      break;
    case reflection::Byte:
      *static_cast<int64_t *>(ptr) = Zfb::GetFieldI<int8_t>(*fbo, *field);
      break;
    case reflection::Short:
      *static_cast<int64_t *>(ptr) = Zfb::GetFieldI<int16_t>(*fbo, *field);
      break;
    case reflection::Int:
      *static_cast<int64_t *>(ptr) = Zfb::GetFieldI<int32_t>(*fbo, *field);
      break;
    case reflection::Long:
      *static_cast<int64_t *>(ptr) = Zfb::GetFieldI<int64_t>(*fbo, *field);
      break;
    default:
      break;
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<uint64_t>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  switch (field->type()->base_type()) {
    case reflection::Bool:
      *static_cast<uint64_t *>(ptr) = Zfb::GetFieldI<uint8_t>(*fbo, *field);
      break;
    case reflection::UByte:
      *static_cast<uint64_t *>(ptr) = Zfb::GetFieldI<uint8_t>(*fbo, *field);
      break;
    case reflection::UShort:
      *static_cast<uint64_t *>(ptr) = Zfb::GetFieldI<uint16_t>(*fbo, *field);
      break;
    case reflection::UInt:
      *static_cast<uint64_t *>(ptr) = Zfb::GetFieldI<uint32_t>(*fbo, *field);
      break;
    case reflection::ULong:
      *static_cast<uint64_t *>(ptr) = Zfb::GetFieldI<uint64_t>(*fbo, *field);
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
      new (ptr) Enum{Zfb::GetFieldI<uint8_t>(*fbo, *field)};
      break;
    case reflection::Byte:
      new (ptr) Enum{Zfb::GetFieldI<int8_t>(*fbo, *field)};
      break;
    case reflection::Short:
      new (ptr) Enum{Zfb::GetFieldI<int16_t>(*fbo, *field)};
      break;
    case reflection::Int:
      new (ptr) Enum{Zfb::GetFieldI<int32_t>(*fbo, *field)};
      break;
    case reflection::Long:
      new (ptr) Enum{Zfb::GetFieldI<int64_t>(*fbo, *field)};
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
inline ZuIfT<Type == Value::Index<double>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  switch (field->type()->base_type()) {
    case reflection::Float:
      *static_cast<double *>(ptr) = Zfb::GetFieldF<float>(*fbo, *field);
      break;
    case reflection::Double:
      *static_cast<double *>(ptr) = Zfb::GetFieldF<double>(*fbo, *field);
      break;
    default:
      break;
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuFixed>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZuFixed{
    Zfb::Load::fixed(fbo->GetPointer<const Zfb::Fixed *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuDecimal>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZuDecimal{
    Zfb::Load::decimal(fbo->GetPointer<const Zfb::Decimal *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuTime>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZuTime{
    Zfb::Load::time(fbo->GetPointer<const Zfb::Time *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuDateTime>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZuDateTime{
    Zfb::Load::dateTime(
      fbo->GetPointer<const Zfb::DateTime *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<int128_t>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  *static_cast<int128_t *>(ptr) =
    Zfb::Load::int128(fbo->GetPointer<const Zfb::Int128 *>(field->offset()));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<uint128_t>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  *static_cast<uint128_t *>(ptr) =
    Zfb::Load::uint128(fbo->GetPointer<const Zfb::UInt128 *>(field->offset()));
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZiIP>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZiIP{
    Zfb::Load::ip(fbo->GetPointer<const Zfb::IP *>(field->offset()))
  };
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuID>{}>
loadValue_(void *ptr, const reflection::Field *field, const Zfb::Table *fbo) {
  new (ptr) ZuID{
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
inline ZuIfT<Type == Value::Index<ZtString>{}>
saveOffset_(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  offsets.push(Zfb::Save::str(fbb, value.p<Type>()).Union());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtBytes>{}>
saveOffset_(Zfb::Builder &fbb, Offsets &offsets, const Value &value)
{
  offsets.push(Zfb::Save::bytes(fbb, value.p<Type>()).Union());
}

template <unsigned Type>
inline ZuIfT<
  Type != Value::Index<ZtString>{} &&
  Type != Value::Index<ZtBytes>{}>
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
inline ZuIfT<Type == Value::Index<ZtString>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &offsets,
  const reflection::Field *field, const Value &value)
{
  fbb.AddOffset(field->offset(), offsets.shift());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZtBytes>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &offsets,
  const reflection::Field *field, const Value &value)
{
  fbb.AddOffset(field->offset(), offsets.shift());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<bool>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  fbb.AddElement<uint8_t>(
    field->offset(), value.p<Type>(), field->default_integer());
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<int64_t>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  switch (field->type()->base_type()) {
    case reflection::Bool:
      fbb.AddElement<uint8_t>(
	field->offset(), value.p<Type>(), field->default_integer());
      break;
    case reflection::Byte:
      fbb.AddElement<int8_t>(
	field->offset(), value.p<Type>(), field->default_integer());
      break;
    case reflection::Short:
      fbb.AddElement<int16_t>(
	field->offset(), value.p<Type>(), field->default_integer());
      break;
    case reflection::Int:
      fbb.AddElement<int32_t>(
	field->offset(), value.p<Type>(), field->default_integer());
      break;
    case reflection::Long:
      fbb.AddElement<int64_t>(
	field->offset(), value.p<Type>(), field->default_integer());
      break;
    default:
      break;
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<uint64_t>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  switch (field->type()->base_type()) {
    case reflection::Bool:
    case reflection::UByte:
      fbb.AddElement<uint8_t>(
	field->offset(), value.p<Type>(), field->default_integer());
      break;
    case reflection::UShort:
      fbb.AddElement<uint16_t>(
	field->offset(), value.p<Type>(), field->default_integer());
      break;
    case reflection::UInt:
      fbb.AddElement<uint32_t>(
	field->offset(), value.p<Type>(), field->default_integer());
      break;
    case reflection::ULong:
      fbb.AddElement<uint64_t>(
	field->offset(), value.p<Type>(), field->default_integer());
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
	field->offset(), value.p<Type>(), field->default_integer());
      break;
    case reflection::Short:
      fbb.AddElement<int16_t>(
	field->offset(), value.p<Type>(), field->default_integer());
      break;
    case reflection::Int:
      fbb.AddElement<int32_t>(
	field->offset(), value.p<Type>(), field->default_integer());
      break;
    case reflection::Long:
      fbb.AddElement<int64_t>(
	field->offset(), value.p<Type>(), field->default_integer());
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
	field->offset(), value.p<Type>(), field->default_integer());
      break;
    default:
      break;
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<double>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  switch (field->type()->base_type()) {
    case reflection::Float:
      fbb.AddElement<float>(
	field->offset(), value.p<Type>(), field->default_real());
      break;
    case reflection::Double:
      fbb.AddElement<double>(
	field->offset(), value.p<Type>(), field->default_real());
      break;
    default:
      break;
  }
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuFixed>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::fixed(value.p<Type>());
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuDecimal>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::decimal(value.p<Type>());
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuTime>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::time(value.p<Type>());
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuDateTime>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::dateTime(value.p<Type>());
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<int128_t>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::int128(value.p<Type>());
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<uint128_t>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::uint128(value.p<Type>());
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZiIP>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::ip(value.p<Type>());
  fbb.AddStruct(field->offset(), &v);
}

template <unsigned Type>
inline ZuIfT<Type == Value::Index<ZuID>{}>
saveValue_(
  Zfb::Builder &fbb, const Offsets &,
  const reflection::Field *field, const Value &value)
{
  auto v = Zfb::Save::id(value.p<Type>());
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

// loadTuple() and saveTuple() rely on tuples being a full row
// of values, i.e. tuples.length() == fields.length() == xFields.length()
// (individual elements of the tuple can be null values)

// load tuple from flatbuffer
template <typename Filter>
Tuple loadTuple(
  const ZtMFields &fields,
  const XFields &xFields,
  const Zfb::Table *fbo, Filter filter)
{
  unsigned n = fields.length();
  Tuple tuple(n); // not {}
  for (unsigned i = 0; i < n; i++)
    if (filter(fields[i]))
      new (tuple.push()) Value{};
    else
      loadValue(static_cast<Value *>(tuple.push()), xFields[i], fbo);
  return tuple;
}
Tuple loadTuple(
  const ZtMFields &fields, const XFields &xFields, const Zfb::Table *fbo)
{
  return loadTuple(fields, xFields, fbo, [](const ZtMField *) {
    return false;
  });
}
Tuple loadUpdTuple(
  const ZtMFields &fields, const XFields &xFields, const Zfb::Table *fbo)
{
  return loadTuple(fields, xFields, fbo, [](const ZtMField *field) {
    return (field->type->props & ZtMFieldProp::Update) || (field->keys & 1);
  });
}
Tuple loadDelTuple(
  const ZtMFields &fields, const XFields &xFields, const Zfb::Table *fbo)
{
  return loadTuple(fields, xFields, fbo, [](const ZtMField *field) {
    return (field->keys & 1);
  });
}

template <typename Filter>
Offset saveTuple(
  Zfb::Builder &fbb,
  const ZtMFields &fields,
  const XFields &xFields,
  ZuArray<const Value> tuple,
  Filter filter)
{
  unsigned n = fields.length();
  ZmAssert(n == tuple.length());
  Offsets offsets{ZmAlloc(Offset, n)};
  for (unsigned i = 0; i < n; i++)
    if (!filter(fields[i]))
      saveOffset(fbb, offsets, xFields[i], tuple[i]);
  auto start = fbb.StartTable();
  for (unsigned i = 0; i < n; i++)
    if (!filter(fields[i]))
      saveValue(fbb, offsets, xFields[i], tuple[i]);
  auto end = fbb.EndTable(start);
  return Offset{end};
}
Offset saveTuple(
  Zfb::Builder &fbb,
  const ZtMFields &fields,
  const XFields &xFields,
  ZuArray<const Value> tuple)
{
  return saveTuple(fbb, fields, xFields, tuple, [](const ZtMField *) {
    return false;
  });
}

// update tuple
void updTuple(const ZtMFields &fields, Tuple &data, Tuple &&update) {
  ZmAssert(fields.length() == data.length());
  ZmAssert(data.length() == update.length());
  unsigned n = data.length();
  for (unsigned i = 0; i < n; i++)
    if (fields[i]->type->props & ZtMFieldProp::Update) {
      ZmAssert(update[i].type());
      data[i] = ZuMv(update[i]);
    }
}

// extract key from tuple
Tuple extractKey(
  const ZtMFields &fields,
  const ZtMKeyFields &keyFields,
  unsigned keyID, const Tuple &data)
{
  ZmAssert(fields.length() == data.length());
  Tuple key(keyFields[keyID].length()); // not {}
  unsigned m = fields.length();
  for (unsigned j = 0; j < m; j++)
    if (fields[j]->keys & (1<<keyID)) key.push(data[j]);
  ZmAssert(key.length() == key.size());
  return key;
}

// --- mock row

struct MockRow__ {
  ZdbUN		un;
  ZdbSN		sn;
  ZdbVN		vn;
  Tuple		data;

  static ZdbUN UNAxor(const MockRow__ &row) { return row.un; }
};

// UN index
struct MockRow_ : public ZuObject, public MockRow__ {
  using MockRow__::MockRow__;
  template <typename ...Args>
  MockRow_(Args &&...args) : MockRow__{ZuFwd<Args>(args)...} { }
};
inline constexpr const char *Row_HeapID() { return "MockRow"; }
using IndexUN =
  ZmRBTree<MockRow_,
    ZmRBTreeNode<MockRow_,
      ZmRBTreeKey<MockRow_::UNAxor,
	ZmRBTreeUnique<true,
	  ZmRBTreeHeapID<Row_HeapID>>>>>;
struct MockRow : public IndexUN::Node {
  using Base = IndexUN::Node;
  using Base::Base;
  using MockRow__::data;
};

// key indices
inline constexpr const char *MockRowIndex_HeapID() { return "MockRowIndex"; }
using Index =
  ZmRBTreeKV<Tuple, ZmRef<const MockRow>,
    ZmRBTreeUnique<true,
      ZmRBTreeHeapID<MockRowIndex_HeapID>>>;

// --- mock storeTbl

class StoreTbl : public Zdb_::StoreTbl {
public:
  StoreTbl(
    ZuID id, ZtMFields fields, ZtMKeyFields keyFields,
    const reflection::Schema *schema
  ) :
    m_id{id},
    m_fields{ZuMv(fields)}, m_keyFields{ZuMv(keyFields)}
  {
    m_indices.length(keyFields.length());
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
    for (unsigned i = 0; i < n; i++) {
      unsigned m = m_keyFields[i].length();
      new (m_xKeyFields.push()) XFields{m};
      for (unsigned j = 0; j < m; j++)
	ZtCase::camelSnake(m_keyFields[i][j]->id,
	  [this, fbFields_, i, j](const ZtString &id) {
	    m_xKeyFields[i].push(xField(fbFields_, m_keyFields[i][j], id));
	  });
    }
    m_maxBuf = new AnyBuf{};
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
  ZmRef<const MockRow> loadRow(const ZmRef<const AnyBuf> &buf) {
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
    return new MockRow{record->un(), sn, record->vn(), ZuMv(tuple)};
  }

  // save a row for maxima
  ZmRef<AnyBuf> saveMax(const ZmRef<const MockRow> &row, unsigned keyID) {
    ZmAssert(m_maxBuf->refCount() == 1);
    IOBuilder fbb;
    fbb.buf(m_maxBuf);
    fbb.Finish(saveTuple(fbb, m_fields, m_xFields, row->data,
	[keyID](const ZtMField *field) {
	  return !(field->keys & (1<<keyID));
	}));
    return fbb.buf();
  }

  // save a row to a buffer as a replication/recovery message
  template <bool Recovery>
  ZmRef<AnyBuf> saveRow(const ZmRef<const MockRow> &row) {
    IOBuilder fbb;
    auto data = Zfb::Save::nest(fbb, [this, &row](Zfb::Builder &fbb) {
      return saveTuple(fbb, m_fields, m_xFields, row->data);
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

  void maxima(MaxFn maxFn) {
    unsigned n = m_keyFields.length();
    for (unsigned i = 0; i < n; i++) {
      const auto &keyFields = m_keyFields[i];
      {
	unsigned m = keyFields.length();
	bool isSeries = false;
	for (unsigned j = 0; j < m; j++) {
	  if (keyFields[j]->props & ZtMFieldProp::Series) isSeries = true;
	}
	if (!isSeries) continue;
      }
      auto maximum = m_indices[i].maximum();
      while (maximum) {
	Tuple key = maximum->key();
	ZmAssert(key.length() == keyFields.length());
	ZmRef<const MockRow> maxRow = maximum->val();
	MaxData maxData{
	  .keyID = i,
	  .buf = saveMax(maxRow, i).constRef()
	};
	maxFn(ZuMv(maxData));
	unsigned m = keyFields.length();
	for (unsigned j = 0; j < m; j++)
	  if (keyFields[j]->props & ZtMFieldProp::Series) key[j] = Value{};
	maximum = m_indices[i].find<ZmRBTreeLess>(key);
      }
    }
  }

  void find(unsigned keyID, ZmRef<const AnyBuf> buf, RowFn rowFn) {
    ZmAssert(keyID < m_indices.length());
    ZmAssert(keyID < m_keyFields.length());
    auto work_ =
    [this, keyID, buf = ZuMv(buf), rowFn = ZuMv(rowFn)]() mutable {
      auto tuple = loadTuple(
	m_keyFields[keyID], m_xKeyFields[keyID], Zfb::GetAnyRoot(buf->data()));
      ZmRef<const MockRow> row = m_indices[keyID].findVal(tuple);
      if (row) {
	RowData data{.buf = saveRow<false>(row).constRef()};
	auto callback =
	  [rowFn = ZuMv(rowFn), result = RowResult{ZuMv(data)}]() mutable {
	    rowFn(ZuMv(result));
	  };
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
      } else {
	auto callback = [rowFn = ZuMv(rowFn)]() mutable {
	  rowFn(RowResult{});
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
      }
    };
    deferWork ? work.push(ZuMv(work_)) : work_();
  }

  void recover(UN un, RowFn rowFn) {
    // build Recover buf and return it
    auto work_ = [this, un, rowFn = ZuMv(rowFn)]() mutable {
      ZmRef<const MockRow> row = m_indexUN.find(un);
      if (row) {
	RowData data{.buf = saveRow<true>(row).constRef()};
	auto callback =
	[rowFn = ZuMv(rowFn), result = RowResult{ZuMv(data)}]() mutable {
	  rowFn(ZuMv(result));
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
      } else {
	// missing is not an error, skip over updated/deleted records
	auto callback = [rowFn = ZuMv(rowFn)]() mutable {
	  rowFn(RowResult{});
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
      }
    };
    deferWork ? work.push(ZuMv(work_)) : work_();
  }

  void write(ZmRef<const AnyBuf> buf, CommitFn commitFn) {
    auto work_ =
    [this, buf = ZuMv(buf), commitFn = ZuMv(commitFn)]() mutable {
      // idempotence check
      auto un = record_(msg_(buf->hdr()))->un();
      if (m_indexUN.find(un)) {
	auto callback = [buf = ZuMv(buf), commitFn = ZuMv(commitFn)] {
	  commitFn(ZuMv(buf), CommitResult{});
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
	return;
      }
      // load row, perform insert/update/delete
      ZmRef<MockRow> row = loadRow(buf).mutableRef();
      if (!row->vn)
	insert(ZuMv(row), ZuMv(buf), ZuMv(commitFn));
      else if (row->vn > 0)
	update(ZuMv(row), ZuMv(buf), ZuMv(commitFn));
      else
	del(ZuMv(row), ZuMv(buf), ZuMv(commitFn));
    };
    deferWork ? work.push(ZuMv(work_)) : work_();
  }

  void insert(ZmRef<MockRow> row, ZmRef<const AnyBuf> buf, CommitFn commitFn) {
    m_maxUN = row->un, m_maxSN = row->sn;
    unsigned n = m_keyFields.length();
    for (unsigned i = 0; i < n; i++) {
      auto key = extractKey(m_fields, m_keyFields, i, row->data);
      ZmAssert(key.length() == m_keyFields[i].length());
      if (!i && m_indices[i].findVal(key)) {
	auto callback =
	[this, key = ZuMv(key), buf = ZuMv(buf), commitFn = ZuMv(commitFn)]()
	mutable {
	  commitFn(ZuMv(buf), CommitResult{ZeMEVENT(Error,
	      ([id = this->id(), key = ZuMv(key)](auto &s, const auto &) {
		s << id << " insert(" << ZtJoin(key, ", ")
		  << ") failed - record exists";
	      }))});
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
	return;
      }
      m_indices[i].add(key, row.constRef());
    }
    m_indexUN.addNode(ZuMv(row));
    auto callback = [buf = ZuMv(buf), commitFn = ZuMv(commitFn)]() mutable {
      commitFn(ZuMv(buf), CommitResult{});
    };
    deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
  }

  void update(
    ZmRef<MockRow> updRow, ZmRef<const AnyBuf> buf, CommitFn commitFn
  ) {
    auto key = extractKey(m_fields, m_keyFields, 0, updRow->data);
    ZmRef<MockRow> row = m_indices[0].findVal(key).mutableRef();
    if (row) {
      m_maxUN = updRow->un, m_maxSN = updRow->sn;

      // remember original secondary index key values
      unsigned n = m_keyFields.length();
      auto origKeys = ZmAlloc(Tuple, n - 1);
      for (unsigned i = 1; i < n; i++) {
	auto key = extractKey(m_fields, m_keyFields, i, row->data);
	ZmAssert(key.length() == m_keyFields[i].length());
	new (&origKeys[i - 1]) Tuple{ZuMv(key)};
      }
      // remove from UN index
      m_indexUN.delNode(row);

      row->un = updRow->un;
      row->sn = updRow->sn;
      row->vn = updRow->vn;
      updTuple(m_fields, row->data, ZuMv(updRow->data));

      // add back to UN index
      m_indexUN.addNode(row);
      // update secondary indices if corresponding key changed
      for (unsigned i = 1; i < n; i++) {
	auto key = extractKey(m_fields, m_keyFields, i, row->data);
	if (key != origKeys[i - 1]) {
	  m_indices[i].del(origKeys[i - 1]);
	  m_indices[i].add(key, row.constRef());
	}
      }

      auto callback = [buf = ZuMv(buf), commitFn = ZuMv(commitFn)]() mutable {
	commitFn(ZuMv(buf), CommitResult{});
      };
      deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
    } else {
      auto callback =
      [this, key = ZuMv(key), buf = ZuMv(buf), commitFn = ZuMv(commitFn)]()
      mutable {
	commitFn(ZuMv(buf), CommitResult{
	    ZeMEVENT(Error, ([id = this->id(), key](auto &s, const auto &) {
	      s << id << " update(" << ZtJoin(key, ", ")
		<< ") failed - record missing";
	    }))});
      };
      deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
    }
  }

  void del(ZmRef<MockRow> delRow, ZmRef<const AnyBuf> buf, CommitFn commitFn) {
    auto key = extractKey(m_fields, m_keyFields, 0, delRow->data);
    ZmRef<MockRow> row = m_indices[0].delVal(key).mutableRef();
    if (row) {
      m_maxUN = delRow->un, m_maxSN = delRow->sn;
      m_indexUN.delNode(row);
      unsigned n = m_keyFields.length();
      for (unsigned i = 1; i < n; i++) {
	auto key = extractKey(m_fields, m_keyFields, i, row->data);
	ZmAssert(key.length() == m_keyFields[i].length());
	m_indices[i].del(key);
      }
      auto callback = [buf = ZuMv(buf), commitFn = ZuMv(commitFn)]() mutable {
	commitFn(ZuMv(buf), CommitResult{});
      };
      deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
    } else {
      auto callback =
      [this, key = ZuMv(key), buf = ZuMv(buf), commitFn = ZuMv(commitFn)]()
      mutable {
	commitFn(ZuMv(buf), CommitResult{
	    ZeMEVENT(Error, ([id = this->id(), key](auto &s, const auto &) {
	      s << id << " del(" << ZtJoin(key, ", ")
		<< ") failed - record missing";
	    }))});
      };
      deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
    }
  }

private:
  ZuID			m_id;
  ZtMFields		m_fields;
  ZtMKeyFields		m_keyFields;
  XFields		m_xFields;
  XKeyFields		m_xKeyFields;
  IndexUN		m_indexUN;
  ZtArray<Index>	m_indices;
  UN			m_maxUN = ZdbNullUN();
  SN			m_maxSN = ZdbNullSN();
  bool			m_opened = false;
  ZmRef<AnyBuf>		m_maxBuf;
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
  InitResult init(ZvCf *, ZiMultiplex *, unsigned) {
    if (!m_storeTbls) m_storeTbls = new StoreTbls{};
    return {InitData{.replicated = false}};
  }
  void final() {
    if (!m_preserve) {
      m_storeTbls->clean();
      m_storeTbls = nullptr;
    }
  }

  void preserve() { m_preserve = true; }

  void open(
      ZuID id,
      ZtMFields fields,
      ZtMKeyFields keyFields,
      const reflection::Schema *schema,
      MaxFn maxFn,
      OpenFn openFn) {
    StoreTbls::Node *storeTbl = m_storeTbls->find(id);
    if (storeTbl && storeTbl->opened()) {
      openFn(OpenResult{ZeMEVENT(Error, ([id](auto &s, const auto &) {
	s << "open(" << id << ") failed - already open";
      }))});
      return;
    }
    if (!storeTbl) {
      storeTbl =
	new StoreTbls::Node{id, ZuMv(fields), ZuMv(keyFields), schema};
      m_storeTbls->addNode(storeTbl);
    }
    storeTbl->open();
    if (maxFn) storeTbl->maxima(maxFn);
    openFn(OpenResult{OpenData{
      .storeTbl = storeTbl,
      .count = storeTbl->count(),
      .un = storeTbl->maxUN(),
      .sn = storeTbl->maxSN()
    }});
  }

private:
  ZmRef<StoreTbls>	m_storeTbls;
  bool			m_preserve = false;
};

} // zdbtest

#endif /* ZdbMockStore_HH */
