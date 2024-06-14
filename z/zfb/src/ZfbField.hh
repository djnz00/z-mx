//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// flat object introspection - flatbuffers extensions

#ifndef ZfbField_HH
#define ZfbField_HH

#ifndef ZfbLib_HH
#include <zlib/ZfbLib.hh>
#endif

// ZtField extensions for flatbuffers, with extensible type support

// Syntax
// ------
// (((Accessor)[, (Props...)]), (Type[, Args...]))
// 
// Example: (((id, Rd), (Ctor<0>, Keys<0>)), (String))

// macro DSL syntax is identical to that for ZtField, with the Type
// extended to specify an extensible flatbuffers <-> C++ mapping

// ZfbField	ZtField		C++ Type
// --------	-------		--------
// CString	CString		<CString>
// String	String		<String>
// Bytes	Bytes		<uint8_t[]>
// Bool		Bool		<Integral>
// Int<N>	Int<N>		<Integral>
// UInt<N>	UInt<N>		<Integral>
// Enum, Map	Enum, Map	<Integral>
// Flags, Map	Flags, Map	<Integral>
// Float	Float		<FloatingPoint>
// Fixed	Fixed		ZuFixed
// Decimal	Decimal		ZuDecimal
// Time		Time		ZuTime
// DateTime	DateTime	ZuDateTime
//
// Bitmap	UDT		ZmBitmap
// IP		UDT		ZiIP
// ID		UDT		ZuID
// Object	UDT		<Any>

// UDT example - ZiIP support is added as follows:
//   (network/host byte-order swapping is intentionally elided since
//   IP addresses are in network byte order both on the wire and in memory)
//
// fbs:
//   namespace Zfb;
//   struct IP {
//     addr:[uint8:4];
//   }
//
// C++:
//   namespace Zfb::Save {
//     inline IP ip(ZiIP addr) {
//       return {span<const uint8_t, 4>{
//         reinterpret_cast<const uint8_t *>(&addr.s_addr), 4}};
//     }
//   }
//   namespace Zfb::Load {
//     inline ZiIP ip(const IP *v) {
//       return ZiIP{in_addr{
//         .s_addr = *reinterpret_cast<const uint32_t *>(v->addr()->data())}};
//     }
//   }
//   #define ZfbFieldIP_T UDT
//   #define ZfbFieldIP(O, ...) ZfbFieldInline(O, __VA_ARGS__, ip, ip)

#include <zlib/Zfb.hh>

#include <zlib/ZtField.hh>

void ZfbBuilder_(...);	// default
void ZfbType_(...);	// ''
void ZfbSchema_(...);	// ''

// internal use - pass core object type
template <typename O>
using Zfb_Builder = decltype(ZfbBuilder_(ZuDeclVal<O *>()));
template <typename O>
using Zfb_Type = decltype(ZfbType_(ZuDeclVal<O *>()));
template <typename O>
using Zfb_Schema = decltype(ZfbSchema_(ZuDeclVal<O *>()));

// map object to core object, e.g. map derived key type to actual object type
template <typename O>
using ZfbCore = typename ZuType<0, ZuFieldList<O>>::Core;

// resolve FB type from object type
template <typename O>
using ZfbBuilder = decltype(ZfbBuilder_(ZuDeclVal<ZfbCore<O> *>()));
template <typename O>
using ZfbType = decltype(ZfbType_(ZuDeclVal<ZfbCore<O> *>()));
template <typename O>
using ZfbSchema = decltype(ZfbSchema_(ZuDeclVal<ZfbCore<O> *>()));

// --- load/save handling

namespace ZfbField {

namespace TypeCode = ZtFieldTypeCode;
namespace Prop = ZuFieldProp;

template <typename T> using Offset = Zfb::Offset<T>;

template <typename Field>
struct IsNested : public ZuBool<!Field::Inline> { };
template <
  typename O, typename NestedFields, typename Field,
  bool = IsNested<Field>{}>
struct SaveFieldFn {
  template <typename Builder>
  static void save(Builder &fbb, const O &o, const Offset<void> *) {
    Field::save(fbb, o);
  }
};
template <typename O, typename NestedFields, typename Field>
struct SaveFieldFn<O, NestedFields, Field, true> {
  template <typename Builder>
  static void save(Builder &fbb, const O &, const Offset<void> *offsets) {
    using OffsetIndex = ZuTypeIndex<Field, NestedFields>;
    Field::save(fbb, offsets[OffsetIndex{}]);
  }
};
template <typename O, typename Fields,
  typename NestedFields = ZuTypeGrep<IsNested, Fields>,
  unsigned = NestedFields::N>
struct SaveFieldsFn {
  using Builder = ZfbBuilder<O>;
  using FBType = ZfbType<O>;
  static Offset<FBType> save(Zfb::Builder &fbb_, const O &o) {
    Offset<void> offsets[NestedFields::N];
    ZuUnroll::all<NestedFields>(
	[&fbb_, &o, offsets = &offsets[0]]<typename Field>() {
	  using OffsetIndex = ZuTypeIndex<Field, NestedFields>;
	  offsets[OffsetIndex{}] = Field::save(fbb_, o);
	});
    Builder fbb{fbb_};
    ZuUnroll::all<Fields>(
	[&fbb, &o, offsets = &offsets[0]]<typename Field>() {
	  SaveFieldFn<O, NestedFields, Field>::save(fbb, o, offsets);
	});
    return fbb.Finish();
  }
};
template <typename O, typename Fields, typename NestedFields>
struct SaveFieldsFn<O, Fields, NestedFields, 0> {
  using Builder = ZfbBuilder<O>;
  using FBType = ZfbType<O>;
  static Offset<FBType> save(Zfb::Builder &fbb_, const O &o) {
    Builder fbb{fbb_};
    ZuUnroll::all<Fields>([&fbb, &o]<typename Field>() {
      Field::save(fbb, o);
    });
    return fbb.Finish();
  }
};

template <typename O_>
struct Fielded_ {
  using O = O_;

  using AllFields = ZuFieldList<O>;

  // load fields are all the mutable fields
  template <typename U>
  struct LoadFilter : public ZuBool<!U::ReadOnly> { };
  using LoadFields = ZuTypeGrep<LoadFilter, AllFields>;

  // ctor fields - fields passed to the constructor
  template <typename U>
  using CtorFilter = Prop::HasCtor<typename U::Props>;
  template <typename U>
  using CtorIndex = Prop::GetCtor<typename U::Props>;
  using CtorFields = ZuTypeSort<CtorIndex, ZuTypeGrep<CtorFilter, AllFields>>;

  // init fields - fields set post-constructor
  template <typename U>
  using InitFilter = ZuBool<!Prop::HasCtor<typename U::Props>{}>;
  using InitFields = ZuTypeGrep<InitFilter, LoadFields>;

  // save fields - all the ctor and init fields
  template <typename U>
  using SaveFilter =
    ZuBool<bool(Prop::HasCtor<typename U::Props>{}) || !U::ReadOnly>;
  using SaveFields = ZuTypeGrep<SaveFilter, AllFields>;

  // update fields - mutable fields and primary key fields
  template <typename U>
  using UpdFilter = ZuBool<
    bool(ZuTypeIn<Prop::Update, typename U::Props>{}) ||
    bool(ZuFieldProp::Key<typename U::Props, 0>{})>;
  using UpdFields = ZuTypeGrep<UpdFilter, LoadFields>;

  // delete fields - primary key fields
  using DelFields = ZuKeyFields<O, 0>;

  using FBType = ZfbType<O>;

  static Offset<FBType> save(Zfb::Builder &fbb, const O &o) {
    return SaveFieldsFn<O, SaveFields>::save(fbb, o);
  }
  static Offset<FBType> saveUpd(Zfb::Builder &fbb, const O &o) {
    return SaveFieldsFn<O, UpdFields>::save(fbb, o);
  }
  static Offset<FBType> saveDel(Zfb::Builder &fbb, const O &o) {
    return SaveFieldsFn<O, DelFields>::save(fbb, o);
  }

  template <typename ...Fields>
  struct Ctor {
    static O ctor(const FBType *fbo) {
      return O{Fields::load_(fbo)...};
    }
    static void ctor(void *o, const FBType *fbo) {
      new (o) O{Fields::load_(fbo)...};
    }
  };
  static O ctor(const FBType *fbo) {
    O o = ZuTypeApply<Ctor, CtorFields>::ctor(fbo);
    ZuUnroll::all<InitFields>([&o, fbo]<typename Field>() {
      Field::load(o, fbo);
    });
    return o;
  }
  static void ctor(void *o_, const FBType *fbo) {
    ZuTypeApply<Ctor, CtorFields>::ctor(o_, fbo);
    O &o = *static_cast<O *>(o_);
    ZuUnroll::all<InitFields>([&o, fbo]<typename Field>() {
      Field::load(o, fbo);
    });
  }

  template <typename ...Field>
  struct Load__ : public O {
    Load__() = default;
    Load__(const FBType *fbo) : O{Field::load_(fbo)...} { }
    template <typename ...Args>
    Load__(Args &&... args) : O{ZuFwd<Args>(args)...} { }
  };
  using Load_ = ZuTypeApply<Load__, CtorFields>;
  struct Load : public Load_ {
    Load() = default;
    Load(const FBType *fbo) : Load_{fbo} {
      ZuUnroll::all<InitFields>([this, fbo]<typename Field>() {
	Field::load(*this, fbo);
      });
    }
    template <typename ...Args>
    Load(Args &&... args) : Load_{ZuFwd<Args>(args)...} { }
  };

  static void load(O &o, const FBType *fbo) {
    ZuUnroll::all<LoadFields>([&o, fbo]<typename Field>() {
      Field::load(o, fbo);
    });
  }
  static void update(O &o, const FBType *fbo) {
    ZuUnroll::all<UpdFields>([&o, fbo]<typename Field>() {
      Field::load(o, fbo);
    });
  }

  template <typename Base>
  struct Adapted : public Base {
    using Orig = Base;
    template <template <typename> typename Override>
    using Adapt = Adapted<Override<Orig>>;
    using O = FBType;
    enum { ReadOnly = true };
    enum { I = ZuTypeIndex<Orig, ZuTypeMap<ZuFieldOrig, AllFields>>{} };
    // remove any Ctor property
  private:
    template <typename>
    struct CtorFilter : public ZuTrue { };
    template <unsigned J>
    struct CtorFilter<ZuFieldProp::Ctor<J>> : public ZuFalse { };
  public:
    using Props = ZuTypeGrep<CtorFilter, typename Orig::Props>;
    using Field = ZuType<I, AllFields>;
    static decltype(auto) get(const O &o) { return Field::load_(&o); }
    template <typename U> static void set(O &, U &&v);
  };
  template <typename Field>
  using Map = typename Field::template Adapt<Adapted>;
  using FBFields = ZuTypeMap<Map, AllFields>;
};
template <typename O>
using Fielded = Fielded_<ZuFielded<O>>;

template <typename O>
inline auto save(Zfb::Builder &fbb, const O &o) {
  return Fielded<O>::save(fbb, o);
}
template <typename O>
inline auto saveUpd(Zfb::Builder &fbb, const O &o) {
  return Fielded<O>::saveUpd(fbb, o);
}
template <typename O>
inline auto saveDel(Zfb::Builder &fbb, const O &o) {
  return Fielded<O>::saveDel(fbb, o);
}

template <typename O>
inline const ZfbType<O> *root(const uint8_t *data) {
  return Zfb::GetRoot<ZfbType<O>>(data);
}

template <typename O>
inline const ZfbType<O> *verify(ZuBytes data) {
  if (!Zfb::Verifier{&data[0], data.length()}.VerifyBuffer<ZfbType<O>>())
    return nullptr;
  return root<O>(&data[0]);
}

template <typename O>
inline O ctor(const ZfbType<O> *fbo) {
  return Fielded<O>::ctor(fbo);
}
template <typename O>
inline void ctor(void *o_, const ZfbType<O> *fbo) {
  Fielded<O>::ctor(o_, fbo);
}

template <typename O> using Load = typename Fielded<O>::Load;

template <typename O>
inline void load(O &o, const ZfbType<O> *fbo) {
  Fielded<O>::load(o, fbo);
}
template <typename O>
inline void update(O &o, const ZfbType<O> *fbo) {
  Fielded<O>::update(o, fbo);
}

template <typename O>
using Fields = typename Fielded<O>::FBFields;

} // ZfbField

namespace Zfb {

namespace Save {

  template <typename Builder, typename O>
  inline auto object(Builder &fbb, const O &o) {
    return ZfbField::save(fbb, o);
  }

  template <typename Builder>
  inline auto cstringVec(Builder &fbb, ZtField_::CStringVec a) {
    using CString = const char *;
    return vectorIter<String>(fbb, a.length(),
      [&a](Builder &fbb, unsigned i) mutable {
	return str(fbb, CString(a[i]));
      });
  }
  template <typename Builder>
  inline auto stringVec(Builder &fbb, ZtField_::StringVec a) {
    return vectorIter<String>(fbb, a.length(),
      [&a](Builder &fbb, unsigned i) mutable {
	return str(fbb, ZuString(a[i]));
      });
  }
  template <typename Builder>
  inline auto bytesVec(Builder &fbb, ZtField_::BytesVec a) {
    return vectorIter<Vector<uint8_t>>(fbb, a.length(),
      [&a](Builder &fbb, unsigned i) mutable {
	return bytes(fbb, ZuBytes(a[i]));
      });
  }

#define ZfbField_SaveInt(width) \
  template <typename Builder> \
  inline auto int##width##Vec( \
    Builder &fbb, ZtField_::Int##width##Vec a) \
  { \
    return pvectorIter<int##width##_t>(fbb, a.length(), \
      [&a](Builder &fbb, unsigned i) mutable { \
	return int##width##_t(a[i]); \
      }); \
  } \
  template <typename Builder> \
  inline auto uint##width##Vec( \
    Builder &fbb, ZtField_::UInt##width##Vec a) \
  { \
    return pvectorIter<uint##width##_t>(fbb, a.length(), \
      [&a](Builder &fbb, unsigned i) mutable { \
	return uint##width##_t(a[i]); \
      }); \
  }
  ZfbField_SaveInt(8)
  ZfbField_SaveInt(16)
  ZfbField_SaveInt(32)
  ZfbField_SaveInt(64)

  template <typename Builder>
  inline auto int128Vec(Builder &fbb, ZtField_::Int128Vec a) {
    return structVecIter<Int128>(fbb, a.length(),
      [&a](Int128 *ptr, unsigned i) mutable {
	new (ptr) Int128{int128(int128_t(a[i]))};
      });
  }
  template <typename Builder>
  inline auto uint128Vec(Builder &fbb, ZtField_::UInt128Vec a) {
    return structVecIter<UInt128>(fbb, a.length(),
      [&a](UInt128 *ptr, unsigned i) mutable {
	new (ptr) UInt128{uint128(uint128_t(a[i]))};
      });
  }
  template <typename Builder>
  inline auto floatVec(Builder &fbb, ZtField_::FloatVec a) {
    return pvectorIter<double>(fbb, a.length(),
      [&a](Builder &fbb, unsigned i) mutable { return double(a[i]); });
  }
  template <typename Builder>
  inline auto fixedVec(Builder &fbb, ZtField_::FixedVec a) {
    return structVecIter<Fixed>(fbb, a.length(),
      [&a](Fixed *ptr, unsigned i) mutable {
	new (ptr) Fixed{fixed(ZuFixed(a[i]))};
      });
  }
  template <typename Builder>
  inline auto decimalVec(Builder &fbb, ZtField_::DecimalVec a) {
    return structVecIter<Decimal>(fbb, a.length(),
      [&a](Decimal *ptr, unsigned i) mutable {
	new (ptr) Decimal{decimal(ZuDecimal(a[i]))};
      });
  }
  template <typename Builder>
  inline auto timeVec(Builder &fbb, ZtField_::TimeVec a) {
    return structVecIter<Time>(fbb, a.length(),
      [&a](Time *ptr, unsigned i) mutable {
	new (ptr) Time{time(ZuTime(a[i]))};
      });
  }
  template <typename Builder>
  inline auto dateTimeVec(Builder &fbb, ZtField_::DateTimeVec a) {
    return structVecIter<DateTime>(fbb, a.length(),
      [&a](DateTime *ptr, unsigned i) mutable {
	new (ptr) DateTime{dateTime(ZuDateTime(a[i]))};
      });
  }
}

namespace Load {

  template <typename O>
  inline O object(const ZfbType<O> *fbo) {
    return ZfbField::ctor<O>(fbo);
  }

  inline const ZtField_::CStringVec
  cstringVec(const Vector<Offset<String>> *v) {
    using Vec = Vector<Offset<String>>;
    return ZtField_::CStringVec(*const_cast<Vec *>(v), v->size(),
      [](const void *v_, unsigned i) {
	return reinterpret_cast<const char *>(
	  static_cast<const Vec *>(v_)->Get(i)->Data());
      });
  }
  inline const ZtField_::StringVec
  stringVec(const Vector<Offset<String>> *v) {
    using Vec = Vector<Offset<String>>;
    return ZtField_::StringVec(*const_cast<Vec *>(v), v->size(),
      [](const void *v_, unsigned i) {
	return str(static_cast<const Vec *>(v_)->Get(i));
      });
  }
  inline const ZtField_::BytesVec
  bytesVec(const Vector<Offset<Vector<uint8_t>>> *v) {
    using Vec = Vector<Offset<Vector<uint8_t>>>;
    return ZtField_::BytesVec(*const_cast<Vec *>(v), v->size(),
      [](const void *v_, unsigned i) {
	return bytes(static_cast<const Vec *>(v_)->Get(i));
      });
  }

#define ZfbField_LoadInt(width) \
  inline const ZtField_::Int##width##Vec \
  int##width##Vec(const Vector<int##width##_t> *v) { \
    using Vec = Vector<int##width##_t>; \
    return ZtField_::Int##width##Vec(*const_cast<Vec *>(v), v->size(), \
      [](const void *v_, unsigned i) -> int##width##_t { \
	return static_cast<const Vec *>(v_)->Get(i); \
      }); \
  } \
  inline const ZtField_::UInt##width##Vec \
  uint##width##Vec(const Vector<uint##width##_t> *v) { \
    using Vec = Vector<uint##width##_t>; \
    return ZtField_::UInt##width##Vec(*const_cast<Vec *>(v), v->size(), \
      [](const void *v_, unsigned i) -> uint##width##_t { \
	return static_cast<const Vec *>(v_)->Get(i); \
      }); \
  }

  ZfbField_LoadInt(8)
  ZfbField_LoadInt(16)
  ZfbField_LoadInt(32)
  ZfbField_LoadInt(64)
  
  inline const ZtField_::Int128Vec
  int128Vec(const Vector<const Int128 *> *v) {
    using Vec = Vector<const Int128 *>;
    return ZtField_::Int128Vec(*const_cast<Vec *>(v), v->size(),
      [](const void *v_, unsigned i) {
	return int128_t(int128(static_cast<const Vec *>(v_)->Get(i)));
      });
  }
  inline const ZtField_::UInt128Vec
  uint128Vec(const Vector<const UInt128 *> *v) {
    using Vec = Vector<const UInt128 *>;
    return ZtField_::UInt128Vec(*const_cast<Vec *>(v), v->size(),
      [](const void *v_, unsigned i) {
	return uint128_t(uint128(static_cast<const Vec *>(v_)->Get(i)));
      });
  }

  inline const ZtField_::FloatVec
  floatVec(const Vector<double> *v) {
    using Vec = Vector<double>;
    return ZtField_::FloatVec(*const_cast<Vec *>(v), v->size(),
      [](const void *v_, unsigned i) {
	return static_cast<const Vec *>(v_)->Get(i);
      });
  }

  inline const ZtField_::FixedVec
  fixedVec(const Vector<const Fixed *> *v) {
    using Vec = Vector<const Fixed *>;
    return ZtField_::FixedVec(*const_cast<Vec *>(v), v->size(),
      [](const void *v_, unsigned i) {
	return fixed(static_cast<const Vec *>(v_)->Get(i));
      });
  }
  inline const ZtField_::DecimalVec
  decimalVec(const Vector<const Decimal *> *v) {
    using Vec = Vector<const Decimal *>;
    return ZtField_::DecimalVec(*const_cast<Vec *>(v), v->size(),
      [](const void *v_, unsigned i) {
	return decimal(static_cast<const Vec *>(v_)->Get(i));
      });
  }
  inline const ZtField_::TimeVec
  timeVec(const Vector<const Time *> *v) {
    using Vec = Vector<const Time *>;
    return ZtField_::TimeVec(*const_cast<Vec *>(v), v->size(),
      [](const void *v_, unsigned i) {
	return time(static_cast<const Vec *>(v_)->Get(i));
      });
  }
  inline const ZtField_::DateTimeVec
  dateTimeVec(const Vector<const DateTime *> *v) {
    using Vec = Vector<const DateTime *>;
    return ZtField_::DateTimeVec(*const_cast<Vec *>(v), v->size(),
      [](const void *v_, unsigned i) {
	return dateTime(static_cast<const Vec *>(v_)->Get(i));
      });
  }
}

} // Zfb

#define ZfbFieldTypeName(O, ID) ZfbField_##O##_##ID
#define ZfbFieldTypeName_(O, ID) ZfbField_##O##_##ID##_

#define ZfbFieldGeneric(O_, ID, Base_) \
  template < \
    typename O = O_, typename Base = Base_, typename Core_ = O, \
    bool = Base::ReadOnly, typename = void> \
  struct ZfbFieldTypeName_(O_, ID) : public Base { };

#define ZfbFieldNested(O_, ID, Base_, SaveFn, LoadFn) \
  ZfbFieldGeneric(O_, ID, Base_) \
  template <typename O, typename Base, typename Core_> \
  struct ZfbFieldTypeName_(O_, ID)< \
    O, Base, Core_, true, decltype(&Zfb_Type<Core_>::ID, void())> : \
      public Base { \
    using Core = Core_; \
    template <template <typename> typename Override> \
    using Adapt = ZfbFieldTypeName_(O_, ID)< \
      typename Override<ZuFieldOrig<Base>>::O, \
      typename Base::template Adapt<Override>, Core>; \
    using Builder = Zfb_Builder<Core>; \
    using FBType = Zfb_Type<Core>; \
    enum { Inline = 0 }; \
    static Zfb::Offset<void> save(Zfb::Builder &fbb, const O &o) { \
      return Zfb::Save::SaveFn(fbb, Base::get(o)).Union(); \
    } \
    template <typename Builder> \
    static void save(Builder &fbb, Zfb::Offset<void> offset) { \
      fbb.add_##ID(offset.o); \
    } \
    template <typename FBType_> \
    static decltype(auto) load_(const FBType_ *fbo) { \
      return Zfb::Load::LoadFn(fbo->ID()); \
    } \
    static void load(O &, const FBType *) { } \
  }; \
  template <typename O, typename Base, typename Core_> \
  struct ZfbFieldTypeName_(O_, ID)< \
    O, Base, Core_, false, decltype(&Zfb_Type<Core_>::ID, void())> : \
      public ZfbFieldTypeName_(O_, ID)<O, Base, Core_, true> { \
    using Core = Core_; \
    using FBType = Zfb_Type<Core>; \
    using ZfbFieldTypeName_(O_, ID)<O, Base, Core, true>::load_; \
    static void load(O &o, const FBType *fbo) { \
      Base::set(o, load_(fbo)); \
    } \
  }; \
  using ZfbFieldTypeName(O_, ID) = ZfbFieldTypeName_(O_, ID)<>;

#define ZfbFieldInline(O_, ID, Base_, SaveFn, LoadFn) \
  ZfbFieldGeneric(O_, ID, Base_) \
  template <typename O, typename Base, typename Core_> \
  struct ZfbFieldTypeName_(O_, ID)< \
    O, Base, Core_, true, decltype(&Zfb_Type<Core_>::ID, void())> : \
      public Base { \
    using Core = Core_; \
    template <template <typename> typename Override> \
    using Adapt = ZfbFieldTypeName_(O_, ID)< \
      typename Override<ZuFieldOrig<Base>>::O, \
      typename Base::template Adapt<Override>, Core>; \
    using Builder = Zfb_Builder<Core>; \
    using FBType = Zfb_Type<Core>; \
    enum { Inline = 1 }; \
    template <typename Builder> \
    static void save(Builder &fbb, const O &o) { \
      auto v = Zfb::Save::SaveFn(Base::get(o)); \
      fbb.add_##ID(&v); \
    } \
    template <typename FBType_> \
    static decltype(auto) load_(const FBType_ *fbo) { \
      auto v = fbo->ID(); \
      return Zfb::Load::LoadFn(v); \
    } \
    static void load(O &, const FBType *) { } \
  }; \
  template <typename O, typename Base, typename Core_> \
  struct ZfbFieldTypeName_(O_, ID)< \
    O, Base, Core_, false, decltype(&Zfb_Type<Core_>::ID, void())> : \
      public ZfbFieldTypeName_(O_, ID)<O, Base, Core_, true> { \
    using Core = Core_; \
    using FBType = Zfb_Type<Core>; \
    using ZfbFieldTypeName_(O_, ID)<O, Base, Core, true>::load_; \
    static void load(O &o, const FBType *fbo) { \
      Base::set(o, load_(fbo)); \
    } \
  }; \
  using ZfbFieldTypeName(O_, ID) = ZfbFieldTypeName_(O_, ID)<>;

#define ZfbFieldPrimitive(O_, ID, Base_) \
  ZfbFieldGeneric(O_, ID, Base_) \
  template <typename O, typename Base, typename Core_> \
  struct ZfbFieldTypeName_(O_, ID)< \
    O, Base, Core_, true, decltype(&Zfb_Type<Core_>::ID, void())> : \
      public Base { \
    using Core = Core_; \
    template <template <typename> typename Override> \
    using Adapt = ZfbFieldTypeName_(O_, ID)< \
      typename Override<ZuFieldOrig<Base>>::O, \
      typename Base::template Adapt<Override>, Core>; \
    using FBType = Zfb_Type<Core>; \
    enum { Inline = 1 }; \
    template <typename Builder> \
    static void save(Builder &fbb, const O &o) { \
      using P = \
        ZuType<0, typename ZuDeduce<decltype(&Builder::add_##ID)>::Args>; \
      fbb.add_##ID(static_cast<P>(Base::get(o))); \
    } \
    template <typename FBType_> \
    static auto load_(const FBType_ *fbo) { \
      return static_cast<typename Base::T>(fbo->ID()); \
    } \
    static void load(O &, const FBType *) { } \
  }; \
  template <typename O, typename Base, typename Core_> \
  struct ZfbFieldTypeName_(O_, ID)< \
    O, Base, Core_, false, decltype(&Zfb_Type<Core_>::ID, void())> : \
      public ZfbFieldTypeName_(O_, ID)<O, Base, Core_, true> { \
    using Core = Core_; \
    using FBType = Zfb_Type<Core>; \
    using ZfbFieldTypeName_(O_, ID)<O, Base, Core, true>::load_; \
    static void load(O &o, const FBType *fbo) { \
      Base::set(o, load_(fbo)); \
    } \
  }; \
  using ZfbFieldTypeName(O_, ID) = ZfbFieldTypeName_(O_, ID)<>;

#define ZfbFieldCString_T String
#define ZfbFieldCString(O, ID, Base) ZfbFieldNested(O, ID, Base, str, str)
#define ZfbFieldString_T String
#define ZfbFieldString(O, ID, Base) ZfbFieldNested(O, ID, Base, str, str)
#define ZfbFieldBytes_T String
#define ZfbFieldBytes(O, ID, Base) ZfbFieldNested(O, ID, Base, bytes, bytes)
#define ZfbFieldBool_T Bool
#define ZfbFieldBool ZfbFieldPrimitive
#define ZfbFieldInt8_T Int8
#define ZfbFieldInt8 ZfbFieldPrimitive
#define ZfbFieldUInt8_T UInt8
#define ZfbFieldUInt8 ZfbFieldPrimitive
#define ZfbFieldInt16_T Int16
#define ZfbFieldInt16 ZfbFieldPrimitive
#define ZfbFieldUInt16_T UInt16
#define ZfbFieldUInt16 ZfbFieldPrimitive
#define ZfbFieldInt32_T Int32
#define ZfbFieldInt32 ZfbFieldPrimitive
#define ZfbFieldUInt32_T UInt32
#define ZfbFieldUInt32 ZfbFieldPrimitive
#define ZfbFieldInt64_T Int64
#define ZfbFieldInt64 ZfbFieldPrimitive
#define ZfbFieldUInt64_T UInt64
#define ZfbFieldUInt64 ZfbFieldPrimitive
#define ZfbFieldInt128_T Int128
#define ZfbFieldInt128(O, ...) \
  ZfbFieldInline(O, __VA_ARGS__, int128, int128)
#define ZfbFieldUInt128_T UInt128
#define ZfbFieldUInt128(O, ...) \
  ZfbFieldInline(O, __VA_ARGS__, uint128, uint128)
#define ZfbFieldEnum_T Enum
#define ZfbFieldEnum ZfbFieldPrimitive
#define ZfbFieldFlags_T Flags
#define ZfbFieldFlags ZfbFieldPrimitive
#define ZfbFieldFloat_T Float
#define ZfbFieldFloat ZfbFieldPrimitive
#define ZfbFieldFixed_T Fixed
#define ZfbFieldFixed(O, ...) \
  ZfbFieldInline(O, __VA_ARGS__, fixed, fixed)
#define ZfbFieldDecimal_T Decimal
#define ZfbFieldDecimal(O, ...) \
  ZfbFieldInline(O, __VA_ARGS__, decimal, decimal)
#define ZfbFieldTime_T Time
#define ZfbFieldTime(O, ...) ZfbFieldInline(O, __VA_ARGS__, time, time)
#define ZfbFieldDateTime_T DateTime
#define ZfbFieldDateTime(O, ...) \
  ZfbFieldInline(O, __VA_ARGS__, dateTime, dateTime)

#define ZfbFieldCStringVec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, cstringVec, cstringVec)
#define ZfbFieldCStringVec_T CStringVec
#define ZfbFieldStringVec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, stringVec, stringVec)
#define ZfbFieldStringVec_T StringVec
#define ZfbFieldBytesVec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, bytesVec, bytesVec)
#define ZfbFieldBytesVec_T BytesVec
#define ZfbFieldInt8Vec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, int8Vec, int8Vec)
#define ZfbFieldInt8Vec_T Int8Vec
#define ZfbFieldUInt8Vec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, uint8Vec, uint8Vec)
#define ZfbFieldUInt8Vec_T UInt8Vec
#define ZfbFieldInt16Vec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, int16Vec, int16Vec)
#define ZfbFieldInt16Vec_T Int16Vec
#define ZfbFieldUInt16Vec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, uint16Vec, uint16Vec)
#define ZfbFieldUInt16Vec_T UInt16Vec
#define ZfbFieldInt32Vec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, int32Vec, int32Vec)
#define ZfbFieldInt32Vec_T Int32Vec
#define ZfbFieldUInt32Vec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, uint32Vec, uint32Vec)
#define ZfbFieldUInt32Vec_T UInt32Vec
#define ZfbFieldInt64Vec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, int64Vec, int64Vec)
#define ZfbFieldInt64Vec_T Int64Vec
#define ZfbFieldUInt64Vec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, uint64Vec, uint64Vec)
#define ZfbFieldUInt64Vec_T UInt64Vec
#define ZfbFieldInt128Vec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, int128Vec, int128Vec)
#define ZfbFieldInt128Vec_T Int128Vec
#define ZfbFieldUInt128Vec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, uint128Vec, uint128Vec)
#define ZfbFieldUInt128Vec_T UInt128Vec
#define ZfbFieldFloatVec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, floatVec, floatVec)
#define ZfbFieldFloatVec_T FloatVec
#define ZfbFieldFixedVec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, fixedVec, fixedVec)
#define ZfbFieldFixedVec_T FixedVec
#define ZfbFieldDecimalVec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, decimalVec, decimalVec)
#define ZfbFieldDecimalVec_T DecimalVec
#define ZfbFieldTimeVec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, timeVec, timeVec)
#define ZfbFieldTimeVec_T TimeVec
#define ZfbFieldDateTimeVec(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, dateTimeVec, dateTimeVec)
#define ZfbFieldDateTimeVec_T DateTimeVec

#define ZfbFieldBitmap_T UDT
#define ZfbFieldBitmap(O, ...) ZfbFieldNested(O, __VA_ARGS__, bitmap, bitmap)
#define ZfbFieldIP_T UDT
#define ZfbFieldIP(O, ...) ZfbFieldInline(O, __VA_ARGS__, ip, ip)
#define ZfbFieldID_T UDT
#define ZfbFieldID(O, ...) ZfbFieldInline(O, __VA_ARGS__, id, id)
#define ZfbFieldObject_T UDT
#define ZfbFieldObject(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, object, object<typename Base::T>)

#define ZfbField_Decl__(O, ID, Base, TypeName, Type) \
  ZuPP_Defer(ZtField_Decl_)(O, Base, \
      (ZfbField##TypeName##_T ZuPP_Nest(ZtField_TypeArgs(Type)))) \
  ZuPP_Defer(ZfbField##TypeName)(O, ID, ZuPP_Nest(ZtFieldTypeName(O, ID)))
#define ZfbField_Decl_(O, Base, Type) \
  ZuPP_Defer(ZfbField_Decl__)(O, \
      ZuPP_Nest(ZtField_BaseID(Base)), Base, \
      ZuPP_Nest(ZtField_TypeName(Type)), Type)
#define ZfbField_Decl(O, Args) ZuPP_Defer(ZfbField_Decl_)(O, ZuPP_Strip(Args))

#define ZfbField_Type_(O, Base, ...) \
  ZuPP_Defer(ZfbFieldTypeName)(O, ZuPP_Nest(ZtField_BaseID(Base)))
#define ZfbField_Type(O, Args) ZuPP_Defer(ZfbField_Type_)(O, ZuPP_Strip(Args))

#define ZfbFields(O, ...)  \
  fbs::O##Builder ZfbBuilder_(O *); \
  fbs::O ZfbType_(O *); \
  namespace ZuFields_ { \
    ZuPP_Eval(ZuPP_MapArg(ZfbField_Decl, O, __VA_ARGS__)) \
    using O = \
      ZuTypeList<ZuPP_Eval(ZuPP_MapArgComma(ZfbField_Type, O, __VA_ARGS__))>; \
  } \
  O ZuFielded_(O *); \
  ZuFields_::O ZuFieldList_(O *); \
  \
  using O##_FBFields = ZfbField::Fields<O>; \
  namespace fbs { \
    O ZuFielded_(O *); \
    O##_FBFields ZuFieldList_(O *); \
    ZtFieldPrint ZuPrintType(O *); \
  }

#define ZfbRoot(O) \
  fbs::O##Schema ZfbSchema_(O *)

#endif /* ZfbField_HH */
