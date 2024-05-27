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
// Int		Int		<Integral>
// Hex		Hex		<Integral>
// Enum, Map	Enum, Map	<Integral>
// Flags, Map	Flags, Map	<Integral>
// Float	Float		<FloatingPoint>
// Fixed	Fixed		ZuFixed
// Decimal	Decimal		ZuDecimal
// Time		Time		ZuTime
// DateTime	DateTime	ZuDateTime
//
// Int128	UDT		int128_t
// UInt128	UDT		uint128_t
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
struct HasOffset : public ZuBool<
    Field::Type::Code == TypeCode::CString ||
    Field::Type::Code == TypeCode::String ||
    Field::Type::Code == TypeCode::Bytes ||
    (Field::Type::Code == TypeCode::UDT && !Field::Inline)> { };
template <
  typename O, typename OffsetFields, typename Field,
  bool = HasOffset<Field>{}>
  // could use ZuTypeIn<>, but OffsetFields is built using HasOffset<>
struct SaveFieldFn {
  template <typename Builder>
  static void save(Builder &fbb, const O &o, const Offset<void> *) {
    Field::save(fbb, o);
  }
};
template <typename O, typename OffsetFields, typename Field>
struct SaveFieldFn<O, OffsetFields, Field, true> {
  template <typename Builder>
  static void save(Builder &fbb, const O &, const Offset<void> *offsets) {
    using OffsetIndex = ZuTypeIndex<Field, OffsetFields>;
    Field::save(fbb, offsets[OffsetIndex{}]);
  }
};
template <typename O, typename Fields,
  typename OffsetFields = ZuTypeGrep<HasOffset, Fields>,
  unsigned = OffsetFields::N>
struct SaveFieldsFn {
  using Builder = ZfbBuilder<O>;
  using FBType = ZfbType<O>;
  static Offset<FBType> save(Zfb::Builder &fbb_, const O &o) {
    Offset<void> offsets[OffsetFields::N];
    ZuUnroll::all<OffsetFields>(
	[&fbb_, &o, offsets = &offsets[0]]<typename Field>() {
	  using OffsetIndex = ZuTypeIndex<Field, OffsetFields>;
	  offsets[OffsetIndex{}] = Field::save(fbb_, o);
	});
    Builder fbb{fbb_};
    ZuUnroll::all<Fields>(
	[&fbb, &o, offsets = &offsets[0]]<typename Field>() {
	  SaveFieldFn<O, OffsetFields, Field>::save(fbb, o, offsets);
	});
    return fbb.Finish();
  }
};
template <typename O, typename Fields, typename OffsetFields>
struct SaveFieldsFn<O, Fields, OffsetFields, 0> {
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
struct Fielded_ : public ZtField::Fielded_<O_> {
  using O = O_;
  using Base = ZtField::Fielded_<O>;
  using typename Base::AllFields;
  using typename Base::LoadFields;
  using typename Base::CtorFields;
  using typename Base::InitFields;
  using typename Base::SaveFields;
  using typename Base::UpdFields;
  using typename Base::DelFields;

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
namespace Load {
  template <typename O>
  inline O object(const ZfbType<O> *fbo) {
    return ZfbField::ctor<O>(fbo);
  }
}
namespace Save {
  template <typename Builder, typename O>
  inline auto object(Builder &fbb, const O &o) {
    return ZfbField::save(fbb, o);
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
#define ZfbFieldInt_T Int
#define ZfbFieldInt ZfbFieldPrimitive
#define ZfbFieldUInt_T UInt
#define ZfbFieldUInt ZfbFieldPrimitive
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
#define ZfbFieldInt128_T UDT
#define ZfbFieldInt128(O, ...) \
  ZfbFieldInline(O, __VA_ARGS__, int128, int128)
#define ZfbFieldUInt128_T UDT
#define ZfbFieldUInt128(O, ...) \
  ZfbFieldInline(O, __VA_ARGS__, uint128, uint128)
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
