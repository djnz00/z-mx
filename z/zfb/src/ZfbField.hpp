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

// flat object introspection - flatbuffers extensions

#ifndef ZfbField_HPP
#define ZfbField_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

// ZtField extensions for flatbuffers, with extensible type support

// macro DSL syntax is identical to that for ZtField, with the Type
// extended to specify an extensible flatbuffers<->C++ mapping

// ZvFB Type	ZtField Type	C++ Type
// ---------	------------	--------
// String	String		<String>
// Bytes	String		<String>
// Bool		Bool		<Integral>
// Int		Int		<Integral>
// Hex		Hex		<Integral>
// Enum, Map	Enum, Map	<Integral>
// Flags, Map	Flags, Map	<Integral>
// Float	Float		<FloatingPoint>
// Fixed	Fixed		ZuFixed
// Decimal	Decimal		ZuDecimal
// Time		Time		ZmTime
// Bitmap	Composite	ZmBitmap
// IP		Composite	ZiIP
// ID		Composite	ZuID

// Type extension - ZiIP support is added as follows:
//   (network/host byte-order swapping is deliberately bypassed since
//   IP addresses are in network byte order both on the wire and in memory)
//
// fbs:
//   namespace Zfb;
//   struct IP {
//     addr:[uint8:4];
//   }
//
// C++:
//   namespace Zfb::Load {
//     inline IP ip(ZiIP addr) {
//       return {span<const uint8_t, 4>{
//         reinterpret_cast<const uint8_t *>(&addr.s_addr), 4}};
//     }
//   }
//   namespace Zfb::Save {
//     inline ZiIP ip(const IP *v) {
//       return ZiIP{in_addr{
//         .s_addr = *reinterpret_cast<const uint32_t *>(v->addr()->data())}};
//     }
//   }
//   #define ZfbFieldIP_T Composite
//   #define ZfbFieldIP(O, ...) ZfbFieldInline(O, __VA_ARGS__, ip, ip)

#include <zlib/Zfb.hpp>

#include <zlib/ZtField.hpp>

void *ZfbBuilder_(...); // default
void *ZfbType_(...); // default

template <typename O>
using ZfbBuilder = ZuDecay<decltype(*ZfbBuilder_(ZuDeclVal<O *>()))>;
template <typename O>
using ZfbType = ZuDecay<decltype(*ZfbType_(ZuDeclVal<O *>()))>;

#define ZfbFieldType(O, ID) ZfbField_##O##_##ID
#define ZfbFieldType_(O, ID) ZfbField_##O##_##ID##_

#define ZfbFieldGeneric(O_, ID, Base_) \
  template <typename O = O_, bool = Base_::ReadOnly, typename = void> \
  struct ZfbFieldType_(O_, ID) : public Base_ { \
    using Builder = ZfbBuilder<O>; \
    using FBType = ZfbType<O>; \
    enum { Inline = 1 }; \
    static void save(Builder &, const O &) { } \
    static void load_(const FBType *) { } \
    static void load(O &, const FBType *) { } \
  };

#define ZfbFieldNested(O_, ID, Base_, SaveFn, LoadFn) \
  ZfbFieldGeneric(O_, ID, Base_) \
  template <typename O> \
  struct ZfbFieldType_(O_, ID)<O, true, decltype(&ZfbType<O>::ID, void{})> : \
      public Base_ { \
    using Base = Base_; \
    using Builder = ZfbBuilder<O>; \
    using FBType = ZfbType<O>; \
    enum { Inline = 0 }; \
    static Zfb::Offset<void> save(Zfb::Builder &fbb, const O &o) { \
      using namespace Zfb::Save; \
      return SaveFn(fbb, Base::get(o)).Union(); \
    } \
    template <typename Builder_> \
    static void save(Builder_ &fbb, Zfb::Offset<void> offset) { \
      fbb.add_##ID(offset.o); \
    } \
    template <typename FBType_> \
    static decltype(auto) load_(const FBType_ *fbo) { \
      using namespace Zfb::Load; \
      return LoadFn(fbo->ID()); \
    } \
    static void load(O &, const FBType *) { } \
  }; \
  template <typename O> \
  struct ZfbFieldType_(O_, ID)<O, false, decltype(&ZfbType<O>::ID, void{})> : \
      public ZfbFieldType_(O_, ID)<O, true> { \
    using Base = Base_; \
    using FBType = ZfbType<O>; \
    using ZfbFieldType_(O_, ID)<O, true>::load_; \
    static void load(O &o, const FBType *fbo) { \
      Base::set(o, load_(fbo)); \
    } \
  }; \
  using ZfbFieldType(O_, ID) = ZfbFieldType_(O_, ID)<>;

#define ZfbFieldInline(O_, ID, Base_, SaveFn, LoadFn) \
  ZfbFieldGeneric(O_, ID, Base_) \
  template <typename O> \
  struct ZfbFieldType_(O_, ID)<O, true, decltype(&ZfbType<O>::ID, void{})> : \
      public Base_ { \
    using Base = Base_; \
    using Builder = ZfbBuilder<O>; \
    using FBType = ZfbType<O>; \
    enum { Inline = 1 }; \
    template <typename Builder_> \
    static void save(Builder_ &fbb, const O &o) { \
      using namespace Zfb::Save; \
      auto v = SaveFn(Base::get(o)); \
      fbb.add_##ID(&v); \
    } \
    template <typename FBType_> \
    static decltype(auto) load_(const FBType_ *fbo) { \
      using namespace Zfb::Load; \
      auto v = fbo->ID(); \
      return LoadFn(v); \
    } \
    static void load(O &, const FBType *) { } \
  }; \
  template <typename O> \
  struct ZfbFieldType_(O_, ID)<O, false, decltype(&ZfbType<O>::ID, void{})> : \
      public ZfbFieldType_(O_, ID)<O, true> { \
    using Base = Base_; \
    using FBType = ZfbType<O>; \
    using ZfbFieldType_(O_, ID)<O, true>::load_; \
    static void load(O &o, const FBType *fbo) { \
      Base::set(o, load_(fbo)); \
    } \
  }; \
  using ZfbFieldType(O_, ID) = ZfbFieldType_(O_, ID)<>;

#define ZfbFieldPrimitive(O_, ID, Base_) \
  ZfbFieldGeneric(O_, ID, Base_) \
  template <typename O> \
  struct ZfbFieldType_(O_, ID)<O, true, decltype(&ZfbType<O>::ID, void{})> : \
      public Base_ { \
    using Base = Base_; \
    using Builder = ZfbBuilder<O>; \
    using FBType = ZfbType<O>; \
    enum { Inline = 1 }; \
    template <typename Builder_> \
    static void save(Builder_ &fbb, const O &o) { \
      using P = \
        ZuType<0, typename ZuDeduce<decltype(&Builder_::add_##ID)>::Args>; \
      fbb.add_##ID(static_cast<P>(Base::get(o))); \
    } \
    template <typename FBType_> \
    static decltype(auto) load_(const FBType_ *fbo) { return fbo->ID(); } \
    static void load(O &, const FBType *) { } \
  }; \
  template <typename O> \
  struct ZfbFieldType_(O_, ID)<O, false, decltype(&ZfbType<O>::ID, void{})> : \
      public ZfbFieldType_(O_, ID)<O, true> { \
    using Base = Base_; \
    using FBType = ZfbType<O>; \
    using ZfbFieldType_(O_, ID)<O, true>::load_; \
    static void load(O &o, const FBType *fbo) { \
      Base::set(o, load_(fbo)); \
    } \
  }; \
  using ZfbFieldType(O_, ID) = ZfbFieldType_(O_, ID)<>;

#define ZfbFieldString_T String
#define ZfbFieldString(O, ID, Base) \
  ZfbFieldNested(O, ID, Base, str, str)
#define ZfbFieldBytes_T String
#define ZfbFieldBytes(O, ID, Base) \
  ZfbFieldNested(O, ID, Base, bytes, bytes)
#define ZfbFieldBool_T Bool
#define ZfbFieldBool ZfbFieldPrimitive
#define ZfbFieldInt_T Int
#define ZfbFieldInt ZfbFieldPrimitive
#define ZfbFieldHex_T Hex
#define ZfbFieldHex ZfbFieldPrimitive
#define ZfbFieldEnum_T Enum
#define ZfbFieldEnum ZfbFieldPrimitive
#define ZfbFieldFlags_T Flags
#define ZfbFieldFlags ZfbFieldPrimitive
#define ZfbFieldFloat_T Float
#define ZfbFieldFloat ZfbFieldPrimitive
#define ZfbFieldFixed_T Fixed
#define ZfbFieldFixed ZfbFieldPrimitive
#define ZfbFieldDecimal_T Decimal
#define ZfbFieldDecimal(O, ...) \
  ZfbFieldInline(O, __VA_ARGS__, decimal, decimal)
#define ZfbFieldTime_T Time
#define ZfbFieldTime(O, ...) \
  ZfbFieldInline(O, __VA_ARGS__, dateTime, dateTime)
#define ZfbFieldBitmap_T Composite
#define ZfbFieldBitmap(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, bitmap, bitmap)
#define ZfbFieldIP_T Composite
#define ZfbFieldIP(O, ...) \
  ZfbFieldInline(O, __VA_ARGS__, ip, ip)
#define ZfbFieldID_T Composite
#define ZfbFieldID(O, ...) \
  ZfbFieldInline(O, __VA_ARGS__, id, id)
#define ZfbFieldObject_T Composite
#define ZfbFieldObject(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, Zfb::ctor<O>, Zfb::save<O>)

#define ZfbField_Decl__(O, ID, Base, TypeName, Type, ...) \
  ZuPP_Defer(ZtField_Decl_)(O, Base, \
      (ZfbField##TypeName##_T ZuPP_Nest(ZtField_TypeArgs(Type))) \
      __VA_OPT__(,) __VA_ARGS__) \
  ZuPP_Defer(ZfbField##TypeName)(O, ID, ZuPP_Nest(ZtFieldType(O, ID)))
#define ZfbField_Decl_(O, Base, Type, ...) \
  ZuPP_Defer(ZfbField_Decl__)(O, \
      ZuPP_Nest(ZtField_BaseID(Base)), Base, \
      ZuPP_Nest(ZtField_TypeName(Type)), Type __VA_OPT__(,) __VA_ARGS__)
#define ZfbField_Decl(O, Args) ZuPP_Defer(ZfbField_Decl_)(O, ZuPP_Strip(Args))

#define ZfbField_Type_(O, Base, ...) \
  ZuPP_Defer(ZfbFieldType)(O, ZuPP_Nest(ZtField_BaseID(Base)))
#define ZfbField_Type(O, Args) ZuPP_Defer(ZfbField_Type_)(O, ZuPP_Strip(Args))

#define ZfbFields(O, ...)  \
  fbs::O##Builder *ZfbBuilder_(O *); \
  fbs::O *ZfbType_(O *); \
  namespace ZuFields_ { \
    ZuPP_Eval(ZuPP_MapArg(ZfbField_Decl, O, __VA_ARGS__)) \
    using O = \
      ZuTypeList<ZuPP_Eval(ZuPP_MapArgComma(ZfbField_Type, O, __VA_ARGS__))>; \
  } \
  O *ZuFielded_(O *); \
  ZuFields_::O ZuFieldList_(O *)

namespace ZfbField {

namespace Type = ZtFieldType;
namespace Flags = ZtFieldFlags;

namespace Save {

template <typename T> using Offset = Zfb::Offset<T>;

template <typename Field> struct HasOffset {
  enum { OK =
    Field::Type == Type::String ||
    (Field::Type == Type::Composite && !Field::Inline)
  };
};
template <
  typename O, typename OffsetFieldList, typename Field,
  bool = HasOffset<Field>::OK>
struct SaveField {
  template <typename Builder>
  static void save(Builder &fbb, const O &o, const Offset<void> *) {
    Field::save(fbb, o);
  }
};
template <typename O, typename OffsetFieldList, typename Field>
struct SaveField<O, OffsetFieldList, Field, true> {
  template <typename Builder>
  static void save(Builder &fbb, const O &, const Offset<void> *offsets) {
    using OffsetIndex = ZuTypeIndex<Field, OffsetFieldList>;
    Field::save(fbb, offsets[OffsetIndex::I]);
  }
};
template <typename O, typename FieldList,
  typename OffsetFieldList = ZuTypeGrep<HasOffset, FieldList>,
  int = OffsetFieldList::N>
struct SaveFieldList {
  using Builder = ZfbBuilder<O>;
  using FBType = ZfbType<O>;
  static Zfb::Offset<FBType> save(Zfb::Builder &fbb_, const O &o) {
    Offset<void> offsets[OffsetFieldList::N];
    ZuTypeAll<OffsetFieldList>::invoke(
	[&fbb_, &o, offsets = &offsets[0]]<typename Field>() {
	  using OffsetIndex = ZuTypeIndex<Field, OffsetFieldList>;
	  offsets[OffsetIndex::I] = Field::save(fbb_, o).Union();
	});
    Builder fbb{fbb_};
    ZuTypeAll<FieldList>::invoke(
	[&fbb, &o, offsets = &offsets[0]]<typename Field>() {
	  SaveField<O, OffsetFieldList, Field>::save(fbb, o, offsets);
	});
    return fbb.Finish();
  }
};
template <typename O, typename FieldList, typename OffsetFieldList>
struct SaveFieldList<O, FieldList, OffsetFieldList, 0> {
  using Builder = ZfbBuilder<O>;
  using FBType = ZfbType<O>;
  static Zfb::Offset<FBType> save(Zfb::Builder &fbb_, const O &o) {
    Builder fbb{fbb_};
    ZuTypeAll<FieldList>::invoke(
	[&fbb, &o]<typename Field>() { Field::save(fbb, o); });
    return fbb.Finish();
  }
};

} // Save

template <typename O>
struct Table_ {
  // using Builder = ZfbBuilder<O>;
  using FBType = ZfbType<O>;
  using FieldList = ZuFieldList<O>;

  template <typename U>
  struct AllFilter { enum { OK = !U::ReadOnly }; };
  using AllFields = ZuTypeGrep<AllFilter, FieldList>;

  template <typename U>
  struct UpdatedFilter { enum { OK = U::Flags & Flags::Update }; };
  using UpdatedFields = ZuTypeGrep<UpdatedFilter, AllFields>;

  template <typename U>
  struct CtorFilter { enum { OK = U::Flags & Flags::Ctor_ }; };
  using CtorFields_ = ZuTypeGrep<CtorFilter, AllFields>;
  template <typename U>
  struct CtorIndex {
    enum { I = (U::Flags>>Flags::CtorShift) & Flags::CtorMask };
  };
  using CtorFields = ZuTypeSort<CtorIndex, CtorFields_>;

  template <typename U>
  struct InitFilter { enum { OK = !(U::Flags & Flags::Ctor_) }; };
  using InitFields = ZuTypeGrep<InitFilter, AllFields>;

  static Zfb::Offset<FBType> save(Zfb::Builder &fbb, const O &o) {
    using namespace Save;
    return SaveFieldList<O, AllFields>::save(fbb, o);
  }
  static Zfb::Offset<FBType> saveUpdate(Zfb::Builder &fbb, const O &o) {
    using namespace Save;
    return SaveFieldList<O, UpdatedFields>::save(fbb, o);
  }

  template <typename ...Fields>
  struct Ctor {
    static O ctor(const FBType *fbo) {
      return O{Fields::load_(fbo)...};
    }
    static void ctor(void *ptr, const FBType *fbo) {
      new (ptr) O{Fields::load_(fbo)...};
    }
  };
  static O ctor(const FBType *fbo) {
    O o = ZuTypeApply<Ctor, CtorFields>::ctor(fbo);
    ZuTypeAll<InitFields>::invoke(
	[&o, fbo]<typename Field>() { Field::load(o, fbo); });
    return o;
  }
  static void ctor(void *ptr, const FBType *fbo) {
    ZuTypeApply<Ctor, CtorFields>::ctor(ptr, fbo);
    O &o = *reinterpret_cast<O *>(ptr);
    ZuTypeAll<InitFields>::invoke(
	[&o, fbo]<typename Field>() { Field::load(o, fbo); });
  }

  template <typename ...Fields>
  struct Load__ : public O {
    Load__() = default;
    Load__(const FBType *fbo) : O{Fields::load_(fbo)...} { }
    template <typename ...Args>
    Load__(Args &&... args) : O{ZuFwd<Args>(args)...} { }
  };
  using Load_ = ZuTypeApply<Load__, CtorFields>;
  struct Load : public Load_ {
    Load() = default;
    Load(const FBType *fbo) : Load_{fbo} {
      ZuTypeAll<InitFields>::invoke(
	  [this, fbo]<typename Field>() { Field::load(*this, fbo); });
    }
    template <typename ...Args>
    Load(Args &&... args) : Load_{ZuFwd<Args>(args)...} { }
  };

  static void load(O &o, const FBType *fbo) {
    ZuTypeAll<AllFields>::invoke(
	[&o, fbo]<typename Field>() { Field::load(o, fbo); });
  }
  static void loadUpdate(O &o, const FBType *fbo) {
    ZuTypeAll<UpdatedFields>::invoke(
	[&o, fbo]<typename Field>() { Field::load(o, fbo); });
  }

  template <typename ...Fields>
  struct Key {
    using Tuple = ZuTuple<typename Fields::T...>;
    static decltype(auto) tuple(const FBType *fbo) {
      return Tuple{Fields::load_(fbo)...};
    }
  };
  template <typename ...Fields>
  struct Key<ZuTypeList<Fields...>> : public Key<Fields...> { };

  template <unsigned KeyID>
  struct KeyFilter {
    template <typename U>
    struct T {
      enum { OK = U::keys() & (1<<KeyID) };
    };
  };
  template <unsigned KeyID = 0>
  static decltype(auto) key(const FBType *fbo) {
    using Fields = ZuTypeGrep<KeyFilter<KeyID>::template T, FieldList>;
    return Key<Fields>::tuple(fbo);
  }
};
template <typename O>
using Table = Table_<ZuFielded<O>>;

template <typename O>
inline auto save(Zfb::Builder &fbb, const O &o) {
  return Table<O>::save(fbb, o);
}
template <typename O>
inline auto saveUpdate(Zfb::Builder &fbb, const O &o) {
  return Table<O>::saveUpdate(fbb, o);
}

template <typename O>
inline ZfbType<O> *root(const uint8_t *data) {
  return Zfb::GetRoot<ZfbType<O>>(data);
}

template <typename O>
inline ZfbType<O> *verify(const uint8_t *data, unsigned len) {
  if (!Zfb::Verifier{data, len}.VerifyBuffer<ZfbType<O>>()) return nullptr;
  return root<O>(data);
}

template <typename O>
inline O ctor(const ZfbType<O> *fbo) {
  return Table<O>::ctor(fbo);
}
template <typename O>
inline void ctor(void *ptr, const ZfbType<O> *fbo) {
  Table<O>::ctor(ptr, fbo);
}

template <typename O> using Load = typename Table<O>::Load;

template <typename O>
inline void load(O &o, const ZfbType<O> *fbo) {
  Table<O>::load(o, fbo);
}
template <typename O>
inline void loadUpdate(O &o, const ZfbType<O> *fbo) {
  Table<O>::loadUpdate(o, fbo);
}

template <typename O>
inline auto key(const ZfbType<O> *fbo) {
  return Table<O>::key(fbo);
}

} // ZfbField

#endif /* ZfbField_HPP */
