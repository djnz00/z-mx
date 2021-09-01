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

#ifndef ZvFBField_HPP
#define ZvFBField_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

// ZvField extensions for flatbuffers, with extensible type support

// ZvFB Type	ZvField Type	C++ Type
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

// Type extension - ZiIP support is added as follows:
//
// fbs:
//   namespace Zfb;
//   struct IP {
//     addr:uint32;
//   }
//
// C++:
//   namespace Zfb::Load {
//     inline IP ip(ZiIP addr) { return {static_cast<uint32_t>(addr)}; }
//   }
//   namespace Zfb::Save {
//     inline ZiIP ip(const IP *v) { return {v->addr()}; }
//   }
//   #define ZvFBFieldIP_T Composite
//   #define ZvFBFieldIP(O, ...) ZvFBFieldInline(O, __VA_ARGS__, ip, ip)

#include <zlib/Zfb.hpp>

#include <zlib/ZvField.hpp>

void *ZvFBB_(...); // default
void *ZvFBS_(...); // default

template <typename O>
using ZvFBB = ZuDecay<decltype(*ZvFBB_(ZuDeclVal<O *>()))>;
template <typename O>
using ZvFBS = ZuDecay<decltype(*ZvFBS_(ZuDeclVal<O *>()))>;

#define ZvFBFieldType(O, ID) ZvFBField_##O##_##ID
#define ZvFBFieldType_(O, ID) ZvFBField_##O##_##ID##_

#define ZvFBFieldGeneric(O_, ID, Base_) \
  template <typename O = O_, bool = Base_::ReadOnly, typename = void> \
  struct ZvFBFieldType_(O_, ID) : public Base_ { \
    using FBB = ZvFBB<O>; \
    using FBS = ZvFBS<O>; \
    enum { Inline = 1 }; \
    static void save(FBB &, const O &) { } \
    static void load_(const FBS *) { } \
    static void load(O &, const FBS *) { } \
  };

#define ZvFBFieldNested(O_, ID, Base_, SaveFn, LoadFn) \
  ZvFBFieldGeneric(O_, ID, Base_) \
  template <typename O> \
  struct ZvFBFieldType_(O_, ID)<O, true, decltype(&ZvFBS<O>::ID, void())> : \
      public Base_ { \
    using Base = Base_; \
    using FBB = ZvFBB<O>; \
    using FBS = ZvFBS<O>; \
    enum { Inline = 0 }; \
    static Zfb::Offset<void> save(Zfb::Builder &fbb, const O &o) { \
      using namespace Zfb::Save; \
      return SaveFn(fbb, Base::get(o)).Union(); \
    } \
    template <typename FBB_> \
    static void save(FBB_ &fbb, Zfb::Offset<void> offset) { \
      fbb.add_##ID(offset.o); \
    } \
    template <typename FBS_> \
    static decltype(auto) load_(const FBS_ *o_) { \
      using namespace Zfb::Load; \
      return LoadFn(o_->ID()); \
    } \
    static void load(O &, const FBS *) { } \
  }; \
  template <typename O> \
  struct ZvFBFieldType_(O_, ID)<O, false, decltype(&ZvFBS<O>::ID, void())> : \
      public ZvFBFieldType_(O_, ID)<O, true> { \
    using Base = Base_; \
    using FBS = ZvFBS<O>; \
    using ZvFBFieldType_(O_, ID)<O, true>::load_; \
    static void load(O &o, const FBS *o_) { \
      Base::set(o, load_(o_)); \
    } \
  }; \
  using ZvFBFieldType(O_, ID) = ZvFBFieldType_(O_, ID)<>;

#define ZvFBFieldInline(O_, ID, Base_, SaveFn, LoadFn) \
  ZvFBFieldGeneric(O_, ID, Base_) \
  template <typename O> \
  struct ZvFBFieldType_(O_, ID)<O, true, decltype(&ZvFBS<O>::ID, void())> : \
      public Base_ { \
    using Base = Base_; \
    using FBB = ZvFBB<O>; \
    using FBS = ZvFBS<O>; \
    enum { Inline = 1 }; \
    template <typename FBB_> \
    static void save(FBB_ &fbb, const O &o) { \
      using namespace Zfb::Save; \
      auto v = SaveFn(Base::get(o)); \
      fbb.add_##ID(&v); \
    } \
    template <typename FBS_> \
    static decltype(auto) load_(const FBS_ *o_) { \
      using namespace Zfb::Load; \
      auto v = o_->ID(); \
      return LoadFn(v); \
    } \
    static void load(O &, const FBS *) { } \
  }; \
  template <typename O> \
  struct ZvFBFieldType_(O_, ID)<O, false, decltype(&ZvFBS<O>::ID, void())> : \
      public ZvFBFieldType_(O_, ID)<O, true> { \
    using Base = Base_; \
    using FBS = ZvFBS<O>; \
    using ZvFBFieldType_(O_, ID)<O, true>::load_; \
    static void load(O &o, const FBS *o_) { \
      Base::set(o, load_(o_)); \
    } \
  }; \
  using ZvFBFieldType(O_, ID) = ZvFBFieldType_(O_, ID)<>;

#define ZvFBFieldPrimitive(O_, ID, Base_) \
  ZvFBFieldGeneric(O_, ID, Base_) \
  template <typename O> \
  struct ZvFBFieldType_(O_, ID)<O, true, decltype(&ZvFBS<O>::ID, void())> : \
      public Base_ { \
    using Base = Base_; \
    using FBB = ZvFBB<O>; \
    using FBS = ZvFBS<O>; \
    enum { Inline = 1 }; \
    template <typename FBB_> \
    static void save(FBB_ &fbb, const O &o) { \
      using P = ZuType<0, typename ZuDeduce<decltype(&FBB_::add_##ID)>::Args>; \
      fbb.add_##ID(static_cast<P>(Base::get(o))); \
    } \
    template <typename FBS_> \
    static decltype(auto) load_(const FBS_ *o_) { return o_->ID(); } \
    static void load(O &, const FBS *) { } \
  }; \
  template <typename O> \
  struct ZvFBFieldType_(O_, ID)<O, false, decltype(&ZvFBS<O>::ID, void())> : \
      public ZvFBFieldType_(O_, ID)<O, true> { \
    using Base = Base_; \
    using FBS = ZvFBS<O>; \
    using ZvFBFieldType_(O_, ID)<O, true>::load_; \
    static void load(O &o, const FBS *o_) { \
      Base::set(o, load_(o_)); \
    } \
  }; \
  using ZvFBFieldType(O_, ID) = ZvFBFieldType_(O_, ID)<>;

#define ZvFBFieldString_T String
#define ZvFBFieldString(O, ID, Base) \
  ZvFBFieldNested(O, ID, Base, str, str)
#define ZvFBFieldBytes_T String
#define ZvFBFieldBytes(O, ID, Base) \
  ZvFBFieldNested(O, ID, Base, bytes, bytes)
#define ZvFBFieldBool_T Bool
#define ZvFBFieldBool ZvFBFieldPrimitive
#define ZvFBFieldInt_T Int
#define ZvFBFieldInt ZvFBFieldPrimitive
#define ZvFBFieldHex_T Hex
#define ZvFBFieldHex ZvFBFieldPrimitive
#define ZvFBFieldEnum_T Enum
#define ZvFBFieldEnum ZvFBFieldPrimitive
#define ZvFBFieldFlags_T Flags
#define ZvFBFieldFlags ZvFBFieldPrimitive
#define ZvFBFieldFloat_T Float
#define ZvFBFieldFloat ZvFBFieldPrimitive
#define ZvFBFieldFixed_T Fixed
#define ZvFBFieldFixed ZvFBFieldPrimitive
#define ZvFBFieldDecimal_T Decimal
#define ZvFBFieldDecimal(O, ID, Base) \
  ZvFBFieldInline(O, ID, Base, decimal, decimal)
#define ZvFBFieldTime_T Time
#define ZvFBFieldTime(O, ID, Base) \
  ZvFBFieldInline(O, ID, Base, dateTime, dateTime)
#define ZvFBFieldBitmap_T Composite
#define ZvFBFieldBitmap(O, ID, Base) \
  ZvFBFieldNested(O, ID, Base, bitmap, bitmap)
#define ZvFBFieldIP_T Composite
#define ZvFBFieldIP(O, ID, Base) \
  ZvFBFieldInline(O, ID, Base, ip, ip)

#define ZvFBField_Decl__(O, ID, Base, TypeName, Type, ...) \
  ZuPP_Defer(ZvField_Decl_)(O, Base, \
      (ZvFBField##TypeName##_T ZuPP_Nest(ZvField_TypeArgs(Type))) \
      __VA_OPT__(,) __VA_ARGS__) \
  ZuPP_Defer(ZvFBField##TypeName)(O, ID, ZuPP_Nest(ZvFieldType(O, ID)))
#define ZvFBField_Decl_(O, Base, Type, ...) \
  ZuPP_Defer(ZvFBField_Decl__)(O, \
      ZuPP_Nest(ZvField_BaseID(Base)), Base, \
      ZuPP_Nest(ZvField_TypeName(Type)), Type __VA_OPT__(,) __VA_ARGS__)
#define ZvFBField_Decl(O, Args) ZuPP_Defer(ZvFBField_Decl_)(O, ZuPP_Strip(Args))

#define ZvFBField_Type_(O, Base, ...) \
  ZuPP_Defer(ZvFBFieldType)(O, ZuPP_Nest(ZvField_BaseID(Base)))
#define ZvFBField_Type(O, Args) ZuPP_Defer(ZvFBField_Type_)(O, ZuPP_Strip(Args))

#define ZvFBFields(O, ...)  \
  fbs::O##Builder *ZvFBB_(O *); \
  fbs::O *ZvFBS_(O *); \
  ZuPP_Eval(ZuPP_MapArg(ZvFBField_Decl, O, __VA_ARGS__)) \
  using ZvFBFields_##O = \
    ZuTypeList<ZuPP_Eval(ZuPP_MapArgComma(ZvFBField_Type, O, __VA_ARGS__))>; \
  O *ZuFielded_(O *); \
  ZvFBFields_##O ZuFieldList_(O *)

namespace ZvFB {

namespace Type = ZvFieldType;
namespace Flags = ZvFieldFlags;

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
  template <typename FBB>
  static void save(FBB &fbb, const O &o, const Offset<void> *) {
    Field::save(fbb, o);
  }
};
template <typename O, typename OffsetFieldList, typename Field>
struct SaveField<O, OffsetFieldList, Field, true> {
  template <typename FBB>
  static void save(FBB &fbb, const O &, const Offset<void> *offsets) {
    using OffsetIndex = ZuTypeIndex<Field, OffsetFieldList>;
    Field::save(fbb, offsets[OffsetIndex::I]);
  }
};
template <typename O, typename FieldList>
struct SaveFieldList {
  using FBB = ZvFBB<O>;
  using FBS = ZvFBS<O>;
  static Zfb::Offset<FBS> save(Zfb::Builder &fbb_, const O &o) {
    using OffsetFieldList = ZuTypeGrep<HasOffset, FieldList>;
    Offset<void> offsets[OffsetFieldList::N];
    ZuTypeAll<OffsetFieldList>::invoke(
	[&fbb_, &o, offsets = &offsets[0]]<typename Field>() {
	  using OffsetIndex = ZuTypeIndex<Field, OffsetFieldList>;
	  offsets[OffsetIndex::I] = Field::save(fbb_, o).Union();
	});
    FBB fbb{fbb_};
    ZuTypeAll<FieldList>::invoke(
	[&fbb, &o, offsets = &offsets[0]]<typename Field>() {
	  SaveField<O, OffsetFieldList, Field>::save(fbb, o, offsets);
	});
    return fbb.Finish();
  }
};

} // namespace Save

template <typename O>
struct Table_ {
  using FBB = ZvFBB<O>;
  using FBS = ZvFBS<O>;
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

  static Zfb::Offset<FBS> save(Zfb::Builder &fbb, const O &o) {
    using namespace Save;
    return SaveFieldList<O, AllFields>::save(fbb, o);
  }
  static Zfb::Offset<FBS> saveUpdate(Zfb::Builder &fbb, const O &o) {
    using namespace Save;
    return SaveFieldList<O, UpdatedFields>::save(fbb, o);
  }

  template <typename ...Fields>
  struct Ctor {
    static O ctor(const FBS *o_) {
      return O{Fields::load_(o_)...};
    }
  };
  static O ctor(const FBS *o_) {
    O o = ZuTypeApply<Ctor, CtorFields>::ctor(o_);
    ZuTypeAll<InitFields>::invoke(
	[&o, o_]<typename Field>() { Field::load(o, o_); });
    return o;
  }

  template <typename ...Fields>
  struct Load_Ctor_ : public O {
    Load_Ctor_() = default;
    Load_Ctor_(const FBS *o_) : O{Fields::load_(o_)...} { }
    template <typename ...Args>
    Load_Ctor_(Args &&... args) : O{ZuFwd<Args>(args)...} { }
  };
  using Load_Ctor = ZuTypeApply<Load_Ctor_, CtorFields>;
  struct Load : public Load_Ctor {
    Load() = default;
    Load(const FBS *o_) : Load_Ctor{o_} {
      ZuTypeAll<InitFields>::invoke(
	  [this, o_]<typename Field>() { Field::load(*this, o_); });
    }
    template <typename ...Args>
    Load(Args &&... args) : Load_Ctor{ZuFwd<Args>(args)...} { }
  };

  static void load(O &o, const FBS *o_) {
    ZuTypeAll<AllFields>::invoke(
	[&o, o_]<typename Field>() { Field::load(o, o_); });
  }
  static void loadUpdate(O &o, const FBS *o_) {
    ZuTypeAll<UpdatedFields>::invoke(
	[&o, o_]<typename Field>() { Field::load(o, o_); });
  }

  template <typename ...Fields>
  struct Key {
    using Tuple = ZuTuple<typename Fields::T...>;
    static decltype(auto) tuple(const FBS *o_) {
      return Tuple{Fields::load_(o_)...};
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
  static decltype(auto) key(const FBS *o_) {
    using Fields = ZuTypeGrep<KeyFilter<KeyID>::template T, FieldList>;
    return Key<Fields>::tuple(o_);
  }
};
template <typename T>
using Table = Table_<ZuFielded<T>>;

template <typename T>
inline auto save(Zfb::Builder &fbb, const T &o) {
  return Table<T>::save(fbb, o);
}
template <typename T>
inline auto saveUpdate(Zfb::Builder &fbb, const T &o) {
  return Table<T>::saveUpdate(fbb, o);
}

template <typename T>
inline T ctor(const ZvFBS<T> *o_) {
  return Table<T>::ctor(o_);
}

template <typename T> using Load = typename Table<T>::Load;

template <typename T>
inline void load(T &o, const ZvFBS<T> *o_) {
  Table<T>::load(o, o_);
}
template <typename T>
inline void loadUpdate(T &o, const ZvFBS<T> *o_) {
  Table<T>::loadUpdate(o, o_);
}

template <typename T>
inline auto key(const ZvFBS<T> *o_) {
  return Table<T>::key(o_);
}

} // namespace ZvFB

#endif /* ZvFBField_HPP */
