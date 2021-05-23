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

#include <zlib/Zfb.hpp>

#include <zlib/ZvField.hpp>

void *ZvFBB_(...); // default
void *ZvFBS_(...); // default

template <typename T>
using ZvFBB = ZuDecay<decltype(*ZvFBB_(ZuDeclVal<T *>()))>;
template <typename T>
using ZvFBS = ZuDecay<decltype(*ZvFBS_(ZuDeclVal<T *>()))>;

#define ZvFBFieldType(U, ID) ZvFBField_##U##_##ID

#define ZvFBFieldRd_(U, ID, Base) \
  struct ZvFBFieldType(U, ID) : public Base { };

#define ZvFBFieldComposite_(U, ID, Base_, SaveFn, LoadFn) \
  struct ZvFBFieldType(U, ID) : public Base_ { \
    using Base = Base_; \
    using FBB = ZvFBB<U>; \
    using FBS = ZvFBS<U>; \
    enum { Inline = 0 }; \
    static Zfb::Offset<void> save(Zfb::Builder &fbb, const void *o) { \
      using namespace Zfb::Save; \
      return SaveFn(fbb, Base::get(o)).Union(); \
    } \
    static void save(FBB &fbb, Zfb::Offset<void> offset) { \
      fbb.add_##ID(offset.o); \
    } \
    static auto load_(const FBS *o_) { \
      using namespace Zfb::Load; \
      return LoadFn(o_->ID()); \
    } \
    static void load(void *o, const FBS *o_) { \
      Base::set(o, load_(o_)); \
    } \
  };

#define ZvFBFieldInline_(U, ID, Base_, SaveFn, LoadFn) \
  struct ZvFBFieldType(U, ID) : public Base_ { \
    using Base = Base_; \
    using FBB = ZvFBB<U>; \
    using FBS = ZvFBS<U>; \
    enum { Inline = 1 }; \
    static void save(FBB &fbb, const void *o) { \
      using namespace Zfb::Save; \
      auto v = SaveFn(Base::get(o)); \
      fbb.add_##ID(&v); \
    } \
    static auto load_(const FBS *o_) { \
      using namespace Zfb::Load; \
      auto v = o_->ID(); \
      return LoadFn(v); \
    } \
    static void load(void *o, const FBS *o_) { \
      Base::set(o, load_(o_)); \
    } \
  };

#define ZvFBFieldPrimitive_(U, ID, Base_) \
  struct ZvFBFieldType(U, ID) : public Base_ { \
    using Base = Base_; \
    using FBB = ZvFBB<U>; \
    using FBS = ZvFBS<U>; \
    enum { Inline = 1 }; \
    static void save(FBB &fbb, const void *o) { fbb.add_##ID(Base::get(o)); } \
    static auto load_(const FBS *o_) { return o_->ID(); } \
    static void load(void *o, const FBS *o_) { Base::set(o, load_(o_)); } \
  };

#define ZvFBFieldString(U, ...) \
  ZvFBFieldComposite_(U, __VA_ARGS__, str, str)

#define ZvFBFieldBool(U, ...) ZvFBFieldPrimitive_(U, __VA_ARGS__)
#define ZvFBFieldInt(U, ...) ZvFBFieldPrimitive_(U, __VA_ARGS__)
#define ZvFBFieldHex(U, ...) ZvFBFieldPrimitive_(U, __VA_ARGS__)
#define ZvFBFieldEnum(U, ...) ZvFBFieldPrimitive_(U, __VA_ARGS__)
#define ZvFBFieldFlags(U, ...) ZvFBFieldPrimitive_(U, __VA_ARGS__)
#define ZvFBFieldFloat(U, ...) ZvFBFieldPrimitive_(U, __VA_ARGS__)
#define ZvFBFieldFixed(U, ...) ZvFBFieldPrimitive_(U, __VA_ARGS__)

#define ZvFBFieldDecimal(U, ...) \
  ZvFBFieldInline_(U, __VA_ARGS__, decimal, decimal)

#define ZvFBFieldTime(U, Method, ...) \
  ZvFBFieldInline_(U, __VA_ARGS__, dateTime, dateTime)

#define ZvFBFieldBitmap(U, ...) \
  ZvFBFieldComposite_(U, __VA_ARGS__, bitmap, bitmap)

#define ZvFBFieldIP(U, ...) \
  ZvFBFieldInline_(U, __VA_ARGS__, ip, ip)

#define ZvFBField_(U, Type, ...) ZvFBField##Type(U, __VA_ARGS__)
#define ZvFBField_X(U, Type, ...) ZvFBField##Type(U, __VA_ARGS__)
#define ZvFBField_Fn(U, Type, ...) ZvFBField##Type(U, __VA_ARGS__)
#define ZvFBField_XFn(U, Type, ...) ZvFBField##Type(U, __VA_ARGS__)
#define ZvFBField_Lambda(U, Type, ...) ZvFBField##Type(U, __VA_ARGS__)

#define ZvFBField_RdFn(U, Type, ...) ZvFBFieldRd_(U, __VA_ARGS__)
#define ZvFBField_XRdFn(U, Type, ...) ZvFBFieldRd_(U, __VA_ARGS__)
#define ZvFBField_Rd(U, Type, ...) ZvFBFieldRd_(U, __VA_ARGS__)
#define ZvFBField_XRd(U, Type, ...) ZvFBFieldRd_(U, __VA_ARGS__)
#define ZvFBField_RdLambda(U, Type, ...) ZvFBFieldRd_(U, __VA_ARGS__)

#define ZvFBField_Decl_(U, Method, Type, ID, ...) \
  ZvFBField_##Method(U, Type, ID, ZvField__(U, Method, Type, ID, __VA_ARGS__))
#define ZvFBField_Decl(U, Args) ZuPP_Defer(ZvFBField_Decl_)(U, ZuPP_Strip(Args))

#define ZvFBField_Type_(U, Method, Type, ID, ...) ZvFBFieldType(U, ID)
#define ZvFBField_Type(U, Args) ZuPP_Defer(ZvFBField_Type_)(U, ZuPP_Strip(Args))

#define ZvFBFields(U, ...)  \
  fbs::U##Builder *ZvFBB_(U *); \
  fbs::U *ZvFBS_(U *); \
  namespace { \
    ZuPP_Eval(ZuPP_MapArg(ZuField_Decl, U, __VA_ARGS__)) \
    ZuPP_Eval(ZuPP_MapArg(ZvField_Decl, U, __VA_ARGS__)) \
    ZuPP_Eval(ZuPP_MapArg(ZvFBField_Decl, U, __VA_ARGS__)) \
    using ZvFBFields_##U = \
      ZuTypeList<ZuPP_Eval(ZuPP_MapArgComma(ZvFBField_Type, U, __VA_ARGS__))>; \
  } \
  U *ZuFielded_(U *); \
  ZvFBFields_##U ZuFieldList_(U *)

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
  typename OffsetFieldList, typename Field, bool = HasOffset<Field>::OK>
struct SaveField {
  template <typename FBB>
  static void save(FBB &fbb, const void *o, const Offset<void> *) {
    Field::save(fbb, o);
  }
};
template <typename OffsetFieldList, typename Field>
struct SaveField<OffsetFieldList, Field, true> {
  template <typename FBB>
  static void save(FBB &fbb, const void *, const Offset<void> *offsets) {
    using OffsetIndex = ZuTypeIndex<Field, OffsetFieldList>;
    Field::save(fbb, offsets[OffsetIndex::I]);
  }
};
template <typename T, typename FieldList>
struct SaveFieldList {
  using FBB = ZvFBB<T>;
  using FBS = ZvFBS<T>;
  static Zfb::Offset<FBS> save(Zfb::Builder &fbb_, const void *o) {
    using OffsetFieldList = ZuTypeGrep<HasOffset, FieldList>;
    Offset<void> offsets[OffsetFieldList::N];
    ZuTypeAll<OffsetFieldList>::invoke(
	[o, &fbb_, offsets = &offsets[0]]<typename Field>() {
	  using OffsetIndex = ZuTypeIndex<Field, OffsetFieldList>;
	  offsets[OffsetIndex::I] = Field::save(fbb_, o).Union();
	});
    FBB fbb{fbb_};
    ZuTypeAll<FieldList>::invoke(
	[o, &fbb, offsets = &offsets[0]]<typename Field>() {
	  SaveField<OffsetFieldList, Field>::save(fbb, o, offsets);
	});
    return fbb.Finish();
  }
};

} // namespace Save

template <typename T>
struct Table_ {
  using FBB = ZvFBB<T>;
  using FBS = ZvFBS<T>;
  using FieldList = List<T>;

  template <typename U>
  struct AllFilter { enum { OK = !(U::Flags & Flags::ReadOnly) }; };
  using AllFields = ZuTypeGrep<AllFilter, FieldList>;

  template <typename U>
  struct UpdatedFilter { enum {
    OK = U::Flags & (Flags::Primary | Flags::Update) }; };
  using UpdatedFields = ZuTypeGrep<UpdatedFilter, AllFields>;

  template <typename U>
  struct CtorFilter { enum { OK = U::Flags & Flags::Ctor_ }; };
  using CtorFields_ = ZuTypeGrep<CtorFilter, AllFields>;
  template <typename U>
  struct CtorIndex { enum { I = Flags::CtorIndex(U::Flags) }; };
  using CtorFields = ZuTypeSort<CtorIndex, CtorFields_>;

  template <typename U>
  struct InitFilter { enum { OK = !(U::Flags & Flags::Ctor_) }; };
  using InitFields = ZuTypeGrep<InitFilter, AllFields>;

  template <typename U>
  struct KeyFilter { enum { OK = U::Flags & Flags::Primary }; };
  using KeyFields = ZuTypeGrep<KeyFilter, AllFields>;

  static Zfb::Offset<FBS> save(Zfb::Builder &fbb, const void *o) {
    using namespace Save;
    return SaveFieldList<T, AllFields>::save(fbb, o);
  }
  static Zfb::Offset<FBS> saveUpdate(Zfb::Builder &fbb, const void *o) {
    using namespace Save;
    return SaveFieldList<T, UpdatedFields>::save(fbb, o);
  }
  static void load(void *o, const FBS *o_) {
    ZuTypeAll<AllFields>::invoke(
	[o, o_]<typename Field>() { Field::load(o, o_); });
  }
  static void loadUpdate(void *o, const FBS *o_) {
    ZuTypeAll<UpdatedFields>::invoke(
	[o, o_]<typename Field>() { Field::load(o, o_); });
  }

  template <typename ...Fields>
  struct Load_Ctor_ : public T {
    Load_Ctor_() = default;
    Load_Ctor_(const FBS *o_) : T{Fields::load_(o_)...} { }
  };
  using Load_Ctor = ZuTypeApply<Load_Ctor_, CtorFields>;
  struct Load : public Load_Ctor {
    Load() = default;
    Load(const FBS *o_) : Load_Ctor{o_} {
      ZuTypeAll<InitFields>::invoke(
	  [this, o_]<typename Field>() {
	    Field::load(static_cast<T *>(this), o_);
	  });
    }
  };

  template <typename ...Fields>
  struct Key {
    using Tuple = ZuTuple<typename Fields::T...>;
    static auto tuple(const FBS *o_) { return Tuple{Fields::load_(o_)...}; }
  };
  static auto key(const FBS *o_) {
    using Mk = ZuTypeApply<Key, KeyFields>;
    return Mk::tuple(o_);
  }
};
template <typename T>
using Table = Table_<ZuFielded<T>>;

template <typename T>
inline auto save(Zfb::Builder &fbb, const T &o) {
  return Table<T>::save(fbb, &o);
}
template <typename T>
inline auto saveUpdate(Zfb::Builder &fbb, const T &o) {
  return Table<T>::saveUpdate(fbb, &o);
}

template <typename T>
inline void load(T &o, const ZvFBS<T> *o_) {
  Table<T>::load(&o, o_);
}
template <typename T>
inline void loadUpdate(T &o, const ZvFBS<T> *o_) {
  Table<T>::loadUpdate(&o, o_);
}

template <typename T> using Load = typename Table<T>::Load;

template <typename T>
inline auto key(const ZvFBS<T> *o_) {
  return Table<T>::key(o_);
}

} // namespace ZvFB

#endif /* ZvFBField_HPP */
