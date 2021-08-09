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

// composite object field metadata framework
//
// enables compile-time generic identification and access to fields
//
// each field within any tuple-like composite object has a constexpr
// string ID, a get accessor and a set accessor (unless read-only)

#ifndef ZuField_HPP
#define ZuField_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuLambdaFn.hpp>
#include <zlib/ZuTuple.hpp>

#define ZuFieldType(U, ID) ZuField_##U##_##ID

// Metadata macro DSL for identifying and accessing data fields and keys:
//
// Syntax
// ------
// ((Accessor)[, (Keys...)])
//
// Accessor			Description
// --------			-----------
// ID				data member
// ID, Rd			read-only data member
// ID, Alias, Member		data member - ID aliased to member
// ID, AliasRd, Member		read-only ''
// ID, Fn			function
// ID, RdFn			read-only function
// ID, AliasFn, Get, Set	function - ID aliased to getter, setter
// ID, AliasRdFn, Get		read-only ''
// ID, Lambda, Get, Set		lambda accessor
// ID, RdLambda, Get		read-only lambda accessor
//
// if specified, Keys is a parenthesized list of key IDs 0..63, allocated
// from 0, e.g. (0, 1) - by convention, 0 is the primary key and 1+ are
// secondary keys

#define ZuField_Keys__(Key) (static_cast<uint64_t>(1)<<Key) |
#define ZuField_Keys_(...) (ZuPP_Map(ZuField_Keys__, __VA_ARGS__) 0)
#define ZuField_Keys(Args) ZuPP_Defer(ZuField_Keys_)(ZuPP_Strip(Args))

#define ZuField_ID(ID) \
  static constexpr const char *id() { return #ID; }
#define ZuField_1(ID) \
  ZuField_ID(ID) \
  static constexpr uint64_t keys() { return 0; }
#define ZuField_2(ID, Keys_) \
  ZuField_ID(ID) \
  static constexpr uint64_t keys() { return ZuField_Keys(Keys_); }
#define ZuField_N(_0, _1, Fn, ...) Fn
#define ZuField__(...) \
  ZuField_N(__VA_ARGS__, ZuField_2(__VA_ARGS__), ZuField_1(__VA_ARGS__))
#define ZuField_(ID, Args) \
  ZuPP_Defer(ZuField__)(ID ZuPP_StripAppend(Args))

#define ZuFieldAliasRd_(U, Member) \
  using T = ZuDecay<decltype(ZuDeclVal<const U &>().Member)>; \
  static decltype(auto) get(const void *o) { \
    return static_cast<const U *>(o)->Member; \
  }
#define ZuFieldAlias_(U, Member) \
  template <typename V> \
  static void set(void *o, V &&v) { \
    static_cast<U *>(o)->Member = ZuFwd<V>(v); \
  }
#define ZuFieldAliasRd(U, ID, Member, Args) \
  struct ZuFieldType(U, ID) { \
    enum { ReadOnly = 1 }; \
    ZuField_(ID, Args) \
    ZuFieldAliasRd_(U, Member) \
  };
#define ZuFieldAlias(U, ID, Member, Args) \
  struct ZuFieldType(U, ID) { \
    enum { ReadOnly = 0 }; \
    ZuField_(ID, Args) \
    ZuFieldAliasRd_(U, Member) \
    ZuFieldAlias_(U, Member) \
  };
#define ZuFieldAliasRdFn_(U, Get) \
  using T = ZuDecay<decltype(ZuDeclVal<const U &>().Get())>; \
  static decltype(auto) get(const void *o) { \
    return static_cast<const U *>(o)->Get(); \
  }
#define ZuFieldAliasFn_(U, Set) \
  template <typename V> \
  static void set(void *o, V &&v) { \
    static_cast<U *>(o)->Set(ZuFwd<V>(v)); \
  }
#define ZuFieldAliasRdFn(U, ID, Get, Args) \
  struct ZuFieldType(U, ID) { \
    enum { ReadOnly = 1 }; \
    ZuField_(ID, Args) \
    ZuFieldAliasRdFn_(U, Get) \
  };
#define ZuFieldAliasFn(U, ID, Get, Set, Args) \
  struct ZuFieldType(U, ID) { \
    enum { ReadOnly = 0 }; \
    ZuField_(ID, Args) \
    ZuFieldAliasRdFn_(U, Get) \
    ZuFieldAliasFn_(U, Set) \
  };
#define ZuFieldRdLambda_(U, Get) \
  static decltype(auto) get(const void *o) { \
    auto fn = Get(); \
    return fn(*static_cast<const U *>(o)); \
  }
#define ZuFieldLambda_(U, Set) \
  template <typename V> \
  static void set(void *o, V &&v) { \
    auto fn = ZuPP_Strip(Set); \
    fn(*static_cast<U *>(o), ZuFwd<V>(v)); \
  }
#define ZuFieldRdLambda(U, ID, Get, Args) \
  inline auto ZuField_##U##_##ID##_get() { return ZuPP_Strip(Get); } \
  struct ZuFieldType(U, ID) { \
    enum { ReadOnly = 1 }; \
    using T = \
      ZuDecay<decltype(ZuField_##U##_##ID##_get()(ZuDeclVal<const U &>()))>; \
    ZuField_(ID, Args) \
    ZuFieldRdLambda_(U, ZuField_##U##_##ID##_get) \
  };
#define ZuFieldLambda(U, ID, Get, Set, Args) \
  inline auto ZuField_##U##_##ID##_get() { return ZuPP_Strip(Get); } \
  struct ZuFieldType(U, ID) { \
    enum { ReadOnly = 0 }; \
    using T = \
      ZuDecay<decltype(ZuField_##U##_##ID##_get()(ZuDeclVal<const U &>()))>; \
    ZuField_(ID, Args) \
    ZuFieldRdLambda_(U, ZuField_##U##_##ID##_get) \
    ZuFieldLambda_(U, Set) \
  };

#define ZuField(U, Member, Args) \
  ZuFieldAlias(U, Member, Member, Args)
#define ZuFieldRd(U, Member, Args) \
  ZuFieldAliasRd(U, Member, Member, Args)

#define ZuFieldFn(U, Fn, Args) \
  ZuFieldAliasFn(U, Fn, Fn, Fn, Args)
#define ZuFieldRdFn(U, Fn, Args) \
  ZuFieldAliasRdFn(U, Fn, Fn, Args)

#define ZuField_Decl_2(U, ID, Args) \
  ZuField(U, ID, Args)
#define ZuField_Decl_3(U, ID, Method, Args) \
  ZuField##Method(U, ID, Args)
#define ZuField_Decl_4(U, ID, Method, Get, Args) \
  ZuField##Method(U, ID, Get, Args)
#define ZuField_Decl_5(U, ID, Method, Get, Set, Args) \
  ZuField##Method(U, ID, Get, Set, Args)
#define ZuField_Decl_N(U, _0, _1, _2, _3, _4, Fn, ...) Fn
#define ZuField_Decl__(U, ...) \
  ZuField_Decl_N(U, __VA_ARGS__, \
      ZuField_Decl_5(U, __VA_ARGS__), \
      ZuField_Decl_4(U, __VA_ARGS__), \
      ZuField_Decl_3(U, __VA_ARGS__), \
      ZuField_Decl_2(U, __VA_ARGS__))
#define ZuField_Decl_(U, Axor, ...) \
  ZuPP_Defer(ZuField_Decl__)(U, ZuPP_Strip(Axor), (__VA_ARGS__))
#define ZuField_Decl(U, Args) ZuPP_Defer(ZuField_Decl_)(U, ZuPP_Strip(Args))

#define ZuField_Type__(U, ID, ...) ZuFieldType(U, ID)
#define ZuField_Type_(U, Axor, ...) \
  ZuPP_Defer(ZuField_Type__)(U, ZuPP_Strip(Axor))
#define ZuField_Type(U, Args) ZuPP_Defer(ZuField_Type_)(U, ZuPP_Strip(Args))

ZuTypeList<> ZuFields_(...); // default

void *ZuFielded_(...); // default

#define ZuFields(U, ...) \
  namespace { \
    ZuPP_Eval(ZuPP_MapArg(ZuField_Decl, U, __VA_ARGS__)) \
    using ZuFields_##U = \
      ZuTypeList<ZuPP_Eval(ZuPP_MapArgComma(ZuField_Type, U, __VA_ARGS__))>; \
  } \
  U *ZuFielded_(U *); \
  ZuFields_##U ZuFieldList_(U *)

template <typename U>
using ZuFieldList = decltype(ZuFieldList_(ZuDeclVal<U *>()));

template <typename T>
using ZuFielded = ZuDeref<decltype(*ZuFielded_(ZuDeclVal<ZuDecay<T> *>()))>;

// derived field list for pointers to fielded types

template <typename U>
struct ZuFields_Ptr {
  using P = U *;
  template <typename Field, bool = Field::ReadOnly>
  struct Map : public Field {
    static decltype(auto) get(const void *o) {
      return Field::get(
	  static_cast<const void *>(&(*(*static_cast<const P *>(o)))));
    }
  };
  template <typename Field>
  struct Map<Field, false> : public Map<Field, true> {
    template <typename V>
    static void set(void *o, V &&v) {
      Field::set(static_cast<void *>(&(*(*static_cast<P *>(o)))), ZuFwd<V>(v));
    }
  };
  using T = ZuTypeMap<Map, ZuFieldList<U>>;
};
template <typename U>
typename ZuFields_Ptr<U>::T ZuFields_(U * const *);

// tuple from type, field list

template <typename V, template <typename> typename Map, typename ...Fields>
class ZuFieldTuple_ : public ZuTuple<Map<typename Fields::T>...> {
  using Base = ZuTuple<Map<typename Fields::T>...>;
public:
  ZuFieldTuple_(const ZuFieldTuple_ &) = default;
  ZuFieldTuple_ &operator =(const ZuFieldTuple_ &) = default;
  ZuFieldTuple_(ZuFieldTuple_ &&) = default;
  ZuFieldTuple_ &operator =(ZuFieldTuple_ &&) = default;
  ZuFieldTuple_(const Base &v) : Base{v} { }
  ZuFieldTuple_ &operator =(const Base &v) {
    Base::operator =(v);
    return *this;
  }
  ZuFieldTuple_(Base &&v) : Base{ZuMv(v)} { }
  ZuFieldTuple_ &operator =(Base &&v) {
    Base::operator =(ZuMv(v));
    return *this;
  }
  ZuFieldTuple_(const V &v) : Base{Fields::get(&v)...} { }
  ZuFieldTuple_ &operator =(const V &v) {
    ~Base();
    new (static_cast<Base *>(this)) Base{Fields::get(&v)...};
    return *this;
  }
};
template <typename V, template <typename> typename Map, typename ...Fields>
struct ZuFieldTuple_<V, Map, ZuTypeList<Fields...>> :
  public ZuFieldTuple_<V, Map, Fields...> { };

template <typename V, typename ...Fields>
struct ZuFieldTuple_Bind {
  static decltype(auto) get(const V &v) {
    return ZuFieldTuple_<V, ZuCRef, Fields...>{v};
  }
  static decltype(auto) get(V &v) {
    return ZuFieldTuple_<V, ZuLRef, Fields...>{v};
  }
  static decltype(auto) get(V &&v) {
    return ZuFieldTuple_<V, ZuAsIs, Fields...>{ZuMv(v)};
  }
};

template <typename ...Fields>
struct ZuFieldTuple {
  template <typename P>
  static decltype(auto) get(P &&v) {
    return ZuFieldTuple_Bind<ZuDecay<P>, Fields...>::get(ZuFwd<P>(v));
  }
};

// generic accessor
template <typename V, unsigned KeyID = 0>
struct ZuFieldAxor {
  template <typename T>
  struct KeyFilter { enum { OK = T::keys() & (1<<KeyID) }; };
  template <typename P>
  static decltype(auto) get(P &&v) {
    return ZuFieldTuple_<
      ZuDecay<P>, ZuTypeGrep<KeyFilter, ZuFieldList<V>>>::get(ZuFwd<P>(v));
  }
};

#endif /* ZuField_HPP */
