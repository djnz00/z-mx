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
//
// macro DSL for declaring metadata identifying and accessing fields and keys:
//
// ZuFields(Type, Field, ...);
//
// Field Syntax
// ------------
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
// ID, LambdaRd, Get		read-only lambda accessor
//
// if specified, Keys is a parenthesized list of key IDs 0..63, allocated
// from 0, e.g. (0, 1) - by convention, 0 is the primary key and 1+ are
// secondary keys

#ifndef ZuField_HPP
#define ZuField_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuTuple.hpp>

#define ZuFieldType(O, ID) ZuField_##O##_##ID

#define ZuField_Keys__(Key) (static_cast<uint64_t>(1)<<Key) |
#define ZuField_Keys_(...) (ZuPP_Map(ZuField_Keys__, __VA_ARGS__) 0)
#define ZuField_Keys(Args) ZuPP_Defer(ZuField_Keys_)Args

#define ZuField_ID(O_, ID) \
  using O = O_; \
  constexpr static const char *id() { return #ID; }
#define ZuField_1(O, ID) \
  ZuField_ID(O, ID) \
  constexpr static uint64_t keys() { return 0; }
#define ZuField_2(O, ID, Keys_) \
  ZuField_ID(O, ID) \
  constexpr static uint64_t keys() { return ZuField_Keys(Keys_); }
#define ZuField_N(O, _0, _1, Fn, ...) Fn
#define ZuField__(O, ...) \
  ZuField_N(O, __VA_ARGS__, \
    ZuField_2(O, __VA_ARGS__), \
    ZuField_1(O, __VA_ARGS__))
#define ZuField_(O, ID, Args) \
  ZuPP_Defer(ZuField__)(O, ID ZuPP_StripAppend(Args))

#define ZuFieldAliasRd_(O, Member) \
  using T = ZuDecay<decltype(ZuDeclVal<const O &>().Member)>; \
  static decltype(auto) get(const O &o) { return o.Member; } \
  static decltype(auto) get(O &o) { return o.Member; } \
  static decltype(auto) get(O &&o) { return ZuMv(o.Member); }
#define ZuFieldAlias_(O, Member) \
  template <typename P> \
  static void set(O &o, P &&v) { o.Member = ZuFwd<P>(v); }
#define ZuFieldAliasRd(O, ID, Member, Args) \
  struct ZuFieldType(O, ID) { \
    enum { ReadOnly = 1 }; \
    ZuField_(O, ID, Args) \
    ZuFieldAliasRd_(O, Member) \
    template <typename P> static void set(O &, P &&) { } \
  };
#define ZuFieldAlias(O, ID, Member, Args) \
  struct ZuFieldType(O, ID) { \
    enum { ReadOnly = 0 }; \
    ZuField_(O, ID, Args) \
    ZuFieldAliasRd_(O, Member) \
    ZuFieldAlias_(O, Member) \
  };
#define ZuFieldAliasRdFn_(O, Get) \
  using T = ZuDecay<decltype(ZuDeclVal<const O &>().Get())>; \
  static decltype(auto) get(const O &o) { return o.Get(); } \
  static decltype(auto) get(O &o) { return o.Get(); } \
  static decltype(auto) get(O &&o) { return ZuMv(ZuMv(o).Get()); }
#define ZuFieldAliasFn_(O, Set) \
  template <typename V> \
  static void set(O &o, V &&v) { o.Set(ZuFwd<V>(v)); }
#define ZuFieldAliasRdFn(O, ID, Get, Args) \
  struct ZuFieldType(O, ID) { \
    enum { ReadOnly = 1 }; \
    ZuField_(O, ID, Args) \
    ZuFieldAliasRdFn_(O, Get) \
    template <typename P> static void set(O &, P &&) { } \
  };
#define ZuFieldAliasFn(O, ID, Get, Set, Args) \
  struct ZuFieldType(O, ID) { \
    enum { ReadOnly = 0 }; \
    ZuField_(O, ID, Args) \
    ZuFieldAliasRdFn_(O, Get) \
    ZuFieldAliasFn_(O, Set) \
  };
#define ZuFieldLambdaRd_(Get) \
  template <typename P> \
  static decltype(auto) get(P &&o) { \
    auto fn = Get(); \
    return fn(ZuFwd<P>(o)); \
  }
#define ZuFieldLambda_(O, Set) \
  template <typename P> \
  static void set(O &o, P &&v) { \
    auto fn = ZuPP_Strip(Set); \
    fn(o, ZuFwd<P>(v)); \
  }
#define ZuFieldLambdaRd(O, ID, Get, Args) \
  inline auto ZuField_##O##_##ID##_get() { return ZuPP_Strip(Get); } \
  struct ZuFieldType(O, ID) { \
    enum { ReadOnly = 1 }; \
    using T = \
      ZuDecay<decltype(ZuField_##O##_##ID##_get()(ZuDeclVal<const O &>()))>; \
    ZuField_(O, ID, Args) \
    ZuFieldLambdaRd_(ZuField_##O##_##ID##_get) \
    template <typename P> static void set(O &, P &&) { } \
  };
#define ZuFieldLambda(O, ID, Get, Set, Args) \
  inline auto ZuField_##O##_##ID##_get() { return ZuPP_Strip(Get); } \
  struct ZuFieldType(O, ID) { \
    enum { ReadOnly = 0 }; \
    using T = \
      ZuDecay<decltype(ZuField_##O##_##ID##_get()(ZuDeclVal<const O &>()))>; \
    ZuField_(O, ID, Args) \
    ZuFieldLambdaRd_(ZuField_##O##_##ID##_get) \
    ZuFieldLambda_(O, Set) \
  };

#define ZuField(U, Member, Args) \
  ZuFieldAlias(U, Member, Member, Args)
#define ZuFieldRd(U, Member, Args) \
  ZuFieldAliasRd(U, Member, Member, Args)

#define ZuFieldFn(U, Fn, Args) \
  ZuFieldAliasFn(U, Fn, Fn, Fn, Args)
#define ZuFieldRdFn(U, Fn, Args) \
  ZuFieldAliasRdFn(U, Fn, Fn, Args)

#define ZuField_Decl_2(O, ID, Args) \
  ZuField(O, ID, Args)
#define ZuField_Decl_3(O, ID, Method, Args) \
  ZuField##Method(O, ID, Args)
#define ZuField_Decl_4(O, ID, Method, Get, Args) \
  ZuField##Method(O, ID, Get, Args)
#define ZuField_Decl_5(O, ID, Method, Get, Set, Args) \
  ZuField##Method(O, ID, Get, Set, Args)
#define ZuField_Decl_N(O, _0, _1, _2, _3, _4, Fn, ...) Fn
#define ZuField_Decl__(O, ...) \
  ZuField_Decl_N(O, __VA_ARGS__, \
      ZuField_Decl_5(O, __VA_ARGS__), \
      ZuField_Decl_4(O, __VA_ARGS__), \
      ZuField_Decl_3(O, __VA_ARGS__), \
      ZuField_Decl_2(O, __VA_ARGS__))
#define ZuField_Decl_(O, Axor, ...) \
  ZuPP_Defer(ZuField_Decl__)(O, ZuPP_Strip(Axor), (__VA_ARGS__))
#define ZuField_Decl(O, Args) ZuPP_Defer(ZuField_Decl_)(O, ZuPP_Strip(Args))

#define ZuField_Type__(O, ID, ...) ZuFieldType(O, ID)
#define ZuField_Type_(O, Axor, ...) \
  ZuPP_Defer(ZuField_Type__)(O, ZuPP_Strip(Axor))
#define ZuField_Type(O, Args) ZuPP_Defer(ZuField_Type_)(O, ZuPP_Strip(Args))

ZuTypeList<> ZuFieldList_(...); // default
void *ZuFielded_(...); // default

#define ZuFields(O, ...) \
  namespace ZuFields_ { \
    ZuPP_Eval(ZuPP_MapArg(ZuField_Decl, O, __VA_ARGS__)) \
    using O = \
      ZuTypeList<ZuPP_Eval(ZuPP_MapArgComma(ZuField_Type, O, __VA_ARGS__))>; \
  } \
  O *ZuFielded_(O *); \
  ZuFields_::O ZuFieldList_(O *)

template <typename U>
using ZuFieldList = decltype(ZuFieldList_(ZuDeclVal<U *>()));

template <typename T>
using ZuFielded = ZuDeref<decltype(*ZuFielded_(ZuDeclVal<ZuDecay<T> *>()))>;

// generic tuple from field list

template <typename O, template <typename> typename Map, typename ...Fields>
struct ZuFieldTuple__ {
  using T = ZuTuple<decltype(Fields::get(ZuDeclVal<Map<O>>()))...>;
};
template <typename O, template <typename> typename Map, typename ...Fields>
struct ZuFieldTuple__<O, Map, ZuTypeList<Fields...>> :
  public ZuFieldTuple__<O, Map, Fields...> { };
template <typename O, template <typename> typename Map, typename ...Fields>
using ZuFieldTuple_ = typename ZuFieldTuple__<O, Map, Fields...>::T;
template <typename O, typename ...Fields>
struct ZuFieldTuple_Bind {
  static decltype(auto) get(const O &o) {
    return ZuFieldTuple_<O, ZuCRef, Fields...>{Fields::get(o)...};
  }
  static decltype(auto) get(O &&o) {
    return ZuFieldTuple_<O, ZuAsIs, Fields...>{Fields::get(ZuMv(o))...};
  }
};
template <typename O, typename ...Fields>
struct ZuFieldTuple_Bind<O, ZuTypeList<Fields...>> :
  public ZuFieldTuple_Bind<O, Fields...> { };
template <typename ...Fields>
struct ZuFieldTuple {
  using O = typename ZuType<0, Fields...>::O;
  template <typename P>
  static decltype(auto) get(P &&o) {
    return ZuFieldTuple_Bind<O, Fields...>::get(ZuFwd<P>(o));
  }
  template <typename ...Args>
  static decltype(auto) mk(Args &&... args) {
    return ZuFieldTuple_<O, ZuDecay, Fields...>{ZuFwd<Args>(args)...};
  }
};
template <typename ...Fields>
struct ZuFieldTuple<ZuTypeList<Fields...>> :
  public ZuFieldTuple<Fields...> { };

// generic key accessor

template <unsigned KeyID>
struct ZuFieldKey {
  template <typename U>
  struct Filter { enum { OK = U::keys() & (1<<KeyID) }; };
};
template <typename O, unsigned KeyID = 0>
inline constexpr auto ZuFieldAxor() {
  return [] <typename P> (P &&o) -> decltype(auto) {
    using Fields =
      ZuTypeGrep<ZuFieldKey<KeyID>::template Filter, ZuFieldList<O>>;
    return ZuFieldTuple_Bind<O, Fields>::get(ZuFwd<P>(o));
  };
}

#endif /* ZuField_HPP */
