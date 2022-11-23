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

// Zu library main header

#ifndef ZuLib_HPP
#define ZuLib_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifdef _WIN32

#ifndef WINVER
#define WINVER 0x0800
#endif

#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0800
#endif

#ifndef _WIN32_DCOM
#define _WIN32_DCOM
#endif

#ifndef _WIN32_WINDOWS
#define _WIN32_WINDOWS 0x0800
#endif

#ifndef _WIN32_IE
#define _WIN32_IE 0x0700
#endif

#ifndef __MSVCRT_VERSION__
#define __MSVCRT_VERSION__ 0x0A00
#endif

#ifndef UNICODE
#define UNICODE
#endif

#ifndef _UNICODE
#define _UNICODE
#endif

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <windows.h>
#include <stdlib.h>
#include <malloc.h>
#include <memory.h>
#include <tchar.h>

#define ZuExport_API __declspec(dllexport)
#define ZuExport_Explicit
#define ZuImport_API __declspec(dllimport)
#define ZuImport_Explicit extern

#ifdef _MSC_VER
#pragma warning(disable:4231 4503)
#endif

#ifdef ZU_EXPORTS
#define ZuAPI ZuExport_API
#define ZuExplicit ZuExport_Explicit
#else
#define ZuAPI ZuImport_API
#define ZuExplicit ZuImport_Explicit
#endif
#define ZuExtern extern ZuAPI

#else /* _WIN32 */

#define ZuAPI
#define ZuExplicit
#define ZuExtern extern

#endif /* _WIN32 */

// to satisfy the pedants
#include <limits.h>
#if CHAR_BIT != 8
#error "Broken platform - CHAR_BIT is not 8 - a byte is not 8 bits!"
#endif
#if UINT_MAX < 0xffffffff
#error "Broken platform - UINT_MAX is < 0xffffffff - an int is < 32 bits!"
#endif

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

#ifdef __GNUC__

#define ZuLikely(x) __builtin_expect(!!(x), 1)
#define ZuUnlikely(x) __builtin_expect(!!(x), 0)

#ifdef ZDEBUG
#define ZuInline inline
#define ZuNoInline inline
#else
#define ZuInline inline __attribute__((always_inline))
#define ZuNoInline inline __attribute__((noinline))
#endif

#else

#define ZuLikely(x) (x)
#define ZuUnlikely(x) (x)

#ifdef _MSC_VER
#define ZuInline __forceinline
#else
#define ZuInline
#endif
#define ZuNoInline

#endif

#ifdef __GNUC__
#define ZuMayAlias(x) __attribute__((__may_alias__)) x
#else
#define ZuMayAlias(x) x
#endif

#if defined (__cplusplus)
// std::remove_reference without dragging in STL cruft
template <typename T_>
struct ZuDeref_ { using T = T_; };
template <typename T_>
struct ZuDeref_<T_ &> { using T = T_; };
template <typename T_>
struct ZuDeref_<const T_ &> { using T = const T_; };
template <typename T_>
struct ZuDeref_<volatile T_ &> { using T = volatile T_; };
template <typename T_>
struct ZuDeref_<const volatile T_ &> { using T = const volatile T_; };
template <typename T_>
struct ZuDeref_<T_ &&> { using T = T_; };
template <typename T>
using ZuDeref = typename ZuDeref_<T>::T;

// std::remove_cv (strip qualifiers) without dragging in STL cruft
template <typename T_>
struct ZuStrip_ { using T = T_; };
template <typename T_>
struct ZuStrip_<const T_> { using T = T_; };
template <typename T_>
struct ZuStrip_<volatile T_> { using T = T_; };
template <typename T_>
struct ZuStrip_<const volatile T_> { using T = T_; };
template <typename T>
using ZuStrip = typename ZuStrip_<T>::T;

// std::decay without dragging in STL cruft
template <typename T> using ZuDecay = ZuStrip<ZuDeref<T>>;

// various type mappings used as template parameters
template <typename T> using ZuAsIs = T;
template <typename T> using ZuConst = const T;
template <typename T> using ZuVolatile = volatile T;
template <typename T> using ZuLRef = T &;
template <typename T> using ZuCRef = const T &;

// shorthand constexpr std::forward without STL cruft
template <typename T>
inline constexpr T &&ZuFwd(ZuDeref<T> &v) noexcept { // fwd lvalue
  return static_cast<T &&>(v);
}
template <typename T>
inline constexpr T &&ZuFwd(ZuDeref<T> &&v) noexcept { // fwd rvalue
  return static_cast<T &&>(v);
}
// shorthand constexpr std::move without STL cruft
template <typename T>
inline constexpr ZuDeref<T> &&ZuMv(T &&v) noexcept {
  return static_cast<ZuDeref<T> &&>(v);
}

// generic RAII guard
template <typename L> struct ZuGuard {
  ZuGuard(L fn_) : fn(ZuMv(fn_)) { }
  ~ZuGuard() { fn(); }
  ZuGuard(const ZuGuard &) = delete;
  ZuGuard &operator =(const ZuGuard &) = delete;
  ZuGuard(ZuGuard &&) = default;
  ZuGuard &operator =(ZuGuard &&) = default;
  L fn;
};
#endif

#if defined(linux) || defined(__mips64)
#include <endian.h>
#if __BYTE_ORDER == __BIG_ENDIAN
#define Zu_BIGENDIAN 1
#else
#define Zu_BIGENDIAN 0
#endif
#else
#ifdef _WIN32
#define Zu_BIGENDIAN 0
#endif
#endif

// safe bool idiom
#define ZuOpBool \
  operator const void *() const { \
    return !*this ? \
      reinterpret_cast<const void *>(0) : \
      static_cast<const void *>(this); \
  }

// move/copy universal reference
// ZuMvCp<U>::mvcp(ZuFwd<U>(u), [](auto &&v) { }, [](const auto &v) { });
template <typename T_> struct ZuMvCp {
  using T = ZuDecay<T_>;

  template <typename Mv, typename Cp>
  static auto mvcp(const T &v, Mv, Cp cp_) { return cp_(v); }
  template <typename Mv, typename Cp>
  static auto mvcp(T &&v, Mv mv_, Cp) { return mv_(ZuMv(v)); }

  template <typename Mv>
  static void mv(const T &v, Mv); // undefined
  template <typename Mv>
  static auto mv(T &&v, Mv mv_) { return mv_(ZuMv(v)); }

  template <typename Cp>
  static auto cp(const T &v, Cp cp_) { return cp_(v); }
  template <typename Cp>
  static void cp(T &&v, Cp); // undefined
};

// compile-time ?:
// ZuIf<bool B, typename T1, typename T2> evaluates to B ? T1 : T2
template <typename T1, typename T2, bool B> struct ZuIf_;
template <typename T1, typename T2> struct ZuIf_<T1, T2, true> {
  using T = T1;
};
template <typename T1, typename T2> struct ZuIf_<T1, T2, false> {
  using T = T2;
};
template <bool B, typename T1, typename T2>
using ZuIf = typename ZuIf_<T1, T2, B>::T;

// compile-time SFINAE (substitution failure is not an error)
// ZuIfT<bool B, typename T = void> evaluates to T (default void)
// if B is true, or is a substitution failure if B is false
template <bool, typename U = void> struct ZuIfT_ { };
template <typename U> struct ZuIfT_<true, U> { using T = U; };
template <bool B, typename U = void>
using ZuIfT = typename ZuIfT_<B, U>::T;

// type list
template <typename ...Args> struct ZuTypeList {
  enum { N = sizeof...(Args) };
  template <typename ...Args_> struct Prepend_ {
    using T = ZuTypeList<Args_..., Args...>;
  };
  template <typename ...Args_> struct Prepend_<ZuTypeList<Args_...>> {
    using T = ZuTypeList<Args_..., Args...>;
  };
  template <typename ...Args_>
  using Prepend = typename Prepend_<Args_...>::T;

  template <typename ...Args_> struct Append_ {
    using T = ZuTypeList<Args..., Args_...>;
  };
  template <typename ...Args_> struct Append_<ZuTypeList<Args_...>> {
    using T = ZuTypeList<Args..., Args_...>;
  };
  template <typename ...Args_>
  using Append = typename Append_<Args_...>::T;
};

// index -> type
template <unsigned, typename ...> struct ZuType_;
template <unsigned I, typename T0, typename ...Args>
struct ZuType__ {
  using T = typename ZuType_<I - 1, Args...>::T;
};
template <typename T0, typename ...Args>
struct ZuType__<0, T0, Args...> {
  using T = T0;
};
template <unsigned I, typename ...Args>
struct ZuType_ : public ZuType__<I, Args...> { };
template <unsigned I, typename ...Args>
struct ZuType_<I, ZuTypeList<Args...>> : public ZuType_<I, Args...> { };
template <unsigned I, typename ...Args>
using ZuType = typename ZuType_<I, Args...>::T;

// type -> index
template <typename, typename ...> struct ZuTypeIndex;
template <typename T, typename ...Args>
struct ZuTypeIndex<T, T, Args...> {
  enum { I = 0 };
};
template <typename T, typename O, typename ...Args>
struct ZuTypeIndex<T, O, Args...> {
  enum { I = 1 + ZuTypeIndex<T, Args...>::I };
};
template <typename T, typename ...Args>
struct ZuTypeIndex<T, ZuTypeList<Args...>> :
  public ZuTypeIndex<T, Args...> { };

// map
template <template <typename> class, typename ...> struct ZuTypeMap_;
template <template <typename> class Map, typename T0>
struct ZuTypeMap_<Map, T0> {
  using T = ZuTypeList<Map<T0>>;
};
template <template <typename> class Map, typename T0, typename ...Args>
struct ZuTypeMap_<Map, T0, Args...> {
  using T =
    typename ZuTypeList<Map<T0>>::template Append<
      typename ZuTypeMap_<Map, Args...>::T>;
};
template <template <typename> class Map, typename ...Args>
struct ZuTypeMap_<Map, ZuTypeList<Args...>> :
  public ZuTypeMap_<Map, Args...> { };
template <template <typename> class Map, typename ...Args>
using ZuTypeMap = typename ZuTypeMap_<Map, Args...>::T;

// grep
template <typename T0, int> struct ZuTypeGrep__ {
  using T = ZuTypeList<T0>;
};
template <typename T0> struct ZuTypeGrep__<T0, 0> {
  using T = ZuTypeList<>;
};
template <template <typename> class, typename ...>
struct ZuTypeGrep_ {
  using T = ZuTypeList<>;
};
template <template <typename> class Filter, typename T0>
struct ZuTypeGrep_<Filter, T0> {
  using T = typename ZuTypeGrep__<T0, Filter<T0>::OK>::T;
};
template <template <typename> class Filter, typename T0, typename ...Args>
struct ZuTypeGrep_<Filter, T0, Args...> {
  using T =
    typename ZuTypeGrep__<T0, Filter<T0>::OK>::T::template Append<
      typename ZuTypeGrep_<Filter, Args...>::T>;
};
template <template <typename> class Filter, typename ...Args>
struct ZuTypeGrep_<Filter, ZuTypeList<Args...>> :
  public ZuTypeGrep_<Filter, Args...> { };
template <template <typename> class Filter, typename ...Args>
using ZuTypeGrep = typename ZuTypeGrep_<Filter, Args...>::T;

// reduce (recursive pair-wise reduction)
template <template <typename...> class, typename ...>
struct ZuTypeReduce_;
template <template <typename...> class Reduce, typename T0>
struct ZuTypeReduce_<Reduce, T0> {
  using T = typename Reduce<T0>::T;
};
template <template <typename...> class Reduce, typename T0, typename T1>
struct ZuTypeReduce_<Reduce, T0, T1> {
  using T = typename Reduce<T0, T1>::T;
};
template <
  template <typename...> class Reduce,
  typename T0, typename T1, typename ...Args>
struct ZuTypeReduce_<Reduce, T0, T1, Args...> {
  using T =
    typename Reduce<T0, typename ZuTypeReduce_<Reduce, T1, Args...>::T>::T;
};
template <template <typename...> class Reduce, typename ...Args>
struct ZuTypeReduce_<Reduce, ZuTypeList<Args...>> :
    public ZuTypeReduce_<Reduce, Args...> { };
template <template <typename...> class Reduce, typename ...Args>
using ZuTypeReduce = typename ZuTypeReduce_<Reduce, Args...>::T;

// split typelist left, 0..N-1
template <unsigned N, typename ...Args> struct ZuTypeLeft_;
template <unsigned N, typename Arg0, typename ...Args>
struct ZuTypeLeft_<N, Arg0, Args...> {
  using T = typename ZuTypeLeft_<N - 1, Args...>::T::template Prepend<Arg0>;
};
template <typename Arg0, typename ...Args>
struct ZuTypeLeft_<0, Arg0, Args...> {
  using T = ZuTypeList<>;
};
template <typename ...Args>
struct ZuTypeLeft_<0, Args...> {
  using T = ZuTypeList<>;
};
template <unsigned N, typename ...Args>
struct ZuTypeLeft_<N, ZuTypeList<Args...>> :
    public ZuTypeLeft_<N, Args...> { };
template <unsigned N, typename ...Args>
using ZuTypeLeft = typename ZuTypeLeft_<N, Args...>::T;

// split typelist right, N..
template <unsigned N, typename ...Args> struct ZuTypeRight_;
template <unsigned N, typename Arg0, typename ...Args>
struct ZuTypeRight_<N, Arg0, Args...> {
  using T = typename ZuTypeRight_<N - 1, Args...>::T;
};
template <typename Arg0, typename ...Args>
struct ZuTypeRight_<0, Arg0, Args...> {
  using T = ZuTypeList<Arg0, Args...>;
};
template <typename ...Args>
struct ZuTypeRight_<0, Args...> {
  using T = ZuTypeList<Args...>;
};
template <unsigned N, typename ...Args>
struct ZuTypeRight_<N, ZuTypeList<Args...>> :
    public ZuTypeRight_<N, Args...> { };
template <unsigned N, typename ...Args>
using ZuTypeRight = typename ZuTypeRight_<N, Args...>::T;

// compile-time merge sort typelist using Index<T>::I
template <template <typename> class Index, typename Left, typename Right>
struct ZuTypeMerge_;
template <template <typename> class, typename, typename, bool>
struct ZuTypeMerge__;
template <
  template <typename> class Index,
  typename LeftArg0, typename ...LeftArgs,
  typename RightArg0, typename ...RightArgs>
struct ZuTypeMerge__<Index,
    ZuTypeList<LeftArg0, LeftArgs...>,
    ZuTypeList<RightArg0, RightArgs...>, false> {
  using T = typename ZuTypeMerge_<Index,
    ZuTypeList<LeftArgs...>,
    ZuTypeList<RightArg0, RightArgs...>>::T::template Prepend<LeftArg0>;
};
template <
  template <typename> class Index,
  typename LeftArg0, typename ...LeftArgs,
  typename RightArg0, typename ...RightArgs>
struct ZuTypeMerge__<Index,
    ZuTypeList<LeftArg0, LeftArgs...>,
    ZuTypeList<RightArg0, RightArgs...>, true> {
  using T = typename ZuTypeMerge_<Index,
    ZuTypeList<LeftArg0, LeftArgs...>,
    ZuTypeList<RightArgs...>>::T::template Prepend<RightArg0>;
};
template <
  template <typename> class Index,
  typename LeftArg0, typename ...LeftArgs,
  typename RightArg0, typename ...RightArgs>
struct ZuTypeMerge_<Index,
  ZuTypeList<LeftArg0, LeftArgs...>,
  ZuTypeList<RightArg0, RightArgs...>> :
    public ZuTypeMerge__<Index, 
      ZuTypeList<LeftArg0, LeftArgs...>,
      ZuTypeList<RightArg0, RightArgs...>,
      (Index<LeftArg0>::I > Index<RightArg0>::I)> { };
template <template <typename> class Index, typename ...Args>
struct ZuTypeMerge_<Index, ZuTypeList<>, ZuTypeList<Args...>> {
  using T = ZuTypeList<Args...>;
};
template <template <typename> class Index, typename ...Args>
struct ZuTypeMerge_<Index, ZuTypeList<Args...>, ZuTypeList<>> {
  using T = ZuTypeList<Args...>;
};
template <template <typename> class Index>
struct ZuTypeMerge_<Index, ZuTypeList<>, ZuTypeList<>> {
  using T = ZuTypeList<>;
};
template <template <typename> class Index, typename Left, typename Right>
using ZuTypeMerge = typename ZuTypeMerge_<Index, Left, Right>::T;
template <template <typename> class Index, typename ...Args>
struct ZuTypeSort_ {
  enum { N = sizeof...(Args) };
  using T = ZuTypeMerge<Index,
    typename ZuTypeSort_<Index, ZuTypeLeft<(N>>1), Args...>>::T,
    typename ZuTypeSort_<Index, ZuTypeRight<(N>>1), Args...>>::T
  >;
};
template <template <typename> class Index, typename Arg0>
struct ZuTypeSort_<Index, Arg0> {
  using T = ZuTypeList<Arg0>;
};
template <template <typename> class Index>
struct ZuTypeSort_<Index> {
  using T = ZuTypeList<>;
};
template <template <typename> class Index, typename ...Args>
struct ZuTypeSort_<Index, ZuTypeList<Args...>> :
    public ZuTypeSort_<Index, Args...> { };
template <template <typename> class Index, typename ...Args>
using ZuTypeSort = typename ZuTypeSort_<Index, Args...>::T;

// apply typelist to template
template <template <typename...> class Type, typename ...Args>
struct ZuTypeApply_ { using T = Type<Args...>; };
template <template <typename...> class Type, typename ...Args>
struct ZuTypeApply_<Type, ZuTypeList<Args...>> :
  public ZuTypeApply_<Type, Args...> { };
template <template <typename...> class Type, typename ...Args>
using ZuTypeApply = typename ZuTypeApply_<Type, Args...>::T;

// invoke lambda on all elements of typelist []<typename T>() { ...; }
template <typename ...>
struct ZuTypeAll {
  template <typename L> static ZuInline void invoke(L) { }
  template <typename L> static ZuInline void invoke_(L &) { }
};
template <typename T0>
struct ZuTypeAll<T0> {
  template <typename L> static ZuInline void invoke(L l) { invoke_(l); }
  template <typename L> static ZuInline void invoke_(L &l) {
    l.template operator ()<T0>();
  }
};
template <typename T0, typename ...Args>
struct ZuTypeAll<T0, Args...> {
  template <typename L> static ZuInline void invoke(L l) { invoke_(l); }
  template <typename L> static ZuInline void invoke_(L &l) {
    l.template operator ()<T0>();
    ZuTypeAll<Args...>::invoke_(l);
  }
};
template <typename ...Args>
struct ZuTypeAll<ZuTypeList<Args...>> : public ZuTypeAll<Args...> { };

// default accessor (pass-through value)
inline constexpr auto ZuDefaultAxor() {
  return [] <typename T> (T &&v) -> decltype(auto) { return ZuFwd<T>(v); };
}

// function signature deduction

// ZuDeduce<decltype(&L::operator())>::R
// ZuDeduce<decltype(&fn)>::R

template <typename> struct ZuDeduce;
template <typename O_, typename R_, typename ...Args_>
struct ZuDeduce<R_ (O_::*)(Args_...) const> {
  enum { Member = 1 };
  using O = O_;
  using R = R_;
  using Args = ZuTypeList<Args_...>;
};
template <typename O_, typename R_, typename ...Args_>
struct ZuDeduce<R_ (O_::*)(Args_...)> {
  enum { Member = 1 };
  using O = O_;
  using R = R_;
  using Args = ZuTypeList<Args_...>;
};
template <typename R_, typename ...Args_>
struct ZuDeduce<R_ (*)(Args_...)> {
  enum { Member = 0 };
  using R = R_;
  using Args = ZuTypeList<Args_...>;
};

// constexpr instantiable numeric constant
template <unsigned I_> struct ZuConstant {
  enum { I = I_ };
  constexpr operator unsigned() const noexcept { return I; }
  constexpr unsigned operator()() const noexcept { return I; }
};

// shorthand for std::declval
template <typename U> struct ZuDeclVal__ { using T = U; };
template <typename T> auto ZuDeclVal_(int) -> typename ZuDeclVal__<T&&>::T;
template <typename T> auto ZuDeclVal_(...) -> typename ZuDeclVal__<T>::T;
template <typename U> decltype(ZuDeclVal_<U>(0)) ZuDeclVal();
 
// shorthand for std::void_t
template <typename ...> struct ZuVoid_ { using T = void; };
template <typename ...Args> using ZuVoid = typename ZuVoid_<Args...>::T;

// cv checking
template <typename U, typename R = void> struct ZuIsConst_;
template <typename U, typename R>
struct ZuIsConst_<const U, R> { using T = R; };
template <typename U, typename R = void>
using ZuIsConst = typename ZuIsConst_<U, R>::T;
template <typename U, typename R = void>
struct ZuNotConst_ { using T = R; };
template <typename U, typename R> struct ZuNotConst_<const U, R> { };
template <typename U, typename R = void>
using ZuNotConst = typename ZuNotConst_<U, R>::T;

template <typename U, typename R = void> struct ZuIsVolatile_;
template <typename U, typename R>
struct ZuIsVolatile_<volatile U, R> { using T = R; };
template <typename U, typename R = void>
using ZuIsVolatile = typename ZuIsVolatile_<U, R>::T;
template <typename U, typename R = void>
struct ZuNotVolatile_ { using T = R; };
template <typename U, typename R> struct ZuNotVolatile_<volatile U, R> { };
template <typename U, typename R = void>
using ZuNotVolatile = typename ZuNotVolatile_<U, R>::T;

// alloca() alias

#ifdef _MSC_VER
#define ZuAlloca(n) _alloca(n)
#else
#ifndef _WIN32
#include <alloca.h>
#endif
#define ZuAlloca(n) alloca(n)
#endif

#endif /* ZuLib_HPP */
