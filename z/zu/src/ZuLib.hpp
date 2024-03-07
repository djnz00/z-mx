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
#define ZuInline inline
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
template <typename T> using ZuMkConst = const T;
template <typename T> using ZuMkVolatile = volatile T;
template <typename T> using ZuMkRRef = T &&;
template <typename T> using ZuMkLRef = T &;
template <typename T> using ZuMkCRef = const T &;

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
  L	fn;
  bool	cancelled = false;

  ZuGuard(L fn_) : fn{ZuMv(fn_)} { }
  ~ZuGuard() { if (!cancelled) fn(); }
  ZuGuard(const ZuGuard &) = delete;
  ZuGuard &operator =(const ZuGuard &) = delete;
  ZuGuard(ZuGuard &&o) : fn{ZuMv(o.fn)} { o.cancelled = true; }
  ZuGuard &operator =(ZuGuard &&o) {
    if (this != &o) { this->~ZuGuard(); new (this) ZuGuard{ZuMv(o)}; }
    return *this;
  }

  void cancel() { cancelled = true; }
  void cancel(bool v) { cancelled = v; }
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

// generic binding of universal reference parameter for move/copy
// ZuBind<U>::mvcp(ZuFwd<U>(u), [](auto &&v) { }, [](const auto &v) { });
template <typename T_> struct ZuBind {
  using T = ZuDecay<T_>;

  template <typename Mv, typename Cp>
  constexpr static auto mvcp(const T &v, Mv, Cp cp_) { return cp_(v); }
  template <typename Mv, typename Cp>
  constexpr static auto mvcp(T &&v, Mv mv_, Cp) { return mv_(ZuMv(v)); }

  // undefined - ensures that parameter is movable at compile time
  template <typename Mv>
  static void mv(const T &v, Mv); // undefined

  template <typename Mv>
  constexpr static auto mv(T &&v, Mv mv_) { return mv_(ZuMv(v)); }

  template <typename Cp>
  constexpr static auto cp(const T &v, Cp cp_) { return cp_(v); }

  // undefined - ensures that parameter is not movable at compile time
  template <typename Cp>
  static void cp(T &&v, Cp); // undefined
};

// compile-time ?:
// ZuIf<typename B, typename T1, typename T2> evaluates to B ? T1 : T2
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

// constexpr instantiable constants
template <typename T_, T_ V> struct ZuConstant {
  using T = T_;
  constexpr operator T() const noexcept { return V; }
  constexpr T operator()() const noexcept { return V; }
};
template <int I> using ZuInt = ZuConstant<int, I>;
template <unsigned I> using ZuUnsigned = ZuConstant<unsigned, I>;
template <auto B> using ZuBool = ZuConstant<bool, static_cast<bool>(B)>;
using ZuFalse = ZuBool<false>;	// interoperable with std::false_type
using ZuTrue = ZuBool<true>;	// interoperable with std::true_type

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
struct ZuTypeIndex<T, T, Args...> : public ZuUnsigned<0> { };
template <typename T, typename O, typename ...Args>
struct ZuTypeIndex<T, O, Args...> :
  public ZuUnsigned<1 + ZuTypeIndex<T, Args...>{}> { };
template <typename T, typename ...Args>
struct ZuTypeIndex<T, ZuTypeList<Args...>> :
  public ZuTypeIndex<T, Args...> { };

// map
// - maps T to Map<T> for each T in the list
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
// - Filter<T>{} should be true to include T in resulting list
template <typename T0, bool> struct ZuTypeGrep__ {
  using T = ZuTypeList<T0>;
};
template <typename T0> struct ZuTypeGrep__<T0, false> {
  using T = ZuTypeList<>;
};
template <template <typename> class, typename ...>
struct ZuTypeGrep_ {
  using T = ZuTypeList<>;
};
template <template <typename> class Filter, typename T0>
struct ZuTypeGrep_<Filter, T0> {
  using T = typename ZuTypeGrep__<T0, Filter<T0>{}>::T;
};
template <template <typename> class Filter, typename T0, typename ...Args>
struct ZuTypeGrep_<Filter, T0, Args...> {
  using T =
    typename ZuTypeGrep__<T0, Filter<T0>{}>::T::template Append<
      typename ZuTypeGrep_<Filter, Args...>::T>;
};
template <template <typename> class Filter, typename ...Args>
struct ZuTypeGrep_<Filter, ZuTypeList<Args...>> :
  public ZuTypeGrep_<Filter, Args...> { };
template <template <typename> class Filter, typename ...Args>
using ZuTypeGrep = typename ZuTypeGrep_<Filter, Args...>::T;

// test for inclusion in list
// ZuTypeIn<T, List>{} evaluates to true if T is in List
template <typename, typename ...>
struct ZuTypeIn : public ZuFalse { };
template <typename U>
struct ZuTypeIn<U, U> : public ZuTrue { };
template <typename U, typename ...Args>
struct ZuTypeIn<U, U, Args...> : public ZuTrue { };
template <typename U, typename T0, typename ...Args>
struct ZuTypeIn<U, T0, Args...> : public ZuTypeIn<U, Args...> { };
template <typename U, typename ...Args>
struct ZuTypeIn<U, ZuTypeList<Args...>> : public ZuTypeIn<U, Args...> { };

// reduce (recursive pair-wise reduction)
// - T0 will be reduced to Reduce<T0>
// - T0, T1 will be reduced to Reduce<T0, T1>
template <template <typename...> class, typename ...>
struct ZuTypeReduce_;
template <template <typename...> class Reduce>
struct ZuTypeReduce_<Reduce> {
  using T = Reduce<>;
};
template <template <typename...> class Reduce, typename T0>
struct ZuTypeReduce_<Reduce, T0> {
  using T = Reduce<T0>;
};
template <template <typename...> class Reduce, typename T0, typename T1>
struct ZuTypeReduce_<Reduce, T0, T1> {
  using T = Reduce<T0, T1>;
};
template <
  template <typename...> class Reduce,
  typename T0, typename T1, typename ...Args>
struct ZuTypeReduce_<Reduce, T0, T1, Args...> {
  using T = Reduce<T0, typename ZuTypeReduce_<Reduce, T1, Args...>::T>;
};
template <template <typename...> class Reduce, typename ...Args>
struct ZuTypeReduce_<Reduce, ZuTypeList<Args...>> :
    public ZuTypeReduce_<Reduce, Args...> { };
template <template <typename...> class Reduce, typename ...Args>
using ZuTypeReduce = typename ZuTypeReduce_<Reduce, Args...>::T;

// split typelist left, 0..N-1
template <unsigned N, typename ...Args> struct ZuTypeLeft__;
template <unsigned N, typename Arg0, typename ...Args>
struct ZuTypeLeft__<N, Arg0, Args...> {
  using T = typename ZuTypeLeft__<N - 1, Args...>::T::template Prepend<Arg0>;
};
template <typename Arg0, typename ...Args>
struct ZuTypeLeft__<0, Arg0, Args...> {
  using T = ZuTypeList<>;
};
template <typename ...Args>
struct ZuTypeLeft__<0, Args...> {
  using T = ZuTypeList<>;
};
template <unsigned N, typename ...Args>
struct ZuTypeLeft_ :
    public ZuTypeLeft__<N, Args...> { };
template <unsigned N, typename ...Args>
struct ZuTypeLeft_<N, ZuTypeList<Args...>> :
    public ZuTypeLeft__<N, Args...> { };
template <unsigned N, typename ...Args>
using ZuTypeLeft = typename ZuTypeLeft_<N, Args...>::T;

// split typelist right, N..
template <unsigned N, typename ...Args> struct ZuTypeRight__;
template <unsigned N, typename Arg0, typename ...Args>
struct ZuTypeRight__<N, Arg0, Args...> {
  using T = typename ZuTypeRight__<N - 1, Args...>::T;
};
template <typename Arg0, typename ...Args>
struct ZuTypeRight__<0, Arg0, Args...> {
  using T = ZuTypeList<Arg0, Args...>;
};
template <typename ...Args>
struct ZuTypeRight__<0, Args...> {
  using T = ZuTypeList<Args...>;
};
template <unsigned N, typename ...Args>
struct ZuTypeRight_ :
    public ZuTypeRight__<N, Args...> { };
template <unsigned N, typename ...Args>
struct ZuTypeRight_<N, ZuTypeList<Args...>> :
    public ZuTypeRight__<N, Args...> { };
template <unsigned N, typename ...Args>
using ZuTypeRight = typename ZuTypeRight_<N, Args...>::T;

// compile-time merge sort typelist using Index<T>{}
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
      (Index<LeftArg0>{} > Index<RightArg0>{})> { };
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

// compile-time numerical sequence
template <unsigned ...> struct ZuSeq { };
template <typename> struct ZuUnshiftSeq_;
template <unsigned ...I>
struct ZuUnshiftSeq_<ZuSeq<I...>> {
  using T = ZuSeq<0, (I + 1)...>;
};
template <unsigned> struct ZuMkSeq_;
template <> struct ZuMkSeq_<0> { using T = ZuSeq<>; };
template <> struct ZuMkSeq_<1> { using T = ZuSeq<0>; };
template <unsigned N> struct ZuMkSeq_ {
  using T = typename ZuUnshiftSeq_<typename ZuMkSeq_<N - 1>::T>::T;
};
template <unsigned N> using ZuMkSeq = typename ZuMkSeq_<N>::T;

// default accessor (pass-through)
inline constexpr auto ZuDefaultAxor() {
  return []<typename T>(T &&v) -> decltype(auto) { return ZuFwd<T>(v); };
}

// ZuSeqCall<Axor, N>(value, lambda)
// invokes lambda(Axor.operator ()<I>(value), ...) for I in [0,N)
template <auto Axor, typename Seq, typename T> struct ZuSeqCall_;
template <auto Axor, unsigned ...I, typename T>
struct ZuSeqCall_<Axor, ZuSeq<I...>, T> {
  template <typename L>
  static decltype(auto) fn(const T &v, L l) {
    return l(Axor.template operator ()<I>(v)...);
  }
  template <typename L>
  static decltype(auto) fn(T &v, L l) {
    return l(Axor.template operator()<I>(v)...);
  }
  template <typename L>
  static decltype(auto) fn(T &&v, L l) {
    return l(Axor.template operator()<I>(ZuMv(v))...);
  }
};
template <unsigned N, auto Axor = ZuDefaultAxor(), typename T, typename L>
inline decltype(auto) ZuSeqCall(T &&v, L l) {
  return ZuSeqCall_<Axor, ZuMkSeq<N>, ZuDecay<T>>::fn(ZuFwd<T>(v), ZuMv(l));
}

// ZuSeqIter<Fn, N>(value, lambda)
// invokes lambda(Fn.operator ()<I>(value)) for I in [0,N)
template <auto Axor, typename Seq, typename T> struct ZuSeqIter_;
template <auto Axor, unsigned I, unsigned ...J, typename T>
struct ZuSeqIter_<Axor, ZuSeq<I, J...>, T> {
  template <typename L>
  static void fn(const T &v, L l) {
    l.template operator ()<I>(Axor.template operator ()<I>(v));
    return ZuSeqIter_<Axor, ZuSeq<J...>, T>::fn(v, ZuMv(l));
  }
  template <typename L>
  static void fn(T &v, L l) {
    l.template operator ()<I>(Axor.template operator()<I>(v));
    return ZuSeqIter_<Axor, ZuSeq<J...>, T>::fn(v, ZuMv(l));
  }
  template <typename L>
  static void fn(T &&v, L l) {
    l.template operator ()<I>(Axor.template operator()<I>(ZuMv(v)));
    return ZuSeqIter_<Axor, ZuSeq<J...>, T>::fn(ZuMv(v), ZuMv(l));
  }
};
template <auto Axor, unsigned I, typename T>
struct ZuSeqIter_<Axor, ZuSeq<I>, T> {
  template <typename L>
  static void fn(const T &v, L l) {
    l.template operator ()<I>(Axor.template operator ()<I>(v));
  }
  template <typename L>
  static void fn(T &v, L l) {
    l.template operator ()<I>(Axor.template operator()<I>(v));
  }
  template <typename L>
  static void fn(T &&v, L l) {
    l.template operator ()<I>(Axor.template operator()<I>(ZuMv(v)));
  }
};
template <auto Axor, typename T>
struct ZuSeqIter_<Axor, ZuSeq<>, T> {
  template <typename L> static void fn(const T &, L) { }
  template <typename L> static void fn(T &, L) { }
  template <typename L> static void fn(T &&, L) { }
};
template <unsigned N, auto Axor = ZuDefaultAxor(), typename T, typename L>
inline decltype(auto) ZuSeqIter(T &&v, L l) {
  return ZuSeqIter_<Axor, ZuMkSeq<N>, ZuDecay<T>>::fn(ZuFwd<T>(v), ZuMv(l));
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

// alternative for std::declval
template <typename U> struct ZuDeclVal__ { using T = U; };
template <typename T> auto ZuDeclVal_(int) -> typename ZuDeclVal__<T&&>::T;
template <typename T> auto ZuDeclVal_(...) -> typename ZuDeclVal__<T>::T;
template <typename U> decltype(ZuDeclVal_<U>(0)) ZuDeclVal();
 
// alternative for std::void_t
template <typename ...> struct ZuVoid_ { using T = void; };
template <typename ...Args> using ZuVoid = typename ZuVoid_<Args...>::T;

// sizeof(void) handling
template <typename T> struct ZuSize : public ZuUnsigned<sizeof(T)> { };
template <> struct ZuSize<void> : public ZuUnsigned<0> { };

// cv checking
template <typename U> struct ZuIsConst : public ZuFalse { };
template <typename U> struct ZuIsConst<const U> : public ZuTrue { };

template <typename U> struct ZuIsVolatile : public ZuFalse { };
template <typename U> struct ZuIsVolatile<volatile U> : public ZuTrue { };

template <typename U, typename R, bool = ZuIsConst<U>{}>
struct ZuConst_;
template <typename U, typename R>
struct ZuConst_<U, R, true> { using T = R; };
template <typename U, typename R = void>
using ZuConst = typename ZuConst_<U, R>::T;

template <typename U, typename R, bool = !ZuIsConst<U>{}>
struct ZuMutable_;
template <typename U, typename R>
struct ZuMutable_<U, R, true> { using T = R; };
template <typename U, typename R = void>
using ZuMutable = typename ZuMutable_<U, R>::T;

template <typename U, typename R, bool = ZuIsVolatile<U>{}>
struct ZuVolatile_;
template <typename U, typename R>
struct ZuVolatile_<U, R, true> { using T = R; };
template <typename U, typename R = void>
using ZuVolatile = typename ZuVolatile_<U, R>::T;

template <typename U, typename R, bool = !ZuIsVolatile<U>{}>
struct ZuNonVolatile_;
template <typename U, typename R>
struct ZuNonVolatile_<U, R, true> { using T = R; };
template <typename U, typename R = void>
using ZuNonVolatile = typename ZuNonVolatile_<U, R>::T;

// ref checking
template <typename U> struct ZuIsRRef : public ZuFalse { };
template <typename U> struct ZuIsRRef<U &&> : public ZuTrue { };

template <typename U> struct ZuIsLRef : public ZuFalse { };
template <typename U> struct ZuIsLRef<U &> : public ZuTrue { };

template <typename U, typename R, bool = ZuIsRRef<U>{}>
struct ZuRRef_;
template <typename U, typename R>
struct ZuRRef_<U, R, true> { using T = R; };
template <typename U, typename R = void>
using ZuRRef = typename ZuRRef_<U, R>::T;

template <typename U, typename R, bool = !ZuIsRRef<U>{}>
struct ZuNotRRef_;
template <typename U, typename R>
struct ZuNotRRef_<U, R, true> { using T = R; };
template <typename U, typename R = void>
using ZuNotRRef = typename ZuNotRRef_<U, R>::T;

template <typename U, typename R, bool = ZuIsLRef<U>{}>
struct ZuLRef_;
template <typename U, typename R>
struct ZuLRef_<U, R, true> { using T = R; };
template <typename U, typename R = void>
using ZuLRef = typename ZuLRef_<U, R>::T;

template <typename U, typename R, bool = !ZuIsLRef<U>{}>
struct ZuNotLRef_;
template <typename U, typename R>
struct ZuNotLRef_<U, R, true> { using T = R; };
template <typename U, typename R = void>
using ZuNotLRef = typename ZuNotLRef_<U, R>::T;

// exact type matching
template <typename U1, typename U2>
struct ZuIsExact : public ZuFalse { };
template <typename U>
struct ZuIsExact<U, U> : public ZuTrue { };

template <typename U1, typename U2, typename R>
struct ZuExact_;
template <typename U, typename R>
struct ZuExact_<U, U, R> { using T = R; };
template <typename U1, typename U2, typename R = void>
using ZuExact = typename ZuExact_<U1, U2, R>::T;

template <typename U1, typename U2, typename R, bool = !ZuIsExact<U1, U2>{}>
struct ZuNotExact_;
template <typename U1, typename U2, typename R>
struct ZuNotExact_<U1, U2, R, true> { using T = R; };
template <typename U1, typename U2, typename R = void>
using ZuNotExact = typename ZuNotExact_<U1, U2, R>::T;

// generic invocation
// ZuInvoke<Fn>(this, args...) invokes one of:
//   (this->*Fn)(args...)	// member function
//   Fn(this, args...)		// bound function (passing this explicitly)
//   Fn(args)			// unbound function (discarding this)

template <auto Fn, typename O, typename Args, typename = void>
struct ZuInvoke_MemberFn_;
template <auto Fn, typename O, typename ...Args>
struct ZuInvoke_MemberFn_<
    Fn, O, ZuTypeList<Args...>,
    decltype((ZuDeclVal<O *>()->*Fn)(ZuDeclVal<Args>()...), void())> {
  using T = decltype((ZuDeclVal<O *>()->*Fn)(ZuDeclVal<Args>()...));
};
template <auto Fn, typename O, typename ...Args>
using ZuInvoke_MemberFn =
  typename ZuInvoke_MemberFn_<Fn, O, ZuTypeList<Args...>>::T;
template <auto Fn, typename O, typename ...Args>
auto ZuInvoke(O *ptr, Args &&... args) -> ZuInvoke_MemberFn<Fn, O, Args...> {
  return (ptr->*Fn)(ZuFwd<Args>(args)...);
}

template <auto Fn, typename O, typename Args, typename = void>
struct ZuInvoke_BoundFn_;
template <auto Fn, typename O, typename ...Args>
struct ZuInvoke_BoundFn_<
    Fn, O, ZuTypeList<Args...>,
    decltype(Fn(ZuDeclVal<O *>(), ZuDeclVal<Args>()...), void())> {
  using T = decltype(Fn(ZuDeclVal<O *>(), ZuDeclVal<Args>()...));
};
template <auto Fn, typename O, typename ...Args>
using ZuInvoke_BoundFn =
  typename ZuInvoke_BoundFn_<Fn, O, ZuTypeList<Args...>>::T;
template <auto Fn, typename O, typename ...Args>
auto ZuInvoke(O *ptr, Args &&... args) -> ZuInvoke_BoundFn<Fn, O, Args...> {
  return Fn(ptr, ZuFwd<Args>(args)...);
}

template <auto Fn, typename O, typename Args, typename = void>
struct ZuInvoke_UnboundFn_;
template <auto Fn, typename O, typename ...Args>
struct ZuInvoke_UnboundFn_<
    Fn, O, ZuTypeList<Args...>,
    decltype(Fn(ZuDeclVal<Args>()...), void())> {
  using T = decltype(Fn(ZuDeclVal<Args>()...));
};
template <auto Fn, typename O, typename ...Args>
using ZuInvoke_UnboundFn =
  typename ZuInvoke_UnboundFn_<Fn, O, ZuTypeList<Args...>>::T;
template <auto Fn, typename O, typename ...Args>
auto ZuInvoke(O *, Args &&... args) -> ZuInvoke_UnboundFn<Fn, O, Args...> {
  return Fn(ZuFwd<Args>(args)...);
}

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
