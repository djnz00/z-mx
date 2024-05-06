//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// Z library main header

#ifndef ZuLib_HH
#define ZuLib_HH

#ifdef _MSC_VER
#pragma once
#endif

#include <assert.h>

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

// sanity check platform
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

// safe bool idiom, given operator !()
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

// typelist
template <typename ...Ts> struct ZuTypeList {
  enum { N = sizeof...(Ts) };
  template <typename ...Ts_> struct Unshift_ {
    using T = ZuTypeList<Ts_..., Ts...>;
  };
  template <typename ...Ts_> struct Unshift_<ZuTypeList<Ts_...>> {
    using T = ZuTypeList<Ts_..., Ts...>;
  };
  template <typename ...Ts_>
  using Unshift = typename Unshift_<Ts_...>::T;

  template <typename ...Ts_> struct Push_ {
    using T = ZuTypeList<Ts..., Ts_...>;
  };
  template <typename ...Ts_> struct Push_<ZuTypeList<Ts_...>> {
    using T = ZuTypeList<Ts..., Ts_...>;
  };
  template <typename ...Ts_>
  using Push = typename Push_<Ts_...>::T;
};

// typelist of repeated type
template <unsigned N, typename List, typename E>
struct ZuTypeRepeat__ {
  using T =
    typename ZuTypeRepeat__<N - 1, typename List::template Push<E>, E>::T;
};
template <typename List, typename E>
struct ZuTypeRepeat__<0, List, E> {
  using T = List;
};
template <unsigned N, typename E>
struct ZuTypeRepeat_ : public ZuTypeRepeat__<N, ZuTypeList<>, E> { };
template <unsigned N, typename E>
using ZuTypeRepeat = typename ZuTypeRepeat_<N, E>::T;

// reverse typelist
template <typename ...Ts> struct ZuTypeRev_;
template <typename ...Ts> struct ZuTypeRev__;
template <typename T0, typename ...Ts>
struct ZuTypeRev__<T0, Ts...> {
  using T = typename ZuTypeRev__<Ts...>::T::template Push<T0>;
};
template <typename T0>
struct ZuTypeRev__<T0> { using T = ZuTypeList<T0>; };
template <>
struct ZuTypeRev__<> { using T = ZuTypeList<>; };
template <typename ...Ts>
struct ZuTypeRev_ { using T = typename ZuTypeRev__<Ts...>::T; };
template <typename ...Ts>
struct ZuTypeRev_<ZuTypeList<Ts...>> {
  using T = typename ZuTypeRev__<Ts...>::T;
};
template <typename ...Ts>
using ZuTypeRev = typename ZuTypeRev_<Ts...>::T;

// index -> type
template <unsigned, typename ...> struct ZuType_;
template <unsigned I, typename T0, typename ...Ts>
struct ZuType__ {
  using T = typename ZuType_<I - 1, Ts...>::T;
};
template <typename T0, typename ...Ts>
struct ZuType__<0, T0, Ts...> {
  using T = T0;
};
template <unsigned I, typename ...Ts>
struct ZuType_ : public ZuType__<I, Ts...> { };
template <unsigned I, typename ...Ts>
struct ZuType_<I, ZuTypeList<Ts...>> : public ZuType_<I, Ts...> { };
template <unsigned I, typename ...Ts>
using ZuType = typename ZuType_<I, Ts...>::T;

// type -> index (returns first match) (undefined if type is not in list)
template <typename, typename ...> struct ZuTypeIndex;
template <typename T, typename ...Ts>
struct ZuTypeIndex<T, T, Ts...> : public ZuUnsigned<0> { };
template <typename T, typename O, typename ...Ts>
struct ZuTypeIndex<T, O, Ts...> :
  public ZuUnsigned<1 + ZuTypeIndex<T, Ts...>{}> { };
template <typename T, typename ...Ts>
struct ZuTypeIndex<T, ZuTypeList<Ts...>> :
  public ZuTypeIndex<T, Ts...> { };

// typelist map
// - maps T to Map<T> for each T in the list
template <template <typename> class, typename ...> struct ZuTypeMap_;
template <template <typename> class Map>
struct ZuTypeMap_<Map> {
  using T = ZuTypeList<>;
};
template <template <typename> class Map, typename T0>
struct ZuTypeMap_<Map, T0> {
  using T = ZuTypeList<Map<T0>>;
};
template <template <typename> class Map, typename T0, typename ...Ts>
struct ZuTypeMap_<Map, T0, Ts...> {
  using T =
    typename ZuTypeList<Map<T0>>::template Push<
      typename ZuTypeMap_<Map, Ts...>::T>;
};
template <template <typename> class Map, typename ...Ts>
struct ZuTypeMap_<Map, ZuTypeList<Ts...>> :
  public ZuTypeMap_<Map, Ts...> { };
template <template <typename> class Map, typename ...Ts>
using ZuTypeMap = typename ZuTypeMap_<Map, Ts...>::T;

// typelist grep
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
template <template <typename> class Filter, typename T0, typename ...Ts>
struct ZuTypeGrep_<Filter, T0, Ts...> {
  using T =
    typename ZuTypeGrep__<T0, Filter<T0>{}>::T::template Push<
      typename ZuTypeGrep_<Filter, Ts...>::T>;
};
template <template <typename> class Filter, typename ...Ts>
struct ZuTypeGrep_<Filter, ZuTypeList<Ts...>> :
  public ZuTypeGrep_<Filter, Ts...> { };
template <template <typename> class Filter, typename ...Ts>
using ZuTypeGrep = typename ZuTypeGrep_<Filter, Ts...>::T;

// test for inclusion in typelist
// ZuTypeIn<T, List>{} evaluates to true if T is in List
template <typename, typename ...>
struct ZuTypeIn : public ZuFalse { };
template <typename U>
struct ZuTypeIn<U, U> : public ZuTrue { };
template <typename U, typename ...Ts>
struct ZuTypeIn<U, U, Ts...> : public ZuTrue { };
template <typename U, typename T0, typename ...Ts>
struct ZuTypeIn<U, T0, Ts...> : public ZuTypeIn<U, Ts...> { };
template <typename U, typename ...Ts>
struct ZuTypeIn<U, ZuTypeList<Ts...>> : public ZuTypeIn<U, Ts...> { };

// typelist reduce (recursive pair-wise reduction)
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
  typename T0, typename T1, typename ...Ts>
struct ZuTypeReduce_<Reduce, T0, T1, Ts...> {
  using T = Reduce<T0, typename ZuTypeReduce_<Reduce, T1, Ts...>::T>;
};
template <template <typename...> class Reduce, typename ...Ts>
struct ZuTypeReduce_<Reduce, ZuTypeList<Ts...>> :
    public ZuTypeReduce_<Reduce, Ts...> { };
template <template <typename...> class Reduce, typename ...Ts>
using ZuTypeReduce = typename ZuTypeReduce_<Reduce, Ts...>::T;

// split typelist left, 0..N-1
template <unsigned N, typename ...Ts> struct ZuTypeLeft__;
template <unsigned N, typename T0, typename ...Ts>
struct ZuTypeLeft__<N, T0, Ts...> {
  using T = typename ZuTypeLeft__<N - 1, Ts...>::T::template Unshift<T0>;
};
template <typename T0, typename ...Ts>
struct ZuTypeLeft__<0, T0, Ts...> {
  using T = ZuTypeList<>;
};
template <typename ...Ts>
struct ZuTypeLeft__<0, Ts...> {
  using T = ZuTypeList<>;
};
template <unsigned N, typename ...Ts>
struct ZuTypeLeft_ :
    public ZuTypeLeft__<N, Ts...> { };
template <unsigned N, typename ...Ts>
struct ZuTypeLeft_<N, ZuTypeList<Ts...>> :
    public ZuTypeLeft__<N, Ts...> { };
template <unsigned N, typename ...Ts>
using ZuTypeLeft = typename ZuTypeLeft_<N, Ts...>::T;

// split typelist right, N..
template <unsigned N, typename ...Ts> struct ZuTypeRight__;
template <unsigned N, typename T0, typename ...Ts>
struct ZuTypeRight__<N, T0, Ts...> {
  using T = typename ZuTypeRight__<N - 1, Ts...>::T;
};
template <typename T0, typename ...Ts>
struct ZuTypeRight__<0, T0, Ts...> {
  using T = ZuTypeList<T0, Ts...>;
};
template <typename ...Ts>
struct ZuTypeRight__<0, Ts...> {
  using T = ZuTypeList<Ts...>;
};
template <unsigned N, typename ...Ts>
struct ZuTypeRight_ :
    public ZuTypeRight__<N, Ts...> { };
template <unsigned N, typename ...Ts>
struct ZuTypeRight_<N, ZuTypeList<Ts...>> :
    public ZuTypeRight__<N, Ts...> { };
template <unsigned N, typename ...Ts>
using ZuTypeRight = typename ZuTypeRight_<N, Ts...>::T;

// compile-time merge-sort typelist using Index<T>{}
template <template <typename> class Index, typename Left, typename Right>
struct ZuTypeMerge_;
template <template <typename> class, typename, typename, bool>
struct ZuTypeMerge__;
template <
  template <typename> class Index,
  typename LeftT0, typename ...LeftTs,
  typename RightT0, typename ...RightTs>
struct ZuTypeMerge__<Index,
    ZuTypeList<LeftT0, LeftTs...>,
    ZuTypeList<RightT0, RightTs...>, false> {
  using T = typename ZuTypeMerge_<Index,
    ZuTypeList<LeftTs...>,
    ZuTypeList<RightT0, RightTs...>>::T::template Unshift<LeftT0>;
};
template <
  template <typename> class Index,
  typename LeftT0, typename ...LeftTs,
  typename RightT0, typename ...RightTs>
struct ZuTypeMerge__<Index,
    ZuTypeList<LeftT0, LeftTs...>,
    ZuTypeList<RightT0, RightTs...>, true> {
  using T = typename ZuTypeMerge_<Index,
    ZuTypeList<LeftT0, LeftTs...>,
    ZuTypeList<RightTs...>>::T::template Unshift<RightT0>;
};
template <
  template <typename> class Index,
  typename LeftT0, typename ...LeftTs,
  typename RightT0, typename ...RightTs>
struct ZuTypeMerge_<Index,
  ZuTypeList<LeftT0, LeftTs...>,
  ZuTypeList<RightT0, RightTs...>> :
    public ZuTypeMerge__<Index, 
      ZuTypeList<LeftT0, LeftTs...>,
      ZuTypeList<RightT0, RightTs...>,
      (Index<LeftT0>{} > Index<RightT0>{})> { };
template <template <typename> class Index, typename ...Ts>
struct ZuTypeMerge_<Index, ZuTypeList<>, ZuTypeList<Ts...>> {
  using T = ZuTypeList<Ts...>;
};
template <template <typename> class Index, typename ...Ts>
struct ZuTypeMerge_<Index, ZuTypeList<Ts...>, ZuTypeList<>> {
  using T = ZuTypeList<Ts...>;
};
template <template <typename> class Index>
struct ZuTypeMerge_<Index, ZuTypeList<>, ZuTypeList<>> {
  using T = ZuTypeList<>;
};
template <template <typename> class Index, typename Left, typename Right>
using ZuTypeMerge = typename ZuTypeMerge_<Index, Left, Right>::T;
template <template <typename> class Index, typename ...Ts>
struct ZuTypeSort_ {
  enum { N = sizeof...(Ts) };
  using T = ZuTypeMerge<Index,
    typename ZuTypeSort_<Index, ZuTypeLeft<(N>>1), Ts...>>::T,
    typename ZuTypeSort_<Index, ZuTypeRight<(N>>1), Ts...>>::T
  >;
};
template <template <typename> class Index, typename T0>
struct ZuTypeSort_<Index, T0> {
  using T = ZuTypeList<T0>;
};
template <template <typename> class Index>
struct ZuTypeSort_<Index> {
  using T = ZuTypeList<>;
};
template <template <typename> class Index, typename ...Ts>
struct ZuTypeSort_<Index, ZuTypeList<Ts...>> :
    public ZuTypeSort_<Index, Ts...> { };
template <template <typename> class Index, typename ...Ts>
using ZuTypeSort = typename ZuTypeSort_<Index, Ts...>::T;

// apply typelist to template
template <template <typename...> class Type, typename ...Ts>
struct ZuTypeApply_ { using T = Type<Ts...>; };
template <template <typename...> class Type, typename ...Ts>
struct ZuTypeApply_<Type, ZuTypeList<Ts...>> :
  public ZuTypeApply_<Type, Ts...> { };
template <template <typename...> class Type, typename ...Ts>
using ZuTypeApply = typename ZuTypeApply_<Type, Ts...>::T;

// compile-time numerical sequence
template <unsigned ...I> struct ZuSeq {
  enum { N = sizeof...(I) };
};

// generate unsigned sequence [0, N)
template <typename T_, unsigned, bool> struct ZuPushSeq_ {
  using T = T_;
};
template <unsigned ...I, unsigned N>
struct ZuPushSeq_<ZuSeq<I...>, N, true> {
  enum { J = sizeof...(I) };
  using T = typename ZuPushSeq_<ZuSeq<I..., J>, N, (J < N - 1)>::T;
};
template <unsigned N> struct ZuMkSeq_ {
  using T = typename ZuPushSeq_<ZuSeq<>, N, (N > 0)>::T;
};
template <unsigned N> using ZuMkSeq = typename ZuMkSeq_<N>::T;

// convert ZuSeq to typelist
template <typename> struct ZuSeqTL_;
template <> struct ZuSeqTL_<ZuSeq<>> { using T = ZuTypeList<>; };
template <unsigned I> struct ZuSeqTL_<ZuSeq<I>> {
  using T = ZuTypeList<ZuUnsigned<I>>;
};
template <unsigned I, unsigned ...Seq>
struct ZuSeqTL_<ZuSeq<I, Seq...>> {
  using T = ZuTypeList<ZuUnsigned<I>>::template Push<
    typename ZuSeqTL_<ZuSeq<Seq...>>::T>;
};
template <typename Seq> using ZuSeqTL = typename ZuSeqTL_<Seq>::T;
// ... and back again
template <typename ...Seq> struct ZuTLSeq_ { using T = ZuSeq<Seq{}...>; };
template <typename ...Seq>
struct ZuTLSeq_<ZuTypeList<Seq...>> : public ZuTLSeq_<Seq...> { };
template <typename ...Seq> using ZuTLSeq = typename ZuTLSeq_<Seq...>::T;

// min/max of a numerical sequence
template <typename> struct ZuMin;
template <> struct ZuMin<ZuSeq<>> : public ZuUnsigned<UINT_MAX> { };
template <unsigned I> struct ZuMin<ZuSeq<I>> : public ZuUnsigned<I> { };
template <unsigned I, unsigned J>
struct ZuMin<ZuSeq<I, J>> : public ZuUnsigned<(I < J) ? I : J> { };
template <unsigned I, unsigned J, unsigned ...Seq>
struct ZuMin<ZuSeq<I, J, Seq...>> :
    public ZuMin<ZuSeq<((I < J) ? I : J), Seq...>> { };

template <typename> struct ZuMax;
template <> struct ZuMax<ZuSeq<>> : public ZuUnsigned<0> { };
template <unsigned I> struct ZuMax<ZuSeq<I>> : public ZuUnsigned<I> { };
template <unsigned I, unsigned J>
struct ZuMax<ZuSeq<I, J>> : public ZuUnsigned<(I > J) ? I : J> { };
template <unsigned I, unsigned J, unsigned ...Seq>
struct ZuMax<ZuSeq<I, J, Seq...>> :
    public ZuMax<ZuSeq<((I > J) ? I : J), Seq...>> { };

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

// function signature deduction

// ZuDeduce<decltype(&L::operator())>::
// ZuDeduce<decltype(&fn)>::
//   O		- object type (undefined for a plain function)
//   R		- return value
//   Args	- argument typelist

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
template <typename ...Ts> using ZuVoid = typename ZuVoid_<Ts...>::T;

// sizeof(void) and empty-class handling:
// - ZuSize<T>{} is 0 if T is void or an empty class
// - ZuSize<T>{} is sizeof(T) otherwise
template <typename T, bool = __is_empty(T)>
struct ZuSize__ : public ZuUnsigned<sizeof(T)> { };
template <typename T>
struct ZuSize__<T, true> : public ZuUnsigned<0> { };
template <typename T, typename = void>
struct ZuSize_ : public ZuUnsigned<sizeof(T)> { };
template <typename T>
struct ZuSize_<T, decltype(sizeof(T), (int T::*){}, void())> :
public ZuSize__<T> { };
template <typename T> struct ZuSize : public ZuSize_<T> { };
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

// recursive decay (for pair, tuple, union, etc.)
struct ZuDefaultRDecayer {
  template <typename T_> struct Decay { using T = T_; };
};
ZuDefaultRDecayer ZuRDecayer(...);
template <typename T_>
struct ZuRDecay_ {
  using Decayer = decltype(ZuRDecayer(ZuDeclVal<T_ *>()));
  using T = typename Decayer::template Decay<T_>::T;
};
template <typename T>
using ZuRDecay = typename ZuRDecay_<ZuDecay<T>>::T;

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
//   Fn(this, args...)		// bound function/lambda (passing this)
//   Fn(args)			// unbound function/lambda (discarding this)

template <auto Fn, typename O, typename Ts, typename = void>
struct ZuInvoke_MemberFn_;
template <auto Fn, typename O, typename ...Ts>
struct ZuInvoke_MemberFn_<
    Fn, O, ZuTypeList<Ts...>,
    decltype((ZuDeclVal<O *>()->*Fn)(ZuDeclVal<Ts>()...), void())> {
  using T = decltype((ZuDeclVal<O *>()->*Fn)(ZuDeclVal<Ts>()...));
};
template <auto Fn, typename O, typename ...Ts>
using ZuInvoke_MemberFn =
  typename ZuInvoke_MemberFn_<Fn, O, ZuTypeList<Ts...>>::T;
template <auto Fn, typename O, typename ...Ts>
auto ZuInvoke(O *ptr, Ts &&... args) -> ZuInvoke_MemberFn<Fn, O, Ts...> {
  return (ptr->*Fn)(ZuFwd<Ts>(args)...);
}

template <auto Fn, typename O, typename Ts, typename = void>
struct ZuInvoke_BoundFn_;
template <auto Fn, typename O, typename ...Ts>
struct ZuInvoke_BoundFn_<
    Fn, O, ZuTypeList<Ts...>,
    decltype(Fn(ZuDeclVal<O *>(), ZuDeclVal<Ts>()...), void())> {
  using T = decltype(Fn(ZuDeclVal<O *>(), ZuDeclVal<Ts>()...));
};
template <auto Fn, typename O, typename ...Ts>
using ZuInvoke_BoundFn =
  typename ZuInvoke_BoundFn_<Fn, O, ZuTypeList<Ts...>>::T;
template <auto Fn, typename O, typename ...Ts>
auto ZuInvoke(O *ptr, Ts &&... args) -> ZuInvoke_BoundFn<Fn, O, Ts...> {
  return Fn(ptr, ZuFwd<Ts>(args)...);
}

template <auto Fn, typename O, typename Ts, typename = void>
struct ZuInvoke_UnboundFn_;
template <auto Fn, typename O, typename ...Ts>
struct ZuInvoke_UnboundFn_<
    Fn, O, ZuTypeList<Ts...>,
    decltype(Fn(ZuDeclVal<Ts>()...), void())> {
  using T = decltype(Fn(ZuDeclVal<Ts>()...));
};
template <auto Fn, typename O, typename ...Ts>
using ZuInvoke_UnboundFn =
  typename ZuInvoke_UnboundFn_<Fn, O, ZuTypeList<Ts...>>::T;
template <auto Fn, typename O, typename ...Ts>
auto ZuInvoke(O *, Ts &&... args) -> ZuInvoke_UnboundFn<Fn, O, Ts...> {
  return Fn(ZuFwd<Ts>(args)...);
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

#endif /* ZuLib_HH */
