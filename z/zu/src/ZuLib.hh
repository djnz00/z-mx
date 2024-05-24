//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

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

// alloca() alias

#ifdef _MSC_VER
#define ZuAlloca(n) _alloca(n)
#else
#ifndef _WIN32
#include <alloca.h>
#endif
#define ZuAlloca(n) alloca(n)
#endif

// default accessor (pass-through)
inline constexpr auto ZuDefaultAxor() {
  return []<typename T>(T &&v) -> decltype(auto) { return ZuFwd<T>(v); };
}

#endif /* ZuLib_HH */
