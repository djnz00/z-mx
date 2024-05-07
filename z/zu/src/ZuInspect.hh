//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// type inspection for convertibility, constructibility & inheritance
// - subtly different than std::is_convertible, is_constructible
//
// ZuInspect<T1, T2>::Converts	 - ZuDeref<T1> can convert into ZuDeref<T2>
// ZuInspect<T1, T2>::Constructs - ZuDeref<T2> can construct from ZuDeref<T1>
// ZuInspect<T1, T2>::Same	 - ZuDecay<T1> is same type as ZuDecay<T2>
// ZuInspect<T1, T2>::Is	 - ZuDecay<T1> is same or a base of ZuDecay<T2>
// ZuInspect<T1, T2>::Base	 - ZuDecay<T1> is a base of ZuDecay<T2>
//
// ZuInspect<T1, T2>::Same is same as ZuIsExact<ZuDecay<T1>, ZuDecay<T2>>{}

#ifndef ZuInspect_HH
#define ZuInspect_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4244 4800)
#endif

namespace ZuInspect_ {

struct Convertible_ {
  typedef char	Small;
  struct	Big { char _[2]; };
};
template <typename T1, typename T2>
struct Convertible : public Convertible_ {
private:
  static Small	test(const T2 &);
  static Big	test(...);

public:
  enum { Converts = sizeof(test(ZuDeclVal<T1 &>())) == sizeof(Small) };
};

template <typename T1, typename T2, typename = void>
struct Constructible {
  enum { Constructs = 0 };
};
template <typename T1, typename T2>
struct Constructible<T1, T2, decltype(T2{ZuDeclVal<T1 &>()}, void())> {
  enum { Constructs = 1 };
};

template <typename T1, typename T2>
struct Inspect__ : public Convertible<T1, T2>, public Constructible<T1, T2> {
  enum { Same = 0 };
};

template <typename T> struct Inspect__<T, T> {
  enum { Converts = 1, Constructs = 1, Same = 1 };
};

template <typename T_> struct Inspect_Array
  { using T = T_; };
template <typename T_> struct Inspect_Array<T_ []>
  { using T = T_ *const; };
template <typename T_> struct Inspect_Array<const T_ []>
  { using T = const T_ *const; };
template <typename T_> struct Inspect_Array<volatile T_ []>
  { using T = volatile T_ *const; };
template <typename T_> struct Inspect_Array<const volatile T_ []>
  { using T = const volatile T_ *const; };
template <typename T_, int N> struct Inspect_Array<T_ [N]>
  { using T = T_ *const; };
template <typename T_, int N> struct Inspect_Array<const T_ [N]>
  { using T = const T_ *const; };
template <typename T_, int N> struct Inspect_Array<volatile T_ [N]>
  { using T = volatile T_ *const; };
template <typename T_, int N> struct Inspect_Array<const volatile T_ [N]>
  { using T = const volatile T_ *const; };

template <typename T1, typename T2> struct Inspect_ {
  using U1 = typename Inspect_Array<T1>::T;
  using U2 = typename Inspect_Array<T2>::T;
  enum {
    Converts = Inspect__<U1, U2>::Converts,
    Constructs = Inspect__<U1, U2>::Constructs,
    Same = Inspect__<const volatile U1, const volatile U2>::Same,
    Is = Inspect__<const volatile U2 *, const volatile U1 *>::Converts &&
	!Inspect__<const volatile U1 *, const volatile void *>::Same
  };
};
template <> struct Inspect_<void, void> {
  enum { Converts = 1, Constructs = 1, Same = 1, Is = 1 };
};
template <typename T> struct Inspect_<void, T> {
  enum { Converts = 0, Constructs = 0, Same = 0, Is = 0 };
};
template <typename T> struct Inspect_<T, void> {
  enum { Converts = 0, Constructs = 0, Same = 0, Is = 0 };
};
template <> struct Inspect_<std::nullptr_t, std::nullptr_t> {
  enum { Converts = 1, Constructs = 1, Same = 1, Is = 1 };
};
template <typename T> struct Inspect_<std::nullptr_t, T *> {
  enum { Converts = 1, Constructs = 1, Same = 0, Is = 0 };
};
template <typename T> struct Inspect_<std::nullptr_t, const T *> {
  enum { Converts = 1, Constructs = 1, Same = 0, Is = 0 };
};
template <typename T> struct Inspect_<std::nullptr_t, volatile T *> {
  enum { Converts = 1, Constructs = 1, Same = 0, Is = 0 };
};
template <typename T> struct Inspect_<std::nullptr_t, const volatile T *> {
  enum { Converts = 1, Constructs = 1, Same = 0, Is = 0 };
};

template <typename T_> struct Inspect_Void { using T = T_; };
template <> struct Inspect_Void<const void> { using T = void; };
template <> struct Inspect_Void<volatile void> { using T = void; };
template <> struct Inspect_Void<const volatile void> { using T = void; };

template <typename T1, typename T2> class Inspect {
  using U1 = typename Inspect_Void<ZuDeref<T1>>::T;
  using U2 = typename Inspect_Void<ZuDeref<T2>>::T;
public:
  enum {
    Same = Inspect_<U1, U2>::Same,
    Converts = Inspect_<U1, U2>::Converts,
    Constructs = Inspect_<U1, U2>::Constructs,
    Is = Inspect_<U1, U2>::Is,
    Base = Inspect_<U1, U2>::Is && !Inspect_<U1, U2>::Same
  };
};

} // ZuInspect_

template <typename T1, typename T2>
using ZuInspect = ZuInspect_::Inspect<T1, T2>;

#define ZuInspectFriend \
  template <typename, typename> friend struct ZuInspect_::Convertible

// SFINAE
template <typename T1, typename T2, typename R = void>
using ZuSame = ZuIfT<ZuInspect<T1, T2>::Same, R>;
template <typename T1, typename T2, typename R = void>
using ZuNotSame = ZuIfT<!ZuInspect<T1, T2>::Same, R>;
template <typename T1, typename T2, typename R = void>
using ZuConvertible = ZuIfT<ZuInspect<T1, T2>::Converts, R>;
template <typename T1, typename T2, typename R = void>
using ZuNotConvertible = ZuIfT<!ZuInspect<T1, T2>::Converts, R>;
template <typename T1, typename T2, typename R = void>
using ZuConstructible = ZuIfT<ZuInspect<T1, T2>::Constructs, R>;
template <typename T1, typename T2, typename R = void>
using ZuNotConstructible = ZuIfT<!ZuInspect<T1, T2>::Constructs, R>;
template <typename T1, typename T2, typename R = void>
using ZuBase = ZuIfT<ZuInspect<T1, T2>::Base, R>;
template <typename T1, typename T2, typename R = void>
using ZuNotBase = ZuIfT<!ZuInspect<T1, T2>::Base, R>;
template <typename T1, typename T2, typename R = void>
using ZuIs = ZuIfT<ZuInspect<T1, T2>::Is, R>;
template <typename T1, typename T2, typename R = void>
using ZuIsNot = ZuIfT<!ZuInspect<T1, T2>::Is, R>;

// type list convertibility
template <typename Ts, typename Us> // true if Ts converts to Us
struct ZuTLConverts : public ZuFalse { };
template <typename, typename, unsigned, bool>
struct ZuTLConverts_ : public ZuFalse { };
template <>
struct ZuTLConverts_<ZuTypeList<>, ZuTypeList<>, 0, true> : public ZuTrue { };
template <typename T0, typename ...Ts, typename U0, typename ...Us, unsigned N>
struct ZuTLConverts_<ZuTypeList<T0, Ts...>, ZuTypeList<U0, Us...>, N, true> :
  public ZuTLConverts_<
    ZuTypeList<Ts...>, ZuTypeList<Us...>, N - 1,
    ZuInspect<T0, U0>::Converts> { };
template <typename ...Ts, typename ...Us>
struct ZuTLConverts<ZuTypeList<Ts...>, ZuTypeList<Us...>> :
  public ZuTLConverts_<
    ZuTypeList<Ts...>, ZuTypeList<Us...>,
    sizeof...(Us), sizeof...(Ts) == sizeof...(Us)> { };

// type list constructibility
template <typename Ts, typename Us> // true if Us can be constructed from Ts
struct ZuTLConstructs : public ZuFalse { };
template <typename, typename, unsigned, bool>
struct ZuTLConstructs_ : public ZuFalse { };
template <>
struct ZuTLConstructs_<ZuTypeList<>, ZuTypeList<>, 0, true> : public ZuTrue { };
template <typename T0, typename ...Ts, typename U0, typename ...Us, unsigned N>
struct ZuTLConstructs_<ZuTypeList<T0, Ts...>, ZuTypeList<U0, Us...>, N, true> :
  public ZuTLConstructs_<
    ZuTypeList<Ts...>, ZuTypeList<Us...>, N - 1,
    ZuInspect<T0, U0>::Constructs> { };
template <typename ...Ts, typename ...Us>
struct ZuTLConstructs<ZuTypeList<Ts...>, ZuTypeList<Us...>> :
  public ZuTLConstructs_<
    ZuTypeList<Ts...>, ZuTypeList<Us...>,
    sizeof...(Us), sizeof...(Ts) == sizeof...(Us)> { };

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZuInspect_HH */
