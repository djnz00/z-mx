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

// type inspection for convertibility & inheritance
//
// ZuInspect<T1, T2>::Converts	- ZuDeref<T1> can be converted into ZuDeref<T2>
// ZuInspect<T1, T2>::Same	- ZuDecay<T1> is same type as ZuDecay<T2>
// ZuInspect<T1, T2>::Is	- ZuDecay<T1> is same or a base of ZuDecay<T2>
// ZuInspect<T1, T2>::Base	- ZuDecay<T1> is a base of ZuDecay<T2>
//
// ZuInspect<T1, T2>::Same is equivalent to ZuIsExact<ZuDecay<T1>, ZuDecay<T2>>{}

#ifndef ZuInspect_HPP
#define ZuInspect_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4244 4800)
#endif

struct ZuInspect___ {
  typedef char	Small;
  struct	Big { char _[2]; };
};
template <typename T1, typename T2>
struct ZuInspect__ : public ZuInspect___ {
private:
  static Small	ZuInspect_test(const T2 &_); // named due to VS2010 bug
  static Big	ZuInspect_test(...);

public:
  ZuInspect__(); // keep gcc quiet
  enum {
    Converts = sizeof(ZuInspect_test(ZuDeclVal<T1 &>())) == sizeof(Small),
    Same = 0
  };
};
#if 0
template <typename T1, typename T2> struct ZuInspect__ {
  enum {
    _ = sizeof(T1) == sizeof(T2), // ensure both types are complete
    Converts =
      std::is_constructible<T2, T1>::value ||
      std::is_convertible<T1, T2>::value,
    Same = 0
  };
};
#endif
template <typename T> struct ZuInspect__<T, T> {
  enum { Converts = 1, Same = 1 };
};
#define ZuInspectFriend \
  template <typename, typename> friend struct ZuInspect__

template <typename T_> struct ZuInspect_Array
  { using T = T_; };
template <typename T_> struct ZuInspect_Array<T_ []>
  { using T = T_ *const; };
template <typename T_> struct ZuInspect_Array<const T_ []>
  { using T = const T_ *const; };
template <typename T_> struct ZuInspect_Array<volatile T_ []>
  { using T = volatile T_ *const; };
template <typename T_> struct ZuInspect_Array<const volatile T_ []>
  { using T = const volatile T_ *const; };
template <typename T_, int N> struct ZuInspect_Array<T_ [N]>
  { using T = T_ *const; };
template <typename T_, int N> struct ZuInspect_Array<const T_ [N]>
  { using T = const T_ *const; };
template <typename T_, int N> struct ZuInspect_Array<volatile T_ [N]>
  { using T = volatile T_ *const; };
template <typename T_, int N> struct ZuInspect_Array<const volatile T_ [N]>
  { using T = const volatile T_ *const; };

template <typename T1, typename T2> struct ZuInspect_ {
  using U1 = typename ZuInspect_Array<T1>::T;
  using U2 = typename ZuInspect_Array<T2>::T;
  enum {
    Converts = ZuInspect__<U1, U2>::Converts,
    Same = ZuInspect__<const volatile U1, const volatile U2>::Same,
    Is = ZuInspect__<const volatile U2 *, const volatile U1 *>::Converts &&
      !ZuInspect__<const volatile U1 *, const volatile void *>::Same
  };
};
template <> struct ZuInspect_<void, void> {
  enum { Converts = 1, Same = 1, Is = 1 };
};
template <typename T> struct ZuInspect_<void, T> {
  enum { Converts = 0, Same = 0, Is = 0 };
};
template <typename T> struct ZuInspect_<T, void> {
  enum { Converts = 0, Same = 0, Is = 0 };
};

template <typename T_> struct ZuInspect_Void { using T = T_; };
template <> struct ZuInspect_Void<const void> { using T = void; };
template <> struct ZuInspect_Void<volatile void> { using T = void; };
template <> struct ZuInspect_Void<const volatile void> { using T = void; };

template <typename T1, typename T2> class ZuInspect {
  using U1 = typename ZuInspect_Void<ZuDeref<T1>>::T;
  using U2 = typename ZuInspect_Void<ZuDeref<T2>>::T;
public:
  enum {
    Same = ZuInspect_<U1, U2>::Same,
    Converts = ZuInspect_<U1, U2>::Converts,
    Is = ZuInspect_<U1, U2>::Is,
    Base = ZuInspect_<U1, U2>::Is && !ZuInspect_<U1, U2>::Same
  };
};

// SFINAE techniques...
template <typename T1, typename T2, typename R = void>
using ZuSame = ZuIfT<ZuInspect<T1, T2>::Same, R>;
template <typename T1, typename T2, typename R = void>
using ZuNotSame = ZuIfT<!ZuInspect<T1, T2>::Same, R>;
template <typename T1, typename T2, typename R = void>
using ZuConvertible = ZuIfT<ZuInspect<T1, T2>::Converts, R>;
template <typename T1, typename T2, typename R = void>
using ZuNotConvertible = ZuIfT<!ZuInspect<T1, T2>::Converts, R>;
template <typename T1, typename T2, typename R = void>
using ZuBase = ZuIfT<ZuInspect<T1, T2>::Base, R>;
template <typename T1, typename T2, typename R = void>
using ZuNotBase = ZuIfT<!ZuInspect<T1, T2>::Base, R>;
template <typename T1, typename T2, typename R = void>
using ZuIs = ZuIfT<ZuInspect<T1, T2>::Is, R>;
template <typename T1, typename T2, typename R = void>
using ZuIsNot = ZuIfT<!ZuInspect<T1, T2>::Is, R>;

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZuInspect_HPP */
