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

// type convertibility & inheritance
//
// all types are de-referenced before comparison, as if ZuDeref was applied
//
// Same, Is and Base ignore cv qualifiers, as if ZuStrip was applied
//
// ZuConversion<T1, T2>::Exists	- T1 can be converted into T2
// ZuConversion<T1, T2>::Same	- T1 is same type as T2
// ZuConversion<T1, T2>::Is	- T1 is the same or a base class of T2
// ZuConversion<T1, T2>::Base	- T1 is a base class of T2

#ifndef ZuConversion_HPP
#define ZuConversion_HPP

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

struct ZuConversion___ {
  typedef char	Small;
  struct	Big { char _[2]; };
};
template <typename T1, typename T2>
struct ZuConversion__ : public ZuConversion___ {
private:
  static Small	ZuConversion_test(const T2 &_); // named due to VS2010 bug
  static Big	ZuConversion_test(...);

public:
  ZuConversion__(); // keep gcc quiet
  enum {
    Exists = sizeof(ZuConversion_test(ZuDeclVal<T1 &>())) == sizeof(Small),
    Same = 0
  };
};

#if 0
template <typename T1, typename T2> struct ZuConversion__ {
  enum {
    _ = sizeof(T1) == sizeof(T2), // ensure both types are complete
    Exists =
      std::is_constructible<T2, T1>::value ||
      std::is_convertible<T1, T2>::value,
    Same = 0
  };
};
#endif

#define ZuConversionFriend \
  template <typename, typename> friend struct ZuConversion__

template <typename T> struct ZuConversion__<T, T>
  { enum { Exists = 1, Same = 1, Base = 0, Is = 1 }; };

template <typename T_> struct ZuConversion_Array
  { using T = T_; };
template <typename T_> struct ZuConversion_Array<T_ []>
  { using T = T_ *const; };
template <typename T_> struct ZuConversion_Array<const T_ []>
  { using T = const T_ *const; };
template <typename T_> struct ZuConversion_Array<volatile T_ []>
  { using T = volatile T_ *const; };
template <typename T_> struct ZuConversion_Array<const volatile T_ []>
  { using T = const volatile T_ *const; };
template <typename T_, int N> struct ZuConversion_Array<T_ [N]>
  { using T = T_ *const; };
template <typename T_, int N> struct ZuConversion_Array<const T_ [N]>
  { using T = const T_ *const; };
template <typename T_, int N> struct ZuConversion_Array<volatile T_ [N]>
  { using T = volatile T_ *const; };
template <typename T_, int N> struct ZuConversion_Array<const volatile T_ [N]>
  { using T = const volatile T_ *const; };

template <typename T1, typename T2> struct ZuConversion_ {
  using U1 = typename ZuConversion_Array<T1>::T;
  using U2 = typename ZuConversion_Array<T2>::T;
  enum {
    Exists = ZuConversion__<U1, U2>::Exists,
    Same = ZuConversion__<const volatile U1, const volatile U2>::Same,
    Is = ZuConversion__<const volatile U2 *, const volatile U1 *>::Exists &&
      !ZuConversion__<const volatile U1 *, const volatile void *>::Same
  };
};

template <> struct ZuConversion_<void, void>
  { enum { Exists = 1, Same = 1, Base = 0, Is = 1 }; };
template <typename T> struct ZuConversion_<void, T>
  { enum { Exists = 0, Same = 0, Base = 0, Is = 0 }; };
template <typename T> struct ZuConversion_<T, void>
  { enum { Exists = 0, Same = 0, Base = 0, Is = 0 }; };

template <typename T_> struct ZuConversion_Void
  { using T = T_; };
template <> struct ZuConversion_Void<const void>
  { using T = void; };
template <> struct ZuConversion_Void<volatile void>
  { using T = void; };
template <> struct ZuConversion_Void<const volatile void>
  { using T = void; };

template <typename T1, typename T2> class ZuConversion {
  using U1 = typename ZuConversion_Void<ZuDeref<T1>>::T;
  using U2 = typename ZuConversion_Void<ZuDeref<T2>>::T;
public:
  enum {
    Exists = ZuConversion_<U1, U2>::Exists,
    Same = ZuConversion_<U1, U2>::Same,
    Is = ZuConversion_<U1, U2>::Is,
    Base = ZuConversion_<U1, U2>::Is && !ZuConversion_<U1, U2>::Same
  };
};

// SFINAE techniques...
template <typename T1, typename T2, typename R = void>
using ZuSame = ZuIfT<ZuConversion<T1, T2>::Same, R>;
template <typename T1, typename T2, typename R = void>
using ZuNotSame = ZuIfT<!ZuConversion<T1, T2>::Same, R>;
template <typename T1, typename T2, typename R = void>
using ZuConvertible = ZuIfT<ZuConversion<T1, T2>::Exists, R>;
template <typename T1, typename T2, typename R = void>
using ZuNotConvertible = ZuIfT<!ZuConversion<T1, T2>::Exists, R>;
template <typename T1, typename T2, typename R = void>
using ZuIsBase = ZuIfT<ZuConversion<T1, T2>::Base, R>;
template <typename T1, typename T2, typename R = void>
using ZuNotBase = ZuIfT<!ZuConversion<T1, T2>::Base, R>;
template <typename T1, typename T2, typename R = void>
using ZuIs = ZuIfT<ZuConversion<T1, T2>::Is, R>;
template <typename T1, typename T2, typename R = void>
using ZuIsNot = ZuIfT<!ZuConversion<T1, T2>::Is, R>;

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZuConversion_HPP */
