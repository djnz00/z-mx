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

// generic three-way and two-way comparison 
// (including distinguished null values)

// UDTs must implement the <, == and ! operators
//
// For more efficiency, implement cmp()
// (operators <, == and ! must always be implemented)
//
// class UDT {
//   ...
//   int cmp(const UDT &t) { ... }
//   // returns -ve if *this < t, 0 if *this == t, +ve if *this > t
//   ...
// };
//
// unless ZuCmp<UDT> is explicitly specialized, UDT::operator !(UDT())
// should always return true
//
// to specialize ZuCmp<UDT>, implement the following:
//
// template <> struct ZuCmp<UDT> {
//   // returns -ve if v1 < v2, 0 if v1 == v2, +ve if v1 > v2
//   static int cmp(const UDT &v1, const UDT &v2) { ... }
//   // returns v1 == v2
//   static bool equals(const UDT &v1, const UDT &v2) { ... }
//   // returns v1 < v2
//   static bool less(const UDT &v1, const UDT &v2) { ... }
//   // returns !t
//   static bool null(const UDT &v) { ... }
//   // returns distinguished "null" value for UDT
//   static const UDT &null() { static UDT udt = ...; return udt; }
// };
//
// ZuCmp<T>::null() returns a distinguished "null" value for T that may
// (or may not) lie within the normal range of values for T; the default
// ZuCmp<T>::null() performs as follows:
//
// Type			Null	Notes
// ----			----	-----
// UDT			T()	result of default constructor
// bool			0	false
// char			0	ASCII NUL
// signed integer	MIN	minimum representable value
// unsigned integer	MAX	maximum representable value
// floating point	NaN	IEEE 754 NaN
// pointer, C string	0	aka nullptr, NULL
//
// Note: int8_t  is a typedef for signed char
//	 uint8_t is a typedef for unsigned char
//	 char is a third, distinct type from both signed char and unsigned char
//	 (but which has the same representation, values, alignment and size
//	 as one of them, depending on the implementation)

#ifndef ZuCmp_HPP
#define ZuCmp_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <string.h>
#include <wchar.h>

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuInt.hpp>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4800)
#endif

template <typename T, bool = ZuTraits<T>::IsString> struct ZuCmp_;

// intentionally undefined template

template <typename T> struct ZuCmp_Cannot;

// sentinel values for integral types
// Note: minimum() and maximum() intentionally exclude the null() sentinel

template <typename T, int Size, bool Signed> struct ZuCmp_IntSentinel;
template <typename T> struct ZuCmp_IntSentinel<T, 1, false> {
  constexpr static T minimum() { return static_cast<T>(0); }
  constexpr static T maximum() { return static_cast<T>(0xfe); }
  constexpr static T null() { return static_cast<T>(0xff); }
};
template <typename T> struct ZuCmp_IntSentinel<T, 1, true> {
  constexpr static T minimum() { return static_cast<T>(-0x7f); }
  constexpr static T maximum() { return static_cast<T>(0x7f); }
  constexpr static T null() { return static_cast<T>(-0x80); }
};
template <typename T> struct ZuCmp_IntSentinel<T, 2, false> {
  constexpr static T minimum() { return static_cast<T>(0); }
  constexpr static T maximum() { return static_cast<T>(0xfffe); }
  constexpr static T null() { return static_cast<T>(0xffff); }
};
template <typename T> struct ZuCmp_IntSentinel<T, 2, true> {
  constexpr static T minimum() { return static_cast<T>(-0x7fff); }
  constexpr static T maximum() { return static_cast<T>(0x7fff); }
  constexpr static T null() { return static_cast<T>(-0x8000); }
};
template <typename T> struct ZuCmp_IntSentinel<T, 4, false> {
  constexpr static T minimum() { return static_cast<T>(0); }
  constexpr static T maximum() { return null() - 1; }
  constexpr static T null() { return ~static_cast<T>(0); }
};
template <typename T> struct ZuCmp_IntSentinel<T, 4, true> {
  constexpr static T minimum() { return static_cast<T>(-0x7fffffff); }
  constexpr static T maximum() { return static_cast<T>(0x7fffffff); }
  constexpr static T null() { return static_cast<T>(-0x80000000); }
};
template <typename T> struct ZuCmp_IntSentinel<T, 8, false> {
  constexpr static T minimum() { return static_cast<T>(0); }
  constexpr static T maximum() { return null() - 1; }
  constexpr static T null() { return ~static_cast<T>(0); }
};
template <typename T> struct ZuCmp_IntSentinel<T, 8, true> {
  constexpr static T minimum() { return static_cast<T>(-0x7fffffffffffffffLL); }
  constexpr static T maximum() { return static_cast<T>(0x7fffffffffffffffLL); }
  constexpr static T null() { return static_cast<T>(-0x8000000000000000LL); }
};
template <typename T> struct ZuCmp_IntSentinel<T, 16, false> {
  constexpr static T minimum() { return static_cast<T>(0); }
  constexpr static T maximum() { return null() - 1; }
  constexpr static T null() { return ~static_cast<T>(0); }
};
template <typename T> struct ZuCmp_IntSentinel<T, 16, true> {
  constexpr static T minimum() { return null() + 1; }
  constexpr static T maximum() { return ~null(); }
  constexpr static T null() { return (static_cast<T>(1))<<127; }
};

// comparison of larger-sized (>= sizeof(int)) integral types

template <typename T, bool isSmallInt>
struct ZuCmp_Integral :
    public ZuCmp_IntSentinel<T, sizeof(T), ZuTraits<T>::IsSigned> {
  using Base = ZuCmp_IntSentinel<T, sizeof(T), ZuTraits<T>::IsSigned>;
  using Base::minimum;
  using Base::maximum;
  using Base::null;
  // delta() returns a value suitable for use in interpolation search
  enum { DeltaShift = ((sizeof(T) - sizeof(int))<<3) + 1 };
  constexpr static int delta(T i1, T i2) {
    if (i1 == i2) return 0;
    if (i1 > i2) {
      i1 -= i2;
      i1 >>= DeltaShift;
      int delta = i1;
      return delta | 1;
    }
    i2 -= i1;
    i2 >>= DeltaShift;
    int delta = i2;
    return -(delta | 1);
  }
  constexpr static int cmp(T i1, T i2) {
    return (i1 > i2) - (i1 < i2);
  }
  constexpr static bool less(T i1, T i2) { return i1 < i2; }
  constexpr static bool equals(T i1, T i2) { return i1 == i2; }
  constexpr static bool null(T i) { return i == null(); }
  constexpr static T epsilon(T f) { return 0; }
};

// comparison of small-sized (< sizeof(int)) integral types

template <typename T>
struct ZuCmp_Integral<T, true> :
    public ZuCmp_IntSentinel<T, sizeof(T), ZuTraits<T>::IsSigned> {
  using Base = ZuCmp_IntSentinel<T, sizeof(T), ZuTraits<T>::IsSigned>;
  using Base::minimum;
  using Base::maximum;
  using Base::null;
  constexpr static int delta(T i1, T i2) {
    return static_cast<int>(i1) - static_cast<int>(i2);
  }
  constexpr static int cmp(T i1, T i2) {
    return static_cast<int>(i1) - static_cast<int>(i2);
  }
  constexpr static bool less(T i1, T i2) { return i1 < i2; }
  constexpr static bool equals(T i1, T i2) { return i1 == i2; }
  constexpr static bool null(T i) { return i == null(); }
  constexpr static T epsilon(T f) { return 0; }
};

// comparison of floating point types

template <typename T> struct ZuCmp_Floating {
  constexpr static int cmp(T f1, T f2) { return (f1 > f2) - (f1 < f2); }
  constexpr static bool less(T f1, T f2) { return f1 < f2; }
  constexpr static bool equals(T f1, T f2) { return f1 == f2; }
  constexpr static T null() { return ZuTraits<T>::nan(); } // can't be constexpr
  constexpr static bool null(T f) { return ZuTraits<T>::nan(f); }
  static T inf() { return ZuTraits<T>::inf(); }
  static bool inf(T f) { return ZuTraits<T>::inf(f); }
  static T epsilon(T f) { return ZuTraits<T>::epsilon(f); }
};

// comparison of real numbers (integral and floating point)

template <typename T, bool isIntegral> struct ZuCmp_Real;
template <typename T> struct ZuCmp_Real<T, false> :
  public ZuCmp_Floating<T> { };
template <typename T> struct ZuCmp_Real<T, true> :
  public ZuCmp_Integral<T, sizeof(T) < sizeof(int)> { };

// comparison of primitive types

template <typename T, bool IsReal, bool IsVoid>
struct ZuCmp_Primitive : public ZuCmp_Cannot<T> { };
template <typename T> struct ZuCmp_Primitive<T, true, false> :
  public ZuCmp_Real<T, ZuTraits<T>::IsIntegral> { };

// default comparison of non-primitive types - uses operators >, <, ==, !

template <typename> struct ZuCmp_Can_;
template <> struct ZuCmp_Can_<int> { using T = void; }; 
template <typename, typename, typename = void>
struct ZuCmp_Can { enum { OK = 0 }; };
template <typename P1, typename P2>
struct ZuCmp_Can<P1, P2, typename ZuCmp_Can_<
    decltype(ZuDeclVal<const P1 &>().cmp(ZuDeclVal<const P2 &>()))>::T> {
  enum { OK = 1 };
};

template <typename T, typename = void>
struct ZuCmp_NullFn {
  template <typename P> static bool null(const P &p) { return !p; }
  static T null() { return T{}; }
};
template <typename T>
struct ZuCmp_NullFn<T, decltype(T{ZuDeclVal<const T &>()}, void())> {
  template <typename P> static bool null(const P &p) { return !p; }
  static const T &null() { static T v; return v; }
};
template <typename T, bool Fwd, bool = ZuCmp_Can<T, T>::OK>
struct ZuCmp_NonPrimitive : public ZuCmp_NullFn<T> {
  template <typename P1, typename P2, typename T_ = T>
  static ZuIfT<ZuCmp_Can<P1, P2>::OK, int>
  cmp(const P1 &p1, const P2 &p2) {
    return p1.cmp(p2);
  }
  template <typename P1, typename P2>
  static ZuIfT<!ZuCmp_Can<P1, P2>::OK, int>
  cmp(const P1 &p1, const P2 &p2) {
    return (p1 > p2) - (p1 < p2);
  }
  template <typename P1, typename P2>
  static bool less(const P1 &p1, const P2 &p2) { return p1 < p2; }
  template <typename P1, typename P2>
  static bool equals(const P1 &p1, const P2 &p2) { return p1 == p2; }
};
template <typename P1, bool IsPrimitive> struct ZuCmp_NonPrimitive___ :
    public ZuCmp_NonPrimitive<P1, false> { };
template <typename P1>
struct ZuCmp_NonPrimitive___<P1, true> : public ZuCmp_<P1> { };
template <typename T, typename P1> struct ZuCmp_NonPrimitive__ :
  public ZuCmp_NonPrimitive___<P1, ZuTraits<P1>::IsPrimitive> { };
template <typename T> struct ZuCmp_NonPrimitive__<T, T> {
  template <typename P2>
  static int cmp(const T &t, const P2 &p2) { return t.cmp(p2); }
};
template <typename T, typename P1, bool Fwd>
struct ZuCmp_NonPrimitive_ : public ZuCmp_NonPrimitive__<T, P1> { };
template <typename T, typename P1> struct ZuCmp_NonPrimitive_<T, P1, false> {
  template <typename P2>
  static int cmp(const T &t, const P2 &p2) { return t.cmp(p2); }
};
template <typename T, bool Fwd>
struct ZuCmp_NonPrimitive<T, Fwd, true> : public ZuCmp_NullFn<T> {
  template <typename P1, typename P2>
  static int cmp(const P1 &p1, const P2 &p2) {
    return ZuCmp_NonPrimitive_<T, ZuDecay<P1>, Fwd>::cmp(p1, p2);
  }
  template <typename P1, typename P2>
  static bool less(const P1 &p1, const P2 &p2) { return p1 < p2; }
  template <typename P1, typename P2>
  static bool equals(const P1 &p1, const P2 &p2) { return p1 == p2; }
};

// comparison of pointers (including smart pointers)

template <typename T> struct ZuCmp_Pointer {
  using P = typename ZuTraits<T>::Elem;
  static int cmp(const P *p1, const P *p2) {
    return ((char *)p1 > (char *)p2) - ((char *)p1 < (char *)p2);
  }
  static bool less(const P *p1, const P *p2) { return p1 < p2; }
  static bool equals(const P *p1, const P *p2) { return p1 == p2; }
  static bool null(const P *p) { return !p; }
  constexpr static T null() { return nullptr; }
};

template <typename T, bool IsCString>
struct ZuCmp_PrimitivePointer : public ZuCmp_Cannot<T> { };

template <typename T> struct ZuCmp_PrimitivePointer<T, false> :
  public ZuCmp_Pointer<T> { };

// non-string comparison

template <typename T, bool IsPrimitive, bool IsPointer> struct ZuCmp_NonString;

template <typename T> struct ZuCmp_NonString<T, false, false> :
  public ZuCmp_NonPrimitive<T, true> { };

template <typename T> struct ZuCmp_NonString<T, false, true> :
  public ZuCmp_Pointer<T> { };

template <typename T> struct ZuCmp_NonString<T, true, false> :
  public ZuCmp_Primitive<T, ZuTraits<T>::IsReal, ZuTraits<T>::IsVoid> { };

template <typename T> struct ZuCmp_NonString<T, true, true> :
  public ZuCmp_PrimitivePointer<T, ZuTraits<T>::IsCString> { };

// string comparison

template <typename T1, typename T2,
	  bool T1IsCStringAndT2IsCString,
	  bool T1IsString,
	  bool T1IsWStringAndT2IsWString> struct ZuCmp_StrCmp;

template <typename T1, typename T2, bool T1IsString>
struct ZuCmp_StrCmp<T1, T2, 1, T1IsString, 0> {
  static int cmp(const T1 &s1_, const T2 &s2_) {
    const char *s1 = ZuTraits<T1>::data(s1_);
    const char *s2 = ZuTraits<T2>::data(s2_);
    if (!s1) {
      if (!s2) return 0;
      s1 = "";
    } else {
      if (!s2) s2 = "";
    }
    return strcmp(s1, s2);
  }
  static bool less(const T1 &s1_, const T2 &s2_) {
    const char *s1 = ZuTraits<T1>::data(s1_);
    const char *s2 = ZuTraits<T2>::data(s2_);
    if (!s1) {
      if (!s2) return false;
      s1 = "";
    } else {
      if (!s2) return false;
    }
    return strcmp(s1, s2) < 0;
  }
  static bool equals(const T1 &s1_, const T2 &s2_) {
    const char *s1 = ZuTraits<T1>::data(s1_);
    const char *s2 = ZuTraits<T2>::data(s2_);
    if (!s1) {
      if (!s2) return true;
      s1 = "";
    } else {
      if (!s2) s2 = "";
    }
    return !strcmp(s1, s2);
  }
};
template <typename T1, typename T2>
struct ZuCmp_StrCmp<T1, T2, 0, 1, 0> {
  static int cmp(const T1 &s1_, const T2 &s2_) {
    const char *s1 = ZuTraits<T1>::data(s1_);
    const char *s2 = ZuTraits<T2>::data(s2_);
    if (!s1) return -!!s2;
    if (!s2) return 1;
    int l1 = ZuTraits<T1>::length(s1_), l2 = ZuTraits<T2>::length(s2_);
    if (int i = strncmp(s1, s2, l1 > l2 ? l2 : l1)) return i;
    return l1 - l2;
  }
  static bool less(const T1 &s1_, const T2 &s2_) {
    const char *s1 = ZuTraits<T1>::data(s1_);
    const char *s2 = ZuTraits<T2>::data(s2_);
    if (!s1) return !!s2;
    if (!s2) return false;
    int l1 = ZuTraits<T1>::length(s1_), l2 = ZuTraits<T2>::length(s2_);
    if (int i = strncmp(s1, s2, l1 > l2 ? l2 : l1)) return i < 0;
    return l1 < l2;
  }
  static bool equals(const T1 &s1_, const T2 &s2_) {
    const char *s1 = ZuTraits<T1>::data(s1_);
    const char *s2 = ZuTraits<T2>::data(s2_);
    if (!s1) return !s2;
    if (!s2) return false;
    int l1 = ZuTraits<T1>::length(s1_), l2 = ZuTraits<T2>::length(s2_);
    if (l1 != l2) return false;
    return !strncmp(s1, s2, l1);
  }
};
template <typename T1, typename T2, bool T1IsString>
struct ZuCmp_StrCmp<T1, T2, 1, T1IsString, 1> {
  static int cmp(const T1 &w1_, const T2 &w2_) {
    const wchar_t *w1 = ZuTraits<T1>::data(w1_);
    const wchar_t *w2 = ZuTraits<T2>::data(w2_);
    if (!w1) return -!!w2;
    if (!w2) return 1;
    return wcscmp(w1, w2);
  }
  static bool less(const T1 &w1_, const T2 &w2_) {
    const wchar_t *w1 = ZuTraits<T1>::data(w1_);
    const wchar_t *w2 = ZuTraits<T2>::data(w2_);
    if (!w1) return !!w2;
    if (!w2) return false;
    return wcscmp(w1, w2) < 0;
  }
  static bool equals(const T1 &w1_, const T2 &w2_) {
    const wchar_t *w1 = ZuTraits<T1>::data(w1_);
    const wchar_t *w2 = ZuTraits<T2>::data(w2_);
    if (!w1) return !w2;
    if (!w2) return false;
    return !wcscmp(w1, w2);
  }
};
template <typename T1, typename T2>
struct ZuCmp_StrCmp<T1, T2, 0, 1, 1> {
  static int cmp(const T1 &w1_, const T2 &w2_) {
    const wchar_t *w1 = ZuTraits<T1>::data(w1_);
    const wchar_t *w2 = ZuTraits<T2>::data(w2_);
    if (!w1) return -!!w2;
    if (!w2) return 1;
    int l1 = ZuTraits<T1>::length(w1_), l2 = ZuTraits<T2>::length(w2_);
    if (int i = wcsncmp(w1, w2, l1 > l2 ? l2 : l1)) return i;
    return l1 - l2;
  }
  static bool less(const T1 &w1_, const T2 &w2_) {
    const wchar_t *w1 = ZuTraits<T1>::data(w1_);
    const wchar_t *w2 = ZuTraits<T2>::data(w2_);
    if (!w1) return !!w2;
    if (!w2) return false;
    int l1 = ZuTraits<T1>::length(w1_), l2 = ZuTraits<T2>::length(w2_);
    if (int i = wcsncmp(w1, w2, l1 > l2 ? l2 : l1)) return i < 0;
    return l1 < l2;
  }
  static bool equals(const T1 &w1_, const T2 &w2_) {
    const wchar_t *w1 = ZuTraits<T1>::data(w1_);
    const wchar_t *w2 = ZuTraits<T2>::data(w2_);
    if (!w1) return !w2;
    if (!w2) return false;
    int l1 = ZuTraits<T1>::length(w1_), l2 = ZuTraits<T2>::length(w1_);
    if (l1 != l2) return false;
    return !wcsncmp(w1, w2, l1);
  }
};

template <typename T, bool IsCString, bool IsString, bool IsWString>
struct ZuCmp_String;

template <typename T, bool IsString>
struct ZuCmp_String<T, 1, IsString, 0> {
  template <typename S1, typename S2>
  static ZuIfT<
      ZuTraits<S1>::IsCString &&
      (ZuTraits<S2>::IsCString || ZuTraits<S2>::IsString) &&
      !ZuTraits<S2>::IsWString, int> cmp(const S1 &s1, const S2 &s2) {
    return ZuCmp_StrCmp<S1, S2,
			ZuTraits<S2>::IsCString,
			ZuTraits<S1>::IsString, 0>::cmp(s1, s2);
  }
  template <typename S1, typename S2>
  static ZuIfT<
      ZuTraits<S1>::IsCString &&
      (ZuTraits<S2>::IsCString || ZuTraits<S2>::IsString) &&
      !ZuTraits<S2>::IsWString, bool> less(const S1 &s1, const S2 &s2) {
    return ZuCmp_StrCmp<S1, S2,
			ZuTraits<S2>::IsCString,
			ZuTraits<S1>::IsString, 0>::less(s1, s2);
  }
  template <typename S1, typename S2>
  static ZuIfT<
      ZuTraits<S1>::IsCString &&
      (ZuTraits<S2>::IsCString || ZuTraits<S2>::IsString) &&
      !ZuTraits<S2>::IsWString, bool> equals(const S1 &s1, const S2 &s2) {
    return ZuCmp_StrCmp<S1, S2,
			ZuTraits<S2>::IsCString,
			ZuTraits<S1>::IsString, 0>::equals(s1, s2);
  }
  static bool null(const char *s) { return !s || !*s; }
  static const T &null() {
    static const T r = static_cast<const T>(static_cast<const char *>(nullptr));
    return r;
  }
};
template <typename T>
struct ZuCmp_String<T, 0, 1, 0> {
  template <typename S1, typename S2>
  static ZuIfT<
      ZuTraits<S1>::IsString &&
      (ZuTraits<S2>::IsCString || ZuTraits<S2>::IsString) &&
      !ZuTraits<S2>::IsWString, int> cmp(const S1 &s1, const S2 &s2) {
    return ZuCmp_StrCmp<S1, S2, 0, 1, 0>::cmp(s1, s2);
  }
  template <typename S1, typename S2>
  static ZuIfT<
      ZuTraits<S1>::IsString &&
      (ZuTraits<S2>::IsCString || ZuTraits<S2>::IsString) &&
      !ZuTraits<S2>::IsWString, bool> less(const S1 &s1, const S2 &s2) {
    return ZuCmp_StrCmp<S1, S2, 0, 1, 0>::less(s1, s2);
  }
  template <typename S1, typename S2>
  static ZuIfT<
      ZuTraits<S1>::IsString &&
      (ZuTraits<S2>::IsCString || ZuTraits<S2>::IsString) &&
      !ZuTraits<S2>::IsWString, bool> equals(const S1 &s1, const S2 &s2) {
    return ZuCmp_StrCmp<S1, S2, 0, 1, 0>::equals(s1, s2);
  }
  static bool null(const T &s) { return !s; }
  static const T &null() { static const T v; return v; }
};
template <typename T, bool IsString>
struct ZuCmp_String<T, 1, IsString, 1> {
  template <typename S1, typename S2>
  static ZuIfT<
      ZuTraits<S1>::IsCString &&
      (ZuTraits<S2>::IsCString || ZuTraits<S2>::IsString) &&
      ZuTraits<S2>::IsWString, int> cmp(const S1 &s1, const S2 &s2) {
    return ZuCmp_StrCmp<
      S1, S2,
      ZuTraits<S2>::IsCString,
      ZuTraits<S1>::IsString, 1>::cmp(s1, s2);
  }
  template <typename S1, typename S2>
  static ZuIfT<
      ZuTraits<S1>::IsCString &&
      (ZuTraits<S2>::IsCString || ZuTraits<S2>::IsString) &&
      ZuTraits<S2>::IsWString, bool> less(const S1 &s1, const S2 &s2) {
    return ZuCmp_StrCmp<
      S1, S2,
      ZuTraits<S2>::IsCString,
      ZuTraits<S1>::IsString, 1>::less(s1, s2);
  }
  template <typename S1, typename S2>
  static ZuIfT<
      ZuTraits<S1>::IsCString &&
      (ZuTraits<S2>::IsCString || ZuTraits<S2>::IsString) &&
      ZuTraits<S2>::IsWString, bool> equals(const S1 &s1, const S2 &s2) {
    return ZuCmp_StrCmp<
      S1, S2,
      ZuTraits<S2>::IsCString,
      ZuTraits<S1>::IsString, 1>::equals(s1, s2);
  }
  static bool null(const wchar_t *w) { return !w || !*w; }
  static const T &null() {
    static const T r =
      static_cast<const T>(static_cast<const wchar_t *>(nullptr));
    return r;
  }
};
template <typename T>
struct ZuCmp_String<T, 0, 1, 1> {
  template <typename S1, typename S2>
  static ZuIfT<
      ZuTraits<S1>::IsString &&
      (ZuTraits<S2>::IsCString || ZuTraits<S2>::IsString) &&
      ZuTraits<S2>::IsWString, int> cmp(const S1 &s1, const S2 &s2) {
    return ZuCmp_StrCmp<S1, S2, 0, 1, 1>::cmp(s1, s2);
  }
  template <typename S1, typename S2>
  static ZuIfT<
      ZuTraits<S1>::IsString &&
      (ZuTraits<S2>::IsCString || ZuTraits<S2>::IsString) &&
      ZuTraits<S2>::IsWString, bool> less(const S1 &s1, const S2 &s2) {
    return ZuCmp_StrCmp<S1, S2, 0, 1, 1>::less(s1, s2);
  }
  template <typename S1, typename S2>
  static ZuIfT<
      ZuTraits<S1>::IsString &&
      (ZuTraits<S2>::IsCString || ZuTraits<S2>::IsString) &&
      ZuTraits<S2>::IsWString, bool> equals(const S1 &s1, const S2 &s2) {
    return ZuCmp_StrCmp<S1, S2, 0, 1, 1>::equals(s1, s2);
  }
  static bool null(const T &s) { return !s; }
  static const T &null() { static const T v; return v; }
};

// generic comparison function

template <typename T> struct ZuCmp_<T, false> :
  public ZuCmp_NonString<T,
    ZuTraits<T>::IsPrimitive, ZuTraits<T>::IsPointer> { };

template <typename T> struct ZuCmp_<T, true> :
  public ZuCmp_String<T,
    ZuTraits<T>::IsCString, ZuTraits<T>::IsString, ZuTraits<T>::IsWString> { };

// comparison of bool

template <> struct ZuCmp_<bool> {
  static int cmp(bool b1, bool b2) { return (int)b1 - (int)b2; }
  static bool less(bool b1, bool b2) { return b1 < b2; }
  static bool equals(bool b1, bool b2) { return b1 == b2; }
  static bool null(bool b) { return !b; }
  constexpr static bool null() { return false; }
};

// comparison of char (null value should be '\0')

template <> struct ZuCmp_<char> {
  static int cmp(char c1, char c2) { return (int)c1 - (int)c2; }
  static bool less(char c1, char c2) { return c1 < c2; }
  static bool equals(char c1, char c2) { return c1 == c2; }
  static bool null(char c) { return !c; }
  constexpr static char null() { return 0; }
};

// comparison of void

template <> struct ZuCmp_<void> { static void null() { } };

// generic template

template <typename T> struct ZuCmp : public ZuCmp_<ZuDecay<T>> { };

// same as ZuCmp<T> but with 0 as the null value

template <typename T> struct ZuCmp0 : public ZuCmp<T> {
  using U = ZuDecay<T>;
  static bool null(U v) { return !v; }
  constexpr static U null() { return 0; }
};

// same as ZuCmp<T> but with -1 (or any negative value) as the null value

template <typename T> struct ZuCmp_1 : public ZuCmp<T> {
  using U = ZuDecay<T>;
  static bool null(U v) { return v < 0; }
  constexpr static U null() { return -1; }
};

// same as ZuCmp<T> but with N as the null value

template <typename T, ZuDecay<T> N> struct ZuCmpN : public ZuCmp<T> {
  using U = ZuDecay<T>;
  static bool null(U v) { return v == N; }
  constexpr static U null() { return N; }
};

// ZuNullRef<T>() returns a sentinel value that is guaranteed to be a const ref

template <typename T, typename Cmp, typename R = decltype(Cmp::null())>
struct ZuNullRef_ {
  static const T &null() noexcept {
    static const T null = Cmp::null();
    return null;
  }
};
template <typename T, typename Cmp>
struct ZuNullRef_<T, Cmp, const T &> {
  ZuInline static const T &null() noexcept { return Cmp::null(); }
};
template <typename T, typename Cmp = ZuCmp<T>>
ZuInline const T &ZuNullRef() noexcept { return ZuNullRef_<T, Cmp>::null(); }

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZuCmp_HPP */
