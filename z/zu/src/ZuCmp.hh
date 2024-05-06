//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// generic three-way and two-way comparison 
// (including distinguished sentinel null values)

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
//   // returns -ve if l < r, 0 if l == r, +ve if l > r
//   template <typename L, typename R>
//   constexpr static int cmp(const L &l, const R &r) { ... }
//   // returns l == r
//   template <typename L, typename R>
//   constexpr static bool equals(const L &l, const R &r) { ... }
//   // returns l < r
//   template <typename L, typename R>
//   constexpr static bool less(const L &l, const R &r) { ... }
//   // returns !t
//   constexpr static bool null(const UDT &v) { ... }
//   // returns distinguished "null" value for UDT
//   constexpr static const UDT &null() { static UDT udt = ...; return udt; }
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

#ifndef ZuCmp_HH
#define ZuCmp_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <string.h>
#include <wchar.h>

#include <zlib/ZuTraits.hh>
#include <zlib/ZuInt.hh>

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
  constexpr static int delta(T l, T r) {
    if (l == r) return 0;
    if (l > r) {
      l -= r;
      l >>= DeltaShift;
      int delta = l;
      return delta | 1;
    }
    r -= l;
    r >>= DeltaShift;
    int delta = r;
    return -(delta | 1);
  }
  constexpr static int cmp(T l, T r) { return (l > r) - (l < r); }
  constexpr static bool less(T l, T r) { return l < r; }
  constexpr static bool equals(T l, T r) { return l == r; }
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
  constexpr static int delta(T l, T r) {
    return static_cast<int>(l) - static_cast<int>(r);
  }
  constexpr static int cmp(T l, T r) {
    return static_cast<int>(l) - static_cast<int>(r);
  }
  constexpr static bool less(T l, T r) { return l < r; }
  constexpr static bool equals(T l, T r) { return l == r; }
  constexpr static bool null(T i) { return i == null(); }
  constexpr static T epsilon(T) { return 0; }
};

// comparison of floating point types

template <typename T> struct ZuCmp_Floating {
  constexpr static int cmp(T l, T r) { return (l > r) - (l < r); }
  constexpr static bool less(T l, T r) { return l < r; }
  constexpr static bool equals(T l, T r) { return l == r; }
  constexpr static T null() { return ZuTraits<T>::nan(); }
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

// default comparison of non-primitive types - uses operators <=>, >, <, ==, !

// test for <=>
template <typename, typename, typename = void>
struct ZuCmp_Can_starship : public ZuFalse { };
template <typename P1, typename P2>
struct ZuCmp_Can_starship<P1, P2,
  decltype((ZuDeclVal<const P1 &>() <=> ZuDeclVal<const P2 &>()), void())> :
    public ZuTrue { };

// test for cmp()
template <typename> struct ZuCmp_Can_cmp_;
template <> struct ZuCmp_Can_cmp_<int> { using T = void; }; 
template <typename, typename, typename = void>
struct ZuCmp_Can_cmp : public ZuFalse { };
template <typename P1, typename P2>
struct ZuCmp_Can_cmp<P1, P2, typename ZuCmp_Can_cmp_<
  decltype(ZuDeclVal<const P1 &>().cmp(ZuDeclVal<const P2 &>()))>::T> :
    public ZuTrue { };

// test for copy-constructible default constructor
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

template <typename T> struct ZuCmp_NonPrimitive : public ZuCmp_NullFn<T> {
  // prefer cmp() to operator <=>()
  template <typename L, typename R>
  static constexpr ZuIfT<ZuCmp_Can_cmp<L, R>{}, int>
  cmp(const L &l, const R &r) { return l.cmp(r); }
  template <typename L, typename R>
  static constexpr ZuIfT<
    ZuCmp_Can_starship<L, R>{} &&
    !ZuCmp_Can_cmp<L, R>{}, int>
  cmp(const L &l, const R &r) {
    auto v = l <=> r;
    return (v > 0) - (v < 0); // bah
  }
  template <typename L, typename R>
  static constexpr ZuIfT<
    !ZuCmp_Can_cmp<L, R>{} &&
    !ZuCmp_Can_starship<L, R>{}, int>
  cmp(const L &l, const R &r) { return (l > r) - (l < r); }
  template <typename L, typename R>
  static constexpr bool less(const L &l, const R &r) { return l < r; }
  template <typename L, typename R>
  static constexpr bool equals(const L &l, const R &r) { return l == r; }
};

// comparison of pointers (including smart pointers)

template <typename T> struct ZuCmp_Pointer {
  using P = typename ZuTraits<T>::Elem;
  static int cmp(const P *l_, const P *r_) {
    auto l = reinterpret_cast<const uint8_t *>(l_);
    auto r = reinterpret_cast<const uint8_t *>(r_);
    return (l > r) - (l < r);
  }
  static bool less(const P *l, const P *r) { return l < r; }
  static bool equals(const P *l, const P *r) { return l == r; }
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
  public ZuCmp_NonPrimitive<T> { };

template <typename T> struct ZuCmp_NonString<T, false, true> :
  public ZuCmp_Pointer<T> { };

template <typename T> struct ZuCmp_NonString<T, true, false> :
  public ZuCmp_Primitive<T, ZuTraits<T>::IsReal, ZuTraits<T>::IsVoid> { };

template <typename T> struct ZuCmp_NonString<T, true, true> :
  public ZuCmp_PrimitivePointer<T, ZuTraits<T>::IsCString> { };

// string comparison

// implementation goal is to leverage the hand-optimized assembly code in libc:
// * concentrate run-time code into denser hotspots, minimizing cache eviction
// * funnel pairs of null-terminated C strings into good ole' strcmp()
// * funnel pairs of null-terminated wchar_t strings into wcscmp()
// * funnel everything else into strncmp or wcsncmp

template <
  typename T1, typename T2,
  bool T1IsCStringAndT2IsCString,
  bool T1IsString,
  bool T1IsWStringAndT2IsWString>
struct ZuCmp_StrCmp;

template <typename L, typename R, bool LIsString>
struct ZuCmp_StrCmp<L, R, 1, LIsString, 0> {
  static int cmp(const L &l_, const R &r_) {
    const char *l = ZuTraits<L>::data(l_);
    const char *r = ZuTraits<R>::data(r_);
    if (!l) {
      if (!r) return 0;
      l = "";
    } else {
      if (!r) r = "";
    }
    return strcmp(l, r);
  }
  static bool less(const L &l_, const R &r_) {
    const char *l = ZuTraits<L>::data(l_);
    const char *r = ZuTraits<R>::data(r_);
    if (!l) {
      if (!r) return false;
      l = "";
    } else {
      if (!r) return false;
    }
    return strcmp(l, r) < 0;
  }
  static bool equals(const L &l_, const R &r_) {
    const char *l = ZuTraits<L>::data(l_);
    const char *r = ZuTraits<R>::data(r_);
    if (!l) {
      if (!r) return true;
      l = "";
    } else {
      if (!r) r = "";
    }
    return !strcmp(l, r);
  }
};
template <typename L, typename R>
struct ZuCmp_StrCmp<L, R, 0, 1, 0> {
  static int cmp(const L &l_, const R &r_) {
    const char *l = ZuTraits<L>::data(l_);
    const char *r = ZuTraits<R>::data(r_);
    if (!l) return -!!r;
    if (!r) return 1;
    int ln = ZuTraits<L>::length(l_), rn = ZuTraits<R>::length(r_);
    if (int i = strncmp(l, r, ln > rn ? rn : ln)) return i;
    return ln - rn;
  }
  static bool less(const L &l_, const R &r_) {
    const char *l = ZuTraits<L>::data(l_);
    const char *r = ZuTraits<R>::data(r_);
    if (!l) return !!r;
    if (!r) return false;
    int ln = ZuTraits<L>::length(l_), rn = ZuTraits<R>::length(r_);
    if (int i = strncmp(l, r, ln > rn ? rn : ln)) return i < 0;
    return ln < rn;
  }
  static bool equals(const L &l_, const R &r_) {
    const char *l = ZuTraits<L>::data(l_);
    const char *r = ZuTraits<R>::data(r_);
    if (!l) return !r;
    if (!r) return false;
    int ln = ZuTraits<L>::length(l_), rn = ZuTraits<R>::length(r_);
    if (ln != rn) return false;
    return !strncmp(l, r, ln);
  }
};
template <typename L, typename R, bool LIsString>
struct ZuCmp_StrCmp<L, R, 1, LIsString, 1> {
  static int cmp(const L &l_, const R &r_) {
    const wchar_t *l = ZuTraits<L>::data(l_);
    const wchar_t *r = ZuTraits<R>::data(r_);
    if (!l) return -!!r;
    if (!r) return 1;
    return wcscmp(l, r);
  }
  static bool less(const L &l_, const R &r_) {
    const wchar_t *l = ZuTraits<L>::data(l_);
    const wchar_t *r = ZuTraits<R>::data(r_);
    if (!l) return !!r;
    if (!r) return false;
    return wcscmp(l, r) < 0;
  }
  static bool equals(const L &l_, const R &r_) {
    const wchar_t *l = ZuTraits<L>::data(l_);
    const wchar_t *r = ZuTraits<R>::data(r_);
    if (!l) return !r;
    if (!r) return false;
    return !wcscmp(l, r);
  }
};
template <typename L, typename R>
struct ZuCmp_StrCmp<L, R, 0, 1, 1> {
  static int cmp(const L &l_, const R &r_) {
    const wchar_t *l = ZuTraits<L>::data(l_);
    const wchar_t *r = ZuTraits<R>::data(r_);
    if (!l) return -!!r;
    if (!r) return 1;
    int ln = ZuTraits<L>::length(l_), rn = ZuTraits<R>::length(r_);
    if (int i = wcsncmp(l, r, ln > rn ? rn : ln)) return i;
    return ln - rn;
  }
  static bool less(const L &l_, const R &r_) {
    const wchar_t *l = ZuTraits<L>::data(l_);
    const wchar_t *r = ZuTraits<R>::data(r_);
    if (!l) return !!r;
    if (!r) return false;
    int ln = ZuTraits<L>::length(l_), rn = ZuTraits<R>::length(r_);
    if (int i = wcsncmp(l, r, ln > rn ? rn : ln)) return i < 0;
    return ln < rn;
  }
  static bool equals(const L &l_, const R &r_) {
    const wchar_t *l = ZuTraits<L>::data(l_);
    const wchar_t *r = ZuTraits<R>::data(r_);
    if (!l) return !r;
    if (!r) return false;
    int ln = ZuTraits<L>::length(l_), rn = ZuTraits<R>::length(l_);
    if (ln != rn) return false;
    return !wcsncmp(l, r, ln);
  }
};

template <typename T, bool IsCString, bool IsString, bool IsWString>
struct ZuCmp_String;

template <typename T, bool IsString>
struct ZuCmp_String<T, 1, IsString, 0> {
  template <typename L, typename R>
  static ZuIfT<
      ZuTraits<L>::IsCString &&
      (ZuTraits<R>::IsCString || ZuTraits<R>::IsString) &&
      !ZuTraits<R>::IsWString, int> cmp(const L &l, const R &r) {
    return ZuCmp_StrCmp<
      L, R,
      ZuTraits<R>::IsCString,
      ZuTraits<L>::IsString, 0>::cmp(l, r);
  }
  template <typename L, typename R>
  static ZuIfT<
      ZuTraits<L>::IsCString &&
      (ZuTraits<R>::IsCString || ZuTraits<R>::IsString) &&
      !ZuTraits<R>::IsWString, bool> less(const L &l, const R &r) {
    return ZuCmp_StrCmp<
      L, R,
      ZuTraits<R>::IsCString,
      ZuTraits<L>::IsString, 0>::less(l, r);
  }
  template <typename L, typename R>
  static ZuIfT<
      ZuTraits<L>::IsCString &&
      (ZuTraits<R>::IsCString || ZuTraits<R>::IsString) &&
      !ZuTraits<R>::IsWString, bool> equals(const L &l, const R &r) {
    return ZuCmp_StrCmp<
      L, R,
      ZuTraits<R>::IsCString,
      ZuTraits<L>::IsString, 0>::equals(l, r);
  }
  static bool null(const char *s) { return !s || !*s; }
  static const T &null() {
    static const T r = static_cast<const T>(static_cast<const char *>(nullptr));
    return r;
  }
};
template <typename T>
struct ZuCmp_String<T, 0, 1, 0> {
  template <typename L, typename R>
  static ZuIfT<
      ZuTraits<L>::IsString &&
      (ZuTraits<R>::IsCString || ZuTraits<R>::IsString) &&
      !ZuTraits<R>::IsWString, int> cmp(const L &l, const R &r) {
    return ZuCmp_StrCmp<L, R, 0, 1, 0>::cmp(l, r);
  }
  template <typename L, typename R>
  static ZuIfT<
      ZuTraits<L>::IsString &&
      (ZuTraits<R>::IsCString || ZuTraits<R>::IsString) &&
      !ZuTraits<R>::IsWString, bool> less(const L &l, const R &r) {
    return ZuCmp_StrCmp<L, R, 0, 1, 0>::less(l, r);
  }
  template <typename L, typename R>
  static ZuIfT<
      ZuTraits<L>::IsString &&
      (ZuTraits<R>::IsCString || ZuTraits<R>::IsString) &&
      !ZuTraits<R>::IsWString, bool> equals(const L &l, const R &r) {
    return ZuCmp_StrCmp<L, R, 0, 1, 0>::equals(l, r);
  }
  static bool null(const T &s) { return !s; }
  static const T &null() { static const T v; return v; }
};
template <typename T, bool IsString>
struct ZuCmp_String<T, 1, IsString, 1> {
  template <typename L, typename R>
  static ZuIfT<
      ZuTraits<L>::IsCString &&
      (ZuTraits<R>::IsCString || ZuTraits<R>::IsString) &&
      ZuTraits<R>::IsWString, int> cmp(const L &l, const R &r) {
    return ZuCmp_StrCmp<
      L, R,
      ZuTraits<R>::IsCString,
      ZuTraits<L>::IsString, 1>::cmp(l, r);
  }
  template <typename L, typename R>
  static ZuIfT<
      ZuTraits<L>::IsCString &&
      (ZuTraits<R>::IsCString || ZuTraits<R>::IsString) &&
      ZuTraits<R>::IsWString, bool> less(const L &l, const R &r) {
    return ZuCmp_StrCmp<
      L, R,
      ZuTraits<R>::IsCString,
      ZuTraits<L>::IsString, 1>::less(l, r);
  }
  template <typename L, typename R>
  static ZuIfT<
      ZuTraits<L>::IsCString &&
      (ZuTraits<R>::IsCString || ZuTraits<R>::IsString) &&
      ZuTraits<R>::IsWString, bool> equals(const L &l, const R &r) {
    return ZuCmp_StrCmp<
      L, R,
      ZuTraits<R>::IsCString,
      ZuTraits<L>::IsString, 1>::equals(l, r);
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
  template <typename L, typename R>
  static ZuIfT<
      ZuTraits<L>::IsString &&
      (ZuTraits<R>::IsCString || ZuTraits<R>::IsString) &&
      ZuTraits<R>::IsWString, int> cmp(const L &l, const R &r) {
    return ZuCmp_StrCmp<L, R, 0, 1, 1>::cmp(l, r);
  }
  template <typename L, typename R>
  static ZuIfT<
      ZuTraits<L>::IsString &&
      (ZuTraits<R>::IsCString || ZuTraits<R>::IsString) &&
      ZuTraits<R>::IsWString, bool> less(const L &l, const R &r) {
    return ZuCmp_StrCmp<L, R, 0, 1, 1>::less(l, r);
  }
  template <typename L, typename R>
  static ZuIfT<
      ZuTraits<L>::IsString &&
      (ZuTraits<R>::IsCString || ZuTraits<R>::IsString) &&
      ZuTraits<R>::IsWString, bool> equals(const L &l, const R &r) {
    return ZuCmp_StrCmp<L, R, 0, 1, 1>::equals(l, r);
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

// ZuNullRef<T>() returns a const ref sentinel value

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

#endif /* ZuCmp_HH */
