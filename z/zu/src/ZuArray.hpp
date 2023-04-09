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

// ZuArray<T> is a wrapper around a pointer+length pair; unlike std::array
// focuses on run-time optimization, rather than compile-time
//
// ZuArrayT<T> is a short cut for ZuArray<const typename ZuTraits<T>::Elem>

#ifndef ZuArray_HPP
#define ZuArray_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <initializer_list>

#include <stdlib.h>
#include <string.h>

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuNull.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuHash.hpp>
#include <zlib/ZuConversion.hpp>
#include <zlib/ZuPrint.hpp>
#include <zlib/ZuArrayFn.hpp>
#include <zlib/ZuEquivChar.hpp>

#ifdef _MSC_VER
#pragma warning(push)
// #pragma warning(disable:4800)
#endif

template <typename T> struct ZuArray_ { };
template <> struct ZuArray_<char> {
  friend ZuPrintString ZuPrintType(ZuArray_ *);
};

template <typename T_, typename Cmp_ = ZuCmp<T_>>
class ZuArray : public ZuArray_<ZuStrip<T_>> {
public:
  using T = T_;
  using Cmp = Cmp_;
  using Elem = T;
  using Ops = ZuArrayFn<T, Cmp>;

  ZuArray() : m_data(0), m_length(0) { }
  ZuArray(const ZuArray &a) : m_data(a.m_data), m_length(a.m_length) { }
  ZuArray &operator =(const ZuArray &a) {
    if (ZuLikely(this != &a)) {
      m_data = a.m_data;
      m_length = a.m_length;
    }
    return *this;
  }
  ZuArray(ZuArray &&a) : m_data(a.m_data), m_length(a.m_length) { }
  ZuArray &operator =(ZuArray &&a) {
    m_data = a.m_data;
    m_length = a.m_length;
    return *this;
  }

  ZuArray(std::initializer_list<T> a) :
    m_data(a.begin()), m_length(a.size()) { }
  ZuArray &operator =(std::initializer_list<T> a) {
    m_data = a.begin();
    m_length = a.size();
    return *this;
  }

public:
  template <typename U> struct IsPrimitiveArray_ {
    using V = typename ZuTraits<U>::Elem;
    enum { OK = ZuConversion<V, T>::Same &&
      ZuTraits<U>::IsArray && ZuTraits<U>::IsPrimitive };
  };
  template <typename U> struct IsChar_ {
    enum { OK =
      ZuConversion<char, U>::Same ||
      ZuConversion<wchar_t, U>::Same
    };
  };
  template <typename U> struct IsCharElem_ {
    using V = typename ZuTraits<U>::Elem;
    enum { OK = IsChar_<V>::OK };
  };
  template <typename U> struct IsStrLiteral {
    enum { OK = IsPrimitiveArray_<U>::OK && IsCharElem_<U>::OK };
  };
  template <typename U> struct IsPrimitiveArray {
    enum { OK = IsPrimitiveArray_<U>::OK && !IsCharElem_<U>::OK };
  };
  template <typename U> struct IsCString {
    enum { OK = !IsPrimitiveArray_<U>::OK && ZuTraits<U>::IsPointer &&
      IsCharElem_<U>::OK };
  };
  template <typename U, typename V = T> struct IsOtherArray {
    enum { OK =
      !IsPrimitiveArray_<U>::OK && !IsCString<U>::OK &&
      (ZuTraits<U>::IsArray || ZuTraits<U>::IsString) &&
      ZuEquivChar<typename ZuTraits<U>::Elem, V>::Same };
  };
  template <typename U, typename V = T> struct IsPtr {
    using W = ZuNormChar<U>;
    using X = ZuNormChar<V>;
    enum { OK = ZuConversion<W *, X *>::Exists };
  };

  template <typename U, typename R = void>
  using MatchStrLiteral = ZuIfT<IsStrLiteral<ZuDecay<U>>::OK, R>; 
  template <typename U, typename R = void>
  using MatchPrimitiveArray = ZuIfT<IsPrimitiveArray<ZuDecay<U>>::OK, R>; 
  template <typename U, typename R = void>
  using MatchCString = ZuIfT<IsCString<ZuDecay<U>>::OK, R>; 
  template <typename U, typename R = void>
  using MatchOtherArray = ZuIfT<IsOtherArray<ZuDecay<U>>::OK, R>; 
  template <typename U, typename R = void>
  using MatchPtr = ZuIfT<IsPtr<ZuDecay<U>>::OK, R>; 

public:
  // compile-time length from string literal (null-terminated)
  template <typename A>
  ZuArray(const A &a, MatchStrLiteral<A> *_ = nullptr) :
    m_data(&a[0]),
    m_length((ZuUnlikely(!(sizeof(a) / sizeof(a[0])) || !a[0])) ? 0U :
      (sizeof(a) / sizeof(a[0])) - 1U) { }
  template <typename A>
  MatchStrLiteral<A, ZuArray &> operator =(A &&a) {
    m_data = &a[0];
    m_length = (ZuUnlikely(!(sizeof(a) / sizeof(a[0])) || !a[0])) ? 0U :
      (sizeof(a) / sizeof(a[0])) - 1U;
    return *this;
  }

  // compile-time length from primitive array
  template <typename A>
  ZuArray(const A &a, MatchPrimitiveArray<A> *_ = nullptr) :
    m_data(&a[0]),
    m_length(sizeof(a) / sizeof(a[0])) { }
  template <typename A>
  MatchPrimitiveArray<A, ZuArray &> operator =(A &&a) {
    m_data = &a[0];
    m_length = sizeof(a) / sizeof(a[0]);
    return *this;
  }

  // length from deferred strlen
  template <typename A>
  ZuArray(const A &a, MatchCString<A> *_ = nullptr) :
	m_data(reinterpret_cast<T *>(a)), m_length(!a ? 0 : -1) { }
  template <typename A>
  MatchCString<A, ZuArray &> operator =(A &&a) {
    m_data = a;
    m_length = !a ? 0 : -1;
    return *this;
  }

  // length from passed type
  template <typename A>
  ZuArray(A &&a, MatchOtherArray<A> *_ = nullptr) :
      m_data{reinterpret_cast<T *>(ZuTraits<A>::data(a))},
      m_length{!m_data ? 0 : (int)ZuTraits<A>::length(a)} { }
  template <typename A>
  MatchOtherArray<A, ZuArray &> operator =(A &&a) {
    m_data = reinterpret_cast<T *>(ZuTraits<A>::data(a));
    m_length = !m_data ? 0 : (int)ZuTraits<A>::length(a);
    return *this;
  }


#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnarrowing"
#endif
  template <typename V>
  ZuArray(V *data, unsigned length, MatchPtr<V> *_ = nullptr) :
      m_data{reinterpret_cast<T *>(data)}, m_length{length} { }
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

  const T *data() const { return m_data; }
  T *data() { return m_data; }

  unsigned length() const { return length_(); }

private:
  template <typename V = T>
  ZuIfT<
      ZuEquivChar<V, char>::Same ||
      ZuEquivChar<V, wchar_t>::Same, unsigned> length_() const {
    using Char = ZuNormChar<V>;
    if (ZuUnlikely(m_length < 0))
      return const_cast<ZuArray *>(this)->m_length =
	ZuTraits<const Char *>::length(
	    reinterpret_cast<const Char *>(m_data));
    return m_length;
  }
  template <typename V = T>
  ZuIfT<
    !ZuEquivChar<V, char>::Same &&
    !ZuEquivChar<V, wchar_t>::Same, unsigned> length_() const {
    return m_length;
  }

public:
  const T &operator [](int i) const { return m_data[i]; }
  T &operator [](int i) { return m_data[i]; }

  const T *begin() const { return m_data; }
  const T *end() const { return m_data + length(); }
  const T *cbegin() const { return m_data; } // sigh
  const T *cend() const { return m_data + length(); }
  T *begin() { return m_data; }
  T *end() { return m_data + length(); }

  bool operator !() const { return !length(); }
  ZuOpBool

  void offset(unsigned n) {
    if (ZuUnlikely(!n)) return;
    if (ZuLikely(n < length()))
      m_data += n, m_length -= n;
    else
      m_data = nullptr, m_length = 0;
  }

  void trunc(unsigned n) {
    if (ZuLikely(n < length())) {
      if (ZuLikely(n))
	m_length = n;
      else
	m_data = nullptr, m_length = 0;
    }
  }

protected:
  bool same(const ZuArray &v) const { return this == &v; }
  template <typename V> bool same(const V &v) const { return false; }

public:
  template <typename V> bool equals(const V &v_) const {
    if (same(v_)) return true;
    ZuArray v{v_};
    unsigned l = length();
    unsigned n = v.length();
    if (l != n) return false;
    return Ops::equals(data(), v.data(), l);
  }
  template <typename V> int cmp(const V &v_) const {
    if (same(v_)) return 0;
    ZuArray v{v_};
    int l = length();
    int n = v.length();
    if (int i = Ops::cmp(data(), v.data(), l > n ? n : l)) return i;
    return ZuCmp<int>::cmp(l, n);
  }
  template <typename L, typename R>
  friend inline ZuIfT<ZuConversion<ZuArray, L>::Is, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline ZuIfT<ZuConversion<ZuArray, L>::Is, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

  uint32_t hash() const { return ZuHash<ZuArray>::hash(*this); }

private:
  T	*m_data;
  int	m_length;	// -ve used to defer calculation 
};

template <typename T> class ZuArray_Null {
  const T *data() const { return nullptr; }
  unsigned length() const { return 0; }

  T operator [](int i) const { return ZuCmp<T>::null(); }

  bool operator !() const { return true; }

  void offset(unsigned n) { }
};

template <typename Cmp>
class ZuArray<ZuNull, Cmp> : public ZuArray_Null<ZuNull> {
public:
  using Elem = ZuNull;

  ZuArray() { }
  ZuArray(const ZuArray &a) { }
  ZuArray &operator =(const ZuArray &a) { return *this; }

  template <typename A>
  ZuArray(const A &a, ZuIfT<
      ZuTraits<A>::IsArray &&
      ZuConversion<typename ZuTraits<A>::Elem, ZuNull>::Exists> *_ = nullptr)
    { }
  template <typename A>
  ZuIfT<
    ZuTraits<A>::IsArray &&
    ZuConversion<typename ZuTraits<A>::Elem, ZuNull>::Exists, ZuArray &>
  operator =(const A &a) { return *this; }

  ZuArray(const ZuNull *data, unsigned length) { }
};

template <typename Cmp>
class ZuArray<void, Cmp> : public ZuArray_Null<void> {
public:
  using Elem = void;

  ZuArray() { }
  ZuArray(const ZuArray &a) { }
  ZuArray &operator =(const ZuArray &a) { return *this; }

  template <typename A>
  ZuArray(const A &a, ZuIfT<
      ZuTraits<A>::IsArray &&
      ZuConversion<typename ZuTraits<A>::Elem, void>::Exists> *_ = nullptr) { }
  template <typename A>
  ZuIfT<
    ZuTraits<A>::IsArray &&
    ZuConversion<typename ZuTraits<A>::Elem, void>::Exists, ZuArray &>
  operator =(const A &a) { return *this; }

  ZuArray(const void *data, unsigned length) { }
};

template <typename T, typename N>
ZuArray(T *data, N length) -> ZuArray<T>;

template <typename Elem_>
struct ZuTraits<ZuArray<Elem_> > : public ZuBaseTraits<ZuArray<Elem_> > {
  using Elem = Elem_;
  using T = ZuArray<Elem>;
  enum {
    IsArray = 1, IsPrimitive = 0,
    IsString =
      ZuConversion<char, Elem>::Same ||
      ZuConversion<wchar_t, Elem>::Same,
    IsWString = ZuConversion<wchar_t, Elem>::Same
  };
  template <typename U = T>
  static ZuNotConst<U, Elem *> data(U &a) { return a.data(); }
  static const Elem *data(const T &a) { return a.data(); }
  static unsigned length(const T &a) { return a.length(); }
};

template <typename T>
using ZuArrayT = ZuArray<const typename ZuTraits<T>::Elem>;

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZuArray_HPP */
