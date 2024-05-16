//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// ZuArray<T> is a wrapper around a pointer+length pair
// unlike std::array, prioritizes run-time optimization over compile-time
// unlike std::span, prioritizes expressiveness over readability and
//   intrusive integration with ZuHash/ZuCmp
//
// ZuArrayT<T> is a short cut for ZuArray<const typename ZuTraits<T>::Elem>

#ifndef ZuArray_HH
#define ZuArray_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <initializer_list>

#include <stdlib.h>
#include <string.h>

#include <zlib/ZuTraits.hh>
#include <zlib/ZuNull.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuHash.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuArrayFn.hh>
#include <zlib/ZuEquivChar.hh>

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

  ZuArray() : m_data{0}, m_length{0} { }
  ZuArray(const ZuArray &a) : m_data{a.m_data}, m_length{a.m_length} { }
  ZuArray &operator =(const ZuArray &a) {
    if (ZuLikely(this != &a)) {
      m_data = a.m_data;
      m_length = a.m_length;
    }
    return *this;
  }
  ZuArray(ZuArray &&a) : m_data{a.m_data}, m_length{a.m_length} { }
  ZuArray &operator =(ZuArray &&a) {
    m_data = a.m_data;
    m_length = a.m_length;
    return *this;
  }

  ZuArray(std::initializer_list<T> a) :
    m_data(const_cast<T *>(a.begin())), m_length(a.size()) { }
  ZuArray &operator =(std::initializer_list<T> a) {
    m_data = const_cast<T *>(a.begin());
    m_length = a.size();
    return *this;
  }

protected:
  // from string literal
  template <typename U, typename V = T>
  struct IsPrimitiveArray_ : public ZuBool<
      ZuTraits<U>::IsArray &&
      ZuTraits<U>::IsPrimitive &&
      ZuInspect<typename ZuTraits<U>::Elem, V>::Same> { };
  template <typename U, typename V = T>
  struct IsChar_ : public ZuBool<
      (ZuInspect<char, U>::Same || ZuInspect<wchar_t, U>::Same) &&
      ZuInspect<U, V>::Same> { };
  template <typename U>
  struct IsCharElem_ : public IsChar_<typename ZuTraits<U>::Elem> { };
  template <typename U, typename V = T>
  struct IsStrLiteral : public ZuBool<
      IsCharElem_<U>{} &&
      ZuIsExact<U, const V (&)[sizeof(U) / sizeof(V)]>{}> { };
  template <typename U, typename R = void>
  using MatchStrLiteral = ZuIfT<IsStrLiteral<U>{}, R>; 

  // from array of primitive types
  template <typename U> struct IsPrimitiveArray :
    public ZuBool<IsPrimitiveArray_<U>{} && !IsCharElem_<U>{}> { };
  template <typename U, typename R = void>
  using MatchPrimitiveArray = ZuIfT<IsPrimitiveArray<ZuDecay<U>>{}, R>; 

  // from C string (as a pointer, not a primitive array or literal)
  template <typename U> struct IsCString : public ZuBool<
      !IsStrLiteral<U>{} &&
      IsCharElem_<U>{} &&
      ZuTraits<U>::IsCString> { };
  template <typename U, typename R = void>
  using MatchCString = ZuIfT<IsCString<U>{}, R>; 

  // from other array (non-primitive, not a C string pointer)
  template <typename U, typename V = T> struct IsOtherArray : public ZuBool<
      !IsPrimitiveArray_<U>{} &&
      !IsCString<U>{} &&
      (ZuTraits<U>::IsArray || ZuTraits<U>::IsString) &&
      bool{ZuEquivChar<typename ZuTraits<U>::Elem, V>{}}> { };
  template <typename U, typename V = T> struct IsPtr : public ZuBool<
      ZuInspect<ZuNormChar<U> *, ZuNormChar<V> *>::Converts> { };
  template <typename U, typename R = void>
  using MatchOtherArray = ZuIfT<IsOtherArray<ZuDecay<U>>{}, R>; 

  // from pointer
  template <typename U, typename R = void>
  using MatchPtr = ZuIfT<IsPtr<ZuDecay<U>>{}, R>; 

public:
  // compile-time length from string literal (null-terminated)
  template <typename A>
  ZuArray(A &&a, MatchStrLiteral<A> *_ = nullptr) :
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
  ZuArray(A &&a, MatchCString<A> *_ = nullptr) :
      m_data(a), m_length(!a ? 0 : -1) { }
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
      m_length{!m_data ? 0 : static_cast<int>(ZuTraits<A>::length(a))} { }
  template <typename A>
  MatchOtherArray<A, ZuArray &> operator =(A &&a) {
    m_data = reinterpret_cast<T *>(ZuTraits<A>::data(a));
    m_length = !m_data ? 0 : static_cast<int>(ZuTraits<A>::length(a));
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
      bool{ZuEquivChar<V, char>{}} ||
      bool{ZuEquivChar<V, wchar_t>{}}, unsigned> length_() const {
    using Char = ZuNormChar<V>;
    if (ZuUnlikely(m_length < 0))
      return const_cast<ZuArray *>(this)->m_length =
	ZuTraits<const Char *>::length(
	    reinterpret_cast<const Char *>(m_data));
    return m_length;
  }
  template <typename V = T>
  ZuIfT<
    !bool{ZuEquivChar<V, char>{}} &&
    !bool{ZuEquivChar<V, wchar_t>{}}, unsigned> length_() const {
    return m_length;
  }

public:
  const T &operator [](int i) const { return m_data[i]; }
  T &operator [](int i) { return m_data[i]; }

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

private:
  bool equals_(const ZuArray &v) const {
    unsigned l = length();
    unsigned n = v.length();
    if (l != n) return false;
    return Ops::equals(data(), v.data(), l);
  }
public:
  bool equals(const ZuArray &v) const {
    if (this == &v) return true;
    return equals_(v);
  }
  template <typename V> bool equals(const V &v_) const {
    ZuArray v{v_};
    return equals_(v);
  }
private:
  int cmp_(const ZuArray &v) const {
    int l = length();
    int n = v.length();
    if (int i = Ops::cmp(data(), v.data(), l > n ? n : l)) return i;
    return ZuCmp<int>::cmp(l, n);
  }
public:
  template <typename V> int cmp(const V &v_) const {
    ZuArray v{v_};
    return cmp_(v);
  }
  int cmp(const ZuArray &v) const {
    if (this == &v) return 0;
    return cmp_(v);
  }

  template <typename L, typename R>
  friend inline
  ZuIfT<ZuInspect<ZuArray, L>::Is && ZuInspect<R, ZuArray>::Constructs, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline
  ZuIfT<ZuInspect<ZuArray, L>::Is && ZuInspect<R, ZuArray>::Constructs, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

  uint32_t hash() const { return ZuHash<ZuArray>::hash(*this); }

// iteration - all() is const by default, all<true>() is mutable
  template <bool Mutable = false, typename L>
  ZuIfT<!Mutable> all(L l) const {
    for (unsigned i = 0, n = length(); i < n; i++) l(m_data[i]);
  }
  template <bool Mutable, typename L>
  ZuIfT<Mutable> all(L l) {
    for (unsigned i = 0, n = length(); i < n; i++) l(m_data[i]);
  }

// STL cruft
  using iterator = T *;
  using const_iterator = const T *;
  const T *begin() const { return m_data; }
  const T *end() const { return m_data + length(); }
  const T *cbegin() const { return m_data; } // sigh
  const T *cend() const { return m_data + length(); }
  T *begin() { return m_data; }
  T *end() { return m_data + length(); }

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

  template <typename L> void all(L l) { }
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
      ZuInspect<typename ZuTraits<A>::Elem, ZuNull>::Constructs> *_ = nullptr)
    { }
  template <typename A>
  ZuIfT<
    ZuTraits<A>::IsArray &&
    ZuInspect<typename ZuTraits<A>::Elem, ZuNull>::Converts, ZuArray &>
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
      ZuInspect<typename ZuTraits<A>::Elem, void>::Constructs> *_ = nullptr) { }
  template <typename A>
  ZuIfT<
    ZuTraits<A>::IsArray &&
    ZuInspect<typename ZuTraits<A>::Elem, void>::Converts, ZuArray &>
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
      bool{ZuIsExact<char, ZuDecay<Elem>>{}} ||
      bool{ZuIsExact<wchar_t, ZuDecay<Elem>>{}},
    IsWString = bool{ZuIsExact<wchar_t, ZuDecay<Elem>>{}}
  };
  static Elem *data(T &a) { return a.data(); }
  static const Elem *data(const T &a) { return a.data(); }
  static unsigned length(const T &a) { return a.length(); }
};

template <typename T>
using ZuArrayT = ZuArray<const typename ZuTraits<T>::Elem>;

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZuArray_HH */
