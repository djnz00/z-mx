//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// ZuSpan<T> is a wrapper around a pointer+length pair
// unlike std::array, prioritizes run-time optimization over compile-time
// unlike std::span, prioritizes:
// - expressiveness over readability
// - intrusive integration with ZuHash/ZuCmp

#ifndef ZuSpan_HH
#define ZuSpan_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
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
#include <zlib/ZuEquiv.hh>

#ifdef _MSC_VER
#pragma warning(push)
// #pragma warning(disable:4800)
#endif

template <typename T> struct ZuSpan_ { };
template <> struct ZuSpan_<char> {
  friend ZuPrintString ZuPrintType(ZuSpan_ *);
};

template <typename T_, typename Cmp_ = ZuCmp<T_>>
class ZuSpan : public ZuSpan_<ZuStrip<T_>> {
template <typename, typename> friend class ZuSpan;

public:
  using T = T_;
  using Cmp = Cmp_;
  using Elem = T;
  using Ops = ZuArrayFn<T, Cmp>;

  ZuSpan() : m_data{0}, m_length{0} { }
  ZuSpan(const ZuSpan &a) : m_data{a.m_data}, m_length{a.m_length} { }
  ZuSpan &operator =(const ZuSpan &a) {
    if (ZuLikely(this != &a)) {
      m_data = a.m_data;
      m_length = a.m_length;
    }
    return *this;
  }
  ZuSpan(ZuSpan &&a) : m_data{a.m_data}, m_length{a.m_length} { }
  ZuSpan &operator =(ZuSpan &&a) {
    m_data = a.m_data;
    m_length = a.m_length;
    return *this;
  }

#if defined(__GNUC__) && !defined(__llvm__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winit-list-lifetime"
#endif
  ZuSpan(std::initializer_list<T> a) :
    m_data(const_cast<T *>(a.begin())), m_length(a.size()) { }
  ZuSpan &operator =(std::initializer_list<T> a) {
    m_data = const_cast<T *>(a.begin());
    m_length = a.size();
    return *this;
  }
#if defined(__GNUC__) && !defined(__llvm__)
#pragma GCC diagnostic pop
#endif

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
      ZuTraits<U>::IsCString &&
      // ZuTraits<U>::IsPointer &&
      ZuTraits<U>::IsPrimitive> { };
  template <typename U, typename R = void>
  using MatchCString = ZuIfT<IsCString<U>{}, R>; 

  // from equivalent ZuSpan
  template <typename U, typename V = T>
  struct IsZuArray : public ZuBool<
    bool(ZuIsExact<ZuSpan<typename ZuTraits<U>::Elem>, U>{}) &&
    bool{ZuEquiv<typename ZuTraits<U>::Elem, V>{}}> { };
  template <typename U, typename R = void>
  using MatchZuArray = ZuIfT<IsZuArray<ZuDecay<U>>{}, R>; 

  // from other array (non-primitive, not a C string pointer)
  template <typename U, typename V = T>
  struct IsOtherArray : public ZuBool<
    !ZuIsExact<ZuSpan<typename ZuTraits<U>::Elem>, U>{} &&
    !IsPrimitiveArray_<U>{} &&
    !IsCString<U>{} &&
    (ZuTraits<U>::IsSpan || ZuTraits<U>::IsString) &&
    bool{ZuEquiv<typename ZuTraits<U>::Elem, V>{}}> { };
  template <typename U, typename R = void>
  using MatchOtherArray = ZuIfT<IsOtherArray<ZuDecay<U>>{}, R>; 

  // from pointer
  template <typename U, typename V = T>
  struct IsPtr : public ZuBool<
      ZuInspect<ZuNormChar<U> *, ZuNormChar<V> *>::Converts> { };
  template <typename U, typename R = void>
  using MatchPtr = ZuIfT<IsPtr<ZuDecay<U>>{}, R>; 

public:
  // compile-time length from string literal (null-terminated)
  template <typename A, decltype(MatchStrLiteral<A>(), int()) = 0>
  ZuSpan(A &&a) :
    m_data(&a[0]),
    m_length((ZuUnlikely(!(sizeof(a) / sizeof(a[0])) || !a[0])) ? 0U :
      (sizeof(a) / sizeof(a[0])) - 1U) { }
  template <typename A>
  MatchStrLiteral<A, ZuSpan &> operator =(A &&a) {
    m_data = &a[0];
    m_length = (ZuUnlikely(!(sizeof(a) / sizeof(a[0])) || !a[0])) ? 0U :
      (sizeof(a) / sizeof(a[0])) - 1U;
    return *this;
  }

  // compile-time length from primitive array
  template <typename A, decltype(MatchPrimitiveArray<A>(), int()) = 0>
  ZuSpan(const A &a) :
    m_data(&a[0]),
    m_length(sizeof(a) / sizeof(a[0])) { }
  template <typename A>
  MatchPrimitiveArray<A, ZuSpan &> operator =(A &&a) {
    m_data = &a[0];
    m_length = sizeof(a) / sizeof(a[0]);
    return *this;
  }

  // length from deferred strlen
#if defined(__GNUC__) && !defined(__llvm__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Waddress"
#pragma GCC diagnostic ignored "-Wnonnull"
#pragma GCC diagnostic ignored "-Wnonnull-compare"
#endif
  template <typename A, decltype(MatchCString<A>(), int()) = 0>
  ZuSpan(A &&a) :
    m_data(a), m_length(!a ? 0 : -1) { }
#if defined(__GNUC__) && !defined(__llvm__)
#pragma GCC diagnostic pop
#endif
  template <typename A>
  MatchCString<A, ZuSpan &> operator =(A &&a) {
    m_data = a;
    m_length = !a ? 0 : -1;
    return *this;
  }

  // from equivalent ZuSpan
  template <typename A, decltype(MatchZuArray<A>(), int()) = 0>
  ZuSpan(A &&a) :
      m_data{reinterpret_cast<T *>(a.m_data)},
      m_length{a.m_length} { }
  template <typename A>
  MatchZuArray<A, ZuSpan &> operator =(A &&a) {
    m_data = reinterpret_cast<T *>(a.m_data);
    m_length = a.m_length;
    return *this;
  }

  // from other array
  template <typename A, decltype(MatchOtherArray<A>(), int()) = 0>
  ZuSpan(A &&a) :
      m_data{reinterpret_cast<T *>(ZuTraits<A>::data(a))},
      m_length{!m_data ? 0 : static_cast<int>(ZuTraits<A>::length(a))} { }
  template <typename A>
  MatchOtherArray<A, ZuSpan &> operator =(A &&a) {
    m_data = reinterpret_cast<T *>(ZuTraits<A>::data(a));
    m_length = !m_data ? 0 : static_cast<int>(ZuTraits<A>::length(a));
    return *this;
  }

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnarrowing"
#endif
  template <typename V, decltype(MatchPtr<V>(), int()) = 0>
  ZuSpan(V *data, unsigned length) :
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
      bool{ZuEquiv<V, char>{}} ||
      bool{ZuEquiv<V, wchar_t>{}}, unsigned> length_() const {
    using Char = ZuNormChar<V>;
    if (ZuUnlikely(m_length < 0))
      return const_cast<ZuSpan *>(this)->m_length =
	ZuTraits<const Char *>::length(
	    reinterpret_cast<const Char *>(m_data));
    return m_length;
  }
  template <typename V = T>
  ZuIfT<
    !bool{ZuEquiv<V, char>{}} &&
    !bool{ZuEquiv<V, wchar_t>{}}, unsigned> length_() const {
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
  bool equals_(const ZuSpan &v) const {
    unsigned l = length();
    unsigned n = v.length();
    if (l != n) return false;
    return Ops::equals(data(), v.data(), l);
  }
public:
  bool equals(const ZuSpan &v) const {
    if (this == &v) return true;
    return equals_(v);
  }
  template <typename V> bool equals(const V &v_) const {
    ZuSpan v{v_};
    return equals_(v);
  }
private:
  int cmp_(const ZuSpan &v) const {
    int l = length();
    int n = v.length();
    if (int i = Ops::cmp(data(), v.data(), l < n ? l : n)) return i;
    return ZuCmp<int>::cmp(l, n);
  }
public:
  template <typename V> int cmp(const V &v_) const {
    ZuSpan v{v_};
    return cmp_(v);
  }
  int cmp(const ZuSpan &v) const {
    if (this == &v) return 0;
    return cmp_(v);
  }

  template <typename L, typename R>
  friend inline
  ZuIfT<ZuInspect<ZuSpan, L>::Is && ZuInspect<R, ZuSpan>::Constructs, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline
  ZuIfT<ZuInspect<ZuSpan, L>::Is && ZuInspect<R, ZuSpan>::Constructs, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

  uint32_t hash() const { return ZuHash<ZuSpan>::hash(*this); }

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

template <typename T> class ZuSpan_Null {
  const T *data() const { return nullptr; }
  unsigned length() const { return 0; }

  T operator [](int i) const { return ZuCmp<T>::null(); }

  bool operator !() const { return true; }

  void offset(unsigned n) { }

  template <typename L> void all(L l) { }
};

template <typename Cmp>
class ZuSpan<ZuNull, Cmp> : public ZuSpan_Null<ZuNull> {
public:
  using Elem = ZuNull;

  ZuSpan() { }
  ZuSpan(const ZuSpan &a) { }
  ZuSpan &operator =(const ZuSpan &a) { return *this; }

  template <typename A, decltype(ZuIfT<
      ZuTraits<A>::IsArray &&
      ZuInspect<typename ZuTraits<A>::Elem, ZuNull>::Constructs>(), int()) = 0>
  ZuSpan(const A &a) { }
  template <typename A>
  ZuIfT<
    ZuTraits<A>::IsArray &&
    ZuInspect<typename ZuTraits<A>::Elem, ZuNull>::Converts, ZuSpan &>
  operator =(const A &a) { return *this; }

  ZuSpan(const ZuNull *data, unsigned length) { }
};

template <typename Cmp>
class ZuSpan<void, Cmp> : public ZuSpan_Null<void> {
public:
  using Elem = void;

  ZuSpan() { }
  ZuSpan(const ZuSpan &a) { }
  ZuSpan &operator =(const ZuSpan &a) { return *this; }

  template <typename A, decltype(ZuIfT<
      ZuTraits<A>::IsArray &&
      ZuInspect<typename ZuTraits<A>::Elem, void>::Constructs>(), int()) = 0>
  ZuSpan(const A &a) { }
  template <typename A>
  ZuIfT<
    ZuTraits<A>::IsArray &&
    ZuInspect<typename ZuTraits<A>::Elem, void>::Converts, ZuSpan &>
  operator =(const A &a) { return *this; }

  ZuSpan(const void *data, unsigned length) { }
};

template <typename T, typename N>
ZuSpan(T *data, N length) -> ZuSpan<T>;

template <typename Elem_>
struct ZuTraits<ZuSpan<Elem_>> : public ZuBaseTraits<ZuSpan<Elem_>> {
  using Elem = Elem_;
private:
  using Array = ZuSpan<Elem>;
public:
  enum {
    IsArray = 1, IsPrimitive = 0,
    IsString =
      bool{ZuIsExact<char, ZuDecay<Elem>>{}} ||
      bool{ZuIsExact<wchar_t, ZuDecay<Elem>>{}},
    IsWString = bool{ZuIsExact<wchar_t, ZuDecay<Elem>>{}}
  };
  static Elem *data(Array &a) { return a.data(); }
  static const Elem *data(const Array &a) { return a.data(); }
  static unsigned length(const Array &a) { return a.length(); }
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZuSpan_HH */
