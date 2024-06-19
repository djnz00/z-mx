//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// monomorphic meta-array
// * encapsulates arbitrary array types into a single type that
//   can be used in compiled code interfaces

#ifndef ZuMArray_HH
#define ZuMArray_HH

#ifndef ZtLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <iterator>

#include <zlib/ZuFmt.hh>

namespace ZuMArray_ {
// STL cruft
template <typename Array_, typename Elem_>
class Iterator {
public:
  using iterator_category = std::bidirectional_iterator_tag;
  using value_type = Elem_;
  using difference_type = ptrdiff_t;
  using pointer = Elem_ *;
  using reference = Elem_ &;

  Iterator() = delete;
  Iterator(Array_ &array, unsigned i) : m_array{array}, m_i{i} { }
  Iterator(const Iterator &) = default;
  Iterator &operator =(const Iterator &) = default;
  Iterator(Iterator &&) = default;
  Iterator &operator =(Iterator &&) = default;

  Elem_ operator *() const { return m_array[m_i]; }

  Iterator &operator++() { ++m_i; return *this; }
  Iterator operator++(int) { Iterator _ = *this; ++(*this); return _; }
  Iterator &operator--() { --m_i; return *this; }
  Iterator operator--(int) { Iterator _ = *this; --(*this); return _; }

  bool operator ==(const Iterator &r) const {
    return &m_array == &r.m_array && m_i == r.m_i;
  }

  friend ptrdiff_t operator -(const Iterator &l, const Iterator &r) {
    return ptrdiff_t(l.m_i) - ptrdiff_t(r.m_i);
  }

private:
  Array_	&m_array;
  unsigned	m_i;
};

template <typename Elem_>
struct ElemTraits : public ZuTraits<typename Elem_::R> {
  enum { IsPrimitive = 0, IsPOD = 0 };
private:
  using T = typename Elem_::T;
  using R = typename Elem_::R;
public:
  using Elem = typename ZuTraits<R>::Elem;
  template <typename U = R>
  static ZuIfT<ZuTraits<U>::IsSpan && !ZuIsConst<U>{}, Elem *>
  data(Elem_ &v) {
    return ZuTraits<U>::data(v.get());
  }
  template <typename U = R>
  static ZuMatchSpan<U, const Elem *> data(const Elem_ &v) {
    return ZuTraits<U>::data(v.get());
  }
  template <typename U = R>
  static ZuMatchArray<U, unsigned> length(const Elem_ &v) {
    return ZuTraits<U>::length(v.get());
  }
};
} // ZuMArray_
template <typename Array, typename Elem>
using ZuMArray_Iterator = ZuMArray_::Iterator<Array, Elem>;
template <typename Elem>
using ZuMArray_ElemTraits = ZuMArray_::ElemTraits<Elem>;

template <typename T_, typename R_ = T_>
class ZuMArray {
public:
  using T = T_;
  using R = R_;

  template <typename Array>
  ZuMArray(const Array &array) :
    m_ptr{const_cast<Array *>(&array)},
    m_length{ZuTraits<Array>::length(array)},
    m_getFn{[](const void *ptr, unsigned i) -> R {
      return (*static_cast<const Array *>(ptr))[i];
    }} { }

  template <typename Array, typename Elem_ = typename ZuTraits<Array>::Elem>
  ZuMArray(Array &array) :
    m_ptr{&array},
    m_length{ZuTraits<Array>::length(array)},
    m_getFn{[](const void *ptr, unsigned i) -> R {
      return (*static_cast<const Array *>(ptr))[i];
    }},
    m_setFn{[](void *ptr, unsigned i, T elem) {
      (*static_cast<Array *>(ptr))[i] = Elem_(ZuMv(elem));
    }} { }

  template <typename Array, typename GetFn_>
  ZuMArray(Array &array, unsigned length, GetFn_ getFn) :
    m_ptr{&array},
    m_length{length},
    m_getFn{getFn} { }

  template <typename Array, typename GetFn_, typename SetFn>
  ZuMArray(Array &array, unsigned length, GetFn_ getFn, SetFn setFn) :
    m_ptr{&array},
    m_length{length},
    m_getFn{getFn},
    m_setFn{setFn} { }

  class Elem;
friend Elem;
  class Elem {
  public:
    using T = T_;
    using R = R_;

    Elem() = delete;
    Elem(ZuMArray &array, unsigned i) : m_array{array}, m_i{i} { }
    Elem(const Elem &) = default;
    Elem &operator =(const Elem &) = default;
    Elem(Elem &&) = default;
    Elem &operator =(Elem &&) = default;

    operator R() const { return (*m_array.m_getFn)(m_array.m_ptr, m_i); }

    Elem &operator =(T v) {
      (*m_array.m_setFn)(m_array.m_ptr, m_i, ZuMv(v));
      return *this;
    };

    bool equals(const Elem &r) const { return R(*this) == R(r); }
    int cmp(const Elem &r) const { return ZuCmp<T>::cmp(R(*this), R(r)); }
    friend inline bool
    operator ==(const Elem &l, const Elem &r) { return l.equals(r); }
    friend inline int
    operator <=>(const Elem &l, const Elem &r) { return l.cmp(r); }

    bool operator !() const { return !R(*this); }

    R get() const { return (*m_array.m_getFn)(m_array.m_ptr, m_i); }

    friend ZuMArray_ElemTraits<Elem> ZuTraitsType(Elem *);

  private:
    ZuMArray	&m_array;
    unsigned	m_i;
  };

  ZuMArray() = default;
  ZuMArray(const ZuMArray &) = default;
  ZuMArray &operator =(const ZuMArray &) = default;
  ZuMArray(ZuMArray &&s) = default;
  ZuMArray &operator =(ZuMArray &&s) = default;

  unsigned length() const { return m_length; }

  const Elem operator[](unsigned i) const {
    return Elem{const_cast<ZuMArray &>(*this), i};
  }
  Elem operator[](unsigned i) { return Elem{*this, i}; }

// iteration - all() is const by default, all<true>() is mutable
  template <bool Mutable = false, typename L>
  ZuIfT<!Mutable> all(L l) const {
    for (unsigned i = 0, n = m_length; i < n; i++) l((*this)[i]);
  }
  template <bool Mutable, typename L>
  ZuIfT<Mutable> all(L l) {
    for (unsigned i = 0, n = m_length; i < n; i++) l((*this)[i]);
  }

  bool equals(const ZuMArray &r) const {
    if (this == &r) return true;
    unsigned l = length();
    unsigned n = r.length();
    if (l != n) return false;
    for (unsigned i = 0; i < n; i++)
      if (R((*this)[i]) != R(r[i])) return false;
    return true;
  }
  int cmp(const ZuMArray &r) const {
    if (this == &r) return 0;
    unsigned ln = m_length;
    unsigned rn = r.m_length;
    unsigned n = ln < rn ? ln : rn;
    for (unsigned i = 0; i < n; i++)
      if (int j = ZuCmp<T>::cmp(R((*this)[i]), R(r[i]))) return j;
    return ZuCmp<int>::cmp(ln, rn);
  }
  friend inline bool
  operator ==(const ZuMArray &l, const ZuMArray &r) { return l.equals(r); }
  friend inline bool
  operator <(const ZuMArray &l, const ZuMArray &r) { return l.cmp(r) < 0; }
  friend inline int
  operator <=>(const ZuMArray &l, const ZuMArray &r) { return l.cmp(r); }

  bool operator !() const { return !m_length; }
  ZuOpBool

  using iterator = ZuMArray_Iterator<ZuMArray, Elem>;
  using const_iterator = ZuMArray_Iterator<const ZuMArray, const Elem>;
  const_iterator begin() const { return const_iterator{*this, 0}; }
  const_iterator end() const { return const_iterator{*this, m_length}; }
  const_iterator cbegin() const { return const_iterator{*this, 0}; }
  const_iterator cend() const { return const_iterator{*this, m_length}; }
  iterator begin() { return iterator{*this, 0}; }
  iterator end() { return iterator{*this, m_length}; }

private:
  typedef R (*GetFn)(const void *, unsigned);
  typedef void (*SetFn)(void *, unsigned, T);

  void		*m_ptr = nullptr;
  unsigned	m_length = 0;
  GetFn		m_getFn = nullptr;
  SetFn		m_setFn = nullptr;
};

#endif /* ZuMArray_HH */
