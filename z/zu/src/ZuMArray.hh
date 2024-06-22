//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// monomorphic meta-array
// * encapsulates arbitrary array types into a single realized type that
//   can be used in compiled code interfaces

#ifndef ZuMArray_HH
#define ZuMArray_HH

#ifndef ZtLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <iterator>

#include <zlib/ZuFmt.hh>

namespace ZuMArray_ {

template <typename Array_, typename Elem_>
class Iterator {
public:
  using Array = Array_;
  using Elem = Elem_;
  using iterator_category = std::bidirectional_iterator_tag;
  using value_type = Elem;
  using difference_type = ptrdiff_t;
  using pointer = Elem *;
  using reference = Elem &;

  Iterator() = delete;
  Iterator(Array_ &array, unsigned i) : m_array{array}, m_i{i} { }
  Iterator(const Iterator &) = default;
  Iterator &operator =(const Iterator &) = default;
  Iterator(Iterator &&) = default;
  Iterator &operator =(Iterator &&) = default;

  Elem operator *() const;

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
  Array		&m_array;
  unsigned	m_i;
};

template <typename Wrapper, typename Under>
struct WrapTraits : public ZuTraits<Under> {
  enum { IsPrimitive = 0, IsPOD = 0 };
  using Elem = typename ZuTraits<Under>::Elem;
  template <typename U = Under>
  static ZuIfT<ZuTraits<U>::IsSpan && !ZuIsConst<U>{}, Elem *>
  data(Wrapper &v) {
    return ZuTraits<U>::data(v.get());
  }
  template <typename U = Under>
  static ZuMatchSpan<U, const Elem *> data(const Wrapper &v) {
    return ZuTraits<U>::data(v.get());
  }
  template <typename U = Under>
  static ZuMatchArray<U, unsigned> length(const Wrapper &v) {
    return ZuTraits<U>::length(v.get());
  }
};

template <typename Array_>
class Elem {
public:
  using Array = Array_;
  using T = typename Array::T;
  using R = typename Array::R;

  Elem() = delete;
  Elem(Array &array, unsigned i) : m_array{array}, m_i{i} { }
  Elem(const Elem &) = default;
  Elem &operator =(const Elem &) = default;
  Elem(Elem &&) = default;
  Elem &operator =(Elem &&) = default;

  R get() const;

  operator R() const { return get(); }

  Elem &operator =(T v);

  bool equals(const Elem &r) const { return get() == r.get(); }
  int cmp(const Elem &r) const { return ZuCmp<T>::cmp(get(), r.get()); }
  friend inline bool
  operator ==(const Elem &l, const Elem &r) { return l.equals(r); }
  friend inline int
  operator <=>(const Elem &l, const Elem &r) { return l.cmp(r); }

  bool operator !() const { return !get(); }

  // traits
  using Traits = WrapTraits<Elem, R>;
  friend Traits ZuTraitsType(Elem *);

  // underlying type
  friend R ZuUnderType(Elem *);

private:
  Array		&m_array;
  unsigned	m_i;
};

template <typename T_, typename R_ = T_>
class Array {
public:
  using T = T_;
  using R = R_;
  using Elem = ZuMArray_::Elem<Array>;

friend Elem;

  template <typename Array_>
  Array(const Array_ &array) :
    m_ptr{const_cast<Array_ *>(&array)},
    m_length{ZuTraits<Array_>::length(array)},
    m_getFn{[](const void *ptr, unsigned i) -> R {
      return (*static_cast<const Array_ *>(ptr))[i];
    }} { }

  template <typename Array_, typename Elem_ = typename ZuTraits<Array_>::Elem>
  Array(Array_ &array) :
    m_ptr{&array},
    m_length{ZuTraits<Array_>::length(array)},
    m_getFn{[](const void *ptr, unsigned i) -> R {
      return (*static_cast<const Array_ *>(ptr))[i];
    }},
    m_setFn{[](void *ptr, unsigned i, T elem) {
      (*static_cast<Array_ *>(ptr))[i] = Elem_(ZuMv(elem));
    }} { }

  template <typename Array_, typename GetFn_>
  Array(Array_ &array, unsigned length, GetFn_ getFn) :
    m_ptr{&array},
    m_length{length},
    m_getFn{getFn} { }

  template <typename Array_, typename GetFn_, typename SetFn_>
  Array(Array_ &array, unsigned length, GetFn_ getFn, SetFn_ setFn) :
    m_ptr{&array},
    m_length{length},
    m_getFn{getFn},
    m_setFn{setFn} { }

  Array() = default;
  Array(const Array &) = default;
  Array &operator =(const Array &) = default;
  Array(Array &&s) = default;
  Array &operator =(Array &&s) = default;

  unsigned length() const { return m_length; }

  const Elem operator[](unsigned i) const {
    return Elem{const_cast<Array &>(*this), i};
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

  bool equals(const Array &r) const {
    if (this == &r) return true;
    unsigned l = length();
    unsigned n = r.length();
    if (l != n) return false;
    for (unsigned i = 0; i < n; i++)
      if (R((*this)[i]) != R(r[i])) return false;
    return true;
  }
  int cmp(const Array &r) const {
    if (this == &r) return 0;
    unsigned ln = m_length;
    unsigned rn = r.m_length;
    unsigned n = ln < rn ? ln : rn;
    for (unsigned i = 0; i < n; i++)
      if (int j = ZuCmp<T>::cmp(R((*this)[i]), R(r[i]))) return j;
    return ZuCmp<int>::cmp(ln, rn);
  }
  friend inline bool
  operator ==(const Array &l, const Array &r) { return l.equals(r); }
  friend inline bool
  operator <(const Array &l, const Array &r) { return l.cmp(r) < 0; }
  friend inline int
  operator <=>(const Array &l, const Array &r) { return l.cmp(r); }

  bool operator !() const { return !m_length; }
  ZuOpBool

  using iterator = Iterator<Array, Elem>;
  using const_iterator = Iterator<const Array, const Elem>;
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

template <typename Array, typename Elem>
inline Elem Iterator<Array, Elem>::operator *() const {
  return m_array[m_i];
}

template <typename Array>
inline typename Elem<Array>::R Elem<Array>::get() const {
  return (*m_array.m_getFn)(m_array.m_ptr, m_i);
}

template <typename Array>
inline Elem<Array> &Elem<Array>::operator =(typename Elem<Array>::T v) {
  (*m_array.m_setFn)(m_array.m_ptr, m_i, ZuMv(v));
  return *this;
}

} // ZuMArray_

template <typename T, typename R = T>
using ZuMArray = ZuMArray_::Array<T, R>;

#endif /* ZuMArray_HH */
