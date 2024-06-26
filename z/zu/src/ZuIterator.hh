//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// generic delegated container STL iterator

#ifndef ZuIterator_HH
#define ZuIterator_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

// template <typename Array_, typename Elem_>
// class Iterator : public ZuIterator<Array_, Elem_> {
//   using Base = ZuIterator<Array_, Elem_>;
// public:
//   using Array = Array_;
//   using Base::Base;
//   using Base::operator =;
// 
//   Elem operator *() const;
// };

template <typename Container_, typename Elem_>
class ZuIterator {
public:
  using Container = Container_;
  using Elem = Elem_;
  using iterator_category = std::bidirectional_iterator_tag;
  using value_type = Elem;
  using difference_type = ptrdiff_t;
  using pointer = Elem *;
  using reference = Elem &;

  Iterator() = delete;
  Iterator(Container_ &container, unsigned i) :
      m_container{container}, m_i{i} { }
  Iterator(const Iterator &) = default;
  Iterator &operator =(const Iterator &) = default;
  Iterator(Iterator &&) = default;
  Iterator &operator =(Iterator &&) = default;

  Iterator &operator++() { ++m_i; return *this; }
  Iterator operator++(int) { Iterator _ = *this; ++(*this); return _; }
  Iterator &operator--() { --m_i; return *this; }
  Iterator operator--(int) { Iterator _ = *this; --(*this); return _; }

  bool operator ==(const Iterator &r) const {
    return &m_container == &r.m_container && m_i == r.m_i;
  }

  friend ptrdiff_t operator -(const Iterator &l, const Iterator &r) {
    return ptrdiff_t(l.m_i) - ptrdiff_t(r.m_i);
  }

private:
  Container	&m_container;
  unsigned	m_i;
};

#endif /* ZuIterator_HH */
