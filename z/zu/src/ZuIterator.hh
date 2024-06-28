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

#include <iterator>

// template <typename Array_, typename Elem_>
// class Iterator : public ZuIterator<Iterator<Array_, Elem_>, Array_, Elem_> {
//   using Base = ZuIterator<Iterator<Array_, Elem_>, Array_, Elem_>;
// public:
//   using Array = Array_;
//   using Base::Base;
//   using Base::operator =;
//   using Base::container;
//   using Base::i;
// 
//   Elem operator *() const;
// };

template <typename Impl_, typename Container_, typename Elem_>
class ZuIterator {
public:
  using Impl = Impl_;
  using Container = Container_;
  using Elem = Elem_;

  auto impl() const { return static_cast<const Impl *>(this); }
  auto impl() { return static_cast<Impl *>(this); }

  using iterator_category = std::bidirectional_iterator_tag;
  using value_type = Elem;
  using difference_type = ptrdiff_t;
  using pointer = Elem *;
  using reference = Elem &;

  ZuIterator() = delete;
  ZuIterator(Container &container_, unsigned i) :
      container{container_}, i{i} { }
  ZuIterator(const ZuIterator &) = default;
  ZuIterator &operator =(const ZuIterator &) = default;
  ZuIterator(ZuIterator &&) = default;
  ZuIterator &operator =(ZuIterator &&) = default;

  Impl &operator++() { ++i; return *impl(); }
  Impl operator++(int) { Impl _ = *impl(); ++(*impl()); return _; }
  Impl &operator--() { --i; return *impl(); }
  Impl operator--(int) { Impl _ = *impl(); --(*impl()); return _; }

  bool operator ==(const ZuIterator &r) const {
    return &container == &r.container && i == r.i;
  }

  friend ptrdiff_t operator -(const ZuIterator &l, const ZuIterator &r) {
    return ptrdiff_t(l.i) - ptrdiff_t(r.i);
  }

protected:
  Container	&container;
  unsigned	i;
};

#endif /* ZuIterator_HH */
