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

// heap-allocated move-only array

#ifndef ZuMvArray_HPP
#define ZuMvArray_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuArray.hpp>
#include <zlib/ZuArrayFn.hpp>

template <typename T_> class ZuMvArray : public ZuArrayFn<T_> {
  ZuMvArray(const ZuMvArray &) = delete;
  ZuMvArray &operator =(const ZuMvArray &) = delete;

public:
  using T = T_;

  ZuMvArray() = default;

  ZuMvArray(unsigned n) :
      m_length{n}, m_data{!n ? (T *)nullptr : (T *)::malloc(n * sizeof(T))} {
    if (ZuUnlikely(n && !m_data)) throw std::bad_alloc();
    this->initItems(m_data, n);
  }
  ZuMvArray(T *data, unsigned n) :
      m_length{n}, m_data{!n ? (T *)nullptr : (T *)::malloc(n * sizeof(T))} {
    if (ZuUnlikely(n && !m_data)) throw std::bad_alloc();
    this->moveItems(m_data, data, n);
  }
  ~ZuMvArray() {
    if (!m_data) return;
    this->destroyItems(m_data, m_length);
    ::free(m_data);
  }

  ZuMvArray(ZuMvArray &&a) {
    m_length = a.m_length;
    m_data = a.m_data;
    a.m_length = 0;
    a.m_data = nullptr;
  }
  ZuMvArray &operator =(ZuMvArray &&a) {
    m_length = a.m_length;
    m_data = a.m_data;
    a.m_length = 0;
    a.m_data = nullptr;
    return *this;
  }

  // array/ptr operators

  T &operator [](int i) { return m_data[i]; }
  const T &operator [](int i) const { return m_data[i]; }

  // accessors

  const T *begin() const { return m_data; }
  const T *end() const { return &m_data[m_length]; }

  unsigned length() const { return m_length; } 

  T *data() { return m_data; }
  const T *data() const { return m_data; }

  // reset to null

  void null() { delete [] m_data; m_length = 0;  m_data = nullptr;}

  // set length

  void length(unsigned newLength) {
    T *oldData = m_data;
    unsigned oldLength = m_length;
    m_length = newLength;
    m_data = (T *)::malloc(newLength * sizeof(T));
    unsigned mvLength = oldLength < newLength ? oldLength : newLength;
    unsigned initLength = newLength - mvLength;
    if (mvLength) this->moveItems(m_data, oldData, mvLength);
    if (initLength) this->initItems(m_data + mvLength, initLength);
    ::free(oldData);
  }

  // comparison

  bool operator !() const { return !m_length; }
  ZuOpBool

protected:
  bool same(const ZuMvArray &a) const { return this == &a; }
  template <typename A> constexpr bool same(const A &) const { return false; }

  auto buf() { return ZuArray{data(), length()}; }
  auto cbuf() const { return ZuArray{data(), length()}; }

  template <typename A>
  bool equals(const A &a) const {
    return same(a) || cbuf().equals(a);
  }
  template <typename A>
  int cmp(const A &a) const {
    if (same(a)) return 0;
    return cbuf().cmp(a);
  }
  template <typename L, typename R>
  friend inline ZuIfT<ZuConversion<ZuMvArray, L>::Is, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline ZuIfT<ZuConversion<ZuMvArray, L>::Is, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

  // hash
  uint32_t hash() const {
    return ZuHash<ZuMvArray>::hash(*this);
  }

  // traits
  struct Traits : public ZuBaseTraits<ZuMvArray> {
    using Elem = T;
    enum {
      IsArray = 1, IsPrimitive = 0, IsPOD = 0,
      IsString =
	ZuConversion<char, T>::Same ||
	ZuConversion<wchar_t, T>::Same,
      IsWString = ZuConversion<wchar_t, T>::Same
    };
    template <typename U = ZuMvArray>
    static typename ZuNotConst<U, T *>::T data(U &a) { return a.data(); }
    static const T *data(const ZuMvArray &a) { return a.data(); }
    static unsigned length(const ZuMvArray &a) { return a.length(); }
  };
  friend Traits ZuTraitsType(ZuMvArray *);

private:
  uint32_t	m_length = 0;
  T		*m_data = nullptr;
};

// generic printing
template <typename T>
ZuSame<ZuDecay<T>, char, ZuPrintString> ZuPrintType(ZuMvArray<T> *);

#endif /* ZuMvArray_HPP */
