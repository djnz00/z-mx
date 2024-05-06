//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// fixed-size sliding window with dynamically allocated underlying buffer

#ifndef ZtWindow_HH
#define ZtWindow_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZtLib_HH
#include <zlib/ZtLib.hh>
#endif

#include <zlib/ZtArray.hh>

template <typename T_> struct ZtWindow {
public:
  using T = T_;

  ZtWindow() = default;
  ZtWindow(const ZtWindow &) = default;
  ZtWindow &operator =(const ZtWindow &) = default;
  ZtWindow(ZtWindow &&) = default;
  ZtWindow &operator =(ZtWindow &&) = default;

  ZtWindow(unsigned max) : m_max{max} { }

  void clear() {
    m_offset = 0;
    m_data = {};
  }

  class ElemRO {
  public:
    ElemRO(const ZtWindow &window, int index) :
	m_window{window}, m_index{index} { }

    operator const T *() const {
      if (ZuUnlikely(m_index < 0)) return nullptr;
      return m_window.val(m_index);
    }
    T *operator ->() const {
      if (ZuUnlikely(m_index < 0)) return nullptr;
      return m_window.val(m_index);
    }

  protected:
    const ZtWindow	&m_window;
    int			m_index;
  };
  class Elem : public ElemRO {
    using ElemRO::m_window;
    using ElemRO::m_index;
  public:
    Elem(const ZtWindow &window, unsigned index) : ElemRO{window, index} { }

    Elem &operator =(T v) {
      if (ZuUnlikely(m_index < 0)) return *this;
      const_cast<ZtWindow &>(m_window).set(m_index, ZuMv(v));
      return *this;
    }
  };
  ElemRO operator [](unsigned i) const { return {this, i}; }
  Elem operator [](unsigned i) { return {this, i}; }

  void set(unsigned i, T v) {
    if (i < m_offset) return;
    if (i >= m_offset + m_max) {
      unsigned newOffset = (i + 1) - m_max;
      if (newOffset >= m_offset + m_max)
	m_data = {};
      else
	while (m_offset < newOffset) {
	  unsigned j = m_offset % m_max;
	  if (j < m_data.length()) m_data[j] = {};
	  ++m_offset;
	}
    }
    unsigned j = i % m_max;
    unsigned o = m_data.length();
    if (j >= o) {
      unsigned n = j + 1;
      n = ZmGrow(o * sizeof(T), n * sizeof(T)) / sizeof(T);
      if (n > m_max) n = m_max;
      m_data.length(n);
    }
    m_data[j] = ZuMv(v);
  }

  void clr(int i) { if ((i = index(i)) >= 0) m_data[i] = {}; }

  const T *val(int i) const {
    if ((i = index(i)) < 0 || !m_data[i]) return nullptr;
    return &m_data[i];
  }

private:
  int index(unsigned i) const {
    if (i < m_offset || i >= m_offset + m_max) return -1;
    i %= m_max;
    if (i >= m_data.length()) return -1;
    return i;
  }

  ZtArray<T>	m_data;
  unsigned	m_offset = 0;
  unsigned	m_max = 100;
};

#endif /* ZtWindow_HH */
