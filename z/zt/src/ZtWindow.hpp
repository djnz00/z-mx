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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// fixed-size sliding window with dynamically allocated underlying buffer

#ifndef ZtWindow_HPP
#define ZtWindow_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZtLib_HPP
#include <zlib/ZtLib.hpp>
#endif

#include <zlib/ZtArray.hpp>

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

#endif /* ZtWindow_HPP */
