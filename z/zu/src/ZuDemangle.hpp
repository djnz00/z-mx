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

// C++ demangling

#ifndef ZuDemangle_HPP
#define ZuDemangle_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <cxxabi.h>

#include <zlib/ZuString.hpp>
#include <zlib/ZuPrint.hpp>

template <unsigned BufSize>
class ZuDemangle : public ZuPrintable {
  ZuDemangle(const ZuDemangle &) = delete;
  ZuDemangle &operator =(const ZuDemangle &) = delete;
  ZuDemangle(ZuDemangle &&) = delete;
  ZuDemangle &operator =(ZuDemangle &&) = delete;

public:
  ZuDemangle() { m_buf[0] = 0; }

  ZuDemangle(ZuString symbol) {
    int status;
    size_t len = BufSize;
    const char *demangled =
      __cxxabiv1::__cxa_demangle(symbol, m_buf, &len, &status);
    m_buf[BufSize - 1] = 0;
    if (ZuUnlikely(status || !demangled))
      m_output = symbol;
    else {
      if (len == BufSize) len = strlen(demangled);
      m_output = ZuString{demangled, (unsigned)len};
    }
  }

  ZuDemangle &operator =(ZuString symbol) {
    // this->~ZuDemangle();
    new (this) ZuDemangle{ZuMv(symbol)};
    return *this;
  }

  operator ZuString() const { return m_output; }

  template <typename S> void print(S &s) const { s << m_output; }

private:
  ZuString	m_output;
  char		m_buf[BufSize];
};

#endif /* ZuDemangle_HPP */
