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

#ifndef ZmDemangle_HPP
#define ZmDemangle_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

// #include <cxxabi.h>

#include <zlib/ZuString.hpp>
#include <zlib/ZuPrint.hpp>

// Note: __cxxabiv1::__cxa_demangle() doesn't correctly demangle
// "Z1XvEUlTyOT_E_" to
// "X()::{lambda<typename $T0>($T0&&)#1}"
//
// in contrast, binutils/bfd cplus_demangle() does this correctly

// from binutils demangle.h
#define DMGL_PARAMS	 (1<<0)	/* Include function args */
#define DMGL_ANSI	 (1<<1)	/* Include const, volatile, etc. */
#define DMGL_VERBOSE	 (1<<3)	/* Include implementation details */
#define DMGL_TYPES	 (1<<4)	/* Also try to demangle type encodings */
extern "C" { extern char *cplus_demangle(const char *mangled, int options); }

class ZmDemangle : public ZuPrintable {
  ZmDemangle(const ZmDemangle &) = delete;
  ZmDemangle &operator =(const ZmDemangle &) = delete;
  ZmDemangle(ZmDemangle &&) = delete;
  ZmDemangle &operator =(ZmDemangle &&) = delete;

public:
  ZmDemangle() = default;

  ZmDemangle(const char *mangled) {
    m_output = cplus_demangle(mangled,
	DMGL_TYPES | DMGL_PARAMS | DMGL_ANSI | DMGL_VERBOSE);
    if (m_output)
      m_free = true;
    else
      m_output = mangled;
  }

  ~ZmDemangle() {
    if (m_free && m_output) ::free(const_cast<char *>(m_output));
  }

  ZmDemangle &operator =(const char *symbol) {
    this->~ZmDemangle();
    new (this) ZmDemangle{symbol};
    return *this;
  }

  operator ZuString() const { return m_output; }

  template <typename S> void print(S &s) const {
    if (m_output) s << m_output;
  }

private:
  const char	*m_output = nullptr;
  bool		m_free = false;
};

#endif /* ZmDemangle_HPP */
