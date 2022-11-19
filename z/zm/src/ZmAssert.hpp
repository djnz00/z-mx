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

// assertions

// differs from assert() in that an assertion failure hangs the program
// rather than crashing it, permitting live debugging

#ifndef ZmAssert_HPP
#define ZmAssert_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

// need to export regardless of NDEBUG to support debug build application
// linking with release build Z

extern "C" {
  ZmExtern void ZmAssert_fail(
      const char *expr, const char *file, unsigned line, const char *fn);
  ZmExtern void ZmAssert_failed();
  ZmExtern void ZmAssert_sleep();
}

#ifdef NDEBUG
#define ZmAssert(x) (void())
#else
#include <zlib/ZuFnName.hpp>
#define ZmAssert(x) \
  ((x) ? void() : ZmAssert_fail(#x, __FILE__, __LINE__, ZuFnName))
#endif

#endif /* ZmAssert_HPP */
