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

// run-time assertion that falls back to ZeBackTrace in release code

// Usage: ZeAssert(assertion, (captures), msg, value);
//
// in debug mode, this is equivalent to ZmAssert(assertion), i.e.
// if the assertion fails the thread will sleep indefinitely so that a
// debugger can be attached to the process without losing any local context
//
// in release mode, ZeBackTrace is called with a Fatal severity level;
// the log event lambda uses _captures_ to append _msg_ to the log, and the
// calling function will return with _value_

// Example:
//
// void foo() {
//   int i = 42, j = 43;
//   ZeAssert(i == j - 1, (i, j), "i=" << i << " j=" << j, void());
// }

#ifndef ZeAssert_HPP
#define ZeAssert_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZeLib_HPP
#include <zlib/ZeLib.hpp>
#endif

#include <zlib/ZuPP.hpp>
#include <zlib/ZuFnName.hpp>

#include <zlib/ZmAssert.hpp>

#ifdef NDEBUG
#define ZeAssert(x, c, m, r) \
  do { if (ZuUnlikely(!(x))) { ZeBackTrace(Fatal, ([ZuPP_Strip(c)](auto &s) { \
    s << "\"" __FILE__ "\":" << __LINE__ << ' ' << ZuFnName << \
      " Assertion '" #x "' failed " << m; \
  })); return r; } } while (0)
#else
#define ZeAssert(x, c, m, r) ZmAssert(x)
#endif

#endif /* ZeAssert_HPP */
