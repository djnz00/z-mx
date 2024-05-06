//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// run-time assertion that falls back to ZeLOGBT in release code

// Usage: ZeAssert(assertion, (captures), msg, value);
//
// in debug mode, this is equivalent to ZmAssert(assertion), i.e.
// if the assertion fails the program will abort
//
// in release mode, ZeBackTrace is called with a Fatal severity level;
// the log event lambda uses captures to append msg to the log, and the
// calling function will return with value

// Example:
//
// void foo() {
//   int i = 42, j = 43;
//   ZeAssert(i == j - 1, (i, j), "i=" << i << " j=" << j, void());
// }

#ifndef ZeAssert_HH
#define ZeAssert_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZeLib_HH
#include <zlib/ZeLib.hh>
#endif

#include <zlib/ZuPP.hh>
#include <zlib/ZuFnName.hh>

#include <zlib/ZmAssert.hh>

#ifdef NDEBUG
#define ZeAssert(x, c, m, r) \
  do { if (ZuUnlikely(!(x))) { ZeLOGBT(Fatal, ([ZuPP_Strip(c)](auto &s) { \
    s << "\"" __FILE__ "\":" << __LINE__ << ' ' << ZuFnName << \
      " Assertion '" #x "' failed " << m; \
  })); return r; } } while (0)
#else
#define ZeAssert(x, c, m, r) ZmAssert(x)
#endif

#endif /* ZeAssert_HH */
