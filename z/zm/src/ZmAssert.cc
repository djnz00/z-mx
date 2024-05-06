//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// assertions

#include <stdio.h>
#include <assert.h>

#include <zlib/ZuBox.hh>
#include <zlib/ZuStringN.hh>

#include <zlib/ZmAssert.hh>
#include <zlib/ZmPlatform.hh>
#include <zlib/ZmTime.hh>
#include <zlib/ZmTrap.hh>

#ifdef _WIN32
#define snprintf _snprintf
#endif

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4996)
#endif

void ZmAssert_fail(
    const char *expr, const char *file, unsigned line, const char *fn)
{
  ZuStringN<512> buf;

  if (fn)
    buf << '"' << file << "\":" << line <<
      ' ' << fn << " Assertion '" << expr << "' failed";
  else
    buf << '"' << file << "\":" << line <<
      " Assertion '" << expr << "' failed";

  ZmTrap::log(buf);
  ::abort();
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif
