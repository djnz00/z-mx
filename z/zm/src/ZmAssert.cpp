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

#include <zlib/ZuBox.hpp>
#include <zlib/ZuStringN.hpp>

#include <zlib/ZmAssert.hpp>

#include <zlib/ZmPlatform.hpp>
#include <zlib/ZmTime.hpp>

#include <stdio.h>

#ifdef _WIN32
#define snprintf _snprintf
#endif

// attaching debuggers can breakpoint on this function
void ZmAssert_sleep()
{
  Zm::sleep(1);
}

void ZmAssert_failed()
{
  for (;;) ZmAssert_sleep();
}

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4996)
#endif

void ZmAssert_fail(
    const char *expr, const char *file, unsigned line, const char *fn)
{
  ZuStringN<512> buf;

  if (fn)
    buf << '"' << file << "\":" << ZuBoxed(line) <<
      ' ' << fn << " Assertion '" << expr << "' failed.";
  else
    buf << '"' << file << "\":" << ZuBoxed(line) <<
      " Assertion '" << expr << "' failed.";

#ifndef _WIN32
  std::cerr << buf << std::flush;
#else
  MessageBoxA(0, buf, "Assertion Failure", MB_ICONEXCLAMATION);
#endif
  ZmAssert_failed();
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif
