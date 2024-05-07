//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// platform-specific

#ifndef ZtPlatform_HH
#define ZtPlatform_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZtLib_HH
#include <zlib/ZtLib.hh>
#endif

#include <locale.h>
#include <string.h>
#include <wchar.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <time.h>

#ifndef _WIN32
#include <sys/types.h>
#endif

#include <zlib/ZuInt.hh>

namespace Zt {

// environment and timezone manipulation
#ifndef _WIN32
inline int putenv(const char *s) { return ::putenv((char *)s); }
inline void tzset(void) { ::tzset(); }
#else
inline int putenv(const char *s) { return ::_putenv((char *)s); }
inline void tzset(void) { ::_tzset(); }
#endif

} // namespace Zt

#endif /* ZtPlatform_HH */
