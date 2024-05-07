//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// tzOffset() calls tzset() if the tz argument is non-null
// - this should not be called with high frequency by high-performance
//   applications with a non-null tz since:
// - it acquires/releases a global lock, since tzset() is not thread-safe
// - it is potentially time-consuming
// - it may access and temporarily modify global environment variables
// - tzset() may in turn access system configuration files and/or external
//   timezone databases

#ifndef ZtTimeZone_HH
#define ZtTimeZone_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZtLib_HH
#include <zlib/ZtLib.hh>
#endif

#include <zlib/ZuDateTime.hh>

namespace Zt {

ZtExtern int tzOffset(const ZuDateTime &value, const char *tz = nullptr);

} // Zt

#endif /* ZtTimeZone_HH */
