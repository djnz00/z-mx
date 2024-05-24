//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// mbedtls C++ wrapper

#ifndef ZtlsLib_HH
#define ZtlsLib_HH

#include <zlib/ZuLib.hh>

#ifdef _WIN32

#ifdef ZTLS_EXPORTS
#define ZtlsAPI ZuExport_API
#define ZtlsExplicit ZuExport_Explicit
#else
#define ZtlsAPI ZuImport_API
#define ZtlsExplicit ZuImport_Explicit
#endif
#define ZtlsExtern extern ZtlsAPI

#else

#define ZtlsAPI
#define ZtlsExplicit
#define ZtlsExtern extern

#endif

#include <mbedtls/error.h>

#include <zlib/ZtString.hh>

namespace Ztls {
  inline ZtString strerror_(int n) {
    ZtString s(100);
    mbedtls_strerror(n, s.data(), s.size() - 1);
    s.calcLength();
    s.chomp();
    return s;
  }
}

#endif /* ZtlsLib_HH */
