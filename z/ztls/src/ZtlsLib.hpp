//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2

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

// mbedtls C++ wrapper

#ifndef ZtlsLib_HPP
#define ZtlsLib_HPP

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuLib.hpp>

#ifdef _WIN32

#ifdef ZI_EXPORTS
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

#include <zlib/ZtString.hpp>

namespace Ztls {
  inline ZtString strerror_(int n) {
    ZtString s(100);
    mbedtls_strerror(n, s.data(), s.size() - 1);
    s[s.size() - 1] = 0;
    s.calcLength();
    s.chomp();
    return s;
  }
}

#endif /* ZtlsLib_HPP */
