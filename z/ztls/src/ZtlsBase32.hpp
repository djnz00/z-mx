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

// cppcodec C++ wrapper - Base 32 encode/decode

#ifndef ZtlsBase32_HPP
#define ZtlsBase32_HPP

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZtlsLib.hpp>

#include <cppcodec/base32_rfc4648.hpp>

namespace Ztls {
namespace Base32 {

// both encode and decode return count of bytes written

// does not null-terminate dst
ZuInline constexpr unsigned enclen(unsigned slen) { return ((slen + 4)/5)<<3; }
ZuInline unsigned encode(
    char *dst, unsigned dlen, const void *src, unsigned slen) {
  using base32 = cppcodec::base32_rfc4648;
  try {
    return base32::encode(dst, dlen, (const uint8_t *)src, slen);
  } catch (...) {
    return 0;
  }
}

// does not null-terminate dst
ZuInline constexpr unsigned declen(unsigned slen) { return ((slen + 7)>>3)*5; }
ZuInline unsigned decode(
    void *dst, unsigned dlen, const char *src, unsigned slen) {
  using base32 = cppcodec::base32_rfc4648;
  try {
    return base32::decode((uint8_t *)dst, dlen, src, slen);
  } catch (...) {
    return 0;
  }
}

}
}

#endif /* ZtlsBase32_HPP */
