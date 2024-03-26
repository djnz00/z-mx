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

// cppcodec C++ wrapper - Hex (uppercase) encode/decode

#ifndef ZtlsHex_HPP
#define ZtlsHex_HPP

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZtlsLib.hpp>

#include <cppcodec/hex_upper.hpp>

namespace Ztls::Hex {

// both encode and decode return count of bytes written

// does not null-terminate dst
ZuInline constexpr unsigned enclen(unsigned slen) { return slen<<1; }
ZuInline unsigned encode(ZuArray<uint8_t> dst, ZuBytes src) {
  using hex = cppcodec::hex_upper;
  return hex::encode(
      reinterpret_cast<char *>(dst.data()), dst.length(),
      src.data(), src.length());
}

// does not null-terminate dst
ZuInline constexpr unsigned declen(unsigned slen) { return (slen + 1)>>1; }
ZuInline unsigned decode(ZuArray<uint8_t> dst, ZuBytes src) {
  using hex = cppcodec::hex_upper;
  return hex::decode(
      dst.data(), dst.length(),
      reinterpret_cast<const char *>(src.data()), src.length());
}

}

#endif /* ZtlsHex_HPP */
