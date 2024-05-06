//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// cppcodec C++ wrapper - Hex (uppercase) encode/decode

#ifndef ZtlsHex_HH
#define ZtlsHex_HH

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZtlsLib.hh>

#include <cppcodec/hex_upper.hh>

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

#endif /* ZtlsHex_HH */
