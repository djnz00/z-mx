//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// cppcodec C++ wrapper - Base 32 encode/decode

#ifndef ZtlsBase32_HH
#define ZtlsBase32_HH

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZtlsLib.hh>

#include <cppcodec/base32_rfc4648.hpp>

namespace Ztls::Base32 {

// both encode and decode return count of bytes written

// does not null-terminate dst
ZuInline constexpr unsigned enclen(unsigned slen) { return ((slen + 4)/5)<<3; }
ZuInline unsigned encode(ZuArray<uint8_t> dst, ZuBytes src) {
  using base32 = cppcodec::base32_rfc4648;
  try {
    return base32::encode(
	reinterpret_cast<char *>(dst.data()), dst.length(),
	src.data(), src.length());
  } catch (...) {
    return 0;
  }
}

// does not null-terminate dst
ZuInline constexpr unsigned declen(unsigned slen) { return ((slen + 7)>>3)*5; }
ZuInline unsigned decode(ZuArray<uint8_t> dst, ZuBytes src) {
  using base32 = cppcodec::base32_rfc4648;
  try {
    return base32::decode(
	dst.data(), dst.length(),
	reinterpret_cast<const char *>(src.data()), src.length());
  } catch (...) {
    return 0;
  }
}

}

#endif /* ZtlsBase32_HH */
