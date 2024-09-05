//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// cppcodec C++ wrapper - Base 64 encode/decode

#ifndef ZuBase64_HH
#define ZuBase64_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuBytes.hh>

#include <cppcodec/base64_rfc4648.hpp>

namespace ZuBase64 {

ZuInline constexpr bool is(char c) {
  return
    (c >= 'A' && c <= 'Z') ||
    (c >= 'a' && c <= 'z') ||
    (c >= '0' && c <= '9') ||
    c == '+' || c == '/' || c == '=';
}

// both encode and decode return count of bytes written

// does not null-terminate dst
ZuInline constexpr unsigned enclen(unsigned slen) { return ((slen + 2)/3)<<2; }
ZuInline unsigned encode(ZuSpan<uint8_t> dst, ZuBytes src) {
  using base64 = cppcodec::base64_rfc4648;
  try {
    return base64::encode(
	reinterpret_cast<char *>(dst.data()), dst.length(),
	src.data(), src.length());
  } catch (...) {
    return 0;
  }
}

// does not null-terminate dst
ZuInline constexpr unsigned declen(unsigned slen) { return ((slen + 3)>>2)*3; }
ZuInline unsigned decode(ZuSpan<uint8_t> dst, ZuBytes src) {
  using base64 = cppcodec::base64_rfc4648;
  try {
    return base64::decode(
	dst.data(), dst.length(),
	reinterpret_cast<const char *>(src.data()), src.length());
  } catch (...) {
    return 0;
  }
}

}

#endif /* ZuBase64_HH */
