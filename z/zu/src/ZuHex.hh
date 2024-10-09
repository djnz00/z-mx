//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Hex encode/decode

#ifndef ZuHex_HH
#define ZuHex_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuBytes.hh>

namespace ZuHex {

ZuInline constexpr uint8_t lookup(uint8_t c) {
  if (c >= 'A' && c <= 'F') return (c - 'A') + 10;
  if (c >= '0' && c <= '9') return c - '0';
  return 0xff;
};

ZuInline constexpr bool is(char c) {
  return (c >= 'A' && c <= 'F') || (c >= '0' && c <= '9');
}

// both encode and decode return count of bytes written

// does not null-terminate dst
ZuInline constexpr unsigned enclen(unsigned slen) { return slen<<1; }
ZuInline unsigned encode(ZuSpan<uint8_t> dst, ZuBytes src) {
  static constexpr const char lookup[] = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'A', 'B', 'C', 'D', 'E', 'F'
  };
  auto s = src.data();
  auto d = dst.data();
  auto n = src.length();
  uint8_t i;
  while (n > 0) {
    i = *s++;
    *d++ = lookup[i>>4];
    *d++ = lookup[i & 0xf];
    --n;
  }
  return d - dst.data();
}

// does not null-terminate dst
ZuInline constexpr unsigned declen(unsigned slen) { return (slen + 1)>>1; }
ZuInline unsigned decode(ZuSpan<uint8_t> dst, ZuBytes src) {
  auto s = src.data();
  auto d = dst.data();
  auto n = src.length();
  uint8_t i, j;
  while (n >= 2) {
    i = lookup(*s++); if (i >= 16) break;
    j = lookup(*s++); if (j >= 16) break;
    *d++ = (i<<4) | j;
    n -= 2;
  }
  return d - dst.data();
}

}

#endif /* ZuHex_HH */
