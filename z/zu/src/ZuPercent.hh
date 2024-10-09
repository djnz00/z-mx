//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// HTTP percent encoding

// [a-zA-Z0-9_-.~] are not quoted
// decodes both '+' and '%20' as ' '
// always encodes ' ' as '%20'

#ifndef ZuPercent_HH
#define ZuPercent_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuBytes.hh>

namespace ZuPercent {

ZuInline constexpr uint8_t lookup(uint8_t c) {
  if (c >= 'a' && c <= 'f') return (c - 'a') + 10;
  if (c >= 'A' && c <= 'F') return (c - 'A') + 10;
  if (c >= '0' && c <= '9') return c - '0';
  return 0xff;
};

// both encode and decode return count of bytes written

// does not null-terminate dst
ZuInline constexpr unsigned enclen(unsigned slen) { return slen * 3; }
ZuInline unsigned encode(ZuSpan<uint8_t> dst, ZuBytes src) {
  static constexpr const char lookup[] = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'A', 'B', 'C', 'D', 'E', 'F'
  };
  // little-endian bitmap
  static constexpr const uint8_t special[] = {
    0xff, 0xff, 0xff, 0xff, 0xff, 0x9f, 0x00, 0xfc,
    0x01, 0x00, 0x00, 0x78, 0x01, 0x00, 0x00, 0xb8,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff
  };

  auto s = src.data();
  auto d = dst.data();
  auto n = src.length();
  uint8_t i;
  while (n > 0) {
    i = *s++;
    if (special[i>>3] & (uint8_t(1)<<(i & 0x7))) {
      *d++ = '%';
      *d++ = lookup[i>>4];
      *d++ = lookup[i & 0xf];
    } else {
      *d++ = i;
    }
    --n;
  }
  return d - dst.data();
}

// does not null-terminate dst
ZuInline constexpr unsigned declen(unsigned slen) { return slen; }
ZuInline unsigned decode(ZuSpan<uint8_t> dst, ZuBytes src) {
  auto s = src.data();
  auto d = dst.data();
  auto n = src.length();
  uint8_t i, j;
  while (n > 0) {
    i = *s++;
    --n;
    if (i == '%') {
      i = lookup(*s++); if (i >= 16) break;
      j = lookup(*s++); if (j >= 16) break;
      n -= 2;
      *d++ = (i<<4) | j;
    } else if (i == '+') {
      *d++ = ' ';
    } else {
      *d++ = i;
    }
  }
  return d - dst.data();
}

}

#endif /* ZuPercent_HH */
