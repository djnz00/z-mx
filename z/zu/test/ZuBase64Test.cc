//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <iostream>

#include <zlib/ZuCSpan.hh>
#include <zlib/ZuBase64.hh>

inline void encOut(bool ok, const char *msg, ZuCSpan actual) {
  std::cout << (ok ? "OK  " : "NOK ") << msg << '\n';
  if (!ok) std::cout << "  " << actual << '\n';
}

inline void decOut(bool ok, const char *msg, ZuBytes actual) {
  std::cout << (ok ? "OK  " : "NOK ") << msg << '\n';
  if (!ok) {
    std::cout << "  ";
    unsigned n = actual.length();
    char *buf = static_cast<char *>(ZuAlloca(n * 3, 1));
    char *ptr = buf;
    for (unsigned i = 0; i < n; i++) {
      if (i) *ptr++ = ' ';
      auto c = actual[i];
      static auto hex = [](uint8_t v) {
	return v < 10 ? v + '0' : (v - 10) + 'A';
      };
      *ptr++ = hex(c>>4);
      *ptr++ = hex(c & 0xf);
    }
    std::cout << ZuCSpan(buf, ptr - buf) << '\n';
  }
}

void enc(ZuBytes src, ZuCSpan check, const char *msg) {
  auto n = ZuBase64::enclen(src.length());
  char *buf = static_cast<char *>(ZuAlloca(n, 1));
  auto dst = ZuSpan<uint8_t>(buf, n);
  dst.trunc(ZuBase64::encode(dst, src));
  encOut(ZuCSpan(dst) == check, msg, ZuCSpan(dst));
}

void dec(ZuBytes src, ZuBytes check, const char *msg) {
  auto n = ZuBase64::declen(src.length());
  char *buf = static_cast<char *>(ZuAlloca(n, 1));
  auto dst = ZuSpan<uint8_t>(buf, n);
  dst.trunc(ZuBase64::decode(dst, src));
  decOut(ZuBytes(dst) == check, msg, dst);
}

#define TEST(src, dst) \
  enc(src, dst, #src " -> " #dst); \
  dec(dst, src, #dst " -> " #src);

int main()
{
  TEST((ZuBytes{ }), "");
  TEST((ZuBytes{ 2 }), "Ag==");
  TEST((ZuBytes{ 2, 4 }), "AgQ=");
  TEST((ZuBytes{ 2, 4, 6 }), "AgQG");
  TEST((ZuBytes{ 0x11 }), "EQ==");
  TEST((ZuBytes{ 0x11, 0x22 }), "ESI=");
  TEST((ZuBytes{ 0x11, 0x22, 0x33 }), "ESIz");
  TEST((ZuBytes{ 0x11, 0x22, 0x33, 0x44 }), "ESIzRA==");
  TEST((ZuBytes{ 0x11, 0x22, 0x33, 0x44, 0x55 }), "ESIzRFU=");
  TEST((ZuBytes{ 0x11, 0x22, 0x33, 0x44, 0x55, 0x66 }), "ESIzRFVm");
  TEST((ZuBytes{ 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77 }), "ESIzRFVmdw==");
}
