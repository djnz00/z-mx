//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <iostream>

#include <zlib/ZuCSpan.hh>
#include <zlib/ZuBase32.hh>

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
  auto n = ZuBase32::enclen(src.length());
  char *buf = static_cast<char *>(ZuAlloca(n, 1));
  auto dst = ZuSpan<uint8_t>(buf, n);
  dst.trunc(ZuBase32::encode(dst, src));
  encOut(ZuCSpan(dst) == check, msg, ZuCSpan(dst));
}

void dec(ZuBytes src, ZuBytes check, const char *msg) {
  auto n = ZuBase32::declen(src.length());
  char *buf = static_cast<char *>(ZuAlloca(n, 1));
  auto dst = ZuSpan<uint8_t>(buf, n);
  dst.trunc(ZuBase32::decode(dst, src));
  decOut(ZuBytes(dst) == check, msg, dst);
}

#define TEST(src, dst) \
  enc(src, dst, #src " -> " #dst); \
  dec(dst, src, #dst " -> " #src);

int main()
{
  TEST((ZuBytes{ }), "");
  TEST((ZuBytes{ 2 }), "AI======");
  TEST((ZuBytes{ 2, 4 }), "AICA====");
  TEST((ZuBytes{ 2, 4, 6 }), "AICAM===");
  TEST((ZuBytes{ 2, 4, 6, 8 }), "AICAMCA=");
  TEST((ZuBytes{ 2, 4, 6, 8, 10 }), "AICAMCAK");
  TEST((ZuBytes{ 2, 4, 6, 8, 10, 12 }), "AICAMCAKBQ======");
  TEST((ZuBytes{ 0x11 }), "CE======");
  TEST((ZuBytes{ 0x11, 0x22 }), "CERA====");
  TEST((ZuBytes{ 0x11, 0x22, 0x33 }), "CERDG===");
  TEST((ZuBytes{ 0x11, 0x22, 0x33, 0x44 }), "CERDGRA=");
  TEST((ZuBytes{ 0x11, 0x22, 0x33, 0x44, 0x55 }), "CERDGRCV");
  TEST((ZuBytes{ 0x11, 0x22, 0x33, 0x44, 0x55, 0x66 }), "CERDGRCVMY======");
  TEST((ZuBytes{ 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77 }), "CERDGRCVMZ3Q====");
}
