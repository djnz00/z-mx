//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <iostream>

#include <zlib/ZuCSpan.hh>
#include <zlib/ZuHex.hh>

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
  auto n = ZuHex::enclen(src.length());
  char *buf = static_cast<char *>(ZuAlloca(n, 1));
  auto dst = ZuSpan<uint8_t>(buf, n);
  dst.trunc(ZuHex::encode(dst, src));
  encOut(ZuCSpan(dst) == check, msg, ZuCSpan(dst));
}

void dec(ZuBytes src, ZuBytes check, const char *msg) {
  auto n = ZuHex::declen(src.length());
  char *buf = static_cast<char *>(ZuAlloca(n, 1));
  auto dst = ZuSpan<uint8_t>(buf, n);
  dst.trunc(ZuHex::decode(dst, src));
  decOut(ZuBytes(dst) == check, msg, dst);
}

#define TEST(src, dst) \
  enc(src, dst, #src " -> " #dst); \
  dec(dst, src, #dst " -> " #src);

int main()
{
  TEST((ZuBytes{ }), "");
  TEST((ZuBytes{ 2 }), "02");
  TEST((ZuBytes{ 2, 4 }), "0204");
  TEST((ZuBytes{ 2, 4, 6 }), "020406");
  TEST((ZuBytes{ 2, 4, 6, 8 }), "02040608");
  TEST((ZuBytes{ 2, 4, 6, 8, 10 }), "020406080A");
  TEST((ZuBytes{ 2, 4, 6, 8, 10, 12 }), "020406080A0C");
  TEST((ZuBytes{ 0xa1 }), "A1");
  TEST((ZuBytes{ 0xa1, 0x2b }), "A12B");
  TEST((ZuBytes{ 0xa1, 0x2b, 0xc3 }), "A12BC3");
  TEST((ZuBytes{ 0xa1, 0x2b, 0xc3, 0x4d }), "A12BC34D");
  TEST((ZuBytes{ 0xa1, 0x2b, 0xc3, 0x4d, 0xe5 }), "A12BC34DE5");
  TEST((ZuBytes{ 0xa1, 0x2b, 0xc3, 0x4d, 0xe5, 0x6f }), "A12BC34DE56F");
  TEST((ZuBytes{ 0xa1, 0x2b, 0xc3, 0x4d, 0xe5, 0x6f, 0xaa }), "A12BC34DE56FAA");
}
