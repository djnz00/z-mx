//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <iostream>

#include <zlib/ZuStringN.hh>

#include <zlib/ZdfCompress.hh>
#include <zlib/ZdfSeries.hh>
#include <zlib/ZdfMockStore.hh>

void print(const char *s) {
  std::cout << s << '\n' << std::flush;
}
void print(const char *s, int64_t i) {
  std::cout << s << ' ' << i << '\n' << std::flush;
}
void ok(const char *s) { print(s); }
void ok(const char *s, int64_t i) { print(s, i); }
void fail(const char *s) { print(s); }
void fail(const char *s, int64_t i) { print(s, i); }
#define CHECK(x) ((x) ? ok("OK  " #x) : fail("NOK " #x))
#define CHECK2(x, y) ((x == y) ? ok("OK  " #x, x) : fail("NOK " #x, x))

int main()
{
  using namespace Zdf;
  using namespace ZdfCompress;
  MockStore store;
  store.init(nullptr, nullptr);
  Series s;
  s.init(&store);
  ZmBlock<>{}([&s](auto wake) {
    s.open("test", "test", [wake = ZuMv(wake)](OpenResult) mutable { wake(); });
  });
  {
    auto w = s.writer<DeltaEncoder<>>();
    CHECK(w.write(ZuFixed{42, 0}));
    CHECK(w.write(ZuFixed{42, 0}));
    CHECK(w.write(ZuFixed{4301, 2}));
    CHECK(w.write(ZuFixed{4302, 2}));
    CHECK(w.write(ZuFixed{43030, 3}));
    CHECK(w.write(ZuFixed{43040, 3}));
    CHECK(w.write(ZuFixed{430500, 4}));
    CHECK(w.write(ZuFixed{430600, 4}));
    for (unsigned i = 0; i < 300; i++) {
      CHECK(w.write(ZuFixed{430700, 4}));
    }
  }
  CHECK(s.blkCount() == 4);
  {
    auto r = s.seek<DeltaDecoder<>>();
    ZuFixed v;
    CHECK(r.read(v)); CHECK(v.mantissa() == 42 && !v.ndp());
    CHECK(r.read(v)); CHECK(v.mantissa() == 42 && !v.ndp());
    CHECK(r.read(v)); CHECK(v.mantissa() == 4301 && v.ndp() == 2);
    CHECK(r.read(v)); CHECK(v.mantissa() == 4302 && v.ndp() == 2);
    CHECK(r.read(v)); CHECK(v.mantissa() == 43030 && v.ndp() == 3);
    CHECK(r.read(v)); CHECK(v.mantissa() == 43040 && v.ndp() == 3);
    CHECK(r.read(v)); CHECK(v.mantissa() == 430500 && v.ndp() == 4);
    CHECK(r.read(v)); CHECK(v.mantissa() == 430600 && v.ndp() == 4);
    for (unsigned i = 0; i < 300; i++) {
      CHECK(r.read(v));
      CHECK(v.mantissa() == 430700 && v.ndp() == 4);
    }
    CHECK(!r.read(v));
  }
  {
    auto r = s.find<DeltaDecoder<>>(ZuFixed{425, 1});
    ZuFixed v;
    CHECK(r.read(v)); CHECK(v.mantissa() == 4301 && v.ndp() == 2);
  }
  {
    auto r = s.find<DeltaDecoder<>>(ZuFixed{43020, 3});
    ZuFixed v;
    CHECK(r.read(v)); CHECK(v.mantissa() == 4302 && v.ndp() == 2);
    r.purge();
  }
  CHECK(s.blkCount() == 3);
  {
    auto r = s.find<DeltaDecoder<>>(ZuFixed{44, 0});
    ZuFixed v;
    CHECK(!r);
    CHECK(!r.read(v));
  }
  {
    auto r = s.seek<DeltaDecoder<>>();
    ZuFixed v;
    CHECK(r.read(v)); CHECK(v.mantissa() == 4301 && v.ndp() == 2);
  }
  {
    auto r = s.seek<DeltaDecoder<>>(208);
    ZuFixed v;
    for (unsigned i = 0; i < 50; i++) {
      CHECK(r.read(v));
      CHECK(v.mantissa() == 430700 && v.ndp() == 4);
    }
    CHECK(r.offset() == 258);
    for (unsigned i = 0; i < 50; i++) {
      CHECK(r.read(v));
      CHECK(v.mantissa() == 430700 && v.ndp() == 4);
    }
    CHECK(!r.read(v));
  }
  ZmBlock<>{}([&s](auto wake) {
    s.close([wake = ZuMv(wake)](CloseResult) mutable { wake(); });
  });
  // s.final();
  // store.final();
}
