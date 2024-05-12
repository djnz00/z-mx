//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZtLib.hh>

#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <limits.h>
#include <math.h>

#include <zlib/ZuDateTime.hh>

#include <zlib/ZmPlatform.hh>
#include <zlib/ZmTime.hh>

#define CHECK(x) ((x) ? puts("OK  " #x) : puts("NOK " #x))

struct Null {
  template <typename S> void print(S &s) const { s << "null"; }
  friend ZuPrintFn ZuPrintType(Null *);
};

void test(ZuDateTime d1)
{
  ZuStringN<32> fix;
  ZuDateTimeFmt::FIX<-9, Null> fmt;
  fix << d1.print(fmt);
  puts(fix);
  ZuDateTime d2(ZuDateTimeScan::FIX{}, fix);
  puts(ZuStringN<32>() << d2.print(fmt));
  CHECK(d1 == d2);
}

int main(int argc, char **argv)
{
  test(ZuDateTime{time_t(0)});
  test(ZuDateTime{1, 1, 1});
  test(ZuDateTime{Zm::now()});

  if (argc < 2) { fputs("usage: ZuDateTimeFixTest N\n", stderr); Zm::exit(1); }
  unsigned n = atoi(argv[1]);
  {
    ZuDateTimeFmt::FIX<-9, Null> fmt;
    ZuStringN<32> fix;
    ZuTime start, end;
    start = Zm::now();
    for (unsigned i = 0; i < n; i++) {
      ZuDateTime d1{Zm::now()};
      fix << d1.print(fmt);
      ZuDateTime d2{ZuDateTimeScan::FIX{}, fix};
    }
    end = Zm::now();
    end -= start;
    ZuTime d1 = end;
    printf("time per cycle 1: %s\n",
      (ZuStringN<32>{} << (end / (long double)n).interval()).data());

    start = Zm::now();
    for (unsigned i = 0; i < n; i++) {
      ZuDateTime d1{Zm::now()};
      fix << d1.print(fmt);
    }
    end = Zm::now();
    end -= start;
    ZuTime d2 = end;
    printf("time per cycle 2: %s\n",
      (ZuStringN<32>{} << (end / (long double)n).interval()).data());

    start = Zm::now();
    for (unsigned i = 0; i < n; i++) {
      ZuDateTime d1{Zm::now()};
    }
    end = Zm::now();
    end -= start;
    ZuTime d3 = end;
    printf("time per cycle 3: %s\n",
      (ZuStringN<32>{} << (end / (long double)n).interval()).data());

    printf("time per FIX format print: %s\n",
      (ZuStringN<32>{} << (d2 - d3).interval()).data());
    printf("time per FIX format scan: %s\n",
      (ZuStringN<32>{} << (d1 - d2).interval()).data());
  }
}
