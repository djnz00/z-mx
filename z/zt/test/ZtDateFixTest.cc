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

#include <zlib/ZmPlatform.hh>

#include <zlib/ZtDate.hh>

#define CHECK(x) ((x) ? puts("OK  " #x) : puts("NOK " #x))

struct Null {
  template <typename S> void print(S &s) const { s << "null"; }
  friend ZuPrintFn ZuPrintType(Null *);
};

void test(ZtDate d1)
{
  ZuStringN<32> fix;
  ZtDateFmt::FIX<-9, Null> fmt;
  fix << d1.print(fmt);
  puts(fix);
  ZtDate d2(ZtDateScan::FIX{}, fix);
  puts(ZuStringN<32>() << d2.print(fmt));
  CHECK(d1 == d2);
}

int main(int argc, char **argv)
{
  test(ZtDate((time_t)0));
  test(ZtDate(1, 1, 1));
  test(ZtDateNow());

  if (argc < 2) { fputs("usage: ZtDateFixTest N\n", stderr); Zm::exit(1); }
  unsigned n = atoi(argv[1]);
  {
    ZtDateFmt::FIX<-9, Null> fmt;
    ZuStringN<32> fix;
    ZuTime start, end;
    start = Zm::now();
    for (unsigned i = 0; i < n; i++) {
      ZtDate d1{ZtDate::Now};
      fix << d1.print(fmt);
      ZtDate d2{ZtDateScan::FIX{}, fix};
    }
    end = Zm::now();
    end -= start;
    double d1 = end.dtime() / (double)n;
    printf("time per cycle 1: %.9f\n", d1);

    start = Zm::now();
    for (unsigned i = 0; i < n; i++) {
      ZtDate d1{ZtDate::Now};
      fix << d1.print(fmt);
    }
    end = Zm::now();
    end -= start;
    double d2 = end.dtime() / (double)n;
    printf("time per cycle 2: %.9f\n", d2);

    start = Zm::now();
    for (unsigned i = 0; i < n; i++) {
      ZtDate d1{ZtDate::Now};
    }
    end = Zm::now();
    end -= start;
    double d3 = end.dtime() / (double)n;
    printf("time per cycle 3: %.9f\n", d3);

    printf("time per FIX format print: %.9f\n", d2 - d3);
    printf("time per FIX format scan: %.9f\n", d1 - d2);
  }
}
