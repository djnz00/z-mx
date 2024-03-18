//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuLib.hpp>

#include <stdio.h>

#include <zlib/ZmTime.hpp>

void fail() { Zm::exit(1); }

#define CHECK(x) ((x) ? (puts("OK  " #x), void()) : (puts("NOK " #x), fail()))

int main()
{
  ZmTime t;
  CHECK(!*t);
  CHECK(!!t);
  CHECK(t);
  ZmTime t2 = 0;
  CHECK(*t2);
  CHECK(!t2);
}
