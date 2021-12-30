//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuLib.hpp>

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include <zlib/ZuBitmap.hpp>
#include <zlib/ZuStringN.hpp>

int main()
{
  ZuBitmap<256> a;
  a.set(2, 6);
  a.set(10, 15);
  a.set(100, 256);
  ZuStringN<100> s;
  s << a;
  std::cout << s << '\n' << std::flush;
  ZuBitmap<256> b(s);
  std::cout << b << '\n' << std::flush;
}
