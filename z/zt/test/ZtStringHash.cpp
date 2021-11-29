//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuLib.hpp>

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include <float.h>

#include <zlib/ZtString.hpp>

#include "../../zu/test/Analyze.hpp"

int main()
{
  FILE *f = fopen("words", "r");
  if (!f) { perror("words"); Zm::exit(1); }
  char buf[512];
  int count[1024];
  memset(count, 0, sizeof(count));
  while (fgets(buf, 512, f)) {
    buf[511] = 0;
    ZtString s; s -= buf;
    int n = s.length();
    if (n <= 1) continue;
    s.length(n - 1);
    count[s.hash() & 1023]++;
  }
  fclose(f);
  analyze("string", count, 1024);
}
