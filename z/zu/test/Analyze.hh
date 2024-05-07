//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <limits.h>
#include <math.h>
#include <stdio.h>

void analyze(const char *run, int *count, int n)
{
  double avg = 0;
  int min = INT_MAX, max = 0;
  for (int i = 0; i < n; i++) {
    avg += count[i];
    if (min > count[i]) min = count[i];
    if (max < count[i]) max = count[i];
  }
  avg /= n;
  double std = 0, delta;
  for (int i = 0; i < n; i++) {
    delta = avg - count[i];
    std += delta * delta;
  }
  std = sqrt(std / n);
  printf("%s min %d max %d avg: %g\n"
	 "     std (68%% CI): %5.4g %.4g%%\n"
	 "  2x std (95%% CI): %5.4g %.4g%%\n",
	 run, min, max, avg, std, std / avg, std * 2, (std * 2) / avg);
}
