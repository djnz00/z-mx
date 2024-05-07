//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Zu library version

#include <zlib/ZuVersion.hh>

#include "../../version.h"

unsigned long ZuVULong(int major, int minor, int patch)
{
  return Z_VULONG(major, minor, patch);
}

unsigned long ZuVersion() { return Z_VERSION; }
const char *ZuVerName() { return Z_VERNAME; }

int ZuVMajor(unsigned long n)
{
  return Z_VMAJOR(n);
}
int ZuVMinor(unsigned long n)
{
  return Z_VMINOR(n);
}
int ZuVPatch(unsigned long n)
{
  return Z_VPATCH(n);
}
