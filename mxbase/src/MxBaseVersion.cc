//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// MxBase

#include <mxbase/MxBaseVersion.hh>

#include "../version.h"

unsigned long MxBaseVULong(int major, int minor, int patch)
{
  return MXBASE_VULONG(major, minor, patch);
}

unsigned long MxBaseVersion() { return MXBASE_VERSION; }
const char *MxBaseVerName() { return MXBASE_VERNAME; }

int MxBaseVMajor(unsigned long n)
{
  return MXBASE_VMAJOR(n);
}
int MxBaseVMinor(unsigned long n)
{
  return MXBASE_VMINOR(n);
}
int MxBaseVPatch(unsigned long n)
{
  return MXBASE_VPATCH(n);
}

