//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxMD

#include <mxmd/MxMDVersion.hh>

#include "../version.h"

unsigned long MxMDVULong(int major, int minor, int patch)
{
  return MXMD_VULONG(major, minor, patch);
}

unsigned long MxMDVersion() { return MXMD_VERSION; }
const char *MxMDVerName() { return MXMD_VERNAME; }

int MxMDVMajor(unsigned long n)
{
  return MXMD_VMAJOR(n);
}
int MxMDVMinor(unsigned long n)
{
  return MXMD_VMINOR(n);
}
int MxMDVPatch(unsigned long n)
{
  return MXMD_VPATCH(n);
}

