//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// MxMD version

#ifndef MxMDVersion_HH
#define MxMDVersion_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxMDExtern
#ifdef _WIN32
#ifdef MXMD_EXPORTS
#define MxMDExtern extern __declspec(dllexport)
#else
#define MxMDExtern extern __declspec(dllimport)
#endif
#else
#define MxMDExtern extern
#endif
#endif

extern "C" {
  MxMDExtern unsigned long MxMDVULong(int major, int minor, int patch);
  MxMDExtern unsigned long MxMDVersion();
  MxMDExtern const char *MxMDVerName();
  MxMDExtern int MxMDVMajor(unsigned long n);
  MxMDExtern int MxMDVMinor(unsigned long n);
  MxMDExtern int MxMDVPatch(unsigned long n);
};

#endif /* MxMDVersion_HH */
