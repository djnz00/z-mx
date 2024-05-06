//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// MxBase version

#ifndef MxBaseVersion_HH
#define MxBaseVersion_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxBaseExtern
#ifdef _WIN32
#ifdef MXBASE_EXPORTS
#define MxBaseExtern extern __declspec(dllexport)
#else
#define MxBaseExtern extern __declspec(dllimport)
#endif
#else
#define MxBaseExtern extern
#endif
#endif

extern "C" {
  MxBaseExtern unsigned long MxBaseVULong(int major, int minor, int patch);
  MxBaseExtern unsigned long MxBaseVersion();
  MxBaseExtern const char *MxBaseVerName();
  MxBaseExtern int MxBaseVMajor(unsigned long n);
  MxBaseExtern int MxBaseVMinor(unsigned long n);
  MxBaseExtern int MxBaseVPatch(unsigned long n);
};

#endif /* MxBaseVersion_HH */
