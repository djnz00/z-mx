//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxT version

#ifndef MxTVersion_HH
#define MxTVersion_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxTExtern
#ifdef _WIN32
#ifdef MXT_EXPORTS
#define MxTExtern extern __declspec(dllexport)
#else
#define MxTExtern extern __declspec(dllimport)
#endif
#else
#define MxTExtern extern
#endif
#endif

extern "C" {
  MxTExtern unsigned long MxTVULong(int major, int minor, int patch);
  MxTExtern unsigned long MxTVersion();
  MxTExtern const char *MxTVerName();
  MxTExtern int MxTVMajor(unsigned long n);
  MxTExtern int MxTVMinor(unsigned long n);
  MxTExtern int MxTVPatch(unsigned long n);
};

#endif /* MxTVersion_HH */
