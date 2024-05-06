//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// Zu library version

#ifndef ZuVersion_HH
#define ZuVersion_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZuExtern
#ifdef _WIN32
#ifdef ZU_EXPORTS
#define ZuExtern extern __declspec(dllexport)
#else
#define ZuExtern extern __declspec(dllimport)
#endif
#else
#define ZuExtern extern
#endif
#endif

extern "C" {
  ZuExtern unsigned long ZuVULong(int major, int minor, int patch);
  ZuExtern unsigned long ZuVersion();
  ZuExtern const char *ZuVerName();
  ZuExtern int ZuVMajor(unsigned long n);
  ZuExtern int ZuVMinor(unsigned long n);
  ZuExtern int ZuVPatch(unsigned long n);
};

#endif /* ZuVersion_HH */
