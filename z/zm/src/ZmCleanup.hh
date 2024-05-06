//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// sequenced cleanup for ZmSingleton/ZmSpecific

#ifndef ZmCleanup_HH
#define ZmCleanup_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

namespace ZmCleanup {
  enum { Application = 0, Library, Platform, Heap, HeapMgr, Thread, Final, N };
}

ZuUnsigned<ZmCleanup::Application> ZmCleanupLevel(...);

#endif /* ZmCleanup_HH */
