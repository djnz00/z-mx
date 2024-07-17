//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#ifndef ZtelClient_HH
#define ZtelClient_HH

#ifndef ZcmdLib_HH
#include <zlib/ZcmdLib.hh>
#endif

#include <zlib/ZuLambdaTraits.hh>

#include <zlib/ZmFn.hh>

#include <zlib/ZtRegex.hh>

#include <zlib/ZvEngine.hh>

#include <zlib/Ztls.hh>

#include <zlib/Zfb.hh>

#include <zlib/Zdb.hh>

// #include <zlib/Zcmd.hh>
#include <zlib/Ztel.hh>

namespace Ztel {

enum { ReqIOBufSize = 128 }; // built-in I/O buffer size

using ReqIOBufAlloc = ZiIOBufAlloc<ReqIOBufSize>;

} // Ztel

#endif /* ZtelClient_HH */
