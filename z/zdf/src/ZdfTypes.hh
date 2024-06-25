//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Zdf vocabulary types

#ifndef ZdfTypes_HH
#define ZdfTypes_HH

#ifndef ZdfLib_HH
#include <zlib/ZdfLib.hh>
#endif

#include <zlib/ZePlatform.hh>

namespace Zdf {

using Event = ZeMEvent;				// monomorphic ZeEvent

using OpenResult = ZuUnion<void, Event>;	// open result
using OpenFn = ZmFn<void(OpenResult)>;		// open callback

using CloseResult = ZuUnion<void, Event>;	// close result
using CloseFn = ZmFn<void(CloseResult)>;		// close callback

} // namespace Zdf

#endif /* ZdfTypes_HH */
