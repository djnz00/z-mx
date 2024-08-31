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

enum { BlkSize = 4096 };

using Shard = uint8_t;			// shard

using DFID = uint32_t;			// data frame ID

using SeriesID = uint32_t;		// series ID

using BlkOffset = uint64_t;		// block offset
using Offset = uint64_t;		// value offset

using Fixed0 = ZuBox0(int64_t);
using Float0 = ZuBox0(double);
using NDP = ZuFixedNDP;			// number of decimal places

using BlkCount = uint16_t;		// count of values within a block

using Event = ZeVEvent;			// monomorphic ZeEvent

} // namespace Zdf

#endif /* ZdfTypes_HH */
