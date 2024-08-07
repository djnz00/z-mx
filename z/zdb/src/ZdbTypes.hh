//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Z Database vocabulary types

#ifndef ZdbTypes_HH
#define ZdbTypes_HH

#ifndef ZdbLib_HH
#include <zlib/ZdbLib.hh>
#endif

#include <zlib/ZuInt.hh>
#include <zlib/ZuCmp.hh>

#include <zlib/Zfb.hh>

#include <zlib/zdb_cache_mode_fbs.h>
#include <zlib/zdb_host_state_fbs.h>

namespace Zdb_ {

// various upper limits
constexpr unsigned maxFields() { return 0x3fff; }
constexpr unsigned maxKeys() { return 0x7ff; }

// Note: at 100K TPS sustained it takes 262,000 years to exhaust a 64bit UN

// update number - secondary key used for replication/recovery
using UN = uint64_t;
inline constexpr UN maxUN() { return ZuCmp<UN>::maximum(); }
inline constexpr UN nullUN() { return ZuCmp<UN>::null(); }

// environment sequence number
using SN = uint128_t;
inline constexpr SN maxSN() { return ZuCmp<SN>::maximum(); }
inline constexpr SN nullSN() { return ZuCmp<SN>::null(); }

// record version number - negative indicates a deleted record
using VN = int64_t;

namespace CacheMode {
  namespace fbs = Ztel::fbs;
  ZfbEnumValues(DBCacheMode,
    Normal,
    All)
}

namespace HostState {
  namespace fbs = Ztel::fbs;
  ZfbEnumValues(DBHostState,
    Instantiated,
    Initialized,
    Electing,
    Active,
    Inactive,
    Stopping)
}

namespace ObjState {
  ZtEnumValues(ObjState, int8_t,
    Undefined = 0,
    Insert,
    Update,
    Committed,
    Delete,
    Deleted);
}

} // Zdb_

using ZdbUN = Zdb_::UN;
#define ZdbNullUN Zdb_::nullUN
#define ZdbMaxUN Zdb_::maxUN
using ZdbSN = Zdb_::SN;
using ZdbVN = Zdb_::VN;
#define ZdbNullSN Zdb_::nullSN
#define ZdbMaxSN Zdb_::maxSN

namespace ZdbCacheMode = Zdb_::CacheMode;
namespace ZdbHostState = Zdb_::HostState;
namespace ZdbObjState = Zdb_::ObjState;

#endif /* ZdbTypes_HH */
