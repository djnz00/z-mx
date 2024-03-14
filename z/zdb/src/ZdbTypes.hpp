//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// Z Database vocabulary types

#ifndef ZdbTypes_HPP
#define ZdbTypes_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdbLib_HPP
#include <zlib/ZdbLib.hpp>
#endif

#include <zlib/ZuInt.hpp>
#include <zlib/ZuCmp.hpp>

namespace Zdb_ {

class DB;			// opaque to data store modules

// Note: at 100K TPS sustained it takes 262,000 years to exhaust a 64bit UN

// record number type and sentinel values
using RN = uint64_t;		// RN is primary object key / ID
inline constexpr RN maxRN() { return ZuCmp<RN>::maximum(); }
inline constexpr RN nullRN() { return ZuCmp<RN>::null(); }

// update number - secondary key used for replication/recovery
using UN = uint64_t;
inline constexpr UN maxUN() { return ZuCmp<UN>::maximum(); }
inline constexpr UN nullUN() { return ZuCmp<UN>::null(); }

// environment sequence number
using SN = uint128_t;
inline constexpr SN maxSN() { return ZuCmp<SN>::maximum(); }
inline constexpr SN nullSN() { return ZuCmp<SN>::null(); }

// record version number
using VN = uint32_t;

} // Zdb_

#endif /* ZdbTypes_HPP */
