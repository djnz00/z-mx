//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2

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

// Multicast Packet Capture Header

#ifndef MxMCapHdr_HPP
#define MxMCapHdr_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxMDLib_HPP
#include <mxbase/MxBaseLib.hpp>
#endif

#pragma pack(push, 1)

// multicast capture header
struct MxMCapHdr {
  uint16_t	len = 0;	// exclusive of MxMCapHdr
  uint16_t	group = 0;
  int64_t	sec = 0;
  uint32_t	nsec = 0;
};

#pragma pack(pop)

#endif /* MxMCapHdr_HPP */
