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

// consistent scanning of bool values

#ifndef ZtScanBool_HPP
#define ZtScanBool_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZtLib_HPP
#include <zlib/ZtLib.hpp>
#endif

#include <zlib/ZuICmp.hpp>
#include <zlib/ZuString.hpp>

struct ZtBadBool { };

template <bool Validate = false>
inline bool ZtScanBool(ZuString s)
{
  using Cmp = ZuICmp<ZuString>;
  if (s == "1" ||
      Cmp::equals(s, "y") ||
      Cmp::equals(s, "yes") ||
      Cmp::equals(s, "true"))
    return true;
  if constexpr (Validate) {
    if (s == "0" ||
	Cmp::equals(s, "n") ||
	Cmp::equals(s, "no") ||
	Cmp::equals(s, "false"))
      return false;
    throw ZtBadBool{};
  } else
    return false;
}

#endif /* ZtScanBool_HPP */
