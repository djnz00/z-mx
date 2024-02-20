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

// normalize:
//
// char/signed char/unsigned char/int8_t/uint8_t -> char
// wchar_t/short/unsigned short/int16_t/uint16_t -> wchar_t
//
// leaves cv-qualifiers intact and all other types unchanged
//
// 16bit types are left unchanged if wchar_t is not 16bit

#ifndef ZuEquivChar_HPP
#define ZuEquivChar_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#include <wchar.h>

#include <zlib/ZuConversion.hpp>
#include <zlib/ZuInt.hpp>

#ifdef _MSC_VER
#pragma once
#endif

template <typename U, typename W = wchar_t,
  bool = ZuConversion<U, char>::Same ||
	 ZuConversion<U, signed char>::Same ||
	 ZuConversion<U, unsigned char>::Same ||
	 ZuConversion<U, int8_t>::Same ||
	 ZuConversion<U, uint8_t>::Same,
  bool = ZuConversion<U, W>::Same ||
	 (sizeof(W) == 2 && (
	       ZuConversion<U, short>::Same ||
	       ZuConversion<U, unsigned short>::Same ||
	       ZuConversion<U, int16_t>::Same ||
	       ZuConversion<U, uint16_t>::Same)) ||
	 (sizeof(W) == 4 && (
	       ZuConversion<U, int32_t>::Same ||
	       ZuConversion<U, uint32_t>::Same))>
struct ZuNormChar_ { using T = U; };

template <typename U, typename W, bool _>
struct ZuNormChar_<U, W, 1, _> { using T = char; };
template <typename U, typename W, bool _>
struct ZuNormChar_<const U, W, 1, _> { using T = const char; };
template <typename U, typename W, bool _>
struct ZuNormChar_<volatile U, W, 1, _> { using T = volatile char; };
template <typename U, typename W, bool _>
struct ZuNormChar_<const volatile U, W, 1, _> {
  using T = const volatile char;
};

template <typename U, typename W>
struct ZuNormChar_<U, W, 0, 1> { using T = W; };
template <typename U, typename W>
struct ZuNormChar_<const U, W, 0, 1> { using T = const W; };
template <typename U, typename W>
struct ZuNormChar_<volatile U, W, 0, 1> { using T = volatile W; };
template <typename U, typename W>
struct ZuNormChar_<const volatile U, W, 0, 1> { using T = const volatile W; };

template <typename U>
using ZuNormChar = typename ZuNormChar_<U>::T;

template <typename U1, typename U2>
struct ZuEquivChar : public ZuConversion<ZuNormChar<U1>, ZuNormChar<U2>> { };

#endif /* ZuEquivChar_HPP */
