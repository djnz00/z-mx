//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// char type normalization
//
// char/signed char/unsigned char/int8_t/uint8_t -> char
// wchar_t/short/unsigned short/int16_t/uint16_t -> wchar_t
//
// 16bit types are left as-is if wchar_t is not 16bit

#ifndef ZuNormChar_HH
#define ZuNormChar_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <wchar.h>

#include <zlib/ZuInt.hh>

template <typename U, typename W = wchar_t,
  bool = bool{ZuIsExact<U, char>{}} ||
	 bool{ZuIsExact<U, signed char>{}} ||
	 bool{ZuIsExact<U, unsigned char>{}} ||
	 bool{ZuIsExact<U, int8_t>{}} ||
	 bool{ZuIsExact<U, uint8_t>{}},
  bool = bool{ZuIsExact<U, W>{}} ||
	 (sizeof(W) == 2 && (
	       bool{ZuIsExact<U, short>{}} ||
	       bool{ZuIsExact<U, unsigned short>{}} ||
	       bool{ZuIsExact<U, int16_t>{}} ||
	       bool{ZuIsExact<U, uint16_t>{}})) ||
	 (sizeof(W) == 4 && (
	       bool{ZuIsExact<U, int32_t>{}} ||
	       bool{ZuIsExact<U, uint32_t>{}}))>
struct ZuNormChar_ { using T = U; };

template <typename U, typename W, bool _>
struct ZuNormChar_<U, W, 1, _> { using T = char; };

template <typename U, typename W>
struct ZuNormChar_<U, W, 0, 1> { using T = W; };

template <typename U>
using ZuNormChar = typename ZuNormChar_<ZuDecay<U>>::T;

#endif /* ZuNormChar_HH */
