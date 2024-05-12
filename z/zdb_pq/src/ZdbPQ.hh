//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#ifndef ZdbPQ_HH
#define ZdbPQ_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdbLib_HH
#include <zlib/ZdbPQLib.hh>
#endif

#include <libpq-fe.h>

#include <zlib/ZdbStore.hh>

// (*)  Postgres uint extension https://github.com/djnz00/pguint
// (**) Z Postgres extension
//
// C++		flatbuffers	pg_type.typname	pg send/receive format
//
// bool		Bool		bool		uint8_t BE
// int8_t	Byte		int1 (*)	int8_t BE
// uint8_t	UByte		uint1 (*)	uint8_t BE
// int16_t	Short		int2		int16_t BE
// uint16_t	UShort		uint2 (*)	uint16_t BE
// int32_t	Int		int4		int16_t BE
// uint32_t	UInt		uint4 (*)	uint32_t BE
// int64_t	Long		int8		int64_t BE
// uint64_t	ULong		uint8 (*)	uint64_t BE
// float	Float		float4		float | int32_t BE
// double	Double		float8		double | int64_t BE
// <string>	String		text		uint32_t len, null-term. data
// 						(len might not be byteswapped?)
//
// ZuFixed	Zfb.Fixed	zdecimal (**)	int128_t BE
// ZuDecimal	Zfb.Decimal	zdecimal (**)	int128_t BE
// ZuTime	Zfb.Time	timestamp	int64_t BE in microseconds
// 						epoch is 2000/1/1 00:00:00
// ZuDateTime	Zfb.DateTime	timestamp	''
// int128_t	Zfb.Int128	int16 (*)	int128_t BE
// uint128_t	Zfb.UInt128	uint16 (*)	uint128_t BE
// ZiIP		Zfb.IP		inet		4 header bytes {
// 						  family(AF_INET=2),
// 						  bits(32),
// 						  is_cidr(false),
// 						  len(4)
// 						}, uint32_t BE address
// ZuID		Zfb.ID		text		see above

namespace ZdbPQ {

}

#endif /* ZdbPQ_HH */
