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

namespace ZdbPQ {

// (*)  Postgres uint extension https://github.com/petere/pguint
// (**) Z Postgres extension
//
// flatbuffers	pg_type.typname
//
// Bool		bool
// Byte		int1 (*)
// UByte	uint1 (*)
// Short	int2
// UShort	uint2 (*)
// Int		int4
// UInt		uint4 (*)
// Long		int8
// ULong	uint8 (*)
// Float	float4
// Double	float8
// String	text
//
// Fixed	(**)		z_decimal
// Decimal	(**)		z_decimal
// Time		(**)		z_time
// Date		(**)		z_date_time
// int128_t	(**)		z_int128
// uint128_t	(**)		z_uint128
// ZiIP		inet
// ZuID		char[8]

}

#endif /* ZdbPQ_HH */
