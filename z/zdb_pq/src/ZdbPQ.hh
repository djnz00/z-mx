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
// (**) libz Postgres extension
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
// ZuTime	Zfb.Time	ztime		int64_t BE, int32_t BE
// ZuDateTime	Zfb.DateTime	ztime		''
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

namespace PQStoreTbl {

using namespace Zdb_;
using namespace Zdb_::StoreTbl_;

class StoreTbl : public Interface {
public:
  StoreTbl(
    ZuID id, ZtMFields fields, ZtMKeyFields keyFields,
    const reflection::Schema *schema);

protected:
  ~StoreTbl();

public:
  void open() { }
  void close() { }

  void warmup() { }

  void drop() { }

  void maxima(MaxFn maxFn) { }

  void find(unsigned keyID, ZmRef<const AnyBuf> buf, RowFn rowFn) { }

  void recover(UN un, RowFn rowFn) { }

  void write(ZmRef<const AnyBuf> buf, CommitFn commitFn) { }

private:
  ZuID			m_id;
  ZtMFields		m_fields;
  ZtMKeyFields		m_keyFields;
  FBFields		m_fbFields;
  FBKeyFields		m_fbKeyFields;
  ZmRef<AnyBuf>		m_maxBuf;
};

} // PQStoreTbl

// --- mock data store

namespace PQStore {

using namespace Zdb_;
using namespace Zdb_::Store_;

using StoreTbl = PQStoreTbl::StoreTbl;

class Store : public Interface, public ZmPolymorph {
public:
  InitResult init(ZvCf *cf, LogFn);
  void final();

  // queue of pending requests
  // PQ socket, wakeup
  // storeTbls can add requests
  // responses are dispatched back to storeTbls
  // requests include completion lambdas, relevant state
  // start with a simple single connection, add failover later

  void open(
      ZuID id,
      ZtMFields fields,
      ZtMKeyFields keyFields,
      const reflection::Schema *schema,
      MaxFn maxFn,
      OpenFn openFn);

private:
  PGconn	*m_conn;
  int		m_socket;
#ifndef _WIN32
  int		m_epollFD = -1;
  int		m_wakeFD = -1, m_wakeFD2 = -1;		// wake pipe
#else
  HANDLE	m_connEvent = INVALID_HANDLE_VALUE;	// connection event
  HANDLE	m_wakeEvent = INVALID_HANDLE_VALUE;	// wake event
#endif
};

} // PQStore

using Store = PQStore::Store;

} // ZdbPQ

#endif /* ZdbPQ_HH */
