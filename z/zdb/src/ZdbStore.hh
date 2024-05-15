//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Z Database data store interface

#ifndef ZdbStore_HH
#define ZdbStore_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdbLib_HH
#include <zlib/ZdbLib.hh>
#endif

#include <flatbuffers/reflection.h>

#include <zlib/ZuUnion.hh>

#include <zlib/ZmPolymorph.hh>

#include <zlib/ZtField.hh>

#include <zlib/ZePlatform.hh>

#include <zlib/ZvCf.hh>

#include <zlib/ZdbBuf.hh>
#include <zlib/ZdbMsg.hh>
#include <zlib/ZdbTypes.hh>

namespace Zdb_ {

// monomorphic ZeEvent
using Event = ZeMEvent;

// row data
struct RowData {
  ZmRef<const AnyBuf>	buf;	// replication message
};
// row result
using RowResult = ZuUnion<
  void,			// missing
  RowData,		// succeeded
  Event>;		// error

// row callback
using RowFn = ZmFn<RowResult>;

// max data (returned by maxima() for all series keys)
struct MaxData {
  unsigned		keyID;
  ZmRef<const AnyBuf>	buf;	// key data, no replication message header
};
// max callback
using MaxFn = ZmFn<MaxData>;	// must process buf contents synchronously

// commit result
using CommitResult = ZuUnion<void, Event>;
// commit callback
using CommitFn = ZmFn<ZmRef<const AnyBuf>, CommitResult>;

// table close callback
using CloseFn = ZmFn<>;

// backing data store table namespace
namespace StoreTbl_ {

// backing table interface
struct Interface {
  virtual void close() = 0;	// idempotent, synchronous

  virtual void warmup() = 0;

  // buf contains key data, no replication message header
  virtual void find(unsigned keyID, ZmRef<const AnyBuf>, RowFn) = 0;

  virtual void recover(UN, RowFn) = 0;

  // buf contains replication message, UN is idempotency key
  virtual void write(ZmRef<const AnyBuf>, CommitFn) = 0;	// idempotent
};
} // StoreTbl_;
using StoreTbl = StoreTbl_::Interface;

// backing data store namespace
namespace Store_ {

// log function
using LogFn = ZmFn<Event>;

// result of store init
struct InitData {
  bool		replicated = false;	// replicated data store?
};
using InitResult = ZuUnion<
  InitData,			// succeeded
  Event>;			// error

// opened table data
// - (*) un and sn may refer to trailing deletions
// - any Zdb data store needs to maintain a "most recent deletes" (MRD)
//   internal table, with rows primary-keyed on the table ID, containing
//   the UN and SN of the last delete applied to each table; an eventually
//   consistent batch, saga or transaction is used to combine deletion
//   from the table with an upsert to the table's corresponding row in the
//   MRD table; the table's MRD row is consulted on open to ensure accurate
//   "last UN" and "last SN" numbers are recovered and returned
struct OpenData {
  StoreTbl	*storeTbl = nullptr;
  uint64_t	count = 0;		// row count
  UN		un = 0;			// last UN applied to table (*)
  SN		sn = 0;			// last SN applied to table (*)
};
// result of table open
using OpenResult = ZuUnion<
  OpenData,			// succeeded
  Event>;			// error
// open callback
using OpenFn = ZmFn<OpenResult>;

// backing data store interface
struct Interface : public ZmPolymorph {
  // init and final are synchronous / blocking
  virtual InitResult init(		// initialize data store - idempotent
      ZvCf *cf,
      LogFn) = 0;
  virtual void final() = 0;		// finalize data store - idempotent

  // multiple calls to MaxFn may continue after open() returns,
  // concluding with a single call to OpenFn
  virtual void open(			// open table - idempotent, async
      ZuID id,				// name of table
      ZtMFields fields,			// fields
      ZtMKeyFields keyFields,		// keys and their fields
      const reflection::Schema *schema,	// flatbuffer reflection schema
      MaxFn,				// maxima callback
      OpenFn) = 0;			// open result callback
};
} // Store_
using Store = Store_::Interface;

// module entry point
typedef Store *(*StoreFn)();

} // Zdb_

extern "C" {
  typedef Zdb_::StoreFn ZdbStoreFn;
}
#define ZdbStoreFnSym	"ZdbStore"	// module symbol

#endif /* ZdbStore_HH */
