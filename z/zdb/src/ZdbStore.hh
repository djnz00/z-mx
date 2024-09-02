//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Z Database data store interface

#ifndef ZdbStore_HH
#define ZdbStore_HH

#ifndef ZdbLib_HH
#include <zlib/ZdbLib.hh>
#endif

#include <flatbuffers/reflection.h>

#include <zlib/ZuUnion.hh>

#include <zlib/ZmPolymorph.hh>

#include <zlib/ZtField.hh>

#include <zlib/ZePlatform.hh>

#include <zlib/ZiMultiplex.hh>

#include <zlib/ZvCf.hh>

#include <zlib/ZdbBuf.hh>
#include <zlib/ZdbMsg.hh>
#include <zlib/ZdbTypes.hh>

namespace Zdb_ {

class Store;
class StoreTbl;

// monomorphic ZeEvent
using Event = ZeVEvent;

// failure notification
using FailFn = ZmFn<void(Event)>;

// result of store init()
struct InitData {
  bool		replicated = false;	// replicated data store?
};
using InitResult = ZuUnion<
  InitData,			// succeeded
  Event>;			// error

// start result
using StartResult = ZuUnion<void, Event>;
// start callback
using StartFn = ZmFn<void(StartResult)>;

// stop result
using StopResult = ZuUnion<void, Event>;
// stop callback
using StopFn = ZmFn<void(StopResult)>;

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
  ZtArray<UN>	un;			// last UN applied to each shard
  SN		sn = 0;			// last SN applied to table (*)
};
// result of table open
using OpenResult = ZuUnion<
  OpenData,			// succeeded
  Event>;			// error
// open callback
using OpenFn = ZmFn<void(OpenResult)>;

// table close callback
using CloseFn = ZmFn<>;

// count data (returned by count)
struct CountData {
  uint64_t		count;
};
// key result
using CountResult = ZuUnion<
  CountData,		// count
  Event>;		// error
// count callback
using CountFn = ZmFn<void(CountResult)>;

// tuple data
struct TupleData {
  int			keyID;	// ZuFieldKeyID::All for entire row tuple
  ZmRef<const IOBuf>	buf;	// tuple data, no replication message header
  unsigned		count;	// number of results so far, including this one
};
// tuple result
using TupleResult = ZuUnion<
  void,			// end of results
  TupleData,		// matching tuple
  Event>;		// error
// tuple callback
// - app must process buf contents synchronously
using TupleFn = ZmFn<void(TupleResult)>;

// row data
struct RowData {
  ZmRef<const IOBuf>	buf;	// replication message
};
// row result
using RowResult = ZuUnion<
  void,			// missing
  RowData,		// succeeded
  Event>;		// error

// row callback
using RowFn = ZmFn<void(RowResult)>;

// commit result
using CommitResult = ZuUnion<void, Event>;
// commit callback
using CommitFn = ZmFn<void(ZmRef<const IOBuf>, CommitResult)>;

// backing table interface
class StoreTbl {
public:
  virtual void close(CloseFn) = 0;	// idempotent

  virtual void warmup() = 0;

  // buf contains key data, no replication message header
  virtual void count(unsigned keyID, ZmRef<const IOBuf>, CountFn) = 0;

  // buf contains key data, no replication message header
  virtual void select(
    bool selectRow, bool selectNext, bool inclusive,
    unsigned keyID, ZmRef<const IOBuf>, unsigned limit, TupleFn) = 0;

  // buf contains key data, no replication message header
  virtual void find(unsigned keyID, ZmRef<const IOBuf>, RowFn) = 0;

  virtual void recover(Shard shard, UN, RowFn) = 0;

  // buf contains replication message, UN is idempotency key
  virtual void write(ZmRef<const IOBuf>, CommitFn) = 0;	// idempotent
};

// backing data store interface
class Store : public ZmPolymorph {
public:
  // init and final are synchronous / blocking
  virtual InitResult init(		// initialize data store - idempotent
      ZvCf *cf,
      ZiMultiplex *mx,
      FailFn failFn) = 0;		// asynchronous failure notification
  virtual void final() = 0;		// finalize data store - idempotent

  virtual void start(StartFn fn) { fn(StartResult{}); }
  virtual void stop(StopFn fn) { fn(StopResult{}); }

  virtual void open(			// open table - idempotent, async
      ZuString id,			// name of table
      unsigned nShards,			// #shards
      ZtVFieldArray fields,		// fields
      ZtVKeyFieldArray keyFields,	// keys and their fields
      const reflection::Schema *schema,	// flatbuffer reflection schema
      IOBufAllocFn,			// buffer allocator
      OpenFn) = 0;			// open result callback
};

// module entry point
typedef Store *(*StoreFn)();

} // Zdb_

extern "C" {
  typedef Zdb_::StoreFn ZdbStoreFn;
}
#define ZdbStoreFnSym	"ZdbStore"	// module symbol

#endif /* ZdbStore_HH */
