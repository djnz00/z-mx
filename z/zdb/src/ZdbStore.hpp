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

// Z Database data store interface

#ifndef ZdbStore_HPP
#define ZdbStore_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdbLib_HPP
#include <zlib/ZdbLib.hpp>
#endif

#include <zlib/ZuUnion.hpp>

#include <zlib/ZtField.hpp>

#include <zlib/ZePlatform.hpp>

#include <zlib/ZdbTypes.hpp>

namespace Zdb_ {

// opaque to data store, corresponds to a data store table
class DB;

// monomorphic ZeEvent
using Event = ZeMEvent;

// ExportFn(ptr, export) - export object to data store
typedef void (*ExportFn)(const void *, ZtField::Export &);

// back-end table namespace
namespace Table_ {

// get data
struct GetData {
  UN			un;
  SN			sn;
  VN			vn;
  const ZtField::Import	&import_;
};
// get result
using GetResult = ZuUnion<
  GetData,		// succeeded
  void,			// missing
  Event>;		// error
// get callback
using GetFn = ZmFn<DB *, RN, GetResult>;

// recovered data
struct RecoverData {
  RN			rn;
  SN			sn;
  VN			vn;
  const ZtField::Import	&import_;
};
// recover result
using RecoverResult = ZuUnion<
  RecoverData,		// succeeded
  void,			// missing
  Event>;		// error
// recover callback
using RecoverFn = ZmFn<DB *, UN, RecoverResult>;

// commit result
using CommitResult = ZuUnion<void, Event>;
// commit callback
using CommitFn = ZmFn<DB *, UN, CommitResult>;

// table close callback
using CloseFn = ZmFn<DB *>;

// back-end table interface
struct Interface {
  virtual void close(CloseFn) = 0;	// idempotent

  virtual void get(RN, GetFn) = 0;

  virtual void recover(UN, RecoverFn) = 0;

  // UN is idempotency key
  virtual void push(			// idempotent
      RN, UN, SN, const void *object, ExportFn, CommitFn) = 0;
  virtual void update(			// idempotent
      RN, UN, SN, VN, const void *object, ExportFn, CommitFn) = 0;
  virtual void del(			// idempotent
      RN, UN, SN, VN, CommitFn) = 0;
};
} // Table_;
using Table = Table_::Interface;

// back-end data store namespace
namespace Store_ {

// log function
using LogFn = ZmFn<Event>;

// result of store init
using InitResult = ZuUnion<void, Event>;

// opened table data
// - (*) un and sn may refer to trailing deletions
// - a data store will need to maintain a "most recent deletes" (MRD)
//   table, keyed on the DB ID, containing the UN and SN of the last
//   delete applied to each table; an eventually consistent batch, saga or
//   transaction is used to combine deletion from the data table with an
//   upsert to the corresponding row in the MRD table; the MRD table is
//   consulted on table open to ensure accurate "last UN" and "last SN"
//   numbers are recovered and returned
struct OpenData {
  Table		*table = nullptr;
  RN		rn = 0;			// last RN in table
  UN		un = 0;			// last UN applied to table (*)
  SN		sn = 0;			// last SN applied to table (*)
};
// result of table open
using OpenResult = ZuUnion<
  void,				// unset
  OpenData,			// succeeded
  Event>;			// error
// table open callback
using OpenFn = ZmFn<OpenResult>;

// table scan data
struct ScanData {
  RN			rn;
  UN			un;
  SN			sn;
  VN			vn;
  const ZtField::Import	&import_;
};
// table scan callback (called from open)
using ScanFn = ZmFn<ScanData>;

// back-end data store interface
struct Interface {
  // init and final are synchronous / blocking
  virtual InitResult init(		// initialize data store - idempotent
      ZvCf *cf,
      LogFn) = 0;
  virtual void final() = 0;		// finalize data store - idempotent

  virtual void open(			// open table - idempotent
      DB *,
      ZuID id,
      ZtMFieldArray fields,
      OpenFn,
      ScanFn) = 0;
};
} // Store_;
using Store = Store_::Interface;

} // Zdb_

extern "C" {
  typedef Zdb_::Store *(*ZdbStoreFn)();	// module entry point
}
#define ZdbStoreFnSym	"ZdbStore"	// module symbol

#endif /* ZdbDataStore_HPP */
