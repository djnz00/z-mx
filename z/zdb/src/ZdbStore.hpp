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

namespace Zdb_ {

// back-end data store table
struct Table {
  // importer {row, indices[]} -> value_get_*(row_get_column(), ...)
  // exporter {statement, indices[]} -> statement_bind_*()
  //
  // note that 
  //
  // struct Exporter : public ZtField::Exporter {
  //   unsigned		indices[];	// VField index -> statement index
  //   // probably have prepared statement in here as well
  // };
  // struct Export : public ZtField::Export {
  //   const auto &exporter_() {
  //     return static_cast<Exporter &>(exporter);
  //   }
  //   CassStatement	*statement;
  // }
  //
  // // same for Importer, except with row instead of statement
  //
  // 1 importer (get)
  // 3 exporters (push, update, del)

  virtual void close(ZmFn<>) = 0;		// idempotent

  using GetResult = ZuUnion<
    const ZtField::Import &,			// succeeded
    void,					// missing
    ZeEvent>;					// error
  using GetFn = ZmFn<RN, GetResult>;

  virtual void get(RN, GetFn) = 0;

  using ExportFn = ZmFn<const ZtField::Export &>;

  using CommitResult = ZuUnion<void, ZeEvent>;
  using CommitFn = ZmFn<UN, RN, CommitResult>;

  // UN is idempotency key
  virtual void push(UN, RN, ExportFn, CommitFn) = 0;	// idempotent
  virtual void update(UN, RN, ExportFn, CommitFn) = 0;	// idempotent
  virtual void del(UN, RN, CommitFn) = 0;		// idempotent
}

// back-end data store
struct Store {
  using LogFn = ZmFn<ZeEvent>;			// log function

  using Result = ZuUnion<void, ZeEvent>;
  using ResultFn = ZmFn<Result>;

  virtual Result init(			// initialize data store - idempotent
      ZvCf *cf,
      LogFn) = 0;

  virtual void final(ZmFn<>) = 0;	// finalize data store - idempotent

  using RecoverFn = ZmFn<RN, const ZtField::Import &>;	// recovered record

  virtual Table *open(			// open table - idempotent
      ZuID id,
      ZtVFieldArray fields,
      ResultFn,
      RecoverFn) = 0;
};

} // Zdb_

extern "C" {
  typedef Zdb_::Store *(*ZdbStoreFn)();	// entry point
}
#define ZdbStoreFnSym	"ZdbStore"	// entry point symbol

#endif /* ZdbDataStore_HPP */
