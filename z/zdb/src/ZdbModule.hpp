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

// Z Database data store module interface

#ifndef ZdbModule_HPP
#define ZdbModule_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdbLib_HPP
#include <zlib/ZdbLib.hpp>
#endif

namespace Zdb_ {

namespace Core {	// Zdb Core

struct Env {
  virtual void initialized() = 0;
  virtual void initFailed() = 0;
  virtual void finalized() = 0;

  virtual void log(ZeEvent, ZtString) = 0;
};

struct Object {
  void		*ptr;
};

struct DB {
  virtual ZuID id() const = 0;

  ZtVFieldArray fields() const = 0;	// used to construct imports/exports

  virtual void opened(UN nextUN, RN nextRN) = 0;
  virtual void openFailed() = 0;
  virtual void closed() = 0;

  virtual void recovered(RN, const ZtField::Import &) = 0;
  virtual void retrieved(RN, const ZtField::Import &) = 0;
  virtual void pushed(RN) = 0;
  virtual void updated(RN) = 0;
  virtual void deleted(RN) = 0;
};

} // Core

namespace DS {		// Zdb Data Store

struct DB {
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
  // // same for Importer, except with row
  //
  // 3 exporters (push, update, del)
  // 1 importer (get)

  virtual void close() = 0;			// -> closed

  virtual void get(RN) = 0;			// -> retrieved

  using ExportFn = ZmFn<const ZtField::Export &>;

  virtual void push(ExportFn) = 0;		// -> pushed
  virtual void update(ExportFn) = 0;		// -> updated
  virtual void del(ExportFn) = 0;		// -> deleted
}

struct Env {
  virtual void init(Core::Env *, ZvCf *cf) = 0;	// -> initialized | initFailed
  virtual void final() = 0;			// -> finalized

  virtual DB *open(Core::DB *, bool recover) = 0; // -> opened, recovered...
};

} // DS

} // Zdb_

extern "C" {
  typedef Zdb_::DS::Env *(*ZdbModuleFn)();	// entry point
}
#define ZdbModuleFnSym	"Zdb_module"		// entry point symbol

#endif /* ZdbModule_HPP */
