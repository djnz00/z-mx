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

// Data Series Manager Interface

#ifndef ZdfStore_HPP
#define ZdfStore_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdfLib_HPP
#include <zlib/ZdfLib.hpp>
#endif

#include <zlib/ZmScheduler.hpp>
#include <zlib/ZmRef.hpp>

#include <zlib/ZvCf.hpp>

#include <zlib/Zfb.hpp>

#include <zlib/ZdfTypes.hpp>
#include <zlib/ZdfBuf.hpp>

// no need for CRTP or explicit jump tables
// - good ole' vtbl is fine due to (relatively) infrequent calls

namespace Zdf {

namespace Store_ {

// open data
struct OpenData {
  unsigned	blkOffset;
};
// open result
using OpenResult = ZuUnion<
  void,			// uninitialized
  OpenData,		// succeeded
  Event>;		// error
// open callback
using OpenFn = ZmFn<OpenResult>;

// close result
using CloseResult = ZuUnion<
  void,			// succeeded
  Event>;		// error
// close callback
using CloseFn = ZmFn<CloseResult>;

// load data
struct LoadData { };	// intentionally empty
// load result
using LoadResult = ZuUnion<
  void,			// missing
  LoadData,		// succeeded
  Event>;		// error
// load callback
using LoadFn = ZmFn<LoadResult>;

// save result
using SaveResult = ZuUnion<
  void,			// succeeded
  Event>;		// error
// save callback
using SaveFn = ZmFn<SaveResult>;

class Interface : public BufMgr {
public:
  ~Interface();

  virtual void init(ZmScheduler *, const ZvCf *) = 0;
  virtual void final(); // intentionally not pure virtual

  // load/save data frame
  virtual void loadDF(
      ZuString name, Zfb::Load::LoadFn, unsigned maxFileSize, LoadFn) = 0;
  virtual void saveDF(
      ZuString name, Zfb::Builder &fbb, SaveFn) = 0;

  // open/close series
  virtual void open(
      unsigned seriesID, ZuString parent, ZuString name, OpenFn) = 0;
  virtual void close(
      unsigned seriesID, CloseFn) = 0;

  // load/save buffers
  virtual bool loadHdr(unsigned seriesID, unsigned blkIndex, Hdr &hdr) = 0;
  virtual bool load(unsigned seriesID, unsigned blkIndex, void *buf) = 0;
  virtual void save(ZmRef<Buf>) = 0;
};
} // Store_
using Store = Store_::Interface;

// module entry point
typedef Store *(*StoreFn)();

} // namespace Zdf

extern "C" {
  typedef Zdf::StoreFn ZdfStoreFn;
}
#define ZdfStoreFnSym	"ZdfStore"	// module symbol

#endif /* ZdfStore_HPP */
