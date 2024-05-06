//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// Data Series - manager interface

#ifndef ZdfStore_HH
#define ZdfStore_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdfLib_HH
#include <zlib/ZdfLib.hh>
#endif

#include <zlib/ZmScheduler.hh>
#include <zlib/ZmRef.hh>

#include <zlib/ZvCf.hh>

#include <zlib/Zfb.hh>

#include <zlib/ZdfTypes.hh>
#include <zlib/ZdfBuf.hh>

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

#endif /* ZdfStore_HH */
