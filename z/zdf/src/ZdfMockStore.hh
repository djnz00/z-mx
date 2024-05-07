//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Series - mock back-end data store

#ifndef ZdfMockStore_HH
#define ZdfMockStore_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdfLib_HH
#include <zlib/ZdfLib.hh>
#endif

#include <zlib/ZdfStore.hh>

namespace ZdfMockStore {

using namespace Zdf;
using namespace Zdf::Store_;

using Zdf::Store_::OpenData;
using Zdf::Store_::OpenResult;
using Zdf::Store_::OpenFn;
using Zdf::Store_::CloseResult;
using Zdf::Store_::CloseFn;

class ZdfAPI MockStore_ : public Interface {
public:
  void shift();
  void push(BufLRUNode *);
  void use(BufLRUNode *);
  void del(BufLRUNode *);
  void purge(unsigned seriesID, unsigned blkIndex);

  void init(ZmScheduler *, const ZvCf *);
  void final();

  void open(unsigned seriesID, ZuString parent, ZuString name, OpenFn);
  void close(unsigned seriesID, CloseFn);

  bool loadHdr(unsigned seriesID, unsigned blkIndex, Hdr &);
  bool load(unsigned seriesID, unsigned blkIndex, void *buf);
  void save(ZmRef<Buf>);

  void loadDF(ZuString, Zfb::Load::LoadFn, unsigned, LoadFn);
  void saveDF(ZuString, Zfb::Builder &, SaveFn);
};

} // namespace ZdfMockStore_;

namespace Zdf {
  using MockStore = ZdfMockStore::MockStore_;
}

#endif /* ZdfMockStore_HH */
