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

// Data Series mock back-end data store

#ifndef ZdfMockStore_HPP
#define ZdfMockStore_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdfLib_HPP
#include <zlib/ZdfLib.hpp>
#endif

#include <zlib/ZdfStore.hpp>

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

#endif /* ZdfMockStore_HPP */
