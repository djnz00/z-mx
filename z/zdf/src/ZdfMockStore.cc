//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// data frame - mock in-memory data store

#include <zlib/ZdfMockStore.hh>

using namespace ZdfMockStore;

void MockStore_::shift() { }
void MockStore_::push(BufLRUNode *) { }
void MockStore_::use(BufLRUNode *) { }
void MockStore_::del(BufLRUNode *) { }
void MockStore_::purge(unsigned, unsigned) { }

void MockStore_::init(ZmScheduler *, const ZvCf *) { BufMgr::init(UINT_MAX); }

void MockStore_::final() { Store::final(); }

void MockStore_::open(unsigned, ZuString, ZuString, OpenFn openFn)
{
  openFn(OpenResult{OpenData{.blkOffset = 0}});
}

void MockStore_::close(unsigned, CloseFn closeFn) {
  closeFn(CloseResult{});
}

bool MockStore_::loadHdr(unsigned, unsigned, Hdr &)
{
  return false;
}

bool MockStore_::load(unsigned, unsigned, void *)
{
  return false;
}

void MockStore_::save(ZmRef<Buf> buf)
{
  buf->unpin();	// normally performed in buf->save_()
}

void MockStore_::loadDF(ZuString, Zfb::Load::LoadFn, unsigned, LoadFn loadFn)
{
  loadFn(LoadResult{ZeMEVENT(Error, "mock data store - loadDF() unsupported")});
}

void MockStore_::saveDF(ZuString, Zfb::Builder &, SaveFn saveFn)
{
  saveFn(SaveResult{ZeMEVENT(Error, "mock data store - saveDF() unsupported")});
}
