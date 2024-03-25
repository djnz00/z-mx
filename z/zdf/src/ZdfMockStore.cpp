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

// Data Series In-Memory

#include <zlib/ZdfMockStore.hpp>

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
