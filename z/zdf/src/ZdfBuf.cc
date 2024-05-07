//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Series Buffer

#include <zlib/ZdfBuf.hh>

using namespace Zdf;

void BufMgr::init(unsigned maxBufs)
{
  m_maxBufs = maxBufs;
}

void BufMgr::final()
{
  m_lru.clean();
}

unsigned BufMgr::alloc(BufUnloadFn unloadFn)
{
  unsigned id = m_unloadFn.length();
  m_unloadFn.push(ZuMv(unloadFn));
  return id;
}

void BufMgr::free(unsigned seriesID) // caller unloads
{
  auto i = m_lru.iterator();
  while (auto node = i.iterate())
    if (node->seriesID == seriesID) i.del();
}

void BufMgr::purge(unsigned seriesID, unsigned blkIndex) // caller unloads
{
  auto i = m_lru.iterator();
  while (auto node = i.iterate())
    if (node->seriesID == seriesID && node->blkIndex < blkIndex) i.del();
}
