//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZcmdDispatcher.hh>

void ZcmdDispatcher::init()
{
}
void ZcmdDispatcher::final()
{
  m_fnMap.clean();
}

void ZcmdDispatcher::deflt(DefltFn fn)
{
  m_defltFn = fn;
}

void ZcmdDispatcher::map(ZuID id, Fn fn)
{
  Guard guard(m_lock);
  if (auto data = m_fnMap.find(id))
    FnMap::ValAxor(*const_cast<FnMap::T *>(data)) = ZuMv(fn);
  else
    m_fnMap.add(id, ZuMv(fn));
}

int ZcmdDispatcher::dispatch(ZuID id, void *link, ZmRef<ZiIOBuf> buf)
{
  if (auto node = m_fnMap.find(id))
    return (node->template p<1>())(link, ZuMv(buf));
  if (m_defltFn) return m_defltFn(link, id, ZuMv(buf));
  return -1;
}
