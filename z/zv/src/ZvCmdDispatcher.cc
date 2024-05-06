//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

#include <zlib/ZvCmdDispatcher.hh>

void ZvCmdDispatcher::init()
{
}
void ZvCmdDispatcher::final()
{
  m_fnMap.clean();
}

void ZvCmdDispatcher::deflt(DefltFn fn)
{
  m_defltFn = fn;
}

void ZvCmdDispatcher::map(ZuID id, Fn fn)
{
  Guard guard(m_lock);
  if (auto data = m_fnMap.find(id))
    FnMap::ValAxor(*const_cast<FnMap::T *>(data)) = ZuMv(fn);
  else
    m_fnMap.add(id, ZuMv(fn));
}

int ZvCmdDispatcher::dispatch(
    ZuID id, void *link, const uint8_t *data, unsigned len)
{
  if (auto node = m_fnMap.find(id))
    return (node->template p<1>())(link, data, len);
  if (m_defltFn) return m_defltFn(link, id, data, len);
  return -1;
}
