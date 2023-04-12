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

// ZmLock debugging

#include <zlib/ZmLock.hpp>

#ifdef ZmLock_DEBUG
#include <zlib/ZmBackTracer.hpp>
#include <zlib/ZmSingleton.hpp>

class ZmLock_Debug_ {
public:
  ZmLock_Debug_() : m_enabled(0) { }

  void enable() { m_enabled = 1; }
  void disable() { m_enabled = 0; }
  void capture(unsigned skip = 1) {
    if (m_enabled) m_tracer.capture(skip);
  }
  ZmBackTracer<64> *tracer() { return &m_tracer; }

  friend ZuConstant<ZmCleanup::Platform> ZmCleanupLevel(ZmLock_Debug_ *);

private:
  ZmAtomic<uint32_t>	m_enabled;
  ZmBackTracer<64>	m_tracer;
};

void ZmLock_Debug::enable()
{
  ZmSingleton<ZmLock_Debug_>::instance()->enable();
}
void ZmLock_Debug::disable()
{
  ZmSingleton<ZmLock_Debug_>::instance()->disable();
}
void ZmLock_Debug::capture(unsigned skip)
{
  ZmSingleton<ZmLock_Debug_>::instance()->capture(skip + 1);
}
ZmBackTracer<64> *ZmLock_Debug::tracer()
{
  return ZmSingleton<ZmLock_Debug_>::instance()->tracer();
}
#endif
