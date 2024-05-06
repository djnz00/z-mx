//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// ZmLock debugging

#include <zlib/ZmLock.hh>

#ifdef ZmLock_DEBUG
#include <zlib/ZmBackTracer.hh>
#include <zlib/ZmSingleton.hh>

class ZmLock_Debug_ {
public:
  ZmLock_Debug_() : m_enabled(0) { }

  void enable() { m_enabled = 1; }
  void disable() { m_enabled = 0; }
  void capture(unsigned skip = 1) {
    if (m_enabled) m_tracer.capture(skip);
  }
  ZmBackTracer<64> *tracer() { return &m_tracer; }

  friend ZuUnsigned<ZmCleanup::Platform> ZmCleanupLevel(ZmLock_Debug_ *);

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
