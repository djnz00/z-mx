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

// generic engine with start, stop control

#ifndef ZmEngine_HPP
#define ZmEngine_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZmFn.hpp>
#include <zlib/ZmVRing.hpp>
#include <zlib/ZmThread.hpp>
#include <zlib/ZmRWLock.hpp>
#include <zlib/ZmBlock.hpp>

namespace ZmEngineState {
  enum _ {
    Stopped = 0, Starting, Running, Stopping,
    StartPending,	// started while stopping
    StopPending		// stopped while starting
  };
}

// CRTP - implementation must conform to the following interface:
#if 0
struct Impl : public ZmEngine<Impl> {
  const ZmThreadParams &thread(); // thread parameters

  bool start_();	// start engine - return true if successful
  void run();		// run engine following successful start_()
  void wake();		// wake-up engine, impl should call stopped()
  bool stop_(); 	// stop engine - return true if successful
};
#endif

template <typename Impl>
class ZmEngine {
  Impl *impl() { return static_cast<Impl *>(this); }
  const Impl *impl() const { return static_cast<const Impl *>(this); }

private:
  using Lock = ZmPRWLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  struct HeapID { static constexpr const char *id() { return "ZmEngine"; } };
  using CtrlFnRing =
    ZmVRing<ZmFn<bool>, ZmVRingLock<ZmNoLock, ZmVRingHeapID<HeapID>>>;

public:
  void start(ZmFn<bool>);	// async
  void stop(ZmFn<bool>);

  bool start();			// sync
  bool stop();

  bool running() const {
    ReadGuard guard(m_lock);
    return running_();
  }
  int state() const {
    ReadGuard guard(m_lock);
    return m_state;
  }

  bool stopped();		// returns true if engine has stopped

protected:
  bool running_() const {
    using namespace ZmEngineState;
    return m_state == Running;
  }

private:
  void started(bool ok);
  void stopped(bool ok);

  Lock			m_lock;
    ZmThread		  m_thread;
    CtrlFnRing		  m_startFn, m_stopFn;
    int			  m_state = ZmEngineState::Stopped;
};

template <typename Impl>
inline void ZmEngine<Impl>::start(ZmFn<bool> startFn)
{
  bool ok;

  {
    using namespace ZmEngineState;
    Guard stateGuard(m_lock);

    if (m_state == Running) { startFn(true); return; } // idempotent

    if (startFn) m_startFn.push(ZuMv(startFn));

    switch (m_state) {
      case Stopped:	m_state = Starting; break;
      case Stopping:	m_state = StartPending; return;
      case StopPending:	m_state = Starting; return;
      default: return; // Starting || Running || StartPending
    }

    m_thread = ZmThread(0,
	ZmFn<>{this, [](ZmEngine *self) {
	  bool ok = self->impl()->start_();
	  self->started(ok);
	  if (ok) self->impl()->run();
	}},
	impl()->thread());

    ok = !!m_thread;
  }

  if (!ok) { started(false); return; }
}
template <typename Impl>
inline bool ZmEngine<Impl>::start()
{
  return ZmBlock<bool>{}([this](auto wake) { start(ZuMv(wake)); });
}
template <typename Impl>
inline void ZmEngine<Impl>::started(bool ok)
{
  Guard guard(m_lock);
  using namespace ZmEngineState;
  bool stop = false, stopped = false;
  auto startFn = ZuMv(m_startFn);
  m_startFn.clean();
  if (!ok) {
    switch (m_state) {
      case Starting:
	m_state = Stopped;
	break;
      case StopPending:
	stopped = true;		// call stopped()
	m_state = Stopping;	// stopped() will transition to Stopped
	break;
    }
  } else {
    switch (m_state) {
      case Starting:
	m_state = Running;
	break;
      case StopPending:
	stop = true;		// call stop()
	m_state = Running;	// stop() will transition to Stopping
	break;
    }
  }
  guard.unlock();
  while (auto fn = startFn.shift()) fn(ok);
  if (stop) this->stop({});
  else if (stopped) this->stopped(true);
}

template <typename Impl>
inline void ZmEngine<Impl>::stop(ZmFn<bool> stopFn)
{
  {
    using namespace ZmEngineState;
    Guard guard(m_lock);

    if (m_state == Stopped) { stopFn(true); return; } // idempotent

    if (stopFn) m_stopFn.push(ZuMv(stopFn));

    switch (m_state) {
      case Running:	m_state = Stopping; break;
      case Starting:	m_state = StopPending; return;
      case StartPending:m_state = Stopping; return;
      default: return; // Stopping || Stopped || StopPending
    }
  }

  impl()->wake();
}
template <typename Impl>
inline bool ZmEngine<Impl>::stop()
{
  return ZmBlock<bool>{}([this](auto wake) { stop(ZuMv(wake)); });
}
template <typename Impl>
inline bool ZmEngine<Impl>::stopped()
{
  Guard guard(m_lock);
  using namespace ZmEngineState;
  if (m_state == Stopping || m_state == StartPending) {
    guard.unlock();
    bool ok = impl()->stop_();
    stopped(ok);
    return ok;
  }
  return false;
}
template <typename Impl>
inline void ZmEngine<Impl>::stopped(bool ok)
{
  Guard guard(m_lock);
  using namespace ZmEngineState;
  bool start = false, started = false;
  auto stopFn = ZuMv(m_stopFn);
  m_stopFn.clean();
  if (!ok) {
    switch (m_state) {
      case Stopping:
	m_state = Running;
	break;
      case StartPending:
	started = true;		// call started()
	m_state = Starting;	// started() will transition to Running
	break;
    }
  } else {
    switch (m_state) {
      case Stopping:
	m_state = Stopped;
	break;
      case StartPending:
	start = true;		// call start()
	m_state = Stopped;	// start() will transition to Starting
	break;
    }
    m_thread = {};
  }
  guard.unlock();
  while (auto fn = stopFn.shift()) fn(ok);
  if (start) this->start({});
  else if (started) this->started(true);
}

#endif /* ZmEngine_HPP */
