//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// generic engine with start, stop control

#ifndef ZmEngine_HH
#define ZmEngine_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <zlib/ZmFn.hh>
#include <zlib/ZmXRing.hh>
#include <zlib/ZmRWLock.hh>
#include <zlib/ZmBlock.hh>

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
  void start_(); // start engine - impl should eventually call started(ok)
  void stop_();  // stop engine - impl should eventually call stopped(ok)

  // optional functions

  void stateChanged();	// state change notification

  template <typename L>
  bool spawn(L);	// spawn control thread - returns true if successful
  void wake();		// wake-up control thread, have it evaluate stopped()
			// and exit if true
};
#endif

template <typename Impl>
class ZmEngine {
  auto impl() const { return static_cast<const Impl *>(this); }
  auto impl() { return static_cast<Impl *>(this); }

private:
  using Lock = ZmPRWLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  static const char *HeapID() { return "ZmEngine"; };
  using CtrlFnRing =
    ZmXRing<ZmFn<bool>, ZmXRingHeapID<HeapID>>;

public:
  template <typename L>
  bool lock(int state, L l) {
    Guard guard(m_lock);
    if (m_state != state) return false;
    return l();
  }

  void start(ZmFn<bool>);	// async
  void stop(ZmFn<bool>);

  bool start();			// sync
  bool stop();

  int state() const { return m_state; }
  bool running() const {
    using namespace ZmEngineState;
    switch (m_state) {
      case Starting:
      case Running:
	return true;
    }
    return false;
  }

  bool stopped();		// returns true if engine has stopped

protected:
  void started(bool ok);
  void stopped(bool ok);

  void stateChanged() { }			// optional

  template <typename L>
  bool spawn(L l) { l(); return true; }		// default
  void wake() { stopped(); }			// default

private:
  Lock			m_lock;
    CtrlFnRing		  m_startFn, m_stopFn;
    ZmAtomic<int>	  m_state = ZmEngineState::Stopped;
};

template <typename Impl>
inline void ZmEngine<Impl>::start(ZmFn<bool> startFn)
{
  using namespace ZmEngineState;
  bool ok;

  {
    Guard guard(m_lock);

    if (m_state == Running) { startFn(true); return; } // idempotent

    if (startFn) m_startFn.push(ZuMv(startFn));

    switch (m_state.load_()) {
      case Stopped:
	m_state = Starting;
	break;
      case Stopping:
	m_state = StartPending;
	guard.unlock();
	impl()->stateChanged();
	return;
      case StopPending:
	m_state = Starting;
	guard.unlock();
	impl()->stateChanged();
	return;
      default:
	return; // Starting || Running || StartPending
    }
  }

  ok = impl()->spawn([this]() { impl()->start_(); });

  if (ok)
    impl()->stateChanged();
  else
    started(false);
}
template <typename Impl>
inline bool ZmEngine<Impl>::start()
{
  return ZmBlock<bool>{}([this](auto wake) { this->start(ZuMv(wake)); });
}
template <typename Impl>
inline void ZmEngine<Impl>::started(bool ok)
{
  using namespace ZmEngineState;
  Guard guard(m_lock);
  bool stop = false, stopped = false;
  auto startFn = ZuMv(m_startFn);
  m_startFn.clean();
  if (!ok) {
    switch (m_state) {
      case Starting:
	m_state = Stopped;
	break;
      case StopPending:
	stopped = true;		// call stopped(true)
	m_state = Stopping;	// stopped(true) will transition to Stopped
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
  impl()->stateChanged();
  while (auto fn = startFn.shift()) fn(ok);
  if (stop) this->stop({});
  else if (stopped) this->stopped(true);
}

template <typename Impl>
inline void ZmEngine<Impl>::stop(ZmFn<bool> stopFn)
{
  using namespace ZmEngineState;

  {
    Guard guard(m_lock);

    if (m_state == Stopped) { stopFn(true); return; } // idempotent

    if (stopFn) m_stopFn.push(ZuMv(stopFn));

    switch (m_state.load_()) {
      case Running:
	m_state = Stopping;
	break;
      case Starting:
	m_state = StopPending;
	guard.unlock();
	impl()->stateChanged();
	return;
      case StartPending:
	m_state = Stopping;
	guard.unlock();
	impl()->stateChanged();
	return;
      default:
	return; // Stopping || Stopped || StopPending
    }
  }

  impl()->stateChanged();

  impl()->wake();
}
template <typename Impl>
inline bool ZmEngine<Impl>::stop()
{
  return ZmBlock<bool>{}([this](auto wake) { this->stop(ZuMv(wake)); });
}
template <typename Impl>
inline bool ZmEngine<Impl>::stopped()
{
  using namespace ZmEngineState;
  switch (m_state) {
    case Stopping:
    case StartPending:
      impl()->stop_();
      return true;
  }
  return false;
}
template <typename Impl>
inline void ZmEngine<Impl>::stopped(bool ok)
{
  using namespace ZmEngineState;
  Guard guard(m_lock);
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
  }
  guard.unlock();
  impl()->stateChanged();
  while (auto fn = stopFn.shift()) fn(ok);
  if (start) this->start({});
  else if (started) this->started(true);
}

#endif /* ZmEngine_HH */
