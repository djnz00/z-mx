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

// scheduler with thread pool

#include <zlib/ZuBox.hpp>

#include <zlib/ZmScheduler.hpp>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4800)
#endif

using namespace ZmSchedState;

ZmScheduler::ZmScheduler(ZmSchedParams params) :
  m_params(ZuMv(params)),
  m_stateCond(m_stateLock),
  m_state(Stopped),
  m_drained(0),
  m_nWorkers(0),
  m_runThreads(0)
{
  unsigned n = m_params.nThreads();
  for (unsigned tid = 0; tid <= n; tid++) {
    auto &thread = m_params.thread(tid);
    if (!thread.name()) {
      if (!tid)
	thread.name(ZmThreadName() << m_params.id() << ":timer");
      else
	thread.name(ZmThreadName() << m_params.id() << ':' << ZuBoxed(tid));
    }
    if (!thread.stackSize()) thread.stackSize(m_params.stackSize());
    if (thread.priority() < 0) thread.priority(m_params.priority());
    if (!thread.partition()) thread.partition(m_params.partition());
  }
  m_threads = new Thread[n];
  m_workers = new Thread *[n];
  for (unsigned i = 0; i < n; i++) {
    unsigned tid = i + 1;
    Ring &ring = m_threads[i].ring;
    ring.init(ZmRingParams(m_params.queueSize()).
	ll(m_params.ll()).
	spin(m_params.spin()).
	timeout(m_params.timeout()).
	cpuset(m_params.thread(tid).cpuset()));
    int r;
    if ((r = ring.open(Ring::Read | Ring::Write)) != Ring::OK)
      throw ZmRingError(r);
    // if ((r = ring.attach()) != Ring::OK) throw ZmRingError(r);
    m_threads[i].overRing.init(
	ZmDRingParams().initial(0).increment(OverRing_Increment));
    if (!m_params.thread(tid).isolated())
      m_workers[m_nWorkers++] = &m_threads[i];
  }
}

ZmScheduler::~ZmScheduler()
{
  stop();
  for (unsigned i = 0, n = m_params.nThreads(); i < n; i++) {
    Ring &ring = m_threads[i].ring;
    // ring.detach();
    ring.close();
  }
  delete [] m_workers;
  delete [] m_threads;
}

void ZmScheduler::runThreads()
{
  {
    ZmGuard<ZmLock> stateGuard(m_stateLock);

    if (m_state != Running && m_state != Starting) return;
  }

  {
    unsigned n = m_params.nThreads();
    SpawnGuard spawnGuard(m_spawnLock);
    while (m_runThreads < n) {
      ZmBitmap cpuset;
      unsigned tid = ++m_runThreads;
      m_threads[tid - 1].thread = ZmThread(tid,
	  ZmFn<>::Member<&ZmScheduler::work>::fn(this),
	  m_params.thread(tid));
    }
  }
}

void ZmScheduler::startTimer()
{
  if (ZuUnlikely(!m_thread))
    m_thread = ZmThread(0,
	ZmFn<>::Member<&ZmScheduler::timer>::fn(this),
	m_params.thread(0));
}

void ZmScheduler::start()
{
  {
    ZmGuard<ZmLock> stateGuard(m_stateLock);

    switch (m_state) {
      case Draining:
	do { m_stateCond.wait(); } while (m_state != Drained);
      case Drained:
	m_state = Running;
	m_stateCond.broadcast();
	return;
      case Starting:
	do { m_stateCond.wait(); } while (m_state != Running);
      case Running:
	return;
      case Stopping:
	do { m_stateCond.wait(); } while (m_state != Stopped);
      default:
	break;
    }

    m_state = Starting;
    m_stateCond.broadcast();
  }

  {
    ZmGuard<ZmPLock> scheduleGuard(m_scheduleLock);

    if (m_params.startTimer()) startTimer();
  }

  runThreads();

  {
    ZmGuard<ZmLock> stateGuard(m_stateLock);

    m_state = Running;
    m_stateCond.broadcast();
  }
}

void ZmScheduler::drain()
{
  {
    ZmGuard<ZmLock> stateGuard(m_stateLock);

    switch (m_state) {
      case Draining:
	do { m_stateCond.wait(); } while (m_state != Drained);
      case Drained:
	return;
      case Stopping:
	do { m_stateCond.wait(); } while (m_state != Stopped);
      case Stopped:
	return;
      case Starting:
	do { m_stateCond.wait(); } while (m_state != Running);
      default:
	break;
    }

    m_drained = 0;
    m_state = Draining;
    m_stateCond.broadcast();
  }

  for (unsigned i = 0, n = m_params.nThreads(); i < n; i++)
    runWake_(&m_threads[i], ZmFn<>::Member<&ZmScheduler::drained>::fn(this));

  {
    ZmGuard<ZmLock> stateGuard(m_stateLock);

    while (m_state != Drained) m_stateCond.wait();
  }
}

void ZmScheduler::drained()
{
  ZmGuard<ZmLock> stateGuard(m_stateLock);

  if (m_state != Draining) return;

  if (++m_drained < m_params.nThreads()) return;

  m_drained = 0;
  m_state = Drained;
  m_stateCond.broadcast();
}

void ZmScheduler::stop()
{
  {
    ZmGuard<ZmLock> stateGuard(m_stateLock);

    switch (m_state) {
      case Stopping:
	do { m_stateCond.wait(); } while (m_state != Stopped);
      case Stopped:
	return;
      case Draining:
	do { m_stateCond.wait(); } while (m_state != Drained);
      case Drained:
	break;
      case Starting:
	do { m_stateCond.wait(); } while (m_state != Running);
      default:
	break;
    }
    m_state = Stopping;
    m_stateCond.broadcast();
  }

  {
    unsigned n = m_params.nThreads();
    SpawnGuard spawnGuard(m_spawnLock);
    for (unsigned i = 0; i < n; i++)
      m_threads[i].ring.eof(true);
    for (unsigned i = 0; i < n; i++) {
      ZmThread &thread = m_threads[i].thread;
      if (thread) {
	wake(&m_threads[i]);
	thread.join();
	thread = ZmThread();
      }
    }
    for (unsigned i = 0; i < n; i++)
      m_threads[i].ring.eof(false);
  }

  if (m_thread) m_pending.post();

  m_stopped.reset();

  if (m_thread) {
    m_thread.join();
    m_thread = {};
  }

  {
    ZmGuard<ZmLock> stateGuard(m_stateLock);

    m_state = Stopped;
    m_stateCond.broadcast();
  }
}

void ZmScheduler::reset()
{
  if (this->state() != Stopped) return;

  {
    unsigned n = m_params.nThreads();
    SpawnGuard spawnGuard(m_spawnLock);
    for (unsigned i = 0; i < n; i++)
      if (!m_threads[i].thread) m_threads[i].ring.reset();
  }
}

void ZmScheduler::wakeFn(unsigned tid, ZmFn<> fn)
{
  ZmAssert(tid && tid <= m_params.nThreads());
  m_threads[tid - 1].wakeFn = ZuMv(fn);
}

void ZmScheduler::timer()
{
  for (;;) {
    {
      ZmGuard<ZmLock> stateGuard(m_stateLock);

      if (m_state == Stopping || m_state == Stopped) return;
    }

    {
      ZmTime minimum;

      {
	ZmGuard<ZmPLock> scheduleGuard(m_scheduleLock);
	if (Timer *first = m_schedule.minimum()) minimum = first->timeout;
      }

      if (minimum)
	m_pending.timedwait(minimum);
      else
	m_pending.wait();

      {
	ZmGuard<ZmLock> stateGuard(m_stateLock);

	while (m_state == Draining || m_state == Drained)
	  m_stateCond.wait();
	if (m_state == Stopping || m_state == Stopped) return;
      }

      {
	ZmTime now(ZmTime::Now);
	now += m_params.quantum();
	ZmGuard<ZmPLock> scheduleGuard(m_scheduleLock);

	{
	  auto i = m_schedule.iterator<ZmRBTreeLessEqual>(now);
	  while (Timer *timer = i.iterate()) {
	    i.del(timer);
	    timer->timeout = ZmTime{};
	    bool ok;
	    unsigned tid = timer->tid;
	    ZmFn<> fn = timer->fn;
	    if (ZuLikely(tid))
	      ok = tryRunWake_(&m_threads[tid - 1], fn);
	    else
	      ok = timerAdd(fn);
	    if (ZuUnlikely(!ok)) {
	      scheduleGuard.unlock();
	      m_schedule.addNode(timer);
	      ZmPlatform::sleep(m_params.quantum());
	      return;
	    }
	    if (timer->transient) delete timer;
	  }
	}
      }
    }
  }
}
bool ZmScheduler::timerAdd(ZmFn<> &fn)
{
  if (ZuUnlikely(!m_nWorkers)) return false;
  unsigned first = m_next++;
  unsigned next = first;
  do {
    Thread *thread = m_workers[next % m_nWorkers];
    if (tryRunWake_(thread, fn)) return true;
  } while (((next = m_next++) - first) < m_nWorkers);
  return false;
}

void ZmScheduler::run(
    unsigned tid, ZmFn<> fn, ZmTime timeout, int mode, Timer *timer)
{
  ZmAssert(tid <= m_params.nThreads());

  if (ZuUnlikely(!fn)) return;

  bool kick = true;

  if (!timer) timer = new Timer{true};

  {
    ZmGuard<ZmPLock> scheduleGuard(m_scheduleLock);

    if (ZuLikely(timer->timeout)) {
      switch (mode) {
	case Advance:
	  if (ZuUnlikely(timer->timeout <= timeout)) return;
	  break;
	case Defer:
	  if (ZuUnlikely(timer->timeout >= timeout)) return;
	  break;
      }
      m_schedule.del(timer);
      timer->timeout = ZmTime{};
    }

    if (ZuUnlikely(timeout <= ZmTimeNow())) {
      if (ZuLikely(tid)) {
	if (ZuLikely(tryRunWake_(&m_threads[tid - 1], fn))) return;
      } else {
	if (ZuLikely(timerAdd(fn))) return;
      }
    }

    if (Timer *first = m_schedule.minimum())
      kick = timeout < first->timeout;

    timer->timeout = timeout;
    timer->tid = tid;
    timer->fn = ZuMv(fn);
    m_schedule.addNode(timer);

    if (kick) startTimer();
  }

  if (kick) m_pending.post();
}

void ZmScheduler::del(Timer *timer)
{
  ZmGuard<ZmPLock> scheduleGuard(m_scheduleLock);
  if (!timer->timeout) return;
  m_schedule.del(timer);
  timer->timeout = ZmTime{};
  if (timer->transient) delete timer;
}

void ZmScheduler::add(ZmFn<> fn)
{
  if (ZuUnlikely(!fn)) return;
  if (ZuUnlikely(!m_nWorkers)) return;
  unsigned first = m_next++;
  unsigned next = first;
  do {
    auto thread = m_workers[next % m_nWorkers];
    if (tryRunWake_(thread, fn)) return;
  } while (((next = m_next++) - first) < m_nWorkers);
  runWake_(m_workers[first % m_nWorkers], ZuMv(fn));
}

void ZmScheduler::runWake_(Thread *thread, ZmFn<> fn)
{
  if (ZuLikely(run__(thread, ZuMv(fn)))) wake(thread);
}

bool ZmScheduler::tryRunWake_(Thread *thread, ZmFn<> &fn)
{
  if (ZuLikely(tryRun__(thread, fn))) { wake(thread); return true; }
  return false;
}

bool ZmScheduler::run__(Thread *thread, ZmFn<> fn)
{
  // Note: the MPSC requirement is to serialize within the producing thread
  if (ZuLikely(!thread->overCount.load_())) goto push;
overflow:
  ++thread->overCount;
  thread->overRing.push(ZuMv(fn));
  return true;
push:
  {
    // Thread::Guard guard(thread->lock); // ensure serialized ring push()
    void *ptr;
    if (ZuLikely(ptr = thread->ring.tryPush())) {
      new (ptr) ZmFn<>(ZuMv(fn));
      thread->ring.push2(ptr);
      return true;
    }
  }
  int status = thread->ring.writeStatus();
  if (status == Ring::EndOfFile) return false;
  if (status >= 0) goto overflow;
  // should never happen - the enqueuing thread will normally
  // be forced to wait for the dequeuing thread to drain the ring,
  // i.e. a slower consumer will apply back-pressure to the producer
  ZuStringN<120> s;
  s << "FATAL - Thread Dispatch Failure - push() failed: ";
  if (status <= 0)
    s << ZmRingError(status);
  else
    s << ZuBoxed(status) << " bytes remaining";
  s << '\n';
#ifndef _WIN32
  std::cerr << s << std::flush;
#else
  MessageBoxA(0, s, "Thread Dispatch Failure", MB_ICONEXCLAMATION);
#endif
  return false;
}

bool ZmScheduler::tryRun__(Thread *thread, ZmFn<> &fn)
{
  {
    // Thread::Guard guard(thread->lock); // ensure serialized ring push()
    void *ptr;
    if (ZuLikely(ptr = thread->ring.tryPush())) {
      new (ptr) ZmFn<>(ZuMv(fn));
      thread->ring.push2(ptr);
      return true;
    }
  }
  return false;
}

void ZmScheduler::work()
{
  unsigned index = ZmThreadContext::self()->index();
  Thread *thread = &m_threads[index - 1];

  thread->tid = ZmPlatform::getTID();

  m_threadInitFn();

  for (;;) {
    if (ZuLikely(!thread->overCount.load_())) goto shift;
    if (ZmFn<> fn = thread->overRing.shift()) {
      void *ptr;
      if (ZuLikely(ptr = thread->ring.tryPush())) {
	new (ptr) ZmFn<>(ZuMv(fn));
	thread->ring.push2(ptr);
	--thread->overCount;
      } else {
	thread->overRing.unshift(ZuMv(fn));
      }
    }
shift:
    if (ZmFn<> *ptr = thread->ring.shift()) {
      ZmFn<> fn = ZuMv(*ptr);
      ptr->~ZmFn<>();
      thread->ring.shift2();
      try { fn(); } catch (...) { }
    } else {
      if (thread->ring.readStatus() == Ring::EndOfFile) break;
    }
  }

  m_threadFinalFn();

  --m_runThreads;

  m_stopped.post();
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif
