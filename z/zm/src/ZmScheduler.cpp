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

ZmScheduler::ZmScheduler(ZmSchedParams params) : m_params{ZuMv(params)}
{
  unsigned n = m_params.nThreads();
  for (unsigned index = 0; index <= n; index++) {
    auto &thread = m_params.thread(index);
    if (!thread.name()) {
      if (!index)
	thread.name(ZmThreadName() << m_params.id() << ":timer");
      else
	thread.name(ZmThreadName() << m_params.id() << ':' << ZuBoxed(index));
    }
    if (!thread.stackSize()) thread.stackSize(m_params.stackSize());
    if (thread.priority() < 0) thread.priority(m_params.priority());
    if (!thread.partition()) thread.partition(m_params.partition());
  }
  m_threads = new Thread[n];
  m_workers = new Thread *[n];
  for (unsigned i = 0; i < n; i++) {
    unsigned index = i + 1;
    Ring &ring = m_threads[i].ring;
    ring.init(ZmRingParams(m_params.queueSize()).
	ll(m_params.ll()).
	spin(m_params.spin()).
	timeout(m_params.timeout()).
	cpuset(m_params.thread(index).cpuset()));
    int r;
    if ((r = ring.open(Ring::Read | Ring::Write)) != Ring::OK)
      throw ZmRingError(r);
    // if ((r = ring.attach()) != Ring::OK) throw ZmRingError(r);
    m_threads[i].overRing.init(
	ZmVRingParams().initial(0).increment(OverRing_Increment));
    if (!m_params.thread(index).isolated())
      m_workers[m_nWorkers++] = &m_threads[i];
  }
}

ZmScheduler::~ZmScheduler()
{
  for (unsigned i = 0, n = m_params.nThreads(); i < n; i++) {
    Ring &ring = m_threads[i].ring;
    // ring.detach();
    ring.close();
  }
  delete [] m_workers;
  delete [] m_threads;
}

bool ZmScheduler::start_()
{
  {
    unsigned n = m_params.nThreads();
    SpawnGuard spawnGuard(m_spawnLock);
    for (unsigned i = 0; i < n; i++)
      m_threads[i].ring.eof(false);
    while (m_runThreads < n) {
      unsigned index = ++m_runThreads;
      if (!(m_threads[index - 1].thread = ZmThread(index,
	  ZmFn<>::Member<&ZmScheduler::work>::fn(this),
	  m_params.thread(index)))) {
	ZmScheduler::stop_();
	return false;
      }
    }
  }

  return true;
}
bool ZmScheduler::stop()
{
  // protect against blocking on self-destruction
  auto self = ZmSelf()->tid();
  {
    unsigned n = m_params.nThreads();
    SpawnReadGuard spawnGuard(m_spawnLock);
    for (unsigned i = 0; i < n; i++)
      if (m_threads[i].tid == self) {
	spawnGuard.unlock();
	ZmEngine::stop({});	// async
	return true;	// return success
      }
  }
  return ZmEngine::stop();
}
const ZmThreadParams &ZmScheduler::thread()
{
  return m_params.thread(0);
}
void ZmScheduler::wake()
{
  m_pending.post();
}
bool ZmScheduler::stop_()
{
  {
    unsigned n = m_params.nThreads();
    SpawnGuard spawnGuard(m_spawnLock);
    for (unsigned i = 0; i < n; i++)
      m_threads[i].ring.eof(true);
    for (unsigned i = 0; i < n; i++) {
      auto &thread = m_threads[i].thread;
      if (thread) {
	wake(&m_threads[i]);
	thread.join();
	thread = {};
      }
    }
  }
  return true;
}
bool ZmScheduler::reset()
{
  if (running()) return false;

  {
    unsigned n = m_params.nThreads();
    SpawnGuard spawnGuard(m_spawnLock);
    for (unsigned i = 0; i < n; i++)
      if (!m_threads[i].thread) m_threads[i].ring.reset();
  }

  return true;
}

void ZmScheduler::wakeFn(unsigned index, ZmFn<> fn)
{
  ZmAssert(index && index <= m_params.nThreads());
  m_threads[index - 1].wakeFn = ZuMv(fn);
}

void ZmScheduler::run()
{
  for (;;) {
    if (stopped()) return;

    {
      ZmTime minimum;

      {
	SchedGuard schedGuard(m_schedLock);

	if (Timer *first = m_schedule.minimum()) minimum = first->timeout;
      }

      if (minimum)
	m_pending.timedwait(minimum);
      else
	m_pending.wait();
    }

    if (stopped()) return;

    ZmTime now(ZmTime::Now);
    now += m_params.quantum();

    {
      SchedGuard schedGuard(m_schedLock);
      auto i = m_schedule.iterator<ZmRBTreeLessEqual>(now);
      while (Timer *timer = i.iterate()) {
	i.del(timer);
	bool ok;
	unsigned index = timer->index;
	ZmFn<> fn = timer->fn;
	if (ZuLikely(index))
	  ok = tryRunWake_(&m_threads[index - 1], fn);
	else
	  ok = timerAdd(fn);
	if (ZuUnlikely(!ok)) {
	  m_schedule.addNode(timer);
	  schedGuard.unlock();
	  Zm::sleep(m_params.quantum());
	  return;
	}
	timer->timeout = ZmTime{};
	if (timer->transient) delete timer;
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
    unsigned index, ZmFn<> fn, ZmTime timeout, int mode, Timer *timer)
{
  ZmAssert(index <= m_params.nThreads());

  if (ZuUnlikely(!fn)) return;

  bool kick = true;

  if (!timer) timer = new Timer{true};

  {
    SchedGuard schedGuard(m_schedLock);

    if (ZuLikely(timer->timeout)) {
      switch (mode) {
	case Advance:
	  if (ZuUnlikely(timer->timeout <= timeout)) return;
	  break;
	case Defer:
	  if (ZuUnlikely(timer->timeout >= timeout)) return;
	  break;
      }
      m_schedule.delNode(timer);
      timer->timeout = ZmTime{};
    }

    if (ZuUnlikely(timeout <= ZmTimeNow())) {
      if (ZuLikely(index)) {
	if (ZuLikely(tryRunWake_(&m_threads[index - 1], fn))) return;
      } else {
	if (ZuLikely(timerAdd(fn))) return;
      }
    }

    if (Timer *first = m_schedule.minimum())
      kick = timeout < first->timeout;

    timer->timeout = timeout;
    timer->index = index;
    timer->fn = ZuMv(fn);
    m_schedule.addNode(timer);
  }

  if (kick) wake();
}

bool ZmScheduler::del(Timer *timer)
{
  SchedGuard schedGuard(m_schedLock);

  if (!timer->timeout) return false;
  bool found = !!m_schedule.delNode(timer);
  timer->timeout = ZmTime{};
  if (timer->transient) delete timer;
  return found;
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

  thread->tid = Zm::getTID();

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
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif
