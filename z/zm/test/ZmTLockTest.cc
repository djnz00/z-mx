//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuLib.hh>

#include <stdio.h>

#include <zlib/ZmObject.hh>
#include <zlib/ZmRef.hh>
#include <zlib/ZmTLock.hh>
#include <zlib/ZmSingleton.hh>
#include <zlib/ZmList.hh>
#include <zlib/ZmFn.hh>
#include <zlib/ZmThread.hh>
#include <zlib/ZmSemaphore.hh>
#include <zlib/ZmTime.hh>

class Thread;

class Global {
public:
  Global() : m_started{Zm::now()} { }
  ~Global() { if (m_threads) delete [] m_threads; }

  void start_(int nThreads, int nLocks);
  void stop_();

  static void start(int nThreads, int nLocks)
    { instance()->start_(nThreads, nLocks); }
  static void stop() { instance()->stop_(); }

  static ZuTime &started() { return instance()->m_started; }

  static Thread *thread(int i) { return instance()->m_threads[i]; }
  static ZmTLock<int, int> &locks() { return instance()->m_locks; }

  static Global *instance();

private:
  ZuTime			m_started;
  ZmSemaphore			m_completed;
  int				m_nThreads = 0;
  ZmRef<Thread>			*m_threads = nullptr;
  ZmTLock<int, int>		m_locks;
};

class Work;

class Thread : public ZmObject {
public:
  Thread(int id) : m_id(id) { }

  void operator()();

  void start() {
    m_thread = ZmThread{[this_ = ZmMkRef(this)]() { (*this_)(); }};
  }
  int synchronous(Work *work) {
    m_work = work;
    m_pending.post();
    m_completed.wait();
    return m_result;
  }
  void asynchronous(Work *work) {
    m_work = work;
    m_pending.post();
  }
  int result() {
    m_completed.wait();
    return m_result;
  }
  void stop() {
    m_work = 0;
    m_pending.post();
    m_thread.join();
  }

private:
  int			m_id;
  ZmThread		m_thread;
  ZmSemaphore		m_pending;
  ZmSemaphore		m_completed;
  ZmRef<Work>		m_work;
  int			m_result;
};

Global *Global::instance() { return ZmSingleton<Global>::instance(); }

void Global::start_(int nThreads, int nLocks)
{
  m_threads = new ZmRef<Thread>[m_nThreads = nThreads];
  for (int i = 0; i < nThreads; i++) {
    (m_threads[i] = new Thread(i))->start();
  }
}

void Global::stop_()
{
  for (int i = 0; i < m_nThreads; i++) {
    m_threads[i]->stop();
    m_threads[i] = 0;
  }
  delete [] m_threads;
  m_threads = 0;
  m_nThreads = 0;
}

class Work : public ZmObject {
public:
  enum Insn { ReadLock, WriteLock, Unlock };

  Work(Insn insn, int lock, bool try_, ZuTime timeout) :
	m_insn{insn}, m_lock{lock}, m_try{try_}, m_timeout{timeout} { }

  int operator ()(int tid);

  void dump(int tid, const char *prePost, ZmTLock<int, int> &locks);

private:
  Insn		m_insn;
  int		m_lock;
  bool		m_try;
  ZuTime	m_timeout;
};

void Thread::operator()()
{
  for (;;) {
    m_pending.wait();
    {
      ZmRef<Work> work = m_work;
      if (!work) return;
      m_work = 0;
      m_result = (*work)(m_id);
    }
    m_completed.post();
  }
}

void Work::dump(int tid, const char *prePost, ZmTLock<int, int> &locks)
{
  const char *insns[] = { "ReadLock", "WriteLock", "Unlock" };

  ZuTime now = Zm::now();
  ZuTime stamp = now - Global::started();
  auto s = locks.dump(m_lock);

  if (*m_timeout) {
    ZuTime timeout = m_timeout - now;
    printf("%s %3d %s %10s %3d %s %s\n",
           (ZuStringN<32>{} << stamp.interval()).data(),
	   tid, prePost, insns[m_insn], m_lock, s.data(),
           (ZuStringN<32>{} << timeout.interval()).data());
  } else if (m_try)
    printf("%s %3d %s %10s %3d %s Try\n",
           (ZuStringN<32>{} << stamp.interval()).data(),
	   tid, prePost, insns[m_insn], m_lock, s.data());
  else
    printf("%s %3d %s %10s %3d %s\n",
           (ZuStringN<32>{} << stamp.interval()).data(),
	   tid, prePost, insns[m_insn], m_lock, s.data());

  fflush(stdout);
}

int Work::operator ()(int tid)
{
  ZmTLock<int, int> &locks = Global::locks();
  int result = 0;

  dump(tid, "PRE ", locks);

  switch (m_insn) {
    case ReadLock:
      if (*m_timeout)
	result = locks.timedReadLock(m_lock, tid, m_timeout);
      else if (m_try)
	result = locks.tryReadLock(m_lock, tid);
      else
	result = locks.readLock(m_lock, tid);
      break;
    case WriteLock:
      if (*m_timeout)
	result = locks.timedWriteLock(m_lock, tid, m_timeout);
      else if (m_try)
	result = locks.tryWriteLock(m_lock, tid);
      else
	result = locks.writeLock(m_lock, tid);
      break;
    case Unlock:
      locks.unlock(m_lock, tid);
      break;
  }

  dump(tid, "POST", locks);

  return result;
}

int synchronous(int tid, Work *work)
{
  return Global::thread(tid)->synchronous(work);
}

void asynchronous(int tid, Work *work)
{
  Global::thread(tid)->asynchronous(work);
}

int result(int tid)
{
  return Global::thread(tid)->result();
}

#define ReadLock(l) new Work(Work::ReadLock, l, false, ZuTime{})
#define TryReadLock(l) new Work(Work::ReadLock, l, true, ZuTime{});
#define TimedReadLock(l, t) new Work(Work::ReadLock, l, true, Zm::now(t));
#define WriteLock(l) new Work(Work::WriteLock, l, false, ZuTime{})
#define TryWriteLock(l) new Work(Work::WriteLock, l, true, ZuTime{});
#define TimedWriteLock(l, t) new Work(Work::WriteLock, l, true, Zm::now(t));
#define Unlock(l) new Work(Work::Unlock, l, false, ZuTime())

struct ZmTLock_Test {
  static ZmTLock<int, int>::LockRef lock(int id) {
    ZmTLock<int, int>::LockHash::NodeRef node;
    if (!(node = Global::locks().m_locks->find(id))) return 0;
    return node->val();
  }
  static void waitForPendingUpgraders(int id, int n) {
    ZmTLock<int, int>::LockRef l = lock(id);
    if (l) while (l->m_upgradeCount < n) Zm::sleep(ZuTime(0.001));
  }
  static void waitForPendingWriters(int id, int n) {
    ZmTLock<int, int>::LockRef l = lock(id);
    if (l) while (l->m_writeCount < n) Zm::sleep(ZuTime(0.001));
  }
};

int main(int argc, char **argv)
{
  int n = argc < 2 ? 1 : atoi(argv[1]);
  Global::start(8, 8);

  for (int i = 0; i < n; i++) {
    synchronous(0, ReadLock(0));
    synchronous(1, ReadLock(0));
    asynchronous(0, WriteLock(0));
    ZmTLock_Test::waitForPendingUpgraders(0, 1);
    asynchronous(3, WriteLock(0));
    ZmTLock_Test::waitForPendingWriters(0, 2);
    asynchronous(2, ReadLock(0));
    synchronous(1, Unlock(0));
    result(0);
    synchronous(0, Unlock(0));
    synchronous(0, Unlock(0));
    result(3);
    synchronous(3, Unlock(0));
    result(2);
    synchronous(2, Unlock(0));
    synchronous(0, ReadLock(0));
    synchronous(1, ReadLock(0));
    asynchronous(0, WriteLock(0));
    ZmTLock_Test::waitForPendingUpgraders(0, 1);
    synchronous(1, WriteLock(0));
    synchronous(1, Unlock(0));
    synchronous(1, Unlock(0));
    result(0);
    synchronous(0, Unlock(0));
    synchronous(0, Unlock(0));
  }

  Global::stop();
  return 0;
}
