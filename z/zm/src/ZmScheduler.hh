//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// scheduler with thread pool
// * globally configured thread pools
//   - CPU affinity, priority, stack size, etc.
// * integrated with telemetry (ZvTelemetry)
// * isolated (dedicated) and shared threads
// * timed events (repeat and one-shot)
// * globally configured CPU affinity, priority, etc.
// * integrates with telemetry (ZvTelemetry)

#ifndef ZmScheduler_HH
#define ZmScheduler_HH

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <stdarg.h>

#include <zlib/ZuNull.hh>
#include <zlib/ZuTuple.hh>
#include <zlib/ZuCSpan.hh>
#include <zlib/ZuCArray.hh>
#include <zlib/ZuMvArray.hh>
#include <zlib/ZuID.hh>

#include <zlib/ZmAtomic.hh>
#include <zlib/ZmObject.hh>
#include <zlib/ZmNoLock.hh>
#include <zlib/ZmLock.hh>
#include <zlib/ZmSemaphore.hh>
#include <zlib/ZmRingFn.hh>
#include <zlib/ZmRing.hh>
#include <zlib/ZmXRing.hh>
#include <zlib/ZmRBTree.hh>
#include <zlib/ZmThread.hh>
#include <zlib/ZuTime.hh>
#include <zlib/ZmFn.hh>
#include <zlib/ZmEngine.hh>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4251 4231 4355 4660)
#endif

class ZmSchedTParams : public ZmThreadParams {
public:
  ZmSchedTParams &&isolated(bool b)
    { m_isolated = b; return ZuMv(*this); }

  bool isolated() const { return m_isolated; }

private:
  bool		m_isolated = false;
};

struct ZmAPI ZmSchedParams {
  ZmSchedParams() = default;
  ZmSchedParams(const ZmSchedParams &) = default;
  ZmSchedParams &operator =(const ZmSchedParams &) = default;
  ZmSchedParams(ZmSchedParams &&) = default;
  ZmSchedParams &operator =(ZmSchedParams &&) = default;

  using Thread = ZmSchedTParams;
  using Threads = ZuMvArray<Thread>;

  using ID = ZuID;

  ZmSchedParams &&id(ID id) { m_id = id; return ZuMv(*this); }
  ZmSchedParams &&nThreads(unsigned v) {
    m_threads.length((m_nThreads = v) + 1);
    return ZuMv(*this);
  }
  ZmSchedParams &&stackSize(unsigned v) { m_stackSize = v; return ZuMv(*this); }
  ZmSchedParams &&priority(unsigned v) { m_priority = v; return ZuMv(*this); }
  ZmSchedParams &&partition(unsigned v) { m_partition = v; return ZuMv(*this); }
  ZmSchedParams &&quantum(ZuTime v) { m_quantum = v; return ZuMv(*this); }

  ZmSchedParams &&queueSize(unsigned v) { m_queueSize = v; return ZuMv(*this); }
  ZmSchedParams &&ll(bool v) { m_ll = v; return ZuMv(*this); }
  ZmSchedParams &&spin(unsigned v) { m_spin = v; return ZuMv(*this); }
  ZmSchedParams &&timeout(unsigned v) { m_timeout = v; return ZuMv(*this); }

  ZmSchedParams &&startTimer(bool b) { m_startTimer = b; return ZuMv(*this); }

  template <typename L>
  ZmSchedParams &&thread(unsigned sid, L l) {
    l(m_threads[sid]);
    return ZuMv(*this);
  }
  Thread &thread(unsigned sid) { return m_threads[sid]; }

  ID id() const { return m_id; }
  unsigned nThreads() const { return m_nThreads; }
  unsigned stackSize() const { return m_stackSize; }
  int priority() const { return m_priority; }
  int partition() const { return m_partition; }
  const ZuTime &quantum() const { return m_quantum; }

  unsigned queueSize() const { return m_queueSize; }
  bool ll() const { return m_ll; }
  unsigned spin() const { return m_spin; }
  unsigned timeout() const { return m_timeout; }

  bool startTimer() const { return m_startTimer; }

  const Thread &thread(unsigned sid) const { return m_threads[sid]; }

public:
  unsigned sid(ZuCSpan s) const {
    unsigned sid;
    if (sid = ZuBox0(unsigned){s}) return sid;
    for (sid = 0; sid <= m_nThreads; sid++)
      if (s == m_threads[sid].name()) return sid;
    return 0;
  }

private:
  ID		m_id;
  unsigned	m_nThreads = 1;
  unsigned	m_stackSize = 0;
  int		m_priority = -1;
  int		m_partition = -1;
  ZuTime	m_quantum{ZuTime::Nano{1000}}; // 1us

  unsigned	m_queueSize = 131072;
  unsigned	m_spin = 1000;
  unsigned	m_timeout = 1;

  Threads	m_threads = Threads{m_nThreads + 1};

  bool		m_startTimer = false;

  bool		m_ll = false;
};

class ZmAPI ZmScheduler : public ZmEngine<ZmScheduler> {
  ZmScheduler(const ZmScheduler &) = delete;
  ZmScheduler &operator =(const ZmScheduler &) = delete;

friend ZmEngine<ZmScheduler>;

public:
  using ID = ZmSchedParams::ID;

private:
  using Ring = ZmRing<ZmRingMW<true>>;

  // run-time encapsulation of generic function/lambda
  static const char *Fn_HeapID() { return "ZmScheduler.Fn"; }
  using Fn = ZmRingFn<ZmRingFnHeapID<Fn_HeapID>>;

  // overflow ring DLQ
  static const char *OverRing_HeapID() { return "ZmScheduler.OverRing"; }
  using OverRing_ = ZmXRing<Fn, ZmXRingHeapID<OverRing_HeapID>>;
  struct OverRing : public OverRing_ {
    using Lock = ZmPLock;
    using Guard = ZmGuard<Lock>;
    using ReadGuard = ZmReadGuard<Lock>;

    ZuInline void push(Fn fn) {
      Guard guard(m_lock);
      OverRing_::push(ZuMv(fn));
      ++m_inCount;
    }
    ZuInline void unshift(Fn fn) {
      Guard guard(m_lock);
      OverRing_::unshift(ZuMv(fn));
      --m_outCount;
    }
    ZuInline Fn shift() {
      Guard guard(m_lock);
      Fn fn = OverRing_::shift();
      if (fn) ++m_outCount;
      return fn;
    }
    void stats(uint64_t &inCount, uint64_t &outCount) const {
      ReadGuard guard(m_lock);
      inCount = m_inCount;
      outCount = m_outCount;
    }

    Lock	m_lock;
    unsigned	  m_inCount = 0;
    unsigned	  m_outCount = 0;
  };
  enum { OverRing_Increment = 128 };

private:
  struct Timer_ {
    Fn		fn;
    unsigned	sid = 0;
    ZuTime	timeout;
    bool	transient = false;

    Timer_() { }
    Timer_(const Timer_ &) = delete;
    Timer_ &operator =(const Timer_ &) = delete;
    Timer_(Timer_ &&) = delete;
    Timer_ &operator =(Timer_ &&) = delete;
    ~Timer_() = default;

    Timer_(bool transient_) : transient{transient_} { }

    bool operator !() const { return !*timeout; }
    ZuOpBool
  };
  static const ZuTime &Timer_TimeoutAxor(const Timer_ &t) { return t.timeout; }
  static const char *ScheduleTree_HeapID() {
    return "ZmScheduler.ScheduleTree";
  }
  using ScheduleTree =
    ZmRBTree<Timer_,
      ZmRBTreeKey<Timer_TimeoutAxor,
	ZmRBTreeNode<Timer_,
	  ZmRBTreeHeapID<ScheduleTree_HeapID,	// overrides Shadow
	    ZmRBTreeShadow<true>>>>>;
public:
  using Timer = ScheduleTree::Node;

public:
  ZmScheduler(ZmSchedParams params = {});
  virtual ~ZmScheduler();

  const ZmSchedParams &params() const { return m_params; }
protected:
  ZmSchedParams &params_() { return m_params; }

public:
  ZuID id() const { return m_params.id(); }

  bool stop();

  bool reset(); // reset while stopped - true if ok, false if running

  void wakeFn(unsigned tid, ZmFn<> fn);

  enum { Update = 0, Advance, Defer }; // mode

  // sid is "slot ID" - array index of a specific thread in the pool [0,n)
  //
  // add(fn) - immediate execution (asynchronous) on any worker thread
  // run(sid, fn) - immediate execution (asynchronous) on a specific thread
  // push(sid, fn) - enqueue without waking a specific thread
  // invoke(sid, fn) - immediate execution on a specific thread
  //   unlike run(), invoke() will execute synchronously if the caller is
  //   already running on the specified thread; returns true if synchronous

  // run(sid, fn, timeout)
  // run(sid, fn, timeout, mode)
  // run(sid, fn, timeout, mode, timer) - deferred execution
  //   sid == 0 - run on any worker thread
  //   mode:
  //     Update - (re)schedule regardless
  //     Advance - reschedule unless outstanding timeout is sooner
  //     Defer - reschedule unless outstanding timeout is later

  // add(fn, timeout) -
  //   forwards to run(0, fn, timeout, Update, nullptr)
  // add(fn, timeout, timer) -
  //   forwards to run(0, fn, timeout, Update, timer)
  // add(fn, timeout, mode, timer) -
  //   forwards to run(0, fn, timeout, mode, timer)
  // add(fn, timeout, mode, timer) -
  //   forwards to run(0, fn, timeout, mode, timer)

  // del(timer) - cancel timer

  template <typename L>
  void add(L l) {
    Fn fn{l};
    add_(fn);
  }

  template <typename L>
  void add(L l, ZuTime timeout) {
    Fn fn{l};
    schedule_(0, fn, timeout, Update, nullptr);
  }
  template <typename L>
  void add(L l, ZuTime timeout, Timer *timer) {
    Fn fn{l};
    schedule_(0, fn, timeout, Update, timer);
  }
  template <typename L>
  void add(L l, ZuTime timeout, int mode, Timer *timer) {
    Fn fn{l};
    schedule_(0, fn, timeout, mode, timer);
  }

  template <typename L>
  void run(unsigned sid, L l, ZuTime timeout) {
    Fn fn{l};
    schedule_(sid, fn, timeout, Update, nullptr);
  }
  template <typename L>
  void run(unsigned sid, L l, ZuTime timeout, Timer *timer) {
    Fn fn{l};
    schedule_(sid, fn, timeout, Update, timer);
  }

  template <typename L>
  void run(unsigned sid, L l, ZuTime timeout, int mode, Timer *timer) {
    Fn fn{l};
    schedule_(sid, fn, timeout, mode, timer);
  }

private:
  void schedule_(unsigned sid, Fn &, ZuTime timeout, int mode, Timer *);

public:
  bool del(Timer *);		// cancel job - returns true if found

  // returns true if caller is running on thread slot sid
  bool invoked(unsigned sid) const {
    ZmAssert(sid && sid <= m_params.nThreads());
    Thread *thread = &m_threads[sid - 1];
    return Zm::getTID() == thread->tid;
  }

  // run and wake thread
  template <typename L>
  void run(unsigned sid, L l) {
    ZmAssert(sid && sid <= m_params.nThreads());
    Fn fn{l}; // l is on-stack
    run_(&m_threads[sid - 1], fn);
  }

  // enqueue for thread without waking it
  template <typename L>
  void push(unsigned sid, L l) {
    ZmAssert(sid && sid <= m_params.nThreads());
    Fn fn{l}; // l is on-stack
    push_(&m_threads[sid - 1], fn);
  }

  // run and wake thread, unless already on-thread, in which case direct call
  template <typename L>
  void invoke(unsigned sid, L l) {
    ZmAssert(sid && sid <= m_params.nThreads());
    Thread *thread = &m_threads[sid - 1];
    if (ZuLikely(Zm::getTID() == thread->tid)) { l(); return; }
    Fn fn{l}; // l is on-stack
    run_(thread, fn);
  }

  // invoke(sid, object, lambda) is a specialized version of invoke()
  // that avoids unnecessary calls to ref/deref the object if the lambda is
  // directly called - the lambda must not capture an object ref,
  // and must return a pointer to the object, which can be used to deref
  //
  // struct A : public ZmObject {
  //   void foo() { ... }
  //
  //   void bar(ZmScheduler *sched, unsigned sid) { 
  //     // need to ensure that this object remains positively ref-counted
  //     // until foo() completes, whether synchronously or asynchronously
  //
  //     // less efficient
  //     sched->invoke(sid, [self = ZmMkRef(this)]() { self->foo(); });
  //
  //     // more efficient, with more natural capture of this
  //     sched->invoke(sid, this, [this]() { foo(); return this; });
  //   }
  // };
private:
  template <typename O1, typename O2>
  struct IsObjectLambda__ : public ZuBool<
      ZuObjectTraits<O1>::IsObject && ZuObjectTraits<O2>::IsObject &&
      (ZuInspect<O1, O2>::Is || ZuInspect<O2, O1>::Is)> { };
  template <typename O, typename L, typename = void>
  struct IsObjectLambda_ : public ZuFalse { };
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-value"
  template <typename O, typename L>
  struct IsObjectLambda_<O, L, decltype(*ZuDeclVal<L &>()(), void())> :
      public IsObjectLambda__<O, decltype(*ZuDeclVal<L &>()())> { };
#pragma GCC diagnostic pop
  template <typename O, typename L, typename R = void>
  using IsObjectLambda = ZuIfT<IsObjectLambda_<O, L>{}, R>;
public:
  template <typename O, typename L>
  IsObjectLambda<O, L> invoke(unsigned sid, O *o, L l) {
    ZmAssert(sid && sid <= m_params.nThreads());
    Thread *thread = &m_threads[sid - 1];
    if (ZuLikely(Zm::getTID() == thread->tid)) {
      l(); // direct invocation without manipulating the reference count
      return;
    }
    o->ref(); // increment reference count
    auto m = [l = ZuMv(l)]() mutable { l()->deref(); }; // invoke and deref
    Fn fn{m};
    run_(thread, fn);
  }

  ZuInline void threadInit(ZmFn<> fn) { m_threadInitFn = ZuMv(fn); }
  ZuInline void threadFinal(ZmFn<> fn) { m_threadFinalFn = ZuMv(fn); }

  ZuInline unsigned nWorkers() const { return m_nWorkers; }
  ZuInline unsigned workerID(unsigned i) const {
    if (ZuLikely(i < m_nWorkers))
      return (m_workers[i] - &m_threads[0]) + 1;
    return 0;
  }

  unsigned size() const {
    return m_threads[0].ring.size() * m_params.nThreads();
  }
  unsigned count_() const {
    unsigned count = 0;
    for (unsigned i = 0, n = m_params.nThreads(); i < n; i++)
      count += m_threads[i].ring.count_();
    return count;
  }
  ZmThreadID tid(unsigned sid) const {
    return m_threads[sid - 1].tid;
  }
  const Ring &ring(unsigned sid) const {
    return m_threads[sid - 1].ring;
  }
  const OverRing &overRing(unsigned sid) const {
    return m_threads[sid - 1].overRing;
  }

  unsigned sid(ZuCSpan s) const {
    unsigned sid;
    if (sid = ZuBox0(unsigned){s}) return sid;
    unsigned n;
    for (sid = 0, n = m_params.nThreads(); sid <= n; sid++)
      if (s == m_params.thread(sid).name()) return sid;
    return 0;
  }

  // control thread
private:
  void start_();
  void stop_();
  template <typename L>
  bool spawn(L l) {
    m_thread = ZmThread{ZuMv(l), m_params.thread(0).detached(true), 0};
    return !!m_thread;
  }
  void wake();

protected:
  virtual bool start__();	 // returns false if failed
  virtual bool stop__();	 // ''

  void busy();
  void idle();

private:
  using SchedLock = ZmPLock;
  using SchedGuard = ZmGuard<SchedLock>;

  using SpawnLock = ZmPLock;
  using SpawnGuard = ZmGuard<SpawnLock>;
  using SpawnReadGuard = ZmReadGuard<SpawnLock>;

  struct Thread {
    Ring		ring;
    ZmFn<>		wakeFn;
    ZmThreadID		tid = 0;
    ZmThread		thread;
    ZmAtomic<unsigned>	overCount;
    OverRing		overRing;	// fallback overflow ring
  };

  void wake(Thread *thread) { (thread->wakeFn)(); }

  void timer();
  bool timerAdd(Fn &fn);

  void add_(Fn &fn);
  void run_(Thread *thread, Fn &fn);
  bool tryRun_(Thread *thread, Fn &fn);
  bool push_(Thread *thread, Fn &fn);
  bool tryPush_(Thread *thread, Fn &fn);

  void work();

  ZmSchedParams			m_params;

  ZmThread		 	m_thread;

  ZmSemaphore			m_pending;

  SchedLock			m_schedLock;
    ScheduleTree		  m_schedule;

  ZmAtomic<unsigned>		m_next;
  Thread			*m_threads;
  unsigned			m_nWorkers = 0;
  Thread			**m_workers;

  SpawnLock			m_spawnLock;
    unsigned			  m_runThreads = 0;

  ZmFn<>			m_threadInitFn;
  ZmFn<>			m_threadFinalFn;
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZmScheduler_HH */
