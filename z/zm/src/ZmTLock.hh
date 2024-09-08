//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// transactional lock manager - ID-indexed R/W locks with deadlock detection

// * upgraders have priority over writers - no lock release during upgrade
// * writers have priority over readers - no writer starvation

#ifndef ZmTLock_HH
#define ZmTLock_HH

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

// undefine to debug infinite recursion
// #define ZmTLock_DebugInfiniteRecursion

#include <zlib/ZuTraits.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuHash.hh>
#include <zlib/ZuCArray.hh>

#include <zlib/ZmLock.hh>
#include <zlib/ZmCondition.hh>
#include <zlib/ZmGuard.hh>
#include <zlib/ZuTime.hh>
#include <zlib/ZmObject.hh>
#include <zlib/ZmRef.hh>
#include <zlib/ZmHash.hh>
#include <zlib/ZmStack.hh>
#include <zlib/ZmList.hh>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4996 4800)
#endif

#ifdef ZmTLock_DebugInfiniteRecursion
#include <stdio.h>

#include <zlib/ZmSpecific.hh>

struct ZmTLock_Depth;

struct ZmTLock_Depth : public ZmObject {
  ZmTLock_Depth() : m_depth(0) { }

  static ZmTLock_Depth *instance() {
    return
      ZmSingleton<ZmTLock_Depth,
	ZmSingletonCleanup<ZmCleanup::Platform>>::instance();
  }

  void inc() {
    if (m_depth > 20) {
      fputs("INFINITE RECURSION\n", stderr);
      Zm::exit(1);
    }
    m_depth++;
  }
  void dec() { --m_depth; }
  int m_depth;
};
#endif

struct ZmTLockParams {
  ZmTLockParams() :
    m_idHash(ZmHashParams{}.bits(8)),
    m_tidHash(ZmHashParams{}.bits(8)) { }

  ZmTLockParams &&idHash(ZmHashParams p) { m_idHash = p; return ZuMv(*this); }
  ZmTLockParams &&tidHash(ZmHashParams p) { m_tidHash = p; return ZuMv(*this); }

  ZmHashParams	m_idHash;
  ZmHashParams	m_tidHash;
};

template <typename ID, typename TID> class ZmTLock;

struct ZmTLock_Held {
  ZuInline ZmTLock_Held() : m_thread(0) { }
  ZuInline ZmTLock_Held(void *thread, int lockCount) :
    m_thread(thread), m_lockCount(lockCount) { }

  ZuInline bool operator !() const { return !m_thread; }
  ZuOpBool

  struct Traits : public ZuBaseTraits<ZmTLock_Held> {
    enum { IsPOD = 1 };
  };
  friend Traits ZuTraitsType(ZmTLock_Held *);

  void		*m_thread;
  int		m_lockCount;
};

inline void *ZmTLock_Held_ThreadAxor(ZmTLock_Held h) { return h.m_thread; }

struct ZmTLock_Test;

template <typename ID, typename TID> class ZmTLock {
  ZmTLock(const ZmTLock &);
  ZmTLock &operator =(const ZmTLock &);	// prevent mis-use

friend ZmTLock_Test;

  enum {				// flags
    Try		= 1,
    Timed	= 2
  };

  using Lock_ = ZmLock;
  using Guard_ = ZmGuard<Lock_>;
  using ReadGuard_ = ZmReadGuard<Lock_>;

  static auto HeapID() { return "ZmTLock"; }

  using Held = ZmTLock_Held;
  using HeldStack_ =
    ZmStack<Held,
      ZmStackKey<ZmTLock_Held_ThreadAxor,
	ZmStackLock<ZmNoLock,
	  ZmStackHeapID<HeapID> > > >;
  struct HeldStack : public HeldStack_ { using HeldStack_::HeldStack_; };
  using HeldStackIterator = typename HeldStack::Iterator;

  struct Lock;
friend Lock;
  struct Lock : public ZmObject {
    template <typename ID_> Lock(ID_ &&id, Lock_ &l) :
      m_id(ZuFwd<ID_>(id)),
      m_useCount(0),
      m_readOK(l),
      m_writeOK(l),
      m_upgradeOK(l),
      m_readCount(0),
      m_upgradeCount(0),
      m_writeCount(0),
      m_lockCount(0),
      m_writeLocker(0) { }
    virtual ~Lock() { }

    // m_lockCount takes the following values:
    //
    // 0	unlocked
    // >0	locked, with count m_lockCount
    // -1	unlocked & deleted
    // <-1	locked & deleted, with count -m_lockCount - 1

    ID				m_id;
    int				m_useCount;	// use count (waiting + held)
    ZmCondition<Lock_>		m_readOK;	// wake-up for read lockers
    ZmCondition<Lock_>		m_writeOK;	// wake-up for write lockers
    ZmCondition<Lock_>		m_upgradeOK;	// wake-up for upgrade lockers
    int				m_readCount;	// #read lockers
    int				m_upgradeCount;	// #pending upgrade lockers
    int				m_writeCount;	// #pending write lockers
    int				m_lockCount;	// write locker lock count
    void			*m_writeLocker;	// write locker
    HeldStack			m_held;		// lockers (inc. write locker)
  };

  using LockRef = ZmRef<Lock>;

  using LockHash_ =
    ZmHashKV<ID, LockRef,
      ZmHashHeapID<HeapID,
	ZmHashLock<ZmNoLock> > >;
  struct LockHash : public LockHash_ { using LockHash_::LockHash_; };

  using LockList_ =
    ZmList<LockRef,
      ZmListLock<ZmNoLock,
	ZmListHeapID<HeapID> > >;
  struct LockList : public LockList_ { using LockList_::LockList_; };

  struct Thread;
friend Thread;
  struct Thread : public ZmObject {
    using LockStack_ = ZmStack<Lock *, ZmStackLock<ZmNoLock> >;
    struct LockStack : public LockStack_ { using LockStack_::LockStack_; };

    template <typename TID_>
    Thread(const TID_ &tid) : m_tid(tid), m_waiting(0) { }

    void readLock(Lock *lock) { m_readLocked.push(lock); }
    bool isReadLocked(Lock *lock) { return m_readLocked.find(lock); }
    bool readUnlock(Lock *lock) { return m_readLocked.del(lock); }

    void writeLock(Lock *lock) { m_writeLocked.push(lock); }
    bool isWriteLocked(Lock *lock) { return m_writeLocked.find(lock); }
    bool writeUnlock(Lock *lock) { return m_writeLocked.del(lock); }

    void upgrade(Lock *lock) { m_upgraded.push(lock); }
    bool isUpgraded(Lock *lock) { return m_upgraded.find(lock); }
    bool downgrade(Lock *lock) { return m_upgraded.del(lock); }

    bool waiting(Lock *lock) {
      {
	HeldStackIterator i(lock->m_held);
	Held *held;

	while (held = i.iteratePtr())
	  if (held->m_lockCount > 0 &&
	      deadlocked((Thread *)held->m_thread)) return true;
      }
      m_waiting = lock;
      return false;
    }

#ifdef ZmTLock_DebugInfiniteRecursion
    struct DepthGuard {
      DepthGuard() { ZmTLock_Depth::instance()->inc(); }
      ~DepthGuard() { ZmTLock_Depth::instance()->dec(); }
    };
#endif
    bool deadlocked(Thread *thread) {
#ifdef ZmTLock_DebugInfiniteRecursion
      DepthGuard depthGuard;
#endif
      Lock *lock;

      // printf("%d -> %d (waiting on lock %d)\n", (int)m_tid, (int)thread->m_tid, thread->m_waiting ? thread->m_waiting->m_id : -1); fflush(stdout);
      if (!(lock = thread->m_waiting)) return false;

      HeldStackIterator i(lock->m_held);
      Held *held;

      while (held = i.iteratePtr())
	if (held->m_lockCount > 0 &&
	    (this == (Thread *)held->m_thread ||
	     deadlocked((Thread *)(held->m_thread)))) return true;
      return false;
    }

    void finalize(ZmTLock &parent) {
      LockStack unlockStack;
      LockRef lock;

      {
	typename LockStack::Iterator i(m_readLocked);

	while (lock = i.iterate()) unlockStack.push(lock);
      }
      {
	typename LockStack::Iterator i(m_writeLocked);

	while (lock = i.iterate()) unlockStack.push(lock);
      }
      {
	typename LockStack::Iterator i(m_upgraded);

	while (lock = i.iterate()) unlockStack.push(lock);
      }

      while (lock = unlockStack.pop()) parent.unlock_(lock, this);
    }

    void running() { m_waiting = 0; }

    TID			m_tid;
    Lock		*m_waiting;
    LockStack		m_readLocked;
    LockStack		m_writeLocked;
    LockStack		m_upgraded;
  };

  using ThreadRef = ZmRef<Thread>;

  using ThreadHash_ = ZmHashKV<TID, ThreadRef, ZmHashLock<ZmNoLock> >;
  struct ThreadHash : public ThreadHash_ { using ThreadHash_::ThreadHash_; };

public:
  ZmTLock(ZmTLockParams params = ZmTLockParams()) {
    m_locks = new LockHash(params.m_idHash);
    m_threads = new ThreadHash(params.m_tidHash);
  }
  virtual ~ZmTLock() { }

private:
  template <typename ID_, typename TID_>
  int readLock_(const ID_ &id, const TID_ &tid, int flags, ZuTime timeout) {
    LockRef lock;
    ThreadRef thread;
    Guard_ guard(m_lock);

    // printf("Read Locking\t(TID = %d, ID = %d)\n", (int)tid, (int)id);

    if (!(lock = m_locks->findVal(id)))
      m_locks->add(id, lock = allocLock(id));

    lock->m_useCount++;

    {
      typename ThreadHash::NodeRef threadNode;

      if (threadNode = m_threads->find(tid))
	thread = threadNode->val();
      else
	m_threads->add(tid, thread = new Thread(tid));
    }

    if (lock->m_writeLocker == thread) { // we already write locked it
      lock->m_lockCount >= 0 ? lock->m_lockCount++ : lock->m_lockCount--;
      // printf("Read Locked 1\t(TID = %d, ID = %d)\n", (int)tid, (int)id);
      return 0;
    }

    if (!lock->m_writeLocker && !lock->m_writeCount) goto acquire;

    if (flags & Try) goto fail;

    if (thread->waiting(lock)) goto fail;

    if (!(flags & Timed))
      while (lock->m_writeLocker || lock->m_writeCount)
	lock->m_readOK.wait();
    else
      while (lock->m_writeLocker || lock->m_writeCount)
	if (lock->m_readOK.timedWait(timeout)) goto fail;

    thread->running();

acquire:
    lock->m_readCount++;
    {
      Held *held = lock->m_held.findPtr(thread);

      if (held)
	++held->m_lockCount;
      else
	lock->m_held.push(Held(thread, 1));
    }
    thread->readLock(lock);
    // printf("Read Locked 3\t(TID = %d, ID = %d)\n", (int)tid, (int)id);
    return 0;

fail:
    if (!--lock->m_useCount) { freeLock(lock); m_locks->del(id); }
    return -1;
  }

  template <typename ID_, typename TID_>
  int writeLock_(const ID_ &id, const TID_ &tid, int flags, ZuTime timeout) {
    LockRef lock;
    ThreadRef thread;
    Guard_ guard(m_lock);

    // printf("Write Locking\t(TID = %d, ID = %d)\n", (int)tid, (int)id);

    {
      typename LockHash::NodeRef lockNode;

      if (lockNode = m_locks->find(id))
	lock = lockNode->val();
      else
	m_locks->add(id, lock = allocLock(id));
    }

    lock->m_useCount++;

    {
      typename ThreadHash::NodeRef threadNode;

      if (threadNode = m_threads->find(tid))
	thread = threadNode->val();
      else
	m_threads->add(tid, thread = new Thread(tid));
    }

    if (lock->m_writeLocker == thread) {	// we already write locked it
      lock->m_lockCount >= 0 ? lock->m_lockCount++ : lock->m_lockCount--;
      // printf("Write Locked 1\t(TID = %d, ID = %d)\n", (int)tid, (int)id);
      return 0;
    }

    Held *held = lock->m_held.findPtr(thread);
    int upgradeCount;

    if (held) {
      held->m_lockCount = -held->m_lockCount;
      upgradeCount = ++lock->m_upgradeCount;
    } else
      upgradeCount = 0;

    if (!lock->m_writeLocker && lock->m_readCount <= upgradeCount)
      goto acquire;

    if (flags & Try) goto fail;

    if (thread->waiting(lock)) goto fail;

    lock->m_writeCount++;

    if (!(flags & Timed))
      do {
	upgradeCount ? lock->m_upgradeOK.wait() : lock->m_writeOK.wait();
	upgradeCount = lock->m_upgradeCount;
      } while (lock->m_writeLocker || lock->m_readCount > upgradeCount);
    else
      do {
	if (upgradeCount ?
	    lock->m_upgradeOK.timedWait(timeout) :
	    lock->m_writeOK.timedWait(timeout)) {
	  lock->m_writeCount--;
	  goto fail;
	}
	upgradeCount = lock->m_upgradeCount;
      } while (lock->m_writeLocker || lock->m_readCount > upgradeCount);

    thread->running();

    lock->m_writeCount--;

acquire:
    lock->m_writeLocker = thread;
    lock->m_lockCount = 1;
    if (upgradeCount) {
      held->m_lockCount = -held->m_lockCount + 1;
      lock->m_upgradeCount--;
      thread->upgrade(lock);
    } else {
      if (held)
	++held->m_lockCount;
      else
	lock->m_held.push(Held(thread, 1));
      thread->writeLock(lock);
    }
    // printf("Write Locked 3\t(TID = %d, ID = %d)\n", (int)tid, (int)id);
    return 0;

fail:
    if (upgradeCount) {
      held->m_lockCount = -held->m_lockCount;
      lock->m_upgradeCount--;
    }

    if (!--lock->m_useCount) m_locks->del(id);
    return -1;
  }

  void unlock_(Lock *lock, Thread *thread) {
    Held *held = lock->m_held.findPtr(thread);

    if (!held) return;
    if (!--held->m_lockCount) lock->m_held.delPtr(held);

    if (!lock->m_writeLocker) {
      thread->readUnlock(lock);
      if (!--lock->m_useCount) goto unused;
      if (--lock->m_readCount <= lock->m_upgradeCount && lock->m_writeCount) {
	if (lock->m_upgradeCount)
	  lock->m_upgradeOK.signal();
	else
	  lock->m_writeOK.signal();
      }
      return;
    }

    if (lock->m_lockCount > 0 ?
	--lock->m_lockCount :
	++lock->m_lockCount != -1) return;

    lock->m_writeLocker = 0;

    if (thread->downgrade(lock)) {
      --lock->m_useCount;
      if (!lock->m_writeCount)
	lock->m_readOK.broadcast();
      return;
    }

    thread->writeUnlock(lock);

    if (!--lock->m_useCount) goto unused;

    if (lock->m_writeCount) {
      if (lock->m_upgradeCount)
	lock->m_upgradeOK.signal();
      else
	lock->m_writeOK.signal();
    } else
      lock->m_readOK.broadcast();
    return;

unused:
    m_locks->del(lock->m_id);
  }


public:
  template <typename ID_, typename TID_>
  ZuInline int readLock(ID_ &&id, TID_ &&tid)
    { return readLock_(ZuFwd<ID_>(id), ZuFwd<TID_>(tid), 0, ZuTime()); }
  template <typename ID_, typename TID_>
  ZuInline int tryReadLock(ID_ &&id, TID_ &&tid)
    { return readLock_(ZuFwd<ID_>(id), ZuFwd<TID_>(tid), Try, ZuTime()); }
  template <typename ID_, typename TID_, typename T>
  ZuInline int timedReadLock(ID_ &&id, TID_ &&tid, T &&t)
    { return readLock_(ZuFwd<ID_>(id), ZuFwd<TID_>(tid), Timed, ZuFwd<T>(t)); }

  template <typename ID_, typename TID_>
  ZuInline int writeLock(ID_ &&id, TID_ &&tid)
    { return writeLock_(ZuFwd<ID_>(id), ZuFwd<TID_>(tid), 0, ZuTime()); }
  template <typename ID_, typename TID_>
  ZuInline int tryWriteLock(ID_ &&id, TID_ &&tid)
    { return writeLock_(ZuFwd<ID_>(id), ZuFwd<TID_>(tid), Try, ZuTime()); }
  template <typename ID_, typename TID_, typename T>
  ZuInline int timedWriteLock(ID_ &&id, TID_ &&tid, T &&t)
    { return writeLock_(ZuFwd<ID_>(id), ZuFwd<TID_>(tid), Timed, ZuFwd<T>(t)); }

#define ZmTLock_ID2LOCK(id, lock, ret) do { \
      typename LockHash::NodeRef lockNode; \
 \
      if (!(lockNode = m_locks->find(id))) return ret; \
      lock = lockNode->val(); \
    } while (0)

#define ZmTLock_TID2THREAD(tid, thread, ret) do { \
      typename ThreadHash::NodeRef threadNode; \
 \
      if (!(threadNode = m_threads->find(tid))) return ret; \
      thread = threadNode->val(); \
    } while (0)

  template <typename ID_, typename TID_>
  void unlock(ID_ &&id, TID_ &&tid) {
    LockRef lock;
    ThreadRef thread;
    Guard_ guard(m_lock);

    ZmTLock_ID2LOCK(ZuFwd<ID_>(id), lock, void());
    ZmTLock_TID2THREAD(ZuFwd<TID_>(tid), thread, void());

    unlock_(lock, thread);
  }

  template <typename ID_, typename TID_>
  bool isReadLocked(ID_ &&id, TID_ &&tid) {
    LockRef lock;
    ThreadRef thread;
    Guard_ guard(m_lock);

    ZmTLock_ID2LOCK(ZuFwd<ID_>(id), lock, false);
    ZmTLock_TID2THREAD(ZuFwd<TID_>(tid), thread, false);

    return lock->m_held.findPtr(thread);
  }

  template <typename ID_, typename TID_>
  bool isWriteLocked(ID_ &&id, TID_ &&tid) {
    LockRef lock;
    ThreadRef thread;
    ReadGuard_ guard(m_lock);

    ZmTLock_ID2LOCK(ZuFwd<ID_>(id), lock, false);
    ZmTLock_TID2THREAD(ZuFwd<TID_>(tid), thread, false);

    return lock->m_writeLocker == thread;
  }

  template <typename ID_, typename TID_>
  bool isUpgraded(ID_ &&id, TID_ &&tid) {
    LockRef lock;
    ThreadRef thread;
    ReadGuard_ guard(m_lock);

    ZmTLock_ID2LOCK(ZuFwd<ID_>(id), lock, false);
    ZmTLock_TID2THREAD(ZuFwd<TID_>(tid), thread, false);

    return lock->m_writeLocker == thread && thread->m_upgraded.find(lock);
  }

  template <typename ID_> ZuCArray<25> dump(ID_ &&id) const {
    LockRef lock;
    ZuCArray<25> s;
    ReadGuard_ guard(m_lock);

    {
      typename LockHash::NodeRef lockNode;

      if (!(lockNode = m_locks->find(ZuFwd<ID_>(id)))) {
	s << "LOCK NOT FOUND";
	return s;
      }
      lock = lockNode->val();
    }

    s << "C" << ZuBoxed(lock->m_useCount).template fmt<ZuFmt::Right<3>>() <<
      ":R" << ZuBoxed(lock->m_readCount).template fmt<ZuFmt::Right<3>>() <<
      ":U" << ZuBoxed(lock->m_upgradeCount).template fmt<ZuFmt::Right<3>>() <<
      ":W" << ZuBoxed(lock->m_writeCount).template fmt<ZuFmt::Right<3>>() <<
      ":L" << ZuBoxed(lock->m_lockCount).template fmt<ZuFmt::Right<3>>();
    return s;
  }

  template <typename ID_, typename OldTID_, typename NewTID_>
  void relock(ID_ &&id, const OldTID_ &oldTID, const NewTID_ &newTID) {
    LockRef lock;
    ThreadRef oldThread, newThread;
    Guard_ guard(m_lock);

    ZmTLock_ID2LOCK(ZuFwd<ID_>(id), lock, void());

    {
      typename ThreadHash::NodeRef threadNode;

      if (!(threadNode = m_threads->find(oldTID))) return;
      oldThread = threadNode->val();

      if (threadNode = m_threads->find(newTID))
	newThread = threadNode->val();
      else
	m_threads->add(newTID, newThread = new Thread(newTID));
    }

    while (oldThread->readUnlock(lock)) newThread->readLock(lock);
    if (lock->m_writeLocker == oldThread) {
      if (oldThread->downgrade(lock))
	newThread->upgrade(lock);
      else {
	oldThread->writeUnlock(lock);
	newThread->writeLock(lock);
      }
      lock->m_writeLocker = newThread;
    }
    Held *held = lock->m_held.findPtr(oldThread);
    if (held) held->m_thread = newThread;
  }

  template <typename TID_>
  void finalize(TID_ &&tid) {
    ThreadRef thread;
    Guard_ guard(m_lock);

    ZmTLock_TID2THREAD(ZuFwd<TID_>(tid), thread, void());

    thread->finalize(*this);
  }

#undef ZmTLock_ID2LOCK
#undef ZmTLock_TID2THREAD

  // unsigned count() { ReadGuard_ guard(m_lock); return m_locks->count_(); }
  unsigned count_() { return m_locks->count_(); }

private:
  template <typename ID_>
  LockRef allocLock(ID_ &&id) {
    LockRef lock = m_freeLocks.popVal();
    if (ZuUnlikely(!lock))
      return new Lock(ZuFwd<ID_>(id), m_lock);
    lock->m_id = ZuFwd<ID_>(id);
    return lock;
  }

  void freeLock(Lock *lock) { m_freeLocks.push(lock); }

  Lock_			m_lock;		// global lock
    ZmRef<LockHash>	  m_locks;	// locks
    ZmRef<ThreadHash>	  m_threads;	// threads
    LockList		  m_freeLocks;	// free list
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZmTLock_HH */
