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

// condition variable

#ifndef ZmCondition_HPP
#define ZmCondition_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZmPlatform.hpp>
#include <zlib/ZmTime.hpp>
#include <zlib/ZmObject.hpp>
#include <zlib/ZmGuard.hpp>
#include <zlib/ZmSpecific.hpp>
#include <zlib/ZmSemaphore.hpp>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4101 4251)
#endif

template <typename Lock> class ZmCondition_ {
  using Wait = typename Lock::Wait;
  ZmCondition_() = delete;

protected:
  ZmCondition_(Lock &lock) : m_lock{lock} { }

  Wait wait() { return m_lock.wait(); }
  void lock_() { m_lock.lock_(); }
  void unlock_() { m_lock.unlock_(); }

private:
  Lock		&m_lock;
};

template <> class ZmCondition_<ZmNoLock> {
  struct Wait { };

protected:
  ZmCondition_() { }

  Wait wait() { return {}; }
  void lock_() { }
  void unlock_() { }
};

template <typename Lock> class ZmCondition : public ZmCondition_<Lock> {
  ZmCondition() = delete;
  ZmCondition(const ZmCondition &) = delete;
  ZmCondition &operator =(const ZmCondition &) = delete; // prevent mis-use

  using Base = ZmCondition_<Lock>;
  using Wait = typename Base::Wait;
  using Base::wait;
  using Base::lock_;
  using Base::unlock_;

  struct Thread : public ZmObject {
    ZmSemaphore	sem;
    Thread	*next = nullptr;
    Thread	*prev = nullptr;
    bool	waiting = false;
  };

public:
  template <typename Lock_ = Lock>
  ZmCondition(ZuIfT<ZuConversion<ZmNoLock, Lock_>::Same> *_ = nullptr) :
    Base{} { }
  template <typename Lock_ = Lock>
  ZmCondition(
      Lock &lock,
      ZuIfT<ZuConversion<ZmNoLock, Lock_>::Same> *_ = nullptr) :
    Base{lock} { }
  ~ZmCondition() { }

  void wait() {
    Wait wait{this->wait()};
    Thread *thread = ZmSpecific<Thread>::instance();
    thread->next = nullptr;
    thread->waiting = true;
    {
      ZmGuard<ZmPLock> guard(m_condLock);
      thread->prev = m_tail;
      if (m_head)
	m_tail->m_next = thread;
      else
	m_head = thread;
      m_tail = thread;
    }
    unlock_();
    thread->sem.wait();
    lock_();
  }
  int timedWait(ZmTime timeout) {
    Wait wait{this->wait()};
    Thread *thread = ZmSpecific<Thread>::instance();
    thread->next = nullptr;
    thread->waiting = true;
    {
      ZmGuard<ZmPLock> guard(m_condLock);
      thread->prev = m_tail;
      if (m_head)
	m_tail->m_next = thread;
      else
	m_head = thread;
      m_tail = thread;
    }
    unlock_();
    if (thread->sem.timedwait(timeout)) { // timed out
      ZmGuard<ZmPLock> guard(m_condLock);
      if (thread->waiting) { // normal case, still waiting
	if (thread->prev)
	  thread->prev->m_next = thread->next;
	else
	  m_head = thread->next;
	if (thread->next)
	  thread->next->m_prev = thread->prev;
	else
	  m_tail = thread->prev;
	thread->waiting = false;
	guard.unlock();
	lock_();
	return -1;
      }
      guard.unlock(); // timed out, but was concurrently woken
      thread->sem.wait();
    }
    lock_();
    return 0;
  }
  void signal() {
    ZmGuard<ZmPLock> guard(m_condLock);
    if (Thread *thread = m_head) {
      if (!(m_head = thread->m_next)) {
	m_tail = nullptr;
      } else {
	m_head->m_prev = nullptr;
      }
      thread->m_waiting = false;
      guard.unlock();
      thread->m_sem.post();
      return;
    }
  }
  void broadcast() {
loop:
    ZmGuard<ZmPLock> guard(m_condLock);
    if (Thread *thread = m_head) {
      if (!(m_head = thread->m_next)) {
	m_tail = nullptr;
      } else {
	m_head->m_prev = nullptr;
      }
      thread->m_waiting = false;
      guard.unlock();
      thread->m_sem.post();
      goto loop;
    }
  }

private:
  ZmPLock	m_condLock;
    Thread	  *m_head = nullptr;
    Thread	  *m_tail = nullptr;
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZmCondition_HPP */
