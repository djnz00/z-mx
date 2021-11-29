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

// semaphore class

#ifndef ZmSemaphore_HPP
#define ZmSemaphore_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZmPlatform.hpp>
#include <zlib/ZmTime.hpp>

class
#ifndef _WIN32
  __attribute__((aligned(Zm::CacheLineSize)))
#endif
  ZmSemaphore {
  ZmSemaphore(const ZmSemaphore &) = delete;
  ZmSemaphore &operator =(const ZmSemaphore &) = delete;

#ifndef _WIN32
  enum { CacheLineSize = Zm::CacheLineSize };
#endif

public:
#ifndef _WIN32
  ZuInline ZmSemaphore() { sem_init(&m_sem, 0, 0); }
  ZuInline ~ZmSemaphore() { sem_destroy(&m_sem); }

  ZuInline void wait() {
    int r;
    do { r = sem_wait(&m_sem); } while (r < 0 && errno == EINTR);
  }
  ZuInline int trywait() { return sem_trywait(&m_sem); }
  ZuInline int timedwait(ZmTime timeout) {
    do {
      if (!sem_timedwait(&m_sem, &timeout)) return 0;
    } while (errno == EINTR);
    return -1;
  }
  ZuInline void post() { sem_post(&m_sem); }
#else
  ZuInline ZmSemaphore() { m_sem = CreateSemaphore(0, 0, 0x7fffffff, 0); }
  ZuInline ~ZmSemaphore() { CloseHandle(m_sem); }

  ZuInline void wait() { WaitForSingleObject(m_sem, INFINITE); }
  ZuInline int trywait() {
    switch (WaitForSingleObject(m_sem, 0)) {
      case WAIT_OBJECT_0: return 0;
      case WAIT_TIMEOUT:  return -1;
    }
    return -1;
  }
  ZuInline int timedwait(ZmTime timeout) {
    timeout -= ZmTimeNow();
    int m = timeout.millisecs();
    if (m <= 0) return -1;
    if (WaitForSingleObject(m_sem, m) == WAIT_OBJECT_0) return 0;
    return -1;
  }
  ZuInline void post() { ReleaseSemaphore(m_sem, 1, 0); }
#endif
  ZuInline void reset() {
    this->~ZmSemaphore();
    new (this) ZmSemaphore();
  }

private:
#ifndef _WIN32
  sem_t		m_sem;
  char		m_pad_1[CacheLineSize - sizeof(sem_t)];
#else
  HANDLE	m_sem;
#endif
};

#endif /* ZmSemaphore_HPP */
