//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// semaphore class

#ifndef ZmSemaphore_HH
#define ZmSemaphore_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <zlib/ZmPlatform.hh>
#include <zlib/ZuTime.hh>

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
  ZuInline int timedwait(ZuTime timeout) {
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
  ZuInline int timedwait(ZuTime timeout) {
    timeout -= Zm::now();
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

#endif /* ZmSemaphore_HH */
