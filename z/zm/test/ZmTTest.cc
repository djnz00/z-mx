//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

/* read/write transaction monitor test program */

#include <zlib/ZuLib.hh>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

#include <zlib/ZmAssert.hh>
#include <zlib/ZmGuard.hh>
#include <zlib/ZmThread.hh>
#include <zlib/ZmTLock.hh>
#include <zlib/ZmSingleton.hh>

ZmAtomic<int> threads;

using TLock = ZmTLock<int, int>;

struct TLockPtr {
  TLockPtr() {
    m_tlock = new TLock(ZmTLockParams().
			idHash(ZmHashParams().bits(8)).
			tidHash(ZmHashParams().bits(8)));
  }
  ~TLockPtr() { delete m_tlock; }
  TLock *m_tlock;
};

static TLock *tlock() {
  return ZmSingleton<TLockPtr>::instance()->m_tlock;
}

void deadlock(int id, int tid)
{
  printf("Deadlock\t(TID = %d, lock ID = %d)\n", (int)tid, id);
}

void readLock(int &result, int id, int tid)
{
  if (result = tlock()->readLock(id, tid))
    deadlock(id, tid);
}

void writeLock(int &result, int id, int tid)
{
  if (result = tlock()->writeLock(id, tid))
    deadlock(id, tid);
}

void unlock(int &result, int id, int tid)
{
  if (!result) tlock()->unlock(id, tid);
}

void reader()
{
  int tid = Zm::getTID();
  int locked[3];
  int i;

  threads++;
  for (i = 0; i < 10000; i++) {
    readLock(locked[0], 1, tid);
    readLock(locked[1], 2, tid);
    readLock(locked[2], 3, tid);
    unlock(locked[2], 3, tid);
    unlock(locked[1], 2, tid);
    unlock(locked[0], 1, tid);
  }
  threads--;
}

void writer()
{
  int tid = Zm::getTID();
  int locked[3];
  int i;

  threads++;
  for (i = 0; i < 10000; i++) {
    writeLock(locked[0], 3, tid);
    writeLock(locked[1], 2, tid);
    writeLock(locked[2], 1, tid);
    unlock(locked[0], 3, tid);
    unlock(locked[1], 2, tid);
    unlock(locked[2], 1, tid);
  }
  threads--;
}

int main()
{
  threads = 0;

  {
    ZmThreadParams params;
    params.detached(true);
    ZmThread{reader, params};
    ZmThread{reader, params};
    ZmThread{reader, params};
    ZmThread{writer, params};
    ZmThread{writer, params};
  }

  for (;;) {
    Zm::sleep(1);
    printf("threads: %d\n", (int)threads);
    if (!threads) break;
  }
  ZmAssert(!tlock()->count_());
}
