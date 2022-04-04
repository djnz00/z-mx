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

/* read/write transaction monitor test program */

#include <zlib/ZuLib.hpp>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

#include <zlib/ZmAssert.hpp>
#include <zlib/ZmGuard.hpp>
#include <zlib/ZmThread.hpp>
#include <zlib/ZmTLock.hpp>
#include <zlib/ZmSingleton.hpp>

ZmAtomic<int> threads;

#define self() (Zm::getTID())

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
  int tid = self();
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
  int tid = self();
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
    ZmThread(0, ZmFn<>::Ptr<&reader>::fn(), params);
    ZmThread(0, ZmFn<>::Ptr<&reader>::fn(), params);
    ZmThread(0, ZmFn<>::Ptr<&reader>::fn(), params);
    ZmThread(0, ZmFn<>::Ptr<&writer>::fn(), params);
    ZmThread(0, ZmFn<>::Ptr<&writer>::fn(), params);
  }

  for (;;) {
    Zm::sleep(1);
    printf("threads: %d\n", (int)threads);
    if (!threads) break;
  }
  ZmAssert(!tlock()->count_());
}
