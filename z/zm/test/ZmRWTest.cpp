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

// read/write lock stress test program

#include <zlib/ZuLib.hpp>

#include <stdio.h>

#include <zlib/ZmGuard.hpp>
#include <zlib/ZmThread.hpp>
#include <zlib/ZmFn.hpp>
#include <zlib/ZmRWLock.hpp>
#include <zlib/ZmAtomic.hpp>
#include <zlib/ZmSpecific.hpp>
#include <zlib/ZmTime.hpp>

ZmAtomic<int> gc = 0;

struct C {
  int		m_counter;
  ZmRWLock	m_rwLock;
};

#define self() (Zm::getTID())

struct T : public ZmObject {
  T() : m_state(0), m_tid(self()) { }

  int		m_state;
  int		m_tid;
};

void reader(C *c)
{
  int i;

  for (i = 0; i < 2000; i++) {
    ZmSpecific<T>::instance()->m_state = 1;
    ZmReadGuard<ZmRWLock> guard(c->m_rwLock);
    ZmSpecific<T>::instance()->m_state = 2;

#if 0
    printf("%d Read Locked TID = %d, counter = %d\n",
	   i, (int)self(), c->m_counter);
    if (i > 10) {
      ZmSpecific<T>::instance()->m_state = 3;
      ZmGuard<ZmRWLock> writeGuard(c->m_rwLock);
      ZmSpecific<T>::instance()->m_state = 4;
      printf("%d Upgrade Locked TID = %d, counter = %d -> %d\n",
	     i, (int)self(), c->m_counter, c->m_counter + 1);
      c->m_counter++;
    }
    ZmSpecific<T>::instance()->m_state = 5;
#endif
  }
  ZmSpecific<T>::instance()->m_state = 6;

  --gc;
}

void writer(C *c)
{
  ZmSpecific<T>::instance()->m_state = 7;
  ZmGuard<ZmRWLock> guard(c->m_rwLock);
  ZmSpecific<T>::instance()->m_state = 8;

  printf("Write Locked TID = %d, counter = %d -> %d\n",
	 (int)self(), c->m_counter, c->m_counter + 1);
  c->m_counter++;
  ZmSpecific<T>::instance()->m_state = 9;

  --gc;
}

const char *state(int i)
{
  switch (i) {
  default:
    return "unknown";
  case 0:
    return "initial";
  case 1:
    return "read locking";
  case 2:
    return "read locked";
  case 3:
    return "upgrade locking";
  case 4:
    return "upgrade locked";
  case 5:
    return "upgrade unlocked";
  case 6:
    return "read unlocked";
  case 7:
    return "write locking";
  case 8:
    return "write locked";
  case 9:
    return "write unlocked";
  }
}

void dump_(T *t)
{
  printf("TID %d State %s\n", t->m_tid, state(t->m_state));
}

void dump(C *c)
{
  printf("counter: %d\n", c->m_counter);
  std::cout << c->m_rwLock << '\n';
  ZmSpecific<T>::all(ZmFn<T *>::Ptr<&dump_>::fn());
  fflush(stdout);
}

int main(int argc, char **argv)
{
  C c;

  c.m_counter = 0;

  int ogc = -1;
  for (int i = 0; i < 200; ) {
    if (ogc != gc) { ogc = gc; printf("gc: %d\n", ogc); i++; }
    if (gc < 2) {
      gc++;
      ZmThread([c = &c]() { reader(c); }, ZmThreadParams().detached(true));
    } else if (gc < 100) {
      gc++;
      ZmThread([c = &c]() { writer(c); }, ZmThreadParams().detached(true));
    }
  }
  dump(&c);
  Zm::sleep(1);
}
