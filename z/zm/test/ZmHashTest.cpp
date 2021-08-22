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

/* test program */

#include <zlib/ZuLib.hpp>

#include <stdio.h>
#include <stdlib.h>

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuHash.hpp>

#include <zlib/ZmRef.hpp>
#include <zlib/ZmHash.hpp>
#include <zlib/ZmList.hpp>
#include <zlib/ZmTime.hpp>
#include <zlib/ZmSemaphore.hpp>
#include <zlib/ZmThread.hpp>
#include <zlib/ZmSingleton.hpp>
#include <zlib/ZmSpecific.hpp>
#include <zlib/ZmPolymorph.hpp>

struct X : public ZmPolymorph {
  virtual void helloWorld();
};

void X::helloWorld() { puts("hello world"); }

struct Y : public X {
  virtual void helloWorld();
};

struct Z : public ZmObject {
  int m_z;

  struct Traits : public ZuBaseTraits<Z> {
    enum {  IsPrimitive = 0, IsReal = 0 };
  };
  friend Traits ZuTraitsType(Z *);
};

template <typename>
struct ZCmp {
  static int cmp(const Z *z1, const Z *z2) { return z1->m_z - z2->m_z; }
  static bool less(const Z *z1, const Z *z2) { return z1->m_z < z2->m_z; }
  static bool equals(const Z *z1, const Z *z2) { return z1->m_z == z2->m_z; }
  static bool null(const Z *z) { return !z; }
  static constexpr const Z *null() { return nullptr; }
};

using ZList = ZmList<ZmRef<Z>, ZmListCmp<ZCmp> >;
using ZHash = ZmHashKV<int, ZmRef<Z> >;

void Y::helloWorld() { puts("hello world [Y]"); }

ZmRef<X> foo(X *xPtr) { return(xPtr); }

int hashTestSize = 1000;

void hashIt(ZHash *h) {
  ZmRef<Z> z = new Z;
  int j;

  for (j = 0; j < hashTestSize; j++)
    h->add(j, z);
  for (j = 0; j < hashTestSize; j++)
    h->del(j);
  for (j = 0; j < hashTestSize; j++) {
    h->add(j, z);
    h->del(j);
  }
}

void semPost(ZmSemaphore *sema) {
  int i;

  for (i = 0; i < 10; i++) sema->post();
}

void semWait(ZmSemaphore *sema) {
  int i;

  for (i = 0; i < 10; i++) sema->wait();
}

struct S : public ZmObject {
  S() { m_i = 0; }

  void foo() { m_i++; }

  static void meyers() {
    for (int i = 0; i < 100000; i++) {
      static ZmRef<S> s_ = new S();
      ZmRef<S> s = s_;

      s->foo();
    }
  }

  static void singleton() {
    for (int i = 0; i < 100000; i++) {
      ZmRef<S> s = ZmSingleton<S>::instance();

      s->foo();
    }
  }

  static void specific() {
    for (int i = 0; i < 100000; i++) {
      ZmRef<S> s = ZmSpecific<S>::instance();

      s->foo();
    }
  }

  int	m_i;
};

struct I {
  I(int i) : m_i(i) { }
  I(const I &i) : m_i(i.m_i) { }
  I &operator =(const I &i)
    { if (this != &i) m_i = i.m_i; return *this; }
  int hash() const { return m_i; }
  int cmp(const I &i) const { return ZuCmp<int>::cmp(m_i, i.m_i); }
  bool operator ==(const I &i) const { return m_i == i.m_i; }
  bool operator !() const { return !m_i; }

  int	m_i;
};

struct J : public ZmObject {
  struct IAccessor {
    static const ::I &get(const J *j) { return j->m_i; }
  };
  J(int i) : m_i(i) { }
  J(const J &j) : m_i(j.m_i.m_i) { }
  J &operator =(const J &j) {
    if (this != &j) m_i.m_i = j.m_i.m_i;
    return *this;
  }

  I	m_i;
};

int main(int argc, char **argv)
{
  ZmTime overallStart, overallEnd;

  overallStart.now();

  if (argc > 1) hashTestSize = atoi(argv[1]);

  ZmThread r[80];
  int j, k;
  int n = ZmPlatform::getncpu();

  for (k = 0; k < 10; k++) {
    ZmRef<ZHash> hash2 = new ZHash(
	ZmHashParams().bits(2).loadFactor(1.0).cBits(1));

    printf("hash count, bits, cbits: %d, %d, %d\n",
      hash2->count_(), hash2->bits(), hash2->cBits());
    printf("spawning %d threads...\n", n);

    ZmTime start, end;

    start.now();

    for (j = 0; j < n; j++) r[j] =
      ZmThread(0, ZmFn<>::Bound<&hashIt>::fn(hash2.ptr()));

    for (j = 0; j < n; j++) r[j].join(0);

    end.now();
    end -= start;
    printf("hash time: %d.%.3d\n", (int)end.sec(), (int)(end.nsec() / 1000000));

    printf("%d threads finished\n", n);
    printf("hash count, bits: %d, %d\n", hash2->count_(), hash2->bits());
  }

  for (k = 0; k < 10; k++) {
    ZmRef<ZHash> hash2 = new ZHash(
	ZmHashParams().bits(4).loadFactor(1.0).cBits(4));

    printf("hash count, bits, cbits: %d, %d, %d\n",
      hash2->count_(), hash2->bits(), hash2->cBits());
    printf("spawning %d threads...\n", n);

    ZmTime start, end;

    start.now();

    for (j = 0; j < n; j++)
      r[j] = ZmThread(0, ZmFn<>::Bound<&hashIt>::fn(hash2.ptr()));

    for (j = 0; j < n; j++) r[j].join(0);

    end.now();
    end -= start;
    printf("hash time: %d.%.3d\n", (int)end.sec(), (int)(end.nsec() / 1000000));

    printf("%d threads finished\n", n);
    printf("hash count, bits: %d, %d\n", hash2->count_(), hash2->bits());
  }

  overallEnd.now();
  overallEnd -= overallStart;
  printf("overall time: %d.%.3d\n",
    (int)overallEnd.sec(), (int)(overallEnd.nsec() / 1000000));

  {
    using H = ZmHash<ZmRef<J>, ZmHashKey<J::IAccessor> >;
    ZmRef<H> h_ = new H();
    H &h = *h_;
    for (int k = 0; k < 100; k++) h.add(ZmRef<J>(new J(k)));
    for (int k = 0; k < 100; k++) {
      I i(k);
      ZmRef<J> j = h.findVal(i);
      printf("%d ", k);
    }
    puts("");
    for (int k = 0; k < 100; k++) h.add(ZmRef<J>(new J(42)));
    {
      H::ReadKeyIterator i{h, I{42}};
      ZmRef<J> k;
      while (k = i.iterateVal()) {
	printf("%d ", k->m_i.m_i);
      }
      puts("");
    }
  }
}
