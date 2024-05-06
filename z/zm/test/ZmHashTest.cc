//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

/* test program */

#include <zlib/ZuLib.hh>

#include <stdio.h>
#include <stdlib.h>

#include <zlib/ZuTraits.hh>
#include <zlib/ZuHash.hh>

#include <zlib/ZmRef.hh>
#include <zlib/ZmHash.hh>
#include <zlib/ZmList.hh>
#include <zlib/ZmTime.hh>
#include <zlib/ZmSemaphore.hh>
#include <zlib/ZmThread.hh>
#include <zlib/ZmSingleton.hh>
#include <zlib/ZmSpecific.hh>
#include <zlib/ZmPolymorph.hh>

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
    enum { IsPrimitive = 0, IsReal = 0 };
  };
  friend Traits ZuTraitsType(Z *);
};

template <typename>
struct ZCmp {
  static int cmp(const Z *z1, const Z *z2) { return z1->m_z - z2->m_z; }
  static bool less(const Z *z1, const Z *z2) { return z1->m_z < z2->m_z; }
  static bool equals(const Z *z1, const Z *z2) { return z1->m_z == z2->m_z; }
  static bool null(const Z *z) { return !z; }
  constexpr static const Z *null() { return nullptr; }
};

using ZHash = ZmHashKV<unsigned, ZmRef<Z>, ZmHashLock<ZmPLock>>;

void Y::helloWorld() { puts("hello world [Y]"); }

ZmRef<X> foo(X *xPtr) { return(xPtr); }

int hashTestSize = 1000;

void hashIt(ZHash *h) {
  ZmRef<Z> z = new Z;
  unsigned j;

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
  for (unsigned i = 0; i < 10; i++) sema->post();
}

void semWait(ZmSemaphore *sema) {
  for (unsigned i = 0; i < 10; i++) sema->wait();
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
  uint32_t hash() const { return m_i; }
  int cmp(const I &i) const { return ZuCmp<int>::cmp(m_i, i.m_i); }
  bool operator !() const { return !m_i; }

  int	m_i;
};

inline bool operator ==(const I &l, const I &r) { return l.m_i == r.m_i; }

struct J : public ZmObject {
  static const ::I &IAxor(const J *j) { return j->m_i; }
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
  int n = Zm::getncpu();

  for (k = 0; k < 10; k++) {
    ZmRef<ZHash> hash2 = new ZHash(
	ZmHashParams().bits(2).loadFactor(1.0).cBits(1));

    printf("hash count, bits, cbits: %d, %d, %d\n",
      hash2->count_(), hash2->bits(), hash2->cBits());
    printf("spawning %d threads...\n", n);

    ZmTime start, end;

    start.now();

    for (j = 0; j < n; j++) r[j] = {[hash2]() { hashIt(hash2.ptr()); }};

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

    for (j = 0; j < n; j++) r[j] = {[hash2]() { hashIt(hash2.ptr()); }};

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
    using H = ZmHash<ZmRef<J>, ZmHashKey<J::IAxor> >;
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
