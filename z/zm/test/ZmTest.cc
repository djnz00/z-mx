//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

/* test program */

#include <zlib/ZuLib.hh>

#include <stdio.h>
#include <stdlib.h>

#include <zlib/ZuTraits.hh>
#include <zlib/ZuHash.hh>

#include <zlib/ZmRef.hh>
#include <zlib/ZmObject.hh>
#include <zlib/ZmHash.hh>
#include <zlib/ZmList.hh>
#include <zlib/ZmTime.hh>
#include <zlib/ZmSemaphore.hh>
#include <zlib/ZmThread.hh>
#include <zlib/ZmSingleton.hh>
#include <zlib/ZmSpecific.hh>
#include <zlib/ZmStream.hh>

#define mb() __asm__ __volatile__("":::"memory")

#define CHECK(x) ((x) ? puts("OK  " #x) : puts("NOK " #x))

struct X : public ZmObject {
  X() : x(0) { }
  virtual ~X() { }
  virtual void helloWorld();
  void inc() { ++x; }
  unsigned x;
};

void X::helloWorld() { puts("hello world"); }

struct Y : public X {
  virtual void helloWorld();
};

struct Z : public ZmObject { int m_z; };

template <typename>
struct ZCmp {
  static int cmp(const Z *z1, const Z *z2) { return z1->m_z - z2->m_z; }
  static bool less(const Z *z1, const Z *z2) { return z1->m_z < z2->m_z; }
  static bool equals(const Z *z1, const Z *z2) { return z1->m_z == z2->m_z; }
  static bool null(const Z *z) { return !z; }
  static const ZmRef<Z> &null() { static const ZmRef<Z> z; return z; }
};

using ZList = ZmList<ZmRef<Z>, ZmListCmp<ZCmp> >;
using ZHash = ZmHashKV<int, ZmRef<Z> >;

using ZList2 = ZmList<ZuStringN<20>, ZmListNode<ZuStringN<20>>>;

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

  ZmSpecific<X>::instance()->inc();
  for (i = 0; i < 10; i++) sema->post();
}

void semWait(ZmSemaphore *sema) {
  int i;

  ZmSpecific<X>::instance()->inc();
  for (i = 0; i < 10; i++) sema->wait();
}

struct S : public ZmObject {
  S() { m_i = 0; m_j++; }

  void foo() { m_i++; }

  static void meyers() {
    for (int i = 0; i < 100000; i++) {
      static ZmRef<S> s_ = new S();
      S *s = s_;
      s->foo();
      mb();
    }
  }

  static void singleton() {
    for (int i = 0; i < 100000; i++) {
      S *s = ZmSingleton<S>::instance();
      s->foo();
      mb();
    }
  }

  static void specific() {
    for (int i = 0; i < 100000; i++) {
      S *s = ZmSpecific<S>::instance();
      s->foo();
      mb();
    }
  }

  static void tls() {
    for (int i = 0; i < 100000; i++) {
      auto &s = ZmTLS([]() -> ZmRef<S> { return new S(); });
      s->foo();
      mb();
    }
  }

  int			m_i;
  static unsigned	m_j;
};

unsigned S::m_j = 0;

struct W {
  void fn(const char *prefix, const ZmThreadContext *c) {
    const ZmThreadName &s = c->name();
    if (!s)
      printf("%s: %d\n", prefix, (int)c->tid());
    else
      printf("%s: %.*s\n", prefix, s.length(), s.data());
  }
  void fn1(const ZmThreadContext *c) { fn("list1", c); }
  void fn2(const ZmThreadContext *c) { fn("list2", c); }
  void post() { m_sem.post(); }
  void wait() { m_sem.wait(); }
  ZmSemaphore				m_sem;
};

struct O : public ZmObject {
  O() : referenced(0), dereferenced(0) { }
#ifdef ZmObject_DEBUG
  void ref(const void *referrer = 0) const {
    ++referenced;
    ZmObject::ref(referrer);
  }
#else
  void ref() const { ++referenced; }
#endif
#ifdef ZmObject_DEBUG
  bool deref(const void *referrer = 0) const {
    ++dereferenced;
    return ZmObject::deref(referrer);
  }
#else
  bool deref() const { return ++dereferenced >= referenced; }
#endif
  mutable unsigned referenced, dereferenced;
};

int main(int argc, char **argv)
{
  ZuTime overallStart, overallEnd;

  overallStart = Zm::now();

  if (argc > 1) hashTestSize = atoi(argv[1]);

  ZmRef<X> x = new X;

  {
    ZmRef<X> nullPtr;
    ZmRef<X> nullPtr_;

    if (!nullPtr) puts("null test 1 ok");

    nullPtr = x;
    if (nullPtr) puts("null test 2 ok");

    nullPtr = 0;
    if (!nullPtr) puts("null test 3 ok");

    nullPtr_ = x;
    if (nullPtr_) puts("null test 5 ok");

    nullPtr_ = nullPtr;
    if (!nullPtr_) puts("null test 6 ok");

    nullPtr = x;
    if (nullPtr) puts("null test 7 ok");

    nullPtr = nullPtr_;
    if (!nullPtr) puts("null test 8 ok");

    nullPtr_ = (X *)0;
    if (!nullPtr_) puts("null test 9 ok");
  }

  {
    ZmRef<X> xPtr = foo(x);
    ZmRef<X> xPtr_ = foo(x);

    if ((X *)xPtr == &(*xPtr)) puts("cast test 1 ok");
    if ((X *)xPtr_ == &(*xPtr_)) puts("cast test 2 ok");
  }

  {
    ZmRef<X> xPtr(x);

    xPtr->helloWorld();

    ZmRef<X> xPtr2 = x;

    (*xPtr2).helloWorld();

    xPtr = x;

    if (xPtr == xPtr2) puts("equality test 1 ok");
    if (xPtr == (ZmRef<X>)xPtr2) puts("equality test 2 ok");

    xPtr->helloWorld();

    X *xRealPtr = (X *)xPtr2;

    xRealPtr->helloWorld();
  }

  {
    ZmRef<Y> y = new Y;
    { ZmRef<Y> y2 = new Y; }

    ZmRef<Y> yPtr = y;
    ZmRef<X> xPtr = (ZmRef<X>)y;

    xPtr->helloWorld();
    yPtr->helloWorld();
    ((ZmRef<Y>)(Y *)(X *)xPtr)->helloWorld();
  }

  ZmRef<ZHash> hash = new ZHash(ZmHashParams().bits(8));
  ZmRef<Z> z = new Z;

  z->m_z = 1;

  hash->add(0, z);
  hash->add(1, z);
  hash->del(0);

  {
    ZHash::Iterator i(*hash);

    if ((Z *)i.iterate()->val() != (Z *)z)
      puts("collection test failed");
  }

  {
    ZList list;
    ZList list1;
    ZList list2;
    ZmRef<Z> z = new Z;

    z->m_z = 1234;

    list.add(z);
    list.add(z);
    list1.add(z);
    list2.add(z);
    list.del(z);
    list1.add(z);
    list2.add(z);
    z = list1.shiftVal();
    if (z->m_z != 1234) puts("list1 test 1 failed");
    z = list2.shiftVal();
    if (z->m_z != 1234) puts("list2 test 1 failed");
    list.del(z);
    z = list1.shiftVal();
    if (z->m_z != 1234) puts("list1 test 2 failed");
    z = list2.shiftVal();
    if (z->m_z != 1234) puts("list2 test 2 failed");

    ZList list3;
    ZmRef<Z> z2 = new Z, z3 = new Z;

    z2->m_z = 2345;
    z3->m_z = 3456;
    list1.add(z);
    list2.add(z);
    list3.add(z);
    list1.add(z2);
    list2.add(z2);
    list3.add(z2);
    list1.add(z3);
    list2.add(z3);
    list3.add(z3);
#ifdef ZmRef_DEBUG
    printf("z: "); z->debug();
    printf("z2: "); z2->debug();
    printf("z3: "); z3->debug();
#endif
    z = list1.shiftVal();
    if (z->m_z != 1234) puts("list1 test 3 failed");
    z = list2.popVal();
    if (z->m_z != 3456) puts("list2 test 3 failed");
    z = list1.shiftVal();
    if (z->m_z != 2345) puts("list1 test 4 failed");
    z = list2.popVal();
    if (z->m_z != 2345) puts("list2 test 4 failed");
    z = list1.shiftVal();
    if (z->m_z != 3456) puts("list1 test 5 failed");
    z = list2.popVal();
    if (z->m_z != 1234) puts("list2 test 5 failed");

    puts("list3 iteration 1");
    {
      ZList::Iterator iter(list3);

      while (z = iter.iterateVal())
	printf("%d\n", z->m_z);
    }

    puts("list3 iteration 2");
    {
      ZList::Iterator iter(list3);

      while (z = iter.iterateVal())
	printf("%d\n", z->m_z);
    }

    puts("list3 iteration 3");
    {
      ZList::Iterator iter(list3);

      while (z = iter.iterateVal())
	printf("%d\n", z->m_z);
    }

    puts("list tests 1 ok");

    printf("list2 count: %u\n", list2.count_());
  }

  {
    ZList2 list;
    list.addNode(new ZList2::Node("foo"));
    list.addNode(new ZList2::Node("bar"));
    list.addNode(new ZList2::Node("baz"));
    {
      ZList2::Iterator iter(list);
      ZList2::NodeRef z;

      while (z = iter.iterate()) puts(*z);
    }
  }

  ZmThread r[80];
  int j;

  {
    ZmSemaphore *sema = new ZmSemaphore;

    puts("spawning 80 threads...");

    for (j = 0; j < 80; j++)
      r[j] = ZmThread{[sema]() { semPost(sema); }};

    for (j = 0; j < 80; j++) sema->wait();

    for (j = 0; j < 80; j++) r[j].join(0);

    puts("80 threads finished");

    puts("spawning 80 threads...");

    for (j = 0; j < 40; j++)
      r[j] = ZmThread{[sema]() { semWait(sema); }};

    for (j = 40; j < 80; j++)
      r[j] = ZmThread{[sema]() { semPost(sema); }};

    for (j = 0; j < 80; j++) r[j].join(0);

    puts("80 threads finished");

    ZuTime start, end;

    start = Zm::now();

    for (j = 0; j < 1000000; j++) {
      sema->post();
      sema->wait();
    }

    end = Zm::now();
    end -= start;
    printf("sem post/wait time: %d.%.3d / 1000000 = %.16f\n",
	(int)end.sec(), (int)(end.nsec() / 1000000), end.dtime() / 1000000.0);

    delete sema;
  }

  {
    puts("starting ZmPLock lock/unlock time test");

    ZmPLock lock;

    ZuTime start, end;

    start = Zm::now();

    for (j = 0; j < 1000000; j++) { lock.lock(); lock.unlock(); }

    end = Zm::now();
    end -= start;
    printf("lock/unlock time: %d.%.3d / 1000000 = %.16f\n",
	(int)end.sec(), (int)(end.nsec() / 1000000), end.dtime() / 1000000.0);
  }

  {
    puts("starting ref/deref time test");

    ZmRef<ZmObject> l = new ZmObject;

    ZuTime start, end;

    start = Zm::now();

    for (j = 0; j < 1000000; j++) { l->ref(); mb(); }

    end = Zm::now();
    end -= start;
    printf("ref time: %d.%.3d / 1000000 = %.16f\n",
	(int)end.sec(), (int)(end.nsec() / 1000000), end.dtime() / 1000000.0);

    start = Zm::now();

    for (j = 0; j < 1000000; j++) { l->deref(); mb(); }

    end = Zm::now();
    end -= start;
    printf("deref time: %d.%.3d / 1000000 = %.16f\n",
	(int)end.sec(), (int)(end.nsec() / 1000000), end.dtime() / 1000000.0);
  }

  int n = Zm::getncpu();
  int t = n * 1000000;

  {
    ZuTime start, end;

    start = Zm::now();

    for (j = 0; j < n; j++)
      r[j] = ZmThread{S::meyers};
    for (j = 0; j < n; j++)
      r[j].join(0);

    end = Zm::now();
    end -= start;

    printf("Meyers singleton time: %d.%.3d / %d = %.16f\n",
	(int)end.sec(), (int)(end.nsec() / 1000000), t,
	end.dtime() / (double)t);
    printf("S() called %u times\n", S::m_j); S::m_j = 0;
  }

  {
    ZuTime start, end;

    start = Zm::now();

    for (j = 0; j < n; j++)
      r[j] = ZmThread{S::singleton};
    for (j = 0; j < n; j++)
      r[j].join(0);

    end = Zm::now();
    end -= start;
    printf("ZmSingleton::instance() time: %d.%.3d / %d = %.16f\n",
	(int)end.sec(), (int)(end.nsec() / 1000000), t,
	end.dtime() / (double)t);
    printf("S() called %u times\n", S::m_j); S::m_j = 0;
  }

  {
    ZuTime start, end;

    start = Zm::now();

    for (j = 0; j < n; j++)
      r[j] = ZmThread{S::specific};
    for (j = 0; j < n; j++)
      r[j].join(0);

    end = Zm::now();
    end -= start;
    printf("ZmSpecific::instance() time: %d.%.3d / %d = %.16f\n",
	(int)end.sec(), (int)(end.nsec() / 1000000), t,
	end.dtime() / (double)t);
    printf("S() called %u times\n", S::m_j); S::m_j = 0;
  }

  {
    ZuTime start, end;

    start = Zm::now();

    for (j = 0; j < n; j++)
      r[j] = ZmThread{S::tls};
    for (j = 0; j < n; j++)
      r[j].join(0);

    end = Zm::now();
    end -= start;
    printf("thread_local time: %d.%.3d / %d = %.16f\n",
	(int)end.sec(), (int)(end.nsec() / 1000000), t,
	end.dtime() / (double)t);
    printf("S() called %u times\n", S::m_j); S::m_j = 0;
  }


  overallEnd = Zm::now();
  overallEnd -= overallStart;
  printf("overall time: %d.%.3d\n",
    (int)overallEnd.sec(), (int)(overallEnd.nsec() / 1000000));

  {
    W w;
    for (j = 0; j < n; j++)
      r[j] = ZmThread{[w = &w]() { w->wait(); }};
    Zm::sleep(1);
    ZmSpecific<ZmThreadContext>::all(
	[&w](const ZmThreadContext *tc) { w.fn1(tc); });
    for (j = 0; j < n; j++) w.post();
    for (j = 0; j < n; j++) r[j].join(0);
    ZmSpecific<ZmThreadContext>::all(
	[&w](const ZmThreadContext *tc) { w.fn2(tc); });
  }

  {
    ZmRef<O> p;
    {
      ZmRef<O> o = new O();

      CHECK(o->referenced == 1 && !o->dereferenced);
      p = o;
    }
    CHECK(p->referenced == 2 && p->dereferenced == 1);
    ZmRef<O> q = ZuMv(p);
    CHECK(!p);
    CHECK(q->referenced == 2 && q->dereferenced == 1);
  }
}
