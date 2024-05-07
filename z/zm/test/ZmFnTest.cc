//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuLib.hh>

#include <stdio.h>

#include <zlib/ZmRef.hh>
#include <zlib/ZmFn.hh>
#include <zlib/ZmTime.hh>
#include <zlib/ZmAtomic.hh>

struct A {
  A(int i) : m_i(i) { }
  void operator()() { printf("A::operator() %d\n", m_i); }
  int	m_i;
};

struct B {
  B(int i) : m_i(i) { }
  int operator()() { printf("B::operator() %d\n", m_i); return m_i; }
  int	m_i;
};

int C() { printf("C() 44\n"); return 44; }

void D() { printf("D()\n"); }

struct E : public ZmPolymorph {
  E(int i) : m_i(i) { }
  virtual ~E() { }
  virtual void foo() = 0;
  virtual int bar() const = 0;
  static void bah() { printf("E::bah()\n"); }
  int	m_i;
};

struct E_ : public E {
  E_(int i) : E(i) { }
  void foo() { printf("E::foo() %d\n", m_i); }
  int bar() const { printf("E::bar() %d\n", m_i); return m_i; }
};

int F(int *i) { printf("F(%d)\n", *i); return *i; }

struct A1 {
  A1(int i) : m_i(i) { }
  void operator()(int j) { printf("A::operator(%d) %d\n", j, m_i); }
  int	m_i;
};

struct B1 {
  B1(int i) : m_i(i) { }
  int operator()(int j) { printf("B::operator(%d) %d\n", j, m_i); return m_i; }
  int	m_i;
};

int C1(int j) { printf("C1(%d) 44\n", j); return 44; }

void D1(int j) { printf("D(%d)\n", j); }

struct E1 {
  E1(int i) : m_i(i) { }
  void foo(int j) { printf("E::foo(%d) %d\n", j, m_i); }
  int bar(int j) const { printf("E::bar(%d) %d\n", j, m_i); return m_i; }
  static void bah(int j) { printf("E::bah(%d)\n", j); }
  int	m_i;
};

int F1(int *i, int j) { printf("F(%d, %d)\n", *i, j); return *i; }

struct A2 {
  A2(int i) : m_i(i) { }
  void operator()(int j, int k)
    { printf("A::operator(%d, %d) %d\n", j, k, m_i); }
  int	m_i;
};

struct B2 {
  B2(int i) : m_i(i) { }
  int operator()(int j, int k)
    { printf("B::operator(%d, %d) %d\n", j, k, m_i); return m_i; }
  int	m_i;
};

int C2(int j, int k) { printf("C2(%d, %d) 44\n", j, k); return 44; }

void D2(int j, int k) { printf("D(%d, %d)\n", j, k); }

struct E2 : public ZmPolymorph {
  E2(int i) : m_i(i) { }
  void foo(int j, int k) { printf("E::foo(%d, %d) %d\n", j, k, m_i); }
  int bar(int j, int k) const
    { printf("E::bar(%d, %d) %d\n", j, k, m_i); return m_i; }
  static void bah(int j, int k) { printf("E::bah(%d, %d)\n", j, k); }
  int	m_i;
};

struct E3 : public ZmPolymorph {
  template <int N> void foo(int i, int j) {
    printf("E3::foo<%d>(%d, %d)\n", N, i, j);
  }
};

int F2(int *i, int j, int k) { printf("F(%d, %d, %d)\n", *i, j, k); return *i; }

struct X {
  X() : m_i(42) { }
  X(const X &x) : m_i(x.m_i) { }
  X(X &&x) : m_i(x.m_i) { x.m_i = 0; }
  int m_i;
};

struct Base {
  Base(ZmAtomic<uint64_t> &i_) : i(i_) { }
  void foo_() { i.xch(i.load_() + 1); }
  void foo() { foo_(); }
  virtual void bar() { }
  ZmAtomic<uint64_t> &i;
};

struct Derived : public Base {
  Derived(ZmAtomic<uint64_t> &i) : Base(i) { }
  void bar() { Base::foo_(); }
};

struct MoveOnly {
  MoveOnly() : i(42) { }
  ~MoveOnly() { }
  MoveOnly(const MoveOnly &) = delete;
  MoveOnly &operator =(const MoveOnly &) = delete;
  MoveOnly(MoveOnly &&m) : i(m.i) { m.i = 0; }
  MoveOnly &operator =(MoveOnly &&m) { i = m.i; m.i = 0; return *this; }
  int	i;
};

void foo(X &x) { printf("%d\n", x.m_i); }

void fail() { Zm::exit(1); }

#define CHECK(x) ((x) ? puts("OK  " #x) : (fail(), puts("NOK " #x)))

#if 0
template <typename L> void isStateless(const L &l) {
  printf("%d", (int)(ZmFn<>::template LambdaStateless<L, void>::OK));
}
auto fast() { return []{ putchar('a'); }; }
auto slow(char c) { return [c]() { putchar(c); }; }

extern "C" {
  void refFn(ZmObject *o);
  void derefFn(ZmObject *o);
};
#endif

int main()
{
  {
    //ZmRef<ZmFn<> > fa = ZmFn<>::fn(A(42));
    //ZmRef<ZmFn<> > fb = ZmFn<>::fn(B(43));
    ZmFn<> fc = ZmFn<>::Ptr<&C>::fn();
    ZmFn<> fd = ZmFn<>::Ptr<&D>::fn();
    //ZmRef<ZmFn<> > fe1 = ZmFn<>::fn<&E::foo>(E(45));
    //ZmRef<ZmFn<> > fe2 = ZmFn<>::fn<&E::bar>(E(46));
    ZmFn<> fe3 = ZmFn<>::Ptr<&E::bah>::fn();
    int i = 47;
    ZmFn<> ff = ZmFn<>::Bound<&F>::fn(&i);

    //printf("fa(A(42)) returned %d\n", (int)(int64_t)((*fa)()));
    //printf("fb(B(43)) returned %d\n", (int)(int64_t)((*fb)()));
    printf("fc(C) returned %d\n", (int)(int64_t)(fc()));
    printf("fd(D) returned %d\n", (int)(int64_t)(fd()));
    //printf("fe1(E(45)) returned %d\n", (int)(int64_t)((*fe1)()));
    //printf("fe2(E(46)) returned %d\n", (int)(int64_t)((*fe2)()));
    printf("fe3(E::bah) returned %d\n", (int)(int64_t)(fe3()));
    printf("ff(47) returned %d\n", (int)(int64_t)(ff()));

    A *a = new A(47);
    B *b = new B(48);
    ZmRef<E> e1 = new E_(49);
    ZmRef<E> e2 = new E_(50);
    // const E *e2c = e2;

    ZmFn<> fap = ZmFn<>::Member<&A::operator()>::fn(a);
    ZmFn<> fbp = ZmFn<>::Member<&B::operator()>::fn(b);
    // ZmFn<> fe1p = ZmFn<>::Member<&E::foo>::fn(e1.ptr());
    // ZmFn<> fe2p = ZmFn<>::Member<&E::bar>::fn(ZmMkRef(e2c));

    printf("fap(new A(47)) returned %d\n", (int)(int64_t)(fap()));
    printf("fbp(new B(48)) returned %d\n", (int)(int64_t)(fbp()));
    // printf("fe1p(new E(49)) returned %d\n", (int)(int64_t)(fe1p()));
    // printf("fe2p(new E(50)) returned %d\n", (int)(int64_t)(fe2p()));

    delete a;
    delete b;
  }
  {
    //ZmRef<ZmFn<int> > fa = ZmFn<int>::fn(A1(42));
    //ZmRef<ZmFn<int> > fb = ZmFn<int>::fn(B1(43));
    ZmFn<int> fc = ZmFn<int>::Ptr<&C1>::fn();
    ZmFn<int> fd = ZmFn<int>::Ptr<&D1>::fn();
    //ZmRef<ZmFn<int> > fe1 = ZmFn<int>::fn<&E1::foo>(E1(45));
    //ZmRef<ZmFn<int> > fe2 = ZmFn<int>::fn<&E1::bar>(E1(46));
    ZmFn<int> fe3 = ZmFn<int>::Ptr<&E1::bah>::fn();
    int i = 47;
    ZmFn<int> ff = ZmFn<int>::Bound<&F1>::fn(&i);

    //printf("fa(A1(42)) returned %d\n", (int)(int64_t)((*fa)(-42)));
    //printf("fb(B1(43)) returned %d\n", (int)(int64_t)((*fb)(-42)));
    printf("fc(C1) returned %d\n", (int)(int64_t)(fc(-42)));
    printf("fd(D1) returned %d\n", (int)(int64_t)(fd(-42)));
    //printf("fe1(E1(45)) returned %d\n", (int)(int64_t)((*fe1)(-42)));
    //printf("fe2(E1(46)) returned %d\n", (int)(int64_t)((*fe2)(-42)));
    printf("fe3(E1::bah) returned %d\n", (int)(int64_t)(fe3(-42)));
    printf("ff(47) returned %d\n", (int)(int64_t)(ff(-42)));

    A1 *a = new A1(47);
    B1 *b = new B1(48);
    E1 *e1 = new E1(49);
    E1 *e2 = new E1(50);
    const E1 *e2c = e2;

    ZmFn<int> fap = ZmFn<int>::Member<&A1::operator()>::fn(a);
    ZmFn<int> fbp = ZmFn<int>::Member<&B1::operator()>::fn(b);
    ZmFn<int> fe1p = ZmFn<int>::Member<&E1::foo>::fn(e1);
    ZmFn<int> fe2p = ZmFn<int>::Member<&E1::bar>::fn(e2c);

    printf("fap(new A1(47)) returned %d\n", (int)(int64_t)(fap(-42)));
    printf("fbp(new B1(48)) returned %d\n", (int)(int64_t)(fbp(-42)));
    printf("fe1p(new E1(49)) returned %d\n", (int)(int64_t)(fe1p(-42)));
    printf("fe2p(new E1(50)) returned %d\n", (int)(int64_t)(fe2p(-42)));

    delete a;
    delete b;
    delete e1;
    delete e2;
  }
  {
    //ZmRef<ZmFn<int, int> > fa = ZmFn<int, int>::fn(A2(42));
    //ZmRef<ZmFn<int, int> > fb = ZmFn<int, int>::fn(B2(43));
    ZmFn<int, int> fc = ZmFn<int, int>::Ptr<&C2>::fn();
    ZmFn<int, int> fd = ZmFn<int, int>::Ptr<&D2>::fn();
    //ZmRef<ZmFn<int, int> > fe1 = ZmFn<int, int>::fn<&E2::foo>(E2(45));
    //ZmRef<ZmFn<int, int> > fe2 = ZmFn<int, int>::fn<&E2::bar>(E2(46));
    ZmFn<int, int> fe3 = ZmFn<int, int>::Ptr<&E2::bah>::fn();
    int i = 47;
    ZmFn<int, int> ff = ZmFn<int, int>::Bound<&F2>::fn(&i);

    //printf("fa(A2(42)) returned %d\n", (int)(int64_t)((*fa)(-42, -43)));
    //printf("fb(B2(43)) returned %d\n", (int)(int64_t)((*fb)(-42, -43)));
    printf("fc(C2) returned %d\n", (int)(int64_t)(fc(-42, -43)));
    printf("fd(D2) returned %d\n", (int)(int64_t)(fd(-42, -43)));
    //printf("fe1(E2(45)) returned %d\n", (int)(int64_t)((*fe1)(-42, -43)));
    //printf("fe2(E2(46)) returned %d\n", (int)(int64_t)((*fe2)(-42, -43)));
    printf("fe3(E2::bah) returned %d\n", (int)(int64_t)(fe3(-42, -43)));
    printf("ff(47) returned %d\n", (int)(int64_t)(ff(-42, -43)));

    A2 *a = new A2(47);
    B2 *b = new B2(48);
    ZmRef<E2> e1 = new E2(49);
    ZmRef<E2> e2 = new E2(50);
    const E2 *e2c = e2;

    ZmFn<int, int> fap = ZmFn<int, int>::Member<&A2::operator()>::fn(a);
    ZmFn<int, int> fbp = ZmFn<int, int>::Member<&B2::operator()>::fn(b);

    printf("fap(new A2(47)) returned %d\n", (int)(int64_t)(fap(-42, -43)));
    printf("fbp(new B2(48)) returned %d\n", (int)(int64_t)(fbp(-42, -43)));

    CHECK(e1->refCount() == 1);
    CHECK(e2->refCount() == 1);

    {
      ZmFn<int, int> fe1p(ZmFn<int, int>::Member<&E2::foo>::fn(e1.ptr()));
      ZmFn<int, int> fe2p(ZmFn<int, int>::Member<&E2::bar>::fn(ZmMkRef(e2c)));

      CHECK(e1->refCount() == 1);
      CHECK(e2->refCount() == 2);

      printf("fe1p(new E2(49)) returned %d\n", (int)(int64_t)(fe1p(-42, -43)));
      printf("fe2p(new E2(50)) returned %d\n", (int)(int64_t)(fe2p(-42, -43)));
    }

    CHECK(e1->refCount() == 1);
    CHECK(e2->refCount() == 1);

    delete a;
    delete b;
  }
  {
    ZmRef<E3> e3 = new E3();
    using TestFn = ZmFn<int, int>;
    TestFn test = TestFn::Member<&E3::foo<1> >::fn(e3);
  }
  {
    {
      ZmFn<> foo = ZmFn<>::Lambda<ZmLambda_HeapID>::fn(
	  []{ puts("Hello World"); });
      foo();
#if 0
      printf("fast slow ");
      isStateless(fast());
      putchar(' ');
      isStateless(slow(' '));
      putchar('\n');
#endif
    }
    {
      ZmFn<> foo = ZmFn<>([]{ puts("Hello World"); return 42; });
      printf("foo() %d (should be 42)\n", (int)foo());
    }
    ZmRef<E3> e3 = new E3();
    ZmFn<> bar = ZmFn<>::fn([e3]() { e3->foo<1>(1, 1); });
    ZmFn<> baz = ZmFn<>(e3, [](E3 *e3) { e3->foo<1>(1, 1); });
    bar();
    baz();
  }
  {
    ZmRef<E_> e = new E_(42);
    ZmFn<> foo = ZmFn<>(e.ptr(), [](E_ *e) { e->foo(); });
    CHECK(e->refCount() == 1);
    foo();
    const char *s = "Hello World";
    ZmFn<> foo2 = ZmFn<>(e, [s](E_ *e) { puts(s); e->bar(); });
    CHECK(e->refCount() == 2);
    foo2();
  }
  {
    X v;
    X &vr = v;
    foo(vr);
    ZmFn<X &> bar = ZmFn<X &>::Ptr<&foo>::fn();
    bar(vr);
    foo(v);
  }
  {
    ZmFn<MoveOnly> fn{[](MoveOnly m) { printf("%d\n", m.i); } };
    fn(MoveOnly());
    MoveOnly m;
    fn(ZuMv(m));
  }
  {
    ZmRef<E_> e = new E_(42);
    ZmFn<ZmAnyFn *> fn{ZuMv(e), [](E_ *, ZmAnyFn *fn) {
	ZmRef<E_> e = fn->mvObject<E_>();
	CHECK(e->refCount() == 1);
      }};
    fn(&fn);
  }
  {
    ZmRef<E_> e = new E_(42);
    ZmFn<> fn{ZmFn<>::mvFn(ZuMv(e), [](ZmRef<E_> e) {
	CHECK(e->refCount() == 1);
      })};
    fn();
  }
  {
    ZmFn<> fn{[]{ return -42; }};
    short x = fn();
    CHECK(x == -42);
  }
  {
    ZmAtomic<uint64_t> i;
    double baseline;
    {
      Derived d(i);
      ZmTime begin(ZmTime::Now);
      for (unsigned i = 0; i < 1000000000; i++) d.foo();
      ZmTime end(ZmTime::Now); end -= begin;
      baseline = end.dtime();
      puts(ZuStringN<80>{} <<
	  "direct call:\t" << ZuBoxed(baseline).fmt<ZuFmt::FP<9>>() <<
	  "\t(" << ZuBox<uint64_t>(d.i) << ")");
    }
    {
      Derived d(i);
      ZmFn<> bar = ZmFn<>::Member<&Base::foo>::fn(&d);
      ZmTime begin(ZmTime::Now);
      for (unsigned i = 0; i < 1000000000; i++) bar();
      ZmTime end(ZmTime::Now); end -= begin;
      puts(ZuStringN<80>{} << "castFn:\t\t" <<
	  ZuBoxed(end.dtime() - baseline).fmt<ZuFmt::FP<9>>() <<
	  "\t(" << ZuBox<uint64_t>(d.i) << ")");
    }
    {
      Derived d(i);
      ZmFn<> baz = ZmFn<>(&d, [](Derived *d_) { d_->foo(); });
      ZmTime begin(ZmTime::Now);
      for (unsigned i = 0; i < 1000000000; i++) baz();
      ZmTime end(ZmTime::Now); end -= begin;
      puts(ZuStringN<80>{} << "fast lambdaFn:\t" <<
	ZuBoxed(end.dtime() - baseline).fmt<ZuFmt::FP<9>>() <<
	"\t(" << ZuBox<uint64_t>(d.i) << ")");
    }
    {
      Derived d(i);
      ZmFn<> baz = ZmFn<>([&d]() { d.foo(); });
      ZmTime begin(ZmTime::Now);
      for (unsigned i = 0; i < 1000000000; i++) baz();
      ZmTime end(ZmTime::Now); end -= begin;
      puts(ZuStringN<80>{} << "slow lambdaFn:\t" <<
	ZuBoxed(end.dtime() - baseline).fmt<ZuFmt::FP<9>>() <<
	"\t(" << ZuBox<uint64_t>(d.i) << ")");
    }
    {
      Derived d(i);
      Base *b = &d;
      ZmTime begin(ZmTime::Now);
      for (unsigned i = 0; i < 1000000000; i++) b->bar();
      ZmTime end(ZmTime::Now); end -= begin;
      puts(ZuStringN<80>{} << "virtual fn:\t" <<
	ZuBoxed(end.dtime() - baseline).fmt<ZuFmt::FP<9>>() <<
	"\t(" << ZuBox<uint64_t>(d.i) << ")");
    }
  }
}
