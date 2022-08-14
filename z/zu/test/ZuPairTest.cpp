//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuLib.hpp>

#include <stdlib.h>
#include <stdio.h>

#include <zlib/ZuPair.hpp>
#include <zlib/ZuTuple.hpp>
#include <zlib/ZuStringN.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuBox.hpp>

#define CHECK(x) ((x) ? puts("OK  " #x) : puts("NOK " #x))

using VPair = ZuPair<int, int>;
using RVPair = ZuPair<const int &, const int &>;
using LVPair = ZuPair<int &, int &>;
using MVPair = ZuPair<int &&, int &&>;

template <typename P> P mkpair() {
  static int i = 42, j = 42;
  P p(i, j);
  P q = p;
  P &r = q;
  const P &s = r;
  return s;
}

template <typename P> P mvpair() {
  static int i = 42, j = 42;
  P p(static_cast<typename P::T0>(i), static_cast<typename P::T1>(j));
  P q = ZuMv(p);
  return q;
}

static unsigned copied = 0;
static unsigned moved = 0;

struct A {
  A() : i(0) { }
  A(int i_) : i(i_) { }
  A(const A &a) : i(a.i) { ++copied; }
  A &operator =(const A &a) { i = a.i; ++copied; return *this; }
  A(A &&a) : i(a.i) { ++moved; }
  A &operator =(A &&a) { i = a.i; ++moved; return *this; }
  int cmp(const A &a) const { return ZuCmp<int>::cmp(i, a.i); }
  int hash() const { return ZuHash<int>::hash(i); }
  bool operator !() const { return !i; }

  struct Traits : public ZuBaseTraits<A> { enum { IsPrimitive = 0 }; };
  friend Traits ZuTraitsType(A *);

  int i;
};

inline bool operator ==(const A &l, const A &r) { return l.i == r.i; }

ZuPair<A, A> mkapair() { return ZuFwdPair(A(42), A(42)); }
ZuPair<A, A> passapair(ZuPair<A, A> a) { return a; }

ZuTuple<A, A, A> mkatuple() { return ZuFwdTuple(A(42), A(42), A(42)); }
ZuTuple<A, A, A> passatuple(ZuTuple<A, A, A> a) { return a; }

ZuDeclTuple(B, (A, foo), (A, foo2), (A, foo3));

int main()
{
  { VPair p = mkpair<VPair>(); CHECK(p.p<0>() == 42); }
  { RVPair p = mkpair<RVPair>(); CHECK(p.p<0>() == 42); }
  { LVPair p = mkpair<LVPair>(); CHECK(p.p<0>() == 42); }
  { MVPair p = mvpair<MVPair>(); CHECK(p.p<0>() == 42); }
  {
    copied = moved = 0;
    ZuPair<A, A> p = mkapair();
    CHECK(!copied && moved == 2 && p.p<0>().i == 42);
  }
  {
    copied = moved = 0;
    ZuPair<A, A> p(mkapair());
    CHECK(!copied && moved == 2 && p.p<0>().i == 42);
  }
  {
    copied = moved = 0;
    ZuPair<A, A> p(passapair(mkapair()));
    CHECK(!copied && moved == 4 && p.p<0>().i == 42);
  }
  {
    copied = moved = 0;
    ZuTuple<A, A, A> p = mkatuple();
    CHECK(!copied && moved == 3 && p.p<0>().i == 42);
  }
  {
    copied = moved = 0;
    ZuTuple<A, A, A> p(mkatuple());
    CHECK(!copied && moved == 3 && p.p<0>().i == 42);
  }
  {
    copied = moved = 0;
    ZuTuple<A, A, A> p(passatuple(mkatuple()));
    CHECK(!copied && moved == 6 && p.p<0>().i == 42);
  }
  {
    copied = moved = 0;
    ZuTuple<A, A, A> p(passatuple(mkatuple()));
    A a = ZuMv(p.p<0>()), b = ZuMv(p.p<1>()), c = ZuMv(p.p<2>());
    CHECK(!copied && moved == 9);
    CHECK(a.i == 42 && b.i == 42 && c.i == 42);
  }
  {
    copied = moved = 0;
    B p(passatuple(mkatuple()));
    A a = ZuMv(p.foo()), b = ZuMv(p.p<1>()), c = ZuMv(p.p<2>());
    CHECK(!copied && moved == 12);
    CHECK(a.i == 42 && b.i == 42 && c.i == 42);
    B q = B().foo(42), r{p};
    p = q;
    r = ZuMv(q);
  }

  {
    ZuTuple<int, int, int> a{1, 2, 3};
    ZuTuple<ZuBox<int>, int, int> b{a};
    CHECK(b.p<0>() == 1 && b.p<1>() == 2 && b.p<2>() == 3);
    ZuStringN<60> s;
    s << a.print(":");
    CHECK(s == "1:2:3");
    s.null();
    auto c = ZuFwdTuple(a);
    s << c.print(";");
    CHECK(s == "1;2;3");
  }

  {
    using T = ZuTuple<int, float, double, int>;
    T a{1, 2.0F, 3.0, 4};
    auto b = a;
    CHECK(b.v<int>() == 1 && b.v<float>() == 2 && b.v<double>() == 3);
  }
}
