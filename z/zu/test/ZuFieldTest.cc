//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuLib.hh>

#include <iostream>

#include <zlib/ZuField.hh>
#include <zlib/ZuPP.hh>
#include <zlib/ZuUnroll.hh>
#include <zlib/ZuDemangle.hh>

inline void out(const char *s) { std::cout << s << '\n'; }

#define CHECK(x) ((x) ? out("OK  " #x) : out("NOK " #x))

namespace Foo {
  struct A {
    int i = 42;
    const char *j_ = "hello";
    const char *j() const { return j_; }
    void j(const char *s) { j_ = s; }
    double k = 42.0;
  };

  ZuFields(A,
      ((i), (0)),
      ((j, Fn), (1)),
      ((k, Lambda,
	([](const A &a) { return a.k; }),
	([](A &a, double v) { a.k = v; })), (1)));

  struct B {
    int i = 42;
    const char *j_ = "hello";
    const char *j() const { return j_; }
    double k = 42.0;
  };

  ZuFields(B,
      ((i, Rd), (0)),
      ((j, RdFn), (0)),
      ((k, LambdaRd, ([](const B &b) { return b.k; }))));
}

int main()
{
  using A = Foo::A;
  using B = Foo::B;
  A a;
  ZuType<1, ZuFieldList<A>>::set(a, "bye");
  ZuType<2, ZuFieldList<A>>::set(a, 43.0);
  ZuUnroll::all<ZuFieldList<A>>([&a]<typename T>() mutable {
    std::cout << T::id() << '=' << T::get(a) << '\n';
  });
  B b;
  ZuUnroll::all<ZuFieldList<B>>([&b]<typename T>() mutable {
    std::cout << T::id() << '=' << T::get(b) << '\n';
  });
  std::cout << ZuFieldAxor<A>()(a) << '\n';
  std::cout << ZuFieldAxor<A, 1>()(a) << '\n';

  using T1 = ZuTuple<int, const char *>;
  using T2 = ZuFieldKeyT<B, 0>;
  std::cout << "T1 = " << ZuDemangle<T1>{} << '\n';
  std::cout << "T2 = " << ZuDemangle<T2>{} << '\n';
  CHECK((ZuInspect<T1, T2>::Is));

  return 0;
}
