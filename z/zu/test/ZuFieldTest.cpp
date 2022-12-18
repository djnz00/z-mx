//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuLib.hpp>

#include <iostream>

#include <zlib/ZuField.hpp>
#include <zlib/ZuPP.hpp>

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
      ((i, Rd)),
      ((j, RdFn)),
      ((k, LambdaRd, ([](const B &b) { return b.k; }))));
}

int main()
{
  using A = Foo::A;
  using B = Foo::B;
  A a;
  ZuType<1, ZuFieldList<A>>::set(a, "bye");
  ZuType<2, ZuFieldList<A>>::set(a, 43.0);
  ZuTypeAll<ZuFieldList<A>>::invoke([&a]<typename T>() mutable {
    std::cout << T::id() << '=' << T::get(a) << '\n';
  });
  B b;
  ZuTypeAll<ZuFieldList<B>>::invoke([&b]<typename T>() mutable {
    std::cout << T::id() << '=' << T::get(b) << '\n';
  });
  std::cout << ZuFieldAxor<A>()(a).print() << '\n';
  std::cout << ZuFieldAxor<A, 1>()(a).print() << '\n';
  return 0;
}
