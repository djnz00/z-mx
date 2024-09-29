//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuLib.hh>

#include <iostream>

#include <zlib/ZuInspect.hh>
#include <zlib/ZuUnion.hh>
#include <zlib/ZuTuple.hh>
#include <zlib/ZuTraits.hh>
#include <zlib/ZuPrint.hh>

inline void out(const char *s) { std::cout << s << '\n'; }

#define CHECK(x) ((x) ? out("OK  " #x) : out("NOK " #x))

struct A { };
struct B : public A { };
struct C { operator A() { return A(); } };
struct D : public C { ~D() { out("~D()"); } };

constexpr auto foo() { return []{ out("Hello World"); }; }

struct A_Print : public ZuPrintDelegate {
  template <typename S>
  static void print(S &s, const A &) { s << "A{}"; }
};
A_Print ZuPrintType(A *);
using APtr = A *;
using Baz = decltype(ZuPrintType(ZuDeclVal<APtr *>()));
struct APtr_Print : public ZuPrintDelegate {
  template <typename S>
  static void print(S &s, APtr) { s << "&A{}"; }
};
APtr_Print ZuPrintType(APtr *);

int main()
{
  CHECK((ZuInspect<void, void>::Converts));
  CHECK((ZuInspect<void, void>::Same));
  CHECK((!ZuInspect<void, void>::Base));
  CHECK((!ZuInspect<void, A>::Converts));
  CHECK((!ZuInspect<void, A>::Same));
  CHECK((!ZuInspect<void, A>::Base));
  CHECK((!ZuInspect<A, void>::Converts));
  CHECK((!ZuInspect<A, void>::Same));
  CHECK((!ZuInspect<A, void>::Base));
  CHECK((ZuInspect<void *, void *>::Converts));
  CHECK((ZuInspect<void *, void *>::Same));
  CHECK((!ZuInspect<void *, void *>::Base));
  CHECK((ZuInspect<A *, void *>::Converts));
  CHECK((!ZuInspect<A *, void *>::Same));
  CHECK((!ZuInspect<A *, void *>::Base));
  CHECK((!ZuInspect<void *, A *>::Converts));
  CHECK((!ZuInspect<void *, A *>::Same));
  CHECK((!ZuInspect<void *, A *>::Base));
  CHECK((ZuInspect<A, A>::Converts));
  CHECK((ZuInspect<A, A>::Same));
  CHECK((!ZuInspect<A, A>::Base));
  CHECK((!ZuInspect<A, B>::Converts));
  CHECK((!ZuInspect<A, B>::Same));
  CHECK((ZuInspect<A, B>::Base));
  CHECK((ZuInspect<B, A>::Converts));
  CHECK((!ZuInspect<B, A>::Same));
  CHECK((!ZuInspect<B, A>::Base));
  CHECK((!ZuInspect<A, C>::Converts));
  CHECK((!ZuInspect<A, C>::Same));
  CHECK((!ZuInspect<A, C>::Base));
  CHECK((ZuInspect<C, A>::Converts));
  CHECK((!ZuInspect<C, A>::Same));
  CHECK((!ZuInspect<C, A>::Base));
  CHECK((ZuInspect<A *, A *>::Converts));
  CHECK((ZuInspect<A *, A *>::Same));
  CHECK((!ZuInspect<A *, A *>::Base));
  CHECK((!ZuInspect<A *, B *>::Converts));
  CHECK((!ZuInspect<A *, B *>::Same));
  CHECK((!ZuInspect<A *, B *>::Base));
  CHECK((ZuInspect<B *, A *>::Converts));
  CHECK((!ZuInspect<B *, A *>::Same));
  CHECK((!ZuInspect<B *, A *>::Base));

  CHECK(ZuTraits<int>::IsPOD);
  CHECK(ZuTraits<void *>::IsPOD);
  CHECK(ZuTraits<A>::IsPOD);
  CHECK(!ZuTraits<D>::IsPOD);
  CHECK((ZuTraits<ZuUnion<int, void *>>::IsPOD));
  CHECK((ZuTraits<ZuUnion<int, void *, A>>::IsPOD));
  CHECK(!(ZuTraits<ZuUnion<int, void *, D>>::IsPOD));
  CHECK((ZuTraits<ZuTuple<int, void *>>::IsPOD));
  CHECK((ZuTraits<ZuTuple<int, void *, A>>::IsPOD));
  CHECK(!(ZuTraits<ZuTuple<int, void *, D>>::IsPOD));

  constexpr auto bar = foo();
  constexpr auto baz = []{ out("Goodbye World"); };
  CHECK((ZuInspect<decltype(foo()), decltype(bar)>::Same));
  CHECK((!ZuInspect<decltype(foo()), decltype(baz)>::Same));

  bar(); baz();

  {
    A a;
    std::cout << a << '\n';
    std::cout << &a << '\n';
  }
  {
    const int &foo(const int &);
    int &foo(int &);
    CHECK((ZuIsExact<int &, decltype(foo(ZuDeclVal<int &>()))>{}));
    CHECK((ZuIsExact<const int &, decltype(foo(ZuDeclVal<const int &>()))>{}));
    CHECK((!ZuIsExact<int &, const int &>{}));
  }

  {
    CHECK(!(ZuInspect<int, unsigned>::Constructs));
    CHECK(!(ZuInspect<unsigned, int>::Constructs));
    CHECK((ZuInspect<short, int>::Constructs));
  }
}
