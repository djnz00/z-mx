//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuLib.hpp>

#include <stdio.h>
#include <iostream>

#include <zlib/ZuInspect.hpp>
#include <zlib/ZuUnion.hpp>
#include <zlib/ZuTuple.hpp>
#include <zlib/ZuTraits.hpp>
#include <zlib/ZuPrint.hpp>

struct A { };
struct B : public A { };
struct C { operator A() { return A(); } };
struct D : public C { ~D() { puts("~D()"); } };

#define CHECK(x) ((x) ? puts("OK  " #x) : puts("NOK " #x))

inline constexpr auto foo() { return []{ puts("Hello World"); }; }

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
  CHECK((ZuInspect<void, void>::Exists));
  CHECK((ZuInspect<void, void>::Same));
  CHECK((!ZuInspect<void, void>::Base));
  CHECK((!ZuInspect<void, A>::Exists));
  CHECK((!ZuInspect<void, A>::Same));
  CHECK((!ZuInspect<void, A>::Base));
  CHECK((!ZuInspect<A, void>::Exists));
  CHECK((!ZuInspect<A, void>::Same));
  CHECK((!ZuInspect<A, void>::Base));
  CHECK((ZuInspect<void *, void *>::Exists));
  CHECK((ZuInspect<void *, void *>::Same));
  CHECK((!ZuInspect<void *, void *>::Base));
  CHECK((ZuInspect<A *, void *>::Exists));
  CHECK((!ZuInspect<A *, void *>::Same));
  CHECK((!ZuInspect<A *, void *>::Base));
  CHECK((!ZuInspect<void *, A *>::Exists));
  CHECK((!ZuInspect<void *, A *>::Same));
  CHECK((!ZuInspect<void *, A *>::Base));
  CHECK((ZuInspect<A, A>::Exists));
  CHECK((ZuInspect<A, A>::Same));
  CHECK((!ZuInspect<A, A>::Base));
  CHECK((!ZuInspect<A, B>::Exists));
  CHECK((!ZuInspect<A, B>::Same));
  CHECK((ZuInspect<A, B>::Base));
  CHECK((ZuInspect<B, A>::Exists));
  CHECK((!ZuInspect<B, A>::Same));
  CHECK((!ZuInspect<B, A>::Base));
  CHECK((!ZuInspect<A, C>::Exists));
  CHECK((!ZuInspect<A, C>::Same));
  CHECK((!ZuInspect<A, C>::Base));
  CHECK((ZuInspect<C, A>::Exists));
  CHECK((!ZuInspect<C, A>::Same));
  CHECK((!ZuInspect<C, A>::Base));
  CHECK((ZuInspect<A *, A *>::Exists));
  CHECK((ZuInspect<A *, A *>::Same));
  CHECK((!ZuInspect<A *, A *>::Base));
  CHECK((!ZuInspect<A *, B *>::Exists));
  CHECK((!ZuInspect<A *, B *>::Same));
  CHECK((!ZuInspect<A *, B *>::Base));
  CHECK((ZuInspect<B *, A *>::Exists));
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
  constexpr auto baz = []{ puts("Goodbye World"); };
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
}
