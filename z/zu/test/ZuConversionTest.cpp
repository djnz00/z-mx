//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuLib.hpp>

#include <stdio.h>

#include <zlib/ZuConversion.hpp>
#include <zlib/ZuUnion.hpp>
#include <zlib/ZuTuple.hpp>
#include <zlib/ZuTraits.hpp>

struct A { };
struct B : public A { };
struct C { operator A() { return A(); } };
struct D : public C { ~D() { puts("~D()"); } };

#define CHECK(x) ((x) ? puts("OK  " #x) : puts("NOK " #x))

inline constexpr auto foo() { return []() { puts("Hello World"); }; }

int main()
{
  CHECK((ZuConversion<void, void>::Exists));
  CHECK((ZuConversion<void, void>::Same));
  CHECK((!ZuConversion<void, void>::Base));
  CHECK((!ZuConversion<void, A>::Exists));
  CHECK((!ZuConversion<void, A>::Same));
  CHECK((!ZuConversion<void, A>::Base));
  CHECK((!ZuConversion<A, void>::Exists));
  CHECK((!ZuConversion<A, void>::Same));
  CHECK((!ZuConversion<A, void>::Base));
  CHECK((ZuConversion<void *, void *>::Exists));
  CHECK((ZuConversion<void *, void *>::Same));
  CHECK((!ZuConversion<void *, void *>::Base));
  CHECK((ZuConversion<A *, void *>::Exists));
  CHECK((!ZuConversion<A *, void *>::Same));
  CHECK((!ZuConversion<A *, void *>::Base));
  CHECK((!ZuConversion<void *, A *>::Exists));
  CHECK((!ZuConversion<void *, A *>::Same));
  CHECK((!ZuConversion<void *, A *>::Base));
  CHECK((ZuConversion<A, A>::Exists));
  CHECK((ZuConversion<A, A>::Same));
  CHECK((!ZuConversion<A, A>::Base));
  CHECK((!ZuConversion<A, B>::Exists));
  CHECK((!ZuConversion<A, B>::Same));
  CHECK((ZuConversion<A, B>::Base));
  CHECK((ZuConversion<B, A>::Exists));
  CHECK((!ZuConversion<B, A>::Same));
  CHECK((!ZuConversion<B, A>::Base));
  CHECK((!ZuConversion<A, C>::Exists));
  CHECK((!ZuConversion<A, C>::Same));
  CHECK((!ZuConversion<A, C>::Base));
  CHECK((ZuConversion<C, A>::Exists));
  CHECK((!ZuConversion<C, A>::Same));
  CHECK((!ZuConversion<C, A>::Base));
  CHECK((ZuConversion<A *, A *>::Exists));
  CHECK((ZuConversion<A *, A *>::Same));
  CHECK((!ZuConversion<A *, A *>::Base));
  CHECK((!ZuConversion<A *, B *>::Exists));
  CHECK((!ZuConversion<A *, B *>::Same));
  CHECK((!ZuConversion<A *, B *>::Base));
  CHECK((ZuConversion<B *, A *>::Exists));
  CHECK((!ZuConversion<B *, A *>::Same));
  CHECK((!ZuConversion<B *, A *>::Base));

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
  constexpr auto baz = []() { puts("Goodbye World"); };
  CHECK((ZuConversion<decltype(foo()), decltype(bar)>::Same));
  CHECK((!ZuConversion<decltype(foo()), decltype(baz)>::Same));

  bar(); baz();
}
