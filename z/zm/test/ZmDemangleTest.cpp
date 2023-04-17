//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuLib.hpp>

#include <zlib/ZmDemangle.hpp>

#include <iostream>

template <template <typename> class, typename>
struct Foo { template <unsigned> static int bar(const char *); };

template <typename> struct Baz { };

int main()
{
  constexpr auto foo = ZuDefaultAxor();
  std::cout << ZmDemangle{"Z13ZuDefaultAxorvEUlTyOT_E_"} << '\n';
  std::cout << ZmDemangle{"Z13ZuDefaultAxorvEUlOT_E_"} << '\n';
  std::cout << typeid(foo).name() << '\n';
  std::cout << ZmDemangle{typeid(foo).name()} << '\n';
  std::cout << ZmDemangle{typeid(Foo<Baz, Baz<int>>).name()} << '\n';
}
