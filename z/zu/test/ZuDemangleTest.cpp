//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuLib.hpp>

#include <zlib/ZuDemangle.hpp>

#include <iostream>

int main()
{
  constexpr auto foo = ZuDefaultAxor();
  std::cout << ZuDemangle<1000>{typeid(foo).name()} << '\n';
}
