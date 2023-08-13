//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// compile-time loop unroll

#ifndef ZuUnroll_HPP
#define ZuUnroll_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

// ZuUnroll::all<3>(i, [](auto i) { foo<i>(); });

// gcc/clang at -O2 or better compiles to a fully unrolled sequence

// the underlying trick is to use std::initializer_list<> to unpack
// a parameter pack, where each expression in the list is evaluated with
// a side effect that invokes the lambda, which in turn is
// passed a constexpr index parameter

#include <initializer_list>

namespace ZuUnroll {

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-value"
template <typename R, typename Seq> struct All;
template <unsigned ...I> struct All<void, ZuSeq<I...>> {
  template <typename L>
  constexpr static void fn(L l) {
    std::initializer_list<int>{
      (l(ZuUnsigned<I>{}), 0)...
    };
  }
};

template <typename R, unsigned ...I> struct All<R, ZuSeq<I...>> {
  template <typename L>
  constexpr static R fn(L l) {
    R r;
    std::initializer_list<int>{
      ((r = l(ZuUnsigned<I>{})), 0)...
    };
    return r;
  }
  // map/reduce all()
  template <typename L>
  constexpr static R fn(R r, L l) {
    std::initializer_list<int>{
      ((r = l(ZuUnsigned<I>{}, r)), 0)...
    };
    return r;
  }
};
#pragma GCC diagnostic pop

template <unsigned N, typename L>
constexpr decltype(auto) all(L l) {
  using R = ZuDecay<decltype(l(ZuUnsigned<0>{}))>;
  return All<R, ZuMkSeq<N>>::fn(ZuMv(l));
}

// map/reduce all() - caller supplies initial value of accumulator
template <unsigned N, typename L, typename R>
constexpr decltype(auto) all(R r, L l) {
  return All<R, ZuMkSeq<N>>::fn(ZuMv(r), ZuMv(l));
}

} // ZuUnroll

#endif /* ZuUnroll_HPP */
