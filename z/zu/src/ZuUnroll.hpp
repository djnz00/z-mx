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

// ZuUnroll::all<ZuMkSeq<3>>([](auto i) { foo<i>(); });
// ZuUnroll::all<ZuTypeList<int, bool>>([]<template T>() { foo<T>(); });

// gcc/clang at -O2 or better compiles to a fully unrolled sequence

// the underlying trick is to use std::initializer_list<> to unpack
// a parameter pack, where each expression in the list is evaluated with
// a side effect that invokes the lambda template operator () with a
// constexpr index parameter

#include <initializer_list>

namespace ZuUnroll {

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-value"
template <typename, typename> struct All;

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

template <typename R, typename ...Args> struct All<R, ZuTypeList<Args...>> {
  template <typename L>
  constexpr static R fn(L l) {
    R r;
    std::initializer_list<int>{
      ((r = l.template operator()<Args>()), 0)...
    };
    return r;
  }
  // map/reduce all()
  template <typename L>
  constexpr static R fn(R r, L l) {
    std::initializer_list<int>{
      ((r = l.template operator()<Args>(r)), 0)...
    };
    return r;
  }
};

template <unsigned ...I> struct All<void, ZuSeq<I...>> {
  template <typename L>
  constexpr static void fn(L l) {
    std::initializer_list<int>{
      (l(ZuUnsigned<I>{}), 0)...
    };
  }
};

template <typename ...Args> struct All<void, ZuTypeList<Args...>> {
  template <typename L>
  constexpr static void fn(L l) {
    std::initializer_list<int>{
      (l.template operator()<Args>(), 0)...
    };
  }
};

template <> struct All<void, ZuSeq<>> {
  template <typename L>
  constexpr static void fn(L) { }
};

template <> struct All<void, ZuTypeList<>> {
  template <typename L>
  constexpr static void fn(L) { }
};

template <typename R> struct All<R, ZuSeq<>> {
  template <typename L>
  constexpr static R fn(L) { return {}; }
};

template <typename R> struct All<R, ZuTypeList<>> {
  template <typename L>
  constexpr static R fn(L) { return {}; }
};

#pragma GCC diagnostic pop

template <typename, typename> struct Deduce;
template <typename L> struct Deduce<ZuSeq<>, L> { using R = void; };
template <unsigned ...I, typename L> struct Deduce<ZuSeq<I...>, L> {
  using R = ZuDecay<decltype(ZuDeclVal<L>()(ZuUnsigned<0>{}))>;
};
template <typename L> struct Deduce<ZuTypeList<>, L> { using R = void; };
template <typename ...Args, typename L> struct Deduce<ZuTypeList<Args...>, L> {
  using R =
    ZuDecay<decltype(ZuDeclVal<L>().template operator()<ZuType<0, Args...>>())>;
};
template <typename Args, typename L>
constexpr decltype(auto) all(L l) {
  return All<typename Deduce<Args, L>::R, Args>::fn(ZuMv(l));
}

// map/reduce all() - caller supplies initial value of accumulator
template <typename Args, typename R, typename L>
constexpr decltype(auto) all(R r, L l) {
  return All<R, Args>::fn(ZuMv(r), ZuMv(l));
}

} // ZuUnroll

#endif /* ZuUnroll_HPP */
