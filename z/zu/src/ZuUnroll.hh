//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// compile-time loop unroll
//
// ZuUnroll::all<3>([](auto i) { foo<i>(); });
// ZuUnroll::all<ZuTypeList<int, bool>>([]<template T>() { foo<T>(); });
//
// gcc/clang at -O2 or better compiles to a fully unrolled sequence
//
// the underlying trick is to use std::initializer_list<> to unpack
// a parameter pack, where each expression in the list is evaluated with
// a side effect that invokes the lambda template operator () with a
// constexpr index parameter
//
// ... note that the old trick of a "swallow" function prevents return
// value map/reduction

#ifndef ZuUnroll_HH
#define ZuUnroll_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#ifdef _MSC_VER
#pragma once
#endif

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

template <typename R, typename ...Ts> struct All<R, ZuTypeList<Ts...>> {
  template <typename L>
  constexpr static R fn(L l) {
    R r;
    std::initializer_list<int>{
      ((r = l.template operator()<Ts>()), 0)...
    };
    return r;
  }
  // map/reduce all()
  template <typename L>
  constexpr static R fn(R r, L l) {
    std::initializer_list<int>{
      ((r = l.template operator()<Ts>(r)), 0)...
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

template <typename ...Ts> struct All<void, ZuTypeList<Ts...>> {
  template <typename L>
  constexpr static void fn(L l) {
    std::initializer_list<int>{
      (l.template operator()<Ts>(), 0)...
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
template <typename ...Ts, typename L> struct Deduce<ZuTypeList<Ts...>, L> {
  using R =
    ZuDecay<decltype(ZuDeclVal<L>().template operator()<ZuType<0, Ts...>>())>;
};

template <typename List, typename L>
constexpr decltype(auto) all(L l) {
  return All<typename Deduce<List, L>::R, List>::fn(ZuMv(l));
}
template <unsigned N, typename L>
constexpr decltype(auto) all(L l) {
  return all<ZuMkSeq<N>>(ZuMv(l));
}

// map/reduce all() - caller supplies initial value of accumulator
template <typename List, typename R, typename L>
constexpr decltype(auto) all(R r, L l) {
  return All<R, List>::fn(ZuMv(r), ZuMv(l));
}
template <unsigned N, typename R, typename L>
constexpr decltype(auto) all(R r, L l) {
  return all<ZuMkSeq<N>>(ZuMv(r), ZuMv(l));
}

} // ZuUnroll

#endif /* ZuUnroll_HH */
