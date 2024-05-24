//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// compile-time switch

// template <unsigned I> ZuIfT<I == 0> foo() { puts("0"); }
// template <unsigned I> ZuIfT<I == 1> foo() { puts("1"); }
// template <unsigned I> ZuIfT<I == 2> foo() { puts("2"); }

// unsigned i = ...;
// ZuSwitch::dispatch<3>(i, [](auto I) { foo<I>(); });
// ZuSwitch::dispatch<3>(i, [](auto I) { foo<I>(); }, []{ puts("default"); });

// gcc/clang at -O2 or better compiles to a classic switch jump table

// the underlying trick is to use std::initializer_list<> to unpack
// a parameter pack, where each expression in the list is evaluated with
// a side effect that conditionally invokes the lambda, which in turn is
// passed a constexpr index parameter; in this way the initializer_list
// composes the switch statement, each item in it becomes a case, and the
// lambda can invoke code that is specialized by the constexpr index, where
// each specialization is the code body of the corresponding case

#ifndef ZuSwitch_HH
#define ZuSwitch_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuSeq.hh>

#include <initializer_list>

namespace ZuSwitch {

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-value"
template <typename R, typename Seq> struct Dispatch;
template <unsigned ...I> struct Dispatch<void, ZuSeq<I...>> {
  template <typename L>
  constexpr static void fn(unsigned i, L l) {
    std::initializer_list<int>{
      (i == I ? l(ZuUnsigned<I>{}), 0 : 0)...
    };
  }
};
template <typename R, unsigned ...I> struct Dispatch<R, ZuSeq<I...>> {
  template <typename L>
  constexpr static R fn(unsigned i, L l) {
    R r = {};
    std::initializer_list<int>{
      (i == I ? (r = l(ZuUnsigned<I>{})), 0 : 0)...
    };
    return r;
  }
};
#pragma GCC diagnostic pop

template <unsigned N, typename L>
constexpr decltype(auto) dispatch(unsigned i, L l) {
  using R = ZuDecay<decltype(l(ZuUnsigned<0>{}))>;
  return Dispatch<R, ZuMkSeq<N>>::fn(i, ZuMv(l));
}

template <unsigned N, typename L, typename D>
constexpr decltype(auto) dispatch(unsigned i, L l, D d) {
  using R = ZuDecay<decltype(l(ZuUnsigned<0>{}))>;
  if (ZuUnlikely(i >= N)) return d();
  return Dispatch<R, ZuMkSeq<N>>::fn(i, ZuMv(l));
}

} // ZuSwitch

#endif /* ZuSwitch_HH */
