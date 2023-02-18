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

// compile-time switch

#ifndef ZuSwitch_HPP
#define ZuSwitch_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

// template <unsigned I> ZuIfT<I == 0> foo() { puts("0"); }
// template <unsigned I> ZuIfT<I == 1> foo() { puts("1"); }
// template <unsigned I> ZuIfT<I == 2> foo() { puts("2"); }
// unsigned i = ...;
// ZuSwitch::dispatch<3>(i, [](auto i) { foo<i>(); });
// ZuSwitch::dispatch<3>(
//   i, [](auto i) { foo<i>(); }, []() { puts("default"); });

// gcc/clang at -O2 or better compiles to a classic switch jump table

// the underlying trick is to use std::initializer_list<> to unpack
// a parameter pack, where each expression in the list is evaluated with
// a side effect that conditionally invokes the lambda, which in turn is
// passed a constexpr index parameter; in this way the initializer_list
// composes the switch statement, each item in it becomes a case, and the
// lambda can invoke code that is specialized by the constexpr index, where
// each specialization is the code body of the corresponding case

#include <initializer_list>

namespace ZuSwitch {

template <unsigned ...> struct Seq { };

template <typename> struct Unshift_;
template <unsigned ...Case>
struct Unshift_<Seq<Case...>> {
  using T = Seq<0, (Case + 1)...>;
};
template <typename T> using Unshift = typename Unshift_<T>::T;

template <unsigned> struct MkSeq_;
template <> struct MkSeq_<0> { using T = Seq<>; };
template <> struct MkSeq_<1> { using T = Seq<0>; };
template <unsigned N> struct MkSeq_ {
  using T = Unshift<typename MkSeq_<N - 1>::T>;
};
template <unsigned N> using MkSeq = typename MkSeq_<N>::T;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-value"
template <typename R, typename Seq> struct Dispatch;
template <unsigned ...Case> struct Dispatch<void, Seq<Case...>> {
  template <typename L>
  constexpr static void fn(unsigned i, L l) {
    std::initializer_list<int>{
      (i == Case ? l(ZuConstant<Case>{}), 0 : 0)...
    };
  }
};
template <typename R, unsigned ...Case> struct Dispatch<R, Seq<Case...>> {
  template <typename L>
  constexpr static R fn(unsigned i, L l) {
    R r;
    std::initializer_list<int>{
      (i == Case ? (r = l(ZuConstant<Case>{})), 0 : 0)...
    };
    return r;
  }
};

template <typename R, typename Seq> struct All;
template <unsigned ...Case> struct All<void, Seq<Case...>> {
  template <typename L>
  constexpr static void fn(L l) {
    std::initializer_list<int>{
      (l(ZuConstant<Case>{}), 0)...
    };
  }
};

template <typename R, unsigned ...Case> struct All<R, Seq<Case...>> {
  template <typename L>
  constexpr static R fn(L l) {
    R r;
    std::initializer_list<int>{
      ((r = l(ZuConstant<Case>{})), 0)...
    };
    return r;
  }
  // map/reduce all()
  template <typename L>
  constexpr static R fn(R r, L l) {
    std::initializer_list<int>{
      ((r = l(ZuConstant<Case>{}, r)), 0)...
    };
    return r;
  }
};

#pragma GCC diagnostic pop

template <unsigned N, typename L>
constexpr decltype(auto) dispatch(unsigned i, L l) {
  using R = ZuDecay<decltype(l(ZuConstant<0>{}))>;
  return Dispatch<R, MkSeq<N>>::fn(i, ZuMv(l));
}

template <unsigned N, typename L, typename D>
constexpr decltype(auto) dispatch(unsigned i, L l, D d) {
  using R = ZuDecay<decltype(l(ZuConstant<0>{}))>;
  if (ZuUnlikely(i >= N)) return d();
  return Dispatch<R, MkSeq<N>>::fn(i, ZuMv(l));
}

template <unsigned N, typename L>
constexpr decltype(auto) all(L l) {
  using R = ZuDecay<decltype(l(ZuConstant<0>{}))>;
  return All<R, MkSeq<N>>::fn(ZuMv(l));
}

// map/reduce all() - caller supplies initial value of accumulator
template <unsigned N, typename L, typename R>
constexpr decltype(auto) all(R r, L l) {
  return All<R, MkSeq<N>>::fn(ZuMv(r), ZuMv(l));
}

} // ZuSwitch

#endif /* ZuSwitch_HPP */
