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

// clang at -O2 or better compiles to a classic switch jump table

// the underlying trick is to use std::initializer_list<> to unpack
// a parameter pack, where each expression in the list is evaluated with
// a side effect that conditionally invokes the lambda, which in turn is
// passed a constexpr index parameter; in this way the initializer_list
// composes the switch statement, each item in it becomes a case, and the
// lambda can invoke code that is specialized by the constexpr index, where
// each specialization is the code body of the case

#include <initializer_list>

namespace ZuSwitch {

template <unsigned ...> struct Seq { };

template <typename> struct Unshift;
template <unsigned ...Case>
struct Unshift<Seq<Case...>> {
  using T = Seq<0, (Case + 1)...>;
};

template <unsigned> struct MkSeq;
template <> struct MkSeq<0> { using T = Seq<>; };
template <> struct MkSeq<1> { using T = Seq<0>; };
template <unsigned N> struct MkSeq {
  using T = typename Unshift<typename MkSeq<N - 1>::T>::T;
};

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-value"
template <typename R, typename Seq> struct Dispatch;
template <unsigned ...Case> struct Dispatch<void, Seq<Case...>> {
  template <typename L>
  static void fn(unsigned i, L l) {
    std::initializer_list<int>{
      (i == Case ? l(ZuConstant<Case>{}), 0 : 0)...
    };
  }
};
template <typename R, unsigned ...Case> struct Dispatch<R, Seq<Case...>> {
  template <typename L>
  static R fn(unsigned i, L l) {
    R r;
    std::initializer_list<int>{
      (i == Case ? (r = l(ZuConstant<Case>{})), 0 : 0)...
    };
    return r;
  }
};
#pragma GCC diagnostic pop

template <unsigned N, typename L>
decltype(auto) dispatch(unsigned i, L l) {
  return Dispatch<decltype(l(ZuConstant<0>{})), typename MkSeq<N>::T>::fn(
      i, static_cast<L &&>(l));
}

template <unsigned N, typename L, typename D>
decltype(auto) dispatch(unsigned i, L l, D d) {
  if (ZuUnlikely(i >= N)) return d();
  return Dispatch<decltype(l(ZuConstant<0>{})), typename MkSeq<N>::T>::fn(
      i, static_cast<L &&>(l));
}

} // ZuSwitch

#endif /* ZuSwitch_HPP */
