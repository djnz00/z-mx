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

// compile-time stateless lambda <-> function pointer conversion

#ifndef ZuLambdaTraits_HPP
#define ZuLambdaTraits_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuConversion.hpp>

template <bool, typename, typename, typename ...> struct ZuLambdaFn_ { };
template <typename R, typename L, typename ...Args>
struct ZuLambdaFn_<true, R, L, Args...> {
  template <typename ...Args_>
  static R invoke(Args_ &&... args) {
    return (*reinterpret_cast<const L *>(0))(ZuFwd<Args_>(args)...);
  }
  static R invoke_(Args... args) {
    return (*reinterpret_cast<const L *>(0))(ZuFwd<Args>(args)...);
  }
  typedef R (*Fn)(Args...);
  static constexpr Fn fn(L l) { return static_cast<Fn>(l); }
  static constexpr Fn fn() { return &ZuLambdaFn_::invoke_; }
};
template <typename L, typename ...Args>
struct ZuLambdaFn_<true, void, L, Args...> {
  template <typename ...Args_>
  static void invoke(Args_ &&... args) {
    (*reinterpret_cast<const L *>(0))(ZuFwd<Args_>(args)...);
  }
  static void invoke_(Args... args) {
    (*reinterpret_cast<const L *>(0))(ZuFwd<Args>(args)...);
  }
  typedef void (*Fn)(Args...);
  static constexpr Fn fn(L l) { return static_cast<Fn>(l); }
  static constexpr Fn fn() { return &ZuLambdaFn_::invoke_; }
};
template <typename R1, typename R2, typename Args1, typename Args2>
struct ZuLambdaMatch_ { enum { OK = 0 }; };
template <typename R, typename Args>
struct ZuLambdaMatch_<R, R, Args, Args> { enum { OK = 1 }; };
template <typename R_, typename L_, typename ...Args_>
struct ZuLambdaTraits__ {
  using L = L_;
  using R = R_;
  using Args = ZuTypeList<Args_...>;
  typedef R (*Fn)(Args_...);
  enum { IsStateless = ZuConversion<L, Fn>::Exists };
  template <typename R__, typename ...Args__>
  struct Match : public ZuLambdaMatch_<R, R__, Args, ZuTypeList<Args__...>> { };
};
template <auto> struct ZuLambdaTraits_;
template <typename R, typename L, typename ...Args, R (L::*Fn)(Args...) const>
struct ZuLambdaTraits_<Fn> :
   public ZuLambdaTraits__<R, L, Args...>,
   public ZuLambdaFn_<
	    ZuLambdaTraits__<R, L, Args...>::IsStateless, R, L, Args...> {
  enum { IsMutable = 0 };
};
template <typename R, typename L, typename ...Args, R (L::*Fn)(Args...)>
struct ZuLambdaTraits_<Fn> :
   public ZuLambdaTraits__<R, L, Args...>,
   public ZuLambdaFn_<
	    ZuLambdaTraits__<R, L, Args...>::IsStateless, R, L, Args...> {
  enum { IsMutable = 1 };
};
template <typename L> struct ZuLambdaTraits :
    public ZuLambdaTraits_<&L::operator()> { };

template <typename L, typename T = void>
using ZuIsMutable = ZuIfT<ZuLambdaTraits<L>::IsMutable, T>;
template <typename L, typename T = void>
using ZuNotMutable = ZuIfT<!ZuLambdaTraits<L>::IsMutable, T>;
template <typename L, typename T = void>
using ZuIsStateless = ZuIfT<ZuLambdaTraits<L>::IsStateless, T>;
template <typename L, typename T = void>
using ZuNotStateless = ZuIfT<!ZuLambdaTraits<L>::IsStateless, T>;

#endif /* ZuLambdaTraits_HPP */
