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

// compile-time callable traits
//
// - relies on decltype(&operator()) to deduce function call signature
// - will not work with templated function call operators (generic lambdas)
// - workaround: a wrapper lambda can instantiate the template member function:
// auto generic = []<typename T>(T v) { std::cout << v << '\n' << std::flush; };
// auto wrapper = [&generic](int v) { generic(v); };

#ifndef ZuFnTraits_HPP
#define ZuFnTraits_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuConversion.hpp>
#include <zlib/ZuTraits.hpp>

template <bool, bool, typename, typename, typename ...>
class ZuFnTraits_3 {
public:
  enum { IsStateless = 0 };
};

template <typename R, typename L, typename ...Args>
class ZuFnTraits_3<false, true, R, L, Args...> {
public:
  enum { IsStateless = 1 };
  template <typename ...Args_>
  static R invoke(Args_ &&... args) {
    return (*reinterpret_cast<const L *>(0))(ZuFwd<Args_>(args)...);
  }
  static R invoke_(Args... args) {
    return (*reinterpret_cast<const L *>(0))(ZuFwd<Args>(args)...);
  }
  typedef R (*Fn)(Args...);
  constexpr static Fn fn(L l) { return &ZuFnTraits_3::invoke_; }
  constexpr static Fn fn() { return &ZuFnTraits_3::invoke_; }
};

template <typename L, typename ...Args>
class ZuFnTraits_3<false, true, void, L, Args...> {
public:
  enum { IsStateless = 1 };
  template <typename ...Args_>
  static void invoke(Args_ &&... args) {
    (*reinterpret_cast<const L *>(0))(ZuFwd<Args_>(args)...);
  }
  static void invoke_(Args... args) {
    (*reinterpret_cast<const L *>(0))(ZuFwd<Args>(args)...);
  }
  typedef void (*Fn)(Args...);
  constexpr static Fn fn(L l) { return &ZuFnTraits_3::invoke_; }
  constexpr static Fn fn() { return &ZuFnTraits_3::invoke_; }
};

template <typename R, typename L, typename ...Args>
class ZuFnTraits_3<true, true, R, L, Args...> :
    public ZuFnTraits_3<false, true, R, L, Args...> {
  using Base = ZuFnTraits_3<false, true, R, L, Args...>;
public:
  using typename Base::Fn;
  constexpr static Fn fn(L l) { return static_cast<Fn>(l); }
  constexpr static Fn fn() { return &Base::invoke_; }
};

template <typename R1, typename R2, typename Args1, typename Args2>
struct ZuFnMatch_ { enum { OK = 0 }; };
template <typename R, typename Args>
struct ZuFnMatch_<R, R, Args, Args> { enum { OK = 1 }; };

template <typename R_, typename L_, typename ...Args_>
struct ZuFnTraits_2 : public ZuFnTraits_3<
    ZuConversion<L_, R_ (*)(Args_...)>::Exists, ZuTraits<L_>::IsEmpty,
    R_, L_, Args_...> {
  using R = R_;
  using L = L_;
  using Args = ZuTypeList<Args_...>;
  typedef R (*Fn)(Args_...);
  template <typename R__, typename ...Args__>
  struct Match :
      public ZuFnMatch_<R, R__, Args, ZuTypeList<Args__...>> { };
};

template <auto> struct ZuFnTraits_1;
template <typename R, typename L, typename ...Args, R (L::*Fn)(Args...) const>
struct ZuFnTraits_1<Fn> : public ZuFnTraits_2<R, L, Args...> {
  enum { IsCallable = 1, IsMutable = 0 };
};
template <typename R, typename L, typename ...Args, R (L::*Fn)(Args...)>
struct ZuFnTraits_1<Fn> : public ZuFnTraits_2<R, L, Args...> {
  enum { IsCallable = 1, IsMutable = 1 };
};
template <typename L, typename = void>
struct ZuFnTraits {
  enum { IsCallable = 0, IsMutable = 0, IsStateless = 0 };
};
template <typename L>
struct ZuFnTraits<L, decltype(&L::operator(), void())> :
    public ZuFnTraits_1<&L::operator()> { };

template <typename L, typename T = void>
using ZuIsMutable =
  ZuIfT<ZuFnTraits<L>::IsCallable && ZuFnTraits<L>::IsMutable, T>;
template <typename L, typename T = void>
using ZuNotMutable =
  ZuIfT<ZuFnTraits<L>::IsCallable && !ZuFnTraits<L>::IsMutable, T>;
template <typename L, typename T = void>
using ZuIsStateless =
  ZuIfT<ZuFnTraits<L>::IsCallable && ZuFnTraits<L>::IsStateless, T>;
template <typename L, typename T = void>
using ZuNotStateless =
  ZuIfT<ZuFnTraits<L>::IsCallable && !ZuFnTraits<L>::IsStateless, T>;

#endif /* ZuFnTraits_HPP */
