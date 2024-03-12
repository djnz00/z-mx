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

// invoke stateless lambdas, using ZuLambdaTraits
//
// - works with templated call operators (generic lambdas)

#ifndef ZuInvoke_HPP
#define ZuInvoke_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuLambdaTraits.hpp>

template <typename L, typename ArgList = ZuArgList<L>, typename ...Args_>
ZuStatelessLambda<L, ArgList, ZuLambdaReturn<L, ArgList>>
ZuInvoke(Args_ &&... args) {
  return (*reinterpret_cast<const L *>(0))(ZuFwd<Args_>(args)...);
}
template <
  typename L, typename ArgList = ZuArgList<L>,
  bool = ZuIsStatelessLambda<L, ArgList>{}>
struct ZuInvokeFn_;
template <typename L, typename ...Args>
struct ZuInvokeFn_<L, ZuTypeList<Args...>, true> {
  static ZuLambdaReturn<L, ZuTypeList<Args...>> fn(Args &&... args) {
    return (*reinterpret_cast<const L *>(0))(ZuFwd<Args>(args)...);
  }
};
template <typename L, typename ArgList = ZuArgList<L>>
constexpr auto ZuInvokeFn() {
  return &ZuInvokeFn_<L, ArgList>::fn;
}

#endif /* ZuInvoke_HPP */
