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

// compile-time determination of maximum sizeof()
//
// ZuLargest<T1, T2> evaluates to whichever of T1 or T2 is largest

#ifndef ZuLargest_HPP
#define ZuLargest_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuNull.hpp>

template <typename ...Args> struct ZuLargest_;
template <typename Arg0> struct ZuLargest_<Arg0> {
  using T = Arg0;
};
template <typename Arg0, typename ...Args> struct ZuLargest_<Arg0, Args...> {
  using T = ZuIf<(sizeof(Arg0) > sizeof(typename ZuLargest_<Args...>::T)),
    Arg0, typename ZuLargest_<Args...>::T>;
};
template <typename ...Args>
using ZuLargest = typename ZuLargest_<Args...>::T;

#endif /* ZuLargest_HPP */
