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

// compile-time traits for intrusively reference-counted objects

#ifndef ZuObjectTraits_HPP
#define ZuObjectTraits_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

void ZuObjectType(...);

template <
  typename U,
  typename O = decltype(ZuObjectType(ZuDeclVal<ZuDecay<U> *>()))>
struct ZuObjectTraits {
  using T = O;
  enum { IsObject = 1 };
};
template <typename U>
struct ZuObjectTraits<U, void> {
  enum { IsObject = 0 };
};
template <typename U>
using ZuIsObject = ZuBool<ZuObjectTraits<U>::IsObject>;
template <typename U, typename R = void>
using ZuMatchObject = ZuIfT<ZuObjectTraits<U>::IsObject, R>;
template <typename U, typename R = void>
using ZuNotObject = ZuIfT<!ZuObjectTraits<U>::IsObject, R>;

#endif /* ZuObjectTraits_HPP */
