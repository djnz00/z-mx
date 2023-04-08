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

// ZmNodeFn - node ownership (referencing/dereferencing)

#ifndef ZmNodeFn_HPP
#define ZmNodeFn_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZuObject.hpp>
#include <zlib/ZuPtr.hpp>

template <bool Shadow, bool IsObject>
struct ZmNodeFn_;
// ref-counted nodes
template <> struct ZmNodeFn_<false, true> {
  template <typename T> using Ref = ZmRef<T>;
  template <typename T> using MvRef = ZmRef<T>;
  template <typename T> void nodeRef(T *o) { ZmREF(o); }
  template <typename T> void nodeRef(const Ref<T> &o) { ZmREF(o); }
  template <typename T> void nodeDeref(T *o) { ZmDEREF(o); }
  template <typename T> void nodeDeref(const Ref<T> &o) { ZmDEREF(o); }
  template <typename T> Ref<T> nodeAcquire(T *o) {
    return Ref<T>::acquire(o);
  }
  template <typename T> static void nodeDelete(T *) { }
  template <typename T> static void nodeDelete(const Ref<T> &) { }
};
// own nodes, delete if not returned to caller
template <>
struct ZmNodeFn_<false, false> {
  template <typename T> using Ref = T *;
  template <typename T> using MvRef = ZuPtr<T>;
  template <typename T> static void nodeRef(T *) { }
  template <typename T> static void nodeDeref(T *) { }
  template <typename T> static T *nodeAcquire(T *o) { return o; }
  template <typename T> static void nodeDelete(T *o) { delete o; }
};
// shadow nodes, never delete
template <bool IsObject>
struct ZmNodeFn_<true, IsObject> {
  template <typename T> using Ref = T *;
  template <typename T> using MvRef = T *;
  template <typename T> static void nodeRef(T *) { }
  template <typename T> static void nodeDeref(T *) { }
  template <typename T> static T *nodeAcquire(T *o) { return o; }
  template <typename T> static void nodeDelete(T *) { }
};

template <bool Shadow, typename T, typename NodeBase>
using ZmNodeFn =
  ZmNodeFn_<Shadow, ZuIsObject_<T>::OK || ZuIsObject_<NodeBase>::OK>;

#endif /* ZmNodeFn_HPP */
