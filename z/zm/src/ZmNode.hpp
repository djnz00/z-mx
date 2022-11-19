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

// intrusive container node (used by ZmHash, ZmRBTree, ...)

// ZmNode - container node
// ZmNodePolicy - node ownership (referencing/dereferencing) policy

#ifndef ZmNode_HPP
#define ZmNode_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZuNull.hpp>
#include <zlib/ZuObject.hpp>

template <
  typename T, auto KeyAxor, auto ValAxor, typename Heap, bool Derive,
  template <typename, typename, bool> typename Fn>
class ZmNode;

// node contains type
template <
  typename T_, auto KeyAxor_, auto ValAxor_, typename Heap,
  template <typename, typename, bool> typename Fn_>
class ZmNode<T_, KeyAxor_, ValAxor_, Heap, 0, Fn_> :
  public Fn_<ZmNode<T_, KeyAxor_, ValAxor_, Heap, 0, Fn_>, Heap, 0> {
public:
  using T = T_;
  constexpr static auto KeyAxor = KeyAxor_;
  constexpr static auto ValAxor = ValAxor_;
  using Fn = Fn_<ZmNode, Heap, 0>;
  using U = ZuDecay<T>;

  ZmNode() = default;
  ZmNode(const ZmNode &) = default;
  ZmNode &operator =(const ZmNode &) = default;
  ZmNode(ZmNode &&) = default;
  ZmNode &operator =(ZmNode &&) = default;
  ~ZmNode() = default;

  template <typename ...Args>
  ZmNode(Args &&... args) : m_data{ZuFwd<Args>(args)...} { }

  const auto &data() const & { return m_data; }
  auto &data() & { return m_data; }
  decltype(auto) data() && { return ZuMv(m_data); }

  decltype(auto) key() const & { return KeyAxor(data()); }
  decltype(auto) key() & { return KeyAxor(data()); }
  decltype(auto) key() && { return KeyAxor(data()); }

  decltype(auto) val() const & { return ValAxor(data()); }
  decltype(auto) val() & { return ValAxor(data()); }
  decltype(auto) val() && { return ValAxor(data()); }

private:
  U	m_data;
};

// node derives from type
template <
  typename T_, auto KeyAxor_, auto ValAxor_, typename Heap,
  template <typename, typename, bool> typename Fn_>
class ZmNode<T_, KeyAxor_, ValAxor_, Heap, 1, Fn_> :
  public ZuDecay<T_>,
  public Fn_<ZmNode<T_, KeyAxor_, ValAxor_, Heap, 1, Fn_>, Heap, 1> {
public:
  using T = T_;
  constexpr static auto KeyAxor = KeyAxor_;
  constexpr static auto ValAxor = ValAxor_;
  using Fn = Fn_<ZmNode, Heap, 1>;
  using U = ZuDecay<T>;

  ZmNode() = default;
  ZmNode(const ZmNode &) = default;
  ZmNode &operator =(const ZmNode &) = default;
  ZmNode(ZmNode &&) = default;
  ZmNode &operator =(ZmNode &&) = default;
  ~ZmNode() = default;

  template <typename ...Args>
  ZmNode(Args &&... args) : U{ZuFwd<Args>(args)...} { }

  decltype(auto) data() const & { return static_cast<const U &>(*this); }
  decltype(auto) data() & { return static_cast<U &>(*this); }
  decltype(auto) data() && { return static_cast<U &&>(*this); }

  decltype(auto) key() const & { return KeyAxor(data()); }
  decltype(auto) key() & { return KeyAxor(data()); }
  decltype(auto) key() && { return KeyAxor(data()); }

  decltype(auto) val() const & { return ValAxor(data()); }
  decltype(auto) val() & { return ValAxor(data()); }
  decltype(auto) val() && { return ValAxor(data()); }
};

template <typename O, bool = ZuIsObject_<O>::OK> struct ZmNodePolicy;
// ref-counted nodes
template <typename O> struct ZmNodePolicy<O, true> {
  using Object = O;
  template <typename T> using Ref = ZmRef<T>;
  template <typename T> void nodeRef(T *o) { ZmREF(o); }
  template <typename T> void nodeRef(const Ref<T> &o) { ZmREF(o); }
  template <typename T> void nodeDeref(T *o) { ZmDEREF(o); }
  template <typename T> void nodeDeref(const Ref<T> &o) { ZmDEREF(o); }
  template <typename T> Ref<T> nodeAcquire(T *o) { return Ref<T>::acquire(o); }
  template <typename T> void nodeDelete(T *) { }
  template <typename T> void nodeDelete(const Ref<T> &) { }
};
// own nodes (with app-specified base), delete if not returned to caller
template <typename O> struct ZmNodePolicy<O, false> {
  using Object = O;
  template <typename T> using Ref = T *;
  template <typename T> void nodeRef(T *) { }
  template <typename T> void nodeDeref(T *) { }
  template <typename T> T *nodeAcquire(T *o) { return o; }
  template <typename T> void nodeDelete(T *o) { delete o; }
};
// own nodes, delete if not returned to caller
template <> struct ZmNodePolicy<ZuNull, false> {
  using Object = ZuNull;
  template <typename T> using Ref = T *;
  template <typename T> void nodeRef(T *) { }
  template <typename T> void nodeDeref(T *) { }
  template <typename T> T *nodeAcquire(T *o) { return o; }
  template <typename T> void nodeDelete(T *o) { delete o; }
};
// shadow nodes, never delete
template <> struct ZmNodePolicy<ZuShadow, false> {
  using Object = ZuNull;
  template <typename T> using Ref = T *;
  template <typename T> void nodeRef(T *) { }
  template <typename T> void nodeDeref(T *) { }
  template <typename T> T *nodeAcquire(T *o) { return o; }
  template <typename T> void nodeDelete(T *) { }
};

#endif /* ZmNode_HPP */
