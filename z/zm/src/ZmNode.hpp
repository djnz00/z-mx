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
  typename T, typename KeyAxor, typename ValAxor, typename Heap, bool Derive,
  template <typename, typename, bool, bool> typename Fn>
class ZmNode;

// node contains type
template <
  typename T_, typename KeyAxor_, typename ValAxor_, typename Heap,
  template <typename, typename, bool, bool> typename Fn_>
class ZmNode<T_, KeyAxor_, ValAxor_, Heap, 0, Fn_> :
  public Fn_<ZmNode<T_, KeyAxor_, ValAxor_, Heap, 0, Fn_>, Heap, 0> {
public:
  using T = T_;
  using KeyAxor = KeyAxor_;
  using ValAxor = ValAxor_;
  using Fn = Fn_<ZmNode, Heap, 0>;
  using U = ZuDecay<T>;

  ZmNode() = default;
  ZmNode(const ZmNode &) = default;
  ZmNode &operator =(const ZmNode &) = default;
  ZmNode(ZmNode &&) = default;
  ZmNode &operator =(ZmNode &&) = default;
  ~ZmNode() = default;

  template <typename P>
  ZmNode(P &&p) : m_data{ZuFwd<P>(p)} { }

  decltype(auto) key() const & { return KeyAxor::get(m_data); }
  decltype(auto) key() & { return KeyAxor::get(m_data); }
  decltype(auto) key() && { return KeyAxor::get(ZuMv(m_data)); }

  decltype(auto) val() const & { return ValAxor::get(m_data); }
  decltype(auto) val() & { return ValAxor::get(m_data); }
  decltype(auto) val() && { return ValAxor::get(ZuMv(m_data)); }

  decltype(auto) data() const & { return m_data; }
  decltype(auto) data() & { return m_data; }
  decltype(auto) data() && { return ZuMv(m_data); }

private:
  U	m_data;
};

// node derives from type
template <
  typename T_, typename KeyAxor_, typename ValAxor_, typename Heap,
  template <typename, typename, bool, bool> typename Fn_>
class ZmNode<T_, KeyAxor_, ValAxor_, Heap, 1, Fn_> :
  public ZuDecay<T_>,
  public Fn_<ZmNode<T_, KeyAxor_, ValAxor_, Heap, 1, Fn_>, Heap, 1> {
public:
  using T = T_;
  using KeyAxor = KeyAxor_;
  using ValAxor = ValAxor_;
  using Fn = Fn_<ZmNode, Heap, 1>;
  using U = ZuDecay<T>;

  ZmNode() = default;
  ZmNode(const ZmNode &) = default;
  ZmNode &operator =(const ZmNode &) = default;
  ZmNode(ZmNode &&) = default;
  ZmNode &operator =(ZmNode &&) = default;
  ~ZmNode() = default;

  template <typename P>
  ZmNode(P &&p) : U{ZuFwd<P>(p)} { }

  decltype(auto) key() const & { return KeyAxor::get(*this); }
  decltype(auto) key() & { return KeyAxor::get(*this); }
  decltype(auto) key() && { return KeyAxor::get(ZuMv(*this)); }

  decltype(auto) val() const & { return ValAxor::get(*this); }
  decltype(auto) val() & { return ValAxor::get(*this); }
  decltype(auto) val() && { return ValAxor::get(ZuMv(*this)); }

  decltype(auto) data() const & { return static_cast<const U &>(*this); }
  decltype(auto) data() & { return static_cast<U &>(*this); }
  decltype(auto) data() && { return static_cast<U &&>(*this); }
};

template <typename, bool> struct ZmNodePolicy_;
template <typename O>
struct ZmNodePolicy : public ZmNodePolicy_<O, ZuIsObject_<O>::OK> { };
// ref-counted nodes
template <typename O> struct ZmNodePolicy_<O, true> {
  using Object = O;
  template <typename T_> struct Ref { using T = ZmRef<T_>; };
  template <typename T> void nodeRef(T &&o) { ZmREF(ZuFwd<T>(o)); }
  template <typename T> void nodeDeref(T &&o) { ZmDEREF(ZuFwd<T>(o)); }
  template <typename T> void nodeDelete(T &&) { }
};
// own nodes (with app-specified base), delete if not returned to caller
template <typename O> struct ZmNodePolicy_<O, false> {
  using Object = O;
  template <typename T_> struct Ref { using T = T_ *; };
  template <typename T> void nodeRef(T &&) { }
  template <typename T> void nodeDeref(T &&) { }
  template <typename T> void nodeDelete(T &&o) { delete ZuFwd<T>(o); }
};
// own nodes, delete if not returned to caller
template <> struct ZmNodePolicy<ZuNull> {
  using Object = ZuNull;
  template <typename T_> struct Ref { using T = T_ *; };
  template <typename T> void nodeRef(T &&) { }
  template <typename T> void nodeDeref(T &&) { }
  template <typename T> void nodeDelete(T &&o) { delete ZuFwd<T>(o); }
};
// shadow nodes, never delete
template <> struct ZmNodePolicy<ZuShadow> {
  using Object = ZuNull;
  template <typename T_> struct Ref { using T = T_ *; };
  template <typename T> void nodeRef(T &&) { }
  template <typename T> void nodeDeref(T &&) { }
  template <typename T> void nodeDelete(T &&) { }
};

#endif /* ZmNode_HPP */
