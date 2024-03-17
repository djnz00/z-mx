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

// ZmNode - intrusive container node (used by ZmHash, ZmRBTree, ...)

#ifndef ZmNode_HPP
#define ZmNode_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZuNull.hpp>

template <typename Base, typename Heap, bool = ZuInspect<Heap, Base>::Is>
struct ZmNode__;
template <typename Base, typename Heap>
struct ZmNode__<Base, Heap, false> : public Heap, public Base {
  ZmNode__() = default;
  ZmNode__(const ZmNode__ &) = default;
  ZmNode__ &operator =(const ZmNode__ &) = default;
  ZmNode__(ZmNode__ &&) = default;
  ZmNode__ &operator =(ZmNode__ &&) = default;
  template <typename ...Args>
  ZmNode__(Args &&... args) : Base{ZuFwd<Args>(args)...} { }
  template <typename Arg>
  ZmNode__ &operator =(Arg &&arg) {
    return static_cast<ZmNode__ &>(Base::operator =(ZuFwd<Arg>(arg)));
  }
};
template <typename Base, typename Heap>
struct ZmNode__<Base, Heap, true> : public Base {
  ZmNode__() = default;
  ZmNode__(const ZmNode__ &) = default;
  ZmNode__ &operator =(const ZmNode__ &) = default;
  ZmNode__(ZmNode__ &&) = default;
  ZmNode__ &operator =(ZmNode__ &&) = default;
  template <typename ...Args>
  ZmNode__(Args &&... args) : Base{ZuFwd<Args>(args)...} { }
  template <typename Arg>
  ZmNode__ &operator =(Arg &&arg) {
    return static_cast<ZmNode__ &>(Base::operator =(ZuFwd<Arg>(arg)));
  }
};
template <typename Base>
struct ZmNode__<Base, ZuNull, false> : public Base {
  ZmNode__() = default;
  ZmNode__(const ZmNode__ &) = default;
  ZmNode__ &operator =(const ZmNode__ &) = default;
  ZmNode__(ZmNode__ &&) = default;
  ZmNode__ &operator =(ZmNode__ &&) = default;
  template <typename ...Args>
  ZmNode__(Args &&... args) : Base{ZuFwd<Args>(args)...} { }
  template <typename Arg>
  ZmNode__ &operator =(Arg &&arg) {
    return static_cast<ZmNode__ &>(Base::operator =(ZuFwd<Arg>(arg)));
  }
};
template <typename Heap>
struct ZmNode__<ZuNull, Heap, false> : public Heap { };
template <>
struct ZmNode__<ZuNull, ZuNull, true> { };

template <
  typename T,
  auto KeyAxor,
  auto ValAxor,
  typename Base,
  template <typename> class NodeExt,
  typename Heap,
  bool Final,
  bool = ZuInspect<Base, T>::Is>
class ZmNode_;

// node contains type
template <
  typename T_,
  auto KeyAxor_,
  auto ValAxor_,
  typename Base_,
  template <typename> class NodeExt,
  typename Heap>
class ZmNode_<T_, KeyAxor_, ValAxor_, Base_, NodeExt, Heap, false, false> :
    public ZmNode__<Base_, Heap>,
    public NodeExt<
      ZmNode_<T_, KeyAxor_, ValAxor_, Base_, NodeExt, Heap, false, false>> {
#define ZmNode_Impl_NotBase \
public: \
  using T = T_; \
  constexpr static auto KeyAxor = KeyAxor_; \
  constexpr static auto ValAxor = ValAxor_; \
  using U = ZuDecay<T>; \
 \
  ZmNode_() = default; \
  ZmNode_(const ZmNode_ &) = default; \
  ZmNode_ &operator =(const ZmNode_ &) = default; \
  ZmNode_(ZmNode_ &&) = default; \
  ZmNode_ &operator =(ZmNode_ &&) = default; \
  template <typename ...Args> \
  ZmNode_(Args &&... args) : m_data{ZuFwd<Args>(args)...} { } \
  template <typename Arg> \
  ZmNode_ &operator =(Arg &&arg) { \
    m_data = ZuFwd<Arg>(arg); \
    return *this; \
  } \
 \
  const auto &data() const & { return m_data; } \
  auto &data() & { return m_data; } \
  decltype(auto) data() && { return ZuMv(m_data); } \
 \
  decltype(auto) key() const & { return KeyAxor(data()); } \
  decltype(auto) key() & { return KeyAxor(data()); } \
  decltype(auto) key() && { return KeyAxor(data()); } \
 \
  decltype(auto) val() const & { return ValAxor(data()); } \
  decltype(auto) val() & { return ValAxor(data()); } \
  decltype(auto) val() && { return ValAxor(data()); } \
 \
private: \
  U	m_data;

  ZmNode_Impl_NotBase
};
template <
  typename T_,
  auto KeyAxor_,
  auto ValAxor_,
  typename Base_,
  template <typename> class NodeExt,
  typename Heap>
class ZmNode_<T_, KeyAxor_, ValAxor_, Base_, NodeExt, Heap, true, false> final :
    public ZmNode__<Base_, Heap>,
    public NodeExt<
      ZmNode_<T_, KeyAxor_, ValAxor_, Base_, NodeExt, Heap, true, false>> {
  ZmNode_Impl_NotBase
};

// node derives from type
template <
  typename T_,
  auto KeyAxor_,
  auto ValAxor_,
  typename Base_,
  template <typename> class NodeExt,
  typename Heap>
class ZmNode_<T_, KeyAxor_, ValAxor_, Base_, NodeExt, Heap, false, true> :
    public ZmNode__<ZuDecay<T_>, Heap>,
    public NodeExt<
      ZmNode_<T_, KeyAxor_, ValAxor_, Base_, NodeExt, Heap, false, true>> {
#define ZmNode_Impl_IsBase \
  using Base = ZmNode__<ZuDecay<T_>, Heap>; \
 \
public: \
  using T = T_; \
  constexpr static auto KeyAxor = KeyAxor_; \
  constexpr static auto ValAxor = ValAxor_; \
  using U = ZuDecay<T>; \
 \
  ZmNode_() = default; \
  ZmNode_(const ZmNode_ &) = default; \
  ZmNode_ &operator =(const ZmNode_ &) = default; \
  ZmNode_(ZmNode_ &&) = default; \
  ZmNode_ &operator =(ZmNode_ &&) = default; \
  template <typename ...Args> \
  ZmNode_(Args &&... args) : Base{ZuFwd<Args>(args)...} { } \
  template <typename Arg> \
  ZmNode_ &operator =(Arg &&arg) { \
    return static_cast<ZmNode_ &>(Base::operator =(ZuFwd<Arg>(arg))); \
  } \
 \
  decltype(auto) data() const & { return static_cast<const U &>(*this); } \
  decltype(auto) data() & { return static_cast<U &>(*this); } \
  decltype(auto) data() && { return static_cast<U &&>(*this); } \
 \
  decltype(auto) key() const & { return KeyAxor(data()); } \
  decltype(auto) key() & { return KeyAxor(data()); } \
  decltype(auto) key() && { return KeyAxor(data()); } \
 \
  decltype(auto) val() const & { return ValAxor(data()); } \
  decltype(auto) val() & { return ValAxor(data()); } \
  decltype(auto) val() && { return ValAxor(data()); }

  ZmNode_Impl_IsBase
};
template <
  typename T_,
  auto KeyAxor_,
  auto ValAxor_,
  typename Base_,
  template <typename> class NodeExt,
  typename Heap>
class ZmNode_<T_, KeyAxor_, ValAxor_, Base_, NodeExt, Heap, true, true> final :
    public ZmNode__<ZuDecay<T_>, Heap>,
    public NodeExt<
      ZmNode_<T_, KeyAxor_, ValAxor_, Base_, NodeExt, Heap, true, true>> {
  ZmNode_Impl_IsBase
};

template <
  typename T,
  auto KeyAxor,
  auto ValAxor,
  typename Base,
  template <typename> class NodeExt,
  bool Final,
  auto HeapID,
  bool Sharded>
using ZmNode =
  ZmNode_<T, KeyAxor, ValAxor, Base, NodeExt,
    ZmHeap<HeapID,
      sizeof(ZmNode_<T, KeyAxor, ValAxor, Base, NodeExt, ZuNull, Final>),
      Sharded>, Final>;

#endif /* ZmNode_HPP */
