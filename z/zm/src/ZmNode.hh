//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// ZmNode - intrusive container node (used by ZmHash, ZmRBTree, ...)

#ifndef ZmNode_HH
#define ZmNode_HH

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <zlib/ZuNull.hh>

template <typename Base, typename Heap, bool = ZuInspect<Heap, Base>::Is>
struct ZmNode__;
template <typename Base, typename Heap>
struct ZmNode__<Base, Heap, false> : public Heap, public Base {
#define ZmNode__Impl_IsBase \
  using Base::Base; \
  template <typename ...Args> \
  ZmNode__(Args &&...args) : Base{ZuFwd<Args>(args)...} { }
  ZmNode__Impl_IsBase
};
template <typename Base, typename Heap>
struct ZmNode__<Base, Heap, true> : public Base {
  ZmNode__Impl_IsBase
};
template <typename Base>
struct ZmNode__<Base, ZuNull, false> : public Base {
  ZmNode__Impl_IsBase
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
  typename NodeExt,
  typename Heap,
  bool = ZuInspect<Base, T>::Is>
class ZmNode_;

// node contains type
template <
  typename T_,
  auto KeyAxor_,
  auto ValAxor_,
  typename Base_,
  typename NodeExt,
  typename Heap>
class ZmNode_<T_, KeyAxor_, ValAxor_, Base_, NodeExt, Heap, false> :
    public ZmNode__<Base_, Heap>,
    public NodeExt {
public:
  using T = T_;
  static constexpr auto KeyAxor = KeyAxor_;
  static constexpr auto ValAxor = ValAxor_;
  using U = ZuDecay<T>;

  ZmNode_() = default;
  ZmNode_(const ZmNode_ &) = default;
  ZmNode_ &operator =(const ZmNode_ &) = default;
  ZmNode_(ZmNode_ &&) = default;
  ZmNode_ &operator =(ZmNode_ &&) = default;
  template <typename ...Args>
  ZmNode_(Args &&...args) : m_data{ZuFwd<Args>(args)...} { }
  template <typename Arg>
  ZmNode_ &operator =(Arg &&arg) {
    m_data = ZuFwd<Arg>(arg);
    return *this;
  }
  virtual ~ZmNode_() = default;

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
  typename T_,
  auto KeyAxor_,
  auto ValAxor_,
  typename Base_,
  typename NodeExt,
  typename Heap>
class ZmNode_<T_, KeyAxor_, ValAxor_, Base_, NodeExt, Heap, true> :
    public ZmNode__<ZuDecay<T_>, Heap>,
    public NodeExt {
  using Base = ZmNode__<ZuDecay<T_>, Heap>;

public:
  using T = T_;
  static constexpr auto KeyAxor = KeyAxor_;
  static constexpr auto ValAxor = ValAxor_;
  using U = ZuDecay<T>;

  using Base::Base;
  using Base::operator =;
  template <typename ...Args>
  ZmNode_(Args &&...args) : Base{ZuFwd<Args>(args)...} { }
  virtual ~ZmNode_() = default;

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

template <
  typename T,
  auto KeyAxor,
  auto ValAxor,
  typename Base,
  typename NodeExt,
  auto HeapID,
  bool Sharded>
using ZmNode =
  ZmNode_<T, KeyAxor, ValAxor, Base, NodeExt,
    ZmHeap<HeapID,
      sizeof(ZmNode_<T, KeyAxor, ValAxor, Base, NodeExt, ZuNull>),
      Sharded>>;

#endif /* ZmNode_HH */
