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

// generic discriminated union
//
// using U = ZuUnion<int, double>;
// U u, v;
// *(u.init<int>()) = 42;
// u.p<0>(42);
// u.p<0>() = 42;
// v = u;
// if (v.type() == 0) { printf("%d\n", v.p<0>()); }
// v.p<1>(42.0);
// if (v.contains<double>()) { printf("%g\n", v.v<double>()); }
// u.~U();
// *reinterpret_cast<int *>(&u) = 43; // *(u.ptr_<Index<int>::I>()) = ...
// u.type_(U::Index<int>::I);
// printf("%d\n", u.v<int>());
//
// namespace {
//   void print(int i) const { printf("%d\n", i); }
//   void print(double d) const { printf("%g\n", d); }
// };
// u.cdispatch([]<typename I>(I &&i) { print(ZuFwd<I>(i)); });
// ZuSwitch::dispatch<U::N>(u.type(), [&u](auto i) { print(u.p<i>()); });

#ifndef ZuUnion_HPP
#define ZuUnion_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuLargest.hpp>
#include <zlib/ZuNull.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuHash.hpp>
#include <zlib/ZuConversion.hpp>
#include <zlib/ZuSwitch.hpp>
#include <zlib/ZuPP.hpp>

template <typename> struct ZuUnion_OpBool;
template <> struct ZuUnion_OpBool<bool> { using T = void; };
template <typename T, typename = void> struct ZuUnion_OpStar {
  static bool star(const void *p) {
    return !ZuCmp<T>::null(*static_cast<const T *>(p));
  }
};
template <typename T> struct ZuUnion_OpStar<T,
	 typename ZuUnion_OpBool<decltype(*(ZuDeclVal<const T &>()))>::T> {
  static bool star(const void *p) {
    return *(*static_cast<const T *>(p));
  }
};
template <typename T, typename = void> struct ZuUnion_OpBang {
  static bool bang(const void *p) {
    return !(*static_cast<const T *>(p));
  }
};
template <typename T>
struct ZuUnion_OpBang<T,
    typename ZuUnion_OpBool<decltype(!(ZuDeclVal<const T &>()))>::T> {
  static bool bang(const void *p) {
    return ZuCmp<T>::null(*static_cast<const T *>(p));
  }
};

template <typename T, bool IsPrimitive, bool IsPointer> class ZuUnion_Ops_;
template <typename T> class ZuUnion_Ops_<T, 0, 0> :
  public ZuUnion_OpStar<T>, public ZuUnion_OpBang<T> {
public:
  static void ctor(void *p) { new (p) T(); }
  template <typename V>
  static void ctor(void *p, V &&v) { new (p) T(ZuFwd<V>(v)); }
  static void dtor(void *p) { static_cast<T *>(p)->~T(); }
};
template <typename T> class ZuUnion_Ops_<T, 1, 0> {
public:
  static void ctor(void *p) {
    *static_cast<T *>(p) = ZuCmp<T>::null();
  }
  template <typename V>
  static void ctor(void *p, V &&v) {
    *static_cast<T *>(p) = ZuFwd<V>(v);
  }
  static void dtor(void *p) { }
  static bool star(const void *p) {
    return !ZuCmp<T>::null(*static_cast<const T *>(p));
  }
  static bool bang(const void *p) {
    return !(*static_cast<const T *>(p));
  }
};
template <typename T> class ZuUnion_Ops_<T, 1, 1> {
public:
  static void ctor(void *p) {
    *static_cast<T *>(p) = nullptr;
  }
  template <typename V>
  static void ctor(void *p, V &&v) {
    *static_cast<T *>(p) = ZuFwd<V>(v);
  }
  static void dtor(void *p) { }
  static bool star(const void *p) {
    return (bool)(*static_cast<const T *>(p));
  }
  static bool bang(const void *p) {
    return !(*static_cast<const T *>(p));
  }
};
template <typename T>
struct ZuUnion_Ops :
    public ZuUnion_Ops_<T,
      ZuTraits<T>::IsPrimitive,
      ZuTraits<T>::IsPrimitive && ZuTraits<T>::IsPointer> {
  template <typename V> static void assign(void *p, V &&v)
    { *static_cast<T *>(p) = ZuFwd<V>(v); }
  template <typename V> static bool less(const void *p, const V &v)
    { return ZuCmp<T>::less(*static_cast<const T *>(p), v); }
  template <typename V> static bool equals(const void *p, const V &v)
    { return ZuCmp<T>::equals(*static_cast<const T *>(p), v); }
  template <typename V> static int cmp(const void *p, const V &v)
    { return ZuCmp<T>::cmp(*static_cast<const T *>(p), v); }
  static uint32_t hash(const void *p)
    { return ZuHash<T>::hash(*static_cast<const T *>(p)); }
};

template <typename Base, typename ...Args> struct ZuUnion_Traits;
template <typename Base, typename Arg0>
struct ZuUnion_Traits<Base, Arg0> : public Base {
  enum { IsPOD = ZuTraits<Arg0>::IsPOD };
};
template <typename Base, typename Arg0, typename ...Args>
struct ZuUnion_Traits<Base, Arg0, Args...> : public Base {
private:
  using Left = ZuTraits<Arg0>;
  using Right = ZuUnion_Traits<Base, Args...>;
public:
  enum { IsPOD = Left::IsPOD && Right::IsPOD };
};

namespace Zu_ {
template <typename ...Args> class Union {
public:
  using Largest = ZuLargest<Args...>;
  enum { Size = sizeof(Largest) };
  enum { N = sizeof...(Args) };
  template <unsigned I> using Type = ZuType<I, Args...>;
  template <unsigned I> using Type_ = ZuDecay<Type<I>>;
  template <typename T> using Index = ZuTypeIndex<T, Args...>;
  template <typename T> using Index_ =
    ZuTypeIndex<T, ZuTypeMap<ZuDecay, Args...>>;

  Union() {
    using T0 = Type<0>;
    type_(0);
    new (&m_u[0]) T0{};
  }

  ~Union() {
    ZuSwitch::dispatch<N>(type(), [this](auto i) {
      using T = Type<i>;
      ZuUnion_Ops<T>::dtor(m_u);
    });
  }

  Union(const Union &u) {
    ZuSwitch::dispatch<N>(type_(u.type()), [this, &u](auto i) {
      using T = Type<i>;
      const T *ZuMayAlias(ptr) = reinterpret_cast<const T *>(u.m_u);
      ZuUnion_Ops<T>::ctor(m_u, *ptr);
    });
  }
  Union(Union &&u) {
    ZuSwitch::dispatch<N>(type_(u.type()), [this, &u](auto i) {
      using T = Type<i>;
      T *ZuMayAlias(ptr) = reinterpret_cast<T *>(u.m_u);
      ZuUnion_Ops<T>::ctor(m_u, ZuMv(*ptr));
    });
  }

  Union &operator =(const Union &u) {
    if (this == &u) return *this;
    if (type() != u.type()) {
      this->~Union();
      new (this) Union(u);
      return *this;
    }
    ZuSwitch::dispatch<N>(type_(u.type()), [this, &u](auto i) {
      using T = Type<i>;
      const T *ZuMayAlias(ptr) = reinterpret_cast<const T *>(u.m_u);
      ZuUnion_Ops<T>::assign(m_u, *ptr);
    });
    return *this;
  }
  Union &operator =(Union &&u) {
    if (type() != u.type()) {
      this->~Union();
      new (this) Union(ZuMv(u));
      return *this;
    }
    ZuSwitch::dispatch<N>(type_(u.type()), [this, &u](auto i) {
      using T = Type<i>;
      T *ZuMayAlias(ptr) = reinterpret_cast<T *>(u.m_u);
      ZuUnion_Ops<T>::assign(m_u, ZuMv(*ptr));
    });
    return *this;
  }

  template <typename T>
  T *new_() {
    this->type_(Index<T>::I);
    return reinterpret_cast<T *>(&m_u[0]);
  }
  template <typename T>
  static T *new_(void *ptr) {
    return reinterpret_cast<Union *>(ptr)->new_<T>();
  }

  template <typename T> T *init() {
    this->~Union();
    this->type_(Index<T>::I);
    return reinterpret_cast<T *>(&m_u[0]);
  }

private:
  template <typename V, bool = ZuConversion<Union, V>::Is> struct Fwd_Ctor_ {
    using T = Type<Index_<V>::I>;
    static void ctor(Union *this_, V &&v) {
      new (new_<T>(this_)) T{ZuMv(v)};
    }
    static void ctor(Union *this_, const V &v) {
      new (new_<T>(this_)) T{v};
    }
  };
  template <typename V> struct Fwd_Ctor_<V, true> {
    static void ctor(Union *this_, V &&v) {
      new (this_) Union{static_cast<Union &&>(v)};
    }
    static void ctor(Union *this_, const V &v) {
      new (this_) Union{static_cast<const Union &>(v)};
    }
  };
  template <typename V>
  struct Fwd_Ctor : public Fwd_Ctor_<ZuDecay<V>> { };

public:
  template <typename V> Union(V &&v) {
    Fwd_Ctor<V>::ctor(this, ZuFwd<V>(v));
  }

private:
  template <typename V, bool = ZuTraits<V>::IsPrimitive> struct Fwd_Assign__ {
    using T = Type<Index_<V>::I>;
    static void assign(Union *this_, V &&v) {
      new (this_->init<T>()) T{ZuMv(v)};
    }
    static void assign(Union *this_, const V &v) {
      new (this_->init<T>()) T{v};
    }
  };
  template <typename V> struct Fwd_Assign__<V, true> {
    using T = Type<Index_<V>::I>;
    static void assign(Union *this_, V v) {
      *(this_->init<T>()) = v;
    }
  };
  template <typename V, bool = ZuConversion<Union, V>::Is>
  struct Fwd_Assign_ : public Fwd_Assign__<V> { };
  template <typename V> struct Fwd_Assign_<V, true> {
    static void assign(Union *this_, V &&v) {
      this_->operator =(static_cast<Union &&>(v));
    }
    static void assign(Union *this_, const V &v) {
      this_->operator =(static_cast<const Union &>(v));
    }
  };
  template <typename V>
  struct Fwd_Assign : public Fwd_Assign_<ZuDecay<V>> { };

public:
  template <typename V> Union &operator =(V &&v) {
    Fwd_Assign<V>::assign(this, ZuFwd<V>(v));
    return *this;
  }

  unsigned type_(unsigned i) { return m_u[Size] = i; }

  void null() {
    ZuSwitch::dispatch<N>(type(), [this](auto i) {
      using T = Type<i>;
      ZuUnion_Ops<T>::dtor(m_u);
    });
    type_(0);
  }

  template <typename P>
  ZuIs<Union, P, bool> equals(const P &p) const {
    if (this == &p) return true;
    if (type() != p.type()) return false;
    return ZuSwitch::dispatch<N>(type(), [this, &p](auto i) -> bool {
      using T = Type<i>;
      const T *ZuMayAlias(ptr) = reinterpret_cast<const T *>(p.m_u);
      return ZuUnion_Ops<T>::equals(m_u, *ptr);
    });
  }
  template <typename P>
  ZuIsNot<Union, P, bool> equals(const P &p) const {
    return ZuSwitch::dispatch<N>(type(), [this, &p](auto i) -> bool {
      using T = Type<i>;
      return ZuUnion_Ops<T>::equals(m_u, p);
    });
  }
  template <typename P>
  ZuIs<Union, P, int> cmp(const P &p) const {
    if (this == &p) return true;
    if (int i = ZuCmp<uint8_t>::cmp(type(), p.type())) return i;
    return ZuSwitch::dispatch<N>(type(), [this, &p](auto i) -> int {
      using T = Type<i>;
      const T *ZuMayAlias(ptr) = reinterpret_cast<const T *>(p.m_u);
      return ZuUnion_Ops<T>::cmp(m_u, *ptr);
    });
  }
  template <typename P>
  ZuIsNot<Union, P, int> cmp(const P &p) const {
    return ZuSwitch::dispatch<N>(type(), [this, &p](auto i) -> int {
      using T = Type<i>;
      return ZuUnion_Ops<T>::cmp(m_u, p);
    });
  }
  template <typename L, typename R>
  friend inline ZuIfT<ZuConversion<Union, L>::Is, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline ZuIfT<ZuConversion<Union, L>::Is, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

  bool operator *() const {
    return ZuSwitch::dispatch<N>(type(), [this](auto i) -> bool {
      using T = Type<i>;
      return ZuUnion_Ops<T>::star(m_u);
    });
  }

  bool operator !() const {
    return ZuSwitch::dispatch<N>(type(), [this](auto i) -> bool {
      using T = Type<i>;
      return ZuUnion_Ops<T>::bang(m_u);
    });
  }

  uint32_t hash() const {
    return ZuSwitch::dispatch<N>(type(), [this](auto i) -> uint32_t {
      using T = Type<i>;
      return ZuHash<uint8_t>::hash(i) ^ ZuUnion_Ops<T>::hash(m_u);
    });
  }

  unsigned type() const { return m_u[Size]; }

  template <typename T>
  bool contains() const { return type() == Index<T>::I; }

  template <unsigned I>
  const Type_<I> &p() const & {
    using T = Type<I>;
    if (type() != I) return ZuNullRef<T>();
    const T *ZuMayAlias(ptr) = reinterpret_cast<const T *>(m_u);
    return *ptr;
  }
  template <unsigned I>
  Type_<I> &p() & {
    using T = Type<I>;
    T *ZuMayAlias(ptr) = reinterpret_cast<T *>(m_u);
    if (ZuUnlikely(type() != I)) {
      this->~Union();
      type_(I);
      ZuUnion_Ops<T>::ctor(m_u);
    }
    return *ptr;
  }
  template <unsigned I>
  Type_<I> &&p() && {
    using T = Type<I>;
    T *ZuMayAlias(ptr) = reinterpret_cast<T *>(m_u);
    if (ZuUnlikely(type() != I)) {
      this->~Union();
      type_(I);
      ZuUnion_Ops<T>::ctor(m_u);
    }
    return ZuMv(*ptr);
  }
  template <unsigned I, typename P>
  Union &p(P &&p) {
    using T = Type<I>;
    if (type() == I) {
      T *ZuMayAlias(ptr) = reinterpret_cast<T *>(m_u);
      *ptr = ZuFwd<P>(p);
      return *this;
    }
    this->~Union();
    type_(I);
    ZuUnion_Ops<T>::ctor(m_u, ZuFwd<P>(p));
    return *this;
  }

  template <typename T>
  const Type_<Index<T>::I> &v() const {
    return this->template p<Index<T>::I>();
  }
  template <typename T, typename P>
  Union &v(P &&p) {
    return this->template p<Index<T>::I>(ZuFwd<P>(p));
  }
  template <typename T>
  Type_<Index<T>::I> &v() {
    return this->template p<Index<T>::I>();
  }

  template <unsigned I>
  const Type<I> *ptr() const {
    if (type() != I) return nullptr;
    return ptr_<I>();
  }
  template <unsigned I>
  Type<I> *ptr() {
    if (type() != I) return nullptr;
    return ptr_<I>();
  }
  template <unsigned I>
  const Type<I> *ptr_() const {
    using T = Type<I>;
    const T *ZuMayAlias(ptr) = reinterpret_cast<const T *>(m_u);
    return ptr;
  }
  template <unsigned I>
  Type<I> *ptr_() {
    using T = Type<I>;
    T *ZuMayAlias(ptr) = reinterpret_cast<T *>(m_u);
    return ptr;
  }

  template <typename L>
  auto dispatch(L l) {
    return ZuSwitch::dispatch<N>(
	type(), [this, &l](auto i) mutable {
	  return l(this->template p<i>());
	});
  }
  template <typename L>
  auto cdispatch(L l) const {
    return ZuSwitch::dispatch<N>(
	type(), [this, &l](auto i) mutable {
	  return l(this->template p<i>());
	});
  }

  // traits
  using Traits = ZuUnion_Traits<ZuBaseTraits<Union>, Args...>;
  friend Traits ZuTraitsType(Union *);

private:
  uint8_t	m_u[Size + 1];
};

} // namespace Zu_
template <typename ...Args> using ZuUnion = Zu_::Union<Args...>;

// STL variant cruft
#include <type_traits>
namespace std {

template <class> struct tuple_size;
template <typename ...Args>
struct tuple_size<ZuUnion<Args...>> :
public integral_constant<size_t, sizeof...(Args)> { };

template <size_t, typename> struct tuple_element;
template <size_t I, typename ...Args>
struct tuple_element<I, ZuUnion<Args...>> {
  using type = typename ZuUnion<Args...>::template Type<I>;
};

} // std

#include <variant>

namespace Zu_ {

using size_t = std::size_t;
using bad_variant_access = std::bad_variant_access;
namespace {
  template <size_t I, typename T>
  using tuple_element_t = typename std::tuple_element<I, T>::type;
}
template <size_t I, typename ...Args>
constexpr tuple_element_t<I, Union<Args...>> &
get(Union<Args...> &p) {
  if (ZuUnlikely(p.type() != I)) throw bad_variant_access{};
  return p.template p<I>();
}
template <size_t I, typename ...Args>
constexpr const tuple_element_t<I, Union<Args...>> &
get(const Union<Args...> &p) {
  if (ZuUnlikely(p.type() != I)) throw bad_variant_access{};
  return p.template p<I>();
}
template <size_t I, typename ...Args>
constexpr tuple_element_t<I, Union<Args...>> &&
get(Union<Args...> &&p) {
  if (ZuUnlikely(p.type() != I)) throw bad_variant_access{};
  return static_cast<tuple_element_t<I, Union<Args...>> &&>(
      p.template p<I>());
}
template <size_t I, typename ...Args>
constexpr const tuple_element_t<I, Union<Args...>> &&
get(const Union<Args...> &&p) {
  if (ZuUnlikely(p.type() != I)) throw bad_variant_access{};
  return static_cast<const tuple_element_t<I, Union<Args...>> &&>(
      p.template p<I>());
}

template <typename T, typename ...Args>
constexpr T &get(Union<Args...> &p) {
  if (ZuUnlikely(p.type() != Union<Args...>::template Index<T>::I))
    throw bad_variant_access{};
  return p.template v<T>();
}
template <typename T, typename ...Args>
constexpr const T &get(const Union<Args...> &p) {
  if (ZuUnlikely(p.type() != Union<Args...>::template Index<T>::I))
    throw bad_variant_access{};
  return p.template v<T>();
}
template <typename T, typename ...Args>
constexpr T &&get(Union<Args...> &&p) {
  if (ZuUnlikely(p.type() != Union<Args...>::template Index<T>::I))
    throw bad_variant_access{};
  return static_cast<T &&>(p.template v<T>());
}
template <typename T, typename ...Args>
constexpr const T &&get(const Union<Args...> &&p) {
  if (ZuUnlikely(p.type() != Union<Args...>::template Index<T>::I))
    throw bad_variant_access{};
  return static_cast<const T &&>(p.template v<T>());
}

} // namespace Zu_

#define ZuUnion_FieldType(args) \
  ZuPP_Defer(ZuUnion_FieldType_)()(ZuPP_Strip(args))
#define ZuUnion_FieldType_() ZuUnion_FieldType__
#define ZuUnion_FieldType__(type, fn) ZuPP_Strip(type)

#define ZuUnion_FieldFn(N, args) \
  ZuPP_Defer(ZuUnion_FieldFn_)()(N, ZuPP_Strip(args))
#define ZuUnion_FieldFn_() ZuUnion_FieldFn__
#define ZuUnion_FieldFn__(N, type, fn) \
  const auto &fn() const & { return this->template p<N>(); } \
  auto &fn() & { return this->template p<N>(); } \
  auto &&fn() && { return ZuMv(this->template p<N>()); } \
  template <typename P> \
  auto &fn(P &&v) { this->template p<N>(ZuFwd<P>(v)); return *this; } \
  auto ptr_##fn() { return this->template ptr<N>(); } \
  auto init_##fn() { return this->template init<ZuPP_Strip(type)>(); }

#define ZuDeclUnion(Type, ...) \
using Type##_ = \
  ZuUnion<ZuPP_Eval(ZuPP_MapComma(ZuUnion_FieldType, __VA_ARGS__))>; \
class Type : public Type##_ { \
  using Union = Type##_; \
public: \
  using Union::Union; \
  using Union::operator =; \
  Type(const Union &v) : Union(v) { }; \
  Type(Union &&v) : Union(ZuMv(v)) { }; \
  ZuPP_Eval(ZuPP_MapIndex(ZuUnion_FieldFn, 0, __VA_ARGS__)) \
  struct Traits : public ZuTraits<Type##_> { using T = Type; }; \
  friend Traits ZuTraitsType(Type *); \
}

#endif /* ZuUnion_HPP */
