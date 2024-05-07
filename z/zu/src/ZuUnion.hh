//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// generic discriminated union; different design trade-offs than std::variant
// * supports void, primitive and pointer types in addition to composite types
// * doesn't throw exceptions
// * relies on the caller to prevent undefined behavior by checking the type
//   before using it (asserts in debug builds)
// * p<I>() - positional accessor
// * p<I>(v) - positional set function
// * p<T>() - access by type
// * type() - return index of current type
// * Types - type list (ZuTypeList)
// * dispatch(lambda) - dispatcher
// * cdispatch(lambda) - const dispatcher
// * operator <=>() - comparison

// using U = ZuUnion<int, double>;
// U u, v;
// *(u.init<int>()) = 42;
// u.p<0>(42);
// u.p<0>() = 42;
// v = u;
// if (v.type() == 0) { printf("%d\n", v.p<0>()); }
// v.p<1>(42.0);
// if (v.is<double>()) { printf("%g\n", v.p<double>()); }
// u.~U();
// *reinterpret_cast<int *>(&u) = 43; // *(u.ptr_<Index<int>::I>()) = ...
// u.type_(U::Index<int>::I);
// printf("%d\n", u.p<int>());
//
// namespace {
//   void print(int i) const { printf("%d\n", i); }
//   void print(double d) const { printf("%g\n", d); }
// };
// u.cdispatch([]<typename I>(I &&i) { print(ZuFwd<I>(i)); });
// ZuSwitch::dispatch<U::N>(u.type(), [&u](auto I) { print(u.p<I>()); });

#ifndef ZuUnion_HH
#define ZuUnion_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuTraits.hh>
#include <zlib/ZuLargest.hh>
#include <zlib/ZuNull.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuHash.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuSwitch.hh>
#include <zlib/ZuPP.hh>

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
    return static_cast<bool>(*static_cast<const T *>(p));
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
  template <typename V> static void assign(void *p, V &&v) {
    *static_cast<T *>(p) = ZuFwd<V>(v);
  }
  template <typename V> static bool less(const void *p, const V &v) {
    return ZuCmp<T>::less(*static_cast<const T *>(p), v);
  }
  template <typename V> static bool equals(const void *p, const V &v) {
    return ZuCmp<T>::equals(*static_cast<const T *>(p), v);
  }
  template <typename V> static int cmp(const void *p, const V &v) {
    return ZuCmp<T>::cmp(*static_cast<const T *>(p), v);
  }
  static uint32_t hash(const void *p) {
    return ZuHash<T>::hash(*static_cast<const T *>(p));
  }
};

template <typename Base, typename ...Ts> struct ZuUnion_Traits;
template <typename Base, typename T0>
struct ZuUnion_Traits<Base, T0> : public Base {
  enum { IsPOD = ZuTraits<T0>::IsPOD };
};
template <typename Base, typename T0, typename ...Ts>
struct ZuUnion_Traits<Base, T0, Ts...> : public Base {
private:
  using Left = ZuTraits<T0>;
  using Right = ZuUnion_Traits<Base, Ts...>;
public:
  enum { IsPOD = Left::IsPOD && Right::IsPOD };
};

template <typename T> struct ZuUnion_IsVoid_ : public ZuFalse { };
template <> struct ZuUnion_IsVoid_<void> : public ZuTrue { };
template <typename T> using ZuUnion_IsVoid = ZuUnion_IsVoid_<ZuDecay<T>>;

namespace Zu_ {

template <typename ...Ts> class Union;

// recursive decay
struct Union_RDecayer {
  template <typename> struct Decay;
  template <typename ...Ts>
  struct Decay<Union<Ts...>> {
    using T = ZuTypeApply<Union, ZuTypeMap<ZuRDecay, Ts...>>;
  };
};

template <typename ...Ts> class Union {
public:
  using Types = ZuTypeList<Ts...>;
  using Largest = ZuLargest<Ts...>;
  enum { Size = ZuSize<Largest>{} };
  enum { N = sizeof...(Ts) };
  template <unsigned I> using Type = ZuType<I, Types>;
  template <unsigned I> using Type_ = ZuDecay<Type<I>>;
  template <typename T> using Index = ZuTypeIndex<T, Types>;
  template <typename T>
  using Index_ = ZuTypeIndex<T, ZuTypeMap<ZuDecay, Types>>;

  Union() {
    using T0 = Type<0>;
    type_(0);
    if constexpr (!ZuUnion_IsVoid<T0>{}) new (&m_u[0]) T0{};
  }

  ~Union() {
    ZuSwitch::dispatch<N>(type(), [this](auto I) {
      using T = Type<I>;
      if constexpr (!ZuUnion_IsVoid<T>{}) ZuUnion_Ops<T>::dtor(m_u);
    });
  }

  Union(const Union &u) {
    ZuSwitch::dispatch<N>(type_(u.type()), [this, &u](auto I) {
      using T = Type<I>;
      if constexpr (!ZuUnion_IsVoid<T>{}) {
	const T *ZuMayAlias(ptr) = reinterpret_cast<const T *>(u.m_u);
	ZuUnion_Ops<T>::ctor(m_u, *ptr);
      }
    });
  }
  Union(Union &&u) {
    ZuSwitch::dispatch<N>(type_(u.type()), [this, &u](auto I) {
      using T = Type<I>;
      if constexpr (!ZuUnion_IsVoid<T>{}) {
	T *ZuMayAlias(ptr) = reinterpret_cast<T *>(u.m_u);
	ZuUnion_Ops<T>::ctor(m_u, ZuMv(*ptr));
      }
    });
  }

  Union &operator =(const Union &u) {
    if (this == &u) return *this;
    if (type() != u.type()) {
      this->~Union();
      new (this) Union(u);
      return *this;
    }
    ZuSwitch::dispatch<N>(type_(u.type()), [this, &u](auto I) {
      using T = Type<I>;
      if constexpr (!ZuUnion_IsVoid<T>{}) {
	const T *ZuMayAlias(ptr) = reinterpret_cast<const T *>(u.m_u);
	ZuUnion_Ops<T>::assign(m_u, *ptr);
      }
    });
    return *this;
  }
  Union &operator =(Union &&u) {
    if (type() != u.type()) {
      this->~Union();
      new (this) Union(ZuMv(u));
      return *this;
    }
    ZuSwitch::dispatch<N>(type_(u.type()), [this, &u](auto I) {
      using T = Type<I>;
      if constexpr (!ZuUnion_IsVoid<T>{}) {
	T *ZuMayAlias(ptr) = reinterpret_cast<T *>(u.m_u);
	ZuUnion_Ops<T>::assign(m_u, ZuMv(*ptr));
      }
    });
    return *this;
  }

  template <unsigned I, bool New = false> // New true elides destructor
  Type<I> *new_() {
    if constexpr (!New) this->~Union();
    this->type_(I);
    return reinterpret_cast<Type<I> *>(&m_u[0]);
  }
  template <unsigned I, bool New = false>
  static Type<I> *new_(void *ptr) {
    return reinterpret_cast<Union *>(ptr)->new_<I, New>();
  }
  template <typename T, bool New = false>
  Type<Index<T>{}> *new_() { return new_<Index<T>{}, New>(); }
  template <typename T, bool New = false>
  static Type<Index<T>{}> *new_(void *ptr) {
    return reinterpret_cast<Union *>(ptr)->new_<Index<T>{}, New>();
  }

private:
  template <typename V, bool = ZuInspect<Union, V>::Is> struct Fwd_Ctor_ {
    enum { I = Index_<V>{} };
    using T = Type<I>;
    static void ctor(Union *this_, V &&v) {
      new (new_<I, true>(this_)) T{ZuMv(v)};
    }
    static void ctor(Union *this_, const V &v) {
      new (new_<I, true>(this_)) T{v};
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
  template <typename V> Union(V &&v, ZuIfT<!ZuUnion_IsVoid<V>{}> *_ = nullptr) {
    Fwd_Ctor<V>::ctor(this, ZuFwd<V>(v));
  }

private:
  template <typename V, bool = ZuTraits<V>::IsPrimitive> struct Fwd_Assign__ {
    enum { I = Index_<V>{} };
    using T = Type<I>;
    static void assign(Union *this_, V &&v) {
      new (this_->new_<I>()) T{ZuMv(v)};
    }
    static void assign(Union *this_, const V &v) {
      new (this_->new_<I>()) T{v};
    }
  };
  template <typename V> struct Fwd_Assign__<V, true> {
    enum { I = Index_<V>{} };
    static void assign(Union *this_, V v) {
      *(this_->new_<I>()) = v;
    }
  };
  template <typename V, bool = ZuInspect<Union, V>::Is>
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
  template <typename V>
  ZuIfT<!ZuUnion_IsVoid<V>{}, Union &> operator =(V &&v) {
    Fwd_Assign<V>::assign(this, ZuFwd<V>(v));
    return *this;
  }

  unsigned type_(unsigned i) { return m_u[Size] = i; }

  void null() {
    ZuSwitch::dispatch<N>(type(), [this](auto I) {
      using T = Type<I>;
      if constexpr (!ZuUnion_IsVoid<T>{}) ZuUnion_Ops<T>::dtor(m_u);
    });
    type_(0);
  }

  template <typename P>
  ZuIs<Union, P, bool> equals(const P &p) const {
    if (this == &p) return true;
    if (type() != p.type()) return false;
    return ZuSwitch::dispatch<N>(type(), [this, &p](auto I) -> bool {
      using T = Type<I>;
      if constexpr (!ZuUnion_IsVoid<T>{}) {
	const T *ZuMayAlias(ptr) = reinterpret_cast<const T *>(p.m_u);
	return ZuUnion_Ops<T>::equals(m_u, *ptr);
      } else
	return true;
    });
  }
  template <typename P>
  ZuIsNot<Union, P, bool> equals(const P &p) const {
    return ZuSwitch::dispatch<N>(type(), [this, &p](auto I) -> bool {
      using T = Type<I>;
      if constexpr (!ZuUnion_IsVoid<T>{})
	return ZuUnion_Ops<T>::equals(m_u, p);
      else
	return false;
    });
  }
  template <typename P>
  ZuIs<Union, P, int> cmp(const P &p) const {
    if (this == &p) return 0;
    if (int i = ZuCmp<uint8_t>::cmp(type(), p.type())) return i;
    return ZuSwitch::dispatch<N>(type(), [this, &p](auto I) -> int {
      using T = Type<I>;
      if constexpr (!ZuUnion_IsVoid<T>{}) {
	const T *ZuMayAlias(ptr) = reinterpret_cast<const T *>(p.m_u);
	return ZuUnion_Ops<T>::cmp(m_u, *ptr);
      } else
	return 0;
    });
  }
  template <typename P>
  ZuIsNot<Union, P, int> cmp(const P &p) const {
    return ZuSwitch::dispatch<N>(type(), [this, &p](auto I) -> int {
      using T = Type<I>;
      if constexpr (!ZuUnion_IsVoid<T>{})
	return ZuUnion_Ops<T>::cmp(m_u, p);
      else
	return -1;
    });
  }
  template <typename L, typename R>
  friend inline ZuIfT<ZuInspect<Union, L>::Is, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline ZuIfT<ZuInspect<Union, L>::Is, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

  bool operator *() const {
    return ZuSwitch::dispatch<N>(type(), [this](auto I) -> bool {
      using T = Type<I>;
      if constexpr (!ZuUnion_IsVoid<T>{})
	return ZuUnion_Ops<T>::star(m_u);
      else
	return false;
    });
  }

  bool operator !() const {
    return ZuSwitch::dispatch<N>(type(), [this](auto I) -> bool {
      using T = Type<I>;
      if constexpr (!ZuUnion_IsVoid<T>{})
	return ZuUnion_Ops<T>::bang(m_u);
      else
	return true;
    });
  }

  uint32_t hash() const {
    return ZuSwitch::dispatch<N>(type(), [this](auto I) -> uint32_t {
      using T = Type<I>;
      if constexpr (!ZuUnion_IsVoid<T>{})
	return ZuHash<uint8_t>::hash(I) ^ ZuUnion_Ops<T>::hash(m_u);
      else
	return ZuHash<uint8_t>::hash(I);
    });
  }

  unsigned type() const { return m_u[Size]; }

  template <typename T>
  bool is() const { return type() == Index<T>{}; }

  template <unsigned I>
  ZuIfT<!ZuUnion_IsVoid<Type_<I>>{}, const Type_<I> &> p() const & {
    using T = Type<I>;
    assert(type() == I);
    const T *ZuMayAlias(ptr) = reinterpret_cast<const T *>(m_u);
    return *ptr;
  }
  template <unsigned I>
  ZuIfT<!ZuUnion_IsVoid<Type_<I>>{}, Type_<I> &> p() & {
    using T = Type<I>;
    assert(type() == I);
    T *ZuMayAlias(ptr) = reinterpret_cast<T *>(m_u);
    return *ptr;
  }
  template <unsigned I>
  ZuIfT<!ZuUnion_IsVoid<Type_<I>>{}, Type_<I> &&> p() && {
    using T = Type<I>;
    assert(type() == I);
    T *ZuMayAlias(ptr) = reinterpret_cast<T *>(m_u);
    return ZuMv(*ptr);
  }
  template <unsigned I, typename P>
  ZuIfT<!ZuUnion_IsVoid<Type_<I>>{}, Union &> p(P &&p) {
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
  ZuIfT<!ZuUnion_IsVoid<T>{}, const ZuDecay<T> &> p() const & {
    return this->template p<Index<T>{}>();
  }
  template <typename T>
  ZuIfT<!ZuUnion_IsVoid<T>{}, ZuDecay<T> &> p() & {
    return this->template p<Index<T>{}>();
  }
  template <typename T>
  ZuIfT<!ZuUnion_IsVoid<T>{}, ZuDecay<T> &&> p() && {
    return ZuMv(*this).template p<Index<T>{}>();
  }
  template <typename T, typename P>
  ZuIfT<!ZuUnion_IsVoid<T>{}, Union &> p(P &&p) {
    return this->template p<Index<T>{}>(ZuFwd<P>(p));
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

  template <typename T> const auto *ptr() const { return ptr<Index<T>{}>(); }
  template <typename T> auto *ptr() { return ptr<Index<T>{}>(); }
  template <typename T> const auto *ptr_() const {return ptr_<Index<T>{}>(); }
  template <typename T> auto *ptr_() { return ptr_<Index<T>{}>(); }

  template <typename L>
  auto dispatch(L l) & {
    return ZuSwitch::dispatch<N>(type(), [this, &l](auto I) mutable {
      if constexpr (!ZuUnion_IsVoid<Type<I>>{})
	return l(I, this->template p<I>());
    });
  }
  template <typename L>
  auto dispatch(L l) const & {
    return ZuSwitch::dispatch<N>(type(), [this, &l](auto I) mutable {
      if constexpr (!ZuUnion_IsVoid<Type<I>>{})
	return l(I, this->template p<I>());
    });
  }
  template <typename L>
  auto dispatch(L l) && {
    return ZuSwitch::dispatch<N>(type(), [this, &l](auto I) mutable {
      if constexpr (!ZuUnion_IsVoid<Type<I>>{})
	return l(I, ZuMv(*this).template p<I>());
    });
  }
  template <typename L>
  auto cdispatch(L l) const & { return dispatch(ZuMv(l)); }

  // traits
  using Traits = ZuUnion_Traits<ZuBaseTraits<Union>, Ts...>;
  friend Traits ZuTraitsType(Union *) { return {}; } // unused

  // recursive decay
  friend Union_RDecayer ZuRDecayer(Union *);

private:
  uint8_t	m_u[Size + 1];
};

} // namespace Zu_

template <typename ...Ts> using ZuUnion = Zu_::Union<Ts...>;

// STL variant cruft
#include <type_traits>
namespace std {

template <class> struct tuple_size;
template <typename ...Ts>
struct tuple_size<ZuUnion<Ts...>> :
public integral_constant<size_t, sizeof...(Ts)> { };

template <size_t, typename> struct tuple_element;
template <size_t I, typename ...Ts>
struct tuple_element<I, ZuUnion<Ts...>> {
  using type = typename ZuUnion<Ts...>::template Type<I>;
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
template <size_t I, typename ...Ts>
constexpr tuple_element_t<I, Union<Ts...>> &
get(Union<Ts...> &p) {
  if (ZuUnlikely(p.type() != I)) throw bad_variant_access{};
  return p.template p<I>();
}
template <size_t I, typename ...Ts>
constexpr const tuple_element_t<I, Union<Ts...>> &
get(const Union<Ts...> &p) {
  if (ZuUnlikely(p.type() != I)) throw bad_variant_access{};
  return p.template p<I>();
}
template <size_t I, typename ...Ts>
constexpr tuple_element_t<I, Union<Ts...>> &&
get(Union<Ts...> &&p) {
  if (ZuUnlikely(p.type() != I)) throw bad_variant_access{};
  return static_cast<tuple_element_t<I, Union<Ts...>> &&>(
      p.template p<I>());
}
template <size_t I, typename ...Ts>
constexpr const tuple_element_t<I, Union<Ts...>> &&
get(const Union<Ts...> &&p) {
  if (ZuUnlikely(p.type() != I)) throw bad_variant_access{};
  return static_cast<const tuple_element_t<I, Union<Ts...>> &&>(
      p.template p<I>());
}

template <typename T, typename ...Ts>
constexpr T &get(Union<Ts...> &p) {
  if (ZuUnlikely(p.type() != typename Union<Ts...>::template Index<T>{}))
    throw bad_variant_access{};
  return p.template p<T>();
}
template <typename T, typename ...Ts>
constexpr const T &get(const Union<Ts...> &p) {
  if (ZuUnlikely(p.type() != typename Union<Ts...>::template Index<T>{}))
    throw bad_variant_access{};
  return p.template p<T>();
}
template <typename T, typename ...Ts>
constexpr T &&get(Union<Ts...> &&p) {
  if (ZuUnlikely(p.type() != typename Union<Ts...>::template Index<T>{}))
    throw bad_variant_access{};
  return static_cast<T &&>(p.template p<T>());
}
template <typename T, typename ...Ts>
constexpr const T &&get(const Union<Ts...> &&p) {
  if (ZuUnlikely(p.type() != typename Union<Ts...>::template Index<T>{}))
    throw bad_variant_access{};
  return static_cast<const T &&>(p.template p<T>());
}

} // namespace Zu_

#define ZuUnion_FieldType(args) \
  ZuPP_Defer(ZuUnion_FieldType_)()(ZuPP_Strip(args))
#define ZuUnion_FieldType_() ZuUnion_FieldType__
#define ZuUnion_FieldType__(type, fn) ZuPP_Strip(type)

#define ZuUnion_FieldFn(N, args) \
  ZuPP_Defer(ZuUnion_FieldFn_)()(N, ZuPP_Strip(args))
#define ZuUnion_FieldFn_() ZuUnion_FieldFn__
#define ZuUnion_FieldFn__(I, type_, fn) \
  auto is_##fn() const { return this->type() == I; } \
  const auto &fn() const & { return this->template p<I>(); } \
  auto &fn() & { return this->template p<I>(); } \
  auto &&fn() && { return ZuMv(this->template p<I>()); } \
  template <typename P> \
  auto &fn(P &&v) { this->template p<I>(ZuFwd<P>(v)); return *this; } \
  auto ptr_##fn() { return this->template ptr<I>(); } \
  auto new_##fn() { return this->template new_<I>(); }

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

#endif /* ZuUnion_HH */
