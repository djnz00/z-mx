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

// generic tuple

#ifndef ZuTuple_HPP
#define ZuTuple_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuPrint.hpp>
#include <zlib/ZuPair.hpp>
#include <zlib/ZuNull.hpp>
#include <zlib/ZuSwitch.hpp>
#include <zlib/ZuPP.hpp>

namespace Zu_ {
  template <typename ...Args> class Tuple_;
}
template <typename ...Args> using ZuTuple = Zu_::Tuple_<Args...>;

struct ZuTuple1_ { }; // tuple containing single value

template <typename T, typename P, bool IsTuple0> struct ZuTuple1_Cvt_;
template <typename T, typename P> struct ZuTuple1_Cvt_<T, P, 0> {
  enum { OK = 0 };
};
template <typename T, typename P> struct ZuTuple1_Cvt_<T, P, 1> {
  enum {
    OK = ZuConversion<typename T::T0, typename P::T0>::Exists
  };
};
template <typename T, typename P> struct ZuTuple1_Cvt :
  public ZuTuple1_Cvt_<ZuDeref<T>, P,
    ZuConversion<ZuTuple1_, ZuDeref<T>>::Base> { };

template <unsigned, typename> struct ZuTuple_Type0_;
template <typename T0>
struct ZuTuple_Type0_<0, T0> { using T = T0; };
template <unsigned I, typename T0>
using ZuTuple_Type0 = typename ZuTuple_Type0_<I, T0>::T;

template <typename U0> struct ZuTuple1_Print_ {
  ZuTuple1_Print_() = delete;
  ZuTuple1_Print_(const ZuTuple1_Print_ &) = delete;
  ZuTuple1_Print_ &operator =(const ZuTuple1_Print_ &) = delete;
  ZuTuple1_Print_(ZuTuple1_Print_ &&) = delete;
  ZuTuple1_Print_ &operator =(ZuTuple1_Print_ &&) = delete;
  ZuTuple1_Print_(const U0 &p0_, const ZuString &delim_) :
      p0{p0_}, delim{delim_} { }
  const U0		&p0;
  const ZuString	&delim;
};
template <typename U0, bool Nested>
struct ZuTuple1_Print;
template <typename U0>
struct ZuTuple1_Print<U0, false> :
  public ZuTuple1_Print_<U0>, public ZuPrintable {
  ZuTuple1_Print(const U0 &p0, const ZuString &delim) :
      ZuTuple1_Print_<U0>{p0, delim} { }
  template <typename S> void print(S &s) const {
    s << this->p0;
  }
};
template <typename U0>
struct ZuTuple1_Print<U0, true> :
  public ZuTuple1_Print_<U0>, public ZuPrintable {
  ZuTuple1_Print(const U0 &p0, const ZuString &delim) :
      ZuTuple1_Print_<U0>{p0, delim} { }
  template <typename S> void print(S &s) const {
    s << this->p0.print(this->delim);
  }
};

namespace Zu_ {
template <typename T0_>
class Tuple_<T0_> : public ZuTuple1_ {
public:
  using T0 = T0_;
  using U0 = ZuDeref<T0>;
  using Types = ZuTypeList<T0>;
  template <unsigned I> using Type = ZuTuple_Type0<I, T0>;
  enum { N = 1 };

  template <typename T>
  using Index = ZuTypeIndex<T, T0>;

  Tuple_() = default;
  Tuple_(const Tuple_ &) = default;
  Tuple_ &operator =(const Tuple_ &) = default;
  Tuple_(Tuple_ &&) = default;
  Tuple_ &operator =(Tuple_ &&) = default;
  ~Tuple_() = default;

private:
  template <typename T, typename> struct Bind_P0 {
    using U0 = typename T::U0;
    static const U0 &p0(const T &v) { return v.m_p0; }
    static U0 &p0(T &v) { return v.m_p0; }
    static U0 &&p0(T &&v) { return ZuMv(v.m_p0); }
  };
  template <typename T, typename P0> struct Bind_P0<T, P0 &> {
    using U0 = typename T::U0;
    static U0 &p0(T &v) { return v.m_p0; }
  };
  template <typename T, typename P0> struct Bind_P0<T, const P0 &> {
    using U0 = typename T::U0;
    static const U0 &p0(const T &v) { return v.m_p0; }
  };
  template <typename T, typename P0> struct Bind_P0<T, volatile P0 &> {
    using U0 = typename T::U0;
    static volatile U0 &p0(volatile T &v) { return v.m_p0; }
  };
  template <typename T, typename P0> struct Bind_P0<T, const volatile P0 &> {
    using U0 = typename T::U0;
    static const volatile U0 &p0(const volatile T &v) {
      return v.m_p0;
    }
  };
  template <typename T>
  struct Bind : public Bind_P0<T, T0> { };

public:
  template <typename T>
  Tuple_(T &&v, ZuIfT<
	ZuTuple1_Cvt<ZuDecay<T>, Tuple_>::OK
      > *_ = nullptr) :
    m_p0{Bind<ZuDecay<T>>::p0(ZuFwd<T>(v))} { }

  template <typename T>
  Tuple_(T &&v, ZuIfT<
      !ZuTuple1_Cvt<ZuDecay<T>, Tuple_>::OK &&
	(!ZuTraits<T0>::IsReference ||
	  ZuConversion<ZuStrip<U0>, ZuDecay<T>>::Is)
      > *_ = nullptr) :
    m_p0{ZuFwd<T>(v)} { }

  template <typename T>
  ZuIfT<ZuTuple1_Cvt<ZuDecay<T>, Tuple_>::OK, Tuple_ &>
  operator =(T &&v) {
    m_p0 = Bind<ZuDecay<T>>::p0(ZuFwd<T>(v));
    return *this;
  }

  template <typename T>
  ZuIfT<
    !ZuTuple1_Cvt<ZuDecay<T>, Tuple_>::OK &&
      (!ZuTraits<T0>::IsReference ||
	ZuConversion<ZuStrip<U0>, ZuDecay<T>>::Is),
    Tuple_ &>
  operator =(T &&v) {
    m_p0 = ZuFwd<T>(v);
    return *this;
  }

  template <typename P0>
  bool equals(const Tuple_<P0> &p) const {
    return ZuCmp<T0>::equals(m_p0, p.template p<0>());
  }
  template <typename T>
  ZuIfT<ZuConversion<ZuStrip<U0>, T>::Exists, bool>
  equals(const T &v) const {
    return ZuCmp<T0>::equals(m_p0, v);
  }
  template <typename P0>
  int cmp(const Tuple_<P0> &p) const {
    return ZuCmp<T0>::cmp(m_p0, p.template p<0>());
  }
  template <typename T>
  ZuIfT<ZuConversion<ZuStrip<U0>, T>::Exists, bool>
  cmp(const T &v) const {
    return ZuCmp<T0>::equals(m_p0, v);
  }
  template <typename L, typename R>
  friend inline ZuIfT<ZuConversion<Tuple_, L>::Is, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline ZuIfT<ZuConversion<Tuple_, L>::Is, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

  bool operator !() const { return !m_p0; }
  ZuOpBool

  uint32_t hash() const {
    return ZuHash<T0>::hash(m_p0);
  }

  template <unsigned I>
  ZuIfT<!I, const U0 &> p() const & {
    return m_p0;
  }
  template <unsigned I>
  ZuIfT<!I, U0 &> p() & {
    return m_p0;
  }
  template <unsigned I>
  ZuIfT<!I, U0 &&> p() && {
    return ZuMv(m_p0);
  }
  template <unsigned I, typename P>
  ZuIfT<!I, Tuple_ &> p(P &&p) {
    m_p0 = ZuFwd<P>(p);
    return *this;
  }

  template <typename T>
  ZuIfT<ZuConversion<T, T0>::Same, const U0 &> v() const & {
    return m_p0;
  }
  template <typename T>
  ZuIfT<ZuConversion<T, T0>::Same, U0 &> v() & {
    return m_p0;
  }
  template <typename T>
  ZuIfT<ZuConversion<T, T0>::Same, U0 &&> v() && {
    return ZuMv(m_p0);
  }
  template <typename T, typename P>
  ZuIfT<ZuConversion<T, T0>::Same, Tuple_ &> v(P &&p) {
    m_p0 = ZuFwd<P>(p);
    return *this;
  }

  using Print = ZuTuple1_Print<U0, ZuConversion<ZuPair_, U0>::Base>;
  Print print() const {
    return Print{m_p0, "|"};
  }
  Print print(const ZuString &delim) const {
    return Print{m_p0, delim};
  }

  template <typename L>
  auto dispatch(unsigned i, L l) ->
      ZuDecay<decltype(l(ZuDeclVal<T0 &>()))> {
    if (i) return {};
    return l(m_p0);
  }
  template <typename L>
  auto cdispatch(unsigned i, L l) ->
      ZuDecay<decltype(l(ZuDeclVal<const T0 &>()))> const {
    if (i) return {};
    return l(m_p0);
  }

  // traits
  struct Traits : public ZuBaseTraits<Tuple_> {
    enum { IsPOD = ZuTraits<T0>::IsPOD };
  };
  friend Traits ZuTraitsType(Tuple_ *);

private:
  T0		m_p0;
};

template <typename T0, typename T1>
class Tuple_<T0, T1> : public Pair_<T0, T1> {
  using Base = Pair_<T0, T1>;

public:
  template <unsigned I> using Type = typename Base::template Type<I>;
  using Types = ZuTypeList<T0, T1>;
  enum { N = 2 };

  template <typename T>
  using Index = ZuTypeIndex<T, T0, T1>;

  Tuple_() = default;
  Tuple_(const Tuple_ &) = default;
  Tuple_ &operator =(const Tuple_ &) = default;
  Tuple_(Tuple_ &&) = default;
  Tuple_ &operator =(Tuple_ &&) = default;
  ~Tuple_() = default;

  template <typename T> Tuple_(T &&v) : Base{ZuFwd<T>(v)} { }

  template <typename T> Tuple_ &operator =(T &&v) noexcept {
    Base::assign(ZuFwd<T>(v));
    return *this;
  }

  template <typename P0, typename P1>
  Tuple_(P0 &&p0, P1 &&p1) : Base{ZuFwd<P0>(p0), ZuFwd<P1>(p1)} { }

  template <unsigned I>
  const Type<I> &p() const {
    return Base::template p<I>();
  }
  template <unsigned I>
  Type<I> &p() {
    return Base::template p<I>();
  }
  template <unsigned I, typename P>
  Tuple_ &p(P &&p) {
    Base::template p<I>(ZuFwd<P>(p));
    return *this;
  }

  template <typename T>
  const Type<Index<T>::I> &v() const {
    return p<Index<T>::I>();
  }
  template <typename T>
  Type<Index<T>::I> &v() {
    return p<Index<T>::I>();
  }
  template <typename T, typename P>
  Tuple_ &v(P &&p) {
    return p<Index<T>::I>(ZuFwd<P>(p));
  }

  template <typename L>
  decltype(auto) dispatch(unsigned i, L l) {
    return ZuSwitch::dispatch<N>(
	i, [this, &l](auto i) mutable {
	  return l(this->p<i>());
	});
  }
  template <typename L>
  decltype(auto) cdispatch(unsigned i, L l) const {
    return ZuSwitch::dispatch<N>(
	i, [this, &l](auto i) mutable {
	  return l(this->p<i>());
	});
  }

  // traits
  friend ZuTraits<Base> ZuTraitsType(Tuple_ *);
};
} // namespace Zu_

template <unsigned I, typename Left, typename Right>
struct ZuTuple_Type_ {
  using T = typename Right::template Type<I - 1>;
};
template <typename Left, typename Right>
struct ZuTuple_Type_<0, Left, Right> {
  using T = Left;
};
template <unsigned I, typename Left, typename Right>
using ZuTuple_Type = typename ZuTuple_Type_<I, Left, Right>::T;

namespace Zu_ {
template <typename T0, typename T1, typename ...Args>
class Tuple_<T0, T1, Args...> : public Pair_<T0, Tuple_<T1, Args...>> {
  using Left = T0;
  using Right = Tuple_<T1, Args...>;
  using Base = Pair_<Left, Right>;

public:
  template <unsigned I> using Type = ZuTuple_Type<I, Left, Right>;
  using Types = ZuTypeList<T0, T1, Args...>;
  enum { N = Right::N + 1 };

  template <typename T>
  using Index = ZuTypeIndex<T, T0, T1, Args...>;

  Tuple_() = default;
  Tuple_(const Tuple_ &) = default;
  Tuple_ &operator =(const Tuple_ &) = default;
  Tuple_(Tuple_ &&) = default;
  Tuple_ &operator =(Tuple_ &&) = default;
  ~Tuple_() = default;

  template <typename T> Tuple_(T &&v) : Base{ZuFwd<T>(v)} { }

  template <typename T> Tuple_ &operator =(T &&v) noexcept {
    Base::assign(ZuFwd<T>(v));
    return *this;
  }

  template <typename P0, typename P1, typename ...Args_>
  Tuple_(P0 &&p0, P1 &&p1, Args_ &&... args) :
      Base{ZuFwd<P0>(p0), Right{ZuFwd<P1>(p1), ZuFwd<Args_>(args)...}} { }

  template <unsigned I>
  ZuIfT<!I, const Type<I> &> p() const & {
    return Base::template p<0>();
  }
  template <unsigned I>
  ZuIfT<!I, Type<I> &> p() & {
    return Base::template p<0>();
  }
  template <unsigned I>
  ZuIfT<!I, Type<I> &&> p() && {
    return ZuMv(ZuMv(*this).Base::template p<0>());
  }
  template <unsigned I, typename P>
  ZuIfT<!I, Tuple_ &> p(P &&p) {
    Base::template p<0>(ZuFwd<P>(p));
    return *this;
  }

  template <unsigned I>
  ZuIfT<(I && I < N), const Type<I> &> p() const & {
    return Base::template p<1>().template p<I - 1>();
  }
  template <unsigned I>
  ZuIfT<(I && I < N), Type<I> &> p() & {
    return Base::template p<1>().template p<I - 1>();
  }
  template <unsigned I>
  ZuIfT<(I && I < N), Type<I> &&> p() && {
    return ZuMv(ZuMv(ZuMv(*this).Base::template p<1>()).template p<I - 1>());
  }
  template <unsigned I, typename P>
  ZuIfT<(I && I < N), Tuple_ &> p(P &&p) {
    Base::template p<1>().template p<I - 1>(ZuFwd<P>(p));
    return *this;
  }

  template <typename T>
  decltype(auto) v() const & {
    return p<Index<T>::I>();
  }
  template <typename T>
  decltype(auto) v() & {
    return p<Index<T>::I>();
  }
  template <typename T>
  decltype(auto) v() && {
    return ZuMv(ZuMv(*this).template p<Index<T>::I>());
  }
  template <typename T, typename P>
  decltype(auto) v(P &&p) {
    return p<Index<T>::I>(ZuFwd<P>(p));
  }

  template <typename L>
  decltype(auto) dispatch(unsigned i, L l) {
    return ZuSwitch::dispatch<N>(
	i, [this, &l](auto i) mutable {
	  return l(this->template p<i>());
	});
  }
  template <typename L>
  decltype(auto) cdispatch(unsigned i, L l) const {
    return ZuSwitch::dispatch<N>(
	i, [this, &l](auto i) mutable {
	  return l(this->template p<i>());
	});
  }

  // traits
  friend ZuTraits<Base> ZuTraitsType(Tuple_ *);
};
} // namespace Zu_

template <typename ...Args>
auto inline ZuFwdTuple(Args &&... args) {
  return ZuTuple<Args &&...>{ZuFwd<Args>(args)...};
}

template <typename ...Args>
auto inline ZuMvTuple(Args... args) {
  return ZuTuple<Args...>{ZuMv(args)...};
}

namespace Zu_ {
  template <typename ...Args> Tuple_(Args...) -> Tuple_<Args...>;
}

// generic accessor
template <unsigned I = 0>
struct ZuTupleAxor {
  template <typename P, unsigned J> struct Bind {
    static decltype(auto) get(const P &v) { return v.template p<J>(); }
    static decltype(auto) get(P &v) { return v.template p<J>(); }
    static decltype(auto) get(P &&v) { return ZuMv(ZuMv(v).template p<J>()); }
  };
  template <typename P>
  static decltype(auto) get(P &&v) {
    return Bind<ZuDecay<P>, I>::get(ZuFwd<P>(v));
  }
};

// STL structured binding cruft
#include <type_traits>
namespace std {
  template <class> struct tuple_size;
  template <typename ...Args>
  struct tuple_size<ZuTuple<Args...>> :
  public integral_constant<size_t, sizeof...(Args)> { };

  template <size_t, typename> struct tuple_element;
  template <size_t I, typename ...Args>
  struct tuple_element<I, ZuTuple<Args...>> {
    using type = typename ZuTuple<Args...>::template Type<I>;
  };
}
namespace Zu_ {
  using size_t = std::size_t;
  namespace {
    template <size_t I, typename T>
    using tuple_element_t = typename std::tuple_element<I, T>::type;
  }
  template <size_t I, typename ...Args>
  constexpr tuple_element_t<I, Tuple_<Args...>> &
  get(Tuple_<Args...> &p) noexcept { return p.template p<I>(); }
  template <size_t I, typename ...Args>
  constexpr const tuple_element_t<I, Tuple_<Args...>> &
  get(const Tuple_<Args...> &p) noexcept { return p.template p<I>(); }
  template <size_t I, typename ...Args>
  constexpr tuple_element_t<I, Tuple_<Args...>> &&
  get(Tuple_<Args...> &&p) noexcept {
    return static_cast<tuple_element_t<I, Tuple_<Args...>> &&>(
	p.template p<I>());
  }
  template <size_t I, typename ...Args>
  constexpr const tuple_element_t<I, Tuple_<Args...>> &&
  get(const Tuple_<Args...> &&p) noexcept {
    return static_cast<const tuple_element_t<I, Tuple_<Args...>> &&>(
	p.template p<I>());
  }

  template <typename T, typename ...Args>
  constexpr T &get(Tuple_<Args...> &p) noexcept {
    return p.template v<T>();
  }
  template <typename T, typename ...Args>
  constexpr const T &get(const Tuple_<Args...> &p) noexcept {
    return p.template v<T>();
  }
  template <typename T, typename ...Args>
  constexpr T &&get(Tuple_<Args...> &&p) noexcept {
    return static_cast<T &&>(p.template v<T>());
  }
  template <typename T, typename ...Args>
  constexpr const T &&get(const Tuple_<Args...> &&p) noexcept {
    return static_cast<const T &&>(p.template v<T>());
  }
} // namespace Zu_

// ZuDeclTuple(Type, (Type0, Fn0), (Type1, Fn1), ...) creates
// a ZuTuple<Type0, ...> with named member functions Fn0, Fn1, etc.
// that alias p<0>, p<1>, etc.
//
// ZuDeclTuple(Person, (ZtString, name), (unsigned, age), (bool, gender));
// Person p = Person().name("Fred").age(1).gender(1);
// p.age() = 42;
// std::cout << p.name() << '\n';

#define ZuTuple_FieldType(args) \
  ZuPP_Defer(ZuTuple_FieldType_)()(ZuPP_Strip(args))
#define ZuTuple_FieldType_() ZuTuple_FieldType__
#define ZuTuple_FieldType__(type, fn) ZuPP_Strip(type)

#define ZuTuple_FieldFn(N, args) \
  ZuPP_Defer(ZuTuple_FieldFn_)()(N, ZuPP_Strip(args))
#define ZuTuple_FieldFn_() ZuTuple_FieldFn__
#define ZuTuple_FieldFn__(N, type, fn) \
  const auto &fn() const { return this->template p<N>(); } \
  auto &fn() { return this->template p<N>(); } \
  template <typename P> \
  auto &fn(P &&v) { this->template p<N>(ZuFwd<P>(v)); return *this; }

#define ZuDeclTuple(Type, ...) \
using Type##_ = \
  ZuTuple<ZuPP_Eval(ZuPP_MapComma(ZuTuple_FieldType, __VA_ARGS__))>; \
class Type : public Type##_ { \
  using Tuple = Type##_; \
public: \
  Type() = default; \
  Type(const Type &) = default; \
  Type &operator =(const Type &) = default; \
  Type(Type &&) = default; \
  Type &operator =(Type &&) = default; \
  ~Type() = default; \
  template <typename ...Args> \
  Type(Args &&... args) : Tuple{ZuFwd<Args>(args)...} { } \
  template <typename ...Args> \
  Type &operator =(ZuTuple<Args...> &&v) { \
    Tuple::operator =(ZuFwd<ZuTuple<Args...>>(v)); \
    return *this; \
  } \
  ZuPP_Eval(ZuPP_MapIndex(ZuTuple_FieldFn, 0, __VA_ARGS__)) \
  friend ZuTraits<Tuple> ZuTraitsType(Type *); \
}

#endif /* ZuTuple_HPP */
