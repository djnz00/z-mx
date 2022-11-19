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

// generic pair

#ifndef ZuPair_HPP
#define ZuPair_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuHash.hpp>
#include <zlib/ZuConversion.hpp>
#include <zlib/ZuPrint.hpp>
#include <zlib/ZuString.hpp>

struct ZuPair_ { };

template <typename T, typename P, bool IsPair> struct ZuPair_Cvt_;
template <typename T, typename P> struct ZuPair_Cvt_<T, P, 0> {
  enum { OK = 0 };
};
template <typename T, typename P> struct ZuPair_Cvt_<T, P, 1> {
  enum {
    OK = ZuConversion<typename T::T0, typename P::T0>::Exists &&
	 ZuConversion<typename T::T1, typename P::T1>::Exists
  };
};
template <typename T, typename P> struct ZuPair_Cvt :
  public ZuPair_Cvt_<ZuDecay<T>, P,
    ZuConversion<ZuPair_, ZuDecay<T>>::Base> { };

// fwd-declare the traits
namespace Zu_ {
  template <typename T0, typename T1> class Pair_;
}
template <typename T0, typename T1>
using ZuPair = Zu_::Pair_<T0, T1>;
template <typename T0, typename T1>
struct ZuTraits<ZuPair<T0, T1>> : public ZuBaseTraits<ZuPair<T0, T1>> {
  enum { IsPOD = ZuTraits<T0>::IsPOD && ZuTraits<T1>::IsPOD };
};

template <typename U0, typename U1> struct ZuPair_Print_ {
  ZuPair_Print_() = delete;
  ZuPair_Print_(const ZuPair_Print_ &) = delete;
  ZuPair_Print_ &operator =(const ZuPair_Print_ &) = delete;
  ZuPair_Print_(ZuPair_Print_ &&) = delete;
  ZuPair_Print_ &operator =(ZuPair_Print_ &&) = delete;
  ZuPair_Print_(const U0 &p0_, const U1 &p1_, const ZuString &delim_) :
      p0{p0_}, p1{p1_}, delim{delim_} { }
  const U0	&p0;
  const U1	&p1;
  ZuString	delim;
};
template <typename U0, typename U1, bool LeftPair, bool RightPair>
struct ZuPair_Print;
template <typename U0, typename U1>
struct ZuPair_Print<U0, U1, false, false> :
  public ZuPair_Print_<U0, U1>, public ZuPrintable {
  ZuPair_Print(const U0 &p0, const U1 &p1, const ZuString &delim) :
      ZuPair_Print_<U0, U1>{p0, p1, delim} { }
  template <typename S> void print(S &s) const {
    s << this->p0 << this->delim << this->p1;
  }
};
template <typename U0, typename U1>
struct ZuPair_Print<U0, U1, false, true> :
  public ZuPair_Print_<U0, U1>, public ZuPrintable {
  ZuPair_Print(const U0 &p0, const U1 &p1, const ZuString &delim) :
      ZuPair_Print_<U0, U1>{p0, p1, delim} { }
  template <typename S> void print(S &s) const {
    s << this->p0 << this->delim << this->p1.print(this->delim);
  }
};
template <typename U0, typename U1>
struct ZuPair_Print<U0, U1, true, false> :
  public ZuPair_Print_<U0, U1>, public ZuPrintable {
  ZuPair_Print(const U0 &p0, const U1 &p1, const ZuString &delim) :
      ZuPair_Print_<U0, U1>{p0, p1, delim} { }
  template <typename S> void print(S &s) const {
    s << this->p0.print(this->delim) << this->delim << this->p1;
  }
};
template <typename U0, typename U1>
struct ZuPair_Print<U0, U1, true, true> :
  public ZuPair_Print_<U0, U1>, public ZuPrintable {
  ZuPair_Print(const U0 &p0, const U1 &p1, const ZuString &delim) :
      ZuPair_Print_<U0, U1>{p0, p1, delim} { }
  template <typename S> void print(S &s) const {
    s <<
      this->p0.print(this->delim) << this->delim <<
      this->p1.print(this->delim);
  }
};

namespace Zu_ {
template <typename T0_, typename T1_> class Pair_ : public ZuPair_ {
  template <typename, typename> friend class Pair_;

public:
  using T0 = T0_;
  using T1 = T1_;
  using U0 = ZuDeref<T0_>;
  using U1 = ZuDeref<T1_>;
  using Types = ZuTypeList<T0, T1>;
  template <unsigned I> using Type = ZuType<I, Types>;

  template <typename T, bool> struct Bind_P0 {
    static decltype(auto) p0(const T &v) { return v.m_p0; }
    static decltype(auto) p0(T &v) { return v.m_p0; }
    static decltype(auto) p0(T &&v) { return ZuMv(v.m_p0); }
  };
  template <typename T> struct Bind_P0<T, true> {
    static decltype(auto) p0(const T &v) { return v.m_p0; }
    static decltype(auto) p0(T &v) { return v.m_p0; }
  };
  template <typename T, bool> struct Bind_P1 {
    static decltype(auto) p1(const T &v) { return v.m_p1; }
    static decltype(auto) p1(T &v) { return v.m_p1; }
    static decltype(auto) p1(T &&v) { return ZuMv(v.m_p1); }
  };
  template <typename T> struct Bind_P1<T, true> {
    static decltype(auto) p1(const T &v) { return v.m_p1; }
    static decltype(auto) p1(T &v) { return v.m_p1; }
  };
  template <typename T>
  struct Bind :
      public Bind_P0<T, ZuTraits<typename T::T0>::IsReference>,
      public Bind_P1<T, ZuTraits<typename T::T1>::IsReference> { };

  Pair_() = default;
  Pair_(const Pair_ &) = default;
  Pair_ &operator =(const Pair_ &) = default;
  Pair_(Pair_ &&) = default;
  Pair_ &operator =(Pair_ &&) = default;
  ~Pair_() = default;

  template <typename T>
  Pair_(T &&v, ZuIfT<ZuPair_Cvt<ZuDecay<T>, Pair_>::OK> *_ = nullptr) :
    m_p0{Bind<ZuDecay<T>>::p0(ZuFwd<T>(v))},
    m_p1{Bind<ZuDecay<T>>::p1(ZuFwd<T>(v))} { }

protected:
  template <typename T>
  ZuIfT<ZuPair_Cvt<ZuDecay<T>, Pair_>::OK> assign(T &&v) {
    m_p0 = Bind<ZuDecay<T>>::p0(ZuFwd<T>(v));
    m_p1 = Bind<ZuDecay<T>>::p1(ZuFwd<T>(v));
  }

public:
  template <typename T> Pair_ &operator =(T &&v) {
    assign(ZuFwd<T>(v));
    return *this;
  }

  template <typename P0, typename P1>
  Pair_(P0 &&p0, P1 &&p1,
      ZuIfT<
	(!ZuTraits<T0>::IsReference ||
	  ZuConversion<ZuDecay<U0>, ZuDecay<P0>>::Is) &&
	(!ZuTraits<T1>::IsReference ||
	  ZuConversion<ZuDecay<U1>, ZuDecay<P1>>::Is)> *_ = nullptr) :
    m_p0{ZuFwd<P0>(p0)}, m_p1{ZuFwd<P1>(p1)} { }

  template <typename P0, typename P1>
  bool equals(const Pair_<P0, P1> &p) const {
    return
      ZuCmp<T0>::equals(m_p0, p.template p<0>()) &&
      ZuCmp<T1>::equals(m_p1, p.template p<1>());
  }
  template <typename P0, typename P1>
  int cmp(const Pair_<P0, P1> &p) const {
    int i;
    if (i = ZuCmp<T0>::cmp(m_p0, p.template p<0>())) return i;
    return ZuCmp<T1>::cmp(m_p1, p.template p<1>());
  }
  template <typename L, typename R>
  friend inline ZuIfT<ZuConversion<Pair_, L>::Is, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline ZuIfT<ZuConversion<Pair_, L>::Is, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

  bool operator !() const { return !m_p0 || !m_p1; }
  ZuOpBool

  uint32_t hash() const {
    return ZuHash<T0>::hash(m_p0) ^ ZuHash<T1>::hash(m_p1);
  }

  template <unsigned I>
  ZuIfT<I == 0, const U0 &> p() const & {
    return m_p0;
  }
  template <unsigned I>
  ZuIfT<I == 0, U0 &> p() & {
    return m_p0;
  }
  template <unsigned I>
  ZuIfT<I == 0, U0 &&> p() && {
    return ZuMv(m_p0);
  }
  template <unsigned I, typename P>
  ZuIfT<I == 0, Pair_ &> p(P &&p) {
    m_p0 = ZuFwd<P>(p);
    return *this;
  }

  template <unsigned I>
  ZuIfT<I == 1, const U1 &> p() const & {
    return m_p1;
  }
  template <unsigned I>
  ZuIfT<I == 1, U1 &> p() & {
    return m_p1;
  }
  template <unsigned I>
  ZuIfT<I == 1, U1 &&> p() && {
    return ZuMv(m_p1);
  }
  template <unsigned I, typename P>
  ZuIfT<I == 1, Pair_ &> p(P &&p) {
    m_p1 = ZuFwd<P>(p);
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
  ZuIfT<ZuConversion<T, T0>::Same, Pair_ &> v(P &&p) {
    m_p0 = ZuFwd<P>(p);
    return *this;
  }

  template <typename T>
  ZuIfT<
      !ZuConversion<T, T0>::Same && ZuConversion<T, T1>::Same,
      const U1 &> v() const & {
    return m_p1;
  }
  template <typename T>
  ZuIfT<
      !ZuConversion<T, T0>::Same && ZuConversion<T, T1>::Same,
      U1 &> v() & {
    return m_p1;
  }
  template <typename T>
  ZuIfT<
      !ZuConversion<T, T0>::Same && ZuConversion<T, T1>::Same,
      U1 &&> v() && {
    return ZuMv(m_p1);
  }
  template <typename T, typename P>
  ZuIfT<
      !ZuConversion<T, T0>::Same && ZuConversion<T, T1>::Same,
      Pair_ &> v(P &&p) {
    m_p1 = ZuFwd<P>(p);
    return *this;
  }

  using Print = ZuPair_Print<U0, U1,
	ZuConversion<ZuPair_, U0>::Base, ZuConversion<ZuPair_, U1>::Base>;
  Print print() const {
    return Print{m_p0, m_p1, "|"};
  }
  Print print(const ZuString &delim) const {
    return Print{m_p0, m_p1, delim};
  }

  struct PrintType : public ZuPrintDelegate {
    template <typename S, typename P>
    static void print(S &s, const P &p) { s << p.print(); }
  };
  friend PrintType ZuPrintType(Pair_ *);

private:
  T0		m_p0;
  T1		m_p1;
};

} // namespace Zu_

template <typename T0, typename T1>
auto ZuFwdPair(T0 &&v0, T1 &&v1) {
  return ZuPair<T0 &&, T1 &&>(ZuFwd<T0>(v0), ZuFwd<T1>(v1));
}

template <typename T0, typename T1>
auto ZuMvPair(T0 v0, T1 v1) {
  return ZuPair<T0, T1>(ZuMv(v0), ZuMv(v1));
}

namespace Zu_ {
  template <typename ...Args> Pair_(Args...) -> Pair_<Args...>;
}

// STL structured binding cruft
#include <type_traits>
namespace std {
  template <class> struct tuple_size;
  template <typename T0, typename T1>
  struct tuple_size<ZuPair<T0, T1>> :
  public integral_constant<size_t, 2> { };

  template <size_t, class> struct tuple_element;
  template <typename T0, typename T1>
  struct tuple_element<0, ZuPair<T0, T1>> { using type = T0; };
  template <typename T0, typename T1>
  struct tuple_element<1, ZuPair<T0, T1>> { using type = T1; };
}
namespace Zu_ {
  using size_t = std::size_t;
  namespace {
    template <size_t I, typename T>
    using tuple_element_t = typename std::tuple_element<I, T>::type;
  }
  template <size_t I, typename T0, typename T1>
  constexpr tuple_element_t<I, Pair_<T0, T1>> &
  get(Pair_<T0, T1> &p) noexcept { return p.template p<I>(); }
  template <size_t I, typename T0, typename T1>
  constexpr const tuple_element_t<I, Pair_<T0, T1>> &
  get(const Pair_<T0, T1> &p) noexcept { return p.template p<I>(); }
  template <size_t I, typename T0, typename T1>
  constexpr tuple_element_t<I, Pair_<T0, T1>> &&
  get(Pair_<T0, T1> &&p) noexcept {
    return static_cast<tuple_element_t<I, Pair_<T0, T1>> &&>(
	p.template p<I>());
  }
  template <size_t I, typename T0, typename T1>
  constexpr const tuple_element_t<I, Pair_<T0, T1>> &&
  get(const Pair_<T0, T1> &&p) noexcept {
    return static_cast<const tuple_element_t<I, Pair_<T0, T1>> &&>(
	p.template p<I>());
  }

  template <typename T, typename U>
  constexpr T &get(Pair_<T, U> &p) noexcept {
    return p.template p<0>();
  }
  template <typename T, typename U>
  constexpr const T &get(const Pair_<T, U> &p) noexcept {
    return p.template p<0>();
  }
  template <typename T, typename U>
  constexpr T &&get(Pair_<T, U> &&p) noexcept {
    return static_cast<T &&>(p.template p<0>());
  }
  template <typename T, typename U>
  constexpr const T &&get(const Pair_<T, U> &&p) noexcept {
    return static_cast<const T &&>(p.template p<0>());
  }

  template <typename T, typename U>
  constexpr T &get(Pair_<U, T> &p) noexcept {
    return p.template p<1>();
  }
  template <typename T, typename U>
  constexpr const T &get(const Pair_<U, T> &p) noexcept {
    return p.template p<1>();
  }
  template <typename T, typename U>
  constexpr T &&get(Pair_<U, T> &&p) noexcept {
    return static_cast<T &&>(p.template p<1>());
  }
  template <typename T, typename U>
  constexpr const T &&get(const Pair_<U, T> &&p) noexcept {
    return static_cast<const T &&>(p.template p<1>());
  }
} // namespace Zu_

// generic accessor
template <
  typename P,
  unsigned J,
  bool = ZuTraits<typename P::template Type<J>>::IsReference>
struct ZuPairAxor_Bind {
  static decltype(auto) get(const P &v) { return v.template p<J>(); }
  static decltype(auto) get(P &v) { return v.template p<J>(); }
  static decltype(auto) get(P &&v) { return ZuMv(ZuMv(v).template p<J>()); }
};
template <typename P, unsigned J>
struct ZuPairAxor_Bind<P, J, true> {
  static decltype(auto) get(const P &v) { return v.template p<J>(); }
  static decltype(auto) get(P &v) { return v.template p<J>(); }
};
template <unsigned I = 0>
inline constexpr auto ZuPairAxor() {
  return [] <typename P> (P &&v) -> decltype(auto) {
    return ZuPairAxor_Bind<ZuDecay<P>, I>::get(ZuFwd<P>(v));
  };
}

#endif /* ZuPair_HPP */
