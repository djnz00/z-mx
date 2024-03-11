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

// 128bit decimal fixed point with 36 digits and constant 10^18 scaling, i.e.
// 18 integer digits and 18 fractional digits (18 decimal places)

#ifndef ZuDecimal_HPP
#define ZuDecimal_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuInt.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuDecimalFn.hpp>
#include <zlib/ZuTraits.hpp>
#include <zlib/ZuPrint.hpp>
#include <zlib/ZuFmt.hpp>
#include <zlib/ZuBox.hpp>
#include <zlib/ZuFixed.hpp>

template <typename Fmt> struct ZuDecimalFmt;	// internal
class ZuDecimalVFmt;				// ''

struct ZuDecimal {
  template <unsigned N> using Pow10 = ZuDecimalFn::Pow10<N>;

  constexpr static const int128_t minimum() {
    return -Pow10<36U>{} + 1;
  }
  constexpr static const int128_t maximum() {
    return Pow10<36U>{} - 1;
  }
  constexpr static const int128_t reset() {
    return -Pow10<36U>{};
  }
  constexpr static const int128_t null() {
    return static_cast<int128_t>(1)<<127;
  }
  constexpr static const uint64_t scale() { // 10^18
    return Pow10<18U>{};
  }
  constexpr static const long double scale_fp() { // 10^18
    return 1000000000000000000.0L;
  }

  int128_t	value;

  constexpr ZuDecimal() : value{null()} { }
  constexpr ZuDecimal(const ZuDecimal &v) : value{v.value} { }
  constexpr ZuDecimal &operator =(const ZuDecimal &v) {
    value = v.value;
    return *this;
  }
  constexpr ZuDecimal(ZuDecimal &&v) : value{v.value} { }
  constexpr ZuDecimal &operator =(ZuDecimal &&v) {
    value = v.value;
    return *this;
  }
  constexpr ~ZuDecimal() { }

  enum NoInit_ { NoInit };
  constexpr ZuDecimal(NoInit_ _) { }

  enum Unscaled_ { Unscaled };
  constexpr ZuDecimal(Unscaled_ _, int128_t v) : value{v} { }

  template <typename V>
  constexpr ZuDecimal(V v, ZuMatchIntegral<V> *_ = nullptr) :
      value(static_cast<int128_t>(v) * scale()) { }

  template <typename V>
  constexpr ZuDecimal(V v, ZuMatchFloatingPoint<V> *_ = nullptr) :
      value((long double)v * scale_fp()) { }

  template <typename V>
  constexpr ZuDecimal(V v, unsigned exponent, ZuMatchIntegral<V> *_ = nullptr) :
      value(static_cast<int128_t>(v) * ZuDecimalFn::pow10_64(18 - exponent)) { }

  constexpr int128_t adjust(unsigned exponent) const {
    if (ZuUnlikely(exponent == 18)) return value;
    return value / ZuDecimalFn::pow10_64(18 - exponent);
  }

  constexpr ZuDecimal operator -() { return ZuDecimal{Unscaled, -value}; }

  constexpr ZuDecimal operator +(const ZuDecimal &v) const {
    return ZuDecimal{Unscaled, value + v.value};
  }
  constexpr ZuDecimal &operator +=(const ZuDecimal &v) {
    value += v.value;
    return *this;
  }

  constexpr ZuDecimal operator -(const ZuDecimal &v) const {
    return ZuDecimal{Unscaled, value - v.value};
  }
  constexpr ZuDecimal &operator -=(const ZuDecimal &v) {
    value -= v.value;
    return *this;
  }

  // mul and div functions based on reference code (BSD licensed) at:
  // https://www.codeproject.com/Tips/618570/UInt-Multiplication-Squaring 

  // h:l = u * v
  static void mul128by128(
      uint128_t u, uint128_t v,
      uint128_t &h, uint128_t &l) {
    uint128_t u1 = static_cast<uint64_t>(u);
    uint128_t v1 = static_cast<uint64_t>(v);
    uint128_t t = (u1 * v1);
    uint128_t w3 = static_cast<uint64_t>(t);
    uint128_t k = (t>>64);

    u >>= 64;
    t = (u * v1) + k;
    k = static_cast<uint64_t>(t);
    uint128_t w1 = (t>>64);

    v >>= 64;
    t = (u1 * v) + k;
    k = (t>>64);

    h = (u * v) + w1 + k;
    l = (t<<64) + w3;
  }

  // same as mul128by128, but constant multiplier 10^18
  // h:l = u * 10^18;
  static void mul128scale(
      uint128_t u,
      uint128_t &h, uint128_t &l) {
    uint128_t u1 = static_cast<uint64_t>(u);
    const uint128_t v1 = scale();
    uint128_t t = (u1 * v1);
    uint128_t w3 = static_cast<uint64_t>(t);
    uint128_t k = (t>>64);

    u >>= 64;

    t = u * v1 + k;
    k = static_cast<uint64_t>(t);

    h = (t>>64) + (k>>64);
    l = (k<<64) + w3;
  }

  // q = u1:u0 / v
  static void div256by128(
      const uint128_t u1, const uint128_t u0, uint128_t v,
      uint128_t &q) {// , uint128_t &r)
    const uint128_t b = ((uint128_t)1)<<64;
    uint128_t un1, un0, vn1, vn0, q1, q0, un128, un21, un10, rhat, left, right;
    size_t s;

    if (!v)
      s = 0;
    else if (v < b)
      s = __builtin_clzll(static_cast<uint64_t>(v)) + 64;
    else
      s = __builtin_clzll(static_cast<uint64_t>(v>>64));
    v <<= s;
    vn0 = static_cast<uint64_t>(v);
    vn1 = v>>64;

    if (s > 0) {
      un128 = (u1<<s) | (u0>>(128 - s));
      un10 = u0<<s;
    } else {
      un128 = u1;
      un10 = u0;
    }

    un1 = un10>>64;
    un0 = static_cast<uint64_t>(un10);

    q1 = un128 / vn1;
    rhat = un128 % vn1;

    left = q1 * vn0;
    right = (rhat<<64) + un1;

  loop1:
    if ((q1 >= b) || (left > right)) {
      --q1;
      rhat += vn1;
      if (rhat < b) {
	left -= vn0;
	right = (rhat<<64) | un1;
	goto loop1;
      }
    }

    un21 = (un128<<64) + (un1 - (q1 * v));

    q0 = un21 / vn1;
    rhat = un21 % vn1;

    left = q0 * vn0;
    right = (rhat<<64) | un0;

  loop2:
    if ((q0 >= b) || (left > right)) {
      --q0;
      rhat += vn1;
      if (rhat < b) {
	left -= vn0;
	right = (rhat<<64) | un0;
	goto loop2;
      }
    }

    // r = ((un21<<64) + (un0 - (q0 * v)))>>s;
    q = (q1<<64) | q0;
  }

  // same as div256by128, but constant divisor 10^18, no remainder
  // q = u1:u0 * 10^-18
  static uint128_t div256scale(const uint128_t u1, const uint128_t u0) {
    const uint128_t b = ((uint128_t)1)<<64;
    const uint128_t v = ((uint128_t)scale())<<68;

    uint128_t un1, un0, vn1, vn0, q1, q0, un128, un21, un10, rhat, left, right;

    vn0 = static_cast<uint64_t>(v);
    vn1 = v>>64;

    un128 = (u1<<68) | (u0>>60);
    un10 = u0<<68;

    un1 = un10>>64;
    un0 = static_cast<uint64_t>(un10);

    q1 = un128 / vn1;
    rhat = un128 % vn1;

    left = q1 * vn0;
    right = (rhat<<64) + un1;

  loop1:
    if ((q1 >= b) || (left > right)) {
      --q1;
      rhat += vn1;
      if (rhat < b) {
	left -= vn0;
	right = (rhat<<64) | un1;
	goto loop1;
      }
    }

    un21 = (un128<<64) + (un1 - (q1 * v));

    q0 = un21 / vn1;
    rhat = un21 % vn1;

    left = q0 * vn0;
    right = (rhat<<64) | un0;

  loop2:
    if ((q0 >= b) || (left > right)) {
      --q0;
      rhat += vn1;
      if (rhat < b) {
	left -= vn0;
	right = (rhat<<64) | un0;
	goto loop2;
      }
    }

    // r = ((un21<<64) + (un0 - (q0 * v)))>>s;
    return (q1<<64) | q0;
  }

  static int128_t mul(int128_t u_, int128_t v_) {
    bool negative = u_ < 0;
    if (negative) u_ = -u_;
    if (v_ < 0) negative = !negative, v_ = -v_;

    uint128_t u = u_, v = v_;

    uint128_t h, l;

    mul128by128(u, v, h, l);

    if (h >= scale()) return null(); // overflow

    u = div256scale(h, l);

    if (negative) return -static_cast<int128_t>(u);
    return u;
  }

  static int128_t div(int128_t u_, int128_t v_) {
    bool negative = u_ < 0;
    if (negative) u_ = -u_;
    if (v_ < 0) negative = !negative, v_ = -v_;

    uint128_t u = u_, v = v_;

    uint128_t h, l;

    mul128scale(u, h, l);

    div256by128(h, l, v, u);

    if (negative) return -static_cast<int128_t>(u);
    return u;
  }

public:
  ZuDecimal operator *(const ZuDecimal &v) const {
    return ZuDecimal{Unscaled, mul(value, v.value)};
  }

  ZuDecimal &operator *=(const ZuDecimal &v) {
    value = mul(value, v.value);
    return *this;
  }

  ZuDecimal operator /(const ZuDecimal &v) const {
    return ZuDecimal{Unscaled, div(value, v.value)};
  }
  ZuDecimal &operator /=(const ZuDecimal &v) {
    value = div(value, v.value);
    return *this;
  }

  // scan from string
  template <typename S>
  ZuDecimal(const S &s_, ZuMatchString<S> *_ = nullptr) {
    ZuString s{s_};
    if (ZuUnlikely(!s)) goto null;
    if (ZuUnlikely(s.length() == 3 &&
	  s[0] == 'n' && s[1] == 'a' && s[2] == 'n')) goto null;
    {
      bool negative = s[0] == '-';
      if (ZuUnlikely(negative)) {
	s.offset(1);
	if (ZuUnlikely(!s)) goto null;
      }
      while (s[0] == '0') {
	s.offset(1);
	if (!s) { value = 0; return; }
      }
      uint64_t iv = 0, fv = 0;
      unsigned n = s.length();
      if (ZuUnlikely(s[0] == '.')) {
	if (ZuUnlikely(n == 1)) { value = 0; return; }
	goto frac;
      }
      n = Zu_atou(iv, s.data(), n);
      if (ZuUnlikely(!n)) goto null;
      if (ZuUnlikely(n > 18)) goto null; // overflow
      s.offset(n);
      if ((n = s.length()) > 1 && s[0] == '.') {
  frac:
	if (--n > 18) n = 18;
	n = Zu_atou(fv, &s[1], n);
	if (fv && n < 18)
	  fv *= ZuDecimalFn::pow10_64(18 - n);
      }
      value = ((uint128_t)iv) * scale() + fv;
      if (ZuUnlikely(negative)) value = -value;
    }
    return;
  null:
    value = null();
    return;
  }

  // convert to floating point
  long double fp() {
    if (ZuUnlikely(value == null())) return ZuFP<long double>::nan();
    return static_cast<long double>(value) / scale_fp();
  }

  // comparisons
  bool equals(const ZuDecimal &v) const { return value == v.value; }
  int cmp(const ZuDecimal &v) const {
    return (value > v.value) - (value < v.value);
  }
  template <typename L, typename R>
  friend inline ZuIfT<ZuInspect<ZuDecimal, L>::Is, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline ZuIfT<ZuInspect<ZuDecimal, L>::Is, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

  // ! is zero, unary * is !null
  bool operator !() const { return !value; }
  ZuOpBool

  bool operator *() const {
    // return value != null(); // disabled due to compiler bug
    int128_t v = value - null();
    return static_cast<bool>(
	static_cast<uint64_t>(v>>64) | static_cast<uint64_t>(v));
  }

  operator int64_t() const {
    uint128_t scale_ = static_cast<uint128_t>(scale());
    return static_cast<int64_t>(value / scale_);
  }
  uint64_t frac_() const {
    uint128_t scale_ = static_cast<uint128_t>(scale());
    uint128_t value_ = value < 0 ? -value : value;
    return value_ % scale_;
  }
  int64_t round() const {
    uint128_t scale_ = static_cast<uint128_t>(scale());
    auto int_ = static_cast<int64_t>(value / scale_);
    return value < 0 ?
  int_ - ((static_cast<uint128_t>(-value) % scale_) > 500000000000000000ULL) :
  int_ + ((static_cast<uint128_t>( value) % scale_) > 500000000000000000ULL);
  }

  unsigned exponent() const {
    auto frac = frac_();
    if (ZuLikely(!frac)) return 0;
    unsigned exp = 18;
    // binary search
    if (!(frac % 1000000000ULL)) { frac /= 1000000000ULL; exp -= 9; }
    if (!(frac % 100000ULL)) { frac /= 100000ULL; exp -= 5; }
    if (!(frac % 1000ULL)) { frac /= 1000ULL; exp -= 3; }
    if (exp >= 2 && !(frac % 100ULL)) { frac /= 100ULL; exp -= 2; }
    if (exp >= 1 && !(frac % 10ULL)) --exp;
    return exp;
  }

  operator ZuFixed() {
    auto e = exponent();
    return {value / ZuDecimalFn::pow10_128(18 - e), e};
  }

  template <typename S> void print(S &s) const;

  template <typename Fmt> ZuDecimalFmt<Fmt> fmt(Fmt) const;
  ZuDecimalVFmt vfmt() const;
  template <typename VFmt>
  ZuDecimalVFmt vfmt(VFmt &&) const;

  // traits
  struct Traits : public ZuTraits<int128_t> { enum { IsPrimitive = 0 }; };
  friend Traits ZuTraitsType(ZuDecimal *);

  // printing
  friend ZuPrintFn ZuPrintType(ZuDecimal *);
};
template <typename Fmt> struct ZuDecimalFmt {
  const ZuDecimal	&fixed;

  template <typename S> void print(S &s) const {
    if (ZuUnlikely(!*fixed)) { s << "nan"; return; }
    uint128_t iv, fv;
    if (ZuUnlikely(fixed.value < 0)) {
      s << '-';
      iv = -fixed.value;
    } else
      iv = fixed.value;
    fv = iv % ZuDecimal::scale();
    iv /= ZuDecimal::scale();
    s << ZuBoxed(iv).fmt<Fmt>();
    if (fv) s << '.' << ZuBoxed(fv).fmt<ZuFmt::Frac<18>>();
  }

  friend ZuPrintFn ZuPrintType(ZuDecimalFmt *);
};
template <class Fmt> inline ZuDecimalFmt<Fmt> ZuDecimal::fmt(Fmt) const
{
  return ZuDecimalFmt<Fmt>{*this};
}
template <typename S> inline void ZuDecimal::print(S &s) const
{
  s << ZuDecimalFmt<ZuFmt::Default>{*this};
}
class ZuDecimalVFmt : public ZuVFmtWrapper<ZuDecimalVFmt> {
public:
  ZuDecimalVFmt(const ZuDecimal &decimal) : m_decimal{decimal} { }
  template <typename VFmt>
  ZuDecimalVFmt(const ZuDecimal &decimal, VFmt &&fmt) :
    ZuVFmtWrapper<ZuDecimalVFmt>{ZuFwd<VFmt>(fmt)}, m_decimal{decimal} { }

  template <typename S> void print(S &s) const {
    if (ZuUnlikely(!*m_decimal)) { s << "nan"; return; }
    uint128_t iv, fv;
    if (ZuUnlikely(m_decimal.value < 0)) {
      s << '-';
      iv = -m_decimal.value;
    } else
      iv = m_decimal.value;
    fv = iv % ZuDecimal::scale();
    iv /= ZuDecimal::scale();
    s << ZuBoxed(iv).vfmt(fmt);
    if (fv) s << '.' << ZuBoxed(fv).fmt<ZuFmt::Frac<18>>();
  }

  friend ZuPrintFn ZuPrintType(ZuDecimalVFmt *);

private:
  const ZuDecimal	&m_decimal;
};

inline ZuDecimalVFmt ZuDecimal::vfmt() const {
  return ZuDecimalVFmt{*this};
}
template <typename VFmt>
inline ZuDecimalVFmt ZuDecimal::vfmt(VFmt &&fmt) const {
  return ZuDecimalVFmt{*this, ZuFwd<VFmt>(fmt)};
}

// ZuCmp has to be specialized since null() is otherwise !t (instead of !*t)
template <> struct ZuCmp<ZuDecimal> {
  using T = ZuDecimal;
  static int cmp(const T &t1, const T &t2) { return t1.cmp(t2); }
  static bool less(const T &t1, const T &t2) { return t1 < t2; }
  static bool equals(const T &t1, const T &t2) { return t1 == t2; }
  static bool null(const T &t) { return !*t; }
  constexpr static T null() { return {}; }
};

#endif /* ZuDecimal_HPP */
