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

// 64bit decimal variable point with variable exponent,
// 18 significant digits and 10^-exponent scaling:
//   (18 - exponent) integer digits
//   exponent fractional digits (i.e. number of decimal places)
// Note: exponent is negated

#ifndef ZuFixed_HPP
#define ZuFixed_HPP

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

// ZuFixedVal value range is +/- 10^18

#define ZuFixedMin (static_cast<int64_t>(-999999999999999999LL))
#define ZuFixedMax (static_cast<int64_t>(999999999999999999LL))
// ZuFixedReset is the distinct sentinel value used to reset values to null
#define ZuFixedReset (static_cast<int64_t>(-1000000000000000000LL))
// ZuFixedNull is the null sentinel value
#define ZuFixedNull (static_cast<int64_t>(-0x8000000000000000LL))

using ZuFixedVal = ZuBoxN(int64_t, ZuFixedNull); // mantissa numerator
using ZuFixedExp = ZuBox0(uint8_t); // exponent (precision in decimal places)

// combination of value and exponent, used in transit for conversions, I/O,
// constructors, scanning:
//   ZuFixed(<integer>, exponent)		// {1042, 2} -> 10.42
//   ZuFixed(<floating point>, exponent)	// {10.42, 2} -> 10.42
//   ZuFixed(<string>, exponent)		// {"10.42", 2} -> 10.42
//   int64_t x = ZuFixed{"42.42", 2}.mantissa()	// x == 4242
//   ZuFixed xn{x, 2}; xn *= ZuFixed{2000, 3}; x = xn.mantissa() // x == 8484
// printing:
//   s << ZuFixed{...}			// print (default)
//   s << ZuFixed{...}.fmt(ZuFmt...)	// print (compile-time formatted)
//   s << ZuFixed{...}.vfmt(ZuVFmt...)	// print (variable run-time formatted)
//   s << ZuFixed(x, 2)			// s << "42.42"
//   x = 4200042;				// 42000.42
//   s << ZuFixed(x, 2).fmt(ZuFmt::Comma<>())	// s << "42,000.42"

template <typename Fmt> struct ZuFixed_Fmt;	// internal
struct ZuFixed_VFmt;				// internal

class ZuFixed {
  static constexpr int64_t null_() {
    return ZuCmp_IntNull<int64_t, 8, 1>::null();
  }

public:
  ZuFixed() = default;

  template <typename M>
  ZuFixed(M m, unsigned e, ZuIsIntegral<M> *_ = 0) :
      m_mantissa{static_cast<int64_t>(m)},
      m_exponent{static_cast<uint8_t>(e)} { }

  template <typename V>
  ZuFixed(V v, unsigned e, ZuIsFloatingPoint<V> *_ = 0) :
    m_mantissa{static_cast<int64_t>(
	static_cast<double>(v) * ZuDecimalFn::pow10_64(e))},
    m_exponent{static_cast<uint8_t>(e)} { }

  // multiply: exponent of result is taken from the LHS
  // a 128bit integer intermediary is used to avoid overflow
  ZuFixed operator *(const ZuFixed &v) const {
    int128_t i = mantissa();
    i *= v.mantissa();
    i /= ZuDecimalFn::pow10_64(v.exponent());
    if (ZuUnlikely(i >= 1000000000000000ULL)) return ZuFixed{};
    return ZuFixed{i, exponent()};
  }

  // divide: exponent of result is taken from the LHS
  // a 128bit integer intermediary is used to avoid overflow
  ZuFixed operator /(const ZuFixed &v) const {
    int128_t i = mantissa();
    i *= ZuDecimalFn::pow10_64(exponent());
    i /= v.mantissa();
    if (ZuUnlikely(i >= 1000000000000000ULL)) return ZuFixed{};
    return ZuFixed{i, exponent()};
  }

  void init(int64_t m, unsigned e) { m_mantissa = m; m_exponent = e; }

  void null() { m_mantissa = ZuFixedNull; m_exponent = 0; }

  int64_t mantissa() const { return m_mantissa; }
  void mantissa(int64_t v) { m_mantissa = v; }

  unsigned exponent() const { return m_exponent; }
  void exponent(unsigned v) { m_exponent = v; }

  // convert to floating point
  template <typename Float = ZuBox<double>>
  Float fp() const {
    if (ZuUnlikely(!operator *())) return Float{};
    return Float{mantissa()} / Float{ZuDecimalFn::pow10_64(exponent())};
  }

  // adjust mantissa to another exponent
  ZuFixedVal adjust(unsigned e) const {
    if (ZuUnlikely(!operator *())) return {};
    if (ZuLikely(e == exponent())) return mantissa();
    if (e > exponent())
      return mantissa() * ZuDecimalFn::pow10_64(e - exponent());
    return mantissa() / ZuDecimalFn::pow10_64(exponent() - e);
  }

  // comparisons
  int cmp(const ZuFixed &v) const {
    if (ZuLikely(exponent() == v.exponent() || !**this || !*v))
      return (m_mantissa > v.m_mantissa) - (m_mantissa < v.m_mantissa);
    int128_t i = mantissa();
    int128_t j = v.mantissa();
    if (exponent() < v.exponent())
      i *= ZuDecimalFn::pow10_64(v.exponent() - exponent());
    else
      j *= ZuDecimalFn::pow10_64(exponent() - v.exponent());
    return (i > j) - (i < j);
  }
  bool less(const ZuFixed &v) const {
    if (ZuLikely(exponent() == v.exponent() || !**this || !*v))
      return m_mantissa < v.m_mantissa;
    int128_t i = mantissa();
    int128_t j = v.mantissa();
    if (exponent() < v.exponent())
      i *= ZuDecimalFn::pow10_64(v.exponent() - exponent());
    else
      j *= ZuDecimalFn::pow10_64(exponent() - v.exponent());
    return i < j;
  }
  bool equals(const ZuFixed &v) const {
    if (ZuLikely(exponent() == v.exponent() || !**this || !*v))
      return m_mantissa == v.m_mantissa;
    int128_t i = mantissa();
    int128_t j = v.mantissa();
    if (exponent() < v.exponent())
      i *= ZuDecimalFn::pow10_64(v.exponent() - exponent());
    else
      j *= ZuDecimalFn::pow10_64(exponent() - v.exponent());
    return i == j;
  }
  bool operator ==(const ZuFixed &v) const { return equals(v); }
  bool operator !=(const ZuFixed &v) const { return !equals(v); }
  bool operator >(const ZuFixed &v) const { return v.less(*this); }
  bool operator >=(const ZuFixed &v) const { return !less(v); }
  bool operator <(const ZuFixed &v) const { return less(v); }
  bool operator <=(const ZuFixed &v) const { return !v.less(*this); }

  // ! is zero, unary * is !null
  bool operator !() const { return !mantissa(); }
  ZuOpBool

  bool operator *() const { return m_mantissa != null_(); }

  // scan from string
  template <typename S>
  ZuFixed(const S &s, ZuIsString<S> *_ = 0) {
    scan<false>(s, 0);
  }
  template <typename S>
  ZuFixed(const S &s, unsigned e, ZuIsString<S> *_ = 0) {
    scan<true>(s, e);
  }
private:
  template <bool Exponent>
  void scan(ZuString s, unsigned e) {
    if (ZuUnlikely(!s)) goto null;
    if constexpr (Exponent) if (e > 18) e = 18;
    {
      bool negative = s[0] == '-';
      if (ZuUnlikely(negative)) {
	s.offset(1);
	if (ZuUnlikely(!s)) goto null;
      }
      while (s[0] == '0') s.offset(1);
      if (!s) goto zero;
      uint64_t iv = 0, fv = 0;
      unsigned n = s.length();
      if (ZuUnlikely(s[0] == '.')) {
	if (ZuUnlikely(n == 1)) goto zero;
	if constexpr (!Exponent) e = n - 1;
	goto frac;
      }
      n = Zu_atou(iv, s.data(), n);
      if (ZuUnlikely(!n)) goto null;
      if (ZuUnlikely(n > (18 - e))) goto null; // overflow
      s.offset(n);
      if constexpr (!Exponent) e = 18 - n;
      if ((n = s.length()) > 1 && s[0] == '.') {
  frac:
	if (--n > e) n = e;
	n = Zu_atou(fv, &s[1], n);
	if (fv && e > n)
	  fv *= ZuDecimalFn::pow10_64(e - n);
      }
      int64_t m = iv * ZuDecimalFn::pow10_64(e) + fv;
      if (ZuUnlikely(negative)) m = -m;
      init(m, e);
    }
    return;
  null:
    null();
    return;
  zero:
    init(0, e);
    return;
  }

public:
  // printing
  template <typename S> void print(S &s) const;
  friend ZuPrintFn ZuPrintType(ZuFixed *);

  template <typename Fmt> ZuFixed_Fmt<Fmt> fmt(Fmt) const;
  ZuFixed_VFmt vfmt() const;
  template <typename VFmt>
  ZuFixed_VFmt vfmt(VFmt &&) const;

private:
  int64_t	m_mantissa = ZuFixedNull;
  uint8_t	m_exponent = 0;
};
template <typename Fmt> struct ZuFixed_Fmt {
  const ZuFixed	&value;

  template <typename S> void print(S &s) const {
    if (ZuUnlikely(!*value)) return;
    auto iv = value.mantissa();
    if (ZuUnlikely(iv < 0)) { s << '-'; iv = -iv; }
    uint64_t factor = ZuDecimalFn::pow10_64(value.exponent());
    ZuFixedVal fv = iv % factor;
    iv /= factor;
    s << ZuBoxed(iv).fmt(Fmt());
    if (fv) s << '.' << ZuBoxed(fv).vfmt().frac(value.exponent());
  }
  friend ZuPrintFn ZuPrintType(ZuFixed_Fmt *);
};
template <class Fmt>
ZuFixed_Fmt<Fmt> ZuFixed::fmt(Fmt) const
{
  return ZuFixed_Fmt<Fmt>{*this};
}
template <typename S> inline void ZuFixed::print(S &s) const
{
  s << ZuFixed_Fmt<ZuFmt::Default>{*this};
}
struct ZuFixed_VFmt : public ZuVFmtWrapper<ZuFixed_VFmt> {
  const ZuFixed	&value;

  ZuFixed_VFmt(const ZuFixed &value_) : value{value_} { }
  template <typename VFmt>
  ZuFixed_VFmt(const ZuFixed &value_, VFmt &&fmt) :
    ZuVFmtWrapper<ZuFixed_VFmt>{ZuFwd<VFmt>(fmt)}, value{value_} { }

  template <typename S> void print(S &s) const {
    if (ZuUnlikely(!*value)) return;
    ZuFixedVal iv = value.mantissa();
    if (ZuUnlikely(iv < 0)) { s << '-'; iv = -iv; }
    uint64_t factor = ZuDecimalFn::pow10_64(value.exponent());
    ZuFixedVal fv = iv % factor;
    iv /= factor;
    s << ZuBoxed(iv).vfmt(this->fmt);
    if (fv) s << '.' << ZuBoxed(fv).vfmt().frac(value.exponent());
  }
  friend ZuPrintFn ZuPrintType(ZuFixed_VFmt *);
};
inline ZuFixed_VFmt ZuFixed::vfmt() const {
  return ZuFixed_VFmt{*this};
}
template <typename VFmt>
inline ZuFixed_VFmt ZuFixed::vfmt(VFmt &&fmt) const {
  return ZuFixed_VFmt{*this, ZuFwd<VFmt>(fmt)};
}

#endif /* ZuFixed_HPP */
