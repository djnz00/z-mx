//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// 64bit decimal variable point with variable number of decimal places,
// 18 significant digits and 10^-<ndp> scaling:
//   <18 - ndp> integer digits
//   <ndp> fractional digits (i.e. number of decimal places)
// Note: <ndp> is the negative of the mathematical exponent
//
// combination of value and ndp, used in transit for conversions, I/O,
// constructors, scanning:
//   ZuFixed(<integer>, ndp)		// {1042, 2} -> 10.42
//   ZuFixed(<floating point>, ndp)	// {10.42, 2} -> 10.42
//   ZuFixed(<string>, ndp)		// {"10.42", 2} -> 10.42
//   int64_t x = ZuFixed{"42.42", 2}.mantissa()	// x == 4242
//   ZuFixed xn{x, 2}; xn *= ZuFixed{2000, 3}; x = xn.mantissa() // x == 8484
// printing:
//   s << ZuFixed{...}			// print (default)
//   s << ZuFixed{...}.fmt(ZuFmt...)	// print (compile-time formatted)
//   s << ZuFixed{...}.vfmt(ZuVFmt...)	// print (variable run-time formatted)
//   s << ZuFixed(x, 2)				// s << "42.42"
//   x = 4200042;				// 42000.42
//   s << ZuFixed(x, 2).fmt(ZuFmt::Comma<>())	// s << "42,000.42"

#ifndef ZuFixed_HH
#define ZuFixed_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuInt.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuDecimalFn.hh>
#include <zlib/ZuTraits.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuFmt.hh>
#include <zlib/ZuBox.hh>

// ZuFixedVal value range is +/- 10^18

#define ZuFixedMin (int64_t(-999999999999999999LL))
#define ZuFixedMax (int64_t(999999999999999999LL))
// ZuFixedReset is the distinct sentinel value used to reset values to null
#define ZuFixedReset (int64_t(-1000000000000000000LL))
// ZuFixedNull is the null sentinel value
#define ZuFixedNull (int64_t(-0x8000000000000000LL))

using ZuFixedVal = ZuBoxN(int64_t, ZuFixedNull); // mantissa numerator
using ZuFixedExp = ZuBox0(uint8_t); // ndp (precision in decimal places)

template <typename Fmt> struct ZuFixed_Fmt;	// internal
struct ZuFixed_VFmt;				// internal

class ZuFixed {
public:
  ZuFixed() = default;

  template <typename M, decltype(ZuMatchIntegral<M>{}, int()) = 0>
  constexpr ZuFixed(M m, unsigned e) :
    m_mantissa{int64_t(m)}, m_ndp{uint8_t(e)} { }

  template <typename V, decltype(ZuMatchFloatingPoint<V>{}, int()) = 0>
  constexpr ZuFixed(V v, unsigned e) :
    m_mantissa{int64_t(double(v) * ZuDecimalFn::pow10_64(e))},
    m_ndp{uint8_t(e)} { }

  // multiply: ndp of result is taken from the LHS
  // a 128bit integer intermediate is used to avoid overflow
  ZuFixed operator *(const ZuFixed &v) const {
    int128_t i = mantissa();
    i *= v.mantissa();
    i /= ZuDecimalFn::pow10_64(v.ndp());
    if (ZuUnlikely(i >= 1000000000000000ULL)) return ZuFixed{};
    return ZuFixed{i, ndp()};
  }

  // divide: ndp of result is taken from the LHS
  // a 128bit integer intermediate is used to avoid overflow
  ZuFixed operator /(const ZuFixed &v) const {
    int128_t i = mantissa();
    i *= ZuDecimalFn::pow10_64(ndp());
    i /= v.mantissa();
    if (ZuUnlikely(i >= 1000000000000000ULL)) return ZuFixed{};
    return ZuFixed{i, ndp()};
  }

  void init(int64_t m, unsigned e) { m_mantissa = m; m_ndp = e; }

  void null() { m_mantissa = ZuFixedNull; m_ndp = 0; }

  int64_t mantissa() const { return m_mantissa; }
  void mantissa(int64_t v) { m_mantissa = v; }

  unsigned ndp() const { return m_ndp; }
  void ndp(unsigned v) { m_ndp = v; }

  // convert to floating point
  template <typename Float = ZuBox<double>>
  Float fp() const {
    if (ZuUnlikely(!operator *())) return Float{};
    return Float{mantissa()} / Float{ZuDecimalFn::pow10_64(ndp())};
  }

  // adjust mantissa to another ndp
  ZuFixedVal adjust(unsigned e) const {
    if (ZuUnlikely(!operator *())) return {};
    if (ZuLikely(e == ndp())) return mantissa();
    if (e > ndp())
      return mantissa() * ZuDecimalFn::pow10_64(e - ndp());
    return mantissa() / ZuDecimalFn::pow10_64(ndp() - e);
  }

  // comparisons
  bool equals(const ZuFixed &v) const {
    if (ZuLikely(ndp() == v.ndp() || !**this || !*v))
      return m_mantissa == v.m_mantissa;
    int128_t i = mantissa();
    int128_t j = v.mantissa();
    if (ndp() < v.ndp())
      i *= ZuDecimalFn::pow10_64(v.ndp() - ndp());
    else
      j *= ZuDecimalFn::pow10_64(ndp() - v.ndp());
    return i == j;
  }
  int cmp(const ZuFixed &v) const {
    if (ZuLikely(ndp() == v.ndp() || !**this || !*v))
      return (m_mantissa > v.m_mantissa) - (m_mantissa < v.m_mantissa);
    int128_t i = mantissa();
    int128_t j = v.mantissa();
    if (ndp() < v.ndp())
      i *= ZuDecimalFn::pow10_64(v.ndp() - ndp());
    else
      j *= ZuDecimalFn::pow10_64(ndp() - v.ndp());
    return (i > j) - (i < j);
  }
  template <typename L, typename R>
  friend inline ZuIfT<ZuInspect<ZuFixed, L>::Is, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline ZuIfT<ZuInspect<ZuFixed, L>::Is, bool>
  operator <(const L &l, const R &r) { return l.cmp(r) < 0; }
  template <typename L, typename R>
  friend inline ZuIfT<ZuInspect<ZuFixed, L>::Is, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

  // ! is zero, unary * is !null
  bool operator !() const { return !mantissa(); }
  ZuOpBool

  constexpr bool operator *() const { return m_mantissa != ZuFixedNull; }

  // hash
  uint32_t hash() const {
    return
      ZuHash<int64_t>::hash(m_mantissa) ^
      ZuHash<uint8_t>::hash(m_ndp);
  }

  // scan from string
  template <typename S, decltype(ZuMatchString<S>{}, int()) = 0>
  ZuFixed(const S &s) {
    scan(s);
  }
  template <typename S, decltype(ZuMatchString<S>{}, int()) = 0>
  ZuFixed(const S &s, unsigned e) {
    scan(s, e);
  }

  template <bool NDP = true>
  unsigned scan(ZuString s, unsigned e) {
    unsigned int m = 0;
    if (ZuUnlikely(!s)) goto null;
    if (ZuUnlikely(s.length() == 3 &&
	  s[0] == 'n' && s[1] == 'a' && s[2] == 'n')) {
      null();
      return 3;
    }
    if constexpr (NDP) if (e > 18) e = 18;
    {
      bool negative = s[0] == '-';
      if (ZuUnlikely(negative)) {
	s.offset(1), ++m;
	if (ZuUnlikely(!s)) goto null;
      }
      while (s[0] == '0') {
	s.offset(1), ++m;
	if (!s) goto zero;
      }
      uint64_t iv = 0, fv = 0;
      unsigned n = s.length();
      if (ZuUnlikely(s[0] == '.')) {
	++m;
	if (ZuUnlikely(n == 1)) goto zero;
	if constexpr (!NDP) e = n - 1;
	goto frac;
      }
      n = Zu_atou(iv, s.data(), n);
      if (ZuUnlikely(!n)) goto null;
      if (ZuUnlikely(n > (18 - e))) goto null; // overflow
      s.offset(n), m += n;
      if constexpr (!NDP) e = 18 - n;
      if ((n = s.length()) > 1 && s[0] == '.') {
	++m;
  frac:
	if (--n > e) n = e;
	n = Zu_atou(fv, &s[1], n);
	m += n;
	if (fv && n < e)
	  fv *= ZuDecimalFn::pow10_64(e - n);
      }
      int64_t v = iv * ZuDecimalFn::pow10_64(e) + fv;
      if (ZuUnlikely(negative)) v = -v;
      init(v, e);
    }
    return m;
  zero:
    init(0, e);
    return m;
  null:
    null();
    return 0;
  }
  unsigned scan(ZuString s) { return scan<false>(s, 0); }

public:
  // printing
  template <typename S> void print(S &s) const;
  friend ZuPrintFn ZuPrintType(ZuFixed *);

  template <typename Fmt> ZuFixed_Fmt<Fmt> fmt() const;
  template <bool Upper = false, typename Fmt = ZuFmt::Default>
  ZuFixed_Fmt<ZuFmt::Hex<Upper, Fmt>> hex() const;
  template <
    int NDP = -ZuFmt::Default::NDP_,
    char Trim = '\0',
    typename Fmt = ZuFmt::Default>
  ZuFixed_Fmt<ZuFmt::FP<NDP, Trim, Fmt>> fp() const;

  ZuFixed_VFmt vfmt() const;
  template <typename VFmt>
  ZuFixed_VFmt vfmt(VFmt &&) const;

private:
  int64_t	m_mantissa = ZuFixedNull;
  uint8_t	m_ndp = 0;
};

template <typename Fmt> struct ZuFixed_Fmt {
  const ZuFixed	&value;

  template <typename S> void print(S &s) const {
    if (ZuUnlikely(!*value)) { s << "nan"; return; }
    auto iv = value.mantissa();
    if (ZuUnlikely(iv < 0)) { s << '-'; iv = -iv; }
    uint64_t factor = ZuDecimalFn::pow10_64(value.ndp());
    ZuFixedVal fv = iv % factor;
    iv /= factor;
    s << ZuBoxed(iv).template fmt<Fmt>();
    auto e = value.ndp();
    if (fv) s << '.' << ZuBoxed(fv).vfmt().frac(e, e);
  }
  friend ZuPrintFn ZuPrintType(ZuFixed_Fmt *);
};
template <class Fmt>
inline ZuFixed_Fmt<Fmt> ZuFixed::fmt() const
{
  return ZuFixed_Fmt<Fmt>{*this};
}
template <bool Upper, typename Fmt>
inline ZuFixed_Fmt<ZuFmt::Hex<Upper, Fmt>> ZuFixed::hex() const
{
  return ZuFixed_Fmt<ZuFmt::Hex<Upper, Fmt>>{*this};
}
template <int NDP, char Trim, typename Fmt>
inline ZuFixed_Fmt<ZuFmt::FP<NDP, Trim, Fmt>> ZuFixed::fp() const
{
  return ZuFixed_Fmt<ZuFmt::FP<NDP, Trim, Fmt>>{*this};
}
template <typename S>
inline void ZuFixed::print(S &s) const
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
    if (ZuUnlikely(!*value)) { s << "nan"; return; }
    ZuFixedVal iv = value.mantissa();
    if (ZuUnlikely(iv < 0)) { s << '-'; iv = -iv; }
    uint64_t factor = ZuDecimalFn::pow10_64(value.ndp());
    ZuFixedVal fv = iv % factor;
    iv /= factor;
    s << ZuBoxed(iv).vfmt(this->fmt);
    auto e = value.ndp();
    if (fv) s << '.' << ZuBoxed(fv).vfmt().frac(e, e);
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

template <> struct ZuCmp<ZuFixed> {
  template <typename L, typename R>
  constexpr static int cmp(const L &l, const R &r) { return l.cmp(r); }
  template <typename L, typename R>
  constexpr static bool equals(const L &l, const R &r) { return l == r; }
  template <typename L, typename R>
  constexpr static bool less(const L &l, const R &r) { return l < r; }
  constexpr static bool null(const ZuFixed &v) { return !*v; }
  constexpr static ZuFixed null() { return {}; }
  constexpr static ZuFixed minimum() { return {ZuFixedMin, 0}; }
  constexpr static ZuFixed maximum() { return {ZuFixedMax, 0}; }
};

#endif /* ZuFixed_HH */
