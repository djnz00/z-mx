//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// "boxed" primitive types (concept terminology from C#)

#ifndef ZuBox_HH
#define ZuBox_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <limits.h>
#include <math.h>
#include <float.h>

#include <zlib/ZuAssert.hh>
#include <zlib/ZuTraits.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuHash.hh>
#include <zlib/ZuInt.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuString.hh>

#include <zlib/ZuFP.hh>
#include <zlib/ZuFmt.hh>

#include <zlib/Zu_ntoa.hh>
#include <zlib/Zu_aton.hh>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4312 4800)
#endif

template <typename T, typename Cmp> class ZuBox;
template <typename> struct ZuIsBoxed : public ZuFalse { };
template <typename T, typename Cmp>
struct ZuIsBoxed<ZuBox<T, Cmp>> : public ZuTrue { };

template <typename U, typename R = void>
using ZuMatchBoxed = ZuIfT<ZuIsBoxed<U>{}, R>;
template <typename U, typename R = void>
using ZuNotBoxed = ZuIfT<!ZuIsBoxed<U>{}, R>;

// compile-time formatting
template <typename T, typename Fmt,
  bool Signed = ZuTraits<T>::IsSigned,
  bool FloatingPoint = Signed && ZuTraits<T>::IsFloatingPoint>
struct ZuBox_Print;
template <typename T, typename Fmt> struct ZuBox_Print<T, Fmt, 0, 0> {
  static unsigned length(T v)
    { return Zu_nprint<Fmt>::ulen(v); }
  static unsigned print(T v, char *buf)
    { return Zu_nprint<Fmt>::utoa(v, buf); }
};
template <typename T, typename Fmt> struct ZuBox_Print<T, Fmt, 1, 0> {
  static unsigned length(T v)
    { return Zu_nprint<Fmt>::ilen(v); }
  static unsigned print(T v, char *buf)
    { return Zu_nprint<Fmt>::itoa(v, buf); }
};
template <typename T, typename Fmt> struct ZuBox_Print<T, Fmt, 1, 1> {
  static unsigned length(T v)
    { return Zu_nprint<Fmt>::flen(v); }
  static unsigned print(T v, char *buf)
    { return Zu_nprint<Fmt>::ftoa(v, buf); }
};

// variable run-time formatting
template <typename T,
  bool Signed = ZuTraits<T>::IsSigned,
  bool FloatingPoint = Signed && ZuTraits<T>::IsFloatingPoint>
struct ZuBox_VPrint;
template <typename T> struct ZuBox_VPrint<T, 0, 0> {
  static unsigned length(const ZuVFmt &fmt, T v)
    { return Zu_vprint::ulen(fmt, v); }
  static unsigned print(const ZuVFmt &fmt, T v, char *buf)
    { return Zu_vprint::utoa(fmt, v, buf); }
};
template <typename T> struct ZuBox_VPrint<T, 1, 0> {
  static unsigned length(const ZuVFmt &fmt, T v)
    { return Zu_vprint::ilen(fmt, v); }
  static unsigned print(const ZuVFmt &fmt, T v, char *buf)
    { return Zu_vprint::itoa(fmt, v, buf); }
};
template <typename T> struct ZuBox_VPrint<T, 1, 1> {
  static unsigned length(const ZuVFmt &fmt, T v)
    { return Zu_vprint::flen(fmt, v); }
  static unsigned print(const ZuVFmt &fmt, T v, char *buf)
    { return Zu_vprint::ftoa(fmt, v, buf); }
};

template <typename T, typename Fmt,
  bool Signed = ZuTraits<T>::IsSigned,
  bool FloatingPoint = Signed && ZuTraits<T>::IsFloatingPoint>
struct ZuBox_Scan;
template <typename T_, typename Fmt> struct ZuBox_Scan<T_, Fmt, 0, 0> {
  using T = uint64_t;
  static unsigned scan(T &v, const char *buf, unsigned n)
    { return Zu_nscan<Fmt>::atou(v, buf, n); }
};
template <typename T_, typename Fmt> struct ZuBox_Scan<T_, Fmt, 1, 0> {
  using T = int64_t;
  static unsigned scan(T &v, const char *buf, unsigned n)
    { return Zu_nscan<Fmt>::atoi(v, buf, n); }
};
template <typename T_, typename Fmt> struct ZuBox_Scan<T_, Fmt, 1, 1> {
  using T = T_;
  static unsigned scan(T &v, const char *buf, unsigned n)
    { return Zu_nscan<Fmt>::atof(v, buf, n); }
};

// generic printing
template <typename B> struct ZuBoxPrint : public ZuPrintBuffer {
  static unsigned length(const B &b) {
    return b.length();
  }
  static unsigned print(char *buf, unsigned, const B &b) {
    return b.print(buf);
  }
};

// compile-time formatting
template <typename Boxed, typename Fmt> class ZuBoxFmt {
template <typename, typename> friend class ZuBox;

  using Print = ZuBox_Print<typename Boxed::T, Fmt>;

  ZuBoxFmt(const Boxed &ref) : m_ref(ref) { }

public:
  unsigned length() const { return Print::length(m_ref); }
  unsigned print(char *buf) const { return Print::print(m_ref, buf); }

  friend ZuBoxPrint<ZuBoxFmt> ZuPrintType(ZuBoxFmt *);

private:
  const Boxed	&m_ref;
};

// run-time formatting
template <typename Boxed>
class ZuBoxVFmt : public ZuVFmtWrapper<ZuBoxVFmt<Boxed>> {
template <typename, typename> friend class ZuBox;

  using Print = ZuBox_VPrint<typename Boxed::T>;

public:
  ZuBoxVFmt(const Boxed &v) : m_value(v) { }
  template <typename VFmt>
  ZuBoxVFmt(const Boxed &v, VFmt &&fmt) :
    ZuVFmtWrapper<ZuBoxVFmt>{ZuFwd<VFmt>(fmt)}, m_value{v} { }

  // print
  unsigned length() const {
    return Print::length(this->fmt, m_value);
  }
  unsigned print(char *buf) const {
    return Print::print(this->fmt, m_value, buf);
  }
  friend ZuBoxPrint<ZuBoxVFmt> ZuPrintType(ZuBoxVFmt *);

private:
  const Boxed	&m_value;
};

// SFINAE...
template <typename U, typename T, typename R = void>
using ZuBox_MatchReal =
  ZuIfT<!ZuIsBoxed<U>{} && !ZuTraits<U>::IsString && (
      ZuTraits<U>::IsReal ||
      ZuInspect<U, T>::Converts ||
      ZuInspect<U, int>::Converts), R>;
template <typename Traits, typename R, bool IsPointer, bool IsArray>
struct ZuBox_MatchCharPtr_ { };
template <typename Traits, typename R>
struct ZuBox_MatchCharPtr_<Traits, R, true, false> :
    public ZuIfT_<ZuInspect<typename Traits::Elem, char>::Same, R> { };
template <typename Traits, typename R>
struct ZuBox_MatchCharPtr_<Traits, R, false, true> :
    public ZuIfT_<ZuInspect<typename Traits::Elem, char>::Same, R> { };
template <typename S, typename R = void>
using ZuBox_MatchCharPtr =
  typename ZuBox_MatchCharPtr_<ZuTraits<S>, R,
    ZuTraits<S>::IsPointer,
    ZuTraits<S>::IsArray>::T;

template <typename U>
struct ZuUnbox_ { using T = U; };
template <typename U, typename Cmp>
struct ZuUnbox_<ZuBox<U, Cmp> > { using T = U; };
template <typename U>
using ZuUnbox = typename ZuUnbox_<U>::T;

template <typename T_, typename Cmp_ = ZuCmp<ZuUnbox<T_>> >
class ZuBox {
template <typename, typename> friend class ZuBox;
template <typename, typename> friend class ZuBoxFmt;
template <typename> friend class ZuBoxVFmt;

public:
  using T = ZuUnbox<T_>;
  using Cmp = Cmp_;

private:
  template <typename Fmt = ZuFmt::Default> using Scan = ZuBox_Scan<T, Fmt>;
  template <typename Fmt = ZuFmt::Default> using Print = ZuBox_Print<T, Fmt>;

  ZuAssert(ZuTraits<T>::IsPrimitive && ZuTraits<T>::IsReal);

public:
  ZuBox() : m_val(Cmp::null()) { }

  ZuBox(const ZuBox &b) : m_val(b.m_val) { }
  ZuBox &operator =(const ZuBox &b) { m_val = b.m_val; return *this; }

  template <typename R>
  ZuBox(R r, ZuBox_MatchReal<R, T> *_ = nullptr) :
    m_val(r) { }

  template <typename B>
  ZuBox(B b, ZuMatchBoxed<B> *_ = nullptr) :
    m_val(!*b ? static_cast<T>(Cmp::null()) : static_cast<T>(b.m_val)) { }

  template <typename S>
  ZuBox(S &&s_, ZuMatchCharString<S> *_ = nullptr) :
      m_val(Cmp::null()) {
    ZuString s(ZuFwd<S>(s_));
    typename Scan<>::T val = 0;
    if (ZuLikely(s && Scan<>::scan(val, s.data(), s.length())))
      m_val = val;
  }
  template <typename Fmt, typename S>
  ZuBox(Fmt, S &&s_, ZuMatchCharString<S> *_ = nullptr) :
      m_val(Cmp::null()) {
    ZuString s(ZuFwd<S>(s_));
    typename Scan<Fmt>::T val = 0;
    if (ZuLikely(s && Scan<Fmt>::scan(val, s.data(), s.length())))
      m_val = val;
  }

  template <typename S>
  ZuBox(S s, unsigned len,
      ZuBox_MatchCharPtr<S> *_ = nullptr) : m_val(Cmp::null()) {
    typename Scan<>::T val = 0;
    if (ZuLikely(s && Scan<>::scan(val, s, len)))
      m_val = val;
  }
  template <typename Fmt, typename S>
  ZuBox(Fmt, S s, unsigned len,
      ZuBox_MatchCharPtr<S> *_ = nullptr) : m_val(Cmp::null()) {
    typename Scan<Fmt>::T val = 0;
    if (ZuLikely(s && Scan<Fmt>::scan(val, s, len)))
      m_val = val;
  }

  T val() const { return m_val; }

private:
  template <typename R>
  ZuBox_MatchReal<R, T> assign(R r) { m_val = r; }

  template <typename B>
  ZuMatchBoxed<B> assign(const B &b) {
    m_val = !*b ? (T)Cmp::null() : (T)b.m_val;
  }

  template <typename S>
  ZuMatchCharString<S> assign(S &&s_) {
    ZuString s(ZuFwd<S>(s_));
    typename Scan<>::T val = 0;
    if (ZuUnlikely(!s || !Scan<>::scan(val, s.data(), s.length())))
      m_val = Cmp::null();
    else
      m_val = val;
  }

public:
  template <typename T>
  ZuBox &operator =(T &&t) { assign(ZuFwd<T>(t)); return *this; }

  template <typename T>
  static ZuMatchFloatingPoint<T, bool> equals_(T t1, T t2) {
    if (Cmp::null(t2)) return Cmp::null(t1);
    if (Cmp::null(t1)) return false;
    return t1 == t2;
  }
  template <typename T>
  static ZuNotFloatingPoint<T, bool> equals_(T t1, T t2) {
    return t1 == t2;
  }
  bool equals(const ZuBox &b) const {
    return equals_(m_val, b.m_val);
  }

  template <typename Cmp__ = Cmp>
  ZuIfT<!ZuInspect<Cmp__, ZuCmp0<T> >::Same, int>
  cmp_(const ZuBox &b) const {
    if (Cmp::null(b.m_val)) return Cmp::null(m_val) ? 0 : 1;
    if (Cmp::null(m_val)) return -1;
    return Cmp::cmp(m_val, b.m_val);
  }
  template <typename Cmp__ = Cmp>
  ZuIfT<ZuInspect<Cmp__, ZuCmp0<T> >::Same, int>
  cmp_(const ZuBox &b) const {
    return Cmp::cmp(m_val, b.m_val);
  }
  int cmp(const ZuBox &b) const { return cmp_<>(b.m_val); }
  template <typename L, typename R>
  friend inline ZuIfT<ZuInspect<ZuBox, L>::Is, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline ZuIfT<ZuInspect<ZuBox, L>::Is, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

  bool operator !() const { return !m_val; }

  bool operator *() const { return !Cmp::null(m_val); }

  uint32_t hash() const { return ZuHash<T>::hash(m_val); }

  operator T() const { return m_val; }
  operator T &() & { return m_val; }

  // compile-time formatting
  template <typename Fmt = ZuFmt::Default>
  ZuBoxFmt<ZuBox, Fmt> fmt() const { 
    return ZuBoxFmt<ZuBox, Fmt>{*this};
  }
  template <bool Upper = false, typename Fmt = ZuFmt::Default>
  ZuBoxFmt<ZuBox, ZuFmt::Hex<Upper, Fmt>> hex() const {
    return ZuBoxFmt<ZuBox, ZuFmt::Hex<Upper, Fmt>>{*this};
  }
  template <
    int NDP = -ZuFmt::Default::NDP_,
    char Trim = '\0',
    typename Fmt = ZuFmt::Default>
  ZuBoxFmt<ZuBox, ZuFmt::FP<NDP, Trim, Fmt>> fp() const {
    return ZuBoxFmt<ZuBox, ZuFmt::FP<NDP, Trim, Fmt>>{*this};
  }
  // run-time formatting
  ZuBoxVFmt<ZuBox> vfmt() const {
    return ZuBoxVFmt<ZuBox>{*this};
  }
  template <typename VFmt>
  ZuBoxVFmt<ZuBox> vfmt(VFmt &&fmt) const {
    return ZuBoxVFmt<ZuBox>{*this, ZuFwd<VFmt>(fmt)};
  }

  template <typename Fmt = ZuFmt::Default, typename S>
  ZuMatchCharString<S, unsigned> scan(S &&s_) {
    ZuString s(ZuFwd<S>(s_));
    typename Scan<>::T val = 0;
    unsigned n = Scan<Fmt>::scan(val, s.data(), s.length());
    if (ZuUnlikely(!n)) {
      m_val = Cmp::null();
      return 0U;
    }
    m_val = val;
    return n;
  }
  template <typename Fmt = ZuFmt::Default, typename S>
  ZuBox_MatchCharPtr<S, unsigned> scan(S s, unsigned len) {
    typename Scan<Fmt>::T val = 0;
    unsigned n = Scan<Fmt>::scan(val, s, len);
    if (ZuUnlikely(!n)) {
      m_val = Cmp::null();
      return 0U;
    }
    m_val = val;
    return n;
  }

  unsigned length() const {
    return Print<>::length(m_val);
  }
  unsigned print(char *buf) const {
    return Print<>::print(m_val, buf);
  }

  // Note: the caller is required to check for null, BY DESIGN

  ZuBox operator -() { return -m_val; }

  template <typename R>
  ZuBox operator +(const R &r) const { return m_val + r; }
  template <typename R>
  ZuBox operator -(const R &r) const { return m_val - r; }
  template <typename R>
  ZuBox operator *(const R &r) const { return m_val * r; }
  template <typename R>
  ZuBox operator /(const R &r) const { return m_val / r; }
  template <typename R>
  ZuBox operator %(const R &r) const { return m_val % r; }
  template <typename R>
  ZuBox operator |(const R &r) const { return m_val | r; }
  template <typename R>
  ZuBox operator &(const R &r) const { return m_val & r; }
  template <typename R>
  ZuBox operator ^(const R &r) const { return m_val ^ r; }

  ZuBox operator ++(int) { return m_val++; }
  ZuBox &operator ++() { ++m_val; return *this; }
  ZuBox operator --(int) { return m_val--; }
  ZuBox &operator --() { --m_val; return *this; }

  template <typename R>
  ZuBox &operator +=(const R &r) { m_val += r; return *this; }
  template <typename R>
  ZuBox &operator -=(const R &r) { m_val -= r; return *this; }
  template <typename R>
  ZuBox &operator *=(const R &r) { m_val *= r; return *this; }
  template <typename R>
  ZuBox &operator /=(const R &r) { m_val /= r; return *this; }
  template <typename R>
  ZuBox &operator %=(const R &r) { m_val %= r; return *this; }
  template <typename R>
  ZuBox &operator |=(const R &r) { m_val |= r; return *this; }
  template <typename R>
  ZuBox &operator &=(const R &r) { m_val &= r; return *this; }
  template <typename R>
  ZuBox &operator ^=(const R &r) { m_val ^= r; return *this; }

  // apply update (leaves existing value in place if u is null)
  ZuBox &update(const ZuBox &u) {
    if (!Cmp::null(u)) m_val = u.m_val;
    return *this;
  }
  // apply update, with additional sentinel value signifying "reset to null"
  ZuBox &update(const ZuBox &u, const ZuBox &reset) {
    if (!Cmp::null(u)) {
      if (u == reset)
	m_val = Cmp::null();
      else
	m_val = u.m_val;
    }
    return *this;
  }

  static ZuBox inf() { return ZuBox{Cmp::inf()}; }
  ZuBox epsilon() const { return ZuBox{Cmp::epsilon(m_val)}; }
  bool feq(T r) const {
    if (Cmp::null(m_val)) return Cmp::null(r);
    if (Cmp::null(r)) return false;
    return feq_(r);
  }
  bool feq_(T r) const {
    if (ZuLikely(m_val == r)) return true;
    if (ZuLikely(m_val >= 0.0)) {
      if (r < 0.0) return false;
      if (m_val > r) return m_val - r < Cmp::epsilon(m_val);
      return r - m_val < Cmp::epsilon(r);
    }
    if (r > 0.0) return false;
    T val = -m_val;
    r = -r;
    if (val > r) return val - r < Cmp::epsilon(val);
    return r - val < Cmp::epsilon(r);
  }
  bool fne(T r) const { return !feq(r); }
  bool fge(T r) const {
    if (Cmp::null(m_val)) return Cmp::null(r);
    if (Cmp::null(r)) return false;
    return m_val > r || feq_(r);
  }
  bool fle(T r) const {
    if (Cmp::null(m_val)) return Cmp::null(r);
    if (Cmp::null(r)) return false;
    return m_val < r || feq_(r);
  }
  bool fgt(T r) const { return !fle(r); }
  bool flt(T r) const { return !fge(r); }

  int fcmp(T r) const {
    if (Cmp::null(r)) return Cmp::null(m_val) ? 0 : 1;
    if (Cmp::null(m_val)) return -1;
    if (feq_(r)) return 0;
    return m_val > r ? 1 : -1;
  }

  // traits
  struct Traits : public ZuTraits<T> { enum { IsPrimitive = 0 }; };
  friend Traits ZuTraitsType(ZuBox *);

  // printing
  friend ZuBoxPrint<ZuBox> ZuPrintType(ZuBox *);

private:
  T	m_val;
};

#define ZuBox0(T) ZuBox<T, ZuCmp0<T> >
#define ZuBox_1(T) ZuBox<T, ZuCmp_1<T> >
#define ZuBoxN(T, N) ZuBox<T, ZuCmpN<T, N> >

// ZuCmp has to be specialized since null() is otherwise !t (instead of !*t)
template <typename T_, typename Cmp>
struct ZuCmp<ZuBox<T_, Cmp> > {
  using T = ZuBox<T_, Cmp>;
  static int cmp(const T &t1, const T &t2) { return t1.cmp(t2); }
  static bool less(const T &t1, const T &t2) { return t1 < t2; }
  static bool equals(const T &t1, const T &t2) { return t1 == t2; }
  static bool null(const T &t) { return !*t; }
  static const T &null() { static const T v; return v; }
};

// ZuBoxT<T> is T if T is already boxed, ZuBox<T> otherwise
template <typename U>
struct ZuBoxT_ { using T = ZuBox<U>; };
template <typename U, typename Cmp>
struct ZuBoxT_<ZuBox<U, Cmp>> { using T = ZuBox<U, Cmp>; };
template <typename U>
using ZuBoxT = typename ZuBoxT_<U>::T;

// ZuBoxed(t) - convenience function to cast primitives to boxed
template <typename T>
const ZuMatchBoxed<T, T> &ZuBoxed(const T &t) { return t; }
template <typename T>
ZuMatchBoxed<T, T> &ZuBoxed(T &t) { return t; }
template <typename T>
const ZuNotBoxed<T, ZuBox<T>> &ZuBoxed(const T &t) {
  const ZuBox<T> *ZuMayAlias(t_) = reinterpret_cast<const ZuBox<T> *>(&t);
  return *t_;
}
template <typename T>
ZuNotBoxed<T, ZuBox<T>> &ZuBoxed(T &t) {
  ZuBox<T> *ZuMayAlias(t_) = reinterpret_cast<ZuBox<T> *>(&t);
  return *t_;
}

// ZuBoxPtr(t) - convenience function to box pointers as uintptr_t
#define ZuBoxPtr(x) \
  (ZuBox<uintptr_t, ZuCmp0<uintptr_t> >{reinterpret_cast<uintptr_t>(x)})

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZuBox_HH */
