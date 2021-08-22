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

// "boxed" primitive types (concept terminology from C#)

#ifndef ZuBox_HPP
#define ZuBox_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <limits.h>
#include <math.h>
#include <float.h>

#include <zlib/ZuAssert.hpp>
#include <zlib/ZuTraits.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuHash.hpp>
#include <zlib/ZuInt.hpp>
#include <zlib/ZuConversion.hpp>
#include <zlib/ZuPrint.hpp>
#include <zlib/ZuString.hpp>

#include <zlib/ZuFP.hpp>
#include <zlib/ZuFmt.hpp>

#include <zlib/Zu_ntoa.hpp>
#include <zlib/Zu_aton.hpp>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4312 4800)
#endif

template <typename T, class Cmp> class ZuBox;
template <typename> struct ZuIsBoxed_ { enum { OK = 0 }; };
template <typename T, typename Cmp>
struct ZuIsBoxed_<ZuBox<T, Cmp>> { enum { OK = 1 }; };

template <typename U, typename R = void>
using ZuIsBoxed = ZuIfT<ZuIsBoxed_<U>::OK, R>;
template <typename U, typename R = void>
using ZuNotBoxed = ZuIfT<!ZuIsBoxed_<U>::OK, R>;

// compile-time formatting
template <typename T, class Fmt,
  bool Signed = ZuTraits<T>::IsSigned,
  bool FloatingPoint = Signed && ZuTraits<T>::IsFloatingPoint>
struct ZuBox_Print;
template <typename T, class Fmt> struct ZuBox_Print<T, Fmt, 0, 0> {
  static unsigned length(T v)
    { return Zu_nprint<Fmt>::ulen(v); }
  static unsigned print(T v, char *buf)
    { return Zu_nprint<Fmt>::utoa(v, buf); }
};
template <typename T, class Fmt> struct ZuBox_Print<T, Fmt, 1, 0> {
  static unsigned length(T v)
    { return Zu_nprint<Fmt>::ilen(v); }
  static unsigned print(T v, char *buf)
    { return Zu_nprint<Fmt>::itoa(v, buf); }
};
template <typename T, class Fmt> struct ZuBox_Print<T, Fmt, 1, 1> {
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

template <typename T, class Fmt,
  bool Signed = ZuTraits<T>::IsSigned,
  bool FloatingPoint = Signed && ZuTraits<T>::IsFloatingPoint>
struct ZuBox_Scan;
template <typename T_, class Fmt> struct ZuBox_Scan<T_, Fmt, 0, 0> {
  using T = uint64_t;
  static unsigned scan(T &v, const char *buf, unsigned n)
    { return Zu_nscan<Fmt>::atou(v, buf, n); }
};
template <typename T_, class Fmt> struct ZuBox_Scan<T_, Fmt, 1, 0> {
  using T = int64_t;
  static unsigned scan(T &v, const char *buf, unsigned n)
    { return Zu_nscan<Fmt>::atoi(v, buf, n); }
};
template <typename T_, class Fmt> struct ZuBox_Scan<T_, Fmt, 1, 1> {
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
template <class Boxed, class Fmt> class ZuBoxFmt {
template <typename, class> friend class ZuBox;

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
template <class Boxed>
class ZuBoxVFmt : public ZuVFmtWrapper<ZuBoxVFmt<Boxed>> {
template <typename, class> friend class ZuBox;

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
using ZuBox_IsReal =
  ZuIfT<!ZuIsBoxed_<U>::OK && !ZuTraits<U>::IsString && (
      ZuTraits<U>::IsReal ||
      ZuConversion<U, T>::Exists ||
      ZuConversion<U, int>::Exists), R>;
template <typename Traits, typename T, bool IsPointer, bool IsArray>
struct ZuBox_IsCharPtr_ { };
template <typename Traits, typename T>
struct ZuBox_IsCharPtr_<Traits, T, 1, 0> :
    public ZuIfT_<ZuConversion<typename Traits::Elem, char>::Same, T> { };
template <typename Traits, typename T>
struct ZuBox_IsCharPtr_<Traits, T, 0, 1> :
    public ZuIfT_<ZuConversion<typename Traits::Elem, char>::Same, T> { };
template <typename S, typename T = void>
using ZuBox_IsCharPtr =
  typename ZuBox_IsCharPtr_<ZuTraits<S>, T,
    ZuTraits<S>::IsPointer,
    ZuTraits<S>::IsArray>::T;

template <typename U>
struct ZuBox_Unbox_ { using T = U; };
template <typename U, class Cmp>
struct ZuBox_Unbox_<ZuBox<U, Cmp> > { using T = U; };
template <typename U>
using ZuBox_Unbox = typename ZuBox_Unbox_<U>::T;

template <typename T_, class Cmp_ = ZuCmp<ZuBox_Unbox<T_>> >
class ZuBox {
template <typename, typename> friend class ZuBox;
template <typename, typename> friend class ZuBoxFmt;
template <typename> friend class ZuBoxVFmt;

public:
  using T = ZuBox_Unbox<T_>;
  using Cmp = Cmp_;

private:
  template <class Fmt = ZuFmt::Default> using Scan = ZuBox_Scan<T, Fmt>;
  template <class Fmt = ZuFmt::Default> using Print = ZuBox_Print<T, Fmt>;

  ZuAssert(ZuTraits<T>::IsPrimitive && ZuTraits<T>::IsReal);

public:
  ZuBox() : m_val(Cmp::null()) { }

  ZuBox(const ZuBox &b) : m_val(b.m_val) { }
  ZuBox &operator =(const ZuBox &b) { m_val = b.m_val; return *this; }

  template <typename R>
  ZuBox(R r, ZuBox_IsReal<R, T> *_ = nullptr) :
    m_val(r) { }

  template <typename B>
  ZuBox(B b, ZuIsBoxed<B> *_ = nullptr) :
    m_val(!*b ? static_cast<T>(Cmp::null()) : static_cast<T>(b.m_val)) { }

  template <typename S>
  ZuBox(S &&s_, ZuIsCharString<S> *_ = nullptr) :
      m_val(Cmp::null()) {
    ZuString s(ZuFwd<S>(s_));
    typename Scan<>::T val = 0;
    if (ZuLikely(s && Scan<>::scan(val, s.data(), s.length())))
      m_val = val;
  }
  template <class Fmt, typename S>
  ZuBox(Fmt, S &&s_, ZuIsCharString<S> *_ = nullptr) :
      m_val(Cmp::null()) {
    ZuString s(ZuFwd<S>(s_));
    typename Scan<Fmt>::T val = 0;
    if (ZuLikely(s && Scan<Fmt>::scan(val, s.data(), s.length())))
      m_val = val;
  }

  template <typename S>
  ZuBox(S s, unsigned len,
      ZuBox_IsCharPtr<S> *_ = nullptr) : m_val(Cmp::null()) {
    typename Scan<>::T val = 0;
    if (ZuLikely(s && Scan<>::scan(val, s, len)))
      m_val = val;
  }
  template <class Fmt, typename S>
  ZuBox(Fmt, S s, unsigned len,
      ZuBox_IsCharPtr<S> *_ = nullptr) : m_val(Cmp::null()) {
    typename Scan<Fmt>::T val = 0;
    if (ZuLikely(s && Scan<Fmt>::scan(val, s, len)))
      m_val = val;
  }

  T val() const { return m_val; }

private:
  template <typename R>
  ZuBox_IsReal<R, T> assign(R r) { m_val = r; }

  template <typename B>
  ZuIsBoxed<B> assign(const B &b) {
    m_val = !*b ? (T)Cmp::null() : (T)b.m_val;
  }

  template <typename S>
  ZuIsCharString<S> assign(S &&s_) {
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

  template <class Cmp__ = Cmp>
  ZuIfT<!ZuConversion<Cmp__, ZuCmp0<T> >::Same, int>
  cmp_(const ZuBox &b) const {
    if (Cmp::null(b.m_val)) return Cmp::null(m_val) ? 0 : 1;
    if (Cmp::null(m_val)) return -1;
    return Cmp::cmp(m_val, b.m_val);
  }
  template <class Cmp__ = Cmp>
  ZuIfT<ZuConversion<Cmp__, ZuCmp0<T> >::Same, int>
  cmp_(const ZuBox &b) const {
    return Cmp::cmp(m_val, b.m_val);
  }
  int cmp(const ZuBox &b) const { return cmp_<>(b.m_val); }

  template <typename T>
  static ZuIsFloatingPoint<T, bool> equals_(T t1, T t2) {
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

  template <class Cmp__ = Cmp>
  ZuIfT<!ZuConversion<Cmp__, ZuCmp0<T> >::Same, bool>
  less_(const ZuBox &b) const {
    if (Cmp::null(b.m_val)) return false;
    if (Cmp::null(m_val)) return true;
    return Cmp::less(m_val, b.m_val);
  }
  template <class Cmp__ = Cmp>
  ZuIfT<ZuConversion<Cmp__, ZuCmp0<T> >::Same, bool>
  less_(const ZuBox &b) const {
    return Cmp::less(m_val, b.m_val);
  }
  bool less(const ZuBox &b) const { return less_<>(b); }

  bool operator !() const { return !m_val; }

  bool operator *() const { return !Cmp::null(m_val); }

  uint32_t hash() const { return ZuHash<T>::hash(m_val); }

  operator T() const { return m_val; }
  operator T &() & { return m_val; }

  // compile-time formatting
  template <class Fmt>
  ZuBoxFmt<ZuBox, Fmt> fmt(Fmt) const { 
    return ZuBoxFmt<ZuBox, Fmt>(*this);
  }
  ZuBoxFmt<ZuBox, ZuFmt::Hex<> > hex() const {
    return ZuBoxFmt<ZuBox, ZuFmt::Hex<> >(*this);
  }
  template <class Fmt>
  ZuBoxFmt<ZuBox, ZuFmt::Hex<0, Fmt> > hex(Fmt) const {
    return ZuBoxFmt<ZuBox, ZuFmt::Hex<0, Fmt> >(*this);
  }
  // run-time formatting
  ZuBoxVFmt<ZuBox> vfmt() const {
    return ZuBoxVFmt<ZuBox>{*this};
  }
  template <typename VFmt>
  ZuBoxVFmt<ZuBox> vfmt(VFmt &&fmt) const {
    return ZuBoxVFmt<ZuBox>{*this, ZuFwd<VFmt>(fmt)};
  }

  template <typename S>
  ZuIsCharString<S, unsigned> scan(S &&s_) {
    ZuString s(ZuFwd<S>(s_));
    typename Scan<>::T val = 0;
    unsigned n = Scan<>::scan(val, s.data(), s.length());
    if (ZuUnlikely(!n)) {
      m_val = Cmp::null();
      return 0U;
    }
    m_val = val;
    return n;
  }
  template <class Fmt, typename S>
  ZuIsCharString<S, unsigned> scan(Fmt, S &&s_) {
    ZuString s(ZuFwd<S>(s_));
    typename Scan<Fmt>::T val = 0;
    unsigned n = Scan<Fmt>::scan(val, s.data(), s.length());
    if (ZuUnlikely(!n)) {
      m_val = Cmp::null();
      return 0U;
    }
    m_val = val;
    return n;
  }
  template <typename S>
  ZuBox_IsCharPtr<S, unsigned> scan(S s, unsigned len) {
    typename Scan<>::T val = 0;
    unsigned n = Scan<>::scan(val, s, len);
    if (ZuUnlikely(!n)) {
      m_val = Cmp::null();
      return 0U;
    }
    m_val = val;
    return n;
  }
  template <class Fmt, typename S>
  ZuBox_IsCharPtr<S, unsigned> scan(Fmt, S s, unsigned len) {
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

// carefully constructed to permit unambiguous interop with primitive types
template <typename T, typename Cmp>
inline bool operator ==(const ZuBox<T, Cmp> &l, const ZuBox<T, Cmp> &r) {
  return l.equals(r);
}
template <typename L, typename T, typename Cmp>
inline bool operator ==(const L &l, const ZuBox<T, Cmp> &r) {
  return ZuBox<T, Cmp>{l}.equals(r);
}
template <typename T, typename Cmp>
inline bool operator <(const ZuBox<T, Cmp> &l, const ZuBox<T, Cmp> &r) {
  return l.less(r);
}
template <typename L, typename T, typename Cmp>
inline bool operator <(const L &l, const ZuBox<T, Cmp> &r) {
  return ZuBox<T, Cmp>{l}.less(r);
}
template <typename T, typename Cmp>
inline bool operator >=(const ZuBox<T, Cmp> &l, const ZuBox<T, Cmp> &r) {
  return !l.less(r);
}
template <typename L, typename T, typename Cmp>
inline bool operator >=(const L &l, const ZuBox<T, Cmp> &r) {
  return !ZuBox<T, Cmp>{l}.less(r);
}
template <typename T, typename Cmp>
inline bool operator >(const ZuBox<T, Cmp> &l, const ZuBox<T, Cmp> &r) {
  return r.less(l);
}
template <typename L, typename T, typename Cmp>
inline bool operator >(const L &l, const ZuBox<T, Cmp> &r) {
  return ZuBox<T, Cmp>{r}.less(l);
}
template <typename T, typename Cmp>
inline bool operator <=(const ZuBox<T, Cmp> &l, const ZuBox<T, Cmp> &r) {
  return !r.less(l);
}
template <typename L, typename T, typename Cmp>
inline bool operator <=(const L &l, const ZuBox<T, Cmp> &r) {
  return !ZuBox<T, Cmp>{r}.less(l);
}

#define ZuBox0(T) ZuBox<T, ZuCmp0<T> >
#define ZuBox_1(T) ZuBox<T, ZuCmp_1<T> >
#define ZuBoxN(T, N) ZuBox<T, ZuCmpN<T, N> >

// ZuCmp has to be specialized since null() is otherwise !t (instead of !*t)
template <typename T_, class Cmp>
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
template <typename U, class Cmp>
struct ZuBoxT_<ZuBox<U, Cmp>> { using T = ZuBox<U, Cmp>; };
template <typename U>
using ZuBoxT = typename ZuBoxT_<U>::T;

// ZuBoxed(t) - convenience function to cast primitives to boxed
template <typename T>
const ZuIsBoxed<T, T> &ZuBoxed(const T &t) { return t; }
template <typename T>
ZuIsBoxed<T, T> &ZuBoxed(T &t) { return t; }
template <typename T>
const ZuNotBoxed<T, ZuBox<T> > &ZuBoxed(const T &t) {
  const ZuBox<T> *ZuMayAlias(t_) = reinterpret_cast<const ZuBox<T> *>(&t);
  return *t_;
}
template <typename T>
ZuNotBoxed<T, ZuBox<T> > &ZuBoxed(T &t) {
  ZuBox<T> *ZuMayAlias(t_) = reinterpret_cast<ZuBox<T> *>(&t);
  return *t_;
}

// ZuBoxPtr(t) - convenience function to box pointers as uintptr_t
#define ZuBoxPtr(x) \
  (ZuBox<uintptr_t, ZuCmp0<uintptr_t> >{reinterpret_cast<uintptr_t>(x)})

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZuBox_HPP */
