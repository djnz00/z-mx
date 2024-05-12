//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// nanosecond precision time class
// - essentially a C++ wrapper for POSIX timespec
// - no attempt is made to disambiguate a time interval from an absolute time

#ifndef ZuTime_HH
#define ZuTime_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuInt.hh>
#include <zlib/ZuTraits.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuHash.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuBox.hh>

#include <time.h>

#ifdef _WIN32
#define ZuTime_FT_Epoch	0x019db1ded53e8000ULL	// 00:00:00 Jan 1 1970

#ifndef _TIMESPEC_DEFINED
#define _TIMESPEC_DEFINED
struct timespec {
  time_t  tv_sec;
  long    tv_nsec;
};
#endif
#endif

class ZuAPI ZuTime : public timespec {
  using ldouble = long double;

public:
  enum Nano_ { Nano };		// ''

  constexpr ZuTime() : timespec{ZuCmp<time_t>::null(), 0} { }

  constexpr ZuTime(const ZuTime &t) : timespec{t.tv_sec, t.tv_nsec} { }
  constexpr ZuTime &operator =(const ZuTime &t) {
    if (this == &t) return *this;
    tv_sec = t.tv_sec, tv_nsec = t.tv_nsec;
    return *this;
  }

  constexpr ~ZuTime() { }

  template <typename T, typename U = ZuStrip<T>>
  struct IsInt : public ZuBool<
      ZuIsExact<int, U>{} ||
      ZuIsExact<unsigned, U>{} ||
      ZuIsExact<long, U>{} ||
      ZuIsExact<time_t, U>{}> { };
  template <typename T, typename R = void>
  using MatchInt = ZuIfT<IsInt<T>{}, R>;

  template <typename T>
  constexpr ZuTime(T v, MatchInt<T> *_ = nullptr) : timespec{v, 0} { }
  constexpr ZuTime(time_t t, long n) : timespec{t, n} { }

  constexpr ZuTime(ldouble d) :
    timespec{time_t(d), long((d - ldouble(time_t(d))) * 1000000000)} { }

  constexpr ZuTime(Nano_, int128_t nano) :
    timespec{time_t(nano / 1000000000), long(nano % 1000000000)} { }

#ifndef _WIN32
  constexpr ZuTime(const timespec &t) : timespec{t.tv_sec, t.tv_nsec} { }
  ZuTime &operator =(const timespec &t) {
    new (this) ZuTime{t};
    return *this;
  }
  constexpr ZuTime(const timeval &t) : timespec{t.tv_sec, t.tv_usec * 1000} { }
  ZuTime &operator =(const timeval &t) {
    new (this) ZuTime{t};
    return *this;
  }
#else
  ZuTime(FILETIME f) {
    int64_t *ZuMayAlias(f_) = reinterpret_cast<int64_t *>(&f);
    int64_t t = *f_;
    t -= ZuTime_FT_Epoch;
    tv_sec = t / 10000000;
    tv_nsec = (int32_t)((t % 10000000) * 100);
  }
  ZuTime &operator =(FILETIME t) {
    new (this) ZuTime{t};
    return *this;
  }
#endif

  template <typename S>
  ZuTime(const S &s, ZuMatchString<S> *_ = nullptr) { scan(s); }

  constexpr time_t as_time_t() const { return(tv_sec); }
  constexpr ldouble as_ldouble() const {
    if (ZuUnlikely(!*this)) return ZuCmp<ldouble>::null();
    return ldouble(tv_sec) + ldouble(tv_nsec) / 1000000000;
  }
#ifndef _WIN32
  timeval as_timeval() const {
    return timeval{tv_sec, tv_nsec / 1000};
  }
#else
  FILETIME as_FILETIME() const {
    FILETIME f;
    int64_t *ZuMayAlias(f_) = reinterpret_cast<int64_t *>(&f);
    *f_ = (int64_t)tv_sec * 10000000 + tv_nsec / 100 + ZuTime_FT_Epoch;
    return f;
  }
#endif

  constexpr int64_t millisecs() const {
    return int64_t(tv_sec) * 1000 + tv_nsec / 1000000;
  }
  constexpr int64_t microsecs() const {
    return int64_t(tv_sec) * 1000000 + tv_nsec / 1000;
  }
  constexpr int128_t nanosecs() const {
    return int128_t(tv_sec) * 1000000000 + tv_nsec;
  }

  constexpr ZuTime &operator =(time_t t) {
    tv_sec = t;
    tv_nsec = 0;
    return *this;
  }
  constexpr ZuTime &operator =(ldouble d) {
    tv_sec = d;
    tv_nsec = (d - ldouble(tv_sec)) * 1000000000;
    return *this;
  }

  constexpr void normalize() {
    if (tv_nsec >= 1000000000) tv_nsec -= 1000000000, ++tv_sec;
    else {
      if (tv_nsec < 0) {
	tv_nsec += 1000000000, --tv_sec;
	if (ZuUnlikely(tv_nsec < 0)) tv_nsec += 1000000000, --tv_sec;
      }
    }
  }

  constexpr ZuTime operator -() const {
    return ZuTime{-tv_sec - 1, 1000000000L - tv_nsec};
  }

  template <typename T>
  constexpr MatchInt<T, ZuTime> operator +(T v) const {
    return ZuTime{tv_sec + v, tv_nsec};
  }
  constexpr ZuTime operator +(ldouble d) const {
    return ZuTime::operator +(ZuTime{d});
  }
  constexpr ZuTime operator +(const ZuTime &t_) const {
    ZuTime t{tv_sec + t_.tv_sec, tv_nsec + t_.tv_nsec};
    t.normalize();
    return t;
  }
  template <typename T>
  constexpr MatchInt<T, ZuTime &> operator +=(T v) {
    tv_sec += v;
    return *this;
  }
  constexpr ZuTime &operator +=(ldouble d) {
    return ZuTime::operator +=(ZuTime{d});
  }
  constexpr ZuTime &operator +=(const ZuTime &t_) {
    tv_sec += t_.tv_sec, tv_nsec += t_.tv_nsec;
    normalize();
    return *this;
  }
  template <typename T>
  constexpr MatchInt<T, ZuTime> operator -(T v) const {
    return ZuTime{tv_sec - v, tv_nsec};
  }
  constexpr ZuTime operator -(ldouble d) const {
    return ZuTime::operator -(ZuTime{d});
  }
  constexpr ZuTime operator -(const ZuTime &t_) const {
    ZuTime t{tv_sec - t_.tv_sec, tv_nsec - t_.tv_nsec};
    t.normalize();
    return t;
  }
  template <typename T>
  constexpr MatchInt<T, ZuTime &> operator -=(T v) {
    tv_sec -= v;
    return *this;
  }
  constexpr ZuTime &operator -=(ldouble d) {
    return ZuTime::operator -=(ZuTime{d});
  }
  constexpr ZuTime &operator -=(const ZuTime &t_) {
    tv_sec -= t_.tv_sec, tv_nsec -= t_.tv_nsec;
    normalize();
    return *this;
  }

  constexpr ZuTime operator *(ldouble d) {
    return ZuTime{as_ldouble() * d};
  }
  constexpr ZuTime &operator *=(ldouble d) {
    return operator =(as_ldouble() * d);
  }
  constexpr ZuTime operator /(ldouble d) {
    return ZuTime{as_ldouble() / d};
  }
  constexpr ZuTime &operator /=(ldouble d) {
    return operator =(as_ldouble() / d);
  }

  constexpr bool equals(const ZuTime &t) const {
    return tv_sec == t.tv_sec && tv_nsec == t.tv_nsec;
  }
  constexpr int cmp(const ZuTime &t) const {
    if (int i = ZuCmp<time_t>::cmp(tv_sec, t.tv_sec)) return i;
    return ZuCmp<long>::cmp(tv_nsec, t.tv_nsec);
  }
  friend inline constexpr bool operator ==(const ZuTime &l, const ZuTime &r) {
    return l.equals(r);
  }
  friend inline constexpr int operator <=>(const ZuTime &l, const ZuTime &r) {
    return l.cmp(r);
  }

  constexpr bool operator *() const {
    return !ZuCmp<time_t>::null(tv_sec);
  }
  constexpr bool operator !() const {
    return !tv_sec && !tv_nsec;
  }
  constexpr operator bool() const {
    return tv_sec || tv_nsec;
  }

  time_t sec() const { return tv_sec; }
  time_t &sec() { return tv_sec; }
  long nsec() const { return tv_nsec; }
  long &nsec() { return tv_nsec; }

  uint32_t hash() const {
    return ZuHash<time_t>::hash(tv_sec) ^ ZuHash<long>::hash(tv_nsec);
  }

  // CSV format scan/print
  unsigned scan(ZuString);

  void ymdhmsn(
    int &year, int &month, int &day,
    int &hour, int &minute, int &sec, int &nsec) const
  {
    {
      int julian;
      int i, j, l, n;

      julian = static_cast<int>((tv_sec / 86400) + 2440588);
      sec = static_cast<int>(tv_sec % 86400);

      l = julian + 68569;
      n = (l<<2) / 146097;
      l = l - ((146097 * n + 3)>>2);
      i = (4000 * (l + 1)) / 1461001;
      l = l - ((1461 * i)>>2) + 31;
      j = (80 * l) / 2447;
      day = l - (2447 * j) / 80;
      l = j / 11;
      month = j + 2 - 12 * l;
      year = (100 * (n - 49) + i + l);
      hour = sec / 3600, sec %= 3600,
      minute = sec / 60, sec %= 60;
    }
    nsec = tv_nsec;
  }

  template <typename S> void print(S &s) const {
    if (!**this) return;
    int year, month, day, hour, minute, sec, nsec;
    ymdhmsn(year, month, day, hour, minute, sec, nsec);
    if (year < 0) { s << '-'; year = -year; }
    s <<
      ZuBoxed(year).fmt<ZuFmt::Right<4>>() << '/' <<
      ZuBoxed(month).fmt<ZuFmt::Right<2>>() << '/' <<
      ZuBoxed(day).fmt<ZuFmt::Right<2>>() << ' ' <<
      ZuBoxed(hour).fmt<ZuFmt::Right<2>>() << ':' <<
      ZuBoxed(minute).fmt<ZuFmt::Right<2>>() << ':' <<
      ZuBoxed(sec).fmt<ZuFmt::Right<2>>() << '.' <<
      ZuBoxed(nsec).fmt<ZuFmt::Frac<9>>();
  }
  friend ZuPrintFn ZuPrintType(ZuTime *);

  // print as interval
  struct Interval {
    const ZuTime &time;
    template <typename S> void print(S &s) const {
      if (!time) return;
      s << ZuBoxed(time.tv_sec) << '.' <<
	ZuBoxed(time.tv_nsec).fmt<ZuFmt::Frac<9>>();
    }
    friend ZuPrintFn ZuPrintType(Interval *);
  };
  Interval interval() const { return {*this}; }

  // traits
  struct Traits : public ZuBaseTraits<ZuTime> { enum { IsPOD = 1 }; };
  friend Traits ZuTraitsType(ZuTime *);
};

#endif /* ZuTime_HH */
