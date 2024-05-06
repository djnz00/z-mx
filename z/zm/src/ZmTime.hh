//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// nanosecond precision time class

#ifndef ZmTime_HH
#define ZmTime_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <zlib/ZuInt.hh>
#include <zlib/ZuTraits.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuHash.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuBox.hh>

#include <zlib/ZmPlatform.hh>

#include <time.h>

#ifdef _WIN32
#define ZmTime_FT_Epoch	0x019db1ded53e8000ULL	// 00:00:00 Jan 1 1970

#ifndef _TIMESPEC_DEFINED
#define _TIMESPEC_DEFINED
struct timespec {
  time_t  tv_sec;
  long    tv_nsec;
};
#endif
#endif

class ZmAPI ZmTime : public timespec {
public:
  enum Now_ { Now };		// disambiguator
  enum Nano_ { Nano };		// ''

  constexpr ZmTime() : timespec{ZuCmp<time_t>::null(), 0} { }

  constexpr ZmTime(const ZmTime &t) : timespec{t.tv_sec, t.tv_nsec} { }
  constexpr ZmTime &operator =(const ZmTime &t) {
    if (this == &t) return *this;
    tv_sec = t.tv_sec, tv_nsec = t.tv_nsec;
    return *this;
  }

  constexpr ~ZmTime() { }

  template <typename T> struct IsInt : public ZuBool<
      ZuInspect<int, T>::Same ||
      ZuInspect<unsigned, T>::Same ||
      ZuInspect<long, T>::Same ||
      ZuInspect<time_t, T>::Same> { };
  template <typename T, typename R = void>
  using MatchInt = ZuIfT<IsInt<T>{}, R>;

  ZmTime(Now_) { now(); }
  template <typename T>
  ZmTime(Now_, T i, MatchInt<T> *_ = nullptr) {
    now(i);
  }
  ZmTime(Now_, double d) { now(d); }
  ZmTime(Now_, const ZmTime &d) { now(d); }

  template <typename T>
  constexpr ZmTime(T v, MatchInt<T> *_ = nullptr) : timespec{v, 0} { }
  constexpr ZmTime(time_t t, long n) : timespec{t, n} { }

  constexpr ZmTime(double d) :
    timespec{
      static_cast<time_t>(d),
      static_cast<long>(
	  (d - static_cast<double>(static_cast<time_t>(d))) * 1000000000)} { }

  constexpr ZmTime(Nano_, int64_t nano) : 
    timespec{
      static_cast<time_t>(nano / 1000000000),
      static_cast<long>(nano % 1000000000)} { }

#ifndef _WIN32
  constexpr ZmTime(const timespec &t) : timespec{t.tv_sec, t.tv_nsec} { }
  ZmTime &operator =(const timespec &t) {
    new (this) ZmTime{t};
    return *this;
  }
  constexpr ZmTime(const timeval &t) : timespec{t.tv_sec, t.tv_usec * 1000} { }
  ZmTime &operator =(const timeval &t) {
    new (this) ZmTime{t};
    return *this;
  }
#else
  ZmTime(FILETIME f) {
    int64_t *ZuMayAlias(f_) = reinterpret_cast<int64_t *>(&f);
    int64_t t = *f_;
    t -= ZmTime_FT_Epoch;
    tv_sec = t / 10000000;
    tv_nsec = (int32_t)((t % 10000000) * 100);
  }
  ZmTime &operator =(FILETIME t) {
    new (this) ZmTime{t};
    return *this;
  }
#endif

#ifndef _WIN32
  ZmTime &now() {
    clock_gettime(CLOCK_REALTIME, this);
    return *this;
  }
#else
  ZmTime &now();

  static uint64_t cpuFreq();
#endif /* !_WIN32 */

  template <typename T>
  MatchInt<T, ZmTime &> now(T i) { return now() += i; }
  ZmTime &now(double d) { return now() += d; }
  ZmTime &now(const ZmTime &d) { return now() += d; }

  constexpr time_t time() const { return(tv_sec); }
  constexpr operator time_t() const { return(tv_sec); }
  constexpr double dtime() const {
    if (ZuUnlikely(!*this)) return ZuCmp<double>::null();
    return (double)tv_sec + (double)tv_nsec / (double)1000000000;
  }
  constexpr int32_t millisecs() const {
    return (int32_t)(tv_sec * 1000 + tv_nsec / 1000000);
  }
  constexpr int32_t microsecs() const {
    return (int32_t)(tv_sec * 1000000 + tv_nsec / 1000);
  }
  constexpr int64_t nanosecs() const {
    return (int64_t)tv_sec * 1000000000 + (int64_t)tv_nsec;
  }

#ifndef _WIN32
  operator timeval() const {
    return timeval{tv_sec, tv_nsec / 1000};
  }
#else
  operator FILETIME() const {
    FILETIME f;
    int64_t *ZuMayAlias(f_) = reinterpret_cast<int64_t *>(&f);
    *f_ = (int64_t)tv_sec * 10000000 + tv_nsec / 100 + ZmTime_FT_Epoch;
    return f;
  }
#endif

  constexpr ZmTime &operator =(time_t t) {
    tv_sec = t;
    tv_nsec = 0;
    return *this;
  }
  constexpr ZmTime &operator =(double d) {
    tv_sec = (time_t)d;
    tv_nsec = (long)((d - (double)tv_sec) * (double)1000000000);
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

  constexpr ZmTime operator -() const {
    return ZmTime{-tv_sec - 1, 1000000000L - tv_nsec};
  }

  template <typename T>
  constexpr MatchInt<T, ZmTime> operator +(T v) const {
    return ZmTime{tv_sec + v, tv_nsec};
  }
  constexpr ZmTime operator +(double d) const {
    return ZmTime::operator +(ZmTime{d});
  }
  constexpr ZmTime operator +(const ZmTime &t_) const {
    ZmTime t{tv_sec + t_.tv_sec, tv_nsec + t_.tv_nsec};
    t.normalize();
    return t;
  }
  template <typename T>
  constexpr MatchInt<T, ZmTime &> operator +=(T v) {
    tv_sec += v;
    return *this;
  }
  constexpr ZmTime &operator +=(double d) {
    return ZmTime::operator +=(ZmTime{d});
  }
  constexpr ZmTime &operator +=(const ZmTime &t_) {
    tv_sec += t_.tv_sec, tv_nsec += t_.tv_nsec;
    normalize();
    return *this;
  }
  template <typename T>
  constexpr MatchInt<T, ZmTime> operator -(T v) const {
    return ZmTime{tv_sec - v, tv_nsec};
  }
  constexpr ZmTime operator -(double d) const {
    return ZmTime::operator -(ZmTime{d});
  }
  constexpr ZmTime operator -(const ZmTime &t_) const {
    ZmTime t{tv_sec - t_.tv_sec, tv_nsec - t_.tv_nsec};
    t.normalize();
    return t;
  }
  template <typename T>
  constexpr MatchInt<T, ZmTime &> operator -=(T v) {
    tv_sec -= v;
    return *this;
  }
  constexpr ZmTime &operator -=(double d) {
    return ZmTime::operator -=(ZmTime{d});
  }
  constexpr ZmTime &operator -=(const ZmTime &t_) {
    tv_sec -= t_.tv_sec, tv_nsec -= t_.tv_nsec;
    normalize();
    return *this;
  }

  constexpr ZmTime operator *(double d) {
    return ZmTime{dtime() * d};
  }
  constexpr ZmTime &operator *=(double d) {
    return operator =(dtime() * d);
  }
  constexpr ZmTime operator /(double d) {
    return ZmTime{dtime() / d};
  }
  constexpr ZmTime &operator /=(double d) {
    return operator =(dtime() / d);
  }

  constexpr bool equals(const ZmTime &t) const {
    return tv_sec == t.tv_sec && tv_nsec == t.tv_nsec;
  }
  constexpr int cmp(const ZmTime &t) const {
    if (int i = ZuCmp<time_t>::cmp(tv_sec, t.tv_sec)) return i;
    return ZuCmp<long>::cmp(tv_nsec, t.tv_nsec);
  }
  friend inline constexpr bool operator ==(const ZmTime &l, const ZmTime &r) {
    return l.equals(r);
  }
  friend inline constexpr int operator <=>(const ZmTime &l, const ZmTime &r) {
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

  struct Traits : public ZuBaseTraits<ZmTime> { enum { IsPOD = 1 }; };
  friend Traits ZuTraitsType(ZmTime *);

  template <typename S> void print(S &s) const {
    if (!*this) return;
    int year, month, day, hour, minute, sec;
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
    s <<
      ZuBoxed(year).fmt<ZuFmt::Right<4>>() << '/' <<
      ZuBoxed(month).fmt<ZuFmt::Right<2>>() << '/' <<
      ZuBoxed(day).fmt<ZuFmt::Right<2>>() << ' ' <<
      ZuBoxed(hour).fmt<ZuFmt::Right<2>>() << ':' <<
      ZuBoxed(minute).fmt<ZuFmt::Right<2>>() << ':' <<
      ZuBoxed(sec).fmt<ZuFmt::Right<2>>() << '.' <<
      ZuBoxed(tv_nsec).fmt<ZuFmt::Frac<9>>();
  }
  friend ZuPrintFn ZuPrintType(ZmTime *);

  // print as interval
  struct Interval {
    const ZmTime &time;
    template <typename S> void print(S &s) const {
      if (!time) return;
      s << ZuBoxed(time.tv_sec) << '.' <<
	ZuBoxed(time.tv_nsec).fmt<ZuFmt::Frac<9>>();
    }
    friend ZuPrintFn ZuPrintType(Interval *);
  };
  Interval interval() const { return {*this}; }
};

inline ZmTime ZmTimeNow() { return ZmTime{ZmTime::Now}; }
template <typename T>
inline typename ZmTime::MatchInt<T, ZmTime>::T ZmTimeNow(T i) {
  return ZmTime{ZmTime::Now, i};
}
inline ZmTime ZmTimeNow(double d) { return ZmTime{ZmTime::Now, d}; }
inline ZmTime ZmTimeNow(const ZmTime &d) { return ZmTime{ZmTime::Now, d}; }

#endif /* ZmTime_HH */
