//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// Julian (serial) date based date/time class - JD(UT1) in IAU nomenclature

// * handles dates from Jan 1st 4713BC to Oct 14th 1465002AD
//   Note: 4713BC is -4712 when printed in ISO8601 format, since
//   historians do not include a 0 year (1BC immediately precedes 1AD)

// * date/times are internally represented as a Julian date with
//   intraday time in POSIX time_t seconds (i.e. UT1 seconds)
//   from midnight in the GMT timezone, and a number of nanoseconds

// * intraday timespan of each day is midnight-to-midnight instead
//   of the historical / astronomical noon-to-noon convention for
//   Julian dates

// * year/month/day conversions account for the reformation of
//   15th October 1582 and the subsequent adoption of the Gregorian
//   calendar; the default effective date is 14th September 1752
//   when Britain and the American colonies adopted the Gregorian
//   calendar by Act of Parliament (consistent with Unix cal); this
//   can be changed by calling ZtDate::reformation(year, month, day)

// * UTC time is composed of atomic time seconds which are of fixed
//   duration; leap seconds have been intermittently added to UTC time
//   since 1972 to correct for perturbations in the Earth's rotation;
//   ZtDate is based on POSIX time_t which is UT1 time, defined
//   directly in terms of the Earth's rotation and composed of
//   seconds which have slightly variable duration; all POSIX times
//   driven by typical computer clocks therefore require occasional
//   adjustment due to the difference between UTC and UT1 - see
//   http://www.boulder.nist.gov/timefreq/pubs/bulletin/leapsecond.htm -
//   this is typically done by running an NTP client on the system;
//   when performing calculations involving time values that are exchanged
//   with external systems requiring accuracy to the atomic second (such
//   as astronomical systems, etc.), the caller may need to compensate for
//   the difference between atomic time (UTC) and POSIX time_t (UT1):
//
//   * when repeatedly adding/subtracting atomic time values to advance or
//     reverse a POSIX time, it is possible to accumulate more than one
//     second of error due to the accumulated difference between UTC and
//     UT1; to compensate, the caller should add/remove a leap second when
//     the POSIX time crosses the midnight that a leap second was applied
//     (typically applied by NTP software)
//
//   * when calculating the time interval between two absolute POSIX times,
//     leap seconds that were applied to UTC for the intervening period
//     should be added/removed, to get an accurate interval

// * conversion functions accept an arbitrary intraday offset in seconds
//   for adjustment between GMT and another time zone; for local time
//   caller should use the value returned by offset() which works for
//   the entire date range supported by ZtDate and performs DST and
//   other adjustments according to the default timezone in effect or
//   the timezone specified by the caller; timezone names are the same as
//   the values that can be assigned to the TZ environment variable:
//
//     ZtDate date;		// date/time
//
//     date.now();
//
//     ZtDateFmt::ISO gmt;			// GMT/UTC
//     ZtDateFmt::ISO local(date.offset());	// default local time (tzset())
//     ZtDateFmt::ISO gb(date.offset("GB"));	// local time in UK
//     ZtDateFmt::ISO est(date.offset("EST"));	// local time in New York
//
//     std::cout << date.iso(gmt);	// print time in ISO8601 format
//
// * WARNING - functions with this annotation call tzset() if the tz argument
//   is non-zero; these functions should not be called with a high frequency
//   by high performance applications with a non-zero tz since they:
//   - acquire and release a global lock, since tzset() is not thread-safe
//   - are potentially time-consuming
//   - may access and modify global environment variables
//   - may access system configuration files and external timezone databases

#ifndef ZtDate_HH
#define ZtDate_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZtLib_HH
#include <zlib/ZtLib.hh>
#endif

#include <time.h>
#include <limits.h>

#include <zlib/ZuTraits.hh>
#include <zlib/ZuString.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuString.hh>
#include <zlib/ZuStringN.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuBox.hh>

#include <zlib/ZmLock.hh>
#include <zlib/ZmGuard.hh>
#include <zlib/ZmTime.hh>
#include <zlib/ZmSpecific.hh>

#include <zlib/ZtString.hh>

#define ZtDate_MaxJulian	(0x1fffffff - 68572)

class ZtDate;

template <int size> class ZtDate_time_t;
template <> class ZtDate_time_t<4> {
friend ZtDate;
private:
  constexpr static int32_t minimum() { return -0x80000000; } 
  constexpr static int32_t maximum() { return 0x7fffffff; } 
public:
  static bool isMinimum(int32_t t) { return t == minimum(); }
  static bool isMaximum(int32_t t) { return t == maximum(); }
  static time_t time(int32_t julian, int second) {
    if (ZuCmp<int32_t>::null(julian)) return ZuCmp<time_t>::null();
    if (julian < 2415732) {
      return minimum();
    } else if (julian == 2415732) {
      if (second < 74752) {
	return minimum();
      }
      julian++, second -= 86400;
    } else if (julian > 2465443 || (julian == 2465443 && second > 11647)) {
      return maximum();
    }
    return (julian - 2440588) * 86400 + second;
  }
};
template <> class ZtDate_time_t<8> {
public:
  constexpr static bool isMinimum(int64_t) { return false; }
  constexpr static bool isMaximum(int64_t) { return false; }
  static time_t time(int32_t julian, int second) {
    if (ZuCmp<int32_t>::null(julian)) return ZuCmp<time_t>::null();
    return ((int64_t)julian - (int64_t)2440588) * (int64_t)86400 +
      (int64_t)second;
  }
};

// compile-time date/time input formatting
namespace ZtDateScan {
  struct CSV {
    const char	*tzName = nullptr;	// optional timezone
    int		tzOffset = 0;
  };
  struct FIX { };
  struct ISO {
    const char	*tzName = nullptr;	// optional timezone
    int		tzOffset = 0;
  };

  ZuDeclUnion(Any,
      (CSV, csv),
      (FIX, fix),
      (ISO, iso));
}

// compile-time date/time output formatting
namespace ZtDateFmt {
  class CSV {
  friend ::ZtDate;
  public:
    CSV(int offset = 0) : m_offset{offset} {
      memcpy(m_yyyymmdd, "0001/01/01", 10);
      memcpy(m_hhmmss, "00:00:00", 8);
    }

    void offset(int o) { m_offset = o; }
    int offset() const { return m_offset; }

    void pad(char c) { m_pad = c; }
    char pad() const { return m_pad; }

  private:
    int			m_offset;
    char		m_pad = 0;

    mutable int		m_julian = 0;
    mutable int		m_sec = 0;
    mutable char	m_yyyymmdd[10];
    mutable char	m_hhmmss[8];
  };

  template <unsigned Width, char Trim> struct FIX_ {
    template <typename S> static void frac_print(S &s, unsigned nsec) {
      s << '.' << ZuBoxed(nsec).fmt(ZuFmt::Frac<Width, Trim>());
    }
  };
  template <unsigned Width> struct FIX_<Width, '\0'> {
    template <typename S> static void frac_print(S &s, unsigned nsec) {
      if (ZuLikely(nsec))
	s << '.' << ZuBoxed(nsec).fmt<ZuFmt::Frac<Width, '\0'>>();
    }
  };
  template <char Trim> struct FIX_<0, Trim> {
    template <typename S> static void frac_print(S &, unsigned) { }
  };
  template <int Exp_, class Null_ = ZuPrintNull>
  class FIX :
      public FIX_<(Exp_ < 0 ? -Exp_ : Exp_), (Exp_ < 0 ? '\0' : '0')> {
  friend ::ZtDate;
  public:
    enum { Exp = Exp_ };
    using Null = Null_;

    ZuAssert(Exp >= -9 && Exp <= 9);

    FIX() {
      memcpy(m_yyyymmdd, "00010101", 8);
      memcpy(m_hhmmss, "00:00:00", 8);
    }

  private:
    mutable int		m_julian = 0;
    mutable int		m_sec = 0;
    mutable char	m_yyyymmdd[8];
    mutable char	m_hhmmss[8];
  };

  class ISO {
  friend ::ZtDate;
  public:
    ISO(int offset = 0) : m_offset{offset} {
      memcpy(m_yyyymmdd, "0001-01-01", 10);
      memcpy(m_hhmmss, "00:00:00", 8);
    }

    void offset(int o) { m_offset = o; }
    int offset() const { return m_offset; }

  private:
    int			m_offset = 0;

    mutable int		m_julian = 0;
    mutable int		m_sec = 0;
    mutable char	m_yyyymmdd[10];
    mutable char	m_hhmmss[8];
  };

  struct Strftime {
    const char		*format;
    int			offset;
  };

  // run-time variable date/time formatting
  struct FIXDeflt_Null : public ZuPrintable {
    template <typename S> void print(S &) const { }
  };
  using FIXDeflt = FIX<-9, FIXDeflt_Null>;
  ZuDeclUnion(Any,
      (CSV, csv),
      (FIXDeflt, fix),
      (ISO, iso),
      (Strftime, strftime));
}

struct ZtDatePrintCSV {
  const ZtDate				&value;
  const ZtDateFmt::CSV			&fmt;

  template <typename S> void print(S &) const;
  friend ZuPrintFn ZuPrintType(ZtDatePrintCSV *);
};
template <int Exp = -9, class Null = ZtDateFmt::FIXDeflt_Null>
struct ZtDatePrintFIX {
  const ZtDate				&value;
  const ZtDateFmt::FIX<Exp, Null>	&fmt;

  template <typename S> void print(S &) const;
  friend ZuPrintFn ZuPrintType(ZtDatePrintFIX *);
};
struct ZtDatePrintISO {
  const ZtDate				&value;
  const ZtDateFmt::ISO			&fmt;

  template <typename S> void print(S &) const;
  friend ZuPrintFn ZuPrintType(ZtDatePrintISO *);
};
struct ZtDatePrintStrftime {
  const ZtDate				&value;
  ZtDateFmt::Strftime			fmt;

  template <typename S> void print(S &) const;
  void print(ZmStream &) const;
  friend ZuPrintFn ZuPrintType(ZtDatePrintStrftime *);
};
struct ZtDatePrint {
  const ZtDate				&value;
  const ZtDateFmt::Any			&fmt;

  template <typename S> void print(S &) const;
  friend ZuPrintFn ZuPrintType(ZtDatePrint *);
};

class ZtAPI ZtDate {
  using Native = ZtDate_time_t<sizeof(time_t)>;

public:
  enum Now_ { Now };		// disambiguator
  enum Julian_ { Julian };	// ''

  ZtDate() = default;

  ZtDate(const ZtDate &date) :
    m_julian{date.m_julian}, m_sec{date.m_sec}, m_nsec{date.m_nsec} { }

  ZtDate &operator =(const ZtDate &date) {
    if (ZuUnlikely(this == &date)) return *this;
    m_julian = date.m_julian;
    m_sec = date.m_sec;
    m_nsec = date.m_nsec;
    return *this;
  }

  void update(const ZtDate &date) {
    if (ZuUnlikely(this == &date) || !date) return;
    m_julian = date.m_julian;
    m_sec = date.m_sec;
    m_nsec = date.m_nsec;
  }

  ZtDate(Now_ _) { now(); }

  // ZmTime

  template <typename T>
  ZtDate(const T &t, ZuExact<ZmTime, T> *_ = nullptr) {
    init(t.sec()), m_nsec = t.nsec();
  }
  template <typename T>
  ZuExact<ZmTime, T, ZtDate &> operator =(const T &t) {
    init(t.sec());
    m_nsec = t.nsec();
    return *this;
  }

  // time_t

  template <typename T>
  ZtDate(const T &t, ZuExact<time_t, T> *_ = nullptr) {
    init(t), m_nsec = 0;
  }
  template <typename T>
  ZuExact<time_t, T, ZtDate &> operator =(const T &t) {
    init(t);
    m_nsec = 0;
    return *this;
  }

  // double

  template <typename T>
  ZtDate(T d, ZuExact<double, T> *_ = nullptr) {
    time_t t = static_cast<time_t>(d);
    init(t);
    m_nsec = static_cast<int>((d - static_cast<double>(t)) * 1000000000.0);
  }
  template <typename T>
  ZuExact<double, T, ZtDate &> operator =(T d) {
    time_t t = static_cast<time_t>(d);
    init(t);
    m_nsec = static_cast<int>((d - static_cast<double>(t)) * 1000000000.0);
    return *this;
  }

  // struct tm

  ZtDate(const struct tm *tm_) {
    int year = tm_->tm_year + 1900, month = tm_->tm_mon + 1, day = tm_->tm_mday;
    int hour = tm_->tm_hour, minute = tm_->tm_min, sec = tm_->tm_sec;
    int nsec = 0;
    normalize(year, month);
    normalize(day, hour, minute, sec, nsec);
    ctor(year, month, day, hour, minute, sec, nsec);
  }
  ZtDate &operator =(const struct tm *tm_) {
    // this->~ZtDate();
    new (this) ZtDate(tm_);
    return *this;
  }

  // multiple parameter constructors

  ZtDate(time_t t, int nsec) { init(t); m_nsec = nsec; }

  explicit ZtDate(Julian_ _, int julian, int sec, int nsec) :
    m_julian(julian), m_sec(sec), m_nsec(nsec) { }

  ZtDate(int year, int month, int day) {
    normalize(year, month);
    ctor(year, month, day);
  }
  ZtDate(int year, int month, int day, int hour, int minute, int sec) {
    normalize(year, month);
    int nsec = 0;
    normalize(day, hour, minute, sec, nsec);
    ctor(year, month, day, hour, minute, sec, nsec);
  }
  ZtDate(
      int year, int month, int day, int hour, int minute, int sec, int nsec) {
    ctor(year, month, day, hour, minute, sec, nsec);
  }
  ZtDate(int hour, int minute, int sec, int nsec) {
    ctor(hour, minute, sec, nsec);
  }

  // the tz parameter in the following ctors can be 0/nullptr for GMT, "" for
  // the local timezone (as obtained by tzset()), or a specific timezone
  // name - the passed in date/time is converted from the date/time
  // in the specified timezone to the internal GMT representation

  ZtDate(struct tm *tm_, const char *tz) { // WARNING
    int year = tm_->tm_year + 1900, month = tm_->tm_mon + 1, day = tm_->tm_mday;
    int hour = tm_->tm_hour, minute = tm_->tm_min, sec = tm_->tm_sec;
    int nsec = 0;

    normalize(year, month);
    normalize(day, hour, minute, sec, nsec);
    if (tz) offset_(year, month, day, hour, minute, sec, tz);
  }
  ZtDate(int year, int month, int day, const char *tz) { // WARNING
    normalize(year, month);
    ctor(year, month, day);
    if (tz) offset_(year, month, day, 0, 0, 0, tz);
  }
  ZtDate(
      int year, int month, int day,
      int hour, int minute, int sec, const char *tz) { // WARNING
    normalize(year, month);
    int nsec = 0;
    normalize(day, hour, minute, sec, nsec);
    ctor(year, month, day, hour, minute, sec, nsec);
    if (tz) offset_(year, month, day, hour, minute, sec, tz);
  }
  ZtDate(
      int year, int month, int day,
      int hour, int minute, int sec, int nsec, const char *tz) { // WARNING
    ctor(year, month, day, hour, minute, sec, nsec);
    if (tz) offset_(year, month, day, hour, minute, sec, tz);
  }
  ZtDate(int hour, int minute, int sec, int nsec, const char *tz) { // WARNING
    ctor(hour, minute, sec, nsec);
    if (tz) {
      int year, month, day;
      ymd(year, month, day);
      offset_(year, month, day, hour, minute, sec, tz);
    }
  }

  // CSV format: YYYY/MM/DD HH:MM:SS with an optional timezone parameter
  template <typename S> ZtDate(
      const ZtDateScan::CSV &fmt, const S &s, ZuMatchString<S> *_ = nullptr) {
    ctor(fmt, s);
  }

  // FIX format: YYYYMMDD-HH:MM:SS.nnnnnnnnn
  template <typename S> ZtDate(
      const ZtDateScan::FIX &fmt, const S &s, ZuMatchString<S> *_ = nullptr) {
    ctor(fmt, s);
  }

  // the ISO8601 ctor accepts the two standard ISO8601 date/time formats
  // "yyyy-mm-dd" and "yyyy-mm-ddThh:mm:ss[.n]Z", where Z is an optional
  // timezone: "Z" (GMT), "+hhmm", "+hh:mm", "-hhmm", or "-hh:mm";
  // if a timezone is present in the string it overrides any tz
  // parameter passed in by the caller
  template <typename S> ZtDate(
      const ZtDateScan::ISO &fmt, const S &s, ZuMatchString<S> *_ = nullptr) {
    ctor(fmt, s);
  }
  // default to ISO
  template <typename S>
  ZtDate(const S &s, ZuMatchString<S> *_ = nullptr) {
    ctor(ZtDateScan::ISO{}, s);
  }

  template <typename S> ZtDate(
      const ZtDateScan::Any &fmt, const S &s, ZuMatchString<S> *_ = nullptr) {
    ctor(fmt, s);
  }

  // common integral forms

  enum YYYYMMDD_ { YYYYMMDD };
  enum YYMMDD_ { YYMMDD };
  enum HHMMSSmmm_ { HHMMSSmmm };
  enum HHMMSS_ { HHMMSS };

  ZtDate(int year, int month, int day, HHMMSSmmm_, int time) {
    int hour, minute, sec, nsec;
    hour = time / 10000000; minute = (time / 100000) % 100;
    sec = (time / 1000) % 100; nsec = (time % 1000) * 1000000;
    normalize(year, month);
    normalize(day, hour, minute, sec, nsec);
    ctor(year, month, day, hour, minute, sec, nsec);
  }
  ZtDate(int year, int month, int day, HHMMSS_, int time) {
    int hour, minute, sec, nsec = 0;
    hour = time / 10000; minute = (time / 100) % 100; sec = time % 100;
    normalize(year, month);
    normalize(day, hour, minute, sec, nsec);
    ctor(year, month, day, hour, minute, sec, nsec);
  }
  ZtDate(YYYYMMDD_, int date, HHMMSSmmm_, int time) {
    int year, month, day, hour, minute, sec, nsec;
    year = date / 10000; month = (date / 100) % 100; day = date % 100;
    hour = time / 10000000; minute = (time / 100000) % 100;
    sec = (time / 1000) % 100; nsec = (time % 1000) * 1000000;
    normalize(year, month);
    normalize(day, hour, minute, sec, nsec);
    ctor(year, month, day, hour, minute, sec, nsec);
  }
  ZtDate(YYYYMMDD_, int date, HHMMSS_, int time) {
    int year, month, day, hour, minute, sec, nsec = 0;
    year = date / 10000; month = (date / 100) % 100; day = date % 100;
    hour = time / 10000; minute = (time / 100) % 100; sec = time % 100;
    normalize(year, month);
    normalize(day, hour, minute, sec, nsec);
    ctor(year, month, day, hour, minute, sec, nsec);
  }
  ZtDate(YYMMDD_, int date, HHMMSSmmm_, int time) {
    int year, month, day, hour, minute, sec, nsec;
    year = date / 10000; month = (date / 100) % 100; day = date % 100;
    hour = time / 10000000; minute = (time / 100000) % 100;
    sec = (time / 1000) % 100; nsec = (time % 1000) * 1000000;
    year += (year < 70) ? 2000 : 1900;
    normalize(year, month);
    normalize(day, hour, minute, sec, nsec);
    ctor(year, month, day, hour, minute, sec, nsec);
  }
  ZtDate(YYMMDD_, int date, HHMMSS_, int time) {
    int year, month, day, hour, minute, sec, nsec = 0;
    year = date / 10000; month = (date / 100) % 100; day = date % 100;
    hour = time / 10000; minute = (time / 100) % 100; sec = time % 100;
    year += (year < 70) ? 2000 : 1900;
    normalize(year, month);
    normalize(day, hour, minute, sec, nsec);
    ctor(year, month, day, hour, minute, sec, nsec);
  }
  ZtDate(
      YYYYMMDD_, int date, HHMMSSmmm_, int time, const char *tz) { // WARNING
    int year, month, day, hour, minute, sec, nsec;
    year = date / 10000; month = (date / 100) % 100; day = date % 100;
    hour = time / 10000000; minute = (time / 100000) % 100;
    sec = (time / 1000) % 100; nsec = (time % 1000) * 1000000;
    normalize(year, month);
    normalize(day, hour, minute, sec, nsec);
    ctor(year, month, day, hour, minute, sec, nsec);
    if (tz) offset_(year, month, day, hour, minute, sec, tz);
  }
  ZtDate(
      YYYYMMDD_, int date, HHMMSS_, int time, const char *tz) { // WARNING
    int year, month, day, hour, minute, sec, nsec = 0;
    year = date / 10000; month = (date / 100) % 100; day = date % 100;
    hour = time / 10000; minute = (time / 100) % 100; sec = time % 100;
    normalize(year, month);
    normalize(day, hour, minute, sec, nsec);
    ctor(year, month, day, hour, minute, sec, nsec);
    if (tz) offset_(year, month, day, hour, minute, sec, tz);
  }
  ZtDate(
      YYMMDD_, int date, HHMMSSmmm_, int time, const char *tz) { // WARNING
    int year, month, day, hour, minute, sec, nsec;
    year = date / 10000; month = (date / 100) % 100; day = date % 100;
    hour = time / 10000000; minute = (time / 100000) % 100;
    sec = (time / 1000) % 100; nsec = (time % 1000) * 1000000;
    year += (year < 70) ? 2000 : 1900;
    normalize(year, month);
    normalize(day, hour, minute, sec, nsec);
    ctor(year, month, day, hour, minute, sec, nsec);
    if (tz) offset_(year, month, day, hour, minute, sec, tz);
  }
  ZtDate(
      YYMMDD_, int date, HHMMSS_, int time, const char *tz) { // WARNING
    int year, month, day, hour, minute, sec, nsec = 0;
    year = date / 10000; month = (date / 100) % 100; day = date % 100;
    hour = time / 10000; minute = (time / 100) % 100; sec = time % 100;
    year += (year < 70) ? 2000 : 1900;
    normalize(year, month);
    normalize(day, hour, minute, sec, nsec);
    ctor(year, month, day, hour, minute, sec, nsec);
    if (tz) offset_(year, month, day, hour, minute, sec, tz);
  }

  int yyyymmdd() const {
    int year, month, day;
    ymd(year, month, day);
    year *= 10000;
    if (year >= 0) return year + month * 100 + day;
    return year - month * 100 - day;
  }
  int yymmdd() const {
    int year, month, day;
    ymd(year, month, day);
    year = (year % 100) * 10000;
    if (year >= 0) return year + month * 100 + day;
    return year - month * 100 - day;
  }
  int hhmmssmmm() const {
    int hour, minute, sec, nsec;
    hmsn(hour, minute, sec, nsec);
    return hour * 10000000 + minute * 100000 + sec * 1000 + nsec / 1000000;
  }
  int hhmmss() const {
    int hour, minute, sec;
    hms(hour, minute, sec);
    return hour * 10000 + minute * 100 + sec;
  }

// accessors for external persistency providers

  const int &julian() const { return m_julian; }
  int &julian() { return m_julian; }
  const int &sec() const { return m_sec; }
  int &sec() { return m_sec; }
  const int &nsec() const { return m_nsec; }
  int &nsec() { return m_nsec; }

// set effective date of reformation (default 14th September 1752)

  // reformation() is purposely not MT safe since MT safety is an
  // unlikely requirement for a function only intended to be called
  // once during program initialization, MT safety would degrade performance
  // considerably by lock acquisition on every conversion

  static void reformation(int year, int month, int day);

// conversions

  // time(), dtime() and zmTime() can result in an out of range conversion
  // to time_t

  operator time_t() const { return this->time(); }
  time_t time() const {
    return (time_t)Native::time(m_julian, m_sec);
  }
  double dtime() const {
    if (ZuUnlikely(!*this)) return ZuCmp<double>::null();
    time_t t = Native::time(m_julian, m_sec);
    if (ZuUnlikely(Native::isMinimum(t))) return -ZuCmp<double>::inf();
    if (ZuUnlikely(Native::isMaximum(t))) return ZuCmp<double>::inf();
    return((double)t + (double)m_nsec / (double)1000000000);
  }
  operator ZmTime() const { return this->zmTime(); }
  ZmTime zmTime() const {
    if (ZuUnlikely(!*this)) return {};
    return ZmTime{this->time(), m_nsec};
  }

  struct tm *tm(struct tm *tm) const;

  void ymd(int &year, int &month, int &day) const;
  void hms(int &hour, int &minute, int &sec) const;
  void hmsn(int &hour, int &minute, int &sec, int &nsec) const;

  // number of days relative to a base date
  int days(int year, int month, int day) const {
    return m_julian - julian(year, month, day);
  }
  // days arg below should be the value of days(year, 1, 1, offset)
  // week (0-53) wkDay (1-7) 1st Monday in year is 1st day of week 1
  void ywd(
      int year, int days, int &week, int &wkDay) const;
  // week (0-53) wkDay (1-7) 1st Sunday in year is 1st day of week 1
  void ywdSun(
      int year, int days, int &week, int &wkDay) const;
  // week (1-53) wkDay (1-7) 1st Thursday in year is 4th day of week 1
  void ywdISO(
      int year, int days, int &wkYear, int &week, int &wkDay) const;

  static ZuString dayShortName(int i); // 1-7
  static ZuString dayLongName(int i); // 1-7
  static ZuString monthShortName(int i); // 1-12
  static ZuString monthLongName(int i); // 1-12

  auto print(const ZtDateFmt::CSV &fmt) const {
    return ZtDatePrintCSV{*this, fmt};
  }
  template <typename S_>
  void csv_print(S_ &s, const ZtDateFmt::CSV &fmt) const {
    if (!*this) return;
    ZtDate date = *this + fmt.m_offset;
    if (ZuUnlikely(date.m_julian != fmt.m_julian)) {
      fmt.m_julian = date.m_julian;
      int y, m, d;
      date.ymd(y, m, d);
      if (ZuUnlikely(y < 1)) { s << '-'; y = 1 - y; }
      else if (ZuUnlikely(y > 9999)) y = 9999;
      fmt.m_yyyymmdd[0] = y / 1000 + '0';
      fmt.m_yyyymmdd[1] = (y / 100) % 10 + '0';
      fmt.m_yyyymmdd[2] = (y / 10) % 10 + '0';
      fmt.m_yyyymmdd[3] = y % 10 + '0';
      fmt.m_yyyymmdd[5] = m / 10 + '0';
      fmt.m_yyyymmdd[6] = m % 10 + '0';
      fmt.m_yyyymmdd[8] = d / 10 + '0';
      fmt.m_yyyymmdd[9] = d % 10 + '0';
    }
    s << ZuString(fmt.m_yyyymmdd, 10) << ' ';
    if (ZuUnlikely(date.m_sec != fmt.m_sec)) {
      fmt.m_sec = date.m_sec;
      int H, M, S;
      hms(H, M, S);
      fmt.m_hhmmss[0] = H / 10 + '0';
      fmt.m_hhmmss[1] = H % 10 + '0';
      fmt.m_hhmmss[3] = M / 10 + '0';
      fmt.m_hhmmss[4] = M % 10 + '0';
      fmt.m_hhmmss[6] = S / 10 + '0';
      fmt.m_hhmmss[7] = S % 10 + '0';
    }
    s << ZuString(fmt.m_hhmmss, 8);
    if (unsigned N = date.m_nsec) {
      char buf[9];
      if (fmt.m_pad) {
	Zu_ntoa::Base10_print_frac(N, 9, fmt.m_pad, buf);
	s << '.' << ZuString(buf, 9);
      } else {
	if (N = Zu_ntoa::Base10_print_frac_truncate(N, 9, buf))
	  s << '.' << ZuString(buf, N);
      }
    }
  }

  template <int Exp, class Null>
  auto print(const ZtDateFmt::FIX<Exp, Null> &fmt) const {
    return ZtDatePrintFIX<Exp, Null>{*this, fmt};
  }
  template <typename S_, int Exp, class Null>
  void fix_print(S_ &s, const ZtDateFmt::FIX<Exp, Null> &fmt) const {
    if (!*this) { s << Null{}; return; }
    if (ZuUnlikely(m_julian != fmt.m_julian)) {
      fmt.m_julian = m_julian;
      int y, m, d;
      ymd(y, m, d);
      if (ZuUnlikely(y < 1)) y = 1;
      else if (ZuUnlikely(y > 9999)) y = 9999;
      fmt.m_yyyymmdd[0] = y / 1000 + '0';
      fmt.m_yyyymmdd[1] = (y / 100) % 10 + '0';
      fmt.m_yyyymmdd[2] = (y / 10) % 10 + '0';
      fmt.m_yyyymmdd[3] = y % 10 + '0';
      fmt.m_yyyymmdd[4] = m / 10 + '0';
      fmt.m_yyyymmdd[5] = m % 10 + '0';
      fmt.m_yyyymmdd[6] = d / 10 + '0';
      fmt.m_yyyymmdd[7] = d % 10 + '0';
    }
    s << ZuString(fmt.m_yyyymmdd, 8) << '-';
    if (ZuUnlikely(m_sec != fmt.m_sec)) {
      fmt.m_sec = m_sec;
      int H, M, S;
      hms(H, M, S);
      fmt.m_hhmmss[0] = H / 10 + '0';
      fmt.m_hhmmss[1] = H % 10 + '0';
      fmt.m_hhmmss[3] = M / 10 + '0';
      fmt.m_hhmmss[4] = M % 10 + '0';
      fmt.m_hhmmss[6] = S / 10 + '0';
      fmt.m_hhmmss[7] = S % 10 + '0';
    }
    s << ZuString(fmt.m_hhmmss, 8);
    fmt.frac_print(s, m_nsec);
  }

  // iso() always generates a full date/time format parsable by the ctor
  auto print(const ZtDateFmt::ISO &fmt) const {
    return ZtDatePrintISO{*this, fmt};
  }
  template <typename S_>
  void iso_print(S_ &s, const ZtDateFmt::ISO &fmt) const {
    if (!*this) return;
    ZtDate date = *this + fmt.m_offset;
    if (ZuUnlikely(date.m_julian != fmt.m_julian)) {
      fmt.m_julian = date.m_julian;
      int y, m, d;
      date.ymd(y, m, d);
      if (ZuUnlikely(y < 1)) { s << '-'; y = 1 - y; }
      else if (ZuUnlikely(y > 9999)) y = 9999;
      fmt.m_yyyymmdd[0] = y / 1000 + '0';
      fmt.m_yyyymmdd[1] = (y / 100) % 10 + '0';
      fmt.m_yyyymmdd[2] = (y / 10) % 10 + '0';
      fmt.m_yyyymmdd[3] = y % 10 + '0';
      fmt.m_yyyymmdd[5] = m / 10 + '0';
      fmt.m_yyyymmdd[6] = m % 10 + '0';
      fmt.m_yyyymmdd[8] = d / 10 + '0';
      fmt.m_yyyymmdd[9] = d % 10 + '0';
    }
    s << ZuString(fmt.m_yyyymmdd, 10) << 'T';
    if (ZuUnlikely(date.m_sec != fmt.m_sec)) {
      fmt.m_sec = date.m_sec;
      int H, M, S;
      hms(H, M, S);
      fmt.m_hhmmss[0] = H / 10 + '0';
      fmt.m_hhmmss[1] = H % 10 + '0';
      fmt.m_hhmmss[3] = M / 10 + '0';
      fmt.m_hhmmss[4] = M % 10 + '0';
      fmt.m_hhmmss[6] = S / 10 + '0';
      fmt.m_hhmmss[7] = S % 10 + '0';
    }
    s << ZuString(fmt.m_hhmmss, 8);
    if (unsigned N = date.m_nsec) {
      char buf[9];
      N = Zu_ntoa::Base10_print_frac_truncate(N, 9, buf);
      if (N) s << '.' << ZuString(buf, N);
    }
    if (fmt.m_offset) {
      int offset_ = (fmt.m_offset < 0) ? -fmt.m_offset : fmt.m_offset;
      int oH = offset_ / 3600, oM = (offset_ % 3600) / 60;
      char buf[5];
      buf[0] = oH / 10 + '0';
      buf[1] = oH % 10 + '0';
      buf[2] = ':';
      buf[3] = oM / 10 + '0';
      buf[4] = oM % 10 + '0';
      s << ((fmt.m_offset < 0) ? '-' : '+') << ZuString(buf, 5);
    } else
      s << 'Z';
  }

  // this strftime(3) is neither system timezone- nor locale- dependent;
  // timezone is specified by the (optional) offset parameter, output is
  // equivalent to the C library strftime under the 'C' locale; it does
  // not call tzset() and is thread-safe; conforms to, variously:
  //   (C90) - ANSI C '90
  //   (C99) - ANSI C '99
  //   (SU) - Single Unix Specification
  //   (MS) - Microsoft CRT
  //   (GNU) - glibc (not all glibc-specific extensions are supported)
  //   (TZ) - Arthur Olson's timezone library
  // the following conversion specifiers are supported:
  //   %# (MS) alt. format
  //   %E (SU) alt. format
  //   %O (SU) alt. digits (has no effect)
  //   %0-9 (GNU) field width (precision) specifier
  //   %a (C90) day of week - short name
  //   %A (C90) day of week - long name
  //   %b (C90) month - short name
  //   %h (SU) '' 
  //   %B (C90) month - long name
  //   %c (C90) Unix asctime() / ctime() (%a %b %e %T %Y)
  //   %C (SU) century
  //   %d (C90) day of month
  //   %x (C90) %m/%d/%y
  //   %D (SU) ''
  //   %e (SU) day of month - space padded
  //   %F (C99) %Y-%m-%d per ISO 8601
  //   %g (TZ) ISO week date year (2 digits)
  //   %G (TZ) '' (4 digits)
  //   %H (C90) hour (24hr)
  //   %I (C90) hour (12hr)
  //   %j (C90) day of year
  //   %m (C90) month
  //   %M (C90) minute
  //   %n (SU) newline
  //   %p (C90) AM/PM
  //   %P (GNU) am/pm
  //   %r (SU) %I:%M:%S %p
  //   %R (SU) %H:%M
  //   %s (TZ) number of seconds since the Epoch
  //   %S (C90) second
  //   %t (SU) TAB
  //   %X (C90) %H:%M:%S
  //   %T (SU) ''
  //   %u (SU) week day as decimal (1-7), 1 is Monday (7 is Sunday)
  //   %U (C90) week (00-53), 1st Sunday in year is 1st day of week 1
  //   %V (SU) week (01-53), per ISO week date
  //   %w (C90) week day as decimal (0-6), 0 is Sunday
  //   %W (C90) week (00-53), 1st Monday in year is 1st day of week 1
  //   %y (C90) year (2 digits)
  //   %Y (C90) year (4 digits)
  //   %z (GNU) RFC 822 timezone offset
  //   %Z (C90) timezone
  //   %% (C90) percent sign
  auto strftime(const char *format, int offset = 0) const {
    return ZtDatePrintStrftime{*this, ZtDateFmt::Strftime{format, offset}};
  }

  // run-time variable date/time formatting
  auto print(const ZtDateFmt::Any &fmt) const {
    return ZtDatePrint{*this, fmt};
  }

  int offset(const char *tz = 0) const; // WARNING

// operators

  // ZtDate is an absolute time, not a time interval, so the difference
  // between two ZtDates is a ZmTime;
  // for the same reason no operator +(ZtDate) is defined
  ZmTime operator -(const ZtDate &date) const {
    int day = m_julian - date.m_julian;
    int sec = m_sec - date.m_sec;
    int nsec = m_nsec - date.m_nsec;

    if (nsec < 0) nsec += 1000000000, --sec;
    if (sec < 0) sec += 86400, --day;

    return ZmTime(day * 86400 + sec, nsec);
  }

  template <typename T>
  ZuExact<ZmTime, T, ZtDate> operator +(const T &t) const {
    int julian, sec, nsec;

    sec = m_sec;
    nsec = m_nsec + t.nsec();
    if (nsec < 0)
      nsec += 1000000000, --sec;
    else if (nsec >= 1000000000)
      nsec -= 1000000000, ++sec;

    {
      int sec_ = t.sec();

      if (sec_ < 0) {
        sec_ = -sec_;
	julian = m_julian - sec_ / 86400;
	sec -= sec_ % 86400;

	if (sec < 0) sec += 86400, --julian;
      } else {
	julian = m_julian + sec_ / 86400;
	sec += sec_ % 86400;

	if (sec >= 86400) sec -= 86400, ++julian;
      }
    }

    return ZtDate{Julian, julian, sec, nsec};
  }
  template <typename T> ZuIfT<
    ZuInspect<time_t, T>::Same ||
    ZuInspect<long, T>::Same ||
    ZuInspect<int, T>::Same, ZtDate> operator +(T sec_) const {
    int julian, sec = sec_;

    if (sec < 0) {
      sec = -sec;
      if (ZuLikely(sec < 86400))
	julian = m_julian, sec = m_sec - sec;
      else
	julian = m_julian - (int)(sec / 86400), sec = m_sec - sec % 86400;

      if (sec < 0) sec += 86400, --julian;
    } else {
      if (ZuLikely(sec < 86400))
	julian = m_julian, sec = m_sec + sec;
      else
	julian = m_julian + (int)(sec / 86400), sec = m_sec + sec % 86400;

      if (sec >= 86400) sec -= 86400, ++julian;
    }

    return ZtDate{Julian, julian, sec, m_nsec};
  }

  template <typename T>
  ZuExact<ZmTime, T, ZtDate &> operator +=(const T &t) {
    int julian, sec, nsec;

    sec = m_sec;
    nsec = m_nsec + t.nsec();
    if (nsec < 0)
      nsec += 1000000000, --sec;
    else if (nsec >= 1000000000)
      nsec -= 1000000000, ++sec;

    {
      int sec_ = t.sec();

      if (sec_ < 0) {
        sec_ = -sec_;
	julian = m_julian - sec_ / 86400;
	sec -= sec_ % 86400;

	if (sec < 0) sec += 86400, --julian;
      } else {
	julian = m_julian + sec_ / 86400;
	sec += sec_ % 86400;

	if (sec >= 86400) sec -= 86400, ++julian;
      }
    }

    m_julian = julian;
    m_sec = sec;
    m_nsec = nsec;

    return *this;
  }
  template <typename T> ZuIfT<
    ZuInspect<time_t, T>::Same ||
    ZuInspect<long, T>::Same ||
    ZuInspect<int, T>::Same, ZtDate &> operator +=(T sec_) {
    int julian, sec = sec_;

    if (sec < 0) {
      sec = -sec;
      if (ZuLikely(sec < 86400))
	julian = m_julian, sec = m_sec - sec;
      else
	julian = m_julian - (int)(sec / 86400), sec = m_sec - sec % 86400;

      if (sec < 0) sec += 86400, --julian;
    } else {
      if (ZuLikely(sec < 86400))
	julian = m_julian, sec = m_sec + sec;
      else
	julian = m_julian + (int)(sec / 86400), sec = m_sec + sec % 86400;

      if (sec >= 86400) sec -= 86400, ++julian;
    }

    m_julian = julian;
    m_sec = sec;

    return *this;
  }

  template <typename T>
  ZuExact<ZmTime, T, ZtDate> operator -(const T &t) const {
    return ZtDate::operator +(-t);
  }
  template <typename T> ZuIfT<
      ZuInspect<time_t, T>::Same ||
      ZuInspect<long, T>::Same ||
      ZuInspect<int, T>::Same, ZtDate> operator -(T sec_) {
    return ZtDate::operator +(-sec_);
  }
  template <typename T>
  ZuExact<ZmTime, T, ZtDate &> operator -=(const T &t) {
    return ZtDate::operator +=(-t);
  }
  template <typename T> ZuIfT<
    ZuInspect<time_t, T>::Same ||
    ZuInspect<long, T>::Same ||
    ZuInspect<int, T>::Same, ZtDate &> operator -=(T sec_) {
    return ZtDate::operator +=(-sec_);
  }

  bool equals(const ZtDate &date) const {
    return m_julian == date.m_julian &&
      m_sec == date.m_sec && m_nsec == date.m_nsec;
  }
  int cmp(const ZtDate &date) const {
    if (int i = ZuCmp<int32_t>::cmp(m_julian, date.m_julian)) return i;
    if (int i = ZuCmp<int32_t>::cmp(m_sec, date.m_sec)) return i;
    return ZuCmp<int32_t>::cmp(m_nsec, date.m_nsec);
  }
  friend inline bool operator ==(const ZtDate &l, const ZtDate &r) {
    return l.equals(r);
  }
  friend inline int operator <=>(const ZtDate &l, const ZtDate &r) {
    return l.cmp(r);
  }

  constexpr bool operator !() const {
    return m_julian == ZuCmp<int32_t>::null();
  }
  ZuOpBool

// utility functions

  ZtDate &now() {
    ZmTime t(ZmTime::Now);
    init(t.sec());
    m_nsec = t.nsec();
    return *this;
  }

  uint32_t hash() const { return m_julian ^ m_sec ^ m_nsec; }

private:
  void ctor(int year, int month, int day) {
    normalize(year, month);
    m_julian = julian(year, month, day);
    m_sec = 0, m_nsec = 0;
  }
  void ctor(int year, int month, int day,
		   int hour, int minute, int sec, int nsec) {
    normalize(year, month);
    normalize(day, hour, minute, sec, nsec);
    m_julian = julian(year, month, day);
    m_sec = second(hour, minute, sec);
    m_nsec = nsec;
  }
  void ctor(int hour, int minute, int sec, int nsec) {
    m_julian = (int32_t)((ZmTime(ZmTime::Now).sec() / 86400) + 2440588);
    {
      int day = 0;
      normalize(day, hour, minute, sec, nsec);
      m_julian += day;
    }
    m_sec = second(hour, minute, sec);
    m_nsec = nsec;
  }

private:
  // parse fixed-width integers
  template <unsigned Width> struct Int {
    template <typename T, typename S>
    static unsigned scan(T &v, S &s) {
      unsigned i = v.scan(ZuFmt::Right<Width>(), s);
      if (ZuLikely(i > 0)) s.offset(i);
      return i;
    }
  };
  // parse nanoseconds
  template <typename T, typename S>
  static unsigned scanFrac(T &v, S &s) {
    unsigned i = v.scan(ZuFmt::Frac<9>(), s);
    if (ZuLikely(i > 0)) s.offset(i);
    return i;
  }

public:
  void ctor(const ZtDateScan::CSV &, ZuString);
  void ctor(const ZtDateScan::FIX &, ZuString);
  void ctor(const ZtDateScan::ISO &, ZuString);
  void ctor(const ZtDateScan::Any &, ZuString);

  void normalize(unsigned &year, unsigned &month);
  void normalize(int &year, int &month);

  void normalize(unsigned &day, unsigned &hour, unsigned &minute,
      unsigned &sec, unsigned &nsec);
  void normalize(int &day, int &hour, int &minute, int &sec, int &nsec);

  static int julian(int year, int month, int day);

  static int second(int hour, int minute, int second) {
    return hour * 3600 + minute * 60 + second;
  }

  void offset_(int year, int month, int day,
	       int hour, int minute, int second, const char *tz);

  void init(time_t t) {
    if (ZuUnlikely(ZuCmp<time_t>::null(t))) {
      m_julian = ZuCmp<int32_t>::null();
      m_sec = 0;
    } else {
      m_julian = static_cast<int32_t>((t / 86400) + 2440588);
      m_sec = static_cast<int32_t>(t % 86400);
    }
    m_nsec = 0;
  }

  // default printing (ISO8601 format)
  struct DefltPrint : public ZuPrintDelegate {
    template <typename S>
    static void print(S &s, const ZtDate &v) {
      auto &fmt = ZmTLS<ZtDateFmt::ISO, (int DefltPrint::*){}>();
      s << v.print(fmt);
    }
  };
  friend DefltPrint ZuPrintType(ZtDate *);

  // traits
  struct Traits : public ZuBaseTraits<ZtDate> { enum { IsPOD = 1 }; };
  friend Traits ZuTraitsType(ZtDate *);

private:
  int32_t	m_julian = ZuCmp<int32_t>::null();	// julian day
  int32_t	m_sec = 0;	// time within day, in seconds, from midnight
  int32_t	m_nsec = 0;	// nanoseconds

// effective date of the reformation (default 14th September 1752)

  static int		m_reformationJulian;
  static int		m_reformationYear;
  static int		m_reformationMonth;
  static int		m_reformationDay;
};

inline ZtDate ZtDateNow() { return ZtDate{ZtDate::Now}; }

struct ZtAPI ZtDate_strftime {
  template <typename Boxed>
  static auto vfmt(ZuVFmt &fmt,
      const Boxed &boxed, unsigned width, bool alt) {
    if (alt)
      fmt.reset();
    else
      fmt.right(width);
    return boxed.vfmt(fmt);
  }
  template <typename Boxed>
  static auto vfmt(ZuVFmt &fmt,
      const Boxed &boxed, ZuBox<unsigned> width, int deflt, bool alt) {
    if (alt)
      fmt.reset();
    else {
      if (!*width) width = deflt;
      fmt.right(width);
    }
    return boxed.vfmt(fmt);
  }
  template <typename Boxed>
  static auto vfmt(ZuVFmt &fmt,
      const Boxed &boxed, ZuBox<unsigned> width, int deflt, bool alt,
      char pad) {
    if (alt)
      fmt.reset();
    else {
      if (!*width) width = deflt;
      fmt.right(width, pad);
    }
    return boxed.vfmt(fmt);
  }

  static void print_(ZmStream &, ZtDate, const char *format, int offset);
};

template <typename S>
inline void ZtDatePrintCSV::print(S &s) const {
  value.csv_print(s, fmt);
}
template <int Exp, class Null>
template <typename S>
inline void ZtDatePrintFIX<Exp, Null>::print(S &s) const {
  value.fix_print(s, fmt);
}
template <typename S>
inline void ZtDatePrintISO::print(S &s) const {
  value.iso_print(s, fmt);
}
template <typename S>
inline void ZtDatePrintStrftime::print(S &s_) const {
  ZmStream s{s_};
  ZtDate_strftime::print_(s, value, fmt.format, fmt.offset);
}
inline void ZtDatePrintStrftime::print(ZmStream &s) const {
  ZtDate_strftime::print_(s, value, fmt.format, fmt.offset);
}
template <typename S>
inline void ZtDatePrint::print(S &s) const {
  using namespace ZtDateFmt;
  switch (fmt.type()) {
    default:
    case Any::Index<CSV>{}:
      s << value.print(fmt.csv());
      break;
    case Any::Index<FIXDeflt>{}:
      s << value.print(fmt.fix());
      break;
    case Any::Index<ISO>{}:
      s << value.print(fmt.iso());
      break;
    case Any::Index<Strftime>{}: {
      const auto &strftime = fmt.strftime();
      s << value.strftime(strftime.format, strftime.offset);
    } break;
  }
}

#endif /* ZtDate_HH */
