//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <time.h>

#include <zlib/ZmAssert.hh>
#include <zlib/ZmGuard.hh>
#include <zlib/ZmPLock.hh>
#include <zlib/ZmSingleton.hh>

#include <zlib/ZtTimeZone.hh>
#include <zlib/ZtPlatform.hh>

class Zt_TzLock : public ZmPLock {
  friend ZuUnsigned<ZmCleanup::Platform> ZmCleanupLevel(Zt_TzLock *);
};

class Zt_TzGuard : public ZmGuard<ZmPLock> {
public:
  Zt_TzGuard(const char *tz);
  ~Zt_TzGuard();

private:
  char	*m_tz = nullptr;
  char	*m_oldTz = nullptr;
};

Zt_TzGuard::Zt_TzGuard(const char *tz) :
  ZmGuard<ZmPLock>{*ZmSingleton<Zt_TzLock>::instance()}
{
  if (tz) {
    if (m_oldTz = ::getenv("TZ")) m_oldTz -= 3; // potentially non-portable

    m_tz = static_cast<char *>(malloc(strlen(tz) + 4));
    ZmAssert(m_tz);
    if (!m_tz) throw std::bad_alloc();
    strcpy(m_tz, "TZ=");
    strcpy(m_tz + 3, tz);

    Zt::putenv(m_tz);
  }

  Zt::tzset();
}

Zt_TzGuard::~Zt_TzGuard()
{
  if (m_tz) {
    if (m_oldTz)
      Zt::putenv(m_oldTz);
    else
      Zt::putenv("TZ=");
    free(m_tz);

    Zt::tzset();
  }
}

int Zt::tzOffset(const ZuDateTime &value, const char *tz)
{
  using Native = ZuDateTime::Native;

  int year, month, day, hour, minute, second;
  value.ymd(year, month, day);
  value.hms(hour, minute, second);

  Zt_TzGuard tzGuard(tz);

  if (year < 1900) return -timezone;

  struct tm tm_ {
    .tm_sec = second,
    .tm_min = minute,
    .tm_hour = hour,
    .tm_mday = day,
    .tm_mon = month - 1,
    .tm_year = year - 1900,
    .tm_isdst = -1
  };

  time_t t = mktime(&tm_);

  if (ZuUnlikely(Native::isMinimum(t) || Native::isMaximum(t)))
    return -timezone;

  time_t t_ = Native::time(
    ZuDateTime::julian(tm_.tm_year + 1900, tm_.tm_mon + 1, tm_.tm_mday),
    ZuDateTime::second(tm_.tm_hour, tm_.tm_min, tm_.tm_sec));

  if (ZuUnlikely(Native::isMinimum(t_) || Native::isMaximum(t_)))
    return -timezone;

  return int(t_ - t);
}
