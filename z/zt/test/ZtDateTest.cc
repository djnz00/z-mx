//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuLib.hh>

#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <limits.h>
#include <math.h>

#include <zlib/ZtDate.hh>

#define CHECK(x) ((x) ? puts("OK  " #x) : puts("NOK " #x))

ZtDatePrintISO isoPrint(const ZtDate &d, int offset = 0) {
  auto &fmt = ZmTLS<ZtDateFmt::ISO, isoPrint>();
  fmt.offset(offset);
  return d.print(fmt);
}

ZuStringN<40> isoStr(const ZtDate &d, int offset = 0) {
  return ZuStringN<40>{} << isoPrint(d, offset);
}

struct LocalDT {
  LocalDT(const ZtDate &d) {
    ZtDate l = d + d.offset();
    l.ymd(m_Y, m_M, m_D);
    l.hmsn(m_h, m_m, m_s, m_n);
    ZmAssert(d == ZtDate(m_Y, m_M, m_D, m_h, m_m, m_s, m_n, ""));
  }
  ZtString dump() {
    return ZtSprintf("Lcl %.4d/%.2d/%.2d %.2d:%.2d:%.2d.%.6d",
	m_Y, m_M, m_D, m_h, m_m, m_s, m_n / 1000);
  }
  int m_Y, m_M, m_D, m_h, m_m, m_s, m_n;
};

struct GMTDT {
  GMTDT(const ZtDate &d) {
    d.ymd(m_Y, m_M, m_D);
    d.hmsn(m_h, m_m, m_s, m_n);
    ZmAssert(d == ZtDate(m_Y, m_M, m_D, m_h, m_m, m_s, m_n, 0));
  }
  ZtString dump() {
    return ZtSprintf("GMT %.4d/%.2d/%.2d %.2d:%.2d:%.2d.%.6d",
	m_Y, m_M, m_D, m_h, m_m, m_s, m_n / 1000);
  }
  int m_Y, m_M, m_D, m_h, m_m, m_s, m_n;
};

void weekDate(ZtDate d, int year, int weekChk, int wkDayChk)
{
  ZtDateFmt::ISO fmt;
  int week, wkDay;
  int days = d.days(year, 1, 1);
  d.ywd(year, days, week, wkDay);
  printf("%s: %d+%d %d+%d.%dW %d %d\n",
      isoStr(d).data(), year, days, year, days / 7, days % 7, week, wkDay);
  CHECK(week == weekChk);
  CHECK(wkDay == wkDayChk);
}

void weekDateSun(ZtDate d, int year, int weekChk, int wkDayChk)
{
  ZtDateFmt::ISO fmt;
  int week, wkDay;
  int days = d.days(year, 1, 1);
  d.ywdSun(year, days, week, wkDay);
  printf("%s: %d+%d %d+%d.%dW %d %d\n",
      isoStr(d).data(), year, days, year, days / 7, days % 7, week, wkDay);
  CHECK(week == weekChk);
  CHECK(wkDay == wkDayChk);
}

void weekDateISO(ZtDate d, int year, int yearChk, int weekChk, int wkDayChk)
{
  ZtDateFmt::ISO fmt;
  int yearISO, weekISO, wkDay;
  int days = d.days(year, 1, 1);
  d.ywdISO(year, days, yearISO, weekISO, wkDay);
  printf("%s: %d+%d %d+%d.%dW %d %d %d\n",
      isoStr(d).data(), year, days, year, days / 7, days % 7,
      yearISO, weekISO, wkDay);
  CHECK(yearISO == yearChk);
  CHECK(weekISO == weekChk);
  CHECK(wkDay == wkDayChk);
}

void strftimeChk(ZtDate d, const char *format, const char *chk)
{
  ZtString s; s << d.strftime(format);
  puts(s);
  puts(chk);
  CHECK(s == chk);
}

int main()
{
#ifdef _WIN32
  static const char *t[3] = { "JST-9", "GMT", "EST5EDT" };
#else
  static const char *t[3] = { "Japan", "GB", "EST5EDT" };
#endif

  Zt::tzset();

  //ZtDate d(1998, 12, 31);
  //ZtDate d(51179 + 2400000,0);
  ZtDate d(1998, 12, 1, 10, 30, 0);
  //ZtDate d(1998, 13, 0, 24, -1, 59);
  //ZtDate d(1970, 1, 1, 9, 0, 0);

  int i;
  ZuStringN<32> s = isoStr(d);

  printf("GMT %s\n", s.data());
  { ZtDate e(s); printf("GMT %s\n%s\n%s\n", isoStr(e).data(), LocalDT(e).dump().data(), GMTDT(e).dump().data()); }
  for (i = 0; i < 3; i++) {
    printf("%s %s\n", t[i], isoStr(d, d.offset(t[i])).data());
    { ZtDate e(s); printf("%s %s\n\n", t[i], isoStr(e, e.offset(t[i])).data()); }
  }
  printf("local %s\n", isoStr(d, d.offset()).data());
  { ZtDate e(s); printf("local %s\n%s\n%s\n", isoStr(e, e.offset()).data(), LocalDT(e).dump().data(), GMTDT(e).dump().data()); }

  d -= ZuTime(180 * 86400, 999995000); // 180 days, .999995 seconds

  printf("GMT %s\n", isoStr(d).data());
  { ZtDate e(s); printf("GMT %s\n\n", isoStr(e).data()); }
  for (i = 0; i < 3; i++) {
    printf("%s %s\n", t[i], isoStr(d, d.offset(t[i])).data());
    { ZtDate e(s); printf("%s %s\n\n", t[i], isoStr(e, e.offset(t[i])).data()); }
  }
  printf("local %s\n", isoStr(d, d.offset()).data());
  { ZtDate e(s); printf("local %s\n\n", isoStr(e, e.offset()).data()); }

  printf("local now %s\n", isoStr(ZtDate(ZtDate::Now), d.offset()).data());

  // ZtDate d(2050, 2, 3, 10, 0, 0, -timezone);

  // ZtDate d_(1752, 9, 1, 12, 30, 0, -timezone);

  d = ZtDate(ZtDate::Julian, 0, 0, 0);
  printf("ZtDate min: %s\n", isoStr(d).data());
  d = ZtDate(d.time());
  printf("time_t min: %s\n", isoStr(d).data());

  d = ZtDate(ZtDate::Julian, ZtDate_MaxJulian, 0, 0);
  printf("ZtDate max: %s\n", isoStr(d).data());
  d = ZtDate(d.time());
  printf("time_t max: %s\n", isoStr(d).data());

  {
    const char *s = "2011-04-07T10:30:00+0800";
    d = ZtDate(ZuString(s));
    printf("%s = %s\n", s, isoStr(d).data());
  }
  {
    const char *s = "2011-04-07T10:30:00.0012345+08:00";
    d = ZtDate(ZuString(s));
    printf("%s = %s\n", s, isoStr(d).data());
  }

  {
    ZtDate d1(ZtDate::Now);
    Zm::sleep(.1);
    ZtDate d2(ZtDate::Now);
    ZuTime t = d2 - d1;

    printf("\n1/10 sec delta time check: %.6f\n\n", t.dtime());
  }

  {
    weekDate(ZtDate(ZtDate::YYYYMMDD, 20080106, ZtDate::HHMMSS, 0), 2008, 0, 7);
    weekDate(ZtDate(ZtDate::YYYYMMDD, 20080107, ZtDate::HHMMSS, 0), 2008, 1, 1);
    weekDateSun(ZtDate(ZtDate::YYYYMMDD, 20070106, ZtDate::HHMMSS, 0), 2007,
		0, 7);
    weekDateSun(ZtDate(ZtDate::YYYYMMDD, 20070107, ZtDate::HHMMSS, 0), 2007,
		1, 1);
    {
      ZtDate d(ZtDate::YYYYMMDD, 20071231, ZtDate::HHMMSS, 0);
      int year, month, day;
      d.ymd(year, month, day);
      CHECK(year == 2007);
      CHECK(month == 12);
      CHECK(day == 31);
      weekDateISO(d, year, 2007, 53, 1);
    }
    weekDateISO(ZtDate(ZtDate::YYYYMMDD, 20070101, ZtDate::HHMMSS, 0), 2007,
	        2007, 1, 1);
    weekDateISO(ZtDate(ZtDate::YYYYMMDD, 20100103, ZtDate::HHMMSS, 0), 2010,
	        2009, 53, 7);
    weekDateISO(ZtDate(ZtDate::YYYYMMDD, 20110102, ZtDate::HHMMSS, 0), 2011,
	        2010, 52, 7);
    weekDateISO(ZtDate(ZtDate::YYYYMMDD, 17520902, ZtDate::HHMMSS, 0), 1752,
	        1752, 36, 3);
    weekDateISO(ZtDate(ZtDate::YYYYMMDD, 17520914, ZtDate::HHMMSS, 0), 1752,
	        1752, 36, 4);
    weekDateISO(ZtDate(ZtDate::YYYYMMDD, 17521231, ZtDate::HHMMSS, 0), 1752,
	        1752, 51, 7);
  }

  {
    strftimeChk(ZtDate(ZtDate::YYYYMMDD, 17520902, ZtDate::HHMMSS, 143000),
      "%a %A %b %B %C %d %e %g %G %H %I %j %m %M %p %P %S %u %V %Y",
      "Wed Wednesday Sep September 17 02  2 52 1752 "
      "14 02 246 09 30 PM pm 00 3 36 1752");
  }

#ifndef _WIN32
  ZuTime t1, t1_;
#else
  ZuTime o1, o2, t1, t1_, t2, t2_;
  double d1 = 0, d2 = 0, d3 = 0,
         d1sum = 0, d2sum = 0, d3sum = 0, d1vsum = 0, d2vsum = 0, d3vsum = 0,
         d1avg = 0, d2avg = 0, d3avg = 0, d1std = 0, d2std = 0, d3std = 0;
  FILETIME f;
#endif

  t1_ = Zm::now();
  for (i = 1; i <= 1000000; i++) t1 = Zm::now();
  t1_ = Zm::now() - t1_;
  double intrinsic = t1_.dtime() / 1000000;

  printf("\nZm::now() intrinsic cost: %12.10f\n", intrinsic);

#ifdef _WIN32
  GetSystemTimeAsFileTime(&f);
  t2_ = f;
  t1_ = Zm::now();

  o2 = t2_;
  o1 = t1_;

  for (i = 1; i <= 5000000; i++) {
    GetSystemTimeAsFileTime(&f);
    t2 = f;
    t1 = Zm::now();
    t1 -= intrinsic;
    d1 = (t1 - t1_).dtime();
    d2 = (t2 - t2_).dtime();
    d3 = (t1 - t2).dtime();
    d1sum += d1;
    d2sum += d2;
    d3sum += d3;
    d1vsum += d1 * d1;
    d2vsum += d2 * d2;
    d3vsum += d3 * d3;
    t1_ = t1;
    t2_ = t2;
  }
  --i;

  d1avg = d1sum / i;
  d2avg = d2sum / i;
  d3avg = d3sum / i;
  d1std = sqrt(d1vsum / i - d1avg * d1avg);
  d2std = sqrt(d2vsum / i - d2avg * d2avg);
  d3std = sqrt(d3vsum / i - d3avg * d3avg);

  printf("\n"
         "ZuTime cnt: % 10d avg: %12.10f std: %12.10f\n"
         "GSTAFT cnt: % 10d avg: %12.10f std: %12.10f\n"
         "ZuTime - GSTAFT skew   avg: %12.10f std: %12.10f\n",
         i, d1avg, d1std,
         i, d2avg, d2std,
         d3avg, d3std);

  for (++i; i <= 10000000; i++) {
    GetSystemTimeAsFileTime(&f);
    t2 = f;
    t1 = Zm::now();
    t1 -= intrinsic;
    d1 = (t1 - t1_).dtime();
    d2 = (t2 - t2_).dtime();
    d3 = (t1 - t2).dtime();
    d1sum += d1;
    d2sum += d2;
    d3sum += d3;
    d1vsum += d1 * d1;
    d2vsum += d2 * d2;
    d3vsum += d3 * d3;
    t1_ = t1;
    t2_ = t2;
  }
  --i;

  GetSystemTimeAsFileTime(&f);
  o2 = ZuTime(f) - o2;
  o1 = Zm::now() - o1;

  d1avg = d1sum / i;
  d2avg = d2sum / i;
  d3avg = d3sum / i;
  d1std = sqrt(d1vsum / i - d1avg * d1avg);
  d2std = sqrt(d2vsum / i - d2avg * d2avg);
  d3std = sqrt(d3vsum / i - d3avg * d3avg);

  printf("\n"
         "ZuTime cnt: % 10d act: %12.10f avg: %12.10f std: %12.10f\n"
         "GSTAFT cnt: % 10d act: %12.10f avg: %12.10f std: %12.10f\n"
	 "ZuTime - GSTAFT skew                     avg: %12.10f std: %12.10f\n",
         i, o1.dtime() / i, d1avg, d1std,
         i, o2.dtime() / i, d2avg, d2std,
         d3avg, d3std);
#endif

#if 0
  long j = d_.julian();
  int s = d_.seconds();
  int i;

  for (i = 0; i < 35; i++) {
    int y,m,da,h,mi,se;

    ZtDate d(j + i, s);

    puts(d.iso(-timezone));

    d.ymd(y, m, da, -timezone);
    d.hms(h, mi, se, -timezone);

    ZtDate d2(y, m, da, h, mi, se, -timezone);

    puts(ZuStringN<32>() << d2.iso(-timezone));

    // struct tm t;
    // char buf[128];

    // strftime(buf, 128, "%Y-%m-%dT%H:%M:%S %Z", d.tm(&t, -timezone));
    // puts(buf);
  }
#endif

//   {
//     ZtDate mabbit;
//     mabbit = 20091204;
//     ZtDate dmabbit(ZtDateNow());
//     printf("Mabbit D Time: %f\n", dmabbit.dtime());
//     printf("Mabbit Time: %d\n", mabbit.yyyymmdd());
//   }
}
