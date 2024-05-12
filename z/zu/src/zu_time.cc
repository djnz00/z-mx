//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// ZuTime - C API

#include <zlib/ZuTime.hh>
#include <zlib/ZuByteSwap.hh>
#include <zlib/Zu_ntoa.hh>

#include <zlib/zu_time.h>

ZuAssert(sizeof(zu_time) == sizeof(ZuTime));

unsigned int zu_time_in(zu_time *v, const char *s)
{
  return reinterpret_cast<ZuTime *>(v)->scan(s);
}
unsigned int zu_time_out_len(const zu_time *v)
{
  return 32;
}
char *zu_time_out(char *s, const zu_time *v_)
{
  auto v = reinterpret_cast<const ZuTime *>(v_);
  if (!*v) { *s++ = 0; return s; }
  int year, month, day, hour, minute, sec, nsec;
  v->ymdhmsn(year, month, day, hour, minute, sec, nsec);
  if (year < 0) { *s++ = '-'; year = -year; }
  *s++ = (year / 1000) + '0';
  *s++ = ((year / 100) % 10) + '0';
  *s++ = ((year / 10) % 10) + '0';
  *s++ = (year % 10) + '0';
  *s++ = '/';
  *s++ = (month / 10) + '0';
  *s++ = (month % 10) + '0';
  *s++ = '/';
  *s++ = (day / 10) + '0';
  *s++ = (day % 10) + '0';
  *s++ = ' ';
  *s++ = (hour / 10) + '0';
  *s++ = (hour % 10) + '0';
  *s++ = ':';
  *s++ = (minute / 10) + '0';
  *s++ = (minute % 10) + '0';
  *s++ = ':';
  *s++ = (sec / 10) + '0';
  *s++ = (sec % 10) + '0';
  *s++ = '.';
  s += Zu_nprint<ZuFmt::Frac<9>>::utoa(nsec, s);
  *s = 0;
  return s;
}

__int128_t zu_time_to_int(const zu_time *v_)
{
  auto v = reinterpret_cast<const ZuTime *>(v_);
  return v->nanosecs();
}

zu_time *zu_time_from_int(zu_time *v_, __int128_t i)
{
  new (v_) ZuTime(ZuTime::Nano, i);
  return v_;
}

long double zu_time_to_ldouble(const zu_time *v_)
{
  auto v = reinterpret_cast<const ZuTime *>(v_);
  return v->as_ldouble();
}

zu_time *zu_time_from_ldouble(zu_time *v_, long double d)
{
  new (v_) ZuTime(d);
  return v_;
}

int zu_time_cmp(const zu_time *l_, const zu_time *r_)
{
  auto l = reinterpret_cast<const ZuTime *>(l_);
  auto r = reinterpret_cast<const ZuTime *>(r_);
  return l->cmp(*r);
}

uint32_t zu_time_hash(const zu_time *v_)
{
  auto v = reinterpret_cast<const ZuTime *>(v_);
  return v->hash();
}

zu_time *zu_time_neg(zu_time *v_, const zu_time *p_)
{
  auto &v = *reinterpret_cast<ZuTime *>(v_);
  const auto &p = *reinterpret_cast<const ZuTime *>(p_);
  v = -p;
  return v_;
}

zu_time *zu_time_add(
    zu_time *v_, const zu_time *l_, const zu_time *r_)
{
  auto &v = *reinterpret_cast<ZuTime *>(v_);
  const auto &l = *reinterpret_cast<const ZuTime *>(l_);
  const auto &r = *reinterpret_cast<const ZuTime *>(r_);
  v = l + r;
  return v_;
}
zu_time *zu_time_sub(
    zu_time *v_, const zu_time *l_, const zu_time *r_)
{
  auto &v = *reinterpret_cast<ZuTime *>(v_);
  const auto &l = *reinterpret_cast<const ZuTime *>(l_);
  const auto &r = *reinterpret_cast<const ZuTime *>(r_);
  v = l - r;
  return v_;
}
zu_time *zu_time_mul(
    zu_time *v_, const zu_time *l_, long double r)
{
  auto &v = *reinterpret_cast<ZuTime *>(v_);
  const auto &l = *reinterpret_cast<const ZuTime *>(l_);
  v = l * r;
  return v_;
}
zu_time *zu_time_div(
    zu_time *v_, const zu_time *l_, long double r)
{
  auto &v = *reinterpret_cast<ZuTime *>(v_);
  const auto &l = *reinterpret_cast<const ZuTime *>(l_);
  v = l / r;
  return v_;
}
