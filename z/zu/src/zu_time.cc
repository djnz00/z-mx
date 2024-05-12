//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// ZuTime - C API

#include <zlib/ZuTime.hh>
#include <zlib/ZuDateTime.hh>
#include <zlib/ZuStream.hh>
#include <zlib/ZuByteSwap.hh>
#include <zlib/Zu_ntoa.hh>

#include <zlib/zu_time.h>

ZuAssert(sizeof(zu_time) == sizeof(ZuTime));

bool zu_time_null(const zu_time *v_)
{
  auto v = reinterpret_cast<const ZuTime *>(v_);
  return !*v;
}

unsigned int zu_time_in_csv(zu_time *v, const char *s)
{
  return reinterpret_cast<ZuTime *>(v)->scan(s);
}
unsigned int zu_time_in_iso(zu_time *v_, const char *s)
{
  auto &v = *reinterpret_cast<ZuTime *>(v_);
  ZuDateTimeScan::ISO fmt;
  ZuDateTime t;
  auto n = t.scan(fmt, s);
  v = t.as_zuTime();
  return n;
}
unsigned int zu_time_in_fix(zu_time *v_, const char *s)
{
  auto &v = *reinterpret_cast<ZuTime *>(v_);
  ZuDateTimeScan::FIX fmt;
  ZuDateTime t;
  auto n = t.scan(fmt, s);
  v = t.as_zuTime();
  return n;
}
unsigned int zu_time_in_interval(zu_time *v_, const char *s)
{
  auto &v = *reinterpret_cast<ZuTime *>(v_);
  ZuBox<long double> d;
  auto n = d.scan(s);
  v = ZuTime{d};
  return n;
}

unsigned int zu_time_out_csv_len(const zu_time *v_)
{
  return 32;
}
char *zu_time_out_csv(char *s_, const zu_time *v_)
{
  thread_local ZuDateTimeFmt::CSV fmt;
  const auto &v = *reinterpret_cast<const ZuTime *>(v_);
  ZuDateTime d{v};
  ZuStream s{s_, 32U};
  s << d.print(fmt);
  return s.data();
}

unsigned int zu_time_out_iso_len(const zu_time *v_)
{
  return 40;
}
char *zu_time_out_iso(char *s_, const zu_time *v_)
{
  thread_local ZuDateTimeFmt::ISO fmt;
  const auto &v = *reinterpret_cast<const ZuTime *>(v_);
  ZuDateTime d{v};
  ZuStream s{s_, 40U};
  s << d.print(fmt);
  return s.data();
}

unsigned int zu_time_out_fix_len(const zu_time *v_)
{
  return 32;
}
char *zu_time_out_fix(char *s_, const zu_time *v_)
{
  thread_local ZuDateTimeFmt::FIX<-3> fmt;
  const auto &v = *reinterpret_cast<const ZuTime *>(v_);
  ZuDateTime d{v};
  ZuStream s{s_, 32U};
  s << d.print(fmt);
  return s.data();
}

unsigned int zu_time_out_interval_len(const zu_time *v_)
{
  return 32;
}
char *zu_time_out_interval(char *s_, const zu_time *v_)
{
  const auto &v = *reinterpret_cast<const ZuTime *>(v_);
  ZuStream s{s_, 32U};
  s << v.interval();
  return s.data();
}

__int128_t zu_time_to_int(const zu_time *v_)
{
  const auto &v = *reinterpret_cast<const ZuTime *>(v_);
  return v.nanosecs();
}

zu_time *zu_time_from_int(zu_time *v_, __int128_t i)
{
  new (v_) ZuTime(ZuTime::Nano{i});
  return v_;
}

long double zu_time_to_ldouble(const zu_time *v_)
{
  const auto &v = *reinterpret_cast<const ZuTime *>(v_);
  return v.as_ldouble();
}

zu_time *zu_time_from_ldouble(zu_time *v_, long double d)
{
  new (v_) ZuTime(d);
  return v_;
}

int zu_time_cmp(const zu_time *l_, const zu_time *r_)
{
  const auto &l = *reinterpret_cast<const ZuTime *>(l_);
  const auto &r = *reinterpret_cast<const ZuTime *>(r_);
  return l.cmp(r);
}

uint32_t zu_time_hash(const zu_time *v_)
{
  const auto &v = *reinterpret_cast<const ZuTime *>(v_);
  return v.hash();
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
