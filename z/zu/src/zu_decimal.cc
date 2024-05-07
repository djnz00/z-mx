//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// ZuDecimal - C API

#include <zlib/ZuDecimal.hh>
#include <zlib/Zu_ntoa.hh>

#include <zlib/zu_decimal.h>

ZuAssert(sizeof(zu_decimal) == sizeof(ZuDecimal));

zu_decimal *zu_decimal_in(zu_decimal *v, const char *s)
{
  new (v) ZuDecimal{s};
  return v;
}
unsigned int zu_decimal_out_len(const zu_decimal *v)
{
  // we could attempt to use Zu_nprint::ulen here to minimize the
  // output length, but doing so would require a 128bit division/modulo
  // and two calls to ulen - judged unlikely to be worth it
  return 40; // actually 39, round to 40
}
char *zu_decimal_out(char *s, const zu_decimal *v_)
{
  const auto &v = *reinterpret_cast<const ZuDecimal *>(v_);
  if (ZuUnlikely(!*v)) { strcpy(s, "nan"); return s + 3; }
  uint128_t iv, fv;
  if (v.value < 0) {
    *s++ = '-';
    iv = -v.value;
  } else
    iv = v.value;
  fv = iv % ZuDecimal::scale();
  iv /= ZuDecimal::scale();
  s += Zu_nprint<>::utoa(iv, s);
  if (fv) {
    *s++ = '.';
    s += Zu_nprint<ZuFmt::Frac<18>>::utoa(fv, s);
  }
  *s = 0;
  return s;
}

int zu_decimal_cmp(const zu_decimal *l_, const zu_decimal *r_)
{
  const auto &l = *reinterpret_cast<const ZuDecimal *>(l_);
  const auto &r = *reinterpret_cast<const ZuDecimal *>(r_);
  return l.cmp(r);
}

uint32_t zu_decimal_hash(const zu_decimal *v_)
{
  const auto &v = *reinterpret_cast<const ZuDecimal *>(v_);
  return v.hash();
}

zu_decimal *zu_decimal_add(
    zu_decimal *v_, const zu_decimal *l_, const zu_decimal *r_)
{
  const auto &l = *reinterpret_cast<const ZuDecimal *>(l_);
  const auto &r = *reinterpret_cast<const ZuDecimal *>(r_);
  auto &v = *reinterpret_cast<ZuDecimal *>(v_);
  v = l + r;
  return v_;
}
zu_decimal *zu_decimal_sub(
    zu_decimal *v_, const zu_decimal *l_, const zu_decimal *r_)
{
  const auto &l = *reinterpret_cast<const ZuDecimal *>(l_);
  const auto &r = *reinterpret_cast<const ZuDecimal *>(r_);
  auto &v = *reinterpret_cast<ZuDecimal *>(v_);
  v = l - r;
  return v_;
}
zu_decimal *zu_decimal_mul(
    zu_decimal *v_, const zu_decimal *l_, const zu_decimal *r_)
{
  const auto &l = *reinterpret_cast<const ZuDecimal *>(l_);
  const auto &r = *reinterpret_cast<const ZuDecimal *>(r_);
  auto &v = *reinterpret_cast<ZuDecimal *>(v_);
  v = l * r;
  return v_;
}
zu_decimal *zu_decimal_div(
    zu_decimal *v_, const zu_decimal *l_, const zu_decimal *r_)
{
  const auto &l = *reinterpret_cast<const ZuDecimal *>(l_);
  const auto &r = *reinterpret_cast<const ZuDecimal *>(r_);
  auto &v = *reinterpret_cast<ZuDecimal *>(v_);
  v = l / r;
  return v_;
}
