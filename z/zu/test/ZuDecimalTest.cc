//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <iostream>

#include <zlib/ZuStringN.hh>

#include <zlib/ZuDecimal.hh>
#include <zlib/ZuFixed.hh>
#include <zlib/zu_decimal.h>

inline void out(const char *s) { std::cout << s << '\n'; }

#define CHECK(x) ((x) ? out("OK  " #x) : out("NOK " #x))

int main()
{
  // check basic string scan
  CHECK((double)(ZuDecimal{"0"}.as_fp()) == 0.0);
  CHECK((double)(ZuDecimal{"."}.as_fp()) == 0.0);
  CHECK((double)(ZuDecimal{".0"}.as_fp()) == 0.0);
  CHECK((double)(ZuDecimal{"0."}.as_fp()) == 0.0);
  CHECK((double)(ZuDecimal{"0.0"}.as_fp()) == 0.0);
  CHECK((double)(ZuDecimal{"-0"}.as_fp()) == 0.0);
  CHECK((double)(ZuDecimal{"-."}.as_fp()) == 0.0);
  CHECK((double)(ZuDecimal{"-.0"}.as_fp()) == 0.0);
  CHECK((double)(ZuDecimal{"-0."}.as_fp()) == 0.0);
  CHECK((double)(ZuDecimal{"-0.0"}.as_fp()) == 0.0);
  CHECK((double)(ZuDecimal{"1000.42"}.as_fp()) == 1000.42);
  CHECK((double)(ZuDecimal{"-1000.42"}.as_fp()) == -1000.42);
  // check basic value scanning
  {
    auto v = ZuDecimal{"1000.42"};
    CHECK((ZuStringN<44>() << v.value) == "1000420000000000000000");
    CHECK((double)(v.as_fp()) == 1000.42);
    v = ZuDecimal{"-1000.4200000000000000001"};
    CHECK((ZuStringN<44>() << v.value) == "-1000420000000000000000");
    CHECK((double)(v.as_fp()) == -1000.42);
  }
  // check leading/trailing zeros
  CHECK((double)(ZuDecimal{"001"}.as_fp()) == 1.0);
  CHECK((double)(ZuDecimal{"1.000"}.as_fp()) == 1.0);
  CHECK((double)(ZuDecimal{"001.000"}.as_fp()) == 1.0);
  CHECK((double)(ZuDecimal{"00.100100100"}.as_fp()) == .1001001);
  CHECK((double)(ZuDecimal{"0.10010010"}.as_fp()) == .1001001);
  CHECK((double)(ZuDecimal{".1001001"}.as_fp()) == .1001001);
  {
    // check basic multiply
    double v = (ZuDecimal{"1000.42"} * ZuDecimal{2.5}).as_fp();
    CHECK(v == 2501.05);
    v = (ZuDecimal{"-1000.42"} * ZuDecimal{2.5}).as_fp();
    CHECK(v == -2501.05);
  }
  {
    // check overflow multiply
    ZuDecimal f{"10000000000000000"};
    int128_t v = (f * f).value;
    CHECK(!*ZuBoxed(v));
    f = 10;
    v = (f * f).value;
    CHECK((double)(ZuDecimal{ZuDecimal::Unscaled{v}}.as_fp()) == 100.0);
  }
  {
    // check underflow multiply
    ZuDecimal f{".000000000000000001"};
    CHECK((int)f.value == 1);
    auto v = (f * f).value;
    CHECK(!v);
    ZuDecimal g{".00000000000000001"};
    CHECK((int)g.value == 10);
    v = (g * ZuDecimal{".1"}).value;
    CHECK((ZuDecimal{ZuDecimal::Unscaled{v}}.as_fp() == .000000000000000001L));
    v = (g * ZuDecimal{".01"}).value;
    CHECK(!(int)v);
  }
  CHECK((!*ZuDecimal{""}));
  // check overflow/underflow strings
  CHECK((!*ZuDecimal{"1000000000000000000"}));
  CHECK((!ZuDecimal{".0000000000000000001"}));
  // check formatted printing
  {
    ZuStringN<60> s;
    s << ZuDecimal{"42000.42"}.fmt<ZuFmt::Comma<>>();
    CHECK(s == "42,000.42");
  }
  CHECK((ZuDecimal{".000000000000000001"}.ndp() == 18));
  CHECK((ZuDecimal{".10000000000000001"}.ndp() == 17));
  CHECK((ZuDecimal{".0000000000000001"}.ndp() == 16));
  CHECK((ZuDecimal{".100000000000001"}.ndp() == 15));
  CHECK((ZuDecimal{".00000000000001"}.ndp() == 14));
  CHECK((ZuDecimal{".1000000000001"}.ndp() == 13));
  CHECK((ZuDecimal{".000000000001"}.ndp() == 12));
  CHECK((ZuDecimal{".10000000001"}.ndp() == 11));
  CHECK((ZuDecimal{".0000000001"}.ndp() == 10));
  CHECK((ZuDecimal{".100000001"}.ndp() == 9));
  CHECK((ZuDecimal{".00000001"}.ndp() == 8));
  CHECK((ZuDecimal{".1000001"}.ndp() == 7));
  CHECK((ZuDecimal{".000001"}.ndp() == 6));
  CHECK((ZuDecimal{".10001"}.ndp() == 5));
  CHECK((ZuDecimal{".0001"}.ndp() == 4));
  CHECK((ZuDecimal{".101"}.ndp() == 3));
  CHECK((ZuDecimal{".01"}.ndp() == 2));
  CHECK((ZuDecimal{".1"}.ndp() == 1));
  CHECK((ZuDecimal{"0"}.ndp() == 0));
  CHECK((ZuDecimal{"1"}.ndp() == 0));
  CHECK((ZuDecimal{"100000000000000000"}.ndp() == 0));
  CHECK((ZuFixed{ZuDecimal("1.0001")}.ndp == 4));
  CHECK(((ZuStringN<32>{} << ZuFixed{ZuDecimal("1.0001")}) == "1.0001"));
  CHECK(((ZuStringN<32>{} << ZuFixed{ZuDecimal("0")}) == "0"));
  CHECK(((ZuStringN<32>{} << ZuFixed{ZuDecimal("1")}) == "1"));
  CHECK(((ZuStringN<48>{} << ZuFixed{ZuDecimal(".000000000000000001")}) == "0.000000000000000001"));
  CHECK(((ZuStringN<48>{} << ZuFixed{ZuDecimal("999999999999999999")}) == "999999999999999999"));

  {
    zu_decimal v_, l_, r_;
    ZuDecimal *ZuMayAlias(ptr) = reinterpret_cast<ZuDecimal *>(&v_);
    auto &v = *ptr;
    zu_decimal_in(&v_, "42.01");
    CHECK(((ZuStringN<40>{} << v) == "42.01"));
    CHECK((!zu_decimal_cmp(&v_, &v_)));
    zu_decimal_in(&l_, "42");
    zu_decimal_in(&r_, "42.010000000000000001");
    CHECK((zu_decimal_cmp(&l_, &v_) < 0));
    CHECK((zu_decimal_cmp(&v_, &r_) < 0));
    zu_decimal_add(&v_, &l_, &r_);
    CHECK(((ZuStringN<40>{} << v) == "84.010000000000000001"));
    zu_decimal_sub(&v_, &v_, &l_);
    CHECK((!zu_decimal_cmp(&v_, &r_)));
    zu_decimal_mul(&v_, &l_, &r_);
    CHECK(((ZuStringN<40>{} << v) == "1764.420000000000000042"));
    char buf[40] = { 0 };
    zu_decimal_out(buf, 39, &v_);
    CHECK(ZuString{buf} == "1764.420000000000000042");
    zu_decimal_div(&v_, &v_, &r_);
    CHECK(((ZuStringN<40>{} << v) == "42"));
  }
  {
    ZuDecimal d;
    CHECK(d.scan("0") == 1);
  }
  {
    ZuDecimal d;
    ZuDecimal e = -d;
    CHECK(!*e);
  }
  {
    ZuDecimal d{"100000000000000000"};
    ZuDecimal e{"-0.1"};
    d /= e; // overflow
    CHECK(!*d);
  }
}
