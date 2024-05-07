//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <iostream>

#include <zlib/ZuStringN.hh>

#include <zlib/ZuDecimal.hh>

#define CHECK(x) ((x) ? puts("OK  " #x) : puts("NOK " #x))

int main()
{
  // check basic string scan
  CHECK((double)(ZuDecimal{"0"}.fp()) == 0.0);
  CHECK((double)(ZuDecimal{"."}.fp()) == 0.0);
  CHECK((double)(ZuDecimal{".0"}.fp()) == 0.0);
  CHECK((double)(ZuDecimal{"0."}.fp()) == 0.0);
  CHECK((double)(ZuDecimal{"0.0"}.fp()) == 0.0);
  CHECK((double)(ZuDecimal{"-0"}.fp()) == 0.0);
  CHECK((double)(ZuDecimal{"-."}.fp()) == 0.0);
  CHECK((double)(ZuDecimal{"-.0"}.fp()) == 0.0);
  CHECK((double)(ZuDecimal{"-0."}.fp()) == 0.0);
  CHECK((double)(ZuDecimal{"-0.0"}.fp()) == 0.0);
  CHECK((double)(ZuDecimal{"1000.42"}.fp()) == 1000.42);
  CHECK((double)(ZuDecimal{"-1000.42"}.fp()) == -1000.42);
  // check basic value scanning
  {
    auto v = ZuDecimal{"1000.42"};
    CHECK((ZuStringN<44>() << v.value) == "1000420000000000000000");
    CHECK((double)(v.fp()) == 1000.42);
    v = ZuDecimal{"-1000.4200000000000000001"};
    CHECK((ZuStringN<44>() << v.value) == "-1000420000000000000000");
    CHECK((double)(v.fp()) == -1000.42);
  }
  // check leading/trailing zeros
  CHECK((double)(ZuDecimal{"001"}.fp()) == 1.0);
  CHECK((double)(ZuDecimal{"1.000"}.fp()) == 1.0);
  CHECK((double)(ZuDecimal{"001.000"}.fp()) == 1.0);
  CHECK((double)(ZuDecimal{"00.100100100"}.fp()) == .1001001);
  CHECK((double)(ZuDecimal{"0.10010010"}.fp()) == .1001001);
  CHECK((double)(ZuDecimal{".1001001"}.fp()) == .1001001);
  {
    // check basic multiply
    double v = (ZuDecimal{"1000.42"} * ZuDecimal{2.5}).fp();
    CHECK(v == 2501.05);
    v = (ZuDecimal{"-1000.42"} * ZuDecimal{2.5}).fp();
    CHECK(v == -2501.05);
  }
  {
    // check overflow multiply
    ZuDecimal f{"10000000000000000"};
    int128_t v = (f * f).value;
    CHECK(!*ZuBoxed(v));
    f = 10;
    v = (f * f).value;
    CHECK((double)(ZuDecimal{ZuDecimal::Unscaled, v}.fp()) == 100.0);
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
    CHECK((ZuDecimal{ZuDecimal::Unscaled, v}.fp() == .000000000000000001L));
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
    s << ZuDecimal{"42000.42"}.fmt(ZuFmt::Comma<>());
    CHECK(s == "42,000.42");
  }
  CHECK((ZuDecimal{".000000000000000001"}.exponent() == 18));
  CHECK((ZuDecimal{".10000000000000001"}.exponent() == 17));
  CHECK((ZuDecimal{".0000000000000001"}.exponent() == 16));
  CHECK((ZuDecimal{".100000000000001"}.exponent() == 15));
  CHECK((ZuDecimal{".00000000000001"}.exponent() == 14));
  CHECK((ZuDecimal{".1000000000001"}.exponent() == 13));
  CHECK((ZuDecimal{".000000000001"}.exponent() == 12));
  CHECK((ZuDecimal{".10000000001"}.exponent() == 11));
  CHECK((ZuDecimal{".0000000001"}.exponent() == 10));
  CHECK((ZuDecimal{".100000001"}.exponent() == 9));
  CHECK((ZuDecimal{".00000001"}.exponent() == 8));
  CHECK((ZuDecimal{".1000001"}.exponent() == 7));
  CHECK((ZuDecimal{".000001"}.exponent() == 6));
  CHECK((ZuDecimal{".10001"}.exponent() == 5));
  CHECK((ZuDecimal{".0001"}.exponent() == 4));
  CHECK((ZuDecimal{".101"}.exponent() == 3));
  CHECK((ZuDecimal{".01"}.exponent() == 2));
  CHECK((ZuDecimal{".1"}.exponent() == 1));
  CHECK((ZuDecimal{"0"}.exponent() == 0));
  CHECK((ZuDecimal{"1"}.exponent() == 0));
  CHECK((ZuDecimal{"100000000000000000"}.exponent() == 0));
  CHECK((ZuFixed{ZuDecimal("1.0001")}.exponent() == 4));
  CHECK(((ZuStringN<32>{} << ZuFixed{ZuDecimal("1.0001")}) == "1.0001"));
  CHECK(((ZuStringN<32>{} << ZuFixed{ZuDecimal("0")}) == "0"));
  CHECK(((ZuStringN<32>{} << ZuFixed{ZuDecimal("1")}) == "1"));
  CHECK(((ZuStringN<48>{} << ZuFixed{ZuDecimal(".000000000000000001")}) == "0.000000000000000001"));
  CHECK(((ZuStringN<48>{} << ZuFixed{ZuDecimal("999999999999999999")}) == "999999999999999999"));
}
