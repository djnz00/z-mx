//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// log10 lookup tables

#ifndef ZuDecimalFn_HH
#define ZuDecimalFn_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#ifdef _MSC_VER
#pragma once
#endif

namespace ZuDecimalFn {
  ZuInline const unsigned pow10_32(unsigned i) {
    constexpr static unsigned pow10[] = {
      1U,
      10U,
      100U,
      1000U,
      10000U,
      100000U,
      1000000U,
      10000000U,
      100000000U,
      1000000000U
    };
    return pow10[i];
  }

  ZuInline const uint64_t pow10_64(unsigned i) {
    constexpr static uint64_t pow10[] = {
      1ULL,
      10ULL,
      100ULL,
      1000ULL,
      10000ULL,
      100000ULL,
      1000000ULL,
      10000000ULL,
      100000000ULL,
      1000000000ULL,
      10000000000ULL,
      100000000000ULL,
      1000000000000ULL,
      10000000000000ULL,
      100000000000000ULL,
      1000000000000000ULL,
      10000000000000000ULL,
      100000000000000000ULL,
      1000000000000000000ULL,
      10000000000000000000ULL
    };
    return pow10[i];
  }

  ZuInline const uint128_t pow10_128(unsigned i) {
    uint128_t v;
    if (ZuLikely(i < 20U))
      v = pow10_64(i);
    else
      v = static_cast<uint128_t>(pow10_64(i - 19U)) *
	  static_cast<uint128_t>(10000000000000000000ULL);
    return v;
  }

  template <unsigned I> struct Pow10 { };
  template <> struct Pow10<0U> : public ZuConstant<unsigned, 1U> { };
  template <> struct Pow10<1U> : public ZuConstant<unsigned, 10U> { };
  template <> struct Pow10<2U> : public ZuConstant<unsigned, 100U> { };
  template <> struct Pow10<3U> : public ZuConstant<unsigned, 1000U> { };
  template <> struct Pow10<4U> : public ZuConstant<unsigned, 10000U> { };
  template <> struct Pow10<5U> : public ZuConstant<unsigned, 100000U> { };
  template <> struct Pow10<6U> : public ZuConstant<unsigned, 1000000U> { };
  template <> struct Pow10<7U> : public ZuConstant<unsigned, 10000000U> { };
  template <> struct Pow10<8U> : public ZuConstant<unsigned, 100000000U> { };
  template <> struct Pow10<9U> : public ZuConstant<unsigned, 1000000000U> { };
  template <> struct Pow10<10U> :
    public ZuConstant<uint64_t, 10000000000ULL> { };
  template <> struct Pow10<11U> :
    public ZuConstant<uint64_t, 100000000000ULL> { };
  template <> struct Pow10<12U> :
    public ZuConstant<uint64_t, 1000000000000ULL> { };
  template <> struct Pow10<13U> :
    public ZuConstant<uint64_t, 10000000000000ULL> { };
  template <> struct Pow10<14U> :
    public ZuConstant<uint64_t, 100000000000000ULL> { };
  template <> struct Pow10<15U> :
    public ZuConstant<uint64_t, 1000000000000000ULL> { };
  template <> struct Pow10<16U> :
    public ZuConstant<uint64_t, 10000000000000000ULL> { };
  template <> struct Pow10<17U> :
    public ZuConstant<uint64_t, 100000000000000000ULL> { };
  template <> struct Pow10<18U> :
    public ZuConstant<uint64_t, 1000000000000000000ULL> { };
  template <> struct Pow10<19U> :
    public ZuConstant<uint64_t, 10000000000000000000ULL> { };
  template <unsigned I> struct Pow10_128 :
    public ZuConstant<uint128_t,
      static_cast<uint128_t>(Pow10<I - 19U>{}) *
      static_cast<uint128_t>(10000000000000000000ULL)> { };
  template <> struct Pow10<20U> : public Pow10_128<20U> { };
  template <> struct Pow10<21U> : public Pow10_128<21U> { };
  template <> struct Pow10<22U> : public Pow10_128<22U> { };
  template <> struct Pow10<23U> : public Pow10_128<23U> { };
  template <> struct Pow10<24U> : public Pow10_128<24U> { };
  template <> struct Pow10<25U> : public Pow10_128<25U> { };
  template <> struct Pow10<26U> : public Pow10_128<26U> { };
  template <> struct Pow10<27U> : public Pow10_128<27U> { };
  template <> struct Pow10<28U> : public Pow10_128<28U> { };
  template <> struct Pow10<29U> : public Pow10_128<29U> { };
  template <> struct Pow10<30U> : public Pow10_128<30U> { };
  template <> struct Pow10<31U> : public Pow10_128<31U> { };
  template <> struct Pow10<32U> : public Pow10_128<32U> { };
  template <> struct Pow10<33U> : public Pow10_128<33U> { };
  template <> struct Pow10<34U> : public Pow10_128<34U> { };
  template <> struct Pow10<35U> : public Pow10_128<35U> { };
  template <> struct Pow10<36U> : public Pow10_128<36U> { };
  template <> struct Pow10<37U> : public Pow10_128<37U> { };
  template <> struct Pow10<38U> : public Pow10_128<38U> { };
  template <> struct Pow10<39U> : public Pow10_128<39U> { };
}

#endif /* ZuDecimalFn_HH */
