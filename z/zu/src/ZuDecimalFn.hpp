//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// log10 lookup tables

#ifndef ZuDecimalFn_HPP
#define ZuDecimalFn_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
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
      v = (uint128_t)pow10_64(i - 19U) * (uint128_t)10000000000000000000ULL;
    return v;
  }

  template <unsigned I> struct Pow10 { };
  template <> struct Pow10<0U> {
    ZuInline constexpr static unsigned pow10() { return 1U; }
  };
  template <> struct Pow10<1U> {
    ZuInline constexpr static unsigned pow10() { return 10U; }
  };
  template <> struct Pow10<2U> {
    ZuInline constexpr static unsigned pow10() { return 100U; }
  };
  template <> struct Pow10<3U> {
    ZuInline constexpr static unsigned pow10() { return 1000U; }
  };
  template <> struct Pow10<4U> {
    ZuInline constexpr static unsigned pow10() { return 10000U; }
  };
  template <> struct Pow10<5U> {
    ZuInline constexpr static unsigned pow10() { return 100000U; }
  };
  template <> struct Pow10<6U> {
    ZuInline constexpr static unsigned pow10() { return 1000000U; }
  };
  template <> struct Pow10<7U> {
    ZuInline constexpr static unsigned pow10() { return 10000000U; }
  };
  template <> struct Pow10<8U> {
    ZuInline constexpr static unsigned pow10() { return 100000000U; }
  };
  template <> struct Pow10<9U> {
    ZuInline constexpr static unsigned pow10() { return 1000000000U; }
  };
  template <> struct Pow10<10U> {
    ZuInline constexpr static uint64_t pow10() { return 10000000000ULL; }
  };
  template <> struct Pow10<11U> {
    ZuInline constexpr static uint64_t pow10() { return 100000000000ULL; }
  };
  template <> struct Pow10<12U> {
    ZuInline constexpr static uint64_t pow10() { return 1000000000000ULL; }
  };
  template <> struct Pow10<13U> {
    ZuInline constexpr static uint64_t pow10() { return 10000000000000ULL; }
  };
  template <> struct Pow10<14U> {
    ZuInline constexpr static uint64_t pow10() { return 100000000000000ULL; }
  };
  template <> struct Pow10<15U> {
    ZuInline constexpr static uint64_t pow10() { return 1000000000000000ULL; }
  };
  template <> struct Pow10<16U> {
    ZuInline constexpr static uint64_t pow10() { return 10000000000000000ULL; }
  };
  template <> struct Pow10<17U> {
    ZuInline constexpr static uint64_t pow10()
      { return 100000000000000000ULL; }
  };
  template <> struct Pow10<18U> {
    ZuInline constexpr static uint64_t pow10()
      { return 1000000000000000000ULL; }
  };
  template <> struct Pow10<19U> {
    ZuInline constexpr static uint64_t pow10()
      { return 10000000000000000000ULL; }
  };
  template <unsigned I> struct Pow10_128 {
    ZuInline constexpr static uint128_t pow10() {
      return
	(uint128_t)Pow10<I - 19U>::pow10() *
	(uint128_t)10000000000000000000ULL;
    }
  };
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

#endif /* ZuDecimalFn_HPP */
