//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// exponential backoff with random perturbation

#ifndef ZmBackoff_HH
#define ZmBackoff_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <zlib/ZmTime.hh>
#include <zlib/ZmRandom.hh>

// ZmBackoff(minimum, maximum, backoff multiplier, random offset)

class ZmAPI ZmBackoff {
public:
  ZmBackoff(
      ZmTime minimum, ZmTime maximum,
      double backoff, double random) :
    m_min(ZuMv(minimum)), m_max(ZuMv(maximum)),
    m_backoff(backoff), m_random(random) { }

  ZmTime minimum() { return(m_min); }
  ZmTime maximum() { return(m_max); }

  ZmTime initial() {
    double d = m_min.dtime();
    if (m_random) d += ZmRand::rand(m_random);
    return ZmTime(d);
  }

  ZmTime backoff(const ZmTime &interval) {
    if (interval >= m_max) return m_max;
    double d = interval.dtime();
    d *= m_backoff;
    if (m_random) d += ZmRand::rand(m_random);
    if (d >= m_max.dtime()) d = m_max.dtime();
    return ZmTime(d);
  }

private:
  const ZmTime	m_min;
  const ZmTime	m_max;
  const double	m_backoff;
  const double	m_random;
};

#endif /* ZmBackoff_HH */
