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

// exponential backoff with random perturbation

#ifndef ZmBackoff_HPP
#define ZmBackoff_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZmTime.hpp>
#include <zlib/ZmRandom.hpp>

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

#endif /* ZmBackoff_HPP */
