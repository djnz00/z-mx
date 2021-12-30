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

// time interval measurement

#ifndef ZmTimeInterval_HPP
#define ZmTimeInterval_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZuBox.hpp>
#include <zlib/ZuPrint.hpp>

#include <zlib/ZmLock.hpp>
#include <zlib/ZmTime.hpp>

#include <stdio.h>
#include <limits.h>

template <typename Lock>
class ZmTimeInterval {
  ZmTimeInterval(const ZmTimeInterval &);		// prevent mis-use
  ZmTimeInterval &operator =(const ZmTimeInterval &);

  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

public:
  ZmTimeInterval() : m_min(INT_MAX), m_max(0), m_total(0), m_count(0) { }

  void add(ZmTime t) {
    Guard guard(m_lock);
    if (t < m_min) m_min = t;
    if (t > m_max) m_max = t;
    m_total += t;
    m_count++;
  }

  template <typename S> void print(S &s) const {
    ZmTime min, max, total;
    ZuBox<double> mean;
    ZuBox<unsigned> count;

    stats(min, max, total, mean, count);

    s << "min=" << ZuBoxed(min.dtime()).fmt<ZuFmt::FP<9, '0'>>()
      << " max=" << ZuBoxed(max.dtime()).fmt<ZuFmt::FP<9, '0'>>()
      << " total=" << ZuBoxed(total.dtime()).fmt<ZuFmt::FP<9, '0'>>()
      << " mean=" << mean.fmt<ZuFmt::FP<9, '0'>>()
      << " count=" << count;
  }

  void stats(
      ZmTime &min, ZmTime &max, ZmTime &total,
      double &mean, unsigned &count) const {
    ReadGuard guard(m_lock);
    min = m_min, max = m_max, total = m_total,
    mean = m_total.dtime() / m_count, count = m_count;
  }
  friend ZuPrintFn ZuPrintType(ZmTimeInterval *);

private:
  Lock		m_lock;
    ZmTime	  m_min;
    ZmTime	  m_max;
    ZmTime	  m_total;
    unsigned	  m_count;
};

#endif /* ZmTimeInterval_HPP */
