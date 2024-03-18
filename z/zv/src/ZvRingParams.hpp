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

// ring buffer configuration

#ifndef ZvRingParams_HPP
#define ZvRingParams_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

#include <zlib/ZiRing.hpp>

#include <zlib/ZvCf.hpp>

struct ZvRingParams : public ZiRingParams {
  ZvRingParams(const ZvCf *cf) { init(cf); }
  ZvRingParams(const ZvCf *cf, ZiRingParams deflt) :
      ZiRingParams{ZuMv(deflt)} { init(cf); }

  void init(const ZvCf *cf) {
    if (!cf) return;
    name(cf->get<true>("name"));
    size(cf->getInt("size", 8192, (1U<<30U), 131072));
    ll(cf->getBool("ll"));
    spin(cf->getInt("spin", 0, INT_MAX, 1000));
    timeout(cf->getInt("timeout", 0, 3600, 1));
    killWait(cf->getInt("killWait", 0, 3600, 1));
    coredump(cf->getBool("coredump"));
  }

  ZvRingParams() = default;
  ZvRingParams(const ZiRingParams &p) : ZiRingParams{p} { }
  ZvRingParams &operator =(const ZiRingParams &p) {
    ZiRingParams::operator =(p);
    return *this;
  }
  ZvRingParams(ZiRingParams &&p) : ZiRingParams{ZuMv(p)} { }
  ZvRingParams &operator =(ZiRingParams &&p) {
    ZiRingParams::operator =(ZuMv(p));
    return *this;
  }
};

#endif /* ZvRingParams_HPP */
