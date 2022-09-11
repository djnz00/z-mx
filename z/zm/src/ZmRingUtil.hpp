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

// ring buffer common utility functions

#ifndef ZmRingUtil_HPP
#define ZmRingUtil_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZmPlatform.hpp>
#include <zlib/ZmAssert.hpp>
#include <zlib/ZmBitmap.hpp>
#include <zlib/ZmTopology.hpp>
#include <zlib/ZmLock.hpp>
#include <zlib/ZmGuard.hpp>
#include <zlib/ZmAtomic.hpp>
#include <zlib/ZmTime.hpp>
#include <zlib/ZmBackTrace.hpp>

namespace ZmRingUtil_ {

template <typename> class Params;
class ParamData {
  template <typename> friend Params;
public:
  unsigned spin() const { return m_spin; }
  unsigned timeout() const { return m_timeout; }

private:
  unsigned	m_spin = 1000;
  unsigned	m_timeout = 1;
};
template <typename Derived> class Params : public ParamData {
  ZuInline Derived &&derived() { return ZuMv(*static_cast<Derived *>(this)); }
public:
  Derived &&spin(unsigned n) { m_spin = n; return derived(); }
  Derived &&timeout(unsigned n) { m_timeout = n; return derived(); }
};

} // ZmRingUtil_

struct ZmRingUtilParams : public ZmRingUtil_::Params<ZmRingUtilParams> { };

class ZmAPI ZmRingUtil {
  using ParamData = ZmRingUtil_::ParamData;
  template <typename Derived> using Params = ZmRingUtil_::Params<Derived>;

public:
  ZmRingUtil() = default;
  template <typename Derived>
  ZmRingUtil(Params<Derived> params) : m_params{ZuMv(params)} { }

  const ParamData &params() const { return m_params; }

  enum { OK = 0, EndOfFile = -1, Error = -2, NotReady = -3 };

  enum { Head = 0, Tail };

  int open();
  int close();

#ifdef linux
#define ZmRing_wait(index, addr, val) wait(addr, val)
#define ZmRing_wake(index, addr, n) wake(addr, n)
  // block until woken or timeout while addr == val
  int wait(ZmAtomic<uint32_t> &addr, uint32_t val);
  // wake up waiters on addr (up to n waiters are woken)
  int wake(ZmAtomic<uint32_t> &addr, unsigned n);
#endif

#ifdef _WIN32
#define ZmRing_wait(index, addr, val) wait(index, addr, val)
#define ZmRing_wake(index, addr, n) wake(index, addr, n)
  // block until woken or timeout while addr == val
  int wait(unsigned index, ZmAtomic<uint32_t> &addr, uint32_t val);
  // wake up waiters on addr (up to n waiters are woken)
  int wake(unsigned index, ZmAtomic<uint32_t> &addr, unsigned n);
#endif

protected:
  ParamData		m_params;
#ifdef _WIN32
  HANDLE		m_sem[2] = { 0, 0 };
#endif
};

#endif /* ZmRingUtil_HPP */
