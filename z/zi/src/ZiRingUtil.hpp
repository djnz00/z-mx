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

// shared memory IPC low-level functions

#ifndef ZiRingUtil_HPP
#define ZiRingUtil_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZiLib_HPP
#include <zlib/ZiLib.hpp>
#endif

#include <zlib/ZmPlatform.hpp>
#include <zlib/ZmAtomic.hpp>
#include <zlib/ZmTime.hpp>
#include <zlib/ZmRingUtil.hpp>

#include <zlib/ZePlatform.hpp>

#include <zlib/ZiPlatform.hpp>
#include <zlib/ZiFile.hpp>

namespace ZiRingUtil_ {

template <typename> class Params;
class ParamData {
  template <typename> friend Params;
public:
  ParamData(ZuString name) : m_name{name} { }

  ParamData() = default;
  ParamData(const ParamData &) = default; 
  ParamData &operator =(const ParamData &) = default;
  ParamData(ParamData &&) = default; 
  ParamData &operator =(ParamData &&) = default;

  const ZtString &name() const { return m_name; }

private:
  ZtString	m_name;
};
template <typename Derived> class Params :
    public ZmRingUtil_::Params<Derived>, public ParamData {
  ZuInline Derived &&derived() { return ZuMv(*static_cast<Derived *>(this)); }
public:
  // placeholder for optional named parameters
};

} // ZiRingUtil_

struct ZiRingUtilParams : public ZiRingUtil_::Params<ZiRingUtilParams> { };

class ZiAPI ZiRingUtil : public ZmRingUtil {
  using ParamData = ZiRingUtil_::ParamData;
  template <typename Derived> using Params = ZiRingUtil_::Params<Derived>;

public:
  ZiRingUtil() = default;
  template <typename Derived>
  ZiRingUtil(Params<Derived> params) :
      ZmRingUtil{ZuMv(params)},
      m_params{ZuMv(params)} { } // yes, moved twice

  const ParamData &params() const { return m_params; }

  int open(ZeError *e = 0);
  int close(ZeError *e = 0);

#ifdef linux
#define ZiRing_wait(index, addr, val) wait(addr, val)
#define ZiRing_wake(index, addr, n) wake(addr, n)
  // block until woken or timeout while addr == val
  int wait(ZmAtomic<uint32_t> &addr, uint32_t val);
  // wake up waiters on addr (up to n waiters are woken)
  int wake(ZmAtomic<uint32_t> &addr, unsigned n);
#endif

#ifdef _WIN32
#define ZiRing_wait(index, addr, val) wait(index, addr, val)
#define ZiRing_wake(index, addr, n) wake(index, addr, n)
  // block until woken or timeout while addr == val
  int wait(unsigned index, ZmAtomic<uint32_t> &addr, uint32_t val);
  // wake up waiters on addr (up to n waiters are woken)
  int wake(unsigned index, ZmAtomic<uint32_t> &addr, unsigned n);
#endif

  static void getpinfo(uint32_t &pid, ZmTime &start);
  static bool alive(uint32_t pid, ZmTime start);
  static bool kill(uint32_t pid, bool coredump);

protected:
  ParamData		m_params;
};

#endif /* ZiRingUtil_HPP */
