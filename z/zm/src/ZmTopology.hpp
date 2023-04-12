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

// hwloc topology

#ifndef ZmTopology_HPP
#define ZmTopology_HPP

#ifdef _MSC_VER
#pragma once
#pragma warning(push)
#pragma warning(disable:4996)
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <hwloc.h>

#include <zlib/ZmPLock.hpp>
#include <zlib/ZmAtomic.hpp>
#include <zlib/ZmCleanup.hpp>

template <typename, bool> struct ZmSingletonCtor;

class ZmAPI ZmTopology {
  ZmTopology(const ZmTopology &) = delete;
  ZmTopology &operator =(const ZmTopology &) = delete;

friend ZmSingletonCtor<ZmTopology, true>;

  ZmTopology();

public:
  ~ZmTopology();

  friend ZuConstant<ZmCleanup::Thread> ZmCleanupLevel(ZmTopology *);

private:
  static ZmTopology *instance();

public:
  static hwloc_topology_t hwloc() { return instance()->m_hwloc; }

  typedef void (*ErrorFn)(int);
  static void errorFn(ErrorFn fn);
  static void error(int errNo);

private:
  ZmPLock_		m_lock;
  hwloc_topology_t	m_hwloc;
  ZmAtomic<ErrorFn>	m_errorFn;
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZmTopology_HPP */
