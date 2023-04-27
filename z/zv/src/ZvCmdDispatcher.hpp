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

// ZvCmd message dispatcher

#ifndef ZvCmdDispatcher_HPP
#define ZvCmdDispatcher_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

#include <zlib/ZuArray.hpp>
#include <zlib/ZuID.hpp>

#include <zlib/ZmFn.hpp>
#include <zlib/ZmRef.hpp>
#include <zlib/ZmPolymorph.hpp>
#include <zlib/ZmNoLock.hpp>
#include <zlib/ZmLHash.hpp>

class ZvAPI ZvCmdDispatcher {
public:
  using Fn = ZmFn<void *, const uint8_t *, unsigned>;
  using DefltFn = ZmFn<void *, ZuID, const uint8_t *, unsigned>;

  void init();
  void final();

  void map(ZuID id, Fn fn);
  void deflt(DefltFn fn);

  int dispatch(ZuID id, void *link, const uint8_t *data, unsigned len);

private:
  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;

  static const char *FnMapID() { return "ZvCmdDispatcher.FnMap"; }
  using FnMap = ZmLHashKV<ZuID, Fn, ZmLHashID<FnMapID, ZmLHashLocal<>>>;

  Lock			m_lock;
    FnMap		  m_fnMap;
    DefltFn		  m_defltFn;
};

#endif /* ZvCmdDispatcher_HPP */
