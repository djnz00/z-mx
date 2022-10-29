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

/* disabled lock */

#ifndef ZmNoLock_HPP
#define ZmNoLock_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZmLockTraits.hpp>

class ZmNoLock {
  ZmNoLock(const ZmNoLock &);
  ZmNoLock &operator =(const ZmNoLock &);	// prevent mis-use

public:
  ZuInline ZmNoLock() { }

  ZuInline void lock() { }
  ZuInline int trylock() { return 0; }
  ZuInline void unlock() { }

  // ZmCondition integration
  struct Wait { };
  Wait wait() { return {}; }
  ZuInline void lock_() { };
  ZuInline void unlock_() { };
};

template <>
struct ZmLockTraits<ZmNoLock> : public ZmGenericLockTraits<ZmNoLock> {
  ZuInline static void lock(ZmNoLock &l) { }
  ZuInline static int trylock(ZmNoLock &l) { return 0; }
  ZuInline static void unlock(ZmNoLock &l) { }
  ZuInline static void readlock(ZmNoLock &l) { }
  ZuInline static int readtrylock(ZmNoLock &l) { return 0; }
  ZuInline static void readunlock(ZmNoLock &l) { }
};

template <class Lock> class ZmGuard;
template <class Lock> class ZmReadGuard;
template <> class ZmGuard<ZmNoLock> {
public:
  ZuInline ZmGuard(ZmNoLock &) { }
  ZuInline ZmGuard(const ZmGuard &guard) { }
  ZuInline ~ZmGuard() { }

  ZuInline void unlock() { }

  ZuInline ZmGuard &operator =(const ZmGuard &guard) { return *this; }
};

template <> class ZmReadGuard<ZmNoLock> {
public:
  ZuInline ZmReadGuard(const ZmNoLock &) { }
  ZuInline ZmReadGuard(const ZmReadGuard &guard) { }
  ZuInline ~ZmReadGuard() { }

  ZuInline void unlock() { }

  ZuInline ZmReadGuard &operator =(const ZmReadGuard &guard) { return *this; }
};

#endif /* ZmNoLock_HPP */
