//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2

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

// lock traits

#ifndef ZmLockTraits_HPP
#define ZmLockTraits_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

template <class Lock_> struct ZmGenericLockTraits {
  typedef Lock_ Lock;
  enum { CanTry = 1, Recursive = 1, RWLock = 0 };
  ZuInline static void lock(Lock &l) { l.lock(); }
  ZuInline static int trylock(Lock &l) { return l.trylock(); }
  ZuInline static void unlock(Lock &l) { l.unlock(); }
  ZuInline static void readlock(Lock &l) { l.lock(); }
  ZuInline static int readtrylock(Lock &l) { return l.trylock(); }
  ZuInline static void readunlock(Lock &l) { l.unlock(); }
};

template <class Lock>
struct ZmLockTraits : public ZmGenericLockTraits<Lock> { };

#endif /* ZmLockTraits_HPP */
