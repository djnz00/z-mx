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

// guard template for locks

#ifndef ZmGuard_HPP
#define ZmGuard_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZmLockTraits.hpp>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4800)
#endif

template <class Lock> class ZmGuard : private ZmLockTraits<Lock> {
  using Traits = ZmLockTraits<Lock>;

public:
  enum Try_ { Try };	// disambiguator

  ZmGuard() : m_lock{nullptr} { }

  ZmGuard(Lock &l) : m_lock(&l) { this->lock(l); }

  explicit ZmGuard(Lock &l, Try_ _) : m_lock(&l) {
    if (this->trylock(l)) m_lock = nullptr;
  }
  explicit ZmGuard(Lock &l, Try_ _, int &r) : m_lock(&l) {
    if (r = this->trylock(l)) m_lock = nullptr;
  }

  ZmGuard(ZmGuard &&guard) : m_lock(guard.m_lock) {
    guard.m_lock = nullptr;
  }
  ZmGuard &operator =(ZmGuard &&guard) {
    if (ZuLikely(this != &guard)) {
      if (m_lock) Traits::unlock(*m_lock);
      m_lock = guard.m_lock;
      guard.m_lock = nullptr;
    }
    return *this;
  }

  ~ZmGuard() { if (m_lock) Traits::unlock(*m_lock); }

  bool locked() const { return m_lock; }

  void unlock() {
    if (m_lock) { Traits::unlock(*m_lock); m_lock = nullptr; }
  }

private:
  Lock		*m_lock;
};

template <typename Lock> ZmGuard(Lock &) -> ZmGuard<Lock>;

#ifdef _MSC_VER
#pragma warning(pop)
#endif

template <class Lock> class ZmReadGuard : private ZmLockTraits<Lock> {
  using Traits = ZmLockTraits<Lock>;

public:
  enum Try_ { Try };	// disambiguator

  ZmReadGuard(const Lock &l) :
    m_lock(&(const_cast<Lock &>(l))) {
    this->readlock(const_cast<Lock &>(l));
  }
  explicit ZmReadGuard(const Lock &l, Try_ _) :
      m_lock(&(const_cast<Lock &>(l))) {
    if (this->tryreadlock(const_cast<Lock &>(l))) m_lock = nullptr;
  }
  explicit ZmReadGuard(const Lock &l, Try_ _, int &r) :
      m_lock(&(const_cast<Lock &>(l))) {
    if (r = this->tryreadlock(const_cast<Lock &>(l))) m_lock = nullptr;
  }
  ZmReadGuard(ZmReadGuard &&guard) : m_lock(guard.m_lock) {
    guard.m_lock = nullptr;
  }
  ~ZmReadGuard() { if (m_lock) Traits::readunlock(*m_lock); }

  void unlock() {
    if (m_lock) { Traits::readunlock(*m_lock); m_lock = nullptr; }
  }

  ZmReadGuard &operator =(ZmReadGuard &&guard) {
    if (ZuLikely(this != &guard)) {
      if (m_lock) Traits::readunlock(*m_lock);
      m_lock = guard.m_lock;
      guard.m_lock = nullptr;
    }
    return *this;
  }

private:
  Lock		*m_lock;
};

#endif /* ZmGuard_HPP */
