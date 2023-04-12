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

// cleanup for ZmSingleton/ZmSpecific

#ifndef ZmGlobal_HPP
#define ZmGlobal_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <stdlib.h>

#include <typeinfo>
#include <typeindex>

#include <zlib/ZmPlatform.hpp>
#include <zlib/ZmAtomic.hpp>
#include <zlib/ZmCleanup.hpp>

extern "C" {
  ZmExtern void ZmGlobal_atexit();
}

#ifdef ZDEBUG
class ZmStream;
#endif

class ZmAPI ZmGlobal {
  ZmGlobal(const ZmGlobal &);
  ZmGlobal &operator =(const ZmGlobal &);	// prevent mis-use

friend ZmAPI void ZmGlobal_atexit();

public:
  virtual ~ZmGlobal() { }

protected:
  ZmGlobal() :
    m_type(typeid(void)),
#ifdef ZDEBUG
    m_name(0),
#endif
    m_next(0) { }

private:
  std::type_index	m_type;		// type index
#ifdef ZDEBUG
  const char		*m_name;	// type name
#endif
  ZmGlobal		*m_next;	// level linked list

  static ZmGlobal *add(
      std::type_index type, unsigned level, ZmGlobal *(*ctor)());
#ifdef ZDEBUG
  static void dump(ZmStream &);
#endif

  template <typename T> struct Ctor {
    static ZmGlobal *_() {
      ZmGlobal *global = static_cast<ZmGlobal *>(new T());
      const std::type_info &info = typeid(T);
      global->m_type = std::type_index(info);
#ifdef ZDEBUG
      global->m_name = info.name();
#endif
      return global;
    }
  };

protected:
  template <typename T> static T *global() {
    static uintptr_t addr_ = 0;
    ZmAtomic<uintptr_t> *ZuMayAlias(addr) =
      reinterpret_cast<ZmAtomic<uintptr_t> *>(&addr_);
    uintptr_t ptr;
    while (ZuUnlikely(!((ptr = addr->load_()) & ~1))) {
      if ((ptr == 1) || addr->cmpXch(1, 0)) {
	Zm::yield();
	continue;
      }
      *addr = ptr = reinterpret_cast<uintptr_t>(
	  ZmGlobal::add(typeid(T), decltype(ZmCleanupLevel(
		ZuDeclVal<typename T::T *>())){}, &Ctor<T>::_));
    }
    return static_cast<T *>(reinterpret_cast<ZmGlobal *>(ptr));
  }
};

#endif /* ZmGlobal_HPP */
