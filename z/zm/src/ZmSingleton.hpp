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

// Singleton with deterministic destruction sequencing

// ZmSingleton<T>::instance() returns T * pointer
//
// ZmCleanup<T>::Level determines order of destruction (per ZmCleanupLevel)
//
// ZmSingleton<T, false>::instance() can return null since T will not be
// constructed on-demand - use ZmSingleton<T, false>::instance(new T(...))
//
// T can be ZuObject-derived, but does not have to be
//
// static T v; can be replaced with:
// auto &v = *ZmSingleton<T>::instance();
// auto &v = ZmStatic([]() { return new T(); });
//
// static T v(args); can be replaced with:
// auto &v = ZmStatic([]() { return new T(args...); });

#ifndef ZmSingleton_HPP
#define ZmSingleton_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZuCmp.hpp>
#include <zlib/ZuConversion.hpp>
#include <zlib/ZuFunctorTraits.hpp>

#include <zlib/ZmRef.hpp>
#include <zlib/ZmCleanup.hpp>
#include <zlib/ZmGlobal.hpp>

extern "C" {
  ZmExtern void ZmSingleton_ctor();
  ZmExtern void ZmSingleton_dtor();
}

template <typename T, bool IsObject = ZuIsObject_<T>::OK> struct ZmSingleton_ {
  void ref(T *p) { ZmREF(p); }
  void deref(T *p) { ZmDEREF(p); }
};
template <typename T> struct ZmSingleton_<T, false> {
  static void ref(T *) { }
  static void deref(T *p) { delete p; }
};

template <typename T, bool Construct = true> struct ZmSingletonCtor {
  static T *fn() { return new T{}; }
};
template <typename T> struct ZmSingletonCtor<T, false> {
  static T *fn() { return nullptr; }
};
template <typename T_, bool Construct_ = true,
  auto CtorFn = ZmSingletonCtor<T_, Construct_>::fn>
class ZmSingleton : public ZmGlobal, public ZmSingleton_<T_> {
  ZmSingleton(const ZmSingleton &);
  ZmSingleton &operator =(const ZmSingleton &);	// prevent mis-use

public:
  using T = T_;

private:
  static void final(...) { }
  template <typename U>
  static auto final(U *u) -> decltype(u->final()) {
    return u->final();
  }

  template <bool Construct = Construct_>
  ZuIfT<Construct> ctor() {
    T *ptr = CtorFn();
    this->ref(ptr);
    m_instance = ptr;
  }
  template <bool Construct = Construct_>
  ZuIfT<!Construct> ctor() { }

public:
  ZmSingleton() {
#ifdef ZDEBUG
    ZmSingleton_ctor();
#endif
    ctor<>();
  };

  ~ZmSingleton() {
#ifdef ZDEBUG
    ZmSingleton_dtor();
#endif
    if (T *ptr = m_instance.load_()) {
      final(ptr); // calls ZmCleanup<T>::final() if it exists
      this->deref(ptr);
    }
  }

private:
  ZmAtomic<T *>	m_instance;

  ZuInline static ZmSingleton *global() {
    return ZmGlobal::global<ZmSingleton>();
  }

  T *instance_(T *ptr) {
    this->ref(ptr);
    if (T *old = m_instance.xch(ptr)) {
      final(old);
      this->deref(old);
    }
    return ptr;
  }

public:
  static T *instance() {
    return global()->m_instance.load_();
  }
  static T *instance(T *ptr) {
    return global()->instance_(ptr);
  }
};

// ODR warning: do not use lambdas in headers outside of an inline function

template <typename L>
inline auto &ZmStatic(L l, ZuIsStateless<L> *_ = nullptr) {
  using T = ZuDecay<decltype(*ZuFunctorTraits<L>::invoke())>;
  return *(ZmSingleton<T, true, ZuFunctorTraits<L>::fn()>::instance());
}

#endif /* ZmSingleton_HPP */
