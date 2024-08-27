//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Singleton with deterministic destruction sequencing

// ZmSingleton<T>::instance() returns T * pointer
//
// ZmSingleton<T, ZmSingletonNoCtor<>>::instance() can return null
// since T will not be constructed on-demand
// - use ...::instance(new T(...))
//
// T can be ZuObject-derived, but does not have to be
//
// static T v; can be replaced with:
// auto &v = *ZmSingleton<T>::instance(); // if T is unique
// auto &v = ZmStatic([]{ return new T(); }); // do not use in a header
//
// static T v(args); can be replaced with:
// auto &v = ZmStatic([]{ return new T(args...); }); // do not use in a header

#ifndef ZmSingleton_HH
#define ZmSingleton_HH

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <zlib/ZuCmp.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuLambdaTraits.hh>

#include <zlib/ZmRef.hh>
#include <zlib/ZmCleanup.hh>
#include <zlib/ZmGlobal.hh>

extern "C" {
  ZmExtern void ZmSingleton_ctor();
  ZmExtern void ZmSingleton_dtor();
}

template <typename T, bool = ZuObjectTraits<T>::IsObject>
struct ZmSingleton_ {
  void ref(T *p) { ZmREF(p); }
  void deref(T *p) { ZmDEREF(p); }
};
template <typename T> struct ZmSingleton_<T, false> {
  static void ref(T *) { }
  static void deref(T *p) { delete p; }
};

// NTP defaults
struct ZmSingleton_Defaults {
  enum { Construct = true };
  template <typename T>
  struct Ctor {
    static constexpr auto Fn = []() { return new T(); };
  };
  enum { Cleanup = ZmCleanup::Application };
};

// ZmSingletonNoCtor - do not construct
template <typename NTP = ZmSingleton_Defaults>
struct ZmSingletonNoCtor : public NTP {
  enum { Construct = false };
};

// ZmSingletonCtor - specify constructor
template <auto CtorFn_, typename NTP = ZmSingleton_Defaults>
struct ZmSingletonCtor : public NTP {
  enum { Construct = true };
  template <typename T>
  struct Ctor {
    static constexpr auto Fn = CtorFn_;
  };
};

// ZmSingletonCleanup - specify cleanup level
template <unsigned Cleanup_, typename NTP = ZmSingleton_Defaults>
struct ZmSingletonCleanup : public NTP {
  enum { Cleanup = Cleanup_ };
};

template <class T_, typename NTP = ZmSingleton_Defaults>
class ZmSingleton : public ZmGlobal, public ZmSingleton_<T_> {
  ZmSingleton(const ZmSingleton &);
  ZmSingleton &operator =(const ZmSingleton &);	// prevent mis-use

public:
  using T = T_;
  enum { Construct = NTP::Construct };
  static constexpr auto CtorFn = NTP::template Ctor<T>::Fn;
  enum { Cleanup = NTP::Cleanup };

private:
  static void final(...) { }
  template <typename U>
  static auto final(U *u) -> decltype(u->final()) {
    return u->final();
  }

  template <bool Construct_ = Construct>
  ZuIfT<Construct_> ctor() {
    T *ptr = CtorFn();
    this->ref(ptr);
    m_instance = ptr;
  }
  template <bool Construct_ = Construct>
  ZuIfT<!Construct_> ctor() { }

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
      final(ptr);
      this->deref(ptr);
    }
  }

private:
  ZmAtomic<T *>	m_instance;

  ZuInline static ZmSingleton *global() {
    return ZmGlobal::global<ZmSingleton, Cleanup>();
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
  ZuInline static T *instance() {
    return global()->m_instance.load_();
  }
  ZuInline static T *instance(T *ptr) {
    return global()->instance_(ptr);
  }
};

// ODR warning: do not use lambdas in headers outside of an inline function

template <
  unsigned Cleanup = ZmCleanup::Application,
  typename L,
  decltype(ZuStatelessLambda<L>(), int()) = 0>
inline auto &ZmStatic(L l) {
  using T = ZuDecay<decltype(*ZuDeclVal<ZuLambdaReturn<L>>())>;
  using Singleton =
    ZmSingleton<T,
      ZmSingletonCtor<ZuInvokeFn(l),
	ZmSingletonCleanup<Cleanup>>>;
  return *(Singleton::instance());
}

#endif /* ZmSingleton_HH */
