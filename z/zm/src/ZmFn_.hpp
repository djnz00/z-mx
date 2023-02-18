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

// function delegates for multithreading and deferred execution (callbacks)

#ifndef ZmFn__HPP
#define ZmFn__HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuFunctorTraits.hpp>
#include <zlib/ZuConversion.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuHash.hpp>
#include <zlib/ZuNull.hpp>

#include <zlib/ZmRef.hpp>
#include <zlib/ZmPolymorph.hpp>

template <typename ...Args> class ZmFn;

template <typename T> constexpr uintptr_t ZmFn_Cast(T v) {
  return static_cast<uintptr_t>(v);
}
template <typename T> constexpr uintptr_t ZmFn_Cast(T *v) {
  return reinterpret_cast<uintptr_t>(v);
}

// ZmFn base class
class ZmAnyFn {
  struct Pass { };

  // 64bit pointer-packing - uses bit 63
  constexpr static const uintptr_t Owned = (static_cast<uintptr_t>(1)<<63);

protected:
  constexpr static bool owned(uintptr_t o) { return o & Owned; }
  static uintptr_t own(uintptr_t o) { return o | Owned; }
  static uintptr_t disown(uintptr_t o) { return o & ~Owned; }

  template <typename O = ZmPolymorph>
  static O *ptr(uintptr_t o) {
    return reinterpret_cast<O *>(o & ~Owned);
  }

public:
  ZmAnyFn() : m_invoker(0), m_object(0) { }

  ~ZmAnyFn() {
    if (ZuUnlikely(owned(m_object))) ZmDEREF(ptr(m_object));
  }

  ZmAnyFn(const ZmAnyFn &fn) :
      m_invoker(fn.m_invoker), m_object(fn.m_object) {
    if (ZuUnlikely(owned(m_object))) ZmREF(ptr(m_object));
  }

  ZmAnyFn(ZmAnyFn &&fn) :
      m_invoker(fn.m_invoker), m_object(fn.m_object) {
    // fn.m_object = disown(fn.m_object);
    fn.m_invoker = fn.m_object = 0;
#ifdef ZmObject_DEBUG
    if (ZuUnlikely(owned(m_object))) ZmMVREF(ptr(m_object), &fn, this);
#endif
  }

  ZmAnyFn &operator =(const ZmAnyFn &fn) {
    if (ZuLikely(this != &fn)) {
      if (ZuUnlikely(owned(fn.m_object))) ZmREF(ptr(fn.m_object));
      if (ZuUnlikely(owned(m_object))) ZmDEREF(ptr(m_object));
      m_invoker = fn.m_invoker;
      m_object = fn.m_object;
    }
    return *this;
  }

  ZmAnyFn &operator =(ZmAnyFn &&fn) {
    if (ZuUnlikely(owned(m_object))) ZmDEREF(ptr(m_object));
    m_invoker = fn.m_invoker;
    m_object = fn.m_object;
    // fn.m_object = disown(fn.m_object);
    fn.m_invoker = fn.m_object = 0;
#ifdef ZmObject_DEBUG
    if (ZuUnlikely(owned(m_object))) ZmMVREF(ptr(m_object), &fn, this);
#endif
    return *this;
  }

  // accessed from ZmFn<...>
protected:
  template <typename Invoker, typename O>
  ZmAnyFn(const Invoker &invoker, O *o) :
      m_invoker{reinterpret_cast<uintptr_t>(invoker)},
      m_object((uintptr_t)o) { }
  template <typename Invoker, typename O>
  ZmAnyFn(const Invoker &invoker, ZmRef<O> o,
      ZuIsBase<ZmPolymorph, O, Pass> *_ = nullptr) :
	m_invoker{reinterpret_cast<uintptr_t>(invoker)} {
    new (&m_object) ZmRef<O>(ZuMv(o));
    m_object = own(m_object);
  }

public:
  // downcast to ZmFn<...>
  template <typename Fn> const Fn &as() const {
    return *static_cast<const Fn *>(this);
  }
  template <typename Fn> Fn &as() {
    return *static_cast<Fn *>(this);
  }

  // access captured object
  template <typename O> O *object() const {
    return ptr<O>(m_object);
  }
  template <typename O> ZmRef<O> mvObject() {
    if (ZuUnlikely(!owned(m_object))) return ZmRef<O>{object<O>()};
    m_object = disown(m_object);
    return ZmRef<O>::acquire(object<O>());
  }
  template <typename O> void object(O *o) {
    if (ZuUnlikely(owned(m_object))) ZmDEREF(ptr(m_object));
    m_object = reinterpret_cast<uintptr_t>(o);
  }
  template <typename O> void object(ZmRef<O> o) {
    if (ZuLikely(owned(m_object))) ZmDEREF(ptr(m_object));
    new (&m_object) ZmRef<O>(ZuMv(o));
    m_object = own(m_object);
  }

  // access invoker
  uintptr_t invoker() const { return m_invoker; }

  bool equals(const ZmAnyFn &fn) const {
    return m_invoker == fn.m_invoker && m_object == fn.m_object;
  }
  int cmp(const ZmAnyFn &fn) const {
    if (m_invoker < fn.m_invoker) return -1;
    if (m_invoker > fn.m_invoker) return 1;
    return ZuCmp<uintptr_t>::cmp(m_object, fn.m_object);
  }
  friend inline bool operator ==(const ZmAnyFn &l, const ZmAnyFn &r) {
    return l.equals(r);
  }
  friend inline int operator <=>(const ZmAnyFn &l, const ZmAnyFn &r) {
    return l.cmp(r);
  }

  bool operator !() const { return !m_invoker; }
  ZuOpBool

  uint32_t hash() const {
    return
      ZuHash<uintptr_t>::hash(m_invoker) ^ ZuHash<uintptr_t>::hash(m_object);
  }

protected:
  uintptr_t		m_invoker;
  mutable uintptr_t	m_object;
};

inline const char *ZmLambda_HeapID() { return "ZmLambda"; }

template <typename ...Args> class ZmFn : public ZmAnyFn {
  typedef uintptr_t (*Invoker)(uintptr_t &, Args...);
  template <bool VoidRet, auto> struct FnInvoker;
  template <typename, bool VoidRet, auto> struct BoundInvoker;
  template <typename, bool VoidRet, auto> struct MemberInvoker;
  template <typename, auto HeapID, bool Sharded> struct LambdaInvoker;
  template <typename, auto HeapID, bool Sharded> struct LBoundInvoker;

  template <typename T> struct IsFunctor_ { enum { OK = 0 }; };
  template <typename L, typename R, typename ...Args_>
    struct IsFunctor_<R (L::*)(Args_...) const> { enum { OK = 1 }; };
  template <typename L, typename R, typename ...Args_>
    struct IsFunctor_<R (L::*)(Args_...)> { enum { OK = 1 }; };
  template <typename T, typename = void>
  struct IsFunctor { enum { OK = 0 }; };
  template <typename T>
  struct IsFunctor<T, decltype(&T::operator(), void())> {
    enum { OK =
      !ZuConversion<ZmAnyFn, T>::Is &&
      IsFunctor_<decltype(&T::operator())>::OK
    };
  };
  template <typename T, typename R = void>
  using MatchFunctor = ZuIfT<IsFunctor<T>::OK, R>;

public:
  ZmFn() : ZmAnyFn() { }
  ZmFn(const ZmFn &fn) : ZmAnyFn(fn) { }
  ZmFn(ZmFn &&fn) : ZmAnyFn(static_cast<ZmAnyFn &&>(fn)) { }

private:
  class Pass {
    friend ZmFn;
    Pass &operator =(const Pass &) = delete;
  };
  template <typename ...Args_>
  ZmFn(Pass, Args_ &&...args) : ZmAnyFn(ZuFwd<Args_>(args)...) { }

public:
  // syntactic sugar for lambdas
  template <typename L>
  ZmFn(L &&l, MatchFunctor<L> *_ = nullptr) :
    ZmAnyFn(fn(ZuFwd<L>(l))) { }
  template <typename O, typename L>
  ZmFn(O &&o, L &&l, MatchFunctor<L> *_ = nullptr) :
    ZmAnyFn(fn(ZuFwd<O>(o), ZuFwd<L>(l))) { }

  ZmFn &operator =(const ZmFn &fn) {
    ZmAnyFn::operator =(fn);
    return *this;
  }
  ZmFn &operator =(ZmFn &&fn) {
    ZmAnyFn::operator =(static_cast<ZmAnyFn &&>(fn));
    return *this;
  }

private:
  ZmFn &operator =(const ZmAnyFn &fn) {
    ZmAnyFn::operator =(fn);
    return *this;
  }
  ZmFn &operator =(ZmAnyFn &&fn) {
    ZmAnyFn::operator =(static_cast<ZmAnyFn &&>(fn));
    return *this;
  }

public:
  template <typename ...Args_>
  uintptr_t operator ()(Args_ &&... args) const {
    if (ZmAnyFn::operator !()) return 0;
    return (*reinterpret_cast<Invoker>(m_invoker))(
	m_object, ZuFwd<Args_>(args)...);
  }

  // plain function pointer
  template <auto Fn> struct Ptr;
  template <typename R, R (*Fn)(Args...)> struct Ptr<Fn> {
    static ZmFn fn() {
      return ZmFn{ZmFn::Pass{},
	  &FnInvoker<ZuConversion<void, R>::Same, Fn>::invoke,
	  static_cast<void *>(nullptr)};
    }
  };

  // bound function pointer
  template <auto Fn> struct Bound;
  template <typename C, typename R, R (*Fn)(C *, Args...)>
  struct Bound<Fn> {
    template <typename O> static ZmFn fn(O *o) {
      return ZmFn{ZmFn::Pass{},
	  &BoundInvoker<O *, ZuConversion<void, R>::Same, Fn>::invoke, o};
    }
    template <typename O> static ZmFn fn(ZmRef<O> o) {
      return ZmFn{ZmFn::Pass{},
	  &BoundInvoker<ZmRef<O>, ZuConversion<void, R>::Same, Fn>::invoke,
	  ZuMv(o)};
    }
    template <typename O> static ZmFn mvFn(ZmRef<O> o) {
      return ZmFn{ZmFn::Pass{},
	  &BoundInvoker<ZmRef<O>, ZuConversion<void, R>::Same, Fn>::invoke,
	  ZuMv(o)};
    }
  };
  template <typename O, typename R, R (*Fn)(ZmRef<O>, Args...)>
  struct Bound<Fn> {
    static ZmFn fn(O *o) {
      return ZmFn{ZmFn::Pass{},
	  &BoundInvoker<O *, ZuConversion<void, R>::Same, Fn>::invoke, o};
    }
    static ZmFn fn(ZmRef<O> o) {
      return ZmFn{ZmFn::Pass{},
	  &BoundInvoker<ZmRef<O>, ZuConversion<void, R>::Same, Fn>::invoke,
	  ZuMv(o)};
    }
    static ZmFn mvFn(ZmRef<O> o) {
      return ZmFn{ZmFn::Pass{},
	  &BoundInvoker<ZmRef<O>, ZuConversion<void, R>::Same, Fn>::mvInvoke,
	  ZuMv(o)};
    }
  };

  // member function
  template <auto Fn> struct Member;
  template <typename C, typename R, R (C::*Fn)(Args...)>
  struct Member<Fn> {
    template <typename O> static ZmFn fn(O *o) {
      return ZmFn{ZmFn::Pass{},
	  &MemberInvoker<O *, ZuConversion<void, R>::Same, Fn>::invoke, o};
    }
    template <typename O> static ZmFn fn(ZmRef<O> o) {
      return ZmFn{ZmFn::Pass{},
	  &MemberInvoker<O *, ZuConversion<void, R>::Same, Fn>::invoke,
	  ZuMv(o)};
    }
  };
  template <typename C, typename R, R (C::*Fn)(Args...) const>
  struct Member<Fn> {
    template <typename O> static ZmFn fn(O *o) {
      return ZmFn{ZmFn::Pass{},
	  &MemberInvoker<const O *, ZuConversion<void, R>::Same, Fn>::invoke,
	  o};
    }
    template <typename O> static ZmFn fn(ZmRef<O> o) {
      return ZmFn{ZmFn::Pass{},
	  &MemberInvoker<const O *, ZuConversion<void, R>::Same, Fn>::invoke,
	  ZuMv(o)};
    }
  };

  // lambdas
  template <auto HeapID, bool Sharded> struct Lambda;
  template <typename Arg0, typename ...Args_>
  static ZmFn fn(Arg0 &&arg0, Args_ &&... args) {
    return Lambda<ZmLambda_HeapID>::fn(
	ZuFwd<Arg0>(arg0), ZuFwd<Args_>(args)...);
  }
  template <typename Arg0, typename ...Args_>
  static ZmFn mvFn(Arg0 &&arg0, Args_ &&... args) {
    return Lambda<ZmLambda_HeapID>::mvFn(
	ZuFwd<Arg0>(arg0), ZuFwd<Args_>(args)...);
  }
  // lambdas (specifying heap ID)
  template <auto HeapID, bool Sharded = false> struct Lambda {
    template <typename L>
    static ZmFn fn(L &&l) {
      return LambdaInvoker<
	decltype(&L::operator ()), HeapID, Sharded>::fn(ZuFwd<L>(l));
    }
    template <typename O, typename L>
    static ZmFn fn(O *o, L &&l) {
      return LBoundInvoker<
	decltype(&L::operator ()), HeapID, Sharded>::fn(o, ZuFwd<L>(l));
    }
    template <typename O, typename L>
    static ZmFn fn(ZmRef<O> o, L &&l) {
      return LBoundInvoker<
	decltype(&L::operator ()), HeapID, Sharded>::fn(ZuMv(o), ZuFwd<L>(l));
    }
    template <typename O, typename L>
    static ZmFn mvFn(ZmRef<O> o, L &&l) {
      return LBoundInvoker<
	decltype(&L::operator ()), HeapID, Sharded>::mvFn(ZuMv(o), ZuFwd<L>(l));
    }
  };

private:
  // unbound functions
  template <typename R, R (*Fn)(Args...)> struct FnInvoker<0, Fn> {
    static uintptr_t invoke(uintptr_t &, Args... args) {
      return ZmFn_Cast((*Fn)(ZuFwd<Args>(args)...));
    }
  };
  template <void (*Fn)(Args...)> struct FnInvoker<1, Fn> {
    static uintptr_t invoke(uintptr_t &, Args... args) {
      (*Fn)(ZuFwd<Args>(args)...);
      return 0;
    }
  };

  // bound functions
  template <typename O, typename C, typename R, R (*Fn)(C *, Args...)>
  struct BoundInvoker<O *, 0, Fn> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      return ZmFn_Cast(
	  (*Fn)(static_cast<C *>(ptr<O>(o)), ZuFwd<Args>(args)...));
    }
  };
  template <typename O, typename C, void (*Fn)(C *, Args...)>
  struct BoundInvoker<O *, 1, Fn> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      (*Fn)(static_cast<C *>(ptr<O>(o)), ZuFwd<Args>(args)...);
      return 0;
    }
  };
  template <typename O, typename R, R (*Fn)(ZmRef<O>, Args...)>
  struct BoundInvoker<ZmRef<O>, 0, Fn> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      return ZmFn_Cast(
	  (*Fn)(ptr<O>(o), ZuFwd<Args>(args)...));
    }
    static uintptr_t mvInvoke(uintptr_t &o, Args... args) {
      o = disown(o);
      return ZmFn_Cast(
	  (*Fn)(ZmRef<O>::acquire(ptr<O>(o)), ZuFwd<Args>(args)...));
    }
  };
  template <typename O, typename R, R (*Fn)(ZmRef<O>, Args...)>
  struct BoundInvoker<ZmRef<O>, 1, Fn> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      (*Fn)(ptr<O>(o), ZuFwd<Args>(args)...);
      return 0;
    }
    static uintptr_t mvInvoke(uintptr_t &o, Args... args) {
      o = disown(o);
      (*Fn)(ZmRef<O>::acquire(ptr<O>(o)), ZuFwd<Args>(args)...);
      return 0;
    }
  };

  // member functions
  template <typename O, typename C, typename R, R (C::*Fn)(Args...)>
  struct MemberInvoker<O *, 0, Fn> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      return ZmFn_Cast(
	  (static_cast<C *>(ptr<O>(o))->*Fn)(ZuFwd<Args>(args)...));
    }
  };
  template <typename O, typename C, void (C::*Fn)(Args...)>
  struct MemberInvoker<O *, 1, Fn> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      (static_cast<C *>(ptr<O>(o))->*Fn)(ZuFwd<Args>(args)...);
      return 0;
    }
  };
  template <typename O, typename C, typename R, R (C::*Fn)(Args...)>
  struct MemberInvoker<ZmRef<O>, 0, Fn> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      return ZmFn_Cast(
	  (static_cast<C *>(ptr<O>(o))->*Fn)(ZuFwd<Args>(args)...));
    }
  };
  template <typename O, typename C, void (C::*Fn)(Args...)>
  struct MemberInvoker<ZmRef<O>, 1, Fn> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      (static_cast<C *>(ptr<O>(o))->*Fn)(ZuFwd<Args>(args)...);
      return 0;
    }
  };
  template <typename O, typename C, typename R, R (C::*Fn)(Args...) const>
  struct MemberInvoker<O *, 0, Fn> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      return ZmFn_Cast(
	  (static_cast<const C *>(ptr<O>(o))->*Fn)(ZuFwd<Args>(args)...));
    }
  };
  template <typename O, typename C, void (C::*Fn)(Args...) const>
  struct MemberInvoker<O *, 1, Fn> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      (static_cast<const C *>(ptr<O>(o))->*Fn)(ZuFwd<Args>(args)...);
      return 0;
    }
  };
  template <typename O, typename C, typename R, R (C::*Fn)(Args...) const>
  struct MemberInvoker<ZmRef<O>, 0, Fn> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      return ZmFn_Cast(
	  (static_cast<const C *>(ptr<O>(o))->*Fn)(ZuFwd<Args>(args)...));
    }
  };
  template <typename O, typename C, void (C::*Fn)(Args...) const>
  struct MemberInvoker<ZmRef<O>, 1, Fn> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      (static_cast<const C *>(ptr<O>(o))->*Fn)(ZuFwd<Args>(args)...);
      return 0;
    }
  };

  // lambdas (unbound)
  template <typename L, typename R, typename ...Args_> struct LambdaStateless {
    typedef R (*Fn)(Args_...);
    enum { OK = ZuConversion<L, Fn>::Exists };
  };
  template <
    typename L, typename R, auto HeapID, bool Sharded, bool, typename ...Args_>
  struct LambdaInvoker_;
  template <typename, typename, auto, bool, bool, typename ...Args_>
friend struct LambdaInvoker_;

  // lambdas with captures (heap-allocated)
  template <
    typename L, typename R, auto HeapID, bool Sharded, typename ...Args_>
  struct LambdaInvoker_<L, R, HeapID, Sharded, 0, Args_...> {
    template <typename L_> static ZmFn fn(L_ &&l);
  };
  template <
    typename L, typename R, auto HeapID, bool Sharded, typename ...Args_>
  struct LambdaInvoker_<const L, R, HeapID, Sharded, 0, Args_...> {
    template <typename L_> static ZmFn fn(L_ &&l);
  };

  // fast stateless lambdas (i.e. empty class without captures)
  template <
    typename L, typename R, auto HeapID, bool Sharded, typename ...Args_>
  struct LambdaInvoker_<L, R, HeapID, Sharded, 1, Args_...> {
    static uintptr_t invoke(uintptr_t, Args... args) {
      return ZmFn_Cast(
	  (*static_cast<const L *>(nullptr))(ZuFwd<Args>(args)...));
    }
    template <typename L_> static ZmFn fn(L_ &&) {
      return ZmFn{
	ZmFn::Pass{}, &LambdaInvoker_::invoke,
	static_cast<void *>(nullptr)};
    }
  };
  template <typename L, auto HeapID, bool Sharded, typename ...Args_>
  struct LambdaInvoker_<L, void, HeapID, Sharded, 1, Args_...> {
    static uintptr_t invoke(uintptr_t, Args... args) {
      (*static_cast<const L *>(nullptr))(ZuFwd<Args>(args)...);
      return 0;
    }
    template <typename L_> static ZmFn fn(L_ &&) {
      return ZmFn{
	  ZmFn::Pass{}, &LambdaInvoker_::invoke,
	  static_cast<void *>(nullptr)};
    }
  };

  template <
    typename R, typename L, typename ...Args_, auto HeapID, bool Sharded>
  struct LambdaInvoker<R (L::*)(Args_...), HeapID, Sharded> :
    public LambdaInvoker_<L, R, HeapID, Sharded, 0, Args_...> { };
  template <
    typename R, typename L, typename ...Args_, auto HeapID, bool Sharded>
  struct LambdaInvoker<R (L::*)(Args_...) const, HeapID, Sharded> :
    public LambdaInvoker_<const L, R, HeapID, Sharded,
	     LambdaStateless<L, R, Args_...>::OK, Args_...> { };

  // bound lambdas
  template <
    typename, typename L, typename R, auto HeapID, bool Sharded, bool Stateless>
  struct LBoundInvoker_;
  template <typename, typename, typename, auto, bool, bool>
friend struct LBoundInvoker_;

  template <typename O, typename L, typename R, auto HeapID, bool Sharded>
  struct LBoundInvoker_<O *, L, R, HeapID, Sharded, 0> {
    template <typename L_>
    static ZuNotMutable<L_, ZmFn> fn(O *o, L_ l) {
      return Lambda<HeapID, Sharded>::fn([l = ZuMv(l), o](Args... args) {
	l(o, ZuFwd<Args>(args)...); });
    }
    template <typename L_>
    static ZuIsMutable<L_, ZmFn> fn(O *o, L_ l) {
      return Lambda<HeapID, Sharded>::fn(
	  [l = ZuMv(l), o](Args... args) mutable {
	    l(o, ZuFwd<Args>(args)...);
	  });
    }
    template <typename L_>
    static ZuNotMutable<L_, ZmFn> fn(ZmRef<O> o, L_ l) {
      return Lambda<HeapID, Sharded>::fn(
	  [l = ZuMv(l), o = ZuMv(o)](Args... args) {
	    l(o, ZuFwd<Args>(args)...);
	  });
    }
    template <typename L_>
    static ZuIsMutable<L_, ZmFn> fn(ZmRef<O> o, L_ l) {
      return Lambda<HeapID, Sharded>::fn(
	  [l = ZuMv(l), o = ZuMv(o)](Args... args) mutable {
	    l(o, ZuFwd<Args>(args)...);
	  });
    }
    template <typename L_>
    static ZuNotMutable<L_, ZmFn> mvFn(ZmRef<O> o, L_ l) {
      return Lambda<HeapID, Sharded>::fn(
	  [l = ZuMv(l), o = ZuMv(o)](Args... args) {
	    l(o, ZuFwd<Args>(args)...);
	  });
    }
    template <typename L_>
    static ZuIsMutable<L_, ZmFn> mvFn(ZmRef<O> o, L_ l) {
      return Lambda<HeapID, Sharded>::fn(
	  [l = ZuMv(l), o = ZuMv(o)](Args... args) mutable {
	    l(o, ZuFwd<Args>(args)...);
	  });
    }
  };

  template <typename O, typename L, typename R, auto HeapID, bool Sharded>
  struct LBoundInvoker_<O *, L, R, HeapID, Sharded, 1> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      return ZmFn_Cast(
	  (*static_cast<const L *>(nullptr))(ptr<O>(o), ZuFwd<Args>(args)...));
    }
    static ZmFn fn(O *o, L) {
      return ZmFn{ZmFn::Pass{}, &LBoundInvoker_::invoke, o};
    }
    static ZmFn fn(ZmRef<O> o, L) {
      return ZmFn{ZmFn::Pass{}, &LBoundInvoker_::invoke, ZuMv(o)};
    }
    static ZmFn mvFn(ZmRef<O> o, L) {
      return ZmFn{ZmFn::Pass{}, &LBoundInvoker_::invoke, ZuMv(o)};
    }
  };
  template <typename O, typename L, auto HeapID, bool Sharded>
  struct LBoundInvoker_<O *, L, void, HeapID, Sharded, 1> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      (*reinterpret_cast<const L *>(0))(ptr<O>(o), ZuFwd<Args>(args)...);
      return 0;
    }
    static ZmFn fn(O *o, L) {
      return ZmFn{ZmFn::Pass{}, &LBoundInvoker_::invoke, o};
    }
    static ZmFn fn(ZmRef<O> o, L) {
      return ZmFn{ZmFn::Pass{}, &LBoundInvoker_::invoke, ZuMv(o)};
    }
    static ZmFn mvFn(ZmRef<O> o, L) {
      return ZmFn{ZmFn::Pass{}, &LBoundInvoker_::invoke, ZuMv(o)};
    }
  };

  template <typename O, typename L, typename R, auto HeapID, bool Sharded>
  struct LBoundInvoker_<ZmRef<O>, L, R, HeapID, Sharded, 0> {
    template <typename L_>
    static ZuNotMutable<L_, ZmFn> fn(ZmRef<O> o, L_ l) {
      return Lambda<HeapID, Sharded>::fn(
	  [l = ZuMv(l), o = ZuMv(o)](Args... args) {
	    l(o, ZuFwd<Args>(args)...);
	  });
    }
    template <typename L_>
    static ZuIsMutable<L_, ZmFn> fn(ZmRef<O> o, L_ l) {
      return Lambda<HeapID, Sharded>::fn(
	  [l = ZuMv(l), o = ZuMv(o)](Args... args) mutable {
	    l(o, ZuFwd<Args>(args)...);
	  });
    }
    template <typename L_>
    static ZmFn mvFn(ZmRef<O> o, L_ l) {
      return Lambda<HeapID, Sharded>::fn(
	  [l = ZuMv(l), o = ZuMv(o)](Args... args) mutable {
	    l(ZuMv(o), ZuFwd<Args>(args)...);
	  });
    }
  };

  template <typename O, typename L, typename R, auto HeapID, bool Sharded>
  struct LBoundInvoker_<ZmRef<O>, L, R, HeapID, Sharded, 1> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      return ZmFn_Cast(
	  (*static_cast<const L *>(nullptr))(ptr<O>(o), ZuFwd<Args>(args)...));
    }
    static uintptr_t mvInvoke(uintptr_t &o, Args... args) {
      o = disown(o);
      return ZmFn_Cast(
	  (*static_cast<const L *>(nullptr))(
	    ZmRef<O>::acquire(ptr<O>(o)),
	    ZuFwd<Args>(args)...));
    }
    static ZmFn fn(O *o, L) {
      return ZmFn{ZmFn::Pass{}, &LBoundInvoker_::invoke, o};
    }
    static ZmFn fn(ZmRef<O> o, L) {
      return ZmFn{ZmFn::Pass{}, &LBoundInvoker_::invoke, ZuMv(o)};
    }
    static ZmFn mvFn(ZmRef<O> o, L) {
      return ZmFn{ZmFn::Pass{}, &LBoundInvoker_::mvInvoke, ZuMv(o)};
    }
  };
  template <typename O, typename L, auto HeapID, bool Sharded>
  struct LBoundInvoker_<ZmRef<O>, L, void, HeapID, Sharded, 1> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      (*reinterpret_cast<const L *>(0))(ptr<O>(o), ZuFwd<Args>(args)...);
      return 0;
    }
    static uintptr_t mvInvoke(uintptr_t &o, Args... args) {
      o = disown(o);
      (*reinterpret_cast<const L *>(0))(
	  ZmRef<O>::acquire(ptr<O>(o)),
	  ZuFwd<Args>(args)...);
      return 0;
    }
    static ZmFn fn(O *o, L) {
      return ZmFn{ZmFn::Pass{}, &LBoundInvoker_::invoke, o};
    }
    static ZmFn fn(ZmRef<O> o, L) {
      return ZmFn{ZmFn::Pass{}, &LBoundInvoker_::invoke, ZuMv(o)};
    }
    static ZmFn mvFn(ZmRef<O> o, L) {
      return ZmFn{ZmFn::Pass{}, &LBoundInvoker_::mvInvoke, ZuMv(o)};
    }
  };

  template <
    typename O, typename R, typename L, typename ...Args_,
    auto HeapID, bool Sharded>
  struct LBoundInvoker<R (L::*)(O *, Args_...), HeapID, Sharded> :
    public LBoundInvoker_<O *, L, R, HeapID, Sharded,
	   LambdaStateless<L, R, O *, Args_...>::OK> { };
  template <
    typename O, typename L, typename ...Args_, auto HeapID, bool Sharded>
  struct LBoundInvoker<void (L::*)(O *, Args_...), HeapID, Sharded> :
    public LBoundInvoker_<O *, L, void, HeapID, Sharded,
	   LambdaStateless<L, void, O *, Args_...>::OK> { };
  template <
    typename O, typename R, typename L, typename ...Args_,
    auto HeapID, bool Sharded>
  struct LBoundInvoker<R (L::*)(O *, Args_...) const, HeapID, Sharded> :
    public LBoundInvoker_<O *, const L, R, HeapID, Sharded,
	   LambdaStateless<L, R, O *, Args_...>::OK> { };
  template <
    typename O, typename L, typename ...Args_, auto HeapID, bool Sharded>
  struct LBoundInvoker<void (L::*)(O *, Args_...) const, HeapID, Sharded> :
    public LBoundInvoker_<O *, const L, void, HeapID, Sharded,
	   LambdaStateless<L, void, O *, Args_...>::OK> { };

  template <
    typename O, typename R, typename L, typename ...Args_,
    auto HeapID, bool Sharded>
  struct LBoundInvoker<R (L::*)(ZmRef<O>, Args_...), HeapID, Sharded> :
    public LBoundInvoker_<ZmRef<O>, L, R, HeapID, Sharded,
	   LambdaStateless<L, R, ZmRef<O>, Args_...>::OK> { };
  template <
    typename O, typename L, typename ...Args_, auto HeapID, bool Sharded>
  struct LBoundInvoker<void (L::*)(ZmRef<O>, Args_...), HeapID, Sharded> :
    public LBoundInvoker_<ZmRef<O>, L, void, HeapID, Sharded,
	   LambdaStateless<L, void, ZmRef<O>, Args_...>::OK> { };
  template <
    typename O, typename R, typename L, typename ...Args_,
    auto HeapID, bool Sharded>
  struct LBoundInvoker<R (L::*)(ZmRef<O>, Args_...) const, HeapID, Sharded> :
    public LBoundInvoker_<ZmRef<O>, const L, R, HeapID, Sharded,
	   LambdaStateless<L, R, ZmRef<O>, Args_...>::OK> { };
  template <
    typename O, typename L, typename ...Args_, auto HeapID, bool Sharded>
  struct LBoundInvoker<void (L::*)(ZmRef<O>, Args_...) const, HeapID, Sharded> :
    public LBoundInvoker_<ZmRef<O>, const L, void, HeapID, Sharded,
	   LambdaStateless<L, void, ZmRef<O>, Args_...>::OK> { };
};

#endif /* ZmFn__HPP */
