//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// function delegates for multithreading and deferred execution (callbacks)

#ifndef ZmFn__HH
#define ZmFn__HH

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <zlib/ZuTraits.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuHash.hh>
#include <zlib/ZuNull.hh>
#include <zlib/ZuLambdaTraits.hh>

#include <zlib/ZmRef.hh>
#include <zlib/ZmPolymorph.hh>

template <typename ...Args> class ZmFn;

template <typename T> constexpr uintptr_t ZmFn_Cast(T v) {
  return static_cast<uintptr_t>(v);
}
template <typename T> constexpr uintptr_t ZmFn_Cast(T *v) {
  return reinterpret_cast<uintptr_t>(v);
}

// ZmFn base class
class ZmAnyFn {
  struct Pass {
    Pass &operator =(const Pass &) = delete;
  };

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
  ZmAnyFn() : m_invoker{0}, m_object{0} { }

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
      ZuBase<ZmPolymorph, O, Pass> *_ = nullptr) :
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

  struct Traits : public ZuBaseTraits<ZmAnyFn> { enum { IsPOD = 1 }; };
  friend Traits ZuTraitsType(ZmAnyFn *);

protected:
  uintptr_t		m_invoker;
  mutable uintptr_t	m_object;
};

inline constexpr const char *ZmLambda_HeapID() { return "ZmLambda"; }

template <typename ...Args> class ZmFn : public ZmAnyFn {
  class Pass {
    friend ZmFn;
    Pass &operator =(const Pass &) = delete;
  };

  typedef uintptr_t (*Invoker)(uintptr_t &, Args...);
  template <bool VoidRet, auto> struct FnInvoker;
  template <typename, bool VoidRet, auto> struct BoundInvoker;
  template <typename, bool VoidRet, auto> struct MemberInvoker;

public:
  template <typename L, typename ArgList, typename = void>
  struct IsCallable_ : public ZuFalse { };
  template <typename L>
  struct IsCallable_<L, ZuTypeList<>, decltype(ZuDeclVal<L &>()(), void())> :
      public ZuBool<!ZuInspect<ZmAnyFn, L>::Is> { };
  template <typename L, typename ...Args_>
  struct IsCallable_<L, ZuTypeList<Args_...>,
    decltype(ZuDeclVal<L &>()(ZuDeclVal<Args_>()...), void())> :
      public ZuBool<!ZuInspect<ZmAnyFn, L>::Is> { };
  template <typename L>
  using IsCallable = IsCallable_<L, ZuTypeList<Args...>>;
  template <typename L, typename R = void>
  using MatchCallable = ZuIfT<IsCallable<L>{}, R>;
  template <typename O, typename L>
  struct IsBoundCallable : public IsCallable_<L, ZuTypeList<O, Args...>> { };
  template <typename L>
  struct IsBoundCallable<Pass, L> : public ZuFalse { };
  template <typename O, typename L, typename R = void>
  using MatchBoundCallable = ZuIfT<IsBoundCallable<O, L>{}, R>;

public:
  ZmFn() : ZmAnyFn{} { }
  ZmFn(const ZmFn &fn) : ZmAnyFn{fn} { }
  ZmFn(ZmFn &&fn) : ZmAnyFn{static_cast<ZmAnyFn &&>(fn)} { }

private:
  template <typename ...Args_>
  ZmFn(Pass, Args_ &&...args) : ZmAnyFn(ZuFwd<Args_>(args)...) { }

public:
  // syntactic sugar for lambdas
  template <typename L>
  ZmFn(L &&l, MatchCallable<L> *_ = nullptr) :
      ZmAnyFn{fn(ZuFwd<L>(l))} { }
  template <typename O, typename L>
  ZmFn(O &&o, L &&l, MatchBoundCallable<ZuDeref<O>, L> *_ = nullptr) :
      ZmAnyFn{fn(ZuFwd<O>(o), ZuFwd<L>(l))} { }

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
	  &FnInvoker<ZuInspect<void, R>::Same, Fn>::invoke,
	  static_cast<void *>(nullptr)};
    }
  };

  // bound function pointer
  template <auto Fn> struct Bound;
  template <typename C, typename R, R (*Fn)(C *, Args...)>
  struct Bound<Fn> {
    template <typename O> static ZmFn fn(O *o) {
      return ZmFn{ZmFn::Pass{},
	  &BoundInvoker<O *, ZuInspect<void, R>::Same, Fn>::invoke, o};
    }
    template <typename O> static ZmFn fn(ZmRef<O> o) {
      return ZmFn{ZmFn::Pass{},
	  &BoundInvoker<ZmRef<O>, ZuInspect<void, R>::Same, Fn>::invoke,
	  ZuMv(o)};
    }
    template <typename O> static ZmFn mvFn(ZmRef<O> o) {
      return ZmFn{ZmFn::Pass{},
	  &BoundInvoker<ZmRef<O>, ZuInspect<void, R>::Same, Fn>::invoke,
	  ZuMv(o)};
    }
  };
  template <typename O, typename R, R (*Fn)(ZmRef<O>, Args...)>
  struct Bound<Fn> {
    static ZmFn fn(O *o) {
      return ZmFn{ZmFn::Pass{},
	  &BoundInvoker<O *, ZuInspect<void, R>::Same, Fn>::invoke, o};
    }
    static ZmFn fn(ZmRef<O> o) {
      return ZmFn{ZmFn::Pass{},
	  &BoundInvoker<ZmRef<O>, ZuInspect<void, R>::Same, Fn>::invoke,
	  ZuMv(o)};
    }
    static ZmFn mvFn(ZmRef<O> o) {
      return ZmFn{ZmFn::Pass{},
	  &BoundInvoker<ZmRef<O>, ZuInspect<void, R>::Same, Fn>::mvInvoke,
	  ZuMv(o)};
    }
  };

  // member function
  template <auto Fn> struct Member;
  template <typename C, typename R, R (C::*Fn)(Args...)>
  struct Member<Fn> {
    template <typename O> static ZmFn fn(O *o) {
      return ZmFn{ZmFn::Pass{},
	  &MemberInvoker<O *, ZuInspect<void, R>::Same, Fn>::invoke, o};
    }
    template <typename O> static ZmFn fn(ZmRef<O> o) {
      return ZmFn{ZmFn::Pass{},
	  &MemberInvoker<O *, ZuInspect<void, R>::Same, Fn>::invoke,
	  ZuMv(o)};
    }
  };
  template <typename C, typename R, R (C::*Fn)(Args...) const>
  struct Member<Fn> {
    template <typename O> static ZmFn fn(O *o) {
      return ZmFn{ZmFn::Pass{},
	  &MemberInvoker<const O *, ZuInspect<void, R>::Same, Fn>::invoke,
	  o};
    }
    template <typename O> static ZmFn fn(ZmRef<O> o) {
      return ZmFn{ZmFn::Pass{},
	  &MemberInvoker<const O *, ZuInspect<void, R>::Same, Fn>::invoke,
	  ZuMv(o)};
    }
  };

  // lambda matching
  template <auto HeapID, bool Sharded> struct Lambda;
  template <typename L>
  static MatchCallable<L, ZmFn> fn(L &&l) {
    return Lambda<ZmLambda_HeapID>::fn(ZuFwd<L>(l));
  }
  template <typename O, typename L>
  static MatchBoundCallable<ZuDeref<O>, L, ZmFn> fn(O &&o, L &&l) {
    return Lambda<ZmLambda_HeapID>::fn(ZuFwd<O>(o), ZuFwd<L>(l));
  }
  template <typename L>
  static MatchCallable<L, ZmFn> mvFn(L &&l) {
    return Lambda<ZmLambda_HeapID>::mvFn(ZuFwd<L>(l));
  }
  template <typename O, typename L>
  static MatchBoundCallable<ZuDeref<O>, L, ZmFn> mvFn(O &&o, L &&l) {
    return Lambda<ZmLambda_HeapID>::mvFn(ZuFwd<O>(o), ZuFwd<L>(l));
  }

private:
  // deduce return type of lambda
  template <typename L, typename ArgList>
  using IsVoidRet_ = ZuLambdaTraits::IsVoidRet<L, ArgList>;
  template <typename L> using IsVoidRet = IsVoidRet_<L, ZuTypeList<Args...>>;
  template <typename O, typename L>
  using IsBoundVoidRet = IsVoidRet_<L, ZuTypeList<O, Args...>>;

  // deduce mutability of lambda
  template <typename L, typename ArgList>
  using IsMutable_ = ZuLambdaTraits::IsMutable<L, ArgList>;
  template <typename L>
  using IsMutable = IsMutable_<L, ZuTypeList<Args...>>;
  template <typename O, typename L>
  using IsBoundMutable = IsMutable_<L, ZuTypeList<O, Args...>>;

  // deduce statefulness of lambda
  template <typename L>
  using IsStateless = ZuIsStatelessLambda<L, ZuTypeList<Args...>>;
  template <typename O, typename L>
  using IsBoundStateless = ZuIsStatelessLambda<L, ZuTypeList<O, Args...>>;

  // pre-declare lambda invokers
  template <auto HeapID, bool Sharded, typename L,
    bool VoidRet = IsVoidRet<L>{},
    bool Stateless = IsStateless<L>{},
    bool Mutable = IsMutable<L>{}>
  struct LambdaInvoker;
  template <auto HeapID, bool Sharded, typename O, typename L,
    bool VoidRet = IsBoundVoidRet<O *, L>{},
    bool Stateless = IsBoundStateless<O *, L>{},
    bool Mutable = IsBoundMutable<O *, L>{}>
  struct LambdaPtrInvoker;
  template <auto HeapID, bool Sharded, typename O, typename L,
    bool VoidRet = IsBoundVoidRet<ZmRef<O>, L>{},
    bool Stateless = IsBoundStateless<ZmRef<O>, L>{},
    bool Mutable = IsBoundMutable<ZmRef<O>, L>{}>
  struct LambdaRefInvoker;
  template <auto HeapID, bool Sharded, typename O, typename L,
    bool VoidRet = IsBoundVoidRet<ZmRef<O>, L>{},
    bool Stateless = IsBoundStateless<ZmRef<O>, L>{},
    bool Mutable = IsBoundMutable<ZmRef<O>, L>{}>
  struct LambdaMvRefInvoker;

public:
  // lambdas (specifying heap ID)
  template <auto HeapID, bool Sharded = false> struct Lambda {
    template <typename L>
    static MatchCallable<L, ZmFn> fn(L &&l) {
      return LambdaInvoker<HeapID, Sharded, L>::fn(ZuFwd<L>(l));
    }
    template <typename O, typename L>
    static MatchBoundCallable<O *, L, ZmFn> fn(O *o, L &&l) {
      return LambdaPtrInvoker<HeapID, Sharded, O, L>::fn(
	  o, ZuFwd<L>(l));
    }
    template <typename O, typename L>
    static MatchBoundCallable<ZmRef<O>, L, ZmFn> fn(ZmRef<O> o, L &&l) {
      return LambdaRefInvoker<HeapID, Sharded, O, L>::fn(
	  ZuMv(o), ZuFwd<L>(l));
    }
    template <typename O, typename L>
    static MatchBoundCallable<ZmRef<O>, L, ZmFn> mvFn(ZmRef<O> o, L &&l) {
      return LambdaMvRefInvoker<HeapID, Sharded, O, L>::fn(
	  ZuMv(o), ZuFwd<L>(l));
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

  // stateless lambda
  template <auto HeapID, bool Sharded, typename L>
  struct LambdaInvoker<HeapID, Sharded, L, false, true, false> {
    static uintptr_t invoke(uintptr_t, Args... args) {
      return ZmFn_Cast(
	  ZuInvokeLambda<L, ZuTypeList<Args...>>(ZuFwd<Args>(args)...));
    }
    static ZmFn fn(L &&) {
      return {
	ZmFn::Pass{}, &LambdaInvoker::invoke,
	static_cast<void *>(nullptr)};
    }
  };
  // stateless lambda returning void
  template <auto HeapID, bool Sharded, typename L>
  struct LambdaInvoker<HeapID, Sharded, L, true, true, false> {
    static uintptr_t invoke(uintptr_t, Args... args) {
      ZuInvokeLambda<L, ZuTypeList<Args...>>(ZuFwd<Args>(args)...);
      return 0;
    }
    static ZmFn fn(L &&) {
      return {
	ZmFn::Pass{}, &LambdaInvoker::invoke,
	static_cast<void *>(nullptr)};
    }
  };
  // stateful immutable lambda
  template <auto HeapID, bool Sharded, typename L, bool VoidRet>
  struct LambdaInvoker<HeapID, Sharded, L, VoidRet, false, false> {
    template <typename L_> static ZmFn fn(L_ &&l);
  };
  // stateful mutable lambda
  template <auto HeapID, bool Sharded, typename L, bool VoidRet>
  struct LambdaInvoker<HeapID, Sharded, L, VoidRet, false, true> {
    template <typename L_> static ZmFn fn(L_ &&l);
  };
  // stateful immutable lambda bound to pointer
  template <auto HeapID, bool Sharded, typename O, typename L, bool VoidRet>
  struct LambdaPtrInvoker<HeapID, Sharded, O, L, VoidRet, false, false> {
    static ZmFn fn(O *o, L l) {
      return Lambda<HeapID, Sharded>::fn(
	  [o, l = ZuMv(l)](Args... args) {
	    l(o, ZuFwd<Args>(args)...);
	  });
    }
  };
  // stateful mutable lambda bound to pointer
  template <auto HeapID, bool Sharded, typename O, typename L, bool VoidRet>
  struct LambdaPtrInvoker<HeapID, Sharded, O, L, VoidRet, false, true> {
    static ZmFn fn(O *o, L l) {
      return Lambda<HeapID, Sharded>::fn(
	  [o, l = ZuMv(l)](Args... args) mutable {
	    l(o, ZuFwd<Args>(args)...);
	  });
    }
  };
  // stateful immutable lambda bound to ZmRef
  template <auto HeapID, bool Sharded, typename O, typename L, bool VoidRet>
  struct LambdaRefInvoker<HeapID, Sharded, O, L, VoidRet, false, false> {
    static ZmFn fn(ZmRef<O> o, L l) {
      return Lambda<HeapID, Sharded>::fn(
	  [o = ZuMv(o), l = ZuMv(l)](Args... args) {
	    l(o, ZuFwd<Args>(args)...);
	  });
    }
  };
  // stateful mutable lambda bound to ZmRef
  template <auto HeapID, bool Sharded, typename O, typename L, bool VoidRet>
  struct LambdaRefInvoker<HeapID, Sharded, O, L, VoidRet, false, true> {
    static ZmFn fn(ZmRef<O> o, L l) {
      return Lambda<HeapID, Sharded>::fn(
	  [o = ZuMv(o), l = ZuMv(l)](Args... args) mutable {
	    l(o, ZuFwd<Args>(args)...);
	  });
    }
  };
  // stateful "one-shot" immutable lambda bound to moved ZmRef
  template <auto HeapID, bool Sharded, typename O, typename L, bool VoidRet>
  struct LambdaMvRefInvoker<HeapID, Sharded, O, L, VoidRet, false, false> {
    static ZmFn fn(ZmRef<O> o, L l) {
      return Lambda<HeapID, Sharded>::fn(
	  [o = ZuMv(o), l = ZuMv(l)](Args... args) mutable {
	    l(ZuMv(o), ZuFwd<Args>(args)...);
	  });
    }
  };
  // stateful "one-shot" mutable lambda bound to moved ZmRef
  template <auto HeapID, bool Sharded, typename O, typename L, bool VoidRet>
  struct LambdaMvRefInvoker<HeapID, Sharded, O, L, VoidRet, false, true> {
    static ZmFn fn(ZmRef<O> o, L l) {
      return Lambda<HeapID, Sharded>::fn(
	  [o = ZuMv(o), l = ZuMv(l)](Args... args) mutable {
	    l(ZuMv(o), ZuFwd<Args>(args)...);
	  });
    }
  };
  // stateless lambda bound to pointer
  template <auto HeapID, bool Sharded, typename O, typename L>
  struct LambdaPtrInvoker<HeapID, Sharded, O, L, false, true, false> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      return ZmFn_Cast(reinterpret_cast<const L *>(0)->operator ()(
	    ptr<O>(o), ZuFwd<Args>(args)...));
    }
    static ZmFn fn(O *o, L) {
      return ZmFn{ZmFn::Pass{}, &LambdaPtrInvoker::invoke, o};
    }
  };
  // stateless lambda bound to pointer returning void
  template <auto HeapID, bool Sharded, typename O, typename L>
  struct LambdaPtrInvoker<HeapID, Sharded, O, L, true, true, false> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      reinterpret_cast<const L *>(0)->operator ()(
	  ptr<O>(o), ZuFwd<Args>(args)...);
      return 0;
    }
    static ZmFn fn(O *o, L) {
      return ZmFn{ZmFn::Pass{}, &LambdaPtrInvoker::invoke, o};
    }
  };
  // stateless lambda bound to ref
  template <auto HeapID, bool Sharded, typename O, typename L>
  struct LambdaRefInvoker<HeapID, Sharded, O, L, false, true, false> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      return ZmFn_Cast(reinterpret_cast<const L *>(0)->operator ()(
	    ptr<O>(o), ZuFwd<Args>(args)...));
    }
    static ZmFn fn(ZmRef<O> o, L) {
      return ZmFn{ZmFn::Pass{}, &LambdaRefInvoker::invoke, ZuMv(o)};
    }
  };
  // stateless lambda bound to ref returning void
  template <auto HeapID, bool Sharded, typename O, typename L>
  struct LambdaRefInvoker<HeapID, Sharded, O, L, true, true, false> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      reinterpret_cast<const L *>(0)->operator ()(
	  ptr<O>(o), ZuFwd<Args>(args)...);
      return 0;
    }
    static ZmFn fn(ZmRef<O> o, L) {
      return ZmFn{ZmFn::Pass{}, &LambdaRefInvoker::invoke, ZuMv(o)};
    }
  };
  // stateless "one-shot" lambda bound to moved ZmRef
  template <auto HeapID, bool Sharded, typename O, typename L>
  struct LambdaMvRefInvoker<HeapID, Sharded, O, L, false, true, false> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      o = disown(o);
      return ZmFn_Cast(reinterpret_cast<const L *>(0)->operator ()(
	    ZmRef<O>::acquire(ptr<O>(o)), ZuFwd<Args>(args)...));
    }
    static ZmFn fn(ZmRef<O> o, L) {
      return ZmFn{ZmFn::Pass{}, &LambdaMvRefInvoker::invoke, ZuMv(o)};
    }
  };
  // stateless "one-shot" lambda bound to moved ZmRef returning void
  template <auto HeapID, bool Sharded, typename O, typename L>
  struct LambdaMvRefInvoker<HeapID, Sharded, O, L, true, true, false> {
    static uintptr_t invoke(uintptr_t &o, Args... args) {
      o = disown(o);
      reinterpret_cast<const L *>(0)->operator ()(
	  ZmRef<O>::acquire(ptr<O>(o)), ZuFwd<Args>(args)...);
      return 0;
    }
    static ZmFn fn(ZmRef<O> o, L) {
      return ZmFn{ZmFn::Pass{}, &LambdaMvRefInvoker::invoke, ZuMv(o)};
    }
  };

  friend ZuTraits<ZmAnyFn> ZuTraitsType(ZmFn *);
};

#endif /* ZmFn__HH */
