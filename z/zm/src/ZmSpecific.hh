//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// TLS with
// * explicit scope control
// * deterministic destruction sequencing
// * iteration over all instances
//   (the iterating thread gains access to other threads' instances)
// * guaranteed safe destruction on both Linux and Windows (mingw64)
// * instance consolidation on Windows with multiple modules (DLLs),
//   including DLLs delay-loaded via LoadLibrary()
// * no false-positive memory leaks at exit

// ZmSpecific overcomes the following challenges:
// * interdependence of thread-local instances where one type requires
//   another to be reliably created before it and/or destroyed after it
//   (destruction sequence is not explictly controllable using thread_local)
// * iterating over all thread-local instances from other threads
//   for statistics gathering, telemetry or other purposes
// * Windows DLL TLS complexity resulting in multiple conflicting
//   instances of the same type within the same thread when multiple modules
//   reference the declaration at compile-time

// performance
// - normal run-time ZmSpecific::instance() calls are lock-free
//   and equivalent or better than thread_local
// - initial/final object construction/destruction involves
//   global lock acquisition and updates to a type-specific linked list
//   (for iteration), and a module-specific linked list (Win32 cleanup only)

// ZmSpecific<T>::instance() returns T * pointer, unique per-thread per-T
//
// ZmSpecific<T>::all(ZmFn<T *> fn) calls fn for all instances of T
//
// ZmCleanupLevel(ZuDeclVal<T *>()) returns ZuUnsigned<N>
// where N determines order of destruction (per ZmCleanup enum)
//
// ZmSpecific<T, false>::instance() can return null since T will not be
// constructed on-demand - use ZmSpecific<T, false>::instance(new T(...))
//
// auto &v = *ZmSpecific<T>::instance();	// 
//
// ... or using ZmTLS, T does not need to be ZmObject derived:
//
// void foo() { thread_local T v; ... }	// can be replaced with:
// auto &v = ZmTLS<T>();		// TLS T with global scope
// auto &v = ZmTLS<T, foo>();		// TLS T scoped to foo()
// auto &v = ZmTLS<T, (int Foo::*){}>();// TLS T scoped to a class/struct Foo
// auto &v = ZmTLS([]{ return T{}; });	// TLS T scoped to a lambda
//
// thread_local T v{args...} can be replaced with:
// auto &v = ZmTLS([]{ return T{args...}; });

#ifndef ZmSpecific_HH
#define ZmSpecific_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#ifndef _WIN32
#include <alloca.h>
#endif

#include <stdlib.h>
#include <typeinfo>

#include <zlib/ZuCmp.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuTuple.hh>
#include <zlib/ZuLambdaTraits.hh>

#include <zlib/ZmRef.hh>
#include <zlib/ZmObject.hh>
#include <zlib/ZmFn_.hh>		// avoid circular dependency
#include <zlib/ZmCleanup.hh>
#include <zlib/ZmGlobal.hh>

class ZmSpecific_;
struct ZmSpecific_Object;

extern "C" {
  ZmExtern void ZmSpecific_lock();
  ZmExtern void ZmSpecific_unlock();
#ifdef _WIN32
  ZmExtern void ZmSpecific_cleanup();
  ZmExtern void ZmSpecific_cleanup_add(ZmSpecific_Object *);
  ZmExtern void ZmSpecific_cleanup_del(ZmSpecific_Object *);
#endif
}

struct ZmAPI ZmSpecific_Object {
  typedef void (*DtorFn)(ZmSpecific_Object *);

  ZmSpecific_Object() { }
  ~ZmSpecific_Object() {
    ZmSpecific_lock();
    dtor();
  }

  void dtor() {
    if (ZuLikely(dtorFn))
      (*dtorFn)(this);
    else
      ZmSpecific_unlock();
  }

  ZmObject		*ptr = nullptr;
  DtorFn		dtorFn = nullptr;
  ZmSpecific_Object	*prev = nullptr;
  ZmSpecific_Object	*next = nullptr;
#ifdef _WIN32
  Zm::ThreadID		tid = 0;
  ZmSpecific_Object	*modPrev = nullptr;
  ZmSpecific_Object	*modNext = nullptr;
#endif
};

#ifndef _WIN32
// pthreads uses built-in cleanup on thread exit
template <typename O = ZmSpecific_Object>
struct ZmSpecific_Allocator {
  pthread_key_t	key;

  ZmSpecific_Allocator() {
    pthread_key_create(&key, [](void *o) { delete static_cast<O *>(o); });
  }
  ~ZmSpecific_Allocator() { pthread_key_delete(key); }

  bool set(const O *o) const {
    return pthread_setspecific(key, static_cast<const void *>(o));
  }
  O *get() const {
    return static_cast<O *>(pthread_getspecific(key));
  }
};
#else
// Windows uses deferred linked-list cleanup in ZmSpecific.cpp
template <typename O = ZmSpecific_Object>
struct ZmSpecific_Allocator {
  DWORD	key;

  ZmSpecific_Allocator() { key = TlsAlloc(); }
  ~ZmSpecific_Allocator() { TlsFree(key); }

  bool set(O *o) const {
    return TlsSetValue(key, static_cast<void *>(o));
  }
  O *get() const {
    return static_cast<O *>(TlsGetValue(key));
  }
};
#endif

class ZmAPI ZmSpecific_ {
  using Object = ZmSpecific_Object;

public:
  ZmSpecific_() { }
  ~ZmSpecific_() {
    Object *o = nullptr;
    for (;;) {
      ZmSpecific_lock();
      o = m_head; // LIFO
      if (!o) { ZmSpecific_unlock(); return; }
      o->dtor(); // unlocks
    }
  }

  void add(Object *o) {
    o->prev = nullptr;
    if (!(o->next = m_head))
      m_tail = o;
    else
      m_head->prev = o;
    m_head = o;
#ifdef _WIN32
    ZmSpecific_cleanup_add(o);
#endif
    ++m_count;
  }
  void del(Object *o) {
    if (!o->prev)
      m_head = o->next;
    else
      o->prev->next = o->next;
    if (!o->next)
      m_tail = o->prev;
    else
      o->next->prev = o->prev;
#ifdef _WIN32
    ZmSpecific_cleanup_del(o);
#endif
    o->dtorFn = nullptr;
    --m_count;
  }

#ifdef _WIN32
  template <typename T>
  void set(Zm::ThreadID tid, T *ptr) {
  retry:
    for (Object *o = m_head; o; o = o->next)
      if (o->tid == tid)
	if (o->ptr != static_cast<ZmObject *>(ptr)) {
	  if (o->ptr) {
	    o->dtor(); // unlocks
	    ZmSpecific_lock();
	    goto retry;
	  }
	  o->ptr = ptr;
	  ZmREF(ptr);
	}
  }
  ZmObject *get(Zm::ThreadID tid) {
    for (Object *o = m_head; o; o = o->next)
      if (o->tid == tid && o->ptr)
	return o->ptr;
    return nullptr;
  }
#endif

  template <typename L>
  void all_(L l) {
    all_2(&l);
  }

  template <typename L>
  void all_2(L *l) {
    ZmSpecific_lock();

    if (ZuUnlikely(!m_count)) { ZmSpecific_unlock(); return; }

    auto objects = static_cast<Object **>(ZuAlloca(m_count * sizeof(Object *)));
    if (ZuUnlikely(!objects)) { ZmSpecific_unlock(); return; }
    memset(objects, 0, sizeof(Object *) * m_count);
    all_3(objects, l);
  }

  template <typename L>
  void all_3(Object **objects, L *l) {
    unsigned j = 0, n = m_count;

    for (Object *o = m_head; j < n && o; ++j, o = o->next)
      if (!o->ptr)
	objects[j] = nullptr;
      else
	objects[j] = o;

#ifdef _WIN32
    // on Windows, there may be multiple Object instances pointing to
    // the same underlying T
    qsort(objects, n, sizeof(Object *),
	[](const void *o1, const void *o2) -> int {
	  return ZuCmp<Zm::ThreadID>::cmp(
	      (*(Object **)o1)->tid, (*(Object **)o2)->tid);
	});
    {
      Zm::ThreadID lastTID = 0;
      for (j = 0; j < n; j++)
	if (Object *o = objects[j])
	  if (o->tid == lastTID)
	    objects[j] = nullptr;
	  else
	    lastTID = o->tid;
    }
#endif

    using Ptr = ZuType<0, typename ZuDeduce<decltype(&L::operator())>::Args>;

    for (j = 0; j < n; j++)
      if (Object *o = objects[j])
	if (Ptr ptr = static_cast<Ptr>(o->ptr))
	  ZmREF(ptr);

    ZmSpecific_unlock();

    for (j = 0; j < n; j++)
      if (Object *o = objects[j])
	if (Ptr ptr = static_cast<Ptr>(o->ptr)) {
	  (*l)(ptr);
	  ZmDEREF(ptr);
	}
  }

protected:
  using Allocator = ZmSpecific_Allocator<>;

  ZuInline const Allocator &allocator() { return m_allocator; }

  Allocator	m_allocator;

private:
  unsigned	m_count = 0;
  Object	*m_head = nullptr;
  Object	*m_tail = nullptr;
};

template <typename T, bool Construct = true> struct ZmSpecificCtor {
  static T *fn() { return new T(); }
};
template <typename T> struct ZmSpecificCtor<T, false> {
  static T *fn() { return nullptr; }
};
template <class T_, bool Construct_ = true,
  auto CtorFn = ZmSpecificCtor<T_, Construct_>::fn>
class ZmSpecific : public ZmGlobal, public ZmSpecific_ {
  ZmSpecific(const ZmSpecific &);
  ZmSpecific &operator =(const ZmSpecific &);	// prevent mis-use

public:
  using T = T_;

private:
  static void final(...) { }
  template <typename U>
  static auto final(U *u) -> decltype(u->final()) {
    return u->final();
  }

  using Object = ZmSpecific_Object;

  ZuInline static ZmSpecific *global() {
    return ZmGlobal::global<ZmSpecific>();
  }

public:
  ZmSpecific() { }
  ~ZmSpecific() { }

private:
  using ZmSpecific_::add;
  using ZmSpecific_::del;
#ifdef _WIN32
  using ZmSpecific_::set;
  using ZmSpecific_::get;
#endif

  ZuInline Object *local_() {
    Object *o = allocator().get();
    if (ZuLikely(o)) return o;
    o = new Object{};
    allocator().set(o);
    return o;
  }

  void dtor_(Object *o) {
    T *ptr;
    if (ptr = static_cast<T *>(o->ptr)) {
      this->del(o);
      o->ptr = nullptr;
    }
    ZmSpecific_unlock();
    if (ptr) {
      final(ptr); // calls ZmCleanup<T>::final() if available
      ZmDEREF(ptr);
    }
  }

  static void dtor__(Object *o) { global()->dtor_(o); }

  template <bool Construct = Construct_>
  ZuIfT<!Construct, T *> create_(Object *) {
    return nullptr;
  }
  template <bool Construct = Construct_>
  ZuIfT<Construct, T *> create_(Object *o) {
    T *ptr = nullptr;
    ZmSpecific_lock();
    if (o->ptr) {
      ptr = static_cast<T *>(o->ptr);
      ZmSpecific_unlock();
      return ptr;
    }
#ifdef _WIN32
    o->tid = Zm::getTID();
#endif
    ZmSpecific_unlock();
    ptr = CtorFn();
    ZmSpecific_lock();
  retry:
    if (!o->ptr) {
      o->ptr = ptr;
      o->dtorFn = dtor__;
      add(o);
      ZmREF(ptr);
    } else {
      dtor_(o); // unlocks
      ZmSpecific_lock();
      goto retry;
    }
#ifdef _WIN32
    set(o->tid, ptr);
#endif
    ZmSpecific_unlock();
    return ptr;
  }

  T *instance_() {
    Object *o = local_();
    auto ptr = o->ptr;
    if (ZuUnlikely(!ptr)) return create_(o);
    return static_cast<T *>(ptr);
  }
  T *instance_(T *ptr) {
    Object *o = local_();
    ZmSpecific_lock();
#ifdef _WIN32
    if (!o->ptr) o->tid = Zm::getTID();
#endif
  retry:
    if (!o->ptr) {
      o->ptr = ptr;
      o->dtorFn = dtor__;
      add(o);
      ZmREF(ptr);
    } else {
      dtor_(o); // unlocks
      ZmSpecific_lock();
      goto retry;
    }
#ifdef _WIN32
    set(o->tid, ptr);
#endif
    ZmSpecific_unlock();
    return ptr;
  }

public:
  ZuInline static T *instance() {
    return global()->instance_();
  }
  static T *instance(T *ptr) {
    return global()->instance_(ptr);
  }

  template <typename L>
  static void all(L l) { return global()->all_(ZuMv(l)); }
};

// pass a function as the second parameter to limit scope
template <typename T, auto> struct ZmTLS_ : public ZmObject {
  T v = {};
  ZmTLS_() = default;
  template <typename U> ZmTLS_(U &&v_) : v{ZuFwd<U>(v_)} { }
};

// lambdas are inherently scoped to their declaration/definition
template <typename L>
inline auto &ZmTLS(L l, ZuStatelessLambda<L> *_ = nullptr) {
  using T = ZuDecay<decltype(ZuDeclVal<ZuLambdaReturn<L>>())>;
  using Object = ZmTLS_<T, &L::operator ()>;
  auto m = []() { return new Object{ZuInvokeLambda<L>()}; };
  return ZmSpecific<Object, true, ZuInvokeFn(m)>::instance()->v;
}

template <typename T, auto Scope = nullptr>
inline auto &ZmTLS() {
  return ZmSpecific<ZmTLS_<T, Scope>>::instance()->v;
}

#endif /* ZmSpecific_HH */
