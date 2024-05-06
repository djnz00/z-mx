//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// function delegate optimized for performance and avoidance of heap allocation
// * most uses of function delegates involve capturing just "this"
// * for such common use cases of callbacks with single context pointers,
//   ZmFn can capture the pointer and the function address by value, avoiding
//   heap allocation
// * built-in by-value capture can either be a raw pointer or a ZmRef<T>
//   where T is ZmPolymorph-derived (i.e. is both intrusively reference-counted
//   and has a virtual destructor); when used with ZmRef/ZmPolymorph, the
//   ZmFn increments the reference-count of the referenced object during its
//   lifetime, ensuring that it does not go out of scope before the ZmFn does
// * falls back to heap allocation for larger capture packs
// * no attempt is made to match return types
// * return types must either be void or be statically convertible to uintptr_t

// usage:
//
// ZmThread t([]{ puts("Hello World"); });	// lambda
//
// R foo() { ... }				// plain function
// ZmThread t(ZmFn<>::Ptr<&foo>::fn());		// R must cast ok to uintptr_t
//
// class F { void *operator ()() { ... } };	// function object
// F f;
// ZmThread t(ZmFn<>::Member<&F::operator ()>::fn(&f));
// 						// Note: pointer to F
// class G {
//   void *bar() const { ... }			// member function
// };
// G g;
// ZmThread t(ZmFn<>::Member<&G::bar>::fn(&g)); // Note: pointer to G
//
// class G2 : public ZmPolymorph, public G { };	// call G::bar via G2
// G2 g2;
// ZmThread t(ZmFn<>::Member<&G::bar>::fn(ZmMkRef(&g2)));
// 						// capture ZmRef to g2
//
// class H { static int bah() { ... } };	// static member function
// ZmThread t(ZmFn<>::Ptr<&H::bah>::fn());
//
// class I { ... };				// bound regular function
// void baz(I *i) { ... }
// I *i;
// ZmThread t(ZmFn<>::Bound<&baz>::fn(i));	// Note: pointer to I
//
// using Fn = ZmFn<Params>;
// void foo(Fn fn);
// foo(Fn([this, ...](params) { ... }));
//
// Note: The lambda closure objects are managed by a ZmHeap, but the size
// of each object depends on the captures used, resulting in multiple
// heap caches for different sizes of lambda
//
// stateless lambdas do not have any captures, i.e. [](...) { ... }
// stateless lambdas should not have data members, and C++11 guarantees they
// are convertible to a primitive function pointer; lambdas with captures
// cannot be converted to a function pointer; stateless lambdas are
// implicitly const (not mutable)
//
// if the lambda has no captures, the lambda will not be instantiated or
// heap allocated; both fn1 and fn2 below behave identically, but fn2 is
// much more efficient since the ZmRef<O> o is captured by ZmFn<>
// and the lambda remains stateless, instead of o being captured by the
// lambda and causing heap allocation; fn3 is more efficient than fn2,
// moving the ZmRef into the ZmFn, avoiding unnecessary manipulation of
// the reference count
//
// struct O { ... void fn() { ... } };
// ZmRef<O> o = new O(...);
// ZmFn<> fn1{[o = ZuMv(o)]() { o->fn(); }};	// inefficient
// ZmFn<> fn2{ZuMv(o), [](O *o) { o->fn(); }};	// built-in ZmFn capture
// ZmFn<> fn3{ZmFn<>::mvFn(ZuMv(o), [](ZmRef<O> o) { o->fn(); })}; // move

// performance:
// member/lambda overhead is ~2ns per call (g++ -O3, Core i7 @3.6GHz)

#ifndef ZmFn_HH
#define ZmFn_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <zlib/ZmFn_.hh>
#include <zlib/ZmHeap.hh>

// stateful lambda wrapper (heap-allocated)
template <typename Heap, typename L, typename ArgList> struct ZmLambda_;
template <typename Heap, typename L, typename ...Args>
struct ZmLambda_<Heap, L, ZuTypeList<Args...>> :
    public Heap, public ZmPolymorph, public L {
  template <typename L_> ZmLambda_(L_ &&l) : L{ZuFwd<L_>(l)} { }
  decltype(auto) invoke(Args... args) {
    return L::operator ()(ZuFwd<Args>(args)...);
  }
  decltype(auto) cinvoke(Args... args) const {
    return L::operator ()(ZuFwd<Args>(args)...);
  }
  ZmLambda_() = delete;
  ZmLambda_(const ZmLambda_ &) = delete;
  ZmLambda_ &operator =(const ZmLambda_ &) = delete;
  ZmLambda_(ZmLambda_ &&) = delete;
  ZmLambda_ &operator =(ZmLambda_ &&) = delete;
};
template <auto HeapID, bool Sharded, typename L, typename ArgList>
using ZmLambda = ZmLambda_<
  ZmHeap<HeapID, sizeof(ZmLambda_<ZuNull, L, ArgList>), Sharded>,
  L, ArgList>;

// stateful immutable lambda
template <typename ...Args>
template <auto HeapID, bool Sharded, typename L, bool VoidRet>
template <typename L_>
ZmFn<Args...>
ZmFn<Args...>::LambdaInvoker<HeapID, Sharded, L, VoidRet, false, false>::fn(
    L_ &&l) {
  ZuAssert((!ZmFn<Args...>::IsMutable<L>{}));
  ZuAssert((!ZmFn<Args...>::IsMutable<ZuDecay<L_>>{}));
  using O = ZmLambda<HeapID, Sharded, ZuDecay<L>, ZuTypeList<Args...>>;
  return Member<&O::cinvoke>::fn(ZmRef<const O>{new O{ZuFwd<L_>(l)}});
}

// stateful mutable lambda
template <typename ...Args>
template <auto HeapID, bool Sharded, typename L, bool VoidRet>
template <typename L_>
ZmFn<Args...>
ZmFn<Args...>::LambdaInvoker<HeapID, Sharded, L, VoidRet, false, true>::fn(
    L_ &&l) {
  using O = ZmLambda<HeapID, Sharded, ZuDecay<L>, ZuTypeList<Args...>>;
  return Member<&O::invoke>::fn(ZmRef<O>{new O{ZuFwd<L>(l)}});
}

#endif /* ZmFn_HH */
