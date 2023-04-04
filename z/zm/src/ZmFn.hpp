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

// function delegate optimized for performance and avoidance of heap allocation

// most uses of function delegates involve capturing "this", and most
// capture packs can be reduced to a single instance pointer; ZmFn<> captures
// a function pointer and an instance pointer without heap allocation,
// falling back to heap allocation for larger capture packs

// return types must either be void or be statically convertible to uintptr_t

// ZmFn<> has a single built-in capture of either an instance pointer, or a
// ZmRef<T> where T is ZmPolymorph-derived (i.e. is both reference-counted
// and has a virtual destructor); when used with ZmRef/ZmPolymorph, the
// ZmFn reference-counts the referenced object during its lifetime, ensuring
// that it does not go out of scope before the ZmFn does

// usage:
//
// ZmThread t([]() { puts("Hello World"); });	// lambda
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
// Stateless lambdas do not have any captures, i.e. [](...) { ... }
// such lambdas should not have data members, and C++11 guarantees they
// are convertible to a primitive function pointer; lambdas with captures
// cannot be converted to a primitive function pointer; note that stateless
// lambdas are implicitly const (not mutable)
//
// If the lambda has no captures, the lambda will not be instantiated or
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

// Performance:
// Member/Lambda overhead is ~2ns per call (g++ -O6, Core i7 @3.6GHz)

#ifndef ZmFn_HPP
#define ZmFn_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZmFn_.hpp>
#include <zlib/ZmHeap.hpp>

// lambda wrapper object (heap-allocated)
template <typename L, typename Heap>
struct ZmLambda_ : public Heap, public ZmPolymorph, public L {
  ZmLambda_() = delete;
  ZmLambda_(const ZmLambda_ &) = delete;
  ZmLambda_ &operator =(const ZmLambda_ &) = delete;
  ZmLambda_(ZmLambda_ &&) = delete;
  ZmLambda_ &operator =(ZmLambda_ &&) = delete;
  template <typename L_> ZuInline ZmLambda_(L_ &&l) : L{ZuFwd<L_>(l)} { }
};
template <typename L, auto HeapID = ZmLambda_HeapID(), bool Sharded = false>
using ZmLambda =
  ZmLambda_<L, ZmHeap<HeapID, sizeof(ZmLambda_<L, ZuNull>), Sharded>>;

template <typename ...Args>
template <typename L, typename R, auto HeapID, bool Sharded, typename ...Args_>
template <typename L_>
ZmFn<Args...>
ZmFn<Args...>::LambdaInvoker_<L, R, HeapID, Sharded, false, Args_...>::fn(
    L_ &&l) {
  using O = ZmLambda<L, HeapID, Sharded>;
  return Member<&L::operator ()>::fn(ZmRef<O>{new O{ZuFwd<L_>(l)}});
}

template <typename ...Args>
template <typename L, typename R, auto HeapID, bool Sharded, typename ...Args_>
template <typename L_>
ZmFn<Args...>
ZmFn<Args...>::LambdaInvoker_<const L, R, HeapID, Sharded, false, Args_...>::fn(
    L_ &&l) {
  using O = ZmLambda<L, HeapID, Sharded>;
  return Member<&L::operator ()>::fn(ZmRef<const O>{new O{ZuFwd<L_>(l)}});
}

#endif /* ZmFn_HPP */
