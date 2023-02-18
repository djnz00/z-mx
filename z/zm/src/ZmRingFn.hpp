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

// ZmRingFn encapsulates a generic pointer to any lambda for use with
// ring buffers containing variable-sized messages; it optimizes for the
// stateless case, while also handling stateful lambdas with captures
//
// ZmRingFn(L &l) stores a pointer to the lambda instance together with
// function pointers that invoke it, move it, allocate a copy of it and free
// it; initially the lambda instance remains on-stack (C++ guarantees that
// it remains in scope)
//
// in the fast path, no heap allocation or freeing is performed during
// subsequent pushing the message onto a ring, shifting it, and invoking it
//
// ZmRingFn move assignment ensures that the lambda becomes heap-allocated;
// this extends its scope and enables deferred execution (timeouts, etc.)
// by extending the ZmRingFn scope beyond the scope of the original
// lambda reference that it was constructed with
//
// pushSize() returns the message size needed to store the
// lambda in a ring buffer together with its invocation function
//
// push() moves the lambda into a ring buffer together with its
// invocation function
//
// invoke() invokes the lambda directly from the ring buffer pointer,
// destroys it and returns the size of the message

// ZmHeap is used for heap allocation

#ifndef ZmRingFn_HPP
#define ZmRingFn_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

// run-time encapsulation of generic functor/lambda
template <auto HeapID, bool Sharded = false>
class ZmRingFn {
  // 64bit pointer-packing - uses bit 63 to indicate on-heap
  constexpr static uintptr_t OnHeap = (static_cast<uintptr_t>(1)<<63);

  typedef unsigned (*InvokeFn)(void *ptr);
  typedef void (*MoveFn)(void *dst, void *src, bool onHeap);
  typedef uintptr_t (*AllocFn)(uintptr_t ptr);

public:
  ZmRingFn() = default;

  ZmRingFn(const ZmRingFn &) = delete;
  ZmRingFn &operator =(const ZmRingFn &) = delete;

  ZmRingFn(ZmRingFn &&fn) :
      m_invokeFn{fn.m_invokeFn}, m_moveFn{fn.m_moveFn}, m_allocFn{fn.m_allocFn},
      m_ptr{fn.m_ptr} {
    fn.clear();
    heapAlloc();
  }
  ZmRingFn &operator =(ZmRingFn &&fn) {
    this->~ZmRingFn();
    new (this) ZmRingFn{ZuMv(fn)};
    return *this;
  }

  template <typename L>
  ZmRingFn(L &l, ZuIsStateless<L> *_ = nullptr) :
      m_invokeFn{[](void *) -> unsigned {
	try { (*reinterpret_cast<const L *>(0))(); } catch (...) { }
	return 0;
      }},
      m_moveFn{nullptr},
      m_allocFn{[](uintptr_t) -> uintptr_t {
	return 0;
      }},
      m_ptr{0} { }

  template <typename L>
  ZmRingFn(L &l, ZuNotStateless<L> *_ = nullptr) :
      m_invokeFn{[](void *ptr_) -> unsigned {
	auto ptr = static_cast<L *>(ptr_);
	try { (*ptr)(); } catch (...) { }
	ptr->~L();
	return sizeof(L);
      }},
      m_moveFn{[](void *dst, void *src_, bool onHeap) {
	using Cache = ZmHeapCacheT<HeapID, sizeof(L), Sharded>;
	auto src = static_cast<L *>(src_);
	new (dst) L{ZuMv(*src)};
	src->~L();
	if (ZuUnlikely(onHeap)) Cache::free(src);
      }},
      m_allocFn{[](uintptr_t ptr_) -> uintptr_t {
	using Cache = ZmHeapCacheT<HeapID, sizeof(L), Sharded>;
	// 0 - return sizeof(L) - used in fast path
	if (ZuLikely(!ptr_)) return sizeof(L);
	// 1 - heap allocation - slow path
	if (ZuLikely(ptr_ == 1))
	  return reinterpret_cast<uintptr_t>(Cache::alloc());
	// * - heap free (unless on stack)
	auto ptr = reinterpret_cast<L *>(ptr_);
	ptr->~L();
	Cache::free(ptr);
	return 0;
      }},
      m_ptr{reinterpret_cast<uintptr_t>(&l)} { }

  template <typename L>
  ZmRingFn &operator =(L l) {
    this->~ZmRingFn();
    new (this) ZmRingFn{l};
    heapAlloc();
    return *this;
  }

  ~ZmRingFn() {
    if (ZuUnlikely(m_invokeFn && (m_ptr & OnHeap)))
      m_allocFn(m_ptr & ~OnHeap); // destroys and frees
  }

  bool operator !() const { return !m_invokeFn; }
  ZuOpBool

  // ring push()
 
  unsigned pushSize() const {
    return sizeof(InvokeFn) + m_allocFn(0); // m_allocFn(0) returns sizeof(L)
  }
  void push(void *dst_) {
    auto dst = reinterpret_cast<InvokeFn *>(dst_);
    *dst = m_invokeFn;
    if (ZuUnlikely(m_ptr)) m_moveFn(&dst[1], ptr(), onHeap());
    clear();
  }

  // ring shift() - invokes lambda and returns size

  ZuInline static unsigned invoke(void *ptr_) {
    auto ptr = reinterpret_cast<InvokeFn *>(ptr_);
    return (**ptr)(static_cast<void *>(&ptr[1])) + sizeof(InvokeFn);
  }

private:
  void *ptr() const { return reinterpret_cast<void *>(m_ptr & ~OnHeap); }

  bool onHeap() const { return m_ptr & OnHeap; }

  void clear() { m_invokeFn = nullptr; }

  void heapAlloc() {
    if (ZuLikely(onHeap())) return;
    if (auto stackPtr = ptr()) {
      auto heapPtr = reinterpret_cast<void *>(m_allocFn(1)); // heap allocates
      if (ZuUnlikely(!heapPtr)) throw std::bad_alloc{};
      m_moveFn(heapPtr, stackPtr, false);
      m_ptr = reinterpret_cast<uintptr_t>(heapPtr) | OnHeap;
    }
  }

private:
  InvokeFn	m_invokeFn = nullptr; // invoke lambda, destroy it, return size
  MoveFn	m_moveFn = nullptr;   // move lambda
  AllocFn	m_allocFn = nullptr;  // size+alloc+free (overloaded function)
  uintptr_t	m_ptr = 0;            // pointer to lambda
};

#endif /* ZmRingFn_HPP */
