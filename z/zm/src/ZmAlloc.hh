//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// safe alloca() smart pointer that stack allocates if requested size
// is less than 50% of the remaining stack space, falling back to RAII heap
//
// {
//   auto x = ZmAlloc(uint8_t, 1024);
//   uint8_t *ptr = &x[0];
//   uint8_t byte = *x;
//   ...
// } // x is automatically freed (if needed) as it goes out of scope

#ifndef ZmAlloc_HH
#define ZmAlloc_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <zlib/ZmThread.hh>

// assumes a downwards-growing stack (the stack address is the limit)
// - this is true for all modern architectures, including x86 and ARM
inline unsigned ZmStackAvail()
{
  uint8_t *sp;
#if defined(__GNUC__) && defined(__x86_64__)
  __asm__("movq %%rsp, %0" : "=q" (sp));
#else
  sp = reinterpret_cast<uint8_t *>(&sp);
#endif
  auto addr = static_cast<uint8_t *>(ZmSelf()->stackAddr());
  auto avail = sp - addr;
  if (ZuUnlikely(avail < 0)) return 0;
  if (avail >= static_cast<ptrdiff_t>(UINT_MAX)) return UINT_MAX;
  return avail;
}

template <typename T>
struct ZmAlloc_ {
  T *ptr = nullptr;

  ZmAlloc_() = default;
  ZmAlloc_(T *ptr_) : ptr{ptr_} { }
  ZmAlloc_(const ZmAlloc_ &) = delete;
  ZmAlloc_ &operator =(const ZmAlloc_ &) = delete;
  ZmAlloc_(ZmAlloc_ &&a) : ptr{a.ptr} { a.ptr = nullptr; }
  ZmAlloc_ &operator =(ZmAlloc_ &&a) {
    ptr = a.ptr;
    a.ptr = nullptr;
    return *this;
  }
  ~ZmAlloc_() {
    if (ZuUnlikely(!ptr)) return;
    uint8_t *ptr_ = reinterpret_cast<uint8_t *>(ptr);
    auto self = ZmSelf();
    auto addr = reinterpret_cast<uint8_t *>(self->stackAddr());
    auto size = self->stackSize();
    if (ZuLikely(ptr_ >= addr && ptr_ < (addr + size)))
      ++self->m_allocStack;
    else {
      ++self->m_allocHeap;
      ::free(ptr_);
    }
  }

  operator T *() const { return ptr; }
  T *operator ->() const { return ptr; }
};

#define ZmAlloc(T, n) \
  ZmAlloc_<T>{static_cast<T *>( \
      (((ZmStackAvail()>>1) < (n * sizeof(T))) ? \
	::malloc(n * sizeof(T)) : ZuAlloca(n * sizeof(T))))}

#endif /* ZmAlloc_HH */
