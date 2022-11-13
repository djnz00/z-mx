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

// safe alloca() smart pointer that stack allocates if requested size
// is less than 50% of remaining stack space, falling back to RAII
// heap allocate/free
//
// usage:
//
// auto x = ZmAlloc(uint8_t, 1024);
// uint8_t *ptr = x.ptr;
// uint8_t byte = *x;

#ifndef ZmAlloc_HPP
#define ZmAlloc_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZmThread.hpp>

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
  T *ptr;
  operator T *() const { return ptr; }
  T *operator ->() const { return ptr; }
  ~ZmAlloc_() {
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
};

#define ZmAlloc(T, n) \
  ZmAlloc_<T>{static_cast<T *>( \
      (((ZmStackAvail()>>1) < (n * sizeof(T))) ? \
	::malloc(n * sizeof(T)) : ZuAlloca(n * sizeof(T))))}

#endif /* ZmAlloc_HPP */
