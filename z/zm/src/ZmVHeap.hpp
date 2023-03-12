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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// variable-sized block allocator that dispatches to fixed-size
// ZmHeaps up to 16k, falling back to malloc thereafter

#ifndef ZmVHeap_HPP
#define ZmVHeap_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZuSwitch.hpp>

#include <zlib/ZmHeap.hpp>

template <auto ID, bool Sharded> class ZmVHeap;
template <auto ID, bool Sharded> class ZmVHeap_Init {
template <auto, bool> friend class ZmVHeap;
  ZmVHeap_Init();
};

inline unsigned ZmGrow(unsigned o, unsigned n)
{
  if (ZuUnlikely(o >= n)) return o;
  n += sizeof(uintptr_t);
  unsigned i = 64 - __builtin_clzll(n);
  if (ZuUnlikely(i >= 17)) return n - sizeof(uintptr_t);
  return ZuSwitch::dispatch<17>(i, [](auto i) {
    return static_cast<unsigned>(ZmHeap_Size<(1<<i)>::Size);
  }) - sizeof(uintptr_t);
}

template <auto ID, bool Sharded = false>
class ZmVHeap {
  template <auto, bool> friend class ZmVHeap_Init;

  template <unsigned N>
  using Cache = ZmHeapCacheT<ID, ZmHeap_Size<(1<<N)>::Size, Sharded>;

public:
  static void *valloc(size_t n) {
    n += sizeof(uintptr_t);
    unsigned i = 64 - __builtin_clzll(n);
    if (ZuUnlikely(i >= 17)) {
      uintptr_t *ptr = static_cast<uintptr_t *>(::malloc(n));
      if (ZuUnlikely(!ptr)) return nullptr;
      *ptr = i;
      return &ptr[1];
    }
    return ZuSwitch::dispatch<17>(i, [](auto i) {
      uintptr_t *ptr = static_cast<uintptr_t *>(Cache<i>::alloc());
      if (ZuUnlikely(!ptr)) return static_cast<void *>(nullptr);
      *ptr = i;
      return static_cast<void *>(&ptr[1]);
    });
  }
  static void vfree(const void *p) {
    if (ZuUnlikely(!p)) return;
    uintptr_t *ptr = static_cast<uintptr_t *>(const_cast<void *>(p));
    auto i = *--ptr;
    if (ZuUnlikely(i >= 16)) {
      ::free(ptr);
      return;
    }
    ZuSwitch::dispatch<16>(i, [ptr](auto i) {
      Cache<i>::free(ptr);
    });
  }

private:
  static ZmVHeap_Init<ID, Sharded>	m_init;
};

template <auto ID, bool Sharded>
ZmVHeap_Init<ID, Sharded>::ZmVHeap_Init() {
  for (unsigned i = 0; i < 16; i++)
    ZuSwitch::dispatch<16>(i, [](auto i) {
      if (auto ptr = ZmVHeap<ID, Sharded>::template Cache<i>::alloc())
	ZmVHeap<ID, Sharded>::template Cache<i>::free(ptr);
    });
}

template <auto ID, bool Sharded>
ZmVHeap_Init<ID, Sharded> ZmVHeap<ID, Sharded>::m_init;

#endif /* ZmVHeap_HPP */
