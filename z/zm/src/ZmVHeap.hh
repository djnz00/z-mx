//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// variable-sized block allocator that dispatches to fixed-size
// ZmHeaps up to 16k, falling back to malloc thereafter

#ifndef ZmVHeap_HH
#define ZmVHeap_HH

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <zlib/ZuSwitch.hh>

#include <zlib/ZmAssert.hh>
#include <zlib/ZmHeap.hh>

template <auto ID, bool Sharded> class ZmVHeap;
template <auto ID, bool Sharded> class ZmVHeap_Init {
template <auto, bool> friend class ZmVHeap;
  ZmVHeap_Init();
};

inline unsigned ZmGrow(unsigned o, unsigned n)
{
  if (ZuUnlikely(o >= n)) return o;
  unsigned i = n <= 1 ? 0 : (sizeof(n)<<3) - ZuIntrin::clz(n - 1);
  if (ZuUnlikely(i >= 17)) return ((n + 0xffffU) & ~0xffffU);
  return ZuSwitch::dispatch<17>(i, [](auto I) {
    return static_cast<unsigned>(ZmHeapAllocSize<(1<<I)>::N);
  });
}

template <auto ID, bool Sharded = false>
class ZmVHeap {
  template <auto, bool> friend class ZmVHeap_Init;

  template <unsigned N>
  using Cache = ZmHeapCacheT<ID, ZmHeapAllocSize<(1<<N)>::N, Sharded>;

public:
  static void *valloc(size_t n) {
    ZmAssert(n < UINT_MAX);
    n += sizeof(uintptr_t);
    unsigned i = (sizeof(n)<<3) - ZuIntrin::clz(n);
    // if this is a giant allocation, just fallback to malloc/free
    if (ZuUnlikely(i >= 17)) {
      uintptr_t *ptr = static_cast<uintptr_t *>(::malloc(n));
      if (ZuUnlikely(!ptr)) return nullptr;
      *ptr = i;
      return &ptr[1];
    }
    return ZuSwitch::dispatch<17>(i, [](auto I) {
      uintptr_t *ptr = static_cast<uintptr_t *>(Cache<I>::alloc());
      if (ZuUnlikely(!ptr)) return static_cast<void *>(nullptr);
      *ptr = I;
      return static_cast<void *>(&ptr[1]);
    });
  }
  static void vfree(const void *p) {
    if (ZuUnlikely(!p)) return;
    uintptr_t *ptr = static_cast<uintptr_t *>(const_cast<void *>(p));
    auto i = *--ptr;
    if (ZuUnlikely(i >= 17)) {
      ::free(ptr);
      return;
    }
    ZuSwitch::dispatch<17>(i, [ptr](auto I) {
      Cache<I>::free(ptr);
    });
  }

private:
  static ZmVHeap_Init<ID, Sharded>	m_init;
};

template <auto ID, bool Sharded>
ZmVHeap_Init<ID, Sharded>::ZmVHeap_Init() {
  for (unsigned i = 0; i < 16; i++)
    ZuSwitch::dispatch<16>(i, [](auto I) {
      if (auto ptr = ZmVHeap<ID, Sharded>::template Cache<I>::alloc())
	ZmVHeap<ID, Sharded>::template Cache<I>::free(ptr);
    });
}

template <auto ID, bool Sharded>
ZmVHeap_Init<ID, Sharded> ZmVHeap<ID, Sharded>::m_init;

#endif /* ZmVHeap_HH */
