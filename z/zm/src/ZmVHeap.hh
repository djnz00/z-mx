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

template <auto ID, unsigned Align, bool Sharded> class ZmVHeap;
template <auto ID, unsigned Align, bool Sharded> class ZmVHeap_Init {
template <auto, unsigned, bool> friend class ZmVHeap;
  ZmVHeap_Init();
};

inline unsigned ZmGrow(unsigned o, unsigned n)
{
  if (ZuUnlikely(o >= n)) return o;
  unsigned i = n <= 1 ? 0 : (sizeof(n)<<3) - ZuIntrin::clz(n - 1);
  if (ZuUnlikely(i >= 17)) return ((n + 0xffffU) & ~0xffffU);
  return ZuSwitch::dispatch<17>(i, [](auto I) -> unsigned {
    return ZmHeapAllocSize<(1<<I)>{};
  });
}

template <auto ID, unsigned Align = 1, bool Sharded = false>
class ZmVHeap {
  template <auto, unsigned, bool> friend class ZmVHeap_Init;

  template <unsigned N>
  using Cache = ZmHeapCacheT<ID, ZmHeapAllocSize<(1<<N)>{}, Align, Sharded>;

public:
  static void *valloc(size_t size) {
    if (ZuUnlikely(!size)) return nullptr;
    ZmAssert(size < UINT_MAX);
    size += Align;
    unsigned i = (sizeof(size)<<3) - ZuIntrin::clz(size);
    // if this is a giant allocation, just fallback to malloc/free
    if (ZuUnlikely(i >= 17)) {
      auto ptr = static_cast<uint8_t *>(Zm::alignedAlloc<Align>(size));
      if (ZuUnlikely(!ptr)) return nullptr;
      *ptr = i;
      return ptr + Align;
    }
    return ZuSwitch::dispatch<17>(i, [](auto I) -> void * {
      auto ptr = static_cast<uint8_t *>(Cache<I>::alloc());
      if (ZuUnlikely(!ptr)) return nullptr;
      *ptr = I;
      return ptr + Align;
    });
  }
  static void vfree(const void *ptr_) {
    if (ZuUnlikely(!ptr_)) return;
    auto ptr = (reinterpret_cast<const uint8_t *>(ptr_) - Align);
    auto i = *ptr;
    if (ZuUnlikely(i >= 17)) {
      Zm::alignedFree(ptr);
      return;
    }
    ZuSwitch::dispatch<17>(i, [ptr](auto I) {
      Cache<I>::free(const_cast<uint8_t *>(ptr));
    });
  }

private:
  static ZmVHeap_Init<ID, Align, Sharded>	m_init;
};

template <auto ID, unsigned Align, bool Sharded>
ZmVHeap_Init<ID, Align, Sharded>::ZmVHeap_Init() {
  using VHeap = ZmVHeap<ID, Align, Sharded>;
  for (unsigned i = 0; i < 16; i++)
    ZuSwitch::dispatch<16>(i, [](auto I) {
      if (auto ptr = VHeap::template Cache<I>::alloc())
	VHeap::template Cache<I>::free(ptr);
    });
}

template <auto ID, unsigned Align, bool Sharded>
ZmVHeap_Init<ID, Align, Sharded>
ZmVHeap<ID, Align, Sharded>::m_init;

#endif /* ZmVHeap_HH */
