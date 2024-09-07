//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// STL allocator using ZmHeap

#ifndef ZmAllocator_HH
#define ZmAllocator_HH

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <new>
#include <memory>

#include <zlib/ZmHeap.hh>
#include <zlib/ZmVHeap.hh>

inline const char *ZmAllocator_ID() { return "ZmAllocator"; }
template <typename T, auto ID = ZmAllocator_ID, bool Sharded = false>
struct ZmAllocator : private ZmVHeap<ID, Sharded, alignof(T)> {
  using size_type = std::size_t;
  using difference_type = ptrdiff_t;
  using pointer = T *;
  using const_pointer = const T *;
  using reference = T &;
  using const_reference = const T &;
  using value_type = T;

  using propagate_on_container_copy_assignment = std::true_type;
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;
  using is_always_equal = std::true_type;

  ZmAllocator() = default;
  ZmAllocator(ZmAllocator &) = default;
  ZmAllocator &operator =(ZmAllocator &) = default;
  ZmAllocator(ZmAllocator &&) = default;
  ZmAllocator &operator =(ZmAllocator &&) = default;
  ~ZmAllocator() = default;

  template <typename U>
  constexpr ZmAllocator(const ZmAllocator<U, ID> &) { }
  template <typename U>
  ZmAllocator &operator =(const ZmAllocator<U, ID> &) {
    return *this;
  }

  template <typename U, auto ID_ = ID>
  struct rebind { using other = ZmAllocator<U, ID_>; };

  T *allocate(std::size_t);
  void deallocate(T *, std::size_t);

private:
  using VHeap = ZmVHeap<ID, Sharded, alignof(T)>;
  using VHeap::valloc;
  using VHeap::vfree;
};
template <typename T, auto ID, bool Sharded>
inline T *ZmAllocator<T, ID, Sharded>::allocate(std::size_t n) {
  using Cache = ZmHeapCacheT<ID, ZmHeapAllocSize<sizeof(T)>::N, Sharded>;
  if (ZuLikely(n == 1)) return static_cast<T *>(Cache::alloc());
  if (auto ptr = static_cast<T *>(valloc(n * sizeof(T))))
    return ptr;
  throw std::bad_alloc{};
}
template <typename T, auto ID, bool Sharded>
inline void ZmAllocator<T, ID, Sharded>::deallocate(T *p, std::size_t n) {
  using Cache = ZmHeapCacheT<ID, ZmHeapAllocSize<sizeof(T)>::N, Sharded>;
  if (ZuLikely(n == 1))
    Cache::free(p);
  else
    vfree(p);
}
template <typename T, typename U, auto ID, bool Sharded>
inline constexpr bool operator ==(
    const ZmAllocator<T, ID, Sharded> &, const ZmAllocator<U, ID, Sharded> &) {
  return true;
}
template <typename T, typename U, auto ID, bool Sharded>
inline constexpr bool operator !=(
    const ZmAllocator<T, ID, Sharded> &, const ZmAllocator<U, ID, Sharded> &) {
  return false;
}

#endif /* ZmAllocator_HH */
