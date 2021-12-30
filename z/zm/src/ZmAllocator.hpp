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

// STL allocator using ZmHeap

#ifndef ZmAllocator_HPP
#define ZmAllocator_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <new>
#include <memory>

#include <zlib/ZmHeap.hpp>
#include <zlib/ZmVHeap.hpp>

struct ZmAllocator_ID {
  static constexpr const char *id() { return "ZmAllocator"; }
};
template <typename T, typename ID = ZmAllocator_ID>
struct ZmAllocator : private ZmVHeap<ID> {
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
  constexpr ZmAllocator(const ZmAllocator<U, ID> &) noexcept { }
  template <typename U>
  ZmAllocator &operator =(const ZmAllocator<U, ID> &) noexcept {
    return *this;
  }

  template <typename U, typename ID_ = ID>
  struct rebind { using other = ZmAllocator<U, ID_>; };

  T *allocate(std::size_t);
  void deallocate(T *, std::size_t) noexcept;

private:
  using ZmVHeap<ID>::valloc;
  using ZmVHeap<ID>::vfree;
};
template <typename T, typename ID>
inline T *ZmAllocator<T, ID>::allocate(std::size_t n) {
  using Cache = ZmHeapCacheT<ID, ZmHeap_Size<sizeof(T)>::Size>;
  if (ZuLikely(n == 1)) return static_cast<T *>(Cache::alloc());
  if (auto ptr = static_cast<T *>(valloc(n * sizeof(T))))
    return ptr;
  throw std::bad_alloc{};
}
template <typename T, typename ID>
inline void ZmAllocator<T, ID>::deallocate(T *p, std::size_t n) noexcept {
  using Cache = ZmHeapCacheT<ID, ZmHeap_Size<sizeof(T)>::Size>;
  if (ZuLikely(n == 1))
    Cache::free(p);
  else
    vfree(p);
}
template <typename T, typename U, class ID>
inline constexpr bool operator ==(
    const ZmAllocator<T, ID> &, const ZmAllocator<U, ID> &) { return true; }
template <typename T, typename U, class ID>
inline constexpr bool operator !=(
    const ZmAllocator<T, ID> &, const ZmAllocator<U, ID> &) { return false; }

#endif /* ZmAllocator_HPP */
