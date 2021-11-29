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

// IO buffer (packet buffer)

#ifndef ZiIOBuf_HPP
#define ZiIOBuf_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZiLib_HPP
#include <zlib/ZiLib.hpp>
#endif

#include <zlib/ZuGrow.hpp>
#include <zlib/ZuPrint.hpp>

#include <zlib/ZmPolymorph.hpp>
#include <zlib/ZmHeap.hpp>

// TCP over Ethernet maximum payload is 1460 (without Jumbo frames)
#define ZiIOBuf_DefaultSize 1460

#pragma pack(push, 2)
struct ZiAnyIOBuf : public ZmPolymorph {
  uint32_t	size = 0;
  uint32_t	length = 0;

  ZiAnyIOBuf() = default;
  ZiAnyIOBuf(uint32_t length_) : length{length_} { }
  ZiAnyIOBuf(const ZiAnyIOBuf &) = default;
  ZiAnyIOBuf &operator =(const ZiAnyIOBuf &) = default;
  ZiAnyIOBuf(ZiAnyIOBuf &&) = default;
  ZiAnyIOBuf &operator =(ZiAnyIOBuf &&) = default;
};
template <unsigned Size_, typename Heap>
struct ZiIOBuf_ : public Heap, public ZiAnyIOBuf {
  void		*owner = nullptr;
  uint8_t	*jumbo = nullptr;
  uint32_t	skip = 0;
  uint8_t	data_[Size_];

  enum { Size = Size_ };

  ZiIOBuf_() { }
  ZiIOBuf_(void *owner_) : owner{owner_} { }
  ZiIOBuf_(void *owner_, uint32_t length) :
      ZiAnyIOBuf{length}, owner{owner_} { }
  ~ZiIOBuf_() { if (ZuUnlikely(jumbo)) ::free(jumbo); }

  uint8_t *alloc(unsigned size_) {
    if (ZuLikely(size_ <= Size)) {
      size = size_;
      return data_;
    }
    if (jumbo = static_cast<uint8_t *>(::malloc(size_))) {
      size = size_;
      return jumbo;
    }
    size = 0;
    return nullptr;
  }

  void free(uint8_t *ptr) {
    if (ZuUnlikely(ptr != data_)) {
      if (ZuUnlikely(jumbo == ptr)) {
	jumbo = nullptr;
	length = size = 0;
      }
      ::free(ptr);
    }
  }

  void clear() {
    length = 0;
    skip = 0;
  }

  const uint8_t *data() const { return const_cast<ZiIOBuf_ *>(this)->data(); }
  uint8_t *data() {
    uint8_t *ptr = ZuUnlikely(jumbo) ? jumbo : data_;
    return ptr + skip;
  }

  template <typename T>
  const T &as() const { return *reinterpret_cast<const T *>(data()); }
  template <typename T>
  T &as() { return *reinterpret_cast<T *>(data()); }

  // reallocate (while building buffer), preserving head and tail bytes
  template <typename Grow>
  uint8_t *realloc(
      unsigned oldSize, unsigned newSize,
      unsigned head, unsigned tail,
      Grow grow = [](unsigned o, unsigned n) { return ZuGrow(o, n); })
  {
    if (ZuLikely(newSize <= Size)) {
      if (tail) memmove(data_ + newSize - tail, data_ + oldSize - tail, tail);
      size = newSize;
      return data_;
    }
    if (ZuUnlikely(newSize <= size)) {
      if (tail) memmove(jumbo + newSize - tail, jumbo + oldSize - tail, tail);
      size = newSize;
      return jumbo;
    }
    newSize = grow(size, newSize);
    uint8_t *old = ZuUnlikely(jumbo) ? jumbo : data_;
    jumbo = static_cast<uint8_t *>(::malloc(newSize));
    if (ZuLikely(jumbo)) {
      if (head) memcpy(jumbo, old, head);
      if (tail) memcpy(jumbo + newSize - tail, old + oldSize - tail, tail);
      size = newSize;
    } else
      length = size = 0;
    if (ZuUnlikely(old != data_)) ::free(old);
    return jumbo;
  }

  // ensure at least newSize bytes in buffer, preserving any existing data
  uint8_t *ensure(unsigned newSize) {
    return
      ensure(newSize, [](unsigned o, unsigned n) { return ZuGrow(o, n); });
  }
  template <typename Grow>
  uint8_t *ensure(unsigned newSize, Grow grow) {
    if (ZuLikely(newSize <= Size)) { size = newSize; return data_; }
    if (ZuUnlikely(newSize <= size)) { size = newSize; return jumbo; }
    newSize = grow(size, newSize);
    uint8_t *old = ZuUnlikely(jumbo) ? jumbo : data_;
    jumbo = static_cast<uint8_t *>(::malloc(newSize));
    if (ZuLikely(jumbo)) {
      if (length) memcpy(jumbo, old, length);
      size = newSize;
    } else
      length = size = 0;
    if (ZuUnlikely(old != data_)) ::free(old);
    return jumbo;
  }

private:
  void append(const uint8_t *data, unsigned length_) {
    unsigned total = length + length_;
    memcpy(ensure(total) + length, data, length_);
    length = total;
  }

  template <typename U, typename R = void>
  using MatchPDelegate = ZuIfT<ZuPrint<U>::Delegate, R>;
  template <typename U, typename R = void>
  using MatchPBuffer = ZuIfT<ZuPrint<U>::Buffer, R>;

  template <typename P>
  MatchPDelegate<P> append(const P &p) {
    ZuPrint<P>::print(*this, p);
  }
  template <typename P>
  MatchPBuffer<P> append(const P &p) {
    unsigned length_ = ZuPrint<P>::length(p);
    length += ZuPrint<P>::print(
	reinterpret_cast<char *>(ensure(length + length_) + length),
	length_, p);
  }

  template <typename U, typename R = void>
  using MatchChar = ZuSame<U, char, R>;

  template <typename U, typename R = void>
  using MatchString = ZuIfT<
    ZuTraits<U>::IsString &&
    !ZuTraits<U>::IsWString &&
    !ZuTraits<U>::IsPrimitive, R>;

  template <typename U, typename R = void>
  using MatchReal = ZuIfT<
    ZuTraits<U>::IsPrimitive &&
    ZuTraits<U>::IsReal &&
    !ZuConversion<U, char>::Same, R>;

  template <typename U, typename R = void>
  using MatchPrint = ZuIfT<
    ZuPrint<U>::OK && !ZuPrint<U>::String, R>;

public:
  template <typename C>
  MatchChar<C, ZiIOBuf_ &> operator <<(C c) {
    this->append(&c, 1);
    return *this;
  }
  template <typename S>
  MatchString<S, ZiIOBuf_ &> operator <<(S &&s_) {
    ZuString s(ZuFwd<S>(s_));
    append(s.data(), s.length());
    return *this;
  }
  ZiIOBuf_ &operator <<(const char *s_) {
    ZuString s(s_);
    append(s.data(), s.length());
    return *this;
  }
  template <typename R>
  MatchReal<R, ZiIOBuf_ &> operator <<(const R &r) {
    append(ZuBoxed(r));
    return *this;
  }
  template <typename P>
  MatchPrint<P, ZiIOBuf_ &> operator <<(const P &p) {
    append(p);
    return *this;
  }

  struct Traits : public ZuBaseTraits<ZiIOBuf_> {
    using Elem = char;
    enum { IsCString = 0, IsString = 1, IsWString = 0 };
    static const char *data(const ZiIOBuf_ &buf) {
      return reinterpret_cast<const char *>(buf.data());
    }
    static unsigned length(const ZiIOBuf_ &buf) {
      return buf.length;
    }
  };
  friend Traits ZuTraitsType(ZiIOBuf_ *);
};
#pragma pack(pop)
struct ZiIOBuf_HeapID {
  static constexpr const char *id() { return "ZiIOBuf"; }
};
template <unsigned Size>
using ZiIOBuf_Heap = ZmHeap<ZiIOBuf_HeapID, sizeof(ZiIOBuf_<Size, ZuNull>)>;
 
template <unsigned Size = ZiIOBuf_DefaultSize>
using ZiIOBuf = ZiIOBuf_<Size, ZiIOBuf_Heap<Size>>;

#endif /* ZiIOBuf_HPP */
