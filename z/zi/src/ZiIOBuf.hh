//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// IO buffer (packet buffer)
//
//              .------------.-------------.
//              |   ZmVHeap  |  ZiAnyIOBuf |
// .------------+--------------------------|
// |   ZmHeap   |         ZiIOVBuf         |
// |---------------------------------------|
// |                ZiIOBuf                |
// '---------------------------------------'
//
// ZiIOBuf<Size> is ZmHeap allocated up to Size, ZmVHeap allocated
// for extended "jumbo" packets >Size
//
// ZiIOVBuf<Size, HeapID> is ZmVHeap allocated for jumbo packets, can
// be used as a base for use of a non-default ZmHeap by derived classes

#ifndef ZiIOBuf_HH
#define ZiIOBuf_HH

#ifndef ZiLib_HH
#include <zlib/ZiLib.hh>
#endif

#include <zlib/ZuPrint.hh>

#include <zlib/ZmPolymorph.hh>
#include <zlib/ZmHeap.hh>
#include <zlib/ZmVHeap.hh>

// TCP over Ethernet maximum payload is 1460 (without Jumbo frames)
#define ZiIOBuf_DefaultSize 1460

#pragma pack(push, 1)

struct ZiAnyIOBuf : public ZmPolymorph {
  uint32_t	size = 0;
  uint32_t	length = 0;

  bool operator !() const { return !length; }
  ZuOpBool

  ZiAnyIOBuf(uint32_t size_, uint32_t length_) :
      size{size_}, length{length_} { }

  ZiAnyIOBuf() = delete;
  ZiAnyIOBuf(const ZiAnyIOBuf &) = delete;
  ZiAnyIOBuf &operator =(const ZiAnyIOBuf &) = delete;
  ZiAnyIOBuf(ZiAnyIOBuf &&) = delete;
  ZiAnyIOBuf &operator =(ZiAnyIOBuf &&) = delete;
};

inline constexpr const char *ZiIOBuf_HeapID() { return "ZiIOBuf"; }
template <unsigned Size_, auto ID = ZiIOBuf_HeapID>
struct ZiIOVBuf : private ZmVHeap<ID>, public ZiAnyIOBuf {
  mutable void	*owner = nullptr;
  uint8_t	*jumbo = nullptr;
  uint32_t	skip = 0;
  uint8_t	data_[Size_];

private:
  using ZmVHeap<ID>::valloc;
  using ZmVHeap<ID>::vfree;

public:
  enum { Size = Size_ };

  ZiIOVBuf() : ZiAnyIOBuf{Size_, 0} { }
  ZiIOVBuf(void *owner_) : ZiAnyIOBuf{Size_, 0}, owner{owner_} { }
  ZiIOVBuf(void *owner_, uint32_t length) :
      ZiAnyIOBuf{Size_, length}, owner{owner_} { }
  ~ZiIOVBuf() { if (ZuUnlikely(jumbo)) vfree(jumbo); }

  ZiIOVBuf(const ZiIOVBuf &buf) :
      ZiAnyIOBuf{buf.size, buf.length}, owner{buf.owner} {
    if (auto data = alloc(buf.length)) {
      skip = buf.skip;
      memcpy(data + skip, buf.data() + skip, length = buf.length);
    }
  }
  ZiIOVBuf &operator =(const ZiIOVBuf &buf) {
    if (ZuLikely(this != &buf)) {
      this->~ZiIOVBuf();
      new (this) ZiIOVBuf{buf};
    }
    return *this;
  }
  ZiIOVBuf(ZiIOVBuf &&buf) :
      ZiAnyIOBuf{buf.size, buf.length},
      owner{buf.owner}, jumbo{buf.jumbo}, skip{buf.skip} {
    if (ZuUnlikely(jumbo)) {
      buf.size = 0;
      buf.length = 0;
      buf.jumbo = nullptr;
      buf.skip = 0;
    } else {
      memcpy(data_ + skip, buf.data_ + skip, length);
    }
  }
  ZiIOVBuf &operator =(ZiIOVBuf &&buf) {
    this->~ZiIOVBuf();
    new (this) ZiIOVBuf{ZuMv(buf)};
    return *this;
  }

  uint8_t *alloc(unsigned size_) {
    if (ZuLikely(size_ <= Size)) {
      size = size_;
      return data_;
    }
    if (jumbo = static_cast<uint8_t *>(valloc(size_))) {
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
      vfree(ptr);
    }
  }

  void clear() {
    length = 0;
    skip = 0;
  }

  const uint8_t *data() const { return const_cast<ZiIOVBuf *>(this)->data(); }
  uint8_t *data() {
    uint8_t *ptr = ZuUnlikely(jumbo) ? jumbo : data_;
    return ptr + skip;
  }

  template <typename T>
  const T *ptr() const { return reinterpret_cast<const T *>(data()); }
  template <typename T>
  T *ptr() { return reinterpret_cast<T *>(data()); }

  template <typename T>
  const T &as() const { return *ptr(); }
  template <typename T>
  T &as() { return *ptr(); }

  // low-level buffer access
  auto buf_() { return ZuArray{data(), size}; }
  auto cbuf_() const { return ZuArray{data(), length}; }

  // reallocate (while building buffer), preserving head and tail bytes
  template <auto Grow = ZmGrow>
  uint8_t *realloc(
      unsigned oldSize, unsigned newSize,
      unsigned head, unsigned tail) {
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
    newSize = Grow(size, newSize);
    uint8_t *old = ZuUnlikely(jumbo) ? jumbo : data_;
    jumbo = static_cast<uint8_t *>(valloc(newSize));
    if (ZuLikely(jumbo)) {
      if (head) memcpy(jumbo, old, head);
      if (tail) memcpy(jumbo + newSize - tail, old + oldSize - tail, tail);
      size = newSize;
    } else
      length = size = 0;
    if (ZuUnlikely(old != data_)) vfree(old);
    return jumbo;
  }

  // ensure at least newSize bytes in buffer, preserving any existing data
  template <auto Grow = ZmGrow>
  uint8_t *ensure(unsigned newSize) {
    ZmAssert(!skip);
    if (ZuLikely(newSize <= Size)) { size = newSize; return data_; }
    if (ZuUnlikely(newSize <= size)) { size = newSize; return jumbo; }
    newSize = Grow(size, newSize);
    uint8_t *old = ZuUnlikely(jumbo) ? jumbo : data_;
    jumbo = static_cast<uint8_t *>(valloc(newSize));
    if (ZuUnlikely(!jumbo)) return nullptr;
    if (length) memcpy(jumbo, old, length);
    size = newSize;
    if (ZuUnlikely(old != data_)) vfree(old);
    return jumbo;
  }

  template <auto Grow = ZmGrow>
  uint8_t *prepend(unsigned length_) {
    ZmAssert(size == skip + length);
    if (ZuLikely(skip >= length_)) {
      skip -= length_;
      length += length_;
      return data();
    }
    auto newSize = length + length_;
    if (ZuLikely(newSize <= Size)) {
      auto newSkip = skip + (Size - size);
      if (length) memmove(data_ + newSkip, data_ + skip, length);
      size = Size;
      skip = newSkip - length_;
      length += length_;
      return data_ + skip;
    }
    newSize = Grow(size, newSize);
    uint8_t *old = ZuUnlikely(jumbo) ? jumbo : data_;
    jumbo = static_cast<uint8_t *>(valloc(newSize));
    if (ZuUnlikely(!jumbo)) return nullptr;
    auto newSkip = skip + (newSize - size);
    if (length) memcpy(jumbo + newSkip, old + skip, length);
    size = newSize;
    skip = newSkip - length_;
    length += length_;
    if (ZuUnlikely(old != data_)) vfree(old);
    return jumbo + skip;
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
    !ZuInspect<U, char>::Same, R>;

  template <typename U, typename R = void>
  using MatchPrint = ZuIfT<
    ZuPrint<U>::OK && !ZuPrint<U>::String, R>;

public:
  ZiIOVBuf &operator <<(const ZuArray<const uint8_t> &buf) {
    append(buf.data(), buf.length());
    return *this;
  }
  template <typename C>
  MatchChar<C, ZiIOVBuf &> operator <<(C c) {
    this->append(&c, 1);
    return *this;
  }
  template <typename S>
  MatchString<S, ZiIOVBuf &> operator <<(S &&s_) {
    ZuString s(ZuFwd<S>(s_));
    append(s.data(), s.length());
    return *this;
  }
  ZiIOVBuf &operator <<(const char *s_) {
    ZuString s(s_);
    append(s.data(), s.length());
    return *this;
  }
  template <typename R>
  MatchReal<R, ZiIOVBuf &> operator <<(const R &r) {
    append(ZuBoxed(r));
    return *this;
  }
  template <typename P>
  MatchPrint<P, ZiIOVBuf &> operator <<(const P &p) {
    append(p);
    return *this;
  }

  struct Traits : public ZuBaseTraits<ZiIOVBuf> {
    using Elem = char;
    enum { IsCString = 0, IsString = 1, IsWString = 0 };
    static char *data(ZiIOVBuf &buf) {
      return reinterpret_cast<char *>(buf.data());
    }
    static const char *data(const ZiIOVBuf &buf) {
      return reinterpret_cast<const char *>(buf.data());
    }
    static unsigned length(const ZiIOVBuf &buf) {
      return buf.length;
    }
  };
  friend Traits ZuTraitsType(ZiIOVBuf *);
};

#pragma pack(pop)

template <unsigned Size_, typename Heap, auto HeapID>
struct ZiIOBuf_ : public Heap, public ZiIOVBuf<Size_, HeapID> {
  using VBuf = ZiIOVBuf<Size_, HeapID>;
  enum { Size = VBuf::Size };
  ZiIOBuf_() = default;
  template <typename ...Args>
  ZiIOBuf_(Args &&...args) : VBuf{ZuFwd<Args>(args)...} { }
  ~ZiIOBuf_() = default;
  ZiIOBuf_(const ZiIOBuf_ &) = delete;
  ZiIOBuf_ &operator =(const ZiIOBuf_ &) = delete;
  ZiIOBuf_(ZiIOBuf_ &&) = delete;
  ZiIOBuf_ &operator =(ZiIOBuf_ &&) = delete;
};
template <unsigned Size, auto HeapID>
using ZiIOBuf_Heap = ZmHeap<HeapID, sizeof(ZiIOBuf_<Size, ZuNull, HeapID>)>;
 
template <unsigned Size = ZiIOBuf_DefaultSize, auto HeapID = ZiIOBuf_HeapID>
using ZiIOBuf = ZiIOBuf_<Size, ZiIOBuf_Heap<Size, HeapID>, HeapID>;

#endif /* ZiIOBuf_HH */
