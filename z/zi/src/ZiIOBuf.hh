//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// IO buffer (packet buffer)
//
//             .-------------.
//             |   ZmVHeap   |
// .-----------+-------------|
// |   ZmHeap  |   ZiIOBuf   |
// |-------------------------|
// |    ZiIOBufAlloc<Size>   |
// '-------------------------'
//
// ZiIOBufAlloc<Size> is ZmHeap allocated up to Size,
// ZmVHeap allocated for extended "jumbo" packets >Size

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
#ifndef ZiIOBuf_DefaultSize
#define ZiIOBuf_DefaultSize 1460
#endif

constexpr const char *ZiIOBuf_HeapID() { return "ZiIOBuf"; }

namespace Zi {

using VHeap = ZmVHeap<ZiIOBuf_HeapID>;

struct IOBuf : private VHeap, public ZmPolymorph {
  mutable void	*owner = nullptr;
  uintptr_t	data__ = 0;
  uint32_t	size = 0;
  uint32_t	length = 0;
  uint32_t	skip = 0;

  // 64bit pointer-packing - uses bit 63
  static constexpr uintptr_t Jumbo = (uintptr_t(1)<<63);

private:
  using VHeap::valloc;
  using VHeap::vfree;

protected:
  IOBuf(uint8_t *data_, uint32_t size_) :
    data__{reinterpret_cast<uintptr_t>(data_)}, size{size_} { }
  IOBuf(uint8_t *data_, uint32_t size_, void *owner_) :
    owner{owner_}, data__{reinterpret_cast<uintptr_t>(data_)}, size{size_} { }
  IOBuf(uint8_t *data_, uint32_t size_, void *owner_, uint32_t length_) :
    owner{owner_}, data__{reinterpret_cast<uintptr_t>(data_)},
    size{size_}, length{length_} { }

public:
  virtual ~IOBuf() { if (ZuUnlikely(data__ & Jumbo)) vfree(data_()); }

private:
  IOBuf(const IOBuf &buf) = delete;
  IOBuf &operator =(const IOBuf &buf) = delete;
  IOBuf(IOBuf &&buf) = delete;
  IOBuf &operator =(IOBuf &&buf) = delete; 

  inline uint8_t *data_() {
    return reinterpret_cast<uint8_t *>(data__ & ~Jumbo);
  }
  inline const uint8_t *data_() const {
    return const_cast<IOBuf *>(this)->data_();
  }

public:
  bool operator !() const { return !length; }

  const uint8_t *data() const { return data_() + skip; }
  uint8_t *data() { return data_() + skip; }

  const uint8_t *end() const { return data_() + skip + length; }
  uint8_t *end() { return data_() + skip + length; }

  unsigned avail() const { return size - (skip + length); }

  uint8_t *alloc(unsigned newSize) {
    if (ZuLikely(newSize <= size)) return data();
    if (auto jumbo = static_cast<uint8_t *>(valloc(newSize))) {
      size = newSize;
      if (ZuUnlikely(data__ & Jumbo)) vfree(data_());
      data__ = reinterpret_cast<uintptr_t>(jumbo) | Jumbo;
      return jumbo;
    }
    size = 0;
    return nullptr;
  }

  void free(uint8_t *ptr) {
    if (!(data__ & Jumbo) && ptr == data_()) return;
    if (ptr == data_()) { data__ = 0; length = size = 0; }
    vfree(ptr);
  }

  void clear() {
    length = 0;
    skip = 0;
  }

  template <typename T>
  const T *ptr() const { return reinterpret_cast<const T *>(data()); }
  template <typename T>
  T *ptr() { return reinterpret_cast<T *>(data()); }

  template <typename T>
  const T &as() const { return *ptr<T>(); }
  template <typename T>
  T &as() { return *ptr<T>(); }

  // low-level buffer access
  auto buf_() { return ZuSpan{data(), size}; }
  auto cbuf_() const { return ZuSpan{data(), length}; }

  // advance/rewind buffer
  void advance(unsigned n) {
    if (ZuUnlikely(!n)) return;
    if (ZuUnlikely(n > length)) n = length;
    skip += n, length -= n;
  }
  void rewind(unsigned n) { // reverses advance - use prepend to grow buffer
    if (ZuUnlikely(!n)) return;
    if (ZuUnlikely(n > skip)) n = skip;
    skip -= n, length += n;
  }

  // reallocate (while building buffer), preserving head and tail bytes
  template <auto Grow = ZmGrow>
  uint8_t *realloc(
      unsigned oldSize, unsigned newSize,
      unsigned head, unsigned tail) {
    ZmAssert(!skip);
    auto old = data_();
    if (ZuLikely(newSize <= size)) {
      if (tail) memmove(old + newSize - tail, old + oldSize - tail, tail);
      size = newSize;
      return old;
    }
    newSize = Grow(size, newSize);
    auto jumbo = reinterpret_cast<uint8_t *>(valloc(newSize));
    if (ZuLikely(jumbo)) {
      if (head) memcpy(jumbo, old, head);
      if (tail) memcpy(jumbo + newSize - tail, old + oldSize - tail, tail);
      size = newSize;
    } else
      length = size = 0;
    if (ZuUnlikely(data__ & Jumbo)) vfree(old);
    data__ = reinterpret_cast<uintptr_t>(jumbo) | Jumbo;
    return jumbo;
  }

  // ensure at least newSize bytes in buffer, preserving any existing data
  template <auto Grow = ZmGrow>
  uint8_t *ensure(unsigned newSize) {
    ZmAssert(!skip);
    if (ZuLikely(newSize <= size)) return data();
    newSize = Grow(size, newSize);
    auto old = data_();
    auto jumbo = reinterpret_cast<uint8_t *>(valloc(newSize));
    if (ZuUnlikely(!jumbo)) return nullptr;
    if (length) memcpy(jumbo, old, length);
    size = newSize;
    if (ZuUnlikely(data__ & Jumbo)) vfree(old);
    data__ = reinterpret_cast<uintptr_t>(jumbo) | Jumbo;
    return jumbo;
  }

  // prepend to buffer (e.g. for a protocol header)
  template <auto Grow = ZmGrow>
  uint8_t *prepend(unsigned length_) {
    ZmAssert(size == skip + length);
    if (ZuLikely(skip >= length_)) {
      skip -= length_;
      length += length_;
      return data();
    }
    auto old = data_();
    auto newSize = length + (length_ - skip);
    if (ZuLikely(newSize <= size)) {
      auto newSkip = size - length;
      if (length) memmove(old + newSkip, old + skip, length);
      skip = newSkip - length_;
      length += length_;
      return data();
    }
    newSize = Grow(size, newSize);
    auto jumbo = reinterpret_cast<uint8_t *>(valloc(newSize));
    if (ZuUnlikely(!jumbo)) return nullptr;
    auto newSkip = newSize - length;
    if (length) memcpy(jumbo + newSkip, old + skip, length);
    size = newSize;
    skip = newSkip - length_;
    length += length_;
    if (ZuUnlikely(data__ & Jumbo)) vfree(old);
    data__ = reinterpret_cast<uintptr_t>(jumbo) | Jumbo;
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
  IOBuf &operator <<(const ZuSpan<const uint8_t> &buf) {
    append(buf.data(), buf.length());
    return *this;
  }
  template <typename C>
  MatchChar<C, IOBuf &> operator <<(C c) {
    this->append(&c, 1);
    return *this;
  }
  template <typename S>
  MatchString<S, IOBuf &> operator <<(S &&s_) {
    ZuCSpan s(ZuFwd<S>(s_));
    append(reinterpret_cast<const uint8_t *>(s.data()), s.length());
    return *this;
  }
  IOBuf &operator <<(const char *s_) {
    ZuCSpan s(s_);
    append(reinterpret_cast<const uint8_t *>(s.data()), s.length());
    return *this;
  }
  template <typename R>
  MatchReal<R, IOBuf &> operator <<(const R &r) {
    append(ZuBoxed(r));
    return *this;
  }
  template <typename P>
  MatchPrint<P, IOBuf &> operator <<(const P &p) {
    append(p);
    return *this;
  }

  struct Traits : public ZuBaseTraits<IOBuf> {
    using Elem = char;
    enum { IsCString = 0, IsString = 1, IsWString = 0 };
    static char *data(IOBuf &buf) {
      return reinterpret_cast<char *>(buf.data());
    }
    static const char *data(const IOBuf &buf) {
      return reinterpret_cast<const char *>(buf.data());
    }
    static unsigned length(const IOBuf &buf) {
      return buf.length;
    }
  };
  friend Traits ZuTraitsType(IOBuf *);
};

// the Base parameter permits Ztls, Zdb, etc. to intrude their own
// buffer type into the hierarchy

template <typename Base, unsigned Size_, typename Heap>
struct IOBufAlloc__ : public Heap, public Base {
  enum { Size = Size_ };

  uint8_t	data_[Size];

  IOBufAlloc__() : Base{&data_[0], Size} { }
  template <typename ...Args>
  IOBufAlloc__(Args &&...args) :
    Base{&data_[0], Size, ZuFwd<Args>(args)...} { }

  ~IOBufAlloc__() = default;

private:
  IOBufAlloc__(const IOBufAlloc__ &) = delete;
  IOBufAlloc__ &operator =(const IOBufAlloc__ &) = delete;
  IOBufAlloc__(IOBufAlloc__ &&) = delete;
  IOBufAlloc__ &operator =(IOBufAlloc__ &&) = delete;
};

template <typename Base, unsigned Size, auto HeapID>
using IOBuf_Heap = ZmHeap<HeapID, IOBufAlloc__<Base, Size, ZuEmpty>>;
 
template <typename Base, unsigned Size, auto HeapID = ZiIOBuf_HeapID>
using IOBufAlloc_ = IOBufAlloc__<Base, Size, IOBuf_Heap<Base, Size, HeapID>>;

template <typename Base>
constexpr unsigned BuiltinSize(unsigned Size) {
  enum { CacheLineSize = Zm::CacheLineSize };
  // MinBufSz - minimum built-in buffer size
  enum { MinBufSz = sizeof(uintptr_t)<<1 };
  // IOBufOverhead - ZiIOBuf overhead
  enum { Overhead = sizeof(IOBufAlloc_<Base, MinBufSz>) - MinBufSz };
  // round up to cache line size, subtract overhead
  // and use that as the built-in buffer size
  return
    ((Size + Overhead + CacheLineSize - 1) & ~(CacheLineSize - 1)) - Overhead;
};

template <typename Base, unsigned Size, auto HeapID>
using IOBufAlloc = IOBufAlloc_<Base, BuiltinSize<Base>(Size), HeapID>;

} // Zi

using ZiIOBuf = Zi::IOBuf;

template <unsigned Size = ZiIOBuf_DefaultSize, auto HeapID = ZiIOBuf_HeapID>
using ZiIOBufAlloc = Zi::IOBufAlloc<ZiIOBuf, Size, HeapID>;

// ensure cache line alignment
ZuAssert(!((sizeof(ZiIOBufAlloc<1>)) & (Zm::CacheLineSize - 1)));

#endif /* ZiIOBuf_HH */
