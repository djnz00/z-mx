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
#define ZiIOBuf_DefaultSize 1460

inline constexpr const char *ZiIOBuf_HeapID() { return "ZiIOBuf"; }
struct ZiIOBuf : public ZmPolymorph, private ZmVHeap<ZiIOBuf_HeapID> {
  mutable void	*owner = nullptr;
  uintptr_t	data__ = 0;
  uint32_t	size = 0;
  uint32_t	length = 0;
  uint32_t	skip = 0;

  // 64bit pointer-packing - uses bit 63
  static constexpr const uintptr_t Jumbo = (uintptr_t(1)<<63);

private:
  using Heap = ZmVHeap<ZiIOBuf_HeapID>;
  using Heap::valloc;
  using Heap::vfree;

protected:
  ZiIOBuf(uint8_t *data_, uint32_t size_) :
    data__{reinterpret_cast<uintptr_t>(data_)}, size{size_} { }
  ZiIOBuf(uint8_t *data_, uint32_t size_, void *owner_) :
    owner{owner_}, data__{reinterpret_cast<uintptr_t>(data_)}, size{size_} { }
  ZiIOBuf(uint8_t *data_, uint32_t size_, void *owner_, uint32_t length_) :
    owner{owner_}, data__{reinterpret_cast<uintptr_t>(data_)},
    size{size_}, length{length_} { }

public:
  virtual ~ZiIOBuf() { if (ZuUnlikely(data__ & Jumbo)) vfree(data()); }

private:
  ZiIOBuf(const ZiIOBuf &buf) = delete;
  ZiIOBuf &operator =(const ZiIOBuf &buf) = delete;
  ZiIOBuf(ZiIOBuf &&buf) = delete;
  ZiIOBuf &operator =(ZiIOBuf &&buf) = delete; 

  inline uint8_t *data_() {
    return reinterpret_cast<uint8_t *>(data__ & ~Jumbo);
  }
  inline const uint8_t *data_() const {
    return const_cast<ZiIOBuf *>(this)->data();
  }

public:
  bool operator !() const { return !length; }

  const uint8_t *data() const { return data_() + skip; }
  uint8_t *data() { return data_() + skip; }

  uint8_t *alloc(unsigned newSize) {
    if (ZuLikely(newSize <= size)) return data();
    if (auto jumbo = static_cast<uint8_t *>(valloc(newSize))) {
      size = newSize;
      if (ZuUnlikely(data__ & Jumbo)) vfree(data());
      data__ = reinterpret_cast<uintptr_t>(jumbo) | Jumbo;
      return jumbo;
    }
    size = 0;
    return nullptr;
  }

  void free(uint8_t *ptr) {
    if (!(data__ & Jumbo) && ptr == data_()) return;
    if (ptr == data()) { data__ = 0; length = size = 0; }
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
  auto buf_() { return ZuArray{data(), size}; }
  auto cbuf_() const { return ZuArray{data(), length}; }

  // advance/rewind buffer
  void advance(unsigned n) {
    if (ZuUnlikely(!n)) return;
    if (ZuUnlikely(n > length)) n = length;
    skip += n, length -= n;
  }
  void rewind(unsigned n) { // reverses skip - use prepend to grow buffer
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
  ZiIOBuf &operator <<(const ZuArray<const uint8_t> &buf) {
    append(buf.data(), buf.length());
    return *this;
  }
  template <typename C>
  MatchChar<C, ZiIOBuf &> operator <<(C c) {
    this->append(&c, 1);
    return *this;
  }
  template <typename S>
  MatchString<S, ZiIOBuf &> operator <<(S &&s_) {
    ZuString s(ZuFwd<S>(s_));
    append(reinterpret_cast<const uint8_t *>(s.data()), s.length());
    return *this;
  }
  ZiIOBuf &operator <<(const char *s_) {
    ZuString s(s_);
    append(reinterpret_cast<const uint8_t *>(s.data()), s.length());
    return *this;
  }
  template <typename R>
  MatchReal<R, ZiIOBuf &> operator <<(const R &r) {
    append(ZuBoxed(r));
    return *this;
  }
  template <typename P>
  MatchPrint<P, ZiIOBuf &> operator <<(const P &p) {
    append(p);
    return *this;
  }

  struct Traits : public ZuBaseTraits<ZiIOBuf> {
    using Elem = char;
    enum { IsCString = 0, IsString = 1, IsWString = 0 };
    static char *data(ZiIOBuf &buf) {
      return reinterpret_cast<char *>(buf.data());
    }
    static const char *data(const ZiIOBuf &buf) {
      return reinterpret_cast<const char *>(buf.data());
    }
    static unsigned length(const ZiIOBuf &buf) {
      return buf.length;
    }
  };
  friend Traits ZuTraitsType(ZiIOBuf *);
};

#pragma pack(push, 1)

template <unsigned Size_, typename Heap>
struct ZiIOBufAlloc_ : public Heap, public ZiIOBuf {
  enum { Size = Size_ };

  uint8_t	data_[Size];

  ZiIOBufAlloc_() : ZiIOBuf{&data_[0], Size} { }
  template <typename ...Args>
  ZiIOBufAlloc_(Args &&...args) :
    ZiIOBuf{&data_[0], Size, ZuFwd<Args>(args)...} { }

  ~ZiIOBufAlloc_() = default;

private:
  ZiIOBufAlloc_(const ZiIOBufAlloc_ &) = delete;
  ZiIOBufAlloc_ &operator =(const ZiIOBufAlloc_ &) = delete;
  ZiIOBufAlloc_(ZiIOBufAlloc_ &&) = delete;
  ZiIOBufAlloc_ &operator =(ZiIOBufAlloc_ &&) = delete;
};

#pragma pack(pop)

template <unsigned Size, auto HeapID>
using ZiIOBuf_Heap = ZmHeap<HeapID, sizeof(ZiIOBufAlloc_<Size, ZuNull>)>;
 
template <unsigned Size = ZiIOBuf_DefaultSize, auto HeapID = ZiIOBuf_HeapID>
using ZiIOBufAlloc = ZiIOBufAlloc_<Size, ZiIOBuf_Heap<Size, HeapID>>;

#endif /* ZiIOBuf_HH */
