//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Z Database buffer

#ifndef ZdbBuf_HH
#define ZdbBuf_HH

#ifndef ZdbLib_HH
#include <zlib/ZdbLib.hh>
#endif

#include <zlib/ZmPolyHash.hh>

#include <zlib/ZiIOBuf.hh>

#include <zlib/ZdbMsg.hh>

namespace Zdb_ {

// --- I/O Buffer Sizes

enum { DefltBufSize = 192 };		// default row buffer size
enum { HBBufSize = 128 };		// heartbeat buffer size
enum { TelBufSize = 128 };		// telemetry buffer size

class AnyTable;
class DB;

// --- I/O buffer

inline constexpr const char *Buf_HeapID() { return "Zdb.Buf"; }

struct IOBuf_ : public ZiIOBuf {
  mutable void	*typed = nullptr;	// points to typed Buf<T>

  using ZiIOBuf::ZiIOBuf;
  template <typename ...Args>
  IOBuf_(Args &&...args) : ZiIOBuf{ZuFwd<Args>(args)...} { }

  auto hdr() const { return ptr<Hdr>(); }
  auto hdr() { return ptr<Hdr>(); }

  struct Print {
    const IOBuf_ *buf = nullptr;
    const AnyTable *table = nullptr;
    template <typename S> void print(S &s) const;
    friend ZuPrintFn ZuPrintType(Print *);
  };
  Print print(AnyTable *table = nullptr) { return Print{this, table}; }
};

inline UN IOBuf_UNAxor(const IOBuf_ &buf) {
  return record_(msg_(buf.hdr()))->un();
}

using BufCacheUN =
  ZmHash<IOBuf_,
    ZmHashNode<IOBuf_,
      ZmHashKey<IOBuf_UNAxor,
	ZmHashLock<ZmPLock,
	  ZmHashShadow<true>>>>>;

struct IOBuf : public BufCacheUN::Node {
  using Base = BufCacheUN::Node;

  using Base::Base;
  using Base::operator =;
  template <typename ...Args>
  IOBuf(Args &&...args) : Base{ZuFwd<Args>(args)...} { }

  using ZiIOBuf::data;
};

// buffer allocator

template <unsigned Size_, typename Heap>
struct IOBufAlloc__ : public Heap, public IOBuf {
  enum { Size = Size_ };

  uint8_t	data_[Size];

  IOBufAlloc__() : IOBuf{&data_[0], Size} { }
  template <typename ...Args>
  IOBufAlloc__(Args &&...args) :
    IOBuf{&data_[0], Size, ZuFwd<Args>(args)...} { }

  ~IOBufAlloc__() = default;

private:
  IOBufAlloc__(const IOBufAlloc__ &) = delete;
  IOBufAlloc__ &operator =(const IOBufAlloc__ &) = delete;
  IOBufAlloc__(IOBufAlloc__ &&) = delete;
  IOBufAlloc__ &operator =(IOBufAlloc__ &&) = delete;
};

template <unsigned Size, auto HeapID>
using IOBuf_Heap = ZmHeap<HeapID, sizeof(IOBufAlloc__<Size, ZuNull>)>;
 
template <unsigned Size = ZiIOBuf_DefaultSize, auto HeapID = Buf_HeapID>
using IOBufAlloc_ = IOBufAlloc__<Size, IOBuf_Heap<Size, HeapID>>;

inline constexpr const unsigned BuiltinSize(unsigned Size) {
  enum { CacheLineSize = Zm::CacheLineSize };
  // MinBufSz - minimum built-in buffer size
  enum { MinBufSz = sizeof(uintptr_t)<<1 };
  // IOBufOverhead - ZiIOBuf overhead
  enum { Overhead = sizeof(IOBufAlloc_<MinBufSz>) - MinBufSz };
  // round up to cache line size, subtract overhead
  // and use that as the built-in buffer size
  return
    ((Size + Overhead + CacheLineSize - 1) & ~(CacheLineSize - 1)) - Overhead;
};

template <unsigned Size>
using IOBufAlloc = IOBufAlloc_<BuiltinSize(Size), Buf_HeapID>;

// ensure cache line alignment
ZuAssert(!((sizeof(IOBufAlloc<1>)) & (Zm::CacheLineSize - 1)));

using RxBufAlloc = IOBufAlloc<ZiIOBuf_DefaultSize>;

} // Zdb_

#endif /* ZdbBuf_HH */
