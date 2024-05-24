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

class AnyTable;
class DB;

// --- I/O buffer

inline constexpr const char *Buf_HeapID() { return "Zdb.Buf"; }

inline constexpr unsigned BuiltinSize() {
  enum { CacheLineSize = Zm::CacheLineSize };
  // MinBufSz - minimum built-in buffer size
  enum { MinBufSz = sizeof(uintptr_t)<<1 };
  // IOBufOverhead - ZiIOBuf overhead
  enum { IOBufOverhead = sizeof(ZiIOBuf<MinBufSz, Buf_HeapID>) - MinBufSz };
  // HashOverhead - ZmHash node overhead
  struct V { int i_; static constexpr int i(const V &v) { return v.i_; } };
  using VHash = ZmHash<V, ZmHashNode<V, ZmHashKey<V::i, ZmHashShadow<true>>>>;
  enum { HashOverhead = sizeof(VHash::Node) - sizeof(V) };
  // Overhead - total buffer overhead
  enum { Overhead = IOBufOverhead + HashOverhead };
  // TCP over Ethernet maximum payload is 1460 (without Jumbo frames)
  enum { Size = 1460 };
  // round up to cache line size, subtract overhead
  // and use that as the built-in buffer size
  return
    ((Size + Overhead + CacheLineSize - 1) & ~(CacheLineSize - 1)) - Overhead;
};
using VBuf = ZiIOVBuf<BuiltinSize(), Buf_HeapID>;

struct AnyBuf_ : public VBuf {
  mutable void	*typed = nullptr;	// points to typed Buf<T>

  using VBuf::VBuf;
  using VBuf::operator =;

  auto hdr() const { return ptr<Hdr>(); }
  auto hdr() { return ptr<Hdr>(); }

  struct Print {
    const AnyBuf_ *buf = nullptr;
    const AnyTable *table = nullptr;
    template <typename S> void print(S &s) const;
    friend ZuPrintFn ZuPrintType(Print *);
  };
  Print print(AnyTable *table = nullptr) { return Print{this, table}; }
};

inline UN AnyBuf_UNAxor(const AnyBuf_ &buf) {
  return record_(msg_(buf.hdr()))->un();
}

using BufCacheUN =
  ZmHash<AnyBuf_,
    ZmHashNode<AnyBuf_,
      ZmHashKey<AnyBuf_UNAxor,
	ZmHashLock<ZmPLock,
	  ZmHashShadow<true>>>>>;

struct AnyBuf : public BufCacheUN::Node {
  using Base = BufCacheUN::Node;
  using Base::Base;
  using Base::operator =;
  using VBuf::data;
};

// ensure cache line alignment
ZuAssert(!((sizeof(AnyBuf)) & (Zm::CacheLineSize - 1)));

using IOBuilder = Zfb::IOBuilder<AnyBuf>;

} // Zdb_

#endif /* ZdbBuf_HH */
