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

inline constexpr const char *IOBuf_HeapID() { return "Zdb.IOBuf"; }

struct IOBuf_ : public ZiIOBuf {
  mutable void	*typed = nullptr;	// points to typed Buf<T>

  using ZiIOBuf::ZiIOBuf;

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
  /* ZmAssert(static_cast<const void *>(buf.hdr()) ==
    reinterpret_cast<const void *>(
      (buf.ZiIOBuf::data__ & ~ZiIOBuf::Jumbo) + buf.ZiIOBuf::skip)); */
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
  using ZiIOBuf::data;
};

// buffer allocation

template <unsigned Size = ZiIOBuf_DefaultSize, auto HeapID = ZiIOBuf_HeapID>
using IOBufAlloc = Zi::IOBufAlloc<IOBuf, Size, HeapID>;

using RxBufAlloc = IOBufAlloc<ZiIOBuf_DefaultSize>;

typedef ZmRef<IOBuf> (*IOBufAllocFn)();

} // Zdb_

#endif /* ZdbBuf_HH */
