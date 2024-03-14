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

// Z Database buffer

#ifndef ZdbBuf_HPP
#define ZdbBuf_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdbLib_HPP
#include <zlib/ZdbLib.hpp>
#endif

#include <zlib/ZdbMsg.hpp>

namespace Zdb_ {

class DB;

// --- I/O buffer

inline constexpr const char *Buf_HeapID() { return "Zdb.Buf"; }
inline constexpr unsigned BuiltinSize() {
  enum { CacheLineSize = Zm::CacheLineSize };
  // MinBufSz - minimum built-in buffer size
  enum { MinBufSz = sizeof(uintptr_t)<<1 };
  // IOBufOverhead - ZiIOBuf overhead
  enum { IOBufOverhead = sizeof(ZiIOBuf<MinBufSz, Buf_HeapID>) - MinBufSz };
  // HashOverhead - ZmHash node overhead - assumed to be sizeof(uintptr_t)
  enum { HashOverhead = sizeof(uintptr_t) };
  // TotalOverhead - total buffer overhead
  enum { Overhead = IOBufOverhead + HashOverhead };
  // round up overhead to cache line size, multiply by 4,
  // subtract original overhead, and use that as the built-in buffer size
  return (((Overhead + CacheLineSize) & ~(CacheLineSize - 1))<<2) - Overhead;
};
using VBuf = ZiIOVBuf<BuiltinSize(), Buf_HeapID>; 
RN Buf_RNAxor(const VBuf &);
using RepBufs =
  ZmHash<VBuf,
    ZmHashNode<VBuf,
      ZmHashKey<Buf_RNAxor,
	ZmHashLock<ZmPLock,
	  ZmHashHeapID<Buf_HeapID>>>>>;
class ZdbAPI Buf : public RepBufs::Node {
  using Buf_ = RepBufs::Node;

public:
  enum { BufSize = VBuf::Size };
  Buf() = default;
  Buf(const Buf &) = default;
  Buf &operator =(const Buf &) = default;
  Buf(Buf &&) = default;
  Buf &operator =(Buf &&) = default;
  template <typename ...Args>
  Buf(Args &&... args) : Buf_{ZuFwd<Args>(args)...} { }
  template <typename Arg>
  Buf &operator =(Arg &&arg) {
    return static_cast<Buf &>(Buf_::operator =(ZuFwd<Arg>(arg)));
  }

  DB *db() const { return static_cast<DB *>(owner); }
  Env *env() const { return static_cast<Env *>(owner); }

  auto hdr() const { return ptr<Hdr>(); }
  auto hdr() { return ptr<Hdr>(); }

  using VBuf::data; // deconflict with RepBufs node data()

  template <typename S> void print(S &) const;
  friend ZuPrintFn ZuPrintType(Buf *);
};
inline RN Buf_RNAxor(const VBuf &buf) {
  return record_(msg_(static_cast<const Buf &>(buf).hdr()))->rn();
}
inline UN Buf_UNAxor(const ZmRef<Buf> &buf) {
  return record_(msg_(static_cast<const Buf *>(buf.ptr())->hdr()))->un();
}
using UpdBufs =
  ZmHash<ZmRef<Buf>,
    ZmHashKey<Buf_UNAxor,
      ZmHashLock<ZmPLock,
	ZmHashHeapID<Buf_HeapID>>>>;

ZuAssert(!((sizeof(Buf)) & (Zm::CacheLineSize - 1)));
using IOBuilder = Zfb::IOBuilder<Buf>;

} // Zdb_

#endif /* ZdbBuf_HPP */
