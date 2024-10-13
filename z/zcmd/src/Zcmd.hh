//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#ifndef Zcmd_HH
#define Zcmd_HH

#ifndef ZcmdLib_HH
#include <zlib/ZcmdLib.hh>
#endif

#include <zlib/ZuInt.hh>
#include <zlib/ZuID.hh>
#include <zlib/ZuByteSwap.hh>

#include <zlib/Zfb.hh>

#include <zlib/Ztls.hh>

#include <zlib/zcmd_request_fbs.h>
#include <zlib/zcmd_reqack_fbs.h>

namespace Zcmd {

using IOFn = ZmFn<void (ZmRef<ZiIOBuf>)>;

// pre-defined message types
namespace Type {
  inline ZuID login()		{ static ZuID id{"login"}; return id; }
  inline ZuID userDB()		{ static ZuID id{"userDB"}; return id; }
  inline ZuID cmd()		{ static ZuID id{"cmd"}; return id; }
  inline ZuID telReq()		{ static ZuID id{"telReq"}; return id; }
  inline ZuID telemetry()	{ static ZuID id{"telemtry"}; return id; }
}

// flatbuffers' built-in prefixing of size and file identifier has
// a couple of shortcomings - file identifiers are limited to 4 bytes
// and are stored after the root vtable, not contiguous with the size prefix

// this is a custom header with a fixed-width 8-byte type identifer
// and an explicitly little-endian uint32 length
#pragma pack(push, 4)
struct Hdr {
  ZuID				type;
  ZuLittleEndian<uint32_t>	length;	// length of message excluding header

  const uint8_t *data() const {
    return reinterpret_cast<const uint8_t *>(this) + sizeof(Hdr);
  }
};
#pragma pack(pop)

template <typename Owner>
inline ZmRef<ZiIOBuf>
saveHdr(ZmRef<ZiIOBuf> buf, ZuID type, Owner *owner) {
  unsigned length = buf->length;
  buf->owner = owner;
  auto ptr = buf->prepend(sizeof(Hdr));
  if (ZuUnlikely(!ptr)) return nullptr;
  new (ptr) Hdr{type, length};
  return buf;
}
inline auto saveHdr(ZmRef<ZiIOBuf> buf, ZuID type) {
  return saveHdr(ZuMv(buf), type, static_cast<void *>(nullptr));
}
// returns the total length of the message including the header, or
// INT_MAX if not enough bytes have been read yet
inline int loadHdr(const ZiIOBuf *buf) {
  if (ZuUnlikely(buf->length < sizeof(Hdr))) return INT_MAX;
  auto hdr = reinterpret_cast<const Hdr *>(buf->data());
  return sizeof(Hdr) + static_cast<uint32_t>(hdr->length);
}
// returns -1 if the header is invalid/corrupted, or lambda return
// - async version - advances buffer past header and moves it to lambda
template <typename L>
inline int verifyHdr(ZmRef<ZiIOBuf> buf, L &&l) {
  if (ZuUnlikely(buf->length < sizeof(Hdr))) return -1;
  auto hdr = reinterpret_cast<const Hdr *>(buf->data());
  unsigned length = hdr->length;
  if (length > (buf->length - sizeof(Hdr))) return -1;
  buf->advance(sizeof(Hdr));
  // -ve - disconnect; 0 - skip remaining data; +ve - continue to next frame
  return ZuFwd<L>(l)(hdr, ZuMv(buf));
}
// returns -1 if the header is invalid/corrupted, or lambda return
// - sync version - does not mutate buffer
template <typename L>
inline int verifyHdrSync(const ZiIOBuf *buf, L &&l) {
  if (ZuUnlikely(buf->length < sizeof(Hdr))) return -1;
  auto hdr = reinterpret_cast<const Hdr *>(buf->data());
  unsigned length = hdr->length;
  if (length > (buf->length - sizeof(Hdr))) return -1;
  int i = ZuFwd<L>(l)(hdr, buf);
  if (i <= 0) return i;
  return sizeof(Hdr) + i;
}

} // Zcmd

#endif /* Zcmd_HH */
