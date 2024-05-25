//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#ifndef ZvCmdNet_HH
#define ZvCmdNet_HH

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <zlib/ZuInt.hh>
#include <zlib/ZuByteSwap.hh>
#include <zlib/ZuBox.hh>

#include <zlib/ZmAssert.hh>

#include <zlib/Zfb.hh>

// custom header

namespace ZvCmd {

namespace Type {
  inline ZuID login()		{ static ZuID id{"login"}; return id; }
  inline ZuID userDB()		{ static ZuID id{"userDB"}; return id; }
  inline ZuID cmd()		{ static ZuID id{"cmd"}; return id; }
  inline ZuID telReq()		{ static ZuID id{"telReq"}; return id; }
  inline ZuID telemetry()	{ static ZuID id{"telemtry"}; return id; }
}

// flatbuffers' built-in prefixing of size and file identifier has
// a couple of shortcomings - file identifiers are limited to 4 bytes,
// and are stored after the root vtable, not contiguous with the size prefix
//
// custom header with a fixed-width 8-byte type identifer
// and an explicitly little-endian uint32 length
#pragma pack(push, 1)
struct Hdr {
  ZuID				type;
  ZuLittleEndian<uint32_t>	length;	// length of message excluding header

  const uint8_t *data() const {
    return reinterpret_cast<const uint8_t *>(this) + sizeof(Hdr);
  }
};
#pragma pack(pop)

// call following Finish() to ensure alignment
template <typename Builder, typename Owner>
inline ZmRef<typename Builder::IOBuf>
saveHdr(Builder &fbb, ZuID type, Owner *owner) {
  unsigned length = fbb.GetSize();
  auto buf = fbb.buf();
  buf->owner = owner;
  auto ptr = buf->prepend(sizeof(Hdr));
  if (ZuUnlikely(!ptr)) return nullptr;
  new (ptr) Hdr{type, length};
  return buf;
}
template <typename Builder>
inline auto saveHdr(Builder &fbb, ZuID type) {
  return saveHdr(fbb, type, static_cast<void *>(nullptr));
}
// returns the total length of the message including the header, or
// INT_MAX if not enough bytes have been read yet
template <typename Buf>
inline int loadHdr(const Buf *buf) {
  if (ZuUnlikely(buf->length < sizeof(Hdr))) return INT_MAX;
  auto hdr = reinterpret_cast<const Hdr *>(buf->data());
  return sizeof(Hdr) + static_cast<uint32_t>(hdr->length);
}
// returns -1 if the header is invalid/corrupted, or lambda return
template <typename Buf, typename Fn>
inline int verifyHdr(const Buf *buf, Fn fn) {
  if (ZuUnlikely(buf->length < sizeof(Hdr))) return -1;
  auto hdr = reinterpret_cast<const Hdr *>(buf->data());
  unsigned length = hdr->length;
  if (length > (buf->length - sizeof(Hdr))) return -1;
  int i = fn(hdr, buf);
  if (i <= 0) return i;
  return sizeof(Hdr) + i;
}

} // namespace ZvCmd

#endif /* ZvCmdNet_HH */
