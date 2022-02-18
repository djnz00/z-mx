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

#ifndef ZvCmdNet_HPP
#define ZvCmdNet_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

#include <zlib/ZuInt.hpp>
#include <zlib/ZuByteSwap.hpp>
#include <zlib/ZuBox.hpp>

#include <zlib/ZmAssert.hpp>

#include <zlib/Zfb.hpp>

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
inline void saveHdr(Zfb::Builder &fbb, ZuID type) {
  unsigned length = fbb.GetSize();
  new (Zfb::Save::extend(fbb, sizeof(Hdr))) Hdr{type, length};
}
// returns the total length of the message including the header, or
// INT_MAX if not enough bytes have been read yet
inline int loadHdr(const uint8_t *data, unsigned length) {
  if (ZuUnlikely(length < sizeof(Hdr))) return INT_MAX;
  auto hdr = reinterpret_cast<const Hdr *>(data);
  return sizeof(Hdr) + static_cast<uint32_t>(hdr->length);
}
// returns -1 if the header is invalid/corrupted, or lambda return
template <typename Fn>
inline int verifyHdr(const uint8_t *data, unsigned length_, Fn fn) {
  if (ZuUnlikely(length_ < sizeof(Hdr))) return -1;
  auto hdr = reinterpret_cast<const Hdr *>(data);
  unsigned length = hdr->length;
  if (length > (length_ - sizeof(Hdr))) return -1;
  int i = fn(hdr, hdr->data(), length);
  if (i < 0) return i;
  return sizeof(Hdr) + i;
}

} // namespace ZvCmd

#endif /* ZvCmdNet_HPP */
