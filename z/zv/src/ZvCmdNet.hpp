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
// we use a custom header with an explicitly little-endian
// uint32 length and a fixed-width 8-byte type identifer
#pragma pack(push, 1)
struct Hdr {
  ZuID				type;
  ZuLittleEndian<uint32_t>	len;	// length of message excluding header

  const uint8_t *data() const {
    return reinterpret_cast<const uint8_t *>(this) + sizeof(Hdr);
  }
};
#pragma pack(pop)

// call following Finish() to ensure alignment
template <typename FBB>
inline void saveHdr(FBB &fbb, ZuID type) {
  Hdr hdr{type, fbb.GetSize()};
  fbb.PushBytes(reinterpret_cast<const uint8_t *>(&hdr), sizeof(Hdr));
}
// returns the total length of the message including the header, or
// INT_MAX if not enough bytes have been read yet
inline int loadHdr(const uint8_t *data, unsigned len) {
  if (ZuUnlikely(len < sizeof(Hdr))) return INT_MAX;
  auto hdr = reinterpret_cast<const Hdr *>(data);
  return sizeof(Hdr) + hdr->len;
}
// returns nullptr if the header is invalid/corrupted
inline const Hdr *verifyHdr(const uint8_t *data, unsigned len) {
  if (len < sizeof(Hdr)) return nullptr;
  auto hdr = reinterpret_cast<const Hdr *>(data);
  if (hdr->len > (len - sizeof(Hdr))) return nullptr;
  return hdr;
}

} // namespace ZvCmd

#endif /* ZvCmdNet_HPP */
