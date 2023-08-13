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

// Z Database vocabulary types

#ifndef ZdbTypes_HPP
#define ZdbTypes_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZdbLib_HPP
#include <zlib/ZdbLib.hpp>
#endif

namespace Zdb_ {

// record number type and sentinel values
using RN = uint64_t;		// RN is primary object key / ID
inline constexpr uint64_t maxRN() { return ~static_cast<uint64_t>(0); }
inline constexpr uint64_t nullRN() { return ZuCmp<RN>::null(); }

// primitive database journal op codes; ops are mutable, lifecycle is:
namespace Op {
  ZtEnumValues("Zdb_::Op",
    Put = 0,	// add this record, delete prevRN (and predecessors)
    Append,	// add this record, preserve prevRN
    Delete,	// tombstone, delete prevRN (no data)
    Purge	// add this purge instruction, delete all < prevRN (no data)
  );
}

namespace RecState {
  ZtEnumValues("Zdb_::RecState",
    Created = 0,
    Deleting,
    Deleted,
    Purging,
    Purged
  );
}

using Offset = int64_t;

// -- message format - used for both file and network

// custom header with an explicitly little-endian uint32 length
#pragma pack(push, 1)
struct Hdr {
  ZuLittleEndian<uint32_t>	length;	// length of body

  const uint8_t *data() const {
    return reinterpret_cast<const uint8_t *>(this) + sizeof(Hdr);
  }
};
#pragma pack(pop)

// call following Finish() to push header and detach buffer
template <typename Builder, typename Owner>
inline auto saveHdr(Builder &fbb, Owner *owner) {
  unsigned length = fbb.GetSize();
  auto buf = fbb.buf();
  buf->owner = owner;
  auto ptr = buf->prepend(sizeof(Hdr));
  if (ZuUnlikely(!ptr)) return decltype(buf){};
  new (ptr) Hdr{length};
  return buf;
}
template <typename Builder>
inline auto saveHdr(Builder &fbb) {
  return saveHdr(fbb, static_cast<void *>(nullptr));
}
// returns the total length of the message including the header,
// INT_MAX if not enough bytes have been read yet, -1 if corrupt
template <typename Buf>
inline int loadHdr(const Buf *buf) {
  if (ZuUnlikely(buf->length < sizeof(Hdr))) return INT_MAX;
  auto hdr = buf->template ptr<Hdr>();
  return sizeof(Hdr) + static_cast<uint32_t>(hdr->length);
}
// returns -1 if the header is invalid/corrupted, or lambda return
template <typename Buf, typename Fn>
inline int verifyHdr(ZmRef<Buf> buf, Fn fn) {
  if (ZuUnlikely(buf->length < sizeof(Hdr))) return -1;
  auto hdr = buf->template ptr<Hdr>();
  unsigned length = hdr->length;
  if (length > (buf->length - sizeof(Hdr))) return -1;
  int i = fn(hdr, ZuMv(buf));
  if (i < 0) return i;
  return sizeof(Hdr) + i;
}
// payload data containing a single whole message
inline ZuArray<const uint8_t> msgData(const Hdr *hdr) {
  if (ZuUnlikely(!hdr)) return {};
  return {
    reinterpret_cast<const uint8_t *>(hdr),
    static_cast<unsigned>(sizeof(Hdr) + hdr->length)};
}

inline const fbs::Msg *msg(const Hdr *hdr) {
  if (ZuUnlikely(!hdr)) return nullptr;
  auto data = hdr->data();
  if (ZuUnlikely((!Zfb::Verifier{data, hdr->length}.VerifyBuffer<fbs::Msg>())))
    return nullptr;
  return Zfb::GetRoot<fbs::Msg>(data);
}
inline const fbs::Msg *msg_(const Hdr *hdr) {
  return Zfb::GetRoot<fbs::Msg>(hdr->data());
}
inline const fbs::Heartbeat *hb_(const fbs::Msg *msg) {
  return static_cast<const fbs::Heartbeat *>(msg->body());
}
inline const fbs::Heartbeat *hb(const fbs::Msg *msg) {
  if (ZuUnlikely(!msg)) return nullptr;
  switch (static_cast<int>(msg->body_type())) {
    default:
      return nullptr;
    case fbs::Body_HB:
      return hb_(msg);
  }
}
inline bool recovery(const fbs::Msg *msg) {
  if (ZuUnlikely(!msg)) return false;
  return msg->body_type() == fbs::Body_Rec;
}
inline bool recovery_(const fbs::Msg *msg) {
  return msg->body_type() == fbs::Body_Rec;
}
inline const fbs::Record *record_(const fbs::Msg *msg) {
  return static_cast<const fbs::Record *>(msg->body());
}
inline const fbs::Record *record(const fbs::Msg *msg) {
  if (ZuUnlikely(!msg)) return nullptr;
  switch (static_cast<int>(msg->body_type())) {
    default:
      return nullptr;
    case fbs::Body_Rep:
    case fbs::Body_Rec:
      return record_(msg);
  }
}
inline const fbs::Gap *gap_(const fbs::Msg *msg) {
  return static_cast<const fbs::Gap *>(msg->body());
}
inline const fbs::Gap *gap(const fbs::Msg *msg) {
  if (ZuUnlikely(!msg)) return nullptr;
  if (static_cast<int>(msg->body_type()) != fbs::Body_Gap)
    return nullptr;
  return gap_(msg);
}
template <typename T>
inline const T *data(const fbs::Record *record) {
  if (ZuUnlikely(!record)) return nullptr;
  auto data = Zfb::Load::bytes(record->data());
  if (ZuUnlikely(!data)) return nullptr;
  if (ZuUnlikely((
	!Zfb::Verifier{data.data(), data.length()}.VerifyBuffer<T>())))
    return nullptr;
  return Zfb::GetRoot<T>(data.data());
}
template <typename T>
inline const T *data_(const fbs::Record *record) {
  auto data = Zfb::Load::bytes(record->data());
  if (ZuUnlikely(!data)) return nullptr;
  return Zfb::GetRoot<T>(data.data());
}

} // Zdb_

#endif /* ZdbTypes_HPP */
