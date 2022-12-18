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

// IO Rx/Tx

#ifndef ZiRx_HPP
#define ZiRx_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZiLib_HPP
#include <zlib/ZiLib.hpp>
#endif

#include <zlib/ZiIOContext.hpp>

namespace ZiRx {

// I/O receiver

// int Hdr(const ZiIOContext &io, const Buf *buf)
//   +ve - length of hdr+body
//   INT_MAX - insufficient data
//   -ve - disconnect
//
// int Body(const ZiIOContext &io, const Buf *buf, unsigned len) // sync
//   0   - skip remaining data (used to defend against DOS)
//   +ve - length of hdr+body (may be less than or equal that returned by Hdr())
//   -ve - disconnect immediately
//
// int Body(const ZiIOContext &io, ZmRef<Buf> buf) // async
//   0   - skip remaining data (used to defend against DOS)
//   +ve - buffer consumed
//   -ve - disconnect immediately

// synchronous receive from ZiIOContext
template <auto Hdr, auto Body, typename Buf>
inline void recv(ZiIOContext &io) {
  ZmRef<Buf> buf = new Buf{};
  auto ptr = buf->data();
  auto size = buf->size;
  io.init(ZiIOFn{ZuMv(buf), [](Buf *buf, ZiIOContext &io) -> uintptr_t {
    unsigned len = io.offset += io.length;
    io.length = 0;

    // scan header
    int frameLen = Hdr(io, buf);
    if (ZuUnlikely(frameLen < 0)) return -1;
    if (len < static_cast<unsigned>(frameLen)) return 0;
    if (ZuUnlikely(static_cast<unsigned>(frameLen) > buf->size)) {
      ZeLOG(Error, "ZdbNet::recv TCP message too big / corrupt");
      io.disconnect();
      return 0;
    }

    // process body
    frameLen = Body(io, buf, frameLen);
    if (ZuUnlikely(frameLen < 0)) return 0;
    if (!frameLen) return len;

    // calculate length of trailing data
    unsigned nextLen = len - frameLen;

    // move trailing data down - recycle buffer for subsequent messages
    if (nextLen) memmove(io.ptr, io.ptr + frameLen, nextLen);

    // set up I/O context for next message
    io.offset = 0;
    io.length = buf->length = nextLen;
    return nextLen;
  }}, ptr, size, 0);
}

// asynchronous (queued) receive from ZiIOContext
template <auto Hdr, auto Body, typename Buf>
inline void recvAsync(ZiIOContext &io) {
  ZmRef<Buf> buf = new Buf{};
  auto ptr = buf->data();
  auto size = buf->size;
  io.init(ZiIOFn{ZuMv(buf), [](Buf *buf, ZiIOContext &io) -> uintptr_t {
    unsigned len = io.offset += io.length;
    io.length = 0;

    // scan header
    int frameLen = Hdr(io, buf);
    if (ZuUnlikely(frameLen < 0)) return -1;
    if (len < static_cast<unsigned>(frameLen)) return 0;
    if (ZuUnlikely(static_cast<unsigned>(frameLen) > buf->size)) {
      ZeLOG(Error, "ZdbNet::recv TCP message too big / corrupt");
      io.disconnect();
      return 0;
    }

    // due to queuing, cannot recycle rx msg buffer for the next message
    ZmRef<Buf> next;
    unsigned nextLen = len - frameLen;
    uint8_t *nextPtr = nullptr;

    // copy any trailing data that is (part of) the next message
    if (nextLen) {
      next = new Buf{};
      nextPtr = next->ensure(nextLen);
      memcpy(nextPtr, io.ptr + frameLen, nextLen);
      next->length = nextLen;
      buf->length = frameLen;
    }

    // process body
    frameLen = Body(io, io.fn.mvObject<Buf>());
    if (ZuUnlikely(frameLen < 0)) return 0;
    if (!frameLen) return len;

    // no trailing data - allocate blank next message
    if (!next) {
      next = new Buf{};
      nextPtr = next->data();
    }

    // set up I/O context for next message
    io.ptr = nextPtr;
    io.size = next->size;
    io.offset = 0;
    io.length = nextLen;
    io.fn.object(ZuMv(next));
    return nextLen;
  }}, ptr, size, 0);
}

// in-memory receiver

// int Hdr(const Buf *buf)
//   +ve - length of hdr+body
//   INT_MAX - insufficient data
//   -ve - disconnect
//
// int Body(const Buf *buf, unsigned len) // sync
//   0   - skip remaining data (used to defend against DOS)
//   +ve - length of hdr+body (may be less than or equal that returned by Hdr())
//   -ve - disconnect immediately
//
// int Body(ZmRef<Buf> buf) // async
//   0   - skip remaining data (used to defend against DOS)
//   +ve - buffer consumed
//   -ve - disconnect immediately

// synchronous recv from memory (e.g. TLS)
// returns bytes consumed, -1 on error
template <auto Hdr, auto Body, typename Buf>
inline int recvMem(const uint8_t *data, unsigned rxLen, ZmRef<Buf> &buf) {
  if (!buf) buf = new Buf{};
  unsigned oldLen = buf->length;
  unsigned len = oldLen + rxLen;
  auto rxData = buf->ensure(len);
  memcpy(rxData + oldLen, data, rxLen);
  buf->length = len;

  while (len) {
    // scan header
    int frameLen = Hdr(buf.ptr());
    if (ZuUnlikely(frameLen < 0)) return -1;
    if (len < static_cast<unsigned>(frameLen)) {
      buf->ensure(frameLen);
      return 0;
    }

    // process body
    frameLen = Body(buf.ptr(), frameLen);
    if (ZuUnlikely(frameLen < 0)) return -1; // error
    if (!frameLen) return rxLen; // EOF - discard remainder

    // skip to next msg
    buf->skip += frameLen;
    rxData += frameLen;
    buf->length = (len -= frameLen);
  }
  if (len && buf->skip) {
    // move down any trailing data
    buf->skip = 0;
    memmove(buf->data(), rxData, len);
  } else
    buf->skip = 0;
  buf->length = len;
  return rxLen;
}

// asynchronous recv from memory
// returns bytes consumed, -1 on error
template <auto Hdr, auto Body, typename Buf>
inline int recvMemAsync(const uint8_t *data, unsigned rxLen, ZmRef<Buf> &buf) {
  if (!buf) buf = new Buf{};
  unsigned oldLen = buf->length;
  unsigned len = oldLen + rxLen;
  auto rxData = buf->ensure(len);
  memcpy(rxData + oldLen, data, rxLen);
  buf->length = len;

  while (len) {
    // scan header
    int frameLen = Hdr(buf.ptr());
    if (ZuUnlikely(frameLen < 0)) return -1;
    if (len < static_cast<unsigned>(frameLen)) return 0;

    // due to queuing, cannot recycle rx msg buffer for the next message
    ZmRef<Buf> next;
    unsigned nextLen = len - frameLen;
    uint8_t *nextPtr = nullptr;

    // copy any trailing data that is (part of) the next message
    if (nextLen) {
      next = new Buf{};
      nextPtr = next->ensure(nextLen);
      memcpy(nextPtr, buf->data() + frameLen, nextLen);
      next->length = nextLen;
      buf->length = frameLen;
    }

    // process body
    frameLen = Body(ZuMv(buf));
    if (ZuUnlikely(frameLen < 0)) return -1; // error
    if (!frameLen) return rxLen; // EOF - discard remainder

    // move on to next message/buffer
    buf = ZuMv(next);
    len = nextLen;
  }
  return rxLen;
}

} // ZiRx

#endif /* ZiRx_HPP */
