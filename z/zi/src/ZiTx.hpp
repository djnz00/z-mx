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

// IO Tx

#ifndef ZiTx_HPP
#define ZiTx_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZiLib_HPP
#include <zlib/ZiLib.hpp>
#endif

#include <zlib/ZiIOContext.hpp>

// CRTP sender

template <typename Impl_, typename Buf_>
class ZiTx {
public:
  using Impl = Impl_;
  using Buf = Buf_;

  auto impl() const { return static_cast<const Impl *>(this); }
  auto impl() { return static_cast<Impl *>(this); }

  template <typename Cxn>
  void send(Cxn *cxn, ZmRef<Buf> buf) {
    cxn->send(ZiIOFn{ZuMv(buf), [](Buf *buf, ZiIOContext &io) {
      io.init(ZiIOFn{io.fn.mvObject<Buf>(), [](Buf *buf, ZiIOContext &io) {
	if (ZuUnlikely((io.offset += io.length) < io.size)) return;
	io.complete();
      }}, buf->data(), buf->length, 0);
    }});
  }

  // void Sent(ZmRef<Buf> buf) // async
  template <auto Sent, typename Cxn>
  void send(Cxn *cxn, ZmRef<Buf> buf) {
    cxn->send(ZiIOFn{ZuMv(buf), [](Buf *buf, ZiIOContext &io) {
      io.init(ZiIOFn{io.fn.mvObject<Buf>(), [](Buf *buf, ZiIOContext &io) {
	if (ZuUnlikely((io.offset += io.length) < io.size)) return;
	io.complete();
	ZuInvoke<Sent>(impl(), io.fn.mvObject<Buf>());
      }}, buf->data(), buf->length, 0);
    }});
  }
};

#endif /* ZiTx_HPP */
