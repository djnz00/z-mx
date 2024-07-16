//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// IO Tx

#ifndef ZiTx_HH
#define ZiTx_HH

#ifndef ZiLib_HH
#include <zlib/ZiLib.hh>
#endif

#include <zlib/ZiIOContext.hh>

// CRTP sender

template <typename Impl_>
class ZiTx {
public:
  using Impl = Impl_;

  auto impl() const { return static_cast<const Impl *>(this); }
  auto impl() { return static_cast<Impl *>(this); }

  static auto impl(const ZiIOBuf *buf) {
    return static_cast<Impl *>(buf->owner);
  }

  void sent(ZmRef<const ZiIOBuf>) { } // can be overridden

  void send(ZmRef<const ZiIOBuf> buf) {
    buf->owner = impl();
    impl()->ZiConnection::send(ZiIOFn{ZuMv(buf),
      [](const ZiIOBuf *buf, ZiIOContext &io) {
	io.init(ZiIOFn{io.fn.mvObject<const ZiIOBuf>(),
	  [](const ZiIOBuf *buf, ZiIOContext &io) {
	    if (ZuUnlikely((io.offset += io.length) < io.size)) return true;
	    auto buf_ = io.fn.mvObject<const ZiIOBuf>();
	    io.complete();
	    auto impl_ = impl(buf_);
	    impl_->sent(ZuMv(buf_));
	    return true;
	  }}, buf->data(), buf->length, 0);
	return true;
      }});
  }
};

#endif /* ZiTx_HH */
