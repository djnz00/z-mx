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

// IO context

// This design intentionally sacrifices encapsulation for performance

#ifndef ZiIOContext_HPP
#define ZiIOContext_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZiLib_HPP
#include <zlib/ZiLib.hpp>
#endif

#include <zlib/ZmAssert.hpp>
#include <zlib/ZmFn.hpp>

#include <zlib/ZiIP.hpp>

class ZiConnection;

class ZiIOContext {
  constexpr ZuInline static uintptr_t invalid_ptr() {
    return static_cast<uintptr_t>(-1);
  }

friend ZiConnection;
  // initialize (called from within send/recv)
  template <typename Fn>
  void init_(Fn &&fn_) {
    fn = ZuFwd<Fn>(fn_); ptr = nullptr; size = offset = length = 0; (*this)();
  }

public:
  // send/receive
  template <typename Fn>
  void init(Fn &&fn_,
      void *ptr_, unsigned size_, unsigned offset_) {
    ZmAssert(size_);
    fn = ZuFwd<Fn>(fn_);
    ptr = static_cast<uint8_t *>(ptr_);
    size = size_; offset = offset_; length = 0;
  }
  // UDP send
  template <typename Fn, typename Addr>
  void init(Fn &&fn_,
      void *ptr_, unsigned size_, unsigned offset_, Addr &&addr_) {
    ZmAssert(size_);
    fn = ZuFwd<Fn>(fn_);
    ptr = static_cast<uint8_t *>(ptr_);
    size = size_; offset = offset_; length = 0;
    addr = ZuFwd<Addr>(addr_);
  }
  // initially, ptr will be null and app must set it via init()
  bool initialized() { return ptr; }

  // complete send/receive without disconnecting
  void complete() {
    fn = ZmAnyFn();
    ptr = nullptr;
  }
  bool completed() const { return !fn; }

  // complete send/receive and disconnect
  void disconnect() {
    fn = ZmAnyFn();
    ptr = reinterpret_cast<uint8_t *>(invalid_ptr());
  }
  bool disconnected() const {
    return reinterpret_cast<uintptr_t>(ptr) == invalid_ptr();
  }

  uintptr_t operator()();

  ZiConnection	*cxn = nullptr;	// connection - set by ZiMultiplex
  ZmAnyFn	fn;		// callback - set by app (clear to complete I/O)
  uint8_t	*ptr = nullptr;	// buffer - set by app (clear to disconnect)
  unsigned	size = 0;	// size of buffer - set by app
  unsigned	offset = 0;	// offset within buffer - set by app
  unsigned	length = 0;	// length - set by ZiMultiplex
  ZiSockAddr	addr;		// set by app (send) / ZiMultiplex (recv)
};
using ZiIOFn = ZmFn<ZiIOContext &>;
inline uintptr_t ZiIOContext::operator ()() { return fn.as<ZiIOFn>()(*this); }

#endif /* ZiIOContext_HPP */
