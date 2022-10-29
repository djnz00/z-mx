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

// ring buffer error codes

#ifndef ZmRingError_HPP
#define ZmRingError_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

namespace ZmRingErrorCode {
  enum { OK = 0, EndOfFile = -1, Error = -2, NotReady = -3 };
}

struct ZmRingError {
  int code;
  ZmRingError(int code_) : code{code_} { }
  template <typename S> void print(S &s) const {
    using namespace ZmRingErrorCode;
    switch (code) {
      case OK:		s << "OK"; break;
      case EndOfFile:	s << "EndOfFile"; break;
      case Error:	s << "Error"; break;
      case NotReady:	s << "NotReady"; break;
      default:		s << "Unknown"; break;
    }
  }
  friend ZuPrintFn ZuPrintType(ZmRingError *);
};

#endif /* ZmRingError_HPP */
