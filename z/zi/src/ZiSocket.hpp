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

// socket I/O

#ifndef ZiSocket_HPP
#define ZiSocket_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZiLib_HPP
#include <zlib/ZiLib.hpp>
#endif

#include <zlib/ZiPlatform.hpp>

class ZiAPI ZiSocket {
public:
  using Socket = Zi::Socket;
  using SocketCmp = Zi::SocketCmp;
  using Hostname = Zi::Hostname;

  enum Result {
    OK		= Zi::OK,
    EndOfFile	= Zi::EndOfFile,
    IOError	= Zi::IOError
  };

  enum Flags {
    OOB		= 0x01,
    GC		= 0x02
  };
  enum Protocol { UDP = 0, TCP = 1 };

  ZiSocket() { }

  Result socket(Protocol protocol);
  Result connect(const Hostname &hostname, int port, Error *Error = 0);
  Result bind(const Hostname &hostname, int port, Error *Error = 0);
  Result listen(Error *error = 0);
  Result accept(ZiSocket &socket, Error *error = 0);

private:
  Socket	m_socket;
};

#endif /* ZiSocket_HPP */
