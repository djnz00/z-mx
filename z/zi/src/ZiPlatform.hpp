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

// platform primitives

#ifndef ZiPlatform_HPP
#define ZiPlatform_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZiLib_HPP
#include <zlib/ZiLib.hpp>
#endif

#ifndef _WIN32

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <limits.h>
#include <fcntl.h>
#include <netdb.h>

#ifdef NETLINK
#include <linux/netlink.h>
#include <linux/genetlink.h>
#endif

#else /* !_WIN32 */

#include <winsock2.h>
#include <ws2tcpip.h>
#include <mswsock.h>

#endif /* !_WIN32 */

#include <zlib/ZuInt.hpp>
#include <zlib/ZuTraits.hpp>

#include <zlib/ZePlatform.hpp>

#include <zlib/ZtString.hpp>

namespace Zi {

#ifndef _WIN32
using Handle = int;
ZuInline constexpr const Handle nullHandle() { return -1; }
ZuInline constexpr bool nullHandle(Handle i) { return i < 0; }
using Socket = int;
ZuInline constexpr const Socket nullSocket() { return -1; }
ZuInline constexpr bool nullSocket(Socket i) { return i < 0; }
inline void closeSocket(Socket s) { ::close(s); }
using MMapPtr = void *;
using Path = ZtString;
using Offset = off_t;
using Hostname = ZtString;
using Username = ZtString;
enum {
  PathMax = PATH_MAX,
  NameMax = NAME_MAX,
  NVecMax = IOV_MAX
};
#else
using Handle = HANDLE;
ZuInline const Handle nullHandle() { return INVALID_HANDLE_VALUE; }
ZuInline bool nullHandle(Handle i) { return !i || i == INVALID_HANDLE_VALUE; }
using Socket = SOCKET;
ZuInline constexpr const Socket nullSocket() { return INVALID_SOCKET; }
ZuInline constexpr bool nullSocket(Socket i) { return i == INVALID_SOCKET; }
inline void closeSocket(Socket s) { ::closesocket(s); }
using MMapPtr = LPVOID;
using Path = ZtWString;
using Offset = int64_t;		// 2x DWORD
using Hostname = ZtWString;
using Username = ZtWString;
enum {
  PathMax = 32767,	// NTFS limit (MAX_PATH is 260 for FAT)
  NameMax = 255,	// NTFS & FAT limit
  NVecMax = 2048	// Undocumented system-wide limit
};
#endif

enum {
  HostnameMax = NI_MAXHOST,
  ServicenameMax = NI_MAXSERV
};

ZiExtern Username username(ZeError *e = 0); /* effective username */
ZiExtern Hostname hostname(ZeError *e = 0);

} // namespace Zi

#ifndef _WIN32
using ZiVec = struct iovec;
using ZiVecPtr = void *;
using ZiVecLen = size_t;
#define ZiVec_ptr(x) (x).iov_base
#define ZiVec_len(x) (x).iov_len
#else
using ZiVec = WSABUF;
using ZiVecPtr = char *;
using ZiVecLen = u_long;
#define ZiVec_ptr(x) (x).buf
#define ZiVec_len(x) (x).len
#endif
inline void ZiVec_init(ZiVec &vec, void *ptr, size_t len) {
  ZiVec_ptr(vec) = static_cast<ZiVecPtr>(ptr);
  ZiVec_len(vec) = len;
}

#ifndef _WIN32
#include <dlfcn.h>	// dlerror()
#include <zlib/ZePlatform.hpp>
#endif

#endif /*  ZiPlatform_HPP */
