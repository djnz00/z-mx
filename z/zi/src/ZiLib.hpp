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

// Zi library main header

#ifndef ZiLib_HPP
#define ZiLib_HPP

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuLib.hpp>

#ifdef _WIN32

#ifdef ZI_EXPORTS
#define ZiAPI ZuExport_API
#define ZiExplicit ZuExport_Explicit
#else
#define ZiAPI ZuImport_API
#define ZiExplicit ZuImport_Explicit
#endif
#define ZiExtern extern ZiAPI

#else

#define ZiAPI
#define ZiExplicit
#define ZiExtern extern

#endif

#include <zlib/ZuIOResult.hpp>

namespace Zi {
  using namespace Zu::IO;
};

#ifndef _WIN32
#define ZiENOMEM ENOMEM
#define ZiENOENT ENOENT
#define ZiEINVAL EINVAL
#define ZiENOTCONN ENOTCONN
#define ZiECONNRESET ECONNRESET
#define ZiENOBUFS ENOBUFS
#define ZiEADDRINUSE EADDRINUSE
#else
#define ZiENOMEM ERROR_NOT_ENOUGH_MEMORY
#define ZiENOENT ERROR_FILE_NOT_FOUND
#define ZiEINVAL WSAEINVAL
#define ZiENOTCONN WSAEDISCON
#define ZiECONNRESET WSAECONNRESET
#define ZiENOBUFS WSAENOBUFS
#define ZiEADDRINUSE WSAEADDRINUSE
#endif

#endif /* ZiLib_HPP */
