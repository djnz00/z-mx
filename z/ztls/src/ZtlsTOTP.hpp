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

// time-based one time password (Google Authenticator compatible)

#ifndef ZtlsTOTP_HPP
#define ZtlsTOTP_HPP

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZtlsLib.hpp>

#include <stdlib.h>

#include <zlib/ZuByteSwap.hpp>

#include <zlib/ZmTime.hpp>

#include <zlib/ZtlsHMAC.hpp>

namespace Ztls::TOTP {

ZtlsExtern unsigned calc(const uint8_t *data, unsigned len, int offset = 0);
ZtlsExtern bool verify(
    const uint8_t *data, unsigned len, unsigned code, unsigned range);

}

#endif /* ZtlsTOTP_HPP */
