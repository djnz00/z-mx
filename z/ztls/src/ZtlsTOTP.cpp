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

#include <zlib/ZtlsTOTP.hpp>

namespace Ztls {
namespace TOTP {

ZtlsExtern unsigned calc(const void *data, unsigned len, int offset)
{
  ZuBigEndian<uint64_t> t = (ZmTimeNow().sec() / 30) + offset;
  HMAC hmac(MBEDTLS_MD_SHA1);
  uint8_t sha1[20];
  hmac.start(data, len);
  hmac.update(&t, 8);
  hmac.finish(sha1);
  ZuBigEndian<uint32_t> code_;
  {
    void *ZuMayAlias(ptr) = reinterpret_cast<void *>(&code_);
    memcpy(ptr, sha1 + (sha1[19] & 0xf), 4);
  }
  uint32_t code = code_;
  code &= ~(static_cast<uint32_t>(1)<<31);
  return code % static_cast<uint32_t>(1000000);
}

ZtlsExtern bool verify(
    const void *data, unsigned len, unsigned code, unsigned range)
{
  for (int i = -static_cast<int>(range); i <= static_cast<int>(range); i++)
    if (code == calc(data, len, i)) return true;
  return false;
}

}
}
