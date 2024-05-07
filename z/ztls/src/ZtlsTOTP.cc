//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// time-based one time password (Google Authenticator compatible)

#include <zlib/ZtlsTOTP.hh>

namespace Ztls::TOTP {

ZtlsExtern unsigned calc(ZuBytes data, int offset)
{
  ZuBigEndian<uint64_t> t = (ZmTimeNow().sec() / 30) + offset;
  HMAC hmac(MBEDTLS_MD_SHA1);
  uint8_t sha1[20];
  hmac.start(data);
  hmac.update({reinterpret_cast<const uint8_t *>(&t), 8});
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

ZtlsExtern bool verify(ZuBytes data, unsigned code, unsigned range)
{
  for (int i = -static_cast<int>(range); i <= static_cast<int>(range); i++)
    if (code == calc(data, i)) return true;
  return false;
}

} // Ztls::TOTP
