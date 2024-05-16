//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// mbedtls C++ wrapper - HMAC message digest

#ifndef ZtlsHMAC_HH
#define ZtlsHMAC_HH

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZtlsLib.hh>

#include <zlib/ZuBytes.hh>

#include <mbedtls/md.h>

namespace Ztls {

template <int> struct HMAC_Size { enum { N = 32 }; };
template <> struct HMAC_Size<MBEDTLS_MD_SHA384> { enum { N = 48 }; };
template <> struct HMAC_Size<MBEDTLS_MD_SHA512> { enum { N = 64 }; };

class HMAC {
public:
  template <int I> using Size = HMAC_Size<I>;

  // mbedtls_md_type_t, e.g. MBEDTLS_MD_SHA256
  HMAC(mbedtls_md_type_t type) {
    mbedtls_md_init(&m_ctx);
    mbedtls_md_setup(&m_ctx, mbedtls_md_info_from_type(type), 1);
  }
  ~HMAC() {
    mbedtls_md_free(&m_ctx);
  }

  ZuInline void start(ZuBytes a) {
    mbedtls_md_hmac_starts(&m_ctx, a.data(), a.length());
  }

  ZuInline void update(ZuBytes a) {
    mbedtls_md_hmac_update(&m_ctx, a.data(), a.length());
  }

  // MBEDTLS_MD_MAX_SIZE is max size of output buffer (i.e. 64 for SHA512)
  ZuInline void finish(uint8_t *output) {
    mbedtls_md_hmac_finish(&m_ctx, output);
  }

  ZuInline void reset() {
    mbedtls_md_hmac_reset(&m_ctx);
  }

private:
  mbedtls_md_context_t	m_ctx;
};

}

#endif /* ZtlsHMAC_HH */
