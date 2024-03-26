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

// mbedtls C++ wrapper - random number generator

#ifndef ZtlsRandom_HPP
#define ZtlsRandom_HPP

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZtlsLib.hpp>

#include <mbedtls/entropy.h>
#include <mbedtls/ctr_drbg.h>

namespace Ztls {

class Random {
public:
  Random() {
    mbedtls_entropy_init(&m_entropy);
    mbedtls_ctr_drbg_init(&m_ctr_drbg);
  }
  ~Random() {
    mbedtls_ctr_drbg_free(&m_ctr_drbg);
    mbedtls_entropy_free(&m_entropy);
  }

  bool init() {
    int n = mbedtls_ctr_drbg_seed(
	&m_ctr_drbg, mbedtls_entropy_func, &m_entropy, 0, 0);
    return !n;
  }

  bool random(ZuArray<uint8_t> data) {
    int i = mbedtls_ctr_drbg_random(&m_ctr_drbg, data.data(), data.length());
    return i >= 0;
  }

protected:
  ZuInline mbedtls_ctr_drbg_context *ctr_drbg() { return &m_ctr_drbg; }

private:
  mbedtls_entropy_context	m_entropy;
  mbedtls_ctr_drbg_context	m_ctr_drbg;
};

}

#endif /* ZtlsRandom_HPP */
