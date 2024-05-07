//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// ZuDecimal - C API

#ifndef zu_decimal_H
#define zu_decimal_H

#ifdef _MSC_VER
#pragma once
#endif

#ifndef zu_lib_H
#include <zlib/zu_lib.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int128_t	value;
} zu_decimal;

/* parse string, returns v */
ZuExtern zu_decimal *zu_decimal_in(zu_decimal *v, const char *s);

/* returns output length including null terminator */
ZuExtern unsigned int zu_decimal_out_len(const zu_decimal *v);

/* output to string, returns end pointer to null terminator */
ZuExtern char *zu_decimal_out(char *s, const zu_decimal *v);

/* 3-way comparison */
ZuExtern int zu_decimal_cmp(const zu_decimal *l, const zu_decimal *r);

/* add, subtract, multiply, divide */
ZuExtern zu_decimal *zu_decimal_add(
    zu_decimal *v, const zu_decimal *l, const zu_decimal *r);
ZuExtern zu_decimal *zu_decimal_sub(
    zu_decimal *v, const zu_decimal *l, const zu_decimal *r);
ZuExtern zu_decimal *zu_decimal_mul(
    zu_decimal *v, const zu_decimal *l, const zu_decimal *r);
ZuExtern zu_decimal *zu_decimal_div(
    zu_decimal *v, const zu_decimal *l, const zu_decimal *r);

#ifdef __cplusplus
}
#endif

#endif /* zu_decimal_H */
