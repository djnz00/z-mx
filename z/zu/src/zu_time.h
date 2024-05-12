//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// ZuTime - C API

#ifndef zu_time_H
#define zu_time_H

#ifdef _MSC_VER
#pragma once
#endif

#ifndef zu_lib_H
#include <zlib/zu_lib.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

/*
 * carbon copy of POSIX struct timespec, padded out to 16 bytes
 */
typedef struct {
  int64_t	tv_sec;
  uint32_t	tv_nsec;
  uint32_t	_;	// pad to 16 bytes
} zu_time;

/* parse string, returns #bytes scanned, 0 on invalid input */
ZuExtern unsigned int zu_time_in_csv(zu_time *v, const char *s);
ZuExtern unsigned int zu_time_in_iso(zu_time *v, const char *s);
ZuExtern unsigned int zu_time_in_fix(zu_time *v, const char *s);
ZuExtern unsigned int zu_time_in_interval(zu_time *v, const char *s);
/* returns output length including null terminator */
ZuExtern unsigned int zu_time_out_csv_len(const zu_time *v);
ZuExtern unsigned int zu_time_out_iso_len(const zu_time *v);
ZuExtern unsigned int zu_time_out_fix_len(const zu_time *v);
ZuExtern unsigned int zu_time_out_interval_len(const zu_time *v);
/* output to string, returns end pointer to null terminator */
ZuExtern char *zu_time_out_csv(char *s, const zu_time *v);
ZuExtern char *zu_time_out_iso(char *s, const zu_time *v);
ZuExtern char *zu_time_out_fix(char *s, const zu_time *v);
ZuExtern char *zu_time_out_interval(char *s, const zu_time *v);

/* convert to/from int128 */
ZuExtern __int128_t zu_time_to_int(const zu_time *v); /* truncates */
ZuExtern zu_time *zu_time_from_int(zu_time *v, __int128_t i);

/* convert to/from long double */
ZuExtern long double zu_time_to_ldouble(const zu_time *v);
ZuExtern zu_time *zu_time_from_ldouble(zu_time *v, long double d);

/* 3-way comparison */
ZuExtern int zu_time_cmp(const zu_time *l, const zu_time *r);

/* hash */
ZuExtern uint32_t zu_time_hash(const zu_time *v);

/* negate */
ZuExtern zu_time *zu_time_neg(zu_time *v, const zu_time *p);

/* add, subtract, multiply, divide */
ZuExtern zu_time *zu_time_add(
    zu_time *v, const zu_time *l, const zu_time *r);
ZuExtern zu_time *zu_time_sub(
    zu_time *v, const zu_time *l, const zu_time *r);
ZuExtern zu_time *zu_time_mul(
    zu_time *v, const zu_time *l, long double r);
ZuExtern zu_time *zu_time_div(
    zu_time *v, const zu_time *l, long double r);

#ifdef __cplusplus
}
#endif

#endif /* zu_time_H */
