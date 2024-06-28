//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// ZtBitmap - C API

#ifndef zu_bitmap_H
#define zu_bitmap_H

#ifndef zu_lib_H
#include <zlib/zu_lib.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* pragma pack() prevents compiler from relying on 16-byte alignment */
#pragma pack(push, 8)
typedef struct {
  uint64_t	length; // in words
  uint64_t	data[1];
} zu_bitmap;
#pragma pack(pop)

/* allocate/free zu_bitmap */
typedef void *(*zu_bitmap_alloc_fn)(unsigned size); // size in bytes
typedef void (*zu_bitmap_free_fn)(void *);

/* register allocate/free functions */
ZuExtern void zu_bitmap_init(
  zu_bitmap_alloc_fn,
  zu_bitmap_free_fn);

/* new, delete */
ZuExtern zu_bitmap *zu_bitmap_new(unsigned n);
ZuExtern zu_bitmap *zu_bitmap_new_fill(unsigned n);
ZuExtern void zu_bitmap_delete(zu_bitmap *v);

/* copy */
ZuExtern zu_bitmap *zu_bitmap_copy(const zu_bitmap *p);

/* get length (in bits) */
ZuExtern unsigned zu_bitmap_get_length(const zu_bitmap *v);

/* parse string, returns #bytes scanned, 0 on invalid input */
ZuExtern unsigned zu_bitmap_in(zu_bitmap **v, const char *s);
/* returns output length including null terminator */
ZuExtern unsigned zu_bitmap_out_len(const zu_bitmap *v);
/* output to string, returns end pointer to null terminator */
ZuExtern char *zu_bitmap_out(char *s, unsigned n, const zu_bitmap *v);

/* get underlying array */
ZuExtern unsigned zu_bitmap_get_wlength(const zu_bitmap *v);
ZuExtern uint64_t zu_bitmap_get_word(const zu_bitmap *v, unsigned i);

/* set underlying array */
ZuExtern zu_bitmap *zu_bitmap_set_wlength(zu_bitmap *v, unsigned n);
ZuExtern void zu_bitmap_set_word(zu_bitmap *v, unsigned i, uint64_t w);

/* 3-way comparison */
ZuExtern int zu_bitmap_cmp(const zu_bitmap *l, const zu_bitmap *r);

/* hash */
ZuExtern uint32_t zu_bitmap_hash(const zu_bitmap *v);

/* basic single-bit functions (get, set, clr) */
ZuExtern bool zu_bitmap_get(const zu_bitmap *v, unsigned i);
ZuExtern void zu_bitmap_set(zu_bitmap *v, unsigned i);
ZuExtern void zu_bitmap_clr(zu_bitmap *v, unsigned i);

/* first, last, next, prev */
ZuExtern unsigned zu_bitmap_first(const zu_bitmap *v);
ZuExtern unsigned zu_bitmap_last(const zu_bitmap *v);
ZuExtern unsigned zu_bitmap_next(const zu_bitmap *v, unsigned i);
ZuExtern unsigned zu_bitmap_prev(const zu_bitmap *v, unsigned i);

/* zero, fill, flip */
ZuExtern zu_bitmap *zu_bitmap_zero(zu_bitmap *v);
ZuExtern zu_bitmap *zu_bitmap_fill(zu_bitmap *v);
ZuExtern zu_bitmap *zu_bitmap_flip(zu_bitmap *v);

/* or, and, xor */
ZuExtern zu_bitmap *zu_bitmap_or(zu_bitmap *v, const zu_bitmap *p);
ZuExtern zu_bitmap *zu_bitmap_and(zu_bitmap *v, const zu_bitmap *p);
ZuExtern zu_bitmap *zu_bitmap_xor(zu_bitmap *v, const zu_bitmap *p);

#ifdef __cplusplus
}
#endif

#endif /* zu_bitmap_H */
