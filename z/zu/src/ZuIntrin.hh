//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Z library intrinsics handling

#ifndef ZuIntrin_HH
#define ZuIntrin_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuInt.hh>

// --- clz (32 and 64 bit only)

// first choice: gcc/clang intrinsics
#ifdef __GNUC__
#define Zu_clz32(v) __builtin_clz(v)
#define Zu_clz64(v) __builtin_clzll(v)
#endif

// second choice: Hacker's Delight C code
ZuInline uint32_t Zu_popcnt(uint32_t v) { // re-used by ctz below
  v -= ((v >> 1) & 0x55555555);
  v = (((v >> 2) & 0x33333333) + (v & 0x33333333));
  v = (((v >> 4) + v) & 0x0f0f0f0f);
  v += (v >> 8);
  v += (v >> 16);
  return v & 0x3f;
}

#ifndef Zu_clz32
ZuInline uint32_t Zu_clz32_(uint32_t v) {
  v |= (v >> 1);
  v |= (v >> 2);
  v |= (v >> 4);
  v |= (v >> 8);
  v |= (v >> 16);
  return 32 - Zu_popcnt(v);
}
#define Zu_clz32(v) Zu_clz32_(v)
#endif
#ifndef Zu_clz64
ZuInline uint64_t Zu_clz64_(uint64_t v) {
  return (v>>32) ? Zu_clz32(v>>32) : 32 + Zu_clz32(v);
}
#define Zu_clz64(v) Zu_clz64_(v)
#endif

// --- ctz (32 and 64 bit only)

// first choice: gcc/clang intrinsics
#ifdef __GNUC__
#define Zu_ctz32(v) __builtin_ctz(v)
#define Zu_ctz64(v) __builtin_ctzll(v)
#endif

// second choice: Hacker's Delight C code
#ifndef Zu_ctz32
ZuInline uint32_t Zu_ctz32_(uint32_t v) {
  return Zu_popcnt((v & -v) - 1);
}
#define Zu_ctz32(v) Zu_ctz32_(v)
#endif
#ifndef Zu_ctz64
ZuInline uint64_t Zu_ctz64_(uint64_t v) {
  return (v<<32) ? Zu_ctz32(v) : 32 + Zu_ctz32(v>>32);
}
#define Zu_ctz64(v) Zu_ctz64_(v)
#endif

// -- bswap (16, 32, 64 and 128 bit)

// first choice: gcc/clang intrinsic
#ifdef __GNUC__
#define Zu_bswap16(x) __builtin_bswap16(x)
#define Zu_bswap32(x) __builtin_bswap32(x)
#define Zu_bswap64(x) __builtin_bswap64(x)
#ifndef __llvm__
#define Zu_bswap128(x) __builtin_bswap128(x)
#endif
#endif

// second choice: C code
#ifndef Zu_bswap16
ZuInline uint16_t Zu_bswap16_(uint16_t v) {
  return (v<<8) | (v>>8);
}
#define Zu_bswap16(v) Zu_bswap16_(v)
#endif
#ifndef Zu_bswap32
ZuInline uint32_t Zu_bswap32_(uint32_t v) {
  return
    ((uint32_t)(Zu_bswap16(v))<<16) |
    (uint32_t)Zu_bswap16(v>>16);
}
#define Zu_bswap32(v) Zu_bswap32_(v)
#endif
#ifndef Zu_bswap64
ZuInline uint64_t Zu_bswap64_(uint64_t v) {
  return
    ((uint64_t)(Zu_bswap32(v))<<32) |
    (uint64_t)Zu_bswap32(v>>32);
}
#define Zu_bswap64(v) Zu_bswap64_(v)
#endif
#ifndef Zu_bswap128
ZuInline uint128_t Zu_bswap128_(uint128_t v) {
  return
    ((uint128_t)(Zu_bswap64(v))<<64) |
    (uint128_t)Zu_bswap64(v>>64);
}
#define Zu_bswap128(v) Zu_bswap128_(v)
#endif

#ifdef __GNUC__
#define Zu_add(l, r, o) __builtin_add_overflow(l, r, o)
#define Zu_sub(l, r, o) __builtin_sub_overflow(l, r, o)
#define Zu_mul(l, r, o) __builtin_mul_overflow(l, r, o)
#endif

#ifndef Zu_add
#error "Broken platform - need integer overflow intrinsics"
#endif

// due to MSVC's continuing lack of 128bit type support, no
// attempt is made to support MSVC here; MSVC did finally add
// full-spectrum integer overflow intrinsics in 2023 -
// see unused reference code in msvc_intrin.cc

#ifdef __GNUC__
#define Zu_nanf() __builtin_nanf("0")
#define Zu_nan() __builtin_nan("0")
#define Zu_nanl() __builtin_nanl("0")
#endif

#ifndef Zu_nan
#error "Broken platform - need NaN generators"
#endif

namespace ZuIntrin {

// clz
template <typename T, ZuIfT<sizeof(T) == 4, int> = 0>
ZuInline unsigned clz(T v) { return Zu_clz32(v); }
template <typename T, ZuIfT<sizeof(T) == 8, int> = 0>
ZuInline unsigned clz(T v) { return Zu_clz64(v); }

// ctz
template <typename T, ZuIfT<sizeof(T) == 4, int> = 0>
ZuInline unsigned ctz(T v) { return Zu_ctz32(v); }
template <typename T, ZuIfT<sizeof(T) == 8, int> = 0>
ZuInline unsigned ctz(T v) { return Zu_ctz64(v); }

// bswap
template <typename T, ZuIfT<sizeof(T) == 2, int> = 0>
ZuInline T bswap(T v) { return T(Zu_bswap16(v)); }
template <typename T, ZuIfT<sizeof(T) == 4, int> = 0>
ZuInline T bswap(T v) { return T(Zu_bswap32(v)); }
template <typename T, ZuIfT<sizeof(T) == 8, int> = 0>
ZuInline T bswap(T v) { return T(Zu_bswap64(v)); }
template <typename T, ZuIfT<sizeof(T) == 16, int> = 0>
ZuInline T bswap(T v) { return T(Zu_bswap128(v)); }

// integer overflow
template <typename T>
ZuInline bool add(T l, T r, T *o) { return Zu_add(l, r, o); }
template <typename T>
ZuInline bool sub(T l, T r, T *o) { return Zu_sub(l, r, o); }
template <typename T>
ZuInline bool mul(T l, T r, T *o) { return Zu_mul(l, r, o); }

// NaN generators
template <typename T, ZuExact<float, T, int> = 0>
ZuInline T nan() { return Zu_nanf(); }
template <typename T, ZuExact<double, T, int> = 0>
ZuInline T nan() { return Zu_nan(); }
template <typename T, ZuExact<long double, T, int> = 0>
ZuInline T nan() { return Zu_nanl(); }

} // ZuIntrin

#endif /* ZuIntrin_HH */
