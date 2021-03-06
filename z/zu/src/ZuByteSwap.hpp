//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2

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

// byte swap utility class

// Example:
// typedef typename ZuBigEndian<ZuBox0(uint32_t)>::T UInt32N;
// #pragma pack(push, 1)
// struct Hdr {
//   ...
//   UInt32N	length;	// length in network byte order (bigendian)
//   ...
// };
// #pragma pack(pop)
// ...
// char buf[1472];
// ...
// char *ptr = buf;
// Hdr *hdr = (Hdr *)ptr;
// printf("%u\n", (uint32_t)hdr->length);
// ...
// ptr += sizeof(Hdr) + hdr->length;

#ifndef ZuByteSwap_HPP
#define ZuByteSwap_HPP

#ifdef _MSC_VER
#pragma once
#endif

#include <stddef.h>

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuConversion.hpp>

#include <zlib/ZuInt.hpp>

// use optimized compiler built-ins wherever possible
#if !defined(__mips64) && defined(__GNUC__)
// bswap16 only available in gcc 4.7 and later
// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=52624
#if (__GNUC__ > 3 && __GNUC_MINOR > 6)
#define Zu_bswap16(x) __builtin_bswap16(x)
#else
inline uint16_t Zu_bswap16(const uint16_t &i) {
  return
    (i << 8) | (i >> 8);
}
#endif
#define Zu_bswap32(x) __builtin_bswap32(x)
#define Zu_bswap64(x) __builtin_bswap64(x)
#elif defined(_MSC_VER) && !defined(_DEBUG)
#include <stdlib.h>
#define Zu_bswap16(x) _byteswap_ushort(x)
#define Zu_bswap32(x) _byteswap_ulong(x)
#define Zu_bswap64(x) _byteswap_uint64(x)
#else
inline uint16_t Zu_bswap16(const uint16_t &i) {
  return
    (i << 8) | (i >> 8);
}
inline uint32_t Zu_bswap32(const uint32_t &i) {
  return
    (i << 24) | ((i & 0xff00UL) << 8) |
    ((i >> 8) & 0xff00UL) | (i >> 24);
}
inline uint64_t Zu_bswap64(const uint64_t &i) {
  return
    (i << 56) | ((i & 0xff00ULL) << 40) |
    ((i & 0xff0000ULL) << 24) | ((i & 0xff000000ULL) << 8) |
    ((i >> 8) & 0xff000000ULL) | ((i >> 24) & 0xff0000ULL) |
    ((i >> 40) & 0xff00ULL) | (i >> 56);
}
#endif

template <typename T, class Cmp> class ZuBox;
template <typename T_> struct ZuByteSwap_Unbox {
  typedef T_ T;
};
template <typename T_, class Cmp> struct ZuByteSwap_Unbox<ZuBox<T_, Cmp> > {
  typedef T_ T;
};

#pragma pack(push, 1)

template <int N> struct ZuByteSwap_;
template <> struct ZuByteSwap_<1> {
  typedef char T;
  inline static T bswap(const T &i) { return i; }
};
template <> struct ZuByteSwap_<2> {
  typedef uint16_t T;
  inline static T bswap(const T &i) { return Zu_bswap16(i); }
};
template <> struct ZuByteSwap_<4> {
  typedef uint32_t T;
  inline static T bswap(const T &i) { return Zu_bswap32(i); }
};
template <> struct ZuByteSwap_<8> {
  typedef uint64_t T;
  inline static T bswap(const T &i) { return Zu_bswap64(i); }
};
template <typename T_> class ZuByteSwap : public ZuByteSwap_<sizeof(T_)> {
  typedef ZuByteSwap_<sizeof(T_)> B;

public:
  typedef T_ T;
  typedef typename ZuByteSwap_Unbox<T>::T U;
  typedef typename B::T I;

  inline ZuByteSwap() { m_i = 0; }
  inline ZuByteSwap(const ZuByteSwap &i) { m_i = i.m_i; }
  inline ZuByteSwap &operator =(const ZuByteSwap &i) {
    if (this != &i) m_i = i.m_i;
    return *this;
  }

  template <typename R>
  inline ZuByteSwap(const R &r) { set(r); }
  template <typename R>
  inline ZuByteSwap &operator =(const R &r) { set(r); return *this; }

  inline operator U() const { return get<U>(); }

  inline ZuByteSwap operator -() { return ZuByteSwap(-get<I>()); }

  template <typename R> inline ZuByteSwap operator +(const R &r) const
    { return ZuByteSwap(get<I>() + r); }
  template <typename R> inline ZuByteSwap operator -(const R &r) const
    { return ZuByteSwap(get<I>() - r); }
  template <typename R> inline ZuByteSwap operator *(const R &r) const
    { return ZuByteSwap(get<I>() * r); }
  template <typename R> inline ZuByteSwap operator /(const R &r) const
    { return ZuByteSwap(get<I>() / r); }
  template <typename R> inline ZuByteSwap operator %(const R &r) const
    { return ZuByteSwap(get<I>() % r); }
  template <typename R> inline ZuByteSwap operator |(const R &r) const
    { return ZuByteSwap(get<I>() | r); }
  template <typename R> inline ZuByteSwap operator &(const R &r) const
    { return ZuByteSwap(get<I>() & r); }
  template <typename R> inline ZuByteSwap operator ^(const R &r) const
    { return ZuByteSwap(get<I>() ^ r); }

  inline ZuByteSwap operator ++(int)
    { ZuByteSwap o = *this; set(get<I>() + 1); return o; }
  inline ZuByteSwap &operator ++()
    { set(get<I>() + 1); return *this; }
  inline ZuByteSwap operator --(int)
    { ZuByteSwap o = *this; set(get<I>() - 1); return o; }
  inline ZuByteSwap &operator --()
    { set(get<I>() - 1); return *this; }

  template <typename R> inline ZuByteSwap &operator +=(const R &r)
    { set(get<I>() + r); return *this; }
  template <typename R> inline ZuByteSwap &operator -=(const R &r)
    { set(get<I>() - r); return *this; }
  template <typename R> inline ZuByteSwap &operator *=(const R &r)
    { set(get<I>() * r); return *this; }
  template <typename R> inline ZuByteSwap &operator /=(const R &r)
    { set(get<I>() / r); return *this; }
  template <typename R> inline ZuByteSwap &operator %=(const R &r)
    { set(get<I>() % r); return *this; }
  template <typename R> inline ZuByteSwap &operator |=(const R &r)
    { set(get<I>() | r); return *this; }
  template <typename R> inline ZuByteSwap &operator &=(const R &r)
    { set(get<I>() & r); return *this; }
  template <typename R> inline ZuByteSwap &operator ^=(const R &r)
    { set(get<I>() ^ r); return *this; }

private:
  template <typename T>
  inline typename ZuIs<T, ZuByteSwap>::T set(const T &i) {
    m_i = i.m_i;
  }
  template <typename T>
  inline typename ZuIfT<
      !ZuConversion<T, ZuByteSwap>::Is &&
      ZuConversion<T, I>::Exists>::T set(const T &i) {
    m_i = B::bswap((I)i);
  }
  template <typename T>
  inline typename ZuIfT<
      !ZuConversion<T, ZuByteSwap>::Is &&
      !ZuConversion<T, I>::Exists &&
      sizeof(T) == sizeof(I)>::T set(const T &i) {
    const I *ZuMayAlias(i_) = (const I *)(const void *)&i;
    m_i = B::bswap(*i_);
  }
  template <typename T>
  inline typename ZuIs<T, ZuByteSwap, T>::T get() const {
    return *this;
  }
  template <typename T>
  inline typename ZuIfT<
      !ZuConversion<T, ZuByteSwap>::Is &&
      ZuConversion<T, I>::Exists, T>::T get() const {
    return (T)B::bswap(m_i);
  }
  template <typename T>
  inline typename ZuIfT<
      !ZuConversion<T, ZuByteSwap>::Is &&
      !ZuConversion<T, I>::Exists &&
      sizeof(T) == sizeof(I), T>::T get() const {
    I i = B::bswap(m_i);
    T *ZuMayAlias(i_) = (T *)(void *)&i;
    return *i_;
  }

  I	m_i;
};

template <typename I>
struct ZuTraits<ZuByteSwap<I> > : public ZuTraits<I> {
  typedef ZuByteSwap<I> T;
  enum { IsBoxed = 0, IsPrimitive = 0 };
};

#pragma pack(pop)

#if Zu_BIGENDIAN
template <typename T> using ZuBigEndian = T;
template <typename T> using ZuLittleEndian = ZuByteSwap<T>;
#else
template <typename T> using ZuBigEndian = ZuByteSwap<T>;
template <typename T> using ZuLittleEndian = T;
#endif

#endif /* ZuByteSwap_HPP */
