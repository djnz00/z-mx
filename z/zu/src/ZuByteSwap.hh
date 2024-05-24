//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// byte swap utility class

// Example:
// using UInt32N = typename ZuBigEndian<ZuBox0(uint32_t)>;
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

#ifndef ZuByteSwap_HH
#define ZuByteSwap_HH

#include <stddef.h>

#include <zlib/ZuTraits.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuInt.hh>
#include <zlib/ZuIntrin.hh>

template <typename T, class Cmp> class ZuBox;
template <typename T_> struct ZuByteSwap_Unbox {
  using T = T_;
};
template <typename T_, class Cmp> struct ZuByteSwap_Unbox<ZuBox<T_, Cmp>> {
  using T = T_;
};

template <unsigned> struct ZuByteSwap_UInt;
template <> struct ZuByteSwap_UInt<2> { using T = uint16_t; };
template <> struct ZuByteSwap_UInt<4> { using T = uint32_t; };
template <> struct ZuByteSwap_UInt<8> { using T = uint64_t; };
template <> struct ZuByteSwap_UInt<16> { using T = uint128_t; };

#pragma pack(push, 1)

template <typename T_> class ZuByteSwap {
public:
  using T = T_;
  using U = typename ZuByteSwap_Unbox<T>::T;
  using I = typename ZuByteSwap_UInt<sizeof(T)>::T;

  ZuByteSwap() { m_i = 0; }
  ZuByteSwap(const ZuByteSwap &i) { m_i = i.m_i; }
  ZuByteSwap &operator =(const ZuByteSwap &i) {
    if (this != &i) m_i = i.m_i;
    return *this;
  }

  template <typename R>
  ZuByteSwap(const R &r) { set(r); }
  template <typename R>
  ZuByteSwap &operator =(const R &r) { set(r); return *this; }

  operator U() const { return get<U>(); }

  ZuByteSwap operator -() { return ZuByteSwap(-get<U>()); }

  template <typename P> ZuByteSwap operator +(const P &p) const {
    return ZuByteSwap(get<U>() + p);
  }
  template <typename P> ZuByteSwap operator -(const P &p) const {
    return ZuByteSwap(get<U>() - p);
  }
  template <typename P> ZuByteSwap operator *(const P &p) const {
    return ZuByteSwap(get<U>() * p);
  }
  template <typename P> ZuByteSwap operator /(const P &p) const {
    return ZuByteSwap(get<U>() / p);
  }
  template <typename P> ZuByteSwap operator %(const P &p) const {
    return ZuByteSwap(get<U>() % p);
  }
  template <typename P> ZuByteSwap operator |(const P &p) const {
    return ZuByteSwap(get<U>() | p);
  }
  template <typename P> ZuByteSwap operator &(const P &p) const {
    return ZuByteSwap(get<U>() & p);
  }
  template <typename P> ZuByteSwap operator ^(const P &p) const {
    return ZuByteSwap(get<U>() ^ p);
  }

  ZuByteSwap operator ++(int) {
    ZuByteSwap o = *this;
    set(get<U>() + 1);
    return o;
  }
  ZuByteSwap &operator ++() {
    set(get<U>() + 1);
    return *this;
  }
  ZuByteSwap operator --(int) {
    ZuByteSwap o = *this;
    set(get<U>() - 1);
    return o;
  }
  ZuByteSwap &operator --() {
    set(get<U>() - 1);
    return *this;
  }

  template <typename P> ZuByteSwap &operator +=(const P &p) {
    set(get<U>() + p);
    return *this;
  }
  template <typename P> ZuByteSwap &operator -=(const P &p) {
    set(get<U>() - p);
    return *this;
  }
  template <typename P> ZuByteSwap &operator *=(const P &p) {
    set(get<U>() * p);
    return *this;
  }
  template <typename P> ZuByteSwap &operator /=(const P &p) {
    set(get<U>() / p);
    return *this;
  }
  template <typename P> ZuByteSwap &operator %=(const P &p) {
    set(get<U>() % p);
    return *this;
  }
  template <typename P> ZuByteSwap &operator |=(const P &p) {
    set(get<U>() | p);
    return *this;
  }
  template <typename P> ZuByteSwap &operator &=(const P &p) {
    set(get<U>() & p);
    return *this;
  }
  template <typename P> ZuByteSwap &operator ^=(const P &p) {
    set(get<U>() ^ p);
    return *this;
  }

private:
  // P is exactly ZuByteSwap<T>
  template <typename P>
  ZuExact<P, ZuByteSwap> set(const P &p) { m_i = p.m_i; }
  template <typename P>
  ZuExact<P, ZuByteSwap, const ZuByteSwap &> get() const { return *this; }

  // P is exactly U or T
  template <typename P>
  ZuIfT<bool(ZuIsExact<P, T>{}) || bool(ZuIsExact<P, U>{})> set(const P &p) {
    I i = 0;
    *reinterpret_cast<U *>(&i) = p;
    m_i = ZuIntrin::bswap(i);
  }
  template <typename P>
  ZuIfT<bool(ZuIsExact<P, T>{}) || bool(ZuIsExact<P, U>{}), P> get() const {
    I i = ZuIntrin::bswap(m_i);
    return *reinterpret_cast<const U *>(&i);
  }

  // P is integral (but not the same)
  template <typename P>
  ZuIfT<
      !ZuIsExact<P, ZuByteSwap>{} &&
      !ZuIsExact<P, T>{} && !ZuIsExact<P, U>{} &&
      ZuTraits<P>::IsIntegral>
  set(P p) {
    m_i = ZuIntrin::bswap(I(p));
  }
  template <typename P>
  ZuIfT<
      !ZuIsExact<P, ZuByteSwap>{} &&
      !ZuIsExact<P, T>{} && !ZuIsExact<P, U>{} &&
      ZuTraits<P>::IsIntegral>
  get() const {
    return ZuIntrin::bswap(m_i);
  }

  // P is non-integral and converts (but is not the same as U)
  template <typename P>
  ZuIfT<
      !ZuIsExact<P, ZuByteSwap>{} &&
      !ZuIsExact<P, T>{} && !ZuIsExact<P, U>{} &&
      !ZuTraits<P>::IsIntegral &&
      ZuInspect<P, U>::Converts>
  set(P p) {
    I i = 0;
    *reinterpret_cast<U *>(&i) = p;
    m_i = ZuIntrin::bswap(i);
  }
  template <typename P>
  ZuIfT<
      !ZuIsExact<P, ZuByteSwap>{} &&
      !ZuIsExact<P, T>{} && !ZuIsExact<P, U>{} &&
      !ZuTraits<P>::IsIntegral &&
      ZuInspect<U, P>::Converts, P>
  get() const {
    I i = ZuIntrin::bswap(m_i);
    return *reinterpret_cast<const U *>(&i);
  }

  // traits
  struct Traits : public ZuTraits<I> { enum { IsPrimitive = 0 }; };
  friend Traits ZuTraitsType(ZuByteSwap *);

private:
  I	m_i;
};

#pragma pack(pop)

#if Zu_BIGENDIAN
template <typename T> using ZuBigEndian = T;
template <typename T> using ZuLittleEndian = ZuByteSwap<T>;
#else
template <typename T> using ZuBigEndian = ZuByteSwap<T>;
template <typename T> using ZuLittleEndian = T;
#endif

#endif /* ZuByteSwap_HH */
