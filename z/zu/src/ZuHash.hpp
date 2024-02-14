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

// generic hashing

// UDTs can implement a "uint32_t hash() const" public member function

// WARNING - all these functions must return consistent hash codes for the
// same values regardless of the enclosing type
//
// unsigned long i;
// signed char j;
// i == j implies ZuHash<unsigned long>::hash(i) == ZuHash<signed char>::hash(j)
//
// const char *s;
// ZtString t;
// s == t implies ZuHash<const char *>::hash(s) == ZuHash<ZtString>::hash(t)

#ifndef ZuHash_HPP
#define ZuHash_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <math.h>

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuInt.hpp>

template <typename T, bool = ZuTraits<T>::IsString> struct ZuHash_;

// deliberately undefined template

template <typename T> struct ZuHash_Cannot;

// golden prime function, specialized for 32bit and 64bit types

struct ZuHash_GoldenPrime32 {
  static uint32_t hash(uint32_t i) { return i * m_goldenPrime; }

private:
  static const uint32_t m_goldenPrime = 0x9e370001UL;
    // 2^31 + 2^29 - 2^25 + 2^22 - 2^19 - 2^16 + 1
};

struct ZuHash_GoldenPrime64 {
  static uint64_t hash(uint64_t i) {
    uint64_t n = i; // most compilers don't optimize a 64bit constant multiply

    n <<= 18; i -= n; n <<= 33; i -= n; n <<= 3; i += n;
    n <<= 3;  i -= n; n <<= 4;	i += n; n <<= 2; i += n;

    return i;
  }

private:
  static const uint64_t m_goldenPrime = 0x9e37fffffffc0001ULL;
    // 2^63 + 2^61 - 2^57 + 2^54 - 2^51 - 2^18 + 1
};

struct ZuHash_GoldenPrime128 {
  static uint128_t hash(uint128_t i) {
    uint128_t n = i;

    n <<= 18; i -= n; n <<= 97; i -= n; n <<= 3; i += n;
    n <<= 3;  i -= n; n <<= 4;	i += n; n <<= 2; i += n;

    return i;
  }
};

// Fowler / Noll / Vo (FNV) hash function (type FNV-1a)

#if defined(__x86_64__) || defined(_WIN64)
struct ZuHash_FNV_ {
  using Value = uint64_t;

  static Value initial_() { return 0xcbf29ce484222325ULL; }

  static Value hash_(Value v, Value i) {
    v ^= i;
    v *= 0x100000001b3ULL;
    return v;
  }
};
#else
struct ZuHash_FNV_ {
  using Value = uint32_t;

  static Value initial_() { return 0x811c9dc5UL; }

  static Value hash_(Value v, Value i) {
    v ^= i;
    v *= 0x1000193UL;
    return v;
  }
};
#endif

struct ZuHash_FNV : public ZuHash_FNV_ {
  using Value = ZuHash_FNV_::Value;

  static uint32_t hash(const unsigned char *p, int n) {
    Value v = initial_();
    while (--n >= 0) v = hash_(v, *p++);
    return (uint32_t)v;
  }
};

// hashing of floats, doubles and long doubles

template <typename T> struct ZuHash_Floating {
  static uint32_t hash(T t) {
    return ZuHash_FNV::hash((const unsigned char *)&t, sizeof(T));
  }
};

// hashing of integral types

template <typename T, int Size> struct ZuHash_Integral {
  static uint32_t hash(const T &t) {
    return ZuHash_GoldenPrime32::hash((uint32_t)t);
  }
};

template <typename T> struct ZuHash_Integral<T, 8> {
  static uint32_t hash(const T &t) {
    return (uint32_t)ZuHash_GoldenPrime64::hash(t);
  }
};

template <typename T> struct ZuHash_Integral<T, 16> {
  static uint32_t hash(const T &t) {
    return (uint32_t)ZuHash_GoldenPrime128::hash(t);
  }
};

// hashing of real types

template <typename T, bool IsIntegral> struct ZuHash_Real :
  public ZuHash_Integral<T, sizeof(T)> { };

template <typename T> struct ZuHash_Real<T, false> :
  public ZuHash_Floating<T> { };

// hashing of primitive types

template <typename T, bool IsReal, bool IsVoid>
struct ZuHash_Primitive : public ZuHash_Cannot<T> { };

template <typename T> struct ZuHash_Primitive<T, true, false> :
  public ZuHash_Real<T, ZuTraits<T>::IsIntegral> { };

// hashing of non-primitive types

template <typename> struct ZuHash_Can_;
template <> struct ZuHash_Can_<uint32_t> { using T = void; }; 
template <typename, typename = void> struct ZuHash_Can : public ZuFalse { };
template <typename U>
struct ZuHash_Can<U,
  typename ZuHash_Can_<decltype(ZuDeclVal<const U &>().hash())>::T> :
  public ZuTrue { };
template <typename T, bool Fwd, bool = ZuHash_Can<T>{}>
struct ZuHash_NonPrimitive : public ZuHash_Cannot<T> { };

template <typename P, bool IsPrimitive> struct ZuHash_NonPrimitive___ :
    public ZuHash_NonPrimitive<P, false> { };
template <typename P>
struct ZuHash_NonPrimitive___<P, true> : public ZuHash_<P> { };

template <typename T, typename P> struct ZuHash_NonPrimitive__ :
    public ZuHash_NonPrimitive___<P, ZuTraits<P>::IsPrimitive> { };
template <typename T> struct ZuHash_NonPrimitive__<T, T> {
  static uint32_t hash(const T &v) { return v.hash(); }
};
template <typename T, typename P, bool Fwd>
struct ZuHash_NonPrimitive_ : public ZuHash_NonPrimitive__<T, P> { };
template <typename T, typename P> struct ZuHash_NonPrimitive_<T, P, false> {
  static uint32_t hash(const T &v) { return v.hash(); }
};
template <typename T, bool Fwd> struct ZuHash_NonPrimitive<T, Fwd, true> {
  template <typename P>
  static uint32_t hash(const P &p) {
    return ZuHash_NonPrimitive_<T, P, Fwd>::hash(p);
  }
};

// hashing of pointers

template <typename T, int Size>
struct ZuHash_Pointer : public ZuHash_Cannot<T> { };

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4311)
#endif
template <typename T> struct ZuHash_Pointer<T, 4> {
  static uint32_t hash(T t) {
    return ZuHash_GoldenPrime32::hash((uint32_t)t);
  }
};
#ifdef _MSC_VER
#pragma warning(pop)
#endif

template <typename T> struct ZuHash_Pointer<T, 8> {
  static uint32_t hash(T t) {
    return (uint32_t)ZuHash_GoldenPrime64::hash((uint64_t)t);
  }
};

// pointer hashing

template <typename T> struct ZuHash_PrimitivePointer;

template <typename T> struct ZuHash_PrimitivePointer<T *> :
  public ZuHash_Pointer<const T *, sizeof(T *)> { };

// non-string hashing

template <typename T, bool IsPrimitive, bool IsPointer> struct ZuHash_NonString;

template <typename T> struct ZuHash_NonString<T, false, false> :
  public ZuHash_NonPrimitive<T, ZuHash_Can<T>{}, true> { };

template <typename T> struct ZuHash_NonString<T, false, true> :
  public ZuHash_Pointer<T, sizeof(T)> { };

template <typename T> struct ZuHash_NonString<T, true, false> :
  public ZuHash_Primitive<T, ZuTraits<T>::IsReal, ZuTraits<T>::IsVoid> { };

template <typename T> struct ZuHash_NonString<T, true, true> :
  public ZuHash_PrimitivePointer<T> { };

// string hashing

// Paul Hsieh's hash, better than FNV if length is known
// adapted from http://www.azillionmonkeys.com/qed/hash.html

#if (defined(__GNUC__) && (defined(__i386__) || defined(__x86_64__))) || \
    defined(_WIN32)
#define ZuStringHash_Misaligned16BitLoadOK
#endif

template <typename T> struct ZuStringHash;
template <> struct ZuStringHash<char> {
  static uint32_t hash(const char *data, size_t len) {
    uint32_t hash = (uint32_t)len;

    if (len <= 0 || !data) return 0;

    // main loop
    while (len>>2) {
#ifdef ZuStringHash_Misaligned16BitLoadOK
      hash += *(uint16_t *)data;
      hash = (hash<<16) ^ (*((uint16_t *)data + 1)<<11) ^ hash;
#else
      hash += *(uint8_t *)data + (*((uint8_t *)data + 1)<<8);
      hash = (hash<<16) ^
	((*((uint8_t *)data + 2) + (*((uint8_t *)data + 3)<<8))<<11) ^ hash;
#endif
      hash += hash>>11;
      data += 4, len -= 4;
    }

    // handle end cases
    switch (len & 3) {
      case 3:
#ifdef ZuStringHash_Misaligned16BitLoadOK
	hash += *(uint16_t *)data;
#else
	hash += *(uint8_t *)data + (*((uint8_t *)data + 1)<<8);
#endif
	hash ^= hash<<16;
	hash ^= *((uint8_t *)data + 2)<<18;
	hash += hash>>11;
	break;
      case 2:
#ifdef ZuStringHash_Misaligned16BitLoadOK
	hash += *(uint16_t *)data;
#else
	hash += *(uint8_t *)data + (*((uint8_t *)data + 1)<<8);
#endif
	hash ^= hash<<11;
	hash += hash>>17;
	break;
      case 1:
	hash += *data;
	hash ^= hash<<10;
	hash += hash>>1;
    }

    // "avalanche" the final character
    hash ^= hash<<3;
    hash += hash>>5;
    hash ^= hash<<4;
    hash += hash>>17;
    hash ^= hash<<25;
    hash += hash>>6;

    return hash;
    // return ZuHash_GoldenPrime32::hash(hash);
  }
};
template <int WCharSize> struct ZuWStringHash;
template <> struct ZuWStringHash<2> {
  static uint32_t hash(const wchar_t *data, size_t len) {
    uint32_t hash = (uint32_t)len;

    if (len <= 0 || !data) return 0;

    // main loop
    while (len>>1) {
      hash += *(uint16_t *)data;
      hash = (hash<<16) ^ (*((uint16_t *)data + 1)<<11) ^ hash;
      hash += hash>>11;
      data += 2, len -= 2;
    }

    // handle end case
    if (len & 1) {
      hash += *(uint16_t *)data;
      hash ^= hash<<11;
      hash += hash>>17;
    }

    // force "avalanching" of final character
    hash ^= hash<<3;
    hash += hash>>5;
    hash ^= hash<<4;
    hash += hash>>17;
    hash ^= hash<<25;
    hash += hash>>6;

    return hash;
    // return ZuHash_GoldenPrime32::hash(hash);
  }
};
template <> struct ZuWStringHash<4> {
  static uint32_t hash(const wchar_t *data, size_t len) {
    uint32_t hash = (uint32_t)len;

    if (len <= 0 || !data) return 0;

    // main loop
    while (len) {
      hash += *(uint16_t *)data;
      hash = (hash<<16) ^ (*((uint16_t *)data + 1)<<11) ^ hash;
      hash += hash>>11;
      data++, len--;
    }

    // force "avalanching" of final character
    hash ^= hash<<3;
    hash += hash>>5;
    hash ^= hash<<4;
    hash += hash>>17;
    hash ^= hash<<25;
    hash += hash>>6;

    return hash;
  }
};
template <>
struct ZuStringHash<wchar_t> : public ZuWStringHash<sizeof(wchar_t)> { };

// generic hashing function

template <typename T>
struct ZuHash_<T, false> : public ZuHash_NonString<
  T, ZuTraits<T>::IsPrimitive, ZuTraits<T>::IsPointer> { };

template <typename T> struct ZuHash_<T, true> {
  template <typename S>
  static uint32_t hash(const S &s) {
    using Traits = ZuTraits<S>;
    using Char = ZuDecay<typename Traits::Elem>;
    return ZuStringHash<Char>::hash(Traits::data(s), Traits::length(s));
  }
};

// generic template

template <typename T> struct ZuHash : public ZuHash_<ZuDecay<T>> { };

#endif /* ZuHash_HPP */
