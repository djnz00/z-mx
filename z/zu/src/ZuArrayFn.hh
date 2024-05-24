//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// fast generic array operations

// ZuArrayFn<T> provides fast array operations used
// by many other templates and classes

#ifndef ZuArrayFn_HH
#define ZuArrayFn_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include <zlib/ZuAssert.hh>
#include <zlib/ZuTraits.hh>
#include <zlib/ZuNull.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuHash.hh>
#include <zlib/ZuInspect.hh>

#ifdef _MSC_VER
#pragma warning(push)
// #pragma warning(disable:4800)
#endif

// array initialization / destruction / move / copy operations

template <typename T, class Cmp, bool IsPrimitive> class ZuArrayFn_ItemOps_;

template <typename T, class Cmp> class ZuArrayFn_ItemOps_<T, Cmp, false> {
public:
  ZuInline static void initItem(void *dst) { new (dst) T{}; }
  template <typename P>
  ZuInline static void initItem(void *dst, P &&p) {
    new (dst) T{ZuFwd<P>(p)};
  }
  ZuInline static void destroyItem(T *dst) { (*dst).~T(); }
};

template <typename T, class Cmp> class ZuArrayFn_ItemOps_<T, Cmp, true> {
public:
  ZuInline static void initItem(void *dst) {
    *static_cast<T *>(dst) = Cmp::null();
  }
  template <typename P>
  ZuInline static void initItem(void *dst, P &&p) {
    *static_cast<T *>(dst) = ZuFwd<P>(p);
  }
  ZuInline static void destroyItem(T *dst) { }
};

template <typename T, class Cmp> class ZuArrayFn_ItemOps :
    public ZuArrayFn_ItemOps_<T, Cmp, ZuTraits<T>::IsPrimitive> {
  using Base = ZuArrayFn_ItemOps_<T, Cmp, ZuTraits<T>::IsPrimitive>;
public:
  static void initItems(T *dst, unsigned length) {
    if (ZuLikely(length))
      do { Base::initItem((void *)dst++); } while (--length > 0);
  }
};

template <typename T, class Cmp, bool IsPOD>
class ZuArrayFn_Ops : public ZuArrayFn_ItemOps<T, Cmp> {
public:
  static void destroyItems(T *dst, unsigned length) {
    if (ZuLikely(length))
      do { (*dst++).~T(); } while (--length > 0);
  }

  template <typename S>
  static void copyItems(T *dst, const S *src, unsigned length) {
    ptrdiff_t diff = 
      reinterpret_cast<const char *>(dst) -
      reinterpret_cast<const char *>(src);
    if (ZuUnlikely(!length || !diff)) return;
    if (diff < 0 || diff > static_cast<ptrdiff_t>(length * sizeof(T))) {
      do {
	new (dst++) T{*src++};
      } while (--length > 0);
    } else {
      dst += length;
      src += length;
      do {
	new (--dst) T{*--src};
      } while (--length > 0);
    }
  }

  template <typename S>
  static void moveItems(T *dst, S *src, unsigned length) {
    ptrdiff_t diff = 
      reinterpret_cast<const char *>(dst) -
      reinterpret_cast<const char *>(src);
    if (ZuUnlikely(!length || !diff)) return;
    if (diff < 0 || diff > static_cast<ptrdiff_t>(length * sizeof(T))) {
      do {
	new (dst++) T{ZuMv(*src)};
	(*src++).~T();
      } while (--length > 0);
    } else {
      dst += length;
      src += length;
      do {
	new (--dst) T{ZuMv(*--src)};
	(*src).~T();
      } while (--length > 0);
    }
  }
};

template <typename T1, typename T2>
struct ZuArrayFn_POD {
  enum { Same = ZuInspect<T1, T2>::Same ||
    (sizeof(T1) == sizeof(T2) &&
     ZuTraits<T1>::IsIntegral && ZuTraits<T1>::IsPrimitive &&
     ZuTraits<T2>::IsIntegral && ZuTraits<T2>::IsPrimitive) };
};
template <typename T1, typename T2, typename R = void>
using ZuArrayFn_SamePOD = ZuIfT<ZuArrayFn_POD<T1, T2>::Same, R>;
template <typename T1, typename T2, typename R = void>
using ZuArrayFn_NotSamePOD = ZuIfT<!ZuArrayFn_POD<T1, T2>::Same, R>;

template <typename T, class Cmp>
class ZuArrayFn_Ops<T, Cmp, true> : public ZuArrayFn_ItemOps<T, Cmp> {
public:
  ZuInline static void destroyItems(T *dst, unsigned length) { }
  template <typename S>
  static ZuArrayFn_NotSamePOD<T, S> copyItems(
      T *dst, const S *src, unsigned length) {
    if (ZuUnlikely(!length || dst == static_cast<const T *>(src))) return;
    if (static_cast<T *>(src) > dst ||
	length < static_cast<unsigned>(dst - static_cast<const T *>(src))) {
      do {
	new (dst++) T{*src++};
      } while (--length > 0);
    } else {
      dst += length;
      src += length;
      do {
	new (--dst) T{*--src};
      } while (--length > 0);
    }
  }
  template <typename S>
  ZuInline static ZuArrayFn_SamePOD<T, S> copyItems(
      T *dst, const S *src, unsigned length) {
    memmove((void *)dst, src, length * sizeof(T));
  }
  template <typename S>
  static ZuArrayFn_NotSamePOD<T, S> moveItems(
      T *dst, const S *src, unsigned length) {
    if (ZuUnlikely(!length || dst == static_cast<const T *>(src))) return;
    if (static_cast<T *>(src) > dst ||
	length < static_cast<unsigned>(dst - static_cast<const T *>(src))) {
      do {
	new (dst++) T{ZuMv(*src++)};
      } while (--length > 0);
    } else {
      dst += length;
      src += length;
      do {
	new (--dst) T{ZuMv(*--src)};
      } while (--length > 0);
    }
  }
  template <typename S>
  ZuInline static ZuArrayFn_SamePOD<T, S> moveItems(
      T *dst, const S *src, unsigned length) {
    memmove((void *)dst, src, length * sizeof(T));
  }
};

// array comparison

template <typename T, class Cmp, class DefltCmp, bool IsIntegral, unsigned Size>
struct ZuArrayFn_Cmp {
  static int cmp(const T *dst, const T *src, unsigned length) {
    while (ZuLikely(length--))
      if (int i = Cmp::cmp(*dst++, *src++)) return i;
    return 0;
  }
  static bool equals(const T *dst, const T *src, unsigned length) {
    while (ZuLikely(length--))
      if (!Cmp::equals(*dst++, *src++)) return false;
    return true;
  }
};

template <typename T, class Cmp> struct ZuArrayFn_Cmp<T, Cmp, Cmp, true, 1> {
  ZuInline static int cmp(const T *dst, const T *src, unsigned length) {
    return memcmp(dst, src, length * sizeof(T));
  }
  ZuInline static bool equals(const T *dst, const T *src, unsigned length) {
    return !memcmp(dst, src, length * sizeof(T));
  }
};

template <typename T> struct ZuArrayFn_Hash {
  static uint32_t hash(const T *data, unsigned length) {
    ZuHash_FNV::Value v = ZuHash_FNV::initial_();
    if (ZuLikely(length))
      do {
	v = ZuHash_FNV::hash_(v, ZuHash<T>::hash(*data++));
      } while (--length > 0);
    return (uint32_t)v;
  }
};
template <typename T> struct ZuArrayFn_StringHash {
  ZuInline static uint32_t hash(const T *data, unsigned length) {
    return ZuStringHash<T>::hash(data, length);
  }
};
template <>
struct ZuArrayFn_Hash<char> : public ZuArrayFn_StringHash<char> { };
template <>
struct ZuArrayFn_Hash<wchar_t> : public ZuArrayFn_StringHash<wchar_t> { };

// main template

template <typename T, class Cmp = ZuCmp<T> > class ZuArrayFn :
  public ZuArrayFn_Ops<T, Cmp, ZuTraits<T>::IsPOD>,
  public ZuArrayFn_Cmp<T, Cmp, ZuCmp<T>, ZuTraits<T>::IsIntegral, sizeof(T)>,
  public ZuArrayFn_Hash<T> { };

template <typename T> class ZuArrayFn_Null {
public:
  ZuInline static void initItem(T *dst) { }
  template <typename P> ZuInline static void initItem(T *dst, P &&p) { }
  ZuInline static void destroyItem(T *dst) { }
  ZuInline static void initItems(T *dst, unsigned length) { }
  ZuInline static void destroyItems(T *dst, unsigned length) { }
  template <typename S>
  ZuInline static void copyItems(T *dst, const S *src, unsigned length) { }
  ZuInline static void moveItems(T *dst, const T *src, unsigned length) { }
  ZuInline static int cmp(const T *dst, const T *src, unsigned length)
    { return 0; }
  ZuInline static bool equals(const T *dst, const T *src, unsigned length)
    { return true; }
  ZuInline static uint32_t hash(const T *data, unsigned length)
    { return 0; }
};

template <typename Cmp>
class ZuArrayFn<ZuNull, Cmp> : public ZuArrayFn_Null<ZuNull> { };
template <typename Cmp>
class ZuArrayFn<void, Cmp> : public ZuArrayFn_Null<void> { };

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZuArrayFn_HH */
