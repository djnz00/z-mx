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

// flatbuffers integration

#ifndef ZfbKVTree_HPP
#define ZfbKVTree_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZfbLib_HPP
#include <zlib/ZfbLib.hpp>
#endif

#include <zlib/Zfb.hpp>

#include <zlib/kvtree_fbs.h>

namespace Zfb {

template <typename SaveFn, typename PrintFn>
struct KVTreeGet {
  SaveFn	saveFn;
  PrintFn	printFn;

  template <typename S> void print(S &s) const { printFn(s); }

  friend ZuPrintFn ZuPrintType(KVTreeGet *);
};
template <typename SaveFn, typename PrintFn>
KVTreeGet(SaveFn saveFn, PrintFn printFn) -> KVTreeGet<SaveFn, PrintFn>;

namespace Save {
  template <typename B, typename ...Args>
  inline auto kvTree(B &b, Args &&... args) {
    using namespace ZfbTree;
    return CreateKVTree(b, ZuFwd<Args>(args)...);
  }

  // kvUInt8Vec(b, "bytes", bytes(b, "data", 4));
  template <typename B, typename ...Args>
  inline auto kvUInt8Vec(B &b, ZuString key, Args &&... args) {
    using namespace ZfbTree;
    return CreateKV(b, str(b, key), Value_UInt8Vec,
	CreateUInt8Vec(b, ZuFwd<Args>(args)...).Union());
  }

  // Example:
  // kvInt32Vec(b, "integers", 2, [](unsigned i) { return i; });
#define ZfbKVTree_Primitive(ctype, fbstype) \
  template <typename B> \
  inline auto kv##fbstype(B &b, ZuString key, ctype value) { \
    using namespace ZfbTree; \
    return CreateKV(b, str(b, key), Value_##fbstype, \
	b.CreateStruct(fbstype{value}).Union()); \
  } \
  template <typename B, typename L> \
  inline auto kv##fbstype##Vec(B &b, ZuString key, unsigned n, L l) { \
    using namespace ZfbTree; \
    return CreateKV(b, str(b, key), Value_##fbstype##Vec, \
	Create##fbstype##Vec(b, pvectorIter(b, n, l)).Union()); \
  }
  ZfbKVTree_Primitive(int, Int32)
  ZfbKVTree_Primitive(unsigned, UInt32)
  ZfbKVTree_Primitive(int64_t, Int64)
  ZfbKVTree_Primitive(uint64_t, UInt64)
  ZfbKVTree_Primitive(double, Double)

  template <typename B>
  inline auto kvString(B &b, ZuString key, ZuString value) {
    using namespace ZfbTree;
    return CreateKV(b, str(b, key), Value_String,
	str(b, value).Union());
  }
  // Example:
  // kvStringVec(b, "strings", 2,
  //   [](unsigned i) { return ZtString{} << "value" << i; });
  template <typename B, typename L>
  inline auto kvStringVec(B &b, ZuString key, unsigned n, L l) {
    using namespace ZfbTree;
    return CreateKV(b, str(b, key), Value_StringVec,
	CreateStringVec(b, strVecIter(b, n, l)).Union());
  }

  template <typename B>
  inline auto kvBitmap(B &b, ZuString key, const ZmBitmap &value) {
    using namespace ZfbTree;
    return CreateKV(b, str(b, key), Value_Bitmap,
	bitmap(b, value).Union());
  }
  // Example:
  // kvBitmapVec(b, "bitmaps", 2, [] <template B> (B &b, unsigned i) {
  //   return bitmap(b, ZmBitmap{static_cast<uint64_t>(i)});
  // }
  template <typename B, typename L>
  inline auto kvBitmapVec(B &b, ZuString key, unsigned n, L l) {
    using namespace ZfbTree;
    return CreateKV(b, str(b, key), Value_BitmapVec,
	CreateBitmapVec(b, vectorIter<Bitmap>(b, n, l)).Union());
  }

  template <typename B>
  inline auto kvDecimal(B &b, ZuString key, const ZuDecimal &value) {
    using namespace ZfbTree;
    return CreateKV(b, str(b, key), Value_Decimal,
	b.CreateStruct(decimal(value)).Union());
  }
  // Example:
  // kvDecimalVec(b, "decimals", 2, [] <template T> (T *ptr, unsigned i) {
  //   new (ptr) Decimal{decimal(ZuDecimal{i})};
  // }
  template <typename B, typename L>
  inline auto kvDecimalVec(B &b, ZuString key, unsigned n, L l) {
    using namespace ZfbTree;
    return CreateKV(b, str(b, key), Value_DecimalVec,
	CreateDecimalVec(b, structVecIter(b, n, l)).Union());
  }

  template <typename B>
  inline auto kvDateTime(B &b, ZuString key, const ZtDate &value) {
    using namespace ZfbTree;
    return CreateKV(b, str(b, key), Value_DateTime,
	b.CreateStruct(dateTime(value)).Union());
  }
  // Example:
  // kvDateTimeVec(b, "dateTimes", 2, [] <template T> (T *ptr, unsigned i) {
  //   new (ptr) DateTime{dateTime(ZtDate{2023, 2, i})};
  // }
  template <typename B, typename L>
  inline auto kvDateTimeVec(B &b, ZuString key, unsigned n, L l) {
    using namespace ZfbTree;
    return CreateKV(b, str(b, key), Value_DateTimeVec,
	CreateDateTimeVec(b, structVecIter(b, n, l)).Union());
  }

  template <typename B>
  inline auto kvIP(B &b, ZuString key, const ZiIP &value) {
    using namespace ZfbTree;
    return CreateKV(b, str(b, key), Value_IP,
	b.CreateStruct(ip(value)).Union());
  }
  // Example:
  // kvIPVec(b, "ips", 2, [] <template T> (T *ptr, unsigned i) {
  //   new (ptr) IP{ip(ZiIP{ZtString{} << "192.168.0." << i})};
  // }
  template <typename B, typename L>
  inline auto kvIPVec(B &b, ZuString key, unsigned n, L l) {
    using namespace ZfbTree;
    return CreateKV(b, str(b, key), Value_IPVec,
	CreateIPVec(b, structVecIter(b, n, l)).Union());
  }

  template <typename B>
  inline auto kvID(B &b, ZuString key, const ZuID &value) {
    using namespace ZfbTree;
    return CreateKV(b, str(b, key), Value_ID,
	b.CreateStruct(id(value)).Union());
  }
  // Example:
  // kvIDVec(b, "ids", 2, [] <template T> (T *ptr, unsigned i) {
  //   new (ptr) ID{id(ZuID{ZtString{} << i})};
  // }
  template <typename B, typename L>
  inline auto kvIDVec(B &b, ZuString key, unsigned n, L l) {
    using namespace ZfbTree;
    return CreateKV(b, str(b, key), Value_IDVec,
	CreateIDVec(b, structVecIter(b, n, l)).Union());
  }

  template <typename B, typename ...Args>
  inline auto kvNested(B &b, ZuString key, Args &&... args) {
    using namespace ZfbTree;
    return CreateKV(b, str(b, key), Value_NestedKVTree,
	CreateNestedKVTree(b,
	  nest(b, [...args = ZuFwd<Args>(args)]<typename B_>(B_ &b) mutable {
	    return CreateKVTree(b, args...);
	  })).Union());
  }
  // Example:
  // kvNestedVec(b, "nested", 2, [] <template B> (B &b, unsigned i) {
  //   return kvNestedVec(b, vector<Item>(b, kvString(b, "key", "value")));
  // }
  template <typename B, typename L>
  inline auto kvNestedVec(B &b, ZuString key, unsigned n, L l) {
    using namespace ZfbTree;
    return CreateKV(b, str(b, key), Value_NestedKVTreeVec,
	CreateBitmapVec(b, vectorIter<NestedKVTree>(b, n, l)).Union());
  }

  // save function for key-value tree
  template <typename B, typename SaveFn, typename PrintFn>
  inline auto kvTreeSave(B &b, KVTreeGet<SaveFn, PrintFn> v) {
    return v.saveFn(b);
  }

} // Save

namespace Load {

  // load function for key-value tree (passthrough)
  template <typename FBO>
  inline auto kvTreeLoad(const FBO *fbo) { return fbo; }

} // Load
} // Zfb

#define ZfbFieldKVTree_T Composite
#define ZfbFieldKVTree(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, kvTreeSave, kvTreeLoad)

#endif /* ZfbKVTree_HPP */
