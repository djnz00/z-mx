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
  template <typename Builder, typename ...Args>
  inline auto kvTree(Builder &fbb, Args &&... args) {
    using namespace ZfbTree;
    return CreateKVTree(fbb, ZuFwd<Args>(args)...);
  }

  // kvUInt8Vec(fbb, "bytes", bytes(fbb, "data", 4));
  template <typename Builder, typename ...Args>
  inline auto kvUInt8Vec(Builder &fbb, ZuString key, Args &&... args) {
    using namespace ZfbTree;
    return CreateKV(fbb, str(fbb, key), Value_UInt8Vec,
	CreateUInt8Vec(fbb, ZuFwd<Args>(args)...).Union());
  }

  // Example:
  // kvInt32Vec(fbb, "integers", 2, [](unsigned i) { return i; });
#define ZfbKVTree_Primitive(ctype, fbstype) \
  template <typename Builder> \
  inline auto kv##fbstype(Builder &fbb, ZuString key, ctype value) { \
    using namespace ZfbTree; \
    return CreateKV(fbb, str(fbb, key), Value_##fbstype, \
	fbb.CreateStruct(fbstype{value}).Union()); \
  } \
  template <typename Builder, typename L> \
  inline auto kv##fbstype##Vec(Builder &fbb, ZuString key, unsigned n, L l) { \
    using namespace ZfbTree; \
    return CreateKV(fbb, str(fbb, key), Value_##fbstype##Vec, \
	Create##fbstype##Vec(fbb, pvectorIter(fbb, n, l)).Union()); \
  }
  ZfbKVTree_Primitive(int, Int32)
  ZfbKVTree_Primitive(unsigned, UInt32)
  ZfbKVTree_Primitive(int64_t, Int64)
  ZfbKVTree_Primitive(uint64_t, UInt64)
  ZfbKVTree_Primitive(double, Double)

  template <typename Builder>
  inline auto kvString(Builder &fbb, ZuString key, ZuString value) {
    using namespace ZfbTree;
    return CreateKV(fbb, str(fbb, key), Value_String, str(fbb, value).Union());
  }
  // Example:
  // kvStringVec(fbb, "strings", 2,
  //   [](unsigned i) { return ZtString{} << "value" << i; });
  template <typename Builder, typename L>
  inline auto kvStringVec(Builder &fbb, ZuString key, unsigned n, L l) {
    using namespace ZfbTree;
    return CreateKV(fbb, str(fbb, key), Value_StringVec,
	CreateStringVec(fbb, strVecIter(fbb, n, l)).Union());
  }

  template <typename Builder>
  inline auto kvBitmap(Builder &fbb, ZuString key, const ZmBitmap &value) {
    using namespace ZfbTree;
    return CreateKV(fbb, str(fbb, key), Value_Bitmap,
	bitmap(fbb, value).Union());
  }
  // Example:
  // kvBitmapVec(fbb, "bitmaps", 2, [] <template B> (Builder &fbb, unsigned i) {
  //   return bitmap(fbb, ZmBitmap{static_cast<uint64_t>(i)});
  // }
  template <typename Builder, typename L>
  inline auto kvBitmapVec(Builder &fbb, ZuString key, unsigned n, L l) {
    using namespace ZfbTree;
    return CreateKV(fbb, str(fbb, key), Value_BitmapVec,
	CreateBitmapVec(fbb, vectorIter<Bitmap>(fbb, n, l)).Union());
  }

  template <typename Builder>
  inline auto kvDecimal(Builder &fbb, ZuString key, const ZuDecimal &value) {
    using namespace ZfbTree;
    return CreateKV(fbb, str(fbb, key), Value_Decimal,
	fbb.CreateStruct(decimal(value)).Union());
  }
  // Example:
  // kvDecimalVec(fbb, "decimals", 2, [] <template T> (T *ptr, unsigned i) {
  //   new (ptr) Decimal{decimal(ZuDecimal{i})};
  // }
  template <typename Builder, typename L>
  inline auto kvDecimalVec(Builder &fbb, ZuString key, unsigned n, L l) {
    using namespace ZfbTree;
    return CreateKV(fbb, str(fbb, key), Value_DecimalVec,
	CreateDecimalVec(fbb, structVecIter(fbb, n, l)).Union());
  }

  template <typename Builder>
  inline auto kvDateTime(Builder &fbb, ZuString key, const ZtDate &value) {
    using namespace ZfbTree;
    return CreateKV(fbb, str(fbb, key), Value_DateTime,
	fbb.CreateStruct(dateTime(value)).Union());
  }
  // Example:
  // kvDateTimeVec(fbb, "dateTimes", 2, [] <template T> (T *ptr, unsigned i) {
  //   new (ptr) DateTime{dateTime(ZtDate{2023, 2, i})};
  // }
  template <typename Builder, typename L>
  inline auto kvDateTimeVec(Builder &fbb, ZuString key, unsigned n, L l) {
    using namespace ZfbTree;
    return CreateKV(fbb, str(fbb, key), Value_DateTimeVec,
	CreateDateTimeVec(fbb, structVecIter(fbb, n, l)).Union());
  }

  template <typename Builder>
  inline auto kvIP(Builder &fbb, ZuString key, const ZiIP &value) {
    using namespace ZfbTree;
    return CreateKV(fbb, str(fbb, key), Value_IP,
	fbb.CreateStruct(ip(value)).Union());
  }
  // Example:
  // kvIPVec(fbb, "ips", 2, [] <template T> (T *ptr, unsigned i) {
  //   new (ptr) IP{ip(ZiIP{ZtString{} << "192.168.0." << i})};
  // }
  template <typename Builder, typename L>
  inline auto kvIPVec(Builder &fbb, ZuString key, unsigned n, L l) {
    using namespace ZfbTree;
    return CreateKV(fbb, str(fbb, key), Value_IPVec,
	CreateIPVec(fbb, structVecIter(fbb, n, l)).Union());
  }

  template <typename Builder>
  inline auto kvID(Builder &fbb, ZuString key, const ZuID &value) {
    using namespace ZfbTree;
    return CreateKV(fbb, str(fbb, key), Value_ID,
	fbb.CreateStruct(id(value)).Union());
  }
  // Example:
  // kvIDVec(fbb, "ids", 2, [] <template T> (T *ptr, unsigned i) {
  //   new (ptr) ID{id(ZuID{ZtString{} << i})};
  // }
  template <typename Builder, typename L>
  inline auto kvIDVec(Builder &fbb, ZuString key, unsigned n, L l) {
    using namespace ZfbTree;
    return CreateKV(fbb, str(fbb, key), Value_IDVec,
	CreateIDVec(fbb, structVecIter(fbb, n, l)).Union());
  }

  template <typename Builder, typename ...Args>
  inline auto kvNested(Builder &fbb, ZuString key, Args &&... args) {
    using namespace ZfbTree;
    return CreateKV(fbb, str(fbb, key), Value_NestedKVTree,
	CreateNestedKVTree(fbb,
	  nest(fbb, [...args = ZuFwd<Args>(args)](auto &fbb) mutable {
	    return CreateKVTree(fbb, args...);
	  })).Union());
  }
  // Example:
  // kvNestedVec(fbb, "nested", 2, [] <template B> (Builder &fbb, unsigned i) {
  //   return kvNestedVec(fbb, vector<Item>(fbb, kvString(fbb, "key", "value")));
  // }
  template <typename Builder, typename L>
  inline auto kvNestedVec(Builder &fbb, ZuString key, unsigned n, L l) {
    using namespace ZfbTree;
    return CreateKV(fbb, str(fbb, key), Value_NestedKVTreeVec,
	CreateBitmapVec(fbb, vectorIter<NestedKVTree>(fbb, n, l)).Union());
  }

  // save function for key-value tree
  template <typename Builder, typename SaveFn, typename PrintFn>
  inline auto kvTreeSave(Builder &fbb, KVTreeGet<SaveFn, PrintFn> v) {
    return v.saveFn(fbb);
  }

} // Save

namespace Load {

  // load function for key-value tree (passthrough)
  template <typename FBO>
  inline auto kvTreeLoad(const FBO *fbo) { return fbo; }

} // Load
} // Zfb

#define ZfbFieldKVTree_T UDT
#define ZfbFieldKVTree(O, ...) \
  ZfbFieldNested(O, __VA_ARGS__, kvTreeSave, kvTreeLoad)

#endif /* ZfbKVTree_HPP */
