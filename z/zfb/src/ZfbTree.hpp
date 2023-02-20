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

#ifndef ZfbTree_HPP
#define ZfbTree_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZfbLib_HPP
#include <zlib/ZfbLib.hpp>
#endif

#include <zlib/Zfb.hpp>

#include <zlib/tree_fbs.h>

namespace ZfbTree {
using namespace Zfb;

namespace Save {
  using namespace Zfb::Save;

  template <typename B, typename ...Args>
  inline auto tree(B &b, Args &&... args) {
    return fbs::CreateTree(b, ZuFwd<Args>(args)...);
  }
  // Example:
  // itemInt32Vec(fbb, "integers", 2, [](unsigned i) { return i; });
#define ZfbTree_Primitive(ctype, fbstype) \
  template <typename B> \
  inline auto item##fbstype(B &b, ZuString key, ctype value) { \
    return fbs::CreateItem(b, str(b, key), fbs::Value_##fbstype, \
	b.CreateStruct(fbs::fbstype{value}).Union()); \
  } \
  template <typename B, typename L> \
  inline auto item##fbstype##Vec(B &b, ZuString key, unsigned n, L l) { \
    return fbs::CreateItem(b, str(b, key), fbs::Value_##fbstype##Vec, \
	fbs::Create##fbstype##Vec(b, pvectorIter(b, n, l)).Union()); \
  }
  ZfbTree_Primitive(int, Int32)
  ZfbTree_Primitive(unsigned, UInt32)
  ZfbTree_Primitive(int64_t, Int64)
  ZfbTree_Primitive(uint64_t, UInt64)
  ZfbTree_Primitive(double, Double)
  template <typename B>
  inline auto itemString(B &b, ZuString key, ZuString value) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_String,
	str(b, value).Union());
  }
  // Example:
  // itemStringVec(b, "strings", 2,
  //   [](unsigned i) { return ZtString{} << "value" << i; });
  template <typename B, typename L>
  inline auto itemStringVec(B &b, ZuString key, unsigned n, L l) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_StringVec,
	fbs::CreateStringVec(b, strVecIter(b, n, l)).Union());
  }
  template <typename B>
  inline auto itemBitmap(B &b, ZuString key, const ZmBitmap &value) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_Bitmap,
	bitmap(b, value).Union());
  }
  // Example:
  // itemBitmapVec(b, "bitmaps", 2, [] <template B> (B &b, unsigned i) {
  //   return bitmap(b, ZmBitmap{static_cast<uint64_t>(i)});
  // }
  template <typename B, typename L>
  inline auto itemBitmapVec(B &b, ZuString key, unsigned n, L l) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_BitmapVec,
	fbs::CreateBitmapVec(b, vectorIter<Bitmap>(b, n, l)).Union());
  }
  template <typename B>
  inline auto itemDecimal(B &b, ZuString key, const ZuDecimal &value) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_Decimal,
	b.CreateStruct(decimal(value)).Union());
  }
  // Example:
  // itemDecimalVec(b, "decimals", 2, [] <template T> (T *ptr, unsigned i) {
  //   new (ptr) Decimal{decimal(ZuDecimal{i})};
  // }
  template <typename B, typename L>
  inline auto itemDecimalVec(B &b, ZuString key, unsigned n, L l) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_DecimalVec,
	fbs::CreateDecimalVec(b, structVecIter(b, n, l)).Union());
  }
  template <typename B>
  inline auto itemDateTime(B &b, ZuString key, const ZtDate &value) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_DateTime,
	b.CreateStruct(dateTime(value)).Union());
  }
  // Example:
  // itemDateTimeVec(b, "dateTimes", 2, [] <template T> (T *ptr, unsigned i) {
  //   new (ptr) DateTime{dateTime(ZtDate{2023, 2, i})};
  // }
  template <typename B, typename L>
  inline auto itemDateTimeVec(B &b, ZuString key, unsigned n, L l) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_DateTimeVec,
	fbs::CreateDateTimeVec(b, structVecIter(b, n, l)).Union());
  }
  template <typename B>
  inline auto itemIP(B &b, ZuString key, const ZiIP &value) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_IP,
	b.CreateStruct(ip(value)).Union());
  }
  // Example:
  // itemIPVec(b, "ips", 2, [] <template T> (T *ptr, unsigned i) {
  //   new (ptr) IP{ip(ZiIP{ZtString{} << "192.168.0." << i})};
  // }
  template <typename B, typename L>
  inline auto itemIPVec(B &b, ZuString key, unsigned n, L l) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_IPVec,
	fbs::CreateIPVec(b, structVecIter(b, n, l)).Union());
  }
  template <typename B>
  inline auto itemID(B &b, ZuString key, const ZuID &value) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_ID,
	b.CreateStruct(id(value)).Union());
  }
  // Example:
  // itemIDVec(b, "ids", 2, [] <template T> (T *ptr, unsigned i) {
  //   new (ptr) fbs::ID{id(ZuID{ZtString{} << i})};
  // }
  template <typename B, typename L>
  inline auto itemIDVec(B &b, ZuString key, unsigned n, L l) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_IDVec,
	fbs::CreateIDVec(b, structVecIter(b, n, l)).Union());
  }
  template <typename B, typename ...Args>
  inline auto nested(B &b, Args &&... args) {
    return fbs::CreateNested(b,
      nest(b, [...args = ZuFwd<Args>(args)] <typename B_> (B_ &b) mutable {
	return fbs::CreateTree(b, args...);
      }));
  }
  template <typename B, typename ...Args>
  inline auto itemNested(B &b, ZuString key, Args &&... args) {
    using namespace Save;
    return fbs::CreateItem(b, str(b, key), fbs::Value_Nested,
	nested(b, ZuFwd<Args>(args)...).Union());
  }
  template <typename B, typename ...Args>
  inline auto itemNested_(B &b, ZuString key, Args &&... args) {
    using namespace Save;
    return fbs::CreateItem(b, str(b, key), fbs::Value_Nested,
	fbs::CreateNested(b, ZuFwd<Args>(args)...).Union());
  }
  // Example:
  // itemNestedVec(b, "nesteds", 2, [] <template B> (B &b, unsigned i) {
  //   return nested(b, vector<fbs::Item>(b, itemString(b, "key", "value")));
  // }
  template <typename B, typename L>
  inline auto itemNestedVec(B &b, ZuString key, unsigned n, L l) {
    return fbs::CreateItem(b, str(b, key), fbs::Value_NestedVec,
	fbs::CreateBitmapVec(b, vectorIter<fbs::Nested>(b, n, l)).Union());
  }
} // Save

} // ZfbTree

#endif /* ZfbTree_HPP */
