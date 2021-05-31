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

// composite object field metadata framework
//
// enables compile-time generic identification and access to fields
//
// each field within any tuple-like composite object has a constexpr
// string ID, a get accessor and a set accessor (unless read-only)

#ifndef ZuField_HPP
#define ZuField_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuLambdaFn.hpp>

#define ZuFieldType(U, ID) ZuField_##U##_##ID

// Methods for identifying and accessing fields:
//
// Method	Parameters	Description
// ------	----------	-----------
// (null)	(Member)	data member
// Ext		(ID, Member)	data member - ID, Member different
// Rd		(Member)	read-only data member
// ExtRd	(ID, Member)	read-only data member - ID, Member different
// Fn		(Fn)		function
// ExtFn	(ID, Get, Set)	function - ID, getter, setter different
// RdFn		(Fn)		read-only function
// ExtRdFn	(ID, Get)	read-only function - ID, getter different
// Lambda	(ID, Get, Set)	lambda accessor
// RdLambda	(ID, Get)	read-only lambda accessor

#define ZuFieldID_(ID) \
  static constexpr const char *id() { return #ID; }

#define ZuFieldExtRd_(U, Member) \
  using T = ZuDecay<decltype(ZuDeclVal<const U &>().Member)>; \
  static decltype(auto) get(const void *o) { \
    return static_cast<const U *>(o)->Member; \
  }
#define ZuFieldExt_(U, Member) \
  template <typename V> \
  static void set(void *o, V &&v) { \
    static_cast<U *>(o)->Member = ZuFwd<V>(v); \
  }
#define ZuFieldExtRd(U, ID, Member) \
  struct ZuFieldType(U, ID) { \
    ZuFieldID_(ID) \
    ZuFieldExtRd_(U, Member) \
  };
#define ZuFieldExt(U, ID, Member) \
  struct ZuFieldType(U, ID) { \
    ZuFieldID_(ID) \
    ZuFieldExtRd_(U, Member) \
    ZuFieldExt_(U, Member) \
  };
#define ZuFieldExtRdFn_(U, Get) \
  using T = ZuDecay<decltype(ZuDeclVal<const U &>().Get())>; \
  static decltype(auto) get(const void *o) { \
    return static_cast<const U *>(o)->Get(); \
  }
#define ZuFieldExtFn_(U, Set) \
  template <typename V> \
  static void set(void *o, V &&v) { \
    static_cast<U *>(o)->Set(ZuFwd<V>(v)); \
  }
#define ZuFieldExtRdFn(U, ID, Get) \
  struct ZuFieldType(U, ID) { \
    ZuFieldID_(ID) \
    ZuFieldExtRdFn_(U, Get) \
  };
#define ZuFieldExtFn(U, ID, Get, Set) \
  struct ZuFieldType(U, ID) { \
    ZuFieldID_(ID) \
    ZuFieldExtRdFn_(U, Get) \
    ZuFieldExtFn_(U, Set) \
  };
#define ZuFieldRdLambda_(U, Get) \
  static decltype(auto) get(const void *o) { \
    auto fn = Get(); \
    return fn(*static_cast<const U *>(o)); \
  }
#define ZuFieldLambda_(U, Set) \
  template <typename V> \
  static void set(void *o, V &&v) { \
    auto fn = ZuPP_Strip(Set); \
    fn(*static_cast<U *>(o), ZuFwd<V>(v)); \
  }
#define ZuFieldRdLambda(U, ID, Get) \
  inline auto ZuField_##U##_##ID##_get() { return ZuPP_Strip(Get); } \
  struct ZuFieldType(U, ID) { \
    using T = \
      ZuDecay<decltype(ZuField_##U##_##ID##_get()(ZuDeclVal<const U &>()))>; \
    ZuFieldID_(ID) \
    ZuFieldRdLambda_(U, ZuField_##U##_##ID##_get) \
  };
#define ZuFieldLambda(U, ID, Get, Set) \
  inline auto ZuField_##U##_##ID##_get() { return ZuPP_Strip(Get); } \
  struct ZuFieldType(U, ID) { \
    using T = \
      ZuDecay<decltype(ZuField_##U##_##ID##_get()(ZuDeclVal<const U &>()))>; \
    ZuFieldID_(ID) \
    ZuFieldRdLambda_(U, ZuField_##U##_##ID##_get) \
    ZuFieldLambda_(U, Set) \
  };

#define ZuField(U, Member) ZuFieldExt(U, Member, Member)
#define ZuFieldRd(U, Member) ZuFieldExtRd(U, Member, Member)

#define ZuFieldFn(U, Fn) ZuFieldExtFn(U, Fn, Fn, Fn)
#define ZuFieldRdFn(U, Fn) ZuFieldExtRdFn(U, Fn, Fn)

#define ZuField_Decl_(U, Method, ...) ZuField##Method(U, __VA_ARGS__)
#define ZuField_Decl(U, Args) ZuPP_Defer(ZuField_Decl_)(U, ZuPP_Strip(Args))

#define ZuField_Type_(U, Method, ID, ...) ZuFieldType(U, ID)
#define ZuField_Type(U, Args) ZuPP_Defer(ZuField_Type_)(U, ZuPP_Strip(Args))

ZuTypeList<> ZuFields_(...); // default

void *ZuFielded_(...); // default

#define ZuFields(U, ...) \
  namespace { \
    ZuPP_Eval(ZuPP_MapArg(ZuField_Decl, U, __VA_ARGS__)) \
    using ZuFields_##U = \
      ZuTypeList<ZuPP_Eval(ZuPP_MapArgComma(ZuField_Type, U, __VA_ARGS__))>; \
  } \
  U *ZuFielded_(U *); \
  ZuFields_##U ZuFieldList_(U *)

template <typename U>
using ZuFieldList = decltype(ZuFieldList_(ZuDeclVal<U *>()));

template <typename T>
using ZuFielded = ZuDeref<decltype(*ZuFielded_(ZuDeclVal<ZuDecay<T> *>()))>;

#endif /* ZuField_HPP */
