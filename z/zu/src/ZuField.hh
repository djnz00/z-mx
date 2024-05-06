//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// composite object field metadata framework
//
// enables compile-time introspection and access to fields
// - ZtField extends ZuField to run-time introspection
//
// each field within any tuple-like composite object has a constexpr
// string ID, a get accessor and a set accessor (unless read-only)
//
// macro DSL for declaring metadata identifying and accessing fields and keys:
//
// ZuFields(Type, Field, ...);
//
// Field Syntax
// ------------
// ((Accessor)[, (Keys...)])
//
// Accessor			Description
// --------			-----------
// ID				data member
// ID, Rd			read-only data member
// ID, Alias, Member		data member - ID aliased to member
// ID, AliasRd, Member		read-only ''
// ID, Fn			function
// ID, RdFn			read-only function
// ID, AliasFn, Get, Set	function - ID aliased to getter, setter
// ID, AliasRdFn, Get		read-only ''
// ID, Lambda, Get, Set		lambda accessor
// ID, LambdaRd, Get		read-only lambda accessor
//
// if specified, Keys is a parenthesized list of key IDs 0..63, allocated
// from 0, e.g. (0, 1) - by convention, 0 is the primary key and 1+ are
// secondary keys

// ZuField API:
//   O		- type of containing object
//   T		- type of field
//   ReadOnly	- true if read-only
//   id()	- identifier (unique name) of field
//   keys()	- keys bitfield (64bit)
//   get(o)	- get function
//   set(o, v)	- set function
//
// ZuFieldKey<KeyID>(O &&) extracts a key tuple from an object
// ZuFieldKeyT<O, KeyID> is the key tuple type
// - ZuFieldKeyID::All extracts a tuple containing all fields
// - ZuFieldKeyID::AllKeys extracts a tuple containing all the key fields
// ZuFieldKeys<O> is a type list of all key types defined for O

#ifndef ZuField_HH
#define ZuField_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuTuple.hh>
#include <zlib/ZuUnion.hh>

#define ZuFieldTypeName(O, ID) ZuField_##O##_##ID

#define ZuField_Keys__(Key) (static_cast<uint64_t>(1)<<Key) |
#define ZuField_Keys_(...) (ZuPP_Map(ZuField_Keys__, __VA_ARGS__) 0)
#define ZuField_Keys(Args) ZuPP_Defer(ZuField_Keys_)Args

#define ZuField_KeySeq_(...) ZuSeq<__VA_ARGS__>
#define ZuField_KeySeq(Args) ZuPP_Defer(ZuField_KeySeq_)Args

#define ZuField_ID(O_, ID) \
  using O = O_; \
  constexpr static const char *id() { return #ID; }
#define ZuField_1(O, ID) \
  ZuField_ID(O, ID) \
  constexpr static uint64_t keys() { return 0; } \
  using Keys = ZuSeq<>;
#define ZuField_2(O, ID, Keys_) \
  ZuField_ID(O, ID) \
  constexpr static uint64_t keys() { return ZuField_Keys(Keys_); } \
  using Keys = ZuField_KeySeq(Keys_);
#define ZuField_N(O, _0, _1, Fn, ...) Fn
#define ZuField__(O, ...) \
  ZuField_N(O, __VA_ARGS__, \
    ZuField_2(O, __VA_ARGS__), \
    ZuField_1(O, __VA_ARGS__))
#define ZuField_(O, ID, Args) \
  ZuPP_Defer(ZuField__)(O, ID ZuPP_StripAppend(Args))

#define ZuFieldAdapt(O, ID) \
    using Orig = ZuFieldTypeName(O, ID); \
    template <template <typename> typename Override> \
    using Adapt = Override<Orig>

#define ZuFieldAliasRd_(O, Member) \
  using T = ZuDecay<decltype(ZuDeclVal<const O &>().Member)>; \
  static const T &get(const O &o) { return o.Member; } \
  static T &get(O &o) { return o.Member; } \
  static T &&get(O &&o) { return ZuMv(o.Member); }
#define ZuFieldAlias_(O, Member) \
  template <typename P> \
  static void set(O &o, P &&v) { o.Member = ZuFwd<P>(v); }
#define ZuFieldAliasRd(O, ID, Member, Args) \
  struct ZuFieldTypeName(O, ID) { \
    ZuFieldAdapt(O, ID); \
    enum { ReadOnly = 1 }; \
    ZuField_(O, ID, Args) \
    ZuFieldAliasRd_(O, Member) \
    template <typename P> static void set(O &, P &&) { } \
  };
#define ZuFieldAlias(O, ID, Member, Args) \
  struct ZuFieldTypeName(O, ID) { \
    ZuFieldAdapt(O, ID); \
    enum { ReadOnly = 0 }; \
    ZuField_(O, ID, Args) \
    ZuFieldAliasRd_(O, Member) \
    ZuFieldAlias_(O, Member) \
  };
#define ZuFieldAliasRdFn_(O, Get) \
  using T = ZuDecay<decltype(ZuDeclVal<const O &>().Get())>; \
  static decltype(auto) get(const O &o) { return o.Get(); } \
  static decltype(auto) get(O &o) { return o.Get(); } \
  static decltype(auto) get(O &&o) { return ZuMv(o).Get(); }
#define ZuFieldAliasFn_(O, Set) \
  template <typename V> \
  static void set(O &o, V &&v) { o.Set(ZuFwd<V>(v)); }
#define ZuFieldAliasRdFn(O, ID, Get, Args) \
  struct ZuFieldTypeName(O, ID) { \
    ZuFieldAdapt(O, ID); \
    enum { ReadOnly = 1 }; \
    ZuField_(O, ID, Args) \
    ZuFieldAliasRdFn_(O, Get) \
    template <typename P> static void set(O &, P &&) { } \
  };
#define ZuFieldAliasFn(O, ID, Get, Set, Args) \
  struct ZuFieldTypeName(O, ID) { \
    ZuFieldAdapt(O, ID); \
    enum { ReadOnly = 0 }; \
    ZuField_(O, ID, Args) \
    ZuFieldAliasRdFn_(O, Get) \
    ZuFieldAliasFn_(O, Set) \
  };
#define ZuFieldLambdaRd_(Get) \
  template <typename P> \
  static decltype(auto) get(P &&o) { \
    constexpr auto fn = Get(); \
    return fn(ZuFwd<P>(o)); \
  }
#define ZuFieldLambda_(O, Set) \
  template <typename P> \
  static void set(O &o, P &&v) { \
    constexpr auto fn = ZuPP_Strip(Set); \
    fn(o, ZuFwd<P>(v)); \
  }
#define ZuFieldLambdaRd(O, ID, Get, Args) \
  inline constexpr auto ZuField_##O##_##ID##_get() { return ZuPP_Strip(Get); } \
  struct ZuFieldTypeName(O, ID) { \
    ZuFieldAdapt(O, ID); \
    enum { ReadOnly = 1 }; \
    using T = \
      ZuDecay<decltype(ZuField_##O##_##ID##_get()(ZuDeclVal<const O &>()))>; \
    ZuField_(O, ID, Args) \
    ZuFieldLambdaRd_(ZuField_##O##_##ID##_get) \
    template <typename P> static void set(O &, P &&) { } \
  };
#define ZuFieldLambda(O, ID, Get, Set, Args) \
  inline constexpr auto ZuField_##O##_##ID##_get() { return ZuPP_Strip(Get); } \
  struct ZuFieldTypeName(O, ID) { \
    ZuFieldAdapt(O, ID); \
    enum { ReadOnly = 0 }; \
    using T = \
      ZuDecay<decltype(ZuField_##O##_##ID##_get()(ZuDeclVal<const O &>()))>; \
    ZuField_(O, ID, Args) \
    ZuFieldLambdaRd_(ZuField_##O##_##ID##_get) \
    ZuFieldLambda_(O, Set) \
  };

#define ZuField(U, Member, Args) \
  ZuFieldAlias(U, Member, Member, Args)
#define ZuFieldRd(U, Member, Args) \
  ZuFieldAliasRd(U, Member, Member, Args)

#define ZuFieldFn(U, Fn, Args) \
  ZuFieldAliasFn(U, Fn, Fn, Fn, Args)
#define ZuFieldRdFn(U, Fn, Args) \
  ZuFieldAliasRdFn(U, Fn, Fn, Args)

#define ZuField_Decl_2(O, ID, Args) \
  ZuField(O, ID, Args)
#define ZuField_Decl_3(O, ID, Method, Args) \
  ZuField##Method(O, ID, Args)
#define ZuField_Decl_4(O, ID, Method, Get, Args) \
  ZuField##Method(O, ID, Get, Args)
#define ZuField_Decl_5(O, ID, Method, Get, Set, Args) \
  ZuField##Method(O, ID, Get, Set, Args)
#define ZuField_Decl_N(O, _0, _1, _2, _3, _4, Fn, ...) Fn
#define ZuField_Decl__(O, ...) \
  ZuField_Decl_N(O, __VA_ARGS__, \
      ZuField_Decl_5(O, __VA_ARGS__), \
      ZuField_Decl_4(O, __VA_ARGS__), \
      ZuField_Decl_3(O, __VA_ARGS__), \
      ZuField_Decl_2(O, __VA_ARGS__))
#define ZuField_Decl_(O, Axor, ...) \
  ZuPP_Defer(ZuField_Decl__)(O, ZuPP_Strip(Axor), (__VA_ARGS__))
#define ZuField_Decl(O, Args) ZuPP_Defer(ZuField_Decl_)(O, ZuPP_Strip(Args))

#define ZuField_Type__(O, ID, ...) ZuFieldTypeName(O, ID)
#define ZuField_Type_(O, Axor, ...) \
  ZuPP_Defer(ZuField_Type__)(O, ZuPP_Strip(Axor))
#define ZuField_Type(O, Args) ZuPP_Defer(ZuField_Type_)(O, ZuPP_Strip(Args))

ZuTypeList<> ZuFieldList_(...); // default
void ZuFielded_(...); // default

#define ZuFields(O, ...) \
  namespace ZuFields_ { \
    ZuPP_Eval(ZuPP_MapArg(ZuField_Decl, O, __VA_ARGS__)) \
    using O = \
      ZuTypeList<ZuPP_Eval(ZuPP_MapArgComma(ZuField_Type, O, __VA_ARGS__))>; \
  } \
  O ZuFielded_(O *); \
  ZuFields_::O ZuFieldList_(O *)

template <typename U>
using ZuFieldList = decltype(ZuFieldList_(ZuDeclVal<U *>()));

template <typename T>
using ZuFielded = decltype(ZuFielded_(ZuDeclVal<ZuDecay<T> *>()));

template <typename T>
using ZuFieldOrig = typename T::Orig;

// generic tuple from field list
template <typename Tuple, typename Fields> struct ZuFieldTuple_;
// recursive decay
struct ZuFieldTuple_RDecayer {
  template <typename> struct Decay;
  template <typename ...Ts, typename Fields>
  struct Decay<ZuFieldTuple_<ZuTuple<Ts...>, Fields>> {
    using T =
      ZuFieldTuple_<ZuTypeApply<ZuTuple, ZuTypeMap<ZuRDecay, Ts...>>, Fields>;
  };
};
template <typename Tuple, typename Fields_>
struct ZuFieldTuple_ : public Tuple {
  using Tuple::Tuple;
  using Tuple::operator =;

  // adapt original fields, overriding get/set
  template <typename Base>
  struct Adapted : public Base {
    using Orig = Base;
    template <template <typename> typename Override>
    using Adapt = Adapted<Override<Orig>>;
    using O = Tuple;
    enum { I = ZuTypeIndex<Base, ZuTypeMap<ZuFieldOrig, Fields_>>{} };
    static decltype(auto) get(const O &o) { return o.template p<I>(); }
    static decltype(auto) get(O &o) { return o.template p<I>(); }
    static decltype(auto) get(O &&o) { return ZuMv(o).template p<I>(); }
    template <typename U>
    static void set(O &o, U &&v) { o.template p<I>(ZuFwd<U>(v)); }
  };
  template <typename Field>
  using Map = typename Field::template Adapt<Adapted>;
  using Fields = ZuTypeMap<Map, Fields_>;
  // bind Fields
  friend ZuFieldTuple_ ZuFielded_(ZuFieldTuple_ *);
  friend Fields ZuFieldList_(ZuFieldTuple_ *);

  // recursive decay
  friend ZuFieldTuple_RDecayer ZuRDecayer(ZuFieldTuple_ *);
};

template <
  typename O,
  template <typename> typename ObjectMap,
  template <typename> typename ValueMap,
  typename ...Fields>
struct ZuFieldTupleT_ {
  using T = ZuFieldTuple_<
    ZuTuple<ValueMap<decltype(Fields::get(ZuDeclVal<ObjectMap<O>>()))>...>,
    ZuTypeList<Fields...>>;
};
template <
  typename O,
  template <typename> typename ObjectMap,
  template <typename> typename ValueMap,
  typename ...Fields>
struct ZuFieldTupleT_<O, ObjectMap, ValueMap, ZuTypeList<Fields...>> :
  public ZuFieldTupleT_<O, ObjectMap, ValueMap, Fields...> { };
template <
  typename O,
  template <typename> typename ObjectMap,
  template <typename> typename ValueMap,
  typename ...Fields>
using ZuFieldTupleT =
  typename ZuFieldTupleT_<O, ObjectMap, ValueMap, Fields...>::T;

// value tuple - i.e. tuple of value types

template <typename O>
using ZuFieldTuple = ZuFieldTupleT<O, ZuMkCRef, ZuDecay, ZuFieldList<O>>;

// bind the appropriate tuple from an object reference

template <typename O, typename ...Fields>
struct ZuFieldTuple_Bind {
  static decltype(auto) get(const O &o) {
    return ZuFieldTupleT<O, ZuMkCRef, ZuAsIs, Fields...>{
      Fields::get(o)...
    };
  }
  static decltype(auto) get(O &&o) {
    return ZuFieldTupleT<O, ZuMkRRef, ZuAsIs, Fields...>{
      Fields::get(ZuMv(o))...
    };
  }
};
template <typename O, typename ...Fields>
struct ZuFieldTuple_Bind<O, ZuTypeList<Fields...>> :
  public ZuFieldTuple_Bind<O, Fields...> { };

// generic tuple extraction

template <typename Fields, typename P>
inline decltype(auto) ZuFieldExtract(P &&o) {
  using O = ZuFielded<P>;
  ZuAssert(Fields::N > 0);
  return ZuFieldTuple_Bind<O, Fields>::get(ZuFwd<P>(o));
}

// sentinel pseudo key IDs

namespace ZuFieldKeyID {
  enum {
    All = -1,		// all fields, including non-key fields
    AllKeys = -2	// all key fields (i.e. a union of all keys)
  };
};

// key fields, given key ID (see ZuFieldKeyID for sentinel values)

template <unsigned KeyID>
struct ZuFieldKey_ {
  template <typename U>
  struct Filter : public ZuBool<U::keys() & (1<<KeyID)> { };
};
template <typename U>
struct ZuFieldKeyAll_ : public ZuBool<U::keys()> { };
template <typename O, int KeyID>
struct ZuKeyFields_ {
  using T = ZuTypeGrep<
    ZuFieldKey_<static_cast<unsigned>(KeyID)>::template Filter,
    ZuFieldList<O>>;
};
template <typename O>
struct ZuKeyFields_<O, ZuFieldKeyID::All> {
  using T = ZuFieldList<O>;
};
template <typename O>
struct ZuKeyFields_<O, ZuFieldKeyID::AllKeys> {
  using T = ZuTypeGrep<ZuFieldKeyAll_, ZuFieldList<O>>;
};
template <typename O, int KeyID = 0>
using ZuKeyFields = typename ZuKeyFields_<O, KeyID>::T;

// generic key extraction

template <int KeyID = 0, typename P>
inline decltype(auto) ZuFieldKey(P &&o) {
  using O = ZuFielded<P>;
  return ZuFieldExtract<ZuKeyFields<O, KeyID>>(ZuFwd<P>(o));
}

// generic key accessor

template <typename O, int KeyID = 0>
inline constexpr auto ZuFieldAxor() {
  // fields in the key
  using KeyFields = ZuKeyFields<O, KeyID>;
  // check that the key comprises at least one field
  ZuAssert(KeyFields::N > 0);
  // return the accessor
  return []<typename P>(P &&o) -> decltype(auto) {
    return ZuFieldTuple_Bind<O, KeyFields>::get(ZuFwd<P>(o));
  };
}

// generic key tuple by value, and associated fields

template <typename O, int KeyID>
struct ZuFieldKeyT_ {
  // fields that comprise key
  using KeyFields = ZuKeyFields<O, KeyID>;
  // check that the key comprises at least one field
  ZuAssert(KeyFields::N > 0);
  // the key value is a tuple
  using T = ZuFieldTupleT<O, ZuMkCRef, ZuDecay, KeyFields>;
};
template <typename O, int KeyID = 0>
using ZuFieldKeyT = typename ZuFieldKeyT_<ZuFielded<O>, KeyID>::T;

// all key IDs for a type, as a ZuSeq<>

template <typename ...Fields>
struct ZuFieldKeyIDs_ {
  using T = ZuMkSeq<ZuMax<ZuSeq<ZuMax<typename Fields::Keys>{}...>>{} + 1>;
};
template <typename ...Fields>
struct ZuFieldKeyIDs_<ZuTypeList<Fields...>> :
    public ZuFieldKeyIDs_<Fields...> { };
template <typename O>
using ZuFieldKeyIDs = typename ZuFieldKeyIDs_<ZuFieldList<O>>::T;

// all keys for a type, as a typelist

template <typename O>
struct ZuFieldKeys_ {
  using KeyIDs = ZuSeqTL<ZuFieldKeyIDs<O>>;
  template <typename KeyID> using KeyT = ZuFieldKeyT<O, KeyID{}>;
  using T = ZuTypeMap<KeyT, KeyIDs>;
};
template <typename O>
using ZuFieldKeys = typename ZuFieldKeys_<O>::T;

// all keys for a type, as a ZuUnion<void, ...>

template <typename O> struct ZuFieldKeyUnion_ {
  using KeyIDs = ZuSeqTL<ZuFieldKeyIDs<O>>;
  template <typename KeyID> using KeyT = ZuFieldKeyT<O, KeyID{}>;
  using UnionTypes = typename ZuTypeMap<KeyT, KeyIDs>::template Unshift<void>;
  using T = ZuTypeApply<ZuUnion, UnionTypes>;
};
template <typename O>
using ZuFieldKeyUnion = typename ZuFieldKeyUnion_<O>::T;

#endif /* ZuField_HH */
