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

// object introspection
// - print/scan (CSV, etc.)
// - ORM
// - data series

// metadata macro DSL for identifying and accessing data fields and keys
//
// ZtFields(Type, Fields...)
//
// each field has compile-time properties, an extensible typelist of types
// that are injected into the ZtFieldProp namespace
//
// a Field is of the form:
// (((Accessor)[, (Keys...)]), (Type[, Args...])[, (Props...)])
//
// Example: (((id, Rd), (0)), (String, "default"), (Ctor(0)))
//
// ZtField Type	C++ Type		ZtField Args
// ------------	--------		------------
// String	<String>		[, default]
// Bytes	<String>		[, default]
// UDT		<UDT>			[, default]
// Bool		<Integral>		[, default]
// Int		<Integral>		[, default, min, max]
// UInt		<Integral>		[, default, min, max]
// Enum, Map	<Integral>		[, default]
// Flags, Map	<Integral>		[, default]
// Float	<FloatingPoint>		[, default, min, max]
// Fixed	ZuFixed			[, default, min, max]
// Decimal	ZuDecimal		[, default, min, max]
// Time		ZmTime			[, default]

// Notes on run-time introspection (ZVField*): virtual polymorphism and
// RTTI are avoided because:
// 1] If ZtVField were virtually polymorphic, deriving from it to enrich it
//    with capabilities such as data-store serdes would entail a far more
//    complex type hierarchy with diamond-shaped inheritance
// 2] ZtVField (and derived classes) benefit from being POD
// 3] Very little syntactic benefit would be obtained

// ZtFieldType provides:
//   T		- underlying type
//   Props	- properties type list
//   Print	- Print{const T &, const ZtFieldFmt &} - formatted printing
//   vtype()	- ZtVFieldType * instance
// 
// ZtField derives from ZuField and adds:
//   Type	- ZtFieldType
//   Map	- map (only defined for Enum and Flags)
//   Print	- Print{const O &, const ZtFieldFmt &} - formatted printing
//   deflt()	- canonical default value
//
// ZtVFieldType provides:
//   code	- ZtFieldTypeCode
//   info	- enum / flags metadata
//   getFn	- get canonical type from opaque value
//   setFn	- set opaque value to canonical type
//   print	- print opaque value
//   scan	- scan opaque value from string
//   cmp	- compare opaque values
//
// ZtVField{ZtField{}} instantiates VField with ZtField
//
// ZtVField provides:
//   type	- ZtVFieldType * instance
//   id		- ZtField::id()
//   props	- ZtVFieldProp properties bitfield
//   keys	- ZtField::keys()
//   constantFn	- run-time constant function (ZtVFieldConstant) -> const void *
//   getFn	- run-time get function (const void *o, ZmFn<const void *> fn)
//   setFn	- run-time set function (void *o) -> ZmFn<void *>

#ifndef ZtField_HPP
#define ZtField_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZtLib_HPP
#include <zlib/ZtLib.hpp>
#endif

#include <typeinfo>

#include <zlib/ZuPrint.hpp>
#include <zlib/ZuBox.hpp>
#include <zlib/ZuArray.hpp>
#include <zlib/ZuString.hpp>
#include <zlib/ZuFixed.hpp>
#include <zlib/ZuDecimal.hpp>
#include <zlib/ZuUnroll.hpp>
#include <zlib/ZuConversion.hpp>
#include <zlib/ZuField.hpp>

#include <zlib/ZmStream.hpp>
#include <zlib/ZmTime.hpp>
#include <zlib/ZmSingleton.hpp>

#include <zlib/ZtEnum.hpp>
#include <zlib/ZtDate.hpp>
#include <zlib/ZtString.hpp>

namespace ZtFieldTypeCode {
  ZtEnumValues("ZtFieldTypeCode",
    String,		// a contiguous UTF-8 string
    Bytes,		// byte array
    UDT,		// generic udt type
    Bool,		// an integral type, interpreted as bool
    Int,		// an integral type <= 64bits
    UInt,		// an unsigned integratl type <= 64bits
    Enum,		// an integral enumerated type
    Flags,		// an integral enumerated bitfield type
    Float,		// floating point type
    Fixed,		// ZuFixed
    Decimal,		// ZuDecimal
    Time		// ZmTime
  );
}

// compile-time field property list - a typelist of individual properties:
// - each type is declared in the ZtFieldProp namespace
// - additional properties can be injected into the ZtFieldProp namespace
namespace ZtFieldProp {
  struct Synthetic { };		// synthetic and read-only
  struct Update { };		// include in updates
  struct Hidden { };		// do not print
  struct Quote { };		// print quoted string
  struct Hex { };		// print hex value
  struct Required { };		// required - do not default
  struct Series { };		// data series column
  struct Index { };		// - index (e.g. time, nonce, offset, seq#)
  struct Delta { };		// - first derivative
  struct Delta2 { };		// - second derivative

  template <unsigned I>
  struct Ctor : public ZuUnsigned<I> { }; // constructor parameter

  template <unsigned I>
  struct NDP : public ZuUnsigned<I> { }; // NDP for printing float/fixed/decimal

  // Int<Prop, List> - read integer property value from List (-1 if unset)
  template <template <unsigned> class Prop>
  struct Grep_ {
    template <typename T> struct Is : public ZuFalse { };
    template <unsigned I> struct Is<Prop<I>> : public ZuTrue { };
    template <typename List> using Apply = ZuTypeGrep<Is, List>;
  };
  template <template <unsigned> class Prop, typename List>
  using Grep = Grep_<Prop>::template Apply<List>;
  template <
    template <unsigned> class Prop,
    typename List,
    typename Filtered = Grep<Prop, List>,
    unsigned N = Filtered::N>
  struct Int : ZuInt<-1> { };
  template <
    template <unsigned> class Prop,
    typename List,
    typename Filtered>
  struct Int<Prop, List, Filtered, 1> : ZuInt<ZuType<0, Filtered>{}> { };

  template <typename Props> using GetCtor = Int<Ctor, Props>;
  template <typename Props> using GetNDP = Int<NDP, Props>;
}

#define ZtField_Props__(Prop) ZtFieldProp::Prop
#define ZtField_Props_(...) ZuPP_MapComma(ZtField_Props__, __VA_ARGS__)
#define ZtField_Props(Args) ZuPP_Defer(ZtField_Props_)(ZuPP_Strip(Args))

// formatted field printing/scanning
struct ZtFieldFmt {
  ZuVFmt		scalar;			// scalar format (print only)
  ZtDateScan::Any	dateScan;		// date/time scan format
  ZtDateFmt::Any	datePrint;		// date/time print format
  char			flagsDelim = '|';	// flags delimiter
};

// type properties are a subset of field properties
template <typename Prop> struct ZtFieldType_Props : public ZuFalse { };
template <> struct ZtFieldType_Props<ZtFieldProp::Hidden> : public ZuTrue { };
template <> struct ZtFieldType_Props<ZtFieldProp::Quote> : public ZuTrue { };
template <> struct ZtFieldType_Props<ZtFieldProp::Hex> : public ZuTrue { };

// ZtVFieldProp bitfield encapsulates introspected ZtField properties
namespace ZtVFieldProp {
  ZtEnumFlags("ZtVFieldProp",
    Synthetic,
    Update,
    Hidden,
    Quote,
    Required,
    Ctor_,
    NDP_,
    Series,
    Index,
    Delta,
    Delta2);

  enum {
    CtorShift	= N,		// bit-shift for constructor parameter index
    CtorMask	= 0x3f		// 6 bits, 0-63
  };
  enum {
    NDPShift	= N + 6,
    NDPMask	= 0x1f		// 5 bits, 0-31
  };

  // parameter index -> flags
  inline constexpr unsigned Ctor(unsigned i) {
    return Ctor_ | (i<<CtorShift);
  }
  inline constexpr unsigned getCtor(unsigned flags) {
    return (flags>>CtorShift) & CtorMask;
  }
  // NDP -> flags
  inline constexpr unsigned NDP(unsigned i) {
    return NDP_ | (i<<NDPShift);
  }
  inline constexpr unsigned getNDP(unsigned flags) {
    return (flags>>NDPShift) & NDPMask;
  }

  // Value<Prop>::N - return bitfield for individual property
  template <typename> struct Value : public ZuUnsigned<0> { }; // default

  namespace _ = ZtFieldProp;

  template <> struct Value<_::Synthetic> : public ZuUnsigned<Synthetic> { };
  template <> struct Value<_::Update>    : public ZuUnsigned<Update> { };
  template <> struct Value<_::Hidden>    : public ZuUnsigned<Hidden> { };
  template <> struct Value<_::Quote>     : public ZuUnsigned<Quote> { };
  template <> struct Value<_::Required>  : public ZuUnsigned<Required> { };
  template <> struct Value<_::Series>    : public ZuUnsigned<Series> { };
  template <> struct Value<_::Index>     : public ZuUnsigned<Index> { };
  template <> struct Value<_::Delta>     : public ZuUnsigned<Delta> { };
  template <> struct Value<_::Delta2>    : public ZuUnsigned<Delta2> { };

  template <unsigned I>
  struct Value<_::Ctor<I>> : public ZuUnsigned<Ctor(I)> { };

  template <unsigned I>
  struct Value<_::NDP<I>>  : public ZuUnsigned<NDP(I)> { };

  // Value<List>::N - return bitfield for property list
  template <typename ...> struct Or_;
  template <> struct Or_<> {
    using T = ZuUnsigned<0>;
  };
  template <typename Prop> struct Or_<Prop> {
    using T = Value<Prop>;
  };
  template <typename Prop1, typename Prop2> struct Or_<Prop1, Prop2> {
    using T = ZuUnsigned<Value<Prop1>{} | Value<Prop2>{}>;
  };
  template <typename ...Props> using Or = typename Or_<Props...>::T;
  template <typename ...Props>
  struct Value<ZuTypeList<Props...>> :
      public ZuTypeReduce<Or, ZuTypeList<Props...>> { };
}

// type is keyed on type-code, underlying type, type properties, type args
// - args are the Map used with Enum and Flags
template <typename Props>
struct ZtFieldType_ {
  constexpr static uint64_t vprops() {
    return ZtVFieldProp::Value<Props>{};
  }
};
// Map is void if neither Enum nor Flags
template <int TypeCode, typename T, typename Map, typename Props>
struct ZtFieldType;

// deduced function to scan from a string
template <typename T, typename = void>
struct ZtFieldType_Scan {
  static auto fn() {
    typedef void (*Fn)(void *, ZuString, const ZtFieldFmt &);
    return static_cast<Fn>(nullptr);
  }
};
template <typename T>
struct ZtFieldType_Scan<T, decltype((ZuDeclVal<T &>() = ZuString{}), void())> {
  static auto fn() {
    return [](void *ptr, ZuString s, const ZtFieldFmt &) {
      *static_cast<T *>(ptr) = s;
    };
  }
};

// deduced comparison function
template <typename T, typename = void>
struct ZtFieldType_Cmp {
  static auto fn() {
    typedef int (*Fn)(const void *, const void *);
    return static_cast<Fn>(nullptr);
  }
};
template <typename T>
struct ZtFieldType_Cmp<T, decltype(&ZuCmp<T>::cmp, void())> {
  static auto fn() {
    return [](const void *p1, const void *p2) -> int {
      return ZuCmp<T>::cmp(
	  *static_cast<const T *>(p1), *static_cast<const T *>(p2));
    };
  }
};

// ZtVFieldEnum encapsulates introspected enum metadata
struct ZtVFieldEnum {
  const char	*(*id)();
  int		(*s2v)(ZuString);
  ZuString	(*v2s)(int);
};
template <typename Map>
struct ZtVFieldEnum_ : public ZtVFieldEnum {
  ZtVFieldEnum_() : ZtVFieldEnum{
    .id = []() -> const char * { return Map::id(); },
    .s2v = [](ZuString s) -> int { return Map::s2v(s); },
    .v2s = [](int i) -> ZuString { return Map::v2s(i); }
  } { }

  static ZtVFieldEnum *instance() {
    return ZmSingleton<ZtVFieldEnum_>::instance();
  }
};

// ZtVFieldFlags encapsulates introspected flags metadata
struct ZtVFieldFlags {
  const char	*(*id)();
  void		(*print)(uint64_t, ZmStream &, const ZtFieldFmt &);
  void		(*scan)(uint64_t &, ZuString, const ZtFieldFmt &);
};
template <typename Map>
struct ZtVFieldFlags_ : public ZtVFieldFlags {
  ZtVFieldFlags_() : ZtVFieldFlags{
    .id = []() -> const char * { return Map::id(); },
    .print = [](uint64_t v, ZmStream &s, const ZtFieldFmt &fmt) -> void {
      s << Map::print(v, fmt.flagsDelim);
    },
    .scan = [](uint64_t &v, ZuString s, const ZtFieldFmt &fmt) -> void {
      v = Map::template scan<uint64_t>(s, fmt.flagsDelim);
    }
  } { }

  static ZtVFieldFlags *instance() {
    return ZmSingleton<ZtVFieldFlags_>::instance();
  }
};

// ZtVFieldType encapsulates introspected type metadata
struct ZtVFieldType {
  int			code;			// ZtFieldTypeCode
  uint32_t		props;			// ZtVFieldProp

  union {
    void		*null;
    ZtVFieldEnum *	(*enum_)();		// Enum
    ZtVFieldFlags *	(*flags_)();		// Flags
  } info;

  union {
    void		*null;
    ZuString		(*string)(const void *);	// String
    ZuBytes		(*bytes)(const void *);		// Bytes
    bool		(*bool_)(const void *);		// Bool
    const void *	(*udt)(const void *);		// UDT
    int64_t		(*int_)(const void *);		// Int
    uint64_t		(*uint_)(const void *);		// UInt
    int			(*enum_)(const void *);		// Enum
    uint64_t		(*flags)(const void *);		// Flags
    double		(*float_)(const void *);	// Float
    ZuFixed		(*fixed)(const void *);		// Fixed
    ZuDecimal		(*decimal)(const void *);	// Decimal
    ZmTime		(*time)(const void *);		// Time
  } getFn;

  union {
    void		*null;
    void		(*string)(void *, ZuString);	// String
    void		(*bytes)(void *, ZuBytes);	// Bytes
    void		(*udt)(void *, const void *);	// UDT
    void		(*bool_)(void *, bool);		// Bool
    void		(*int_)(void *, int64_t);	// Int
    void		(*uint_)(void *, uint64_t);	// UInt
    void		(*enum_)(void *, int);		// Enum
    void		(*flags)(void *, uint64_t);	// Flags
    void		(*float_)(void *, double);	// Float
    void		(*fixed)(void *, ZuFixed);	// Fixed
    void		(*decimal)(void *, ZuDecimal);	// Decimal
    void		(*time)(void *, ZmTime);	// Time
  } setFn;

  void (*print)(const void *, ZmStream &, const ZtFieldFmt &);
  void (*scan)(void *, ZuString, const ZtFieldFmt &);

  int (*cmp)(const void *, const void *);

  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldTypeCode::String> get(const void *p, L &&l) const {
    (ZuFwd<L>(l))(getFn.string(p));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldTypeCode::Bytes> get(const void *p, L &&l) const {
    (ZuFwd<L>(l))(getFn.bytes(p));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldTypeCode::UDT> get(const void *p, L &&l) const {
    (ZuFwd<L>(l))(p);
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldTypeCode::Bool> get(const void *p, L &&l) const {
    (ZuFwd<L>(l))(getFn.bool_(p));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldTypeCode::Int> get(const void *p, L &&l) const {
    (ZuFwd<L>(l))(getFn.int_(p));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldTypeCode::UInt> get(const void *p, L &&l) const {
    (ZuFwd<L>(l))(getFn.uint_(p));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldTypeCode::Enum> get(const void *p, L &&l) const {
    (ZuFwd<L>(l))(getFn.enum_(p));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldTypeCode::Flags> get(const void *p, L &&l) const {
    (ZuFwd<L>(l))(getFn.flags(p));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldTypeCode::Float> get(const void *p, L &&l) const {
    (ZuFwd<L>(l))(getFn.float_(p));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldTypeCode::Fixed> get(const void *p, L &&l) const {
    (ZuFwd<L>(l))(getFn.fixed(p));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldTypeCode::Decimal> get(const void *p, L &&l) const {
    (ZuFwd<L>(l))(getFn.decimal(p));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldTypeCode::Time> get(const void *p, L &&l) const {
    (ZuFwd<L>(l))(getFn.time(p));
  }

  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::String> set(void *p, U &&v) const {
    setFn.string(p, ZuString{ZuFwd<U>(v)});
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Bytes> set(void *p, U &&v) const {
    setFn.bytes(p, ZuBytes{ZuFwd<U>(v)});
  }
  template <unsigned I>
  ZuIfT<I == ZtFieldTypeCode::UDT> set(void *p, const void *v) const {
    setFn.udt(p, v);
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Bool> set(void *p, U &&v) const {
    setFn.bool_(p, ZuFwd<U>(v));
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Int> set(void *p, U &&v) const {
    setFn.int_(p, ZuFwd<U>(v));
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::UInt> set(void *p, U &&v) const {
    setFn.uint_(p, ZuFwd<U>(v));
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Enum> set(void *p, U &&v) const {
    setFn.enum_(p, ZuFwd<U>(v));
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Flags> set(void *p, U &&v) const {
    setFn.flags(p, ZuFwd<U>(v));
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Float> set(void *p, U &&v) const {
    setFn.float_(p, ZuFwd<U>(v));
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Fixed> set(void *p, U &&v) const {
    setFn.fixed(p, ZuFixed{ZuFwd<U>(v)});
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Decimal> set(void *p, U &&v) const {
    setFn.decimal(p, ZuDecimal{ZuFwd<U>(v)});
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Time> set(void *p, U &&v) const {
    setFn.time(p, ZmTime{ZuFwd<U>(v)});
  }
};

// ZtVFieldConstant is used to retrieve field constants
namespace ZtVFieldConstant {
  enum { Null = 0, Deflt, Minimum, Maximum };
}

// ZtField_ compile-time encapsulates an individual field, derives from ZuField
template <typename Base, typename Props_, bool = Base::ReadOnly>
struct ZtField_ : public Base {
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = Props_;
  constexpr static uint64_t vprops() {
    return ZtVFieldProp::Value<Props>{};
  }
  template <typename O_ = O, typename T_ = T>
  static auto getFn(ZuExact<const T_ &,
      decltype(Base::get(ZuDeclVal<const O_ &>()))> *_ = nullptr) {
    return [](const void *o, ZmFn<const void *> fn) {
      const T &v = Base::get(*static_cast<const O *>(o));
      fn(static_cast<const void *>(&v));
    };
  }
  template <typename O_ = O, typename T_ = T>
  static auto getFn(ZuNotExact<const T_ &,
      decltype(Base::get(ZuDeclVal<const O_ &>()))> *_ = nullptr) {
    return [](const void *o, ZmFn<const void *> fn) {
      T v = Base::get(*static_cast<const O *>(o));
      fn(static_cast<const void *>(&v));
    };
  }
  static auto setFn() {
    typedef void (*Fn)(void *, ZmFn<void *>);
    return static_cast<Fn>(nullptr);
  }
};
template <typename Base, typename Props>
struct ZtField_<Base, Props, false> : public ZtField_<Base, Props, true> {
  using O = typename Base::O;
  using T = typename Base::T;
  template <typename O_ = O, typename T_ = T>
  static auto setFn(ZuExact<T_ &,
      decltype(Base::get(ZuDeclVal<O_ &>()))> *_ = nullptr) {
    return [](void *o, ZmFn<void *> fn) {
      T_ &v = Base::get(static_cast<O &>(*static_cast<O *>(o)));
      fn(static_cast<void *>(&v));
    };
  }
  template <typename O_ = O, typename T_ = T>
  static auto setFn(ZuNotExact<T_ &,
      decltype(Base::get(ZuDeclVal<O_ &>()))> *_ = nullptr) {
    return [](void *o, ZmFn<void *> fn) {
      T v;
      fn(static_cast<void *>(&v));
      Base::set(*static_cast<O *>(o), ZuMv(v));
    };
  }
};

// --- String

template <typename T_, typename Props_>
struct ZtFieldType_String : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::String };
  using T = T_;
  using Props = Props_;
  struct Print {
    ZuString v;
    const ZtFieldFmt &fmt; // unused, but needed at compile-time
    enum { Quote =
      ZuTypeIn<ZtFieldProp::Quote, Props>{} &&
      ZuTraits<T>::IsString && !ZuTraits<T>::IsWString };
    template <typename S, bool Quote_ = Quote>
    friend ZuIfT<Quote_, S &> operator <<(S &s, const Print &print) {
      const auto &v = print.v;
      s << '"';
      for (unsigned i = 0, n = v.length(); i < n; i++) {
	char c = v[i];
	if (ZuUnlikely(c == '"')) s << '\\';
	s << c;
      }
      return s << '"';
    }
    template <typename S, bool Quote_ = Quote>
    friend ZuIfT<!Quote_, S &> operator <<(S &s, const Print &print) {
      s << '"';
      s << print.v;
      return s << '"';
    }
  };
  inline static ZtVFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::String, T, void, Props> :
    public ZtFieldType_String<T, Props> { };

template <typename T, typename Props,
  bool = ZuTraits<T>::IsCString && ZuTraits<T>::IsPrimitive>
struct ZtVFieldType_String : public ZtVFieldType {
  ZtVFieldType_String() : ZtVFieldType{
    .code = ZtFieldTypeCode::String,
    .props = ZtVFieldProp::Value<Props>{},
    .info = {.null = nullptr},
    .getFn = {.string = [](const void *ptr) -> ZuString {
      return *(static_cast<const T *>(ptr));
    }},
    .setFn = {.string = [](void *ptr, ZuString v) {
      *(static_cast<T *>(ptr)) = v;
    }},
    .print = [](const void *ptr, ZmStream &s, const ZtFieldFmt &fmt) {
      using Print = typename ZtFieldType_String<T, Props>::Print;
      s << Print{*static_cast<const T *>(ptr), fmt};
    },
    .scan = [](void *ptr, ZuString s, const ZtFieldFmt &fmt) {
      *static_cast<T *>(ptr) = s;
    },
    .cmp = ZtFieldType_Cmp<T>::fn()
  } { }
};
template <typename T, typename Props>
struct ZtVFieldType_String<T, Props, true> : public ZtVFieldType {
  ZtVFieldType_String() : ZtVFieldType{
    .code = ZtFieldTypeCode::String,
    .props = ZtVFieldProp::Value<Props>{},
    .info = {.null = nullptr},
    .getFn = {.string = [](const void *ptr) -> ZuString {
      return *(static_cast<const T *>(ptr));
    }},
    .setFn = {.string = [](void *ptr, ZuString v) { }},
    .print = [](const void *ptr, ZmStream &s, const ZtFieldFmt &fmt) {
      using Print = typename ZtFieldType_String<T, Props>::Print;
      s << Print{*static_cast<const T *>(ptr), fmt};
    },
    .scan = [](void *ptr, ZuString s, const ZtFieldFmt &fmt) { },
    .cmp = ZtFieldType_Cmp<T>::fn()
  } { }
};
template <typename T, typename Props>
ZtVFieldType *ZtFieldType_String<T, Props>::vtype() {
  return ZmSingleton<ZtVFieldType_String<T, Props>>::instance();
}

inline ZuString ZtField_String_Def() { return {}; }
template <typename Base, typename Props, auto Def = ZtField_String_Def>
struct ZtField_String : public ZtField_<Base, Props> {
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_String<
    typename Base::T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  using Print_ = typename Type::Print;
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Base::id() << '=' << Print_{Base::get(print.o), print.fmt};
    }
  };
  static ZuString deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    static T deflt_{Def()};
    return [](int i) -> const void * {
      if (ZuLikely(i == Deflt)) return static_cast<const void *>(&deflt_);
      return nullptr;
    };
  }
};

// --- Bytes

template <typename T_, typename Props_>
struct ZtFieldType_Bytes : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::Bytes };
  using T = T_;
  using Props = Props_;
  struct Print {
    ZuBytes v;
    const ZtFieldFmt &fmt; // unused, but needed at compile-time
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << ZtHexDump_{print.v};
    }
  };
  inline static ZtVFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Bytes, T, void, Props> :
    public ZtFieldType_Bytes<T, Props> { };

template <typename T, typename Props>
struct ZtVFieldType_Bytes : public ZtVFieldType {
  ZtVFieldType_Bytes() : ZtVFieldType{
    .code = ZtFieldTypeCode::Bytes,
    .props = ZtVFieldProp::Value<Props>{},
    .info = {.null = nullptr},
    .getFn = {.bytes = [](const void *ptr) -> ZuBytes {
      return *(static_cast<const T *>(ptr));
    }},
    .setFn = {.bytes = [](void *ptr, ZuBytes v) {
      *(static_cast<T *>(ptr)) = v;
    }},
    .print = [](const void *ptr, ZmStream &s, const ZtFieldFmt &fmt) {
      using Print = typename ZtFieldType_Bytes<T, Props>::Print;
      s << Print{*static_cast<const T *>(ptr), fmt};
    },
    .scan = [](void *ptr, ZuString s, const ZtFieldFmt &fmt) {
      *static_cast<T *>(ptr) =
	ZuBytes{reinterpret_cast<const uint8_t *>(s.data()), s.length()};
    },
    .cmp = ZtFieldType_Cmp<T>::fn()
  } { }
};
template <typename T, typename Props>
ZtVFieldType *ZtFieldType_Bytes<T, Props>::vtype() {
  return ZmSingleton<ZtVFieldType_Bytes<T, Props>>::instance();
}

inline ZuBytes ZtField_Bytes_Def() { return {}; }
template <typename Base, typename Props, auto Def = ZtField_Bytes_Def>
struct ZtField_Bytes : public ZtField_<Base, Props> {
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Bytes<
    typename Base::T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  using Print_ = typename Type::Print;
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Base::id() << '=' << Print_{Base::get(print.o), print.fmt};
    }
  };
  static ZuBytes deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    static T deflt_{Def()};
    return [](int i) -> const void * {
      if (ZuLikely(i == Deflt)) return static_cast<const void *>(&deflt_);
      return nullptr;
    };
  }
};

// --- UDT

template <typename T_, typename Props_>
struct ZtFieldType_UDT : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::UDT };
  using T = T_;
  using Props = Props_;
  struct Print {
    const T &v;
    const ZtFieldFmt &fmt; // unused, but needed at compile-time
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << print.v;
    }
  };
  inline static ZtVFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::UDT, T, void, Props> :
    public ZtFieldType_UDT<T, Props> { };

template <typename T, typename Props>
struct ZtVFieldType_UDT : public ZtVFieldType {
  ZtVFieldType_UDT() : ZtVFieldType{
    .code = ZtFieldTypeCode::UDT,
    .props = ZtVFieldProp::Value<Props>{},
    .info = {.null = nullptr},
    .getFn = {.udt = [](const void *p) {
      return p;
    }},
    .setFn = {.udt = [](void *p, const void *v) {
      static_cast<T *>(p)->~T();
      new (p) T{*static_cast<const T *>(v)};
    }},
    .print = [](const void *ptr, ZmStream &s, const ZtFieldFmt &fmt) {
      s << *static_cast<const T *>(ptr);
    },
    .scan = ZtFieldType_Scan<T>::fn(),
    .cmp = ZtFieldType_Cmp<T>::fn()
  } { }
};
template <typename T, typename Props>
ZtVFieldType *ZtFieldType_UDT<T, Props>::vtype() {
  return ZmSingleton<ZtVFieldType_UDT<T, Props>>::instance();
}

template <typename T, typename = void>
struct ZtField_UDT_Def {
  constexpr static void value() { }
};
template <typename T>
struct ZtField_UDT_Def<T, decltype(T{}, void())> {
  constexpr static T value() { return {}; }
};
template <typename T, typename = void>
struct ZtField_UDT_Null {
  static const void *value() { return nullptr; }
};
template <typename T>
struct ZtField_UDT_Null<T, decltype(T{}, void())> {
  static const void *value() {
    static T null_;
    return static_cast<const void *>(&null_);
  }
};
template <typename T, auto Def, typename = void>
struct ZtField_UDT_Constant {
  static auto constantFn() {
    typedef const void *(*Fn)(int);
    return static_cast<Fn>(nullptr);
  }
};
template <typename T, auto Def>
struct ZtField_UDT_Constant<T, Def, decltype(T{Def()}, void())> {
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    static T deflt_{Def()};
    return [](int i) -> const void * {
      if (ZuLikely(i == Deflt)) return static_cast<const void *>(&deflt_);
      return ZtField_UDT_Null<T>::value();
    };
  }
};
template <
  typename Base, typename Props,
  auto Def = ZtField_UDT_Def<typename Base::T>::value>
struct ZtField_UDT :
    public ZtField_<Base, Props>,
    public ZtField_UDT_Constant<typename Base::T, Def> {
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_UDT<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  using Print_ = typename Type::Print;
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Base::id() << '=' << Print_{Base::get(print.o), print.fmt};
    }
  };
  static auto deflt() { return Def(); }
};

// --- Bool

template <typename T_, typename Props_>
struct ZtFieldType_Bool : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::Bool };
  using T = T_;
  using Props = Props_;
  struct Print {
    bool v;
    const ZtFieldFmt &fmt; // unused, but needed at compile-time
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << (print.v ? '1' : '0');
    }
  };
  inline static ZtVFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Bool, T, void, Props> :
    public ZtFieldType_Bool<T, Props> { };

template <typename T, typename Props>
struct ZtVFieldType_Bool : public ZtVFieldType {
  ZtVFieldType_Bool() : ZtVFieldType{
    .code = ZtFieldTypeCode::Bool,
    .props = ZtVFieldProp::Value<Props>{},
    .info = {.null = nullptr},
    .getFn = {.bool_ = [](const void *ptr) -> bool {
      return *(static_cast<const T *>(ptr));
    }},
    .setFn = {.bool_ = [](void *ptr, bool v) {
      *(static_cast<T *>(ptr)) = v;
    }},
    .print = [](const void *ptr, ZmStream &s, const ZtFieldFmt &fmt) {
      using Print = typename ZtFieldType_Bool<T, Props>::Print;
      s << Print{static_cast<bool>(*static_cast<const T *>(ptr)), fmt};
    },
    .scan = [](void *ptr, ZuString s, const ZtFieldFmt &fmt) {
      *static_cast<T *>(ptr) = s.length() == 1 && s[0] == '1';
    },
    .cmp = ZtFieldType_Cmp<T>::fn()
  } { }
};
template <typename T, typename Props>
ZtVFieldType *ZtFieldType_Bool<T, Props>::vtype() {
  return ZmSingleton<ZtVFieldType_Bool<T, Props>>::instance();
}

inline constexpr bool ZtField_Bool_Def() { return false; }
template <typename Base, typename Props, auto Def = ZtField_Bool_Def>
struct ZtField_Bool : public ZtField_<Base, Props> {
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Bool<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  using Print_ = typename Type::Print;
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Base::id() << '=' << Print_{Base::get(print.o), print.fmt};
    }
  };
  constexpr static auto deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    static T deflt_{Def()};
    static T null_{false};
    return [](int i) -> const void * {
      if (ZuLikely(i == Deflt)) return static_cast<const void *>(&deflt_);
      return static_cast<const void *>(&null_);
    };
  }
};

// --- Int

template <typename T_, typename Props_>
struct ZtFieldType_Int : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::Int };
  using T = T_;
  using Props = Props_;
  struct Print {
    ZuBox<int64_t> v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      if constexpr (ZuTypeIn<ZtFieldProp::Hex, Props>{})
	return s << print.v.vfmt(ZuVFmt{print.fmt.scalar}.hex());
      else
	return s << print.v.vfmt(print.fmt.scalar);
    }
  };
  inline static ZtVFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Int, T, void, Props> :
    public ZtFieldType_Int<T, Props> { };

template <typename T, typename Props>
struct ZtVFieldType_Int : public ZtVFieldType {
  ZtVFieldType_Int() : ZtVFieldType{
    .code = ZtFieldTypeCode::Int,
    .props = ZtVFieldProp::Value<Props>{},
    .info = {.null = nullptr},
    .getFn = {.int_ = [](const void *ptr) -> int64_t {
      return *(static_cast<const T *>(ptr));
    }},
    .setFn = {.int_ = [](void *ptr, int64_t v) {
      *(static_cast<T *>(ptr)) = v;
    }},
    .print = [](const void *ptr, ZmStream &s, const ZtFieldFmt &fmt) {
      using Print = typename ZtFieldType_Int<T, Props>::Print;
      s << Print{*static_cast<const T *>(ptr), fmt};
    },
    .scan = [](void *ptr, ZuString s, const ZtFieldFmt &fmt) {
      *static_cast<T *>(ptr) = ZuBoxT<T>{s};
    },
    .cmp = [](const void *p1, const void *p2) {
      return ZuCmp<T>::cmp(
	  *static_cast<const T *>(p1), *static_cast<const T *>(p2));
    }
  } { }
};
template <typename T, typename Props>
ZtVFieldType *ZtFieldType_Int<T, Props>::vtype() {
  return ZmSingleton<ZtVFieldType_Int<T, Props>>::instance();
}

template <typename T>
struct ZtFieldType_Int_Def {
  constexpr static auto deflt() { return ZuCmp<T>::null(); }
  constexpr static auto minimum() { return ZuCmp<T>::minimum(); }
  constexpr static auto maximum() { return ZuCmp<T>::maximum(); }
};
template <
  typename Base, typename Props,
  auto Def = ZtFieldType_Int_Def<typename Base::T>::deflt,
  auto Min = ZtFieldType_Int_Def<typename Base::T>::minimum,
  auto Max = ZtFieldType_Int_Def<typename Base::T>::maximum>
struct ZtField_Int : public ZtField_<Base, Props> {
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Int<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  using Print_ = typename Type::Print;
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Base::id() << '=' << Print_{Base::get(print.o), print.fmt};
    }
  };
  constexpr static auto deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    static T deflt_{Def()};
    static T min_{Min()};
    static T max_{Max()};
    static T null_{ZuCmp<T>::null()};
    return [](int i) -> const void * {
      switch (i) {
	case Deflt: return static_cast<const void *>(&deflt_);
	case Minimum: return static_cast<const void *>(&min_);
	case Maximum: return static_cast<const void *>(&max_);
      }
      return static_cast<const void *>(&null_);
    };
  }
};

// --- UInt

template <typename T_, typename Props_>
struct ZtFieldType_UInt : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::UInt };
  using T = T_;
  using Props = Props_;
  struct Print {
    ZuBox<uint64_t> v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      if constexpr (ZuTypeIn<ZtFieldProp::Hex, Props>{})
	return s << print.v.vfmt(ZuVFmt{print.fmt.scalar}.hex());
      else
	return s << print.v.vfmt(print.fmt.scalar);
    }
  };
  inline static ZtVFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::UInt, T, void, Props> :
    public ZtFieldType_UInt<T, Props> { };

template <typename T, typename Props>
struct ZtVFieldType_UInt : public ZtVFieldType {
  ZtVFieldType_UInt() : ZtVFieldType{
    .code = ZtFieldTypeCode::UInt,
    .props = ZtVFieldProp::Value<Props>{},
    .info = {.null = nullptr},
    .getFn = {.uint_ = [](const void *ptr) -> uint64_t {
      return *(static_cast<const T *>(ptr));
    }},
    .setFn = {.uint_ = [](void *ptr, uint64_t v) {
      *(static_cast<T *>(ptr)) = v;
    }},
    .print = [](const void *ptr, ZmStream &s, const ZtFieldFmt &fmt) {
      using Print = typename ZtFieldType_UInt<T, Props>::Print;
      s << Print{*static_cast<const T *>(ptr), fmt};
    },
    .scan = [](void *ptr, ZuString s, const ZtFieldFmt &fmt) {
      *static_cast<T *>(ptr) = ZuBoxT<T>{s};
    },
    .cmp = [](const void *p1, const void *p2) {
      return ZuCmp<T>::cmp(
	  *static_cast<const T *>(p1), *static_cast<const T *>(p2));
    }
  } { }
};
template <typename T, typename Props>
ZtVFieldType *ZtFieldType_UInt<T, Props>::vtype() {
  return ZmSingleton<ZtVFieldType_UInt<T, Props>>::instance();
}

template <
  typename Base, typename Props,
  auto Def = ZtFieldType_Int_Def<typename Base::T>::deflt,
  auto Min = ZtFieldType_Int_Def<typename Base::T>::minimum,
  auto Max = ZtFieldType_Int_Def<typename Base::T>::maximum>
struct ZtField_UInt : public ZtField_<Base, Props> {
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_UInt<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  using Print_ = typename Type::Print;
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Base::id() << '=' << Print_{Base::get(print.o), print.fmt};
    }
  };
  constexpr static auto deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    static T deflt_{Def()};
    static T min_{Min()};
    static T max_{Max()};
    static T null_{ZuCmp<T>::null()};
    return [](int i) -> const void * {
      switch (i) {
	case Deflt: return static_cast<const void *>(&deflt_);
	case Minimum: return static_cast<const void *>(&min_);
	case Maximum: return static_cast<const void *>(&max_);
      }
      return static_cast<const void *>(&null_);
    };
  }
};

// --- Enum

template <typename T_, typename Map_, typename Props_>
struct ZtFieldType_Enum : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::Enum };
  using T = T_;
  using Map = Map_;
  using Props = Props_;
  struct Print {
    int v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Map::v2s(print.v);
    }
  };
  inline static ZtVFieldType *vtype();
};
template <typename T, typename Map, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Enum, T, Map, Props> :
    public ZtFieldType_Enum<T, Props, Map> { };

template <typename T, typename Map, typename Props>
struct ZtVFieldType_Enum : public ZtVFieldType {
  ZtVFieldType_Enum() : ZtVFieldType{
    .code = ZtFieldTypeCode::Enum,
    .props = ZtVFieldProp::Value<Props>{},
    .info = {.enum_ = []() -> ZtVFieldEnum * {
      return ZtVFieldEnum_<Map>::instance();
    }},
    .getFn = {.enum_ = [](const void *ptr) -> int {
      return *(static_cast<const T *>(ptr));
    }},
    .setFn = {.enum_ = [](void *ptr, int v) {
      *(static_cast<T *>(ptr)) = v;
    }},
    .print = [](const void *ptr, ZmStream &s, const ZtFieldFmt &fmt) {
      using Print = typename ZtFieldType_Enum<T, Map, Props>::Print;
      s << Print{*static_cast<const T *>(ptr), fmt};
    },
    .scan = [](void *ptr, ZuString s, const ZtFieldFmt &fmt) {
      *static_cast<T *>(ptr) = Map::s2v(s);
    },
    .cmp = [](const void *p1, const void *p2) {
      return ZuCmp<int>::cmp( // intentionally using int comparison
	  *static_cast<const T *>(p1), *static_cast<const T *>(p2));
    }
  } { }
};
template <typename T, typename Map, typename Props>
ZtVFieldType *ZtFieldType_Enum<T, Map, Props>::vtype() {
  return ZmSingleton<ZtVFieldType_Enum<T, Map, Props>>::instance();
}

inline constexpr int ZtField_Enum_Def() { return -1; }
template <
  typename Base, typename Props, typename Map_,
  auto Def = ZtField_Enum_Def>
struct ZtField_Enum : public ZtField_<Base, Props> {
  using O = typename Base::O;
  using T = typename Base::T;
  using Map = Map_;
  using Type = ZtFieldType_Enum<T, Map, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  using Print_ = typename Type::Print;
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Base::id() << '=' << Print_{Base::get(print.o), print.fmt};
    }
  };
  constexpr static auto deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    static T deflt_{Def()};
    static T null_{ZuCmp<T>::null()};
    return [](int i) -> const void * {
      if (ZuLikely(i == Deflt)) return static_cast<const void *>(&deflt_);
      return static_cast<const void *>(&null_);
    };
  }
};

// --- Flags

template <typename T_, typename Map_, typename Props_>
struct ZtFieldType_Flags : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::Flags };
  using T = T_;
  using Map = Map_;
  using Props = Props_;
  struct Print {
    uint64_t v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Map::print(print.v, print.fmt.flagsDelim);
    }
  };
  inline static ZtVFieldType *vtype();
};
template <typename T, typename Map, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Flags, T, Map, Props> :
    public ZtFieldType_Flags<T, Props, Map> { };

template <typename T, typename Map, typename Props>
struct ZtVFieldType_Flags : public ZtVFieldType {
  ZtVFieldType_Flags() : ZtVFieldType{
    .code = ZtFieldTypeCode::Flags,
    .props = ZtVFieldProp::Value<Props>{},
    .info = {.flags_ = []() -> ZtVFieldFlags * {
      return ZtVFieldFlags_<Map>::instance();
    }},
    .getFn = {.flags = [](const void *ptr) -> uint64_t {
      return *(static_cast<const T *>(ptr));
    }},
    .setFn = {.flags = [](void *ptr, uint64_t v) {
      *(static_cast<T *>(ptr)) = v;
    }},
    .print = [](const void *ptr, ZmStream &s, const ZtFieldFmt &fmt) {
      using Print = typename ZtFieldType_Flags<T, Map, Props>::Print;
      s << Print{static_cast<uint64_t>(*static_cast<const T *>(ptr)), fmt};
    },
    .scan = [](void *ptr, ZuString s, const ZtFieldFmt &fmt) {
      *static_cast<T *>(ptr) = Map::template scan<T>(s, fmt.flagsDelim);
    },
    .cmp = [](const void *p1, const void *p2) {
      return ZuCmp<uint64_t>::cmp( // intentionally using uint64_t comparison
	  *static_cast<const T *>(p1), *static_cast<const T *>(p2));
    }
  } { }
};
template <typename T, typename Map, typename Props>
ZtVFieldType *ZtFieldType_Flags<T, Map, Props>::vtype() {
  return ZmSingleton<ZtVFieldType_Flags<T, Map, Props>>::instance();
}

inline constexpr int ZtField_Flags_Def() { return 0; }
template <
  typename Base, typename Props, typename Map_,
  auto Def = ZtField_Flags_Def>
struct ZtField_Flags : public ZtField_<Base, Props> {
  using O = typename Base::O;
  using T = typename Base::T;
  using Map = Map_;
  using Type = ZtFieldType_Flags<T, Map, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  using Print_ = typename Type::Print;
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Base::id() << '=' << Print_{Base::get(print.o), print.fmt};
    }
  };
  constexpr static auto deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    static T deflt_{Def()};
    static T null_{ZuCmp<T>::null()};
    return [](int i) -> const void * {
      if (ZuLikely(i == Deflt)) return static_cast<const void *>(&deflt_);
      return static_cast<const void *>(&null_);
    };
  }
};

// --- Float

template <typename T_, typename Props_>
struct ZtFieldType_Float : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::Float };
  using T = T_;
  using Props = Props_;
  struct Print {
    ZuBox<double> v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      constexpr int NDP = ZtFieldProp::GetNDP<Props>{};
      if constexpr (NDP >= 0)
	return s << print.v.vfmt(print.fmt.scalar).fp(-NDP);
      else
	return s << print.v.vfmt(print.fmt.scalar);
    }
  };
  inline static ZtVFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Float, T, void, Props> :
    public ZtFieldType_Float<T, Props> { };

template <typename T, typename Props>
struct ZtVFieldType_Float : public ZtVFieldType {
  ZtVFieldType_Float() : ZtVFieldType{
    .code = ZtFieldTypeCode::Float,
    .props = ZtVFieldProp::Value<Props>{},
    .info = {.null = nullptr},
    .getFn = {.float_ = [](const void *ptr) -> double {
      return *(static_cast<const T *>(ptr));
    }},
    .setFn = {.float_ = [](void *ptr, double v) {
      *(static_cast<T *>(ptr)) = v;
    }},
    .print = [](const void *ptr, ZmStream &s, const ZtFieldFmt &fmt) {
      using Print = typename ZtFieldType_Float<T, Props>::Print;
      s << Print{*static_cast<const T *>(ptr), fmt};
    },
    .scan = [](void *ptr, ZuString s, const ZtFieldFmt &fmt) {
      *static_cast<T *>(ptr) = ZuBoxT<T>{s};
    },
    .cmp = [](const void *p1, const void *p2) {
      return ZuCmp<T>::cmp(
	  *static_cast<const T *>(p1), *static_cast<const T *>(p2));
    }
  } { }
};
template <typename T, typename Props>
ZtVFieldType *ZtFieldType_Float<T, Props>::vtype() {
  return ZmSingleton<ZtVFieldType_Float<T, Props>>::instance();
}

template <typename T>
struct ZtField_Float_Def {
  constexpr static auto deflt() { return ZuCmp<T>::null(); }
  constexpr static auto minimum() { return -ZuFP<T>::inf(); }
  constexpr static auto maximum() { return ZuFP<T>::inf(); }
};
template <
  typename Base, typename Props,
  auto Def = ZtField_Float_Def<typename Base::T>::deflt,
  auto Min = ZtField_Float_Def<typename Base::T>::minimum,
  auto Max = ZtField_Float_Def<typename Base::T>::maximum>
struct ZtField_Float : public ZtField_<Base, Props> {
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Float<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  using Print_ = typename Type::Print;
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Base::id() << '=' << Print_{Base::get(print.o), print.fmt};
    }
  };
  constexpr static auto deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    static T deflt_{Def()};
    static T min_{Min()};
    static T max_{Max()};
    static T null_{ZuCmp<T>::null()};
    return [](int i) -> const void * {
      switch (i) {
	case Deflt: return static_cast<const void *>(&deflt_);
	case Minimum: return static_cast<const void *>(&min_);
	case Maximum: return static_cast<const void *>(&max_);
      }
      return static_cast<const void *>(&null_);
    };
  }
};

// --- Fixed

template <typename T_, typename Props_>
struct ZtFieldType_Fixed : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::Fixed };
  using T = T_;
  using Props = Props_;
  struct Print {
    ZuFixed v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      constexpr int NDP = ZtFieldProp::GetNDP<Props>{};
      if constexpr (NDP >= 0)
	return s << print.v.vfmt(print.fmt.scalar).fp(-NDP);
      else
	return s << print.v.vfmt(print.fmt.scalar);
    }
  };
  inline static ZtVFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Fixed, T, void, Props> :
    public ZtFieldType_Fixed<T, Props> { };

template <typename T, typename Props>
struct ZtVFieldType_Fixed : public ZtVFieldType {
  ZtVFieldType_Fixed() : ZtVFieldType{
    .code = ZtFieldTypeCode::Fixed,
    .props = ZtVFieldProp::Value<Props>{},
    .info = {.null = nullptr},
    .getFn = {.fixed = [](const void *ptr) -> ZuFixed {
      return *(static_cast<const T *>(ptr));
    }},
    .setFn = {.fixed = [](void *ptr, ZuFixed v) {
      *(static_cast<T *>(ptr)) = v;
    }},
    .print = [](const void *ptr, ZmStream &s, const ZtFieldFmt &fmt) {
      using Print = typename ZtFieldType_Fixed<T, Props>::Print;
      s << Print{*static_cast<const T *>(ptr), fmt};
    },
    .scan = [](void *ptr, ZuString s, const ZtFieldFmt &fmt) {
      // preserve exponent
      unsigned exponent;
      if constexpr (ZuConversion<T, ZuFixed>::Same)
	exponent = static_cast<T *>(ptr)->exponent();
      else
	exponent = ZuFixed{*static_cast<T *>(ptr)}.exponent();
      *static_cast<T *>(ptr) = ZuFixed{s, exponent};
    },
    .cmp = [](const void *p1, const void *p2) {
      return ZuCmp<T>::cmp(
	  *static_cast<const T *>(p1), *static_cast<const T *>(p2));
    }
  } { }
};
template <typename T, typename Props>
ZtVFieldType *ZtFieldType_Fixed<T, Props>::vtype() {
  return ZmSingleton<ZtVFieldType_Fixed<T, Props>>::instance();
}

struct ZtField_Fixed_Def {
  constexpr static ZuFixed deflt() { return {}; }
  constexpr static ZuFixed minimum() { return {ZuFixedMin, 0}; }
  constexpr static ZuFixed maximum() { return {ZuFixedMax, 0}; }
};
template <
  typename Base, typename Props,
  auto Def = ZtField_Fixed_Def::deflt,
  auto Min = ZtField_Fixed_Def::minimum,
  auto Max = ZtField_Fixed_Def::maximum,
  bool = Base::ReadOnly>
struct ZtField_Fixed : public ZtField_<Base, Props> {
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Fixed<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  using Print_ = typename Type::Print;
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Base::id() << '=' << Print_{Base::get(print.o), print.fmt};
    }
  };
  constexpr static auto deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    static T deflt_{Def()};
    static T min_{Min()};
    static T max_{Max()};
    static T null_{ZuCmp<T>::null()};
    return [](int i) -> const void * {
      switch (i) {
	case Deflt: return static_cast<const void *>(&deflt_);
	case Minimum: return static_cast<const void *>(&min_);
	case Maximum: return static_cast<const void *>(&max_);
      }
      return static_cast<const void *>(&null_);
    };
  }
};

// --- Decimal

template <typename T_, typename Props_>
struct ZtFieldType_Decimal : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::Decimal };
  using T = T_;
  using Props = Props_;
  struct Print {
    ZuDecimal v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      constexpr int NDP = ZtFieldProp::GetNDP<Props>{};
      if constexpr (NDP >= 0)
	return s << print.v.vfmt(print.fmt.scalar).fp(-NDP);
      else
	return s << print.v.vfmt(print.fmt.scalar);
    }
  };
  inline static ZtVFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Decimal, T, void, Props> :
    public ZtFieldType_Decimal<T, Props> { };

template <typename T, typename Props>
struct ZtVFieldType_Decimal : public ZtVFieldType {
  ZtVFieldType_Decimal() : ZtVFieldType{
    .code = ZtFieldTypeCode::Decimal,
    .props = ZtVFieldProp::Value<Props>{},
    .info = {.null = nullptr},
    .getFn = {.decimal = [](const void *ptr) -> ZuDecimal {
      return *(static_cast<const T *>(ptr));
    }},
    .setFn = {.decimal = [](void *ptr, ZuDecimal v) {
      *(static_cast<T *>(ptr)) = v;
    }},
    .print = [](const void *ptr, ZmStream &s, const ZtFieldFmt &fmt) {
      using Print = typename ZtFieldType_Decimal<T, Props>::Print;
      s << Print{*static_cast<const T *>(ptr), fmt};
    },
    .scan = [](void *ptr, ZuString s, const ZtFieldFmt &fmt) {
      *static_cast<T *>(ptr) = s;
    },
    .cmp = [](const void *p1, const void *p2) {
      return ZuCmp<T>::cmp(
	  *static_cast<const T *>(p1), *static_cast<const T *>(p2));
    }
  } { }
};
template <typename T, typename Props>
ZtVFieldType *ZtFieldType_Decimal<T, Props>::vtype() {
  return ZmSingleton<ZtVFieldType_Decimal<T, Props>>::instance();
}

struct ZtField_Decimal_Def {
  constexpr static ZuDecimal deflt() {
    return ZuCmp<ZuDecimal>::null();
  }
  constexpr static ZuDecimal minimum() {
    return {ZuDecimal::Unscaled, ZuDecimal::minimum()};
  }
  constexpr static ZuDecimal maximum() {
    return {ZuDecimal::Unscaled, ZuDecimal::maximum()};
  }
};
template <
  typename Base, typename Props,
  auto Def = ZtField_Decimal_Def::deflt,
  auto Min = ZtField_Decimal_Def::minimum,
  auto Max = ZtField_Decimal_Def::maximum,
  bool = Base::ReadOnly>
struct ZtField_Decimal : public ZtField_<Base, Props> {
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Decimal<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  using Print_ = typename Type::Print;
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Base::id() << '=' << Print_{Base::get(print.o), print.fmt};
    }
  };
  constexpr static auto deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    static T deflt_{Def()};
    static T min_{Min()};
    static T max_{Max()};
    static T null_{ZuCmp<T>::null()};
    return [](int i) -> const void * {
      switch (i) {
	case Deflt: return static_cast<const void *>(&deflt_);
	case Minimum: return static_cast<const void *>(&min_);
	case Maximum: return static_cast<const void *>(&max_);
      }
      return static_cast<const void *>(&null_);
    };
  }
};

// --- Time

template <typename T_, typename Props_>
struct ZtFieldType_Time : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::Time };
  using T = T_;
  using Props = Props_;
  struct Print {
    ZmTime v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      ZtDate v{print.v};
      return s << v.print(print.fmt.datePrint);
    }
  };
  inline static ZtVFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Time, T, void, Props> :
    public ZtFieldType_Time<T, Props> { };

template <typename T, typename Props>
struct ZtVFieldType_Time : public ZtVFieldType {
  ZtVFieldType_Time() : ZtVFieldType{
    .code = ZtFieldTypeCode::Time,
    .props = ZtVFieldProp::Value<Props>{},
    .info = {.null = nullptr},
    .getFn = {.time = [](const void *ptr) -> ZmTime {
      return *(static_cast<const T *>(ptr));
    }},
    .setFn = {.time = [](void *ptr, ZmTime v) {
      *(static_cast<T *>(ptr)) = v;
    }},
    .print = [](const void *ptr, ZmStream &s, const ZtFieldFmt &fmt) {
      using Print = typename ZtFieldType_Time<T, Props>::Print;
      s << Print{*static_cast<const T *>(ptr), fmt};
    },
    .scan = [](void *ptr, ZuString s, const ZtFieldFmt &fmt) {
      *static_cast<T *>(ptr) = ZtDate{fmt.dateScan, s}.zmTime();
    },
    .cmp = [](const void *p1, const void *p2) {
      return ZuCmp<T>::cmp(
	  *static_cast<const T *>(p1), *static_cast<const T *>(p2));
    }
  } { }
};
template <typename T, typename Props>
ZtVFieldType *ZtFieldType_Time<T, Props>::vtype() {
  return ZmSingleton<ZtVFieldType_Time<T, Props>>::instance();
}

inline constexpr ZmTime ZtFieldType_Time_Def() { return {}; }
template <
  typename Base, typename Props,
  auto Def = ZtFieldType_Time_Def,
  bool = Base::ReadOnly>
struct ZtField_Time : public ZtField_<Base, Props> {
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Time<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  using Print_ = typename Type::Print;
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Base::id() << '=' << Print_{Base::get(print.o), print.fmt};
    }
  };
  constexpr static auto deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    static T deflt_{Def()};
    static T null_{ZuCmp<T>::null()};
    return [](int i) -> const void * {
      if (ZuLikely(i == Deflt)) return static_cast<const void *>(&deflt_);
      return static_cast<const void *>(&null_);
    };
  }
};

#define ZtField_BaseID__(ID, ...) ID
#define ZtField_BaseID_(Axor, ...) ZuPP_Defer(ZtField_BaseID__)Axor
#define ZtField_BaseID(Base) ZuPP_Defer(ZtField_BaseID_)Base

#define ZtField_TypeName_(Name, ...) Name
#define ZtField_TypeName(Type) ZuPP_Defer(ZtField_TypeName_)Type
#define ZtField_LambdaArg(Arg) []{ return Arg; }

#define ZtField_TypeArgs_String(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Bytes(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_UDT(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Bool(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Int(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_UInt(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Enum(Map, ...) \
  Map __VA_OPT__(, ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__))
#define ZtField_TypeArgs_Flags(Map, ...) \
  Map __VA_OPT__(, ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__))
#define ZtField_TypeArgs_Float(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Fixed(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Decimal(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Time(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)

#define ZtField_TypeArgs_(Name, ...) \
  __VA_OPT__(, ZtField_TypeArgs_##Name(__VA_ARGS__))
#define ZtField_TypeArgs(Type) ZuPP_Defer(ZtField_TypeArgs_)Type

#define ZtFieldTypeName(O, ID) ZtField_##O##_##ID

#define ZtField_Decl_4(O, ID, Base, TypeName, Type) \
  ZuField_Decl(O, Base) \
  using ZtFieldTypeName(O, ID) = \
  ZtField_##TypeName<ZuFieldTypeName(O, ID), \
      ZuTypeList<> ZtField_TypeArgs(Type)>;
#define ZtField_Decl_5(O, ID, Base, TypeName, Type, Props) \
  ZuField_Decl(O, Base) \
  using ZtFieldTypeName(O, ID) = \
  ZtField_##TypeName<ZuFieldTypeName(O, ID), \
      ZuTypeList<ZtField_Props(Props)> ZtField_TypeArgs(Type)>;
#define ZtField_Decl_N(O, _0, _1, _2, _3, _4, Fn, ...) Fn
#define ZtField_Decl__(O, ...) \
  ZtField_Decl_N(O, __VA_ARGS__, \
      ZtField_Decl_5(O, __VA_ARGS__), \
      ZtField_Decl_4(O, __VA_ARGS__))
#define ZtField_Decl_(O, Base, Type, ...) \
  ZuPP_Defer(ZtField_Decl__)(O, \
      ZuPP_Nest(ZtField_BaseID(Base)), Base, \
      ZuPP_Nest(ZtField_TypeName(Type)), Type __VA_OPT__(,) __VA_ARGS__)
#define ZtField_Decl(O, Args) ZuPP_Defer(ZtField_Decl_)(O, ZuPP_Strip(Args))

#define ZtField_Type_(O, Base, ...) \
  ZuPP_Defer(ZtFieldTypeName)(O, ZuPP_Nest(ZtField_BaseID(Base)))
#define ZtField_Type(O, Args) ZuPP_Defer(ZtField_Type_)(O, ZuPP_Strip(Args))

#define ZtFields(O, ...)  \
  namespace ZuFields_ { \
    ZuPP_Eval(ZuPP_MapArg(ZtField_Decl, O, __VA_ARGS__)) \
    using O = \
      ZuTypeList<ZuPP_Eval(ZuPP_MapArgComma(ZtField_Type, O, __VA_ARGS__))>; \
  } \
  O *ZuFielded_(O *); \
  ZuFields_::O ZuFieldList_(O *)

struct ZtFieldPrint : public ZuPrintDelegate {
  template <typename U> struct Print_Filter :
      public ZuBool<!ZuTypeIn<ZtFieldProp::Hidden, typename U::Props>{}> { };
  template <typename S, typename O>
  static void print(S &s, const O &o) {
    using FieldList = ZuTypeGrep<Print_Filter, ZuFieldList<O>>;
    thread_local ZtFieldFmt fmt;
    s << '{';
    ZuTypeAll<FieldList>::invoke([&o, &s]<typename Field>() {
      if constexpr (ZuTypeIndex<Field, FieldList>{}) s << ' ';
      s << typename Field::Print{o, fmt};
    });
    s << '}';
  }
};

struct ZtVField {
  ZtVFieldType		*type;
  const char		*id;
  uint32_t		props;			// ZtVFieldProp
  uint64_t		keys;

  const void *		(*constantFn)(int);
  void			(*getFn)(const void *, ZmFn<const void *>);
  void			(*setFn)(void *, ZmFn<void *>);

  template <typename Field>
  ZtVField(Field) :
      type{Field::Type::vtype()},
      id{Field::id()},
      props{Field::vprops()},
      keys{Field::keys()},
      constantFn{Field::constantFn()},
      getFn{Field::getFn()},
      setFn{Field::setFn()} { }

  template <unsigned I, typename L>
  void get(const void *o, L &&l) const {
    using L_ = decltype(ZuFwd<L>(l));
    struct Context {
      ZtVFieldType *type;
      L_ l_;
    } context{this->type, ZuFwd<L>(l)};
    getFn(o, {&context, [](Context *context, const void *p) {
      context->type->template get<I>(p, ZuFwd<L_>(context->l_));
    }});
  }

  template <unsigned I, typename U>
  void set(void *o, U &&v) const {
    using U_ = decltype(ZuFwd<U>(v));
    struct Context {
      ZtVFieldType *type;
      U_ v_;
    } context{this->type, ZuFwd<U>(v)};
    setFn(o, {&context, [](Context *context, void *p) {
      context->type->template set<I>(p, ZuFwd<U_>(context->v_));
    }});
  }

  // need to de-conflict with print
  template <typename S> void print_(S &s) const {
    s << "id=" << id << " type=" << ZtFieldTypeCode::name(type->code);
    unsigned props_ = props & ZtVFieldProp::Mask;
    props_ &= ~(ZtVFieldProp::Ctor_ | ZtVFieldProp::NDP_);
    s << " props=" << ZtVFieldProp::Map::print(props_);
    if (props & ZtVFieldProp::Ctor_) {
      if (props_) s << '|';
      s << "Ctor(" << ZtVFieldProp::getCtor(props) << ')';
    }
    if (props & ZtVFieldProp::NDP_) {
      if (props & ~ZtVFieldProp::NDP_) s << '|';
      s << "NDP(" << ZtVFieldProp::getNDP(props) << ')';
    }
    s << " keys=" << ZuBoxed(keys).hex();
  }
  friend ZuPrintLambda<[]() {
    return [](auto &s, const auto &v) { v.print_(s); };
  }> ZuPrintType(ZtVField *);
};

using ZtVFieldArray = ZuArray<const ZtVField *>;

template <typename VField, typename ...Fields>
struct ZtVFields_ {
  static ZuArray<const VField *> fields() {
    enum { N = sizeof...(Fields) };
    static const VField fields_[N] =
      // std::initializer_list<ZtVField>
    {
      VField{Fields{}}...
    };
    static const ZtVField *ptr_[N];
    ZuUnroll::all<N>([](auto i) {
      ptr_[i] = static_cast<const ZtVField *>(&fields_[i]);
    });
    return {&ptr_[0], N};
  }
};
template <typename O, typename VField = ZtVField>
inline ZuArray<const VField *> ZtVFields() {
  return ZuTypeApply<
    ZtVFields_, typename ZuFieldList<O>::template Prepend<VField>>::fields();
}

#endif /* ZtField_HPP */
