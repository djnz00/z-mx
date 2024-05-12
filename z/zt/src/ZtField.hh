//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// object introspection
// * layered on ZuField
// * compile-time (ZtField*) and run-time (ZtMField*)
// * print/scan (CSV, etc.)
// * ORM
// * data series
// * ... any other application that needs to introspect structured data

// metadata macro DSL for identifying and using data fields and keys
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
// Meaning: This is a read-only string field named "id" with a default
//   value of "default" that is also the containing object's zeroth
//   constructor parameter
//
// ZtField Type	C/C++ Type	ZtField Args
// ------------	----------	------------
// CString	char *		[, default]
// String	<String>	[, default]
// Bytes	<uint8_t[]>	[, default]
// Bool		<Integral>	[, default]
// Int		<Integral>	[, default, min, max]
// UInt		<Integral>	[, default, min, max]
// Enum, Map	<Integral>	[, default]
// Flags, Map	<Integral>	[, default]
// Float	<FloatingPoint>	[, default, min, max]
// Fixed	ZuFixed		[, default, min, max]
// Decimal	ZuDecimal	[, default, min, max]
// Time		ZuTime		[, default]
// DateTime	ZuDateTime	[, default]
// UDT		<UDT>		[, default]

// Note: Regarding run-time introspection with monomorphic fields (ZtMField),
// virtual polymorphism and RTTI are intentionally avoided:
// 1] if ZtMField were virtually polymorphic, passing it to dynamically
//    loaded libraries (e.g. data store adapters performing serdes) would
//    entail a far more complex type hierarchy with diamond-shaped
//    inheritance, use of dynamic_cast, etc.
// 2] ZtMField (and derived classes) benefit from being POD
// 3] very little syntactic benefit would be obtained

// ZuField<O> is extended to provide:
//   Type	- ZtFieldType<...>
//   deflt()	- canonical default value
//   minimum()	- minimum value (for scalars)
//   maximum()	- maximum value ('')
//
// ZtFieldType is keyed on <Code, T, Map, Props>, provides:
//   Code	- type code
//   T		- underlying type
//   Map	- map (only defined for Enum and Flags)
//   Props	- properties type list
//   Print	- Print{const T &, const ZtFieldFmt &} - formatted printing
//   vtype()	- ZtMFieldType * instance
// 
// ZtMFieldType provides:
//   code	- ZtFieldTypeCode
//   props	- ZtMFieldProp
//   info	- enum / flags / UDT metadata
//
// ZtMField{ZtField{}} instantiates ZtMField from ZtField
//
// ZtMField provides:
//   type	- ZtMFieldType * instance
//   id		- ZtField::id()
//   props	- ZtMFieldProp properties bitfield
//   keys	- ZtField::keys()
//   get	- ZtMFieldGet
//   set	- ZtMFieldSet
//   constant	- ZtMFieldGet for constants (default, minimum, maximum)
//   cget	- cast ZtMFieldConstant to const void * for ZtMFieldGet
//
// ZtMFieldGet provides:
//   get<Code>(const void *o)
//   print<Code>(auto &s, const void *o, const ZtMField *, const ZtFieldFmt &)
//
// ZtMFieldSet provides:
//   set<Code>(void *o, auto &&v)
//   scan<Code>(void *o, ZuString s, const ZtMField *, const ZtFieldFmt &)
//
// ZtMFieldList<O>() returns the ZtMFields for O
// ZtMKeyFieldList<O>() returns the ZtMKeyFields for O
// ZtMKeyFieldList<O>()[KeyID] == ZtMFieldList<ZuFieldKeyT<O, KeyID>>()

#ifndef ZtField_HH
#define ZtField_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZtLib_HH
#include <zlib/ZtLib.hh>
#endif

#include <string.h>

#include <typeinfo>

#include <zlib/ZuPrint.hh>
#include <zlib/ZuBox.hh>
#include <zlib/ZuArray.hh>
#include <zlib/ZuString.hh>
#include <zlib/ZuFixed.hh>
#include <zlib/ZuDecimal.hh>
#include <zlib/ZuUnroll.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuField.hh>
#include <zlib/ZuUnroll.hh>
#include <zlib/ZuDateTime.hh>

#include <zlib/ZmStream.hh>
#include <zlib/ZuTime.hh>
#include <zlib/ZmSingleton.hh>

#include <zlib/ZtEnum.hh>
#include <zlib/ZtString.hh>
#include <zlib/ZtHexDump.hh>
#include <zlib/ZtScanBool.hh>

namespace ZtFieldTypeCode {
  ZtEnumValues(ZtFieldTypeCode,
    CString,		// C UTF-8 string (raw pointer), heap-allocated
    String,		// C++ contiguous UTF-8 string
    Bytes,		// byte array
    Bool,		// an integral type, interpreted as bool
    Int,		// an integral type <= 64bits
    UInt,		// an unsigned integral type <= 64bits
    Enum,		// an integral enumerated type
    Flags,		// an integral enumerated bitfield type
    Float,		// floating point type
    Fixed,		// ZuFixed
    Decimal,		// ZuDecimal
    Time,		// ZuTime - POSIX timespec
    DateTime,		// ZuDateTime - Julian date, seconds, nanoseconds
    UDT			// generic udt type
  );
}

// compile-time field property list - a typelist of individual properties:
// - each type is declared in the ZtFieldProp namespace
// - additional properties can be injected into the ZtFieldProp namespace
namespace ZtFieldProp {
  struct Synthetic { };		// synthetic (implies read-only)
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
  ZuDateTimeScan::Any	dateScan;		// date/time scan format
  ZuDateTimeFmt::Any	datePrint;		// date/time print format
  char			flagsDelim = '|';	// flags delimiter
};

// type properties are a subset of field properties
template <typename Prop> struct ZtFieldType_Props : public ZuFalse { };
template <> struct ZtFieldType_Props<ZtFieldProp::Hidden> : public ZuTrue { };
template <> struct ZtFieldType_Props<ZtFieldProp::Quote> : public ZuTrue { };
template <> struct ZtFieldType_Props<ZtFieldProp::Hex> : public ZuTrue { };

// ZtMFieldProp bitfield encapsulates introspected ZtField properties
namespace ZtMFieldProp {
  ZtEnumFlags(ZtMFieldProp,
    Synthetic,	//   1
    Update,	//   2
    Hidden,	//   4
    Quote,	//   8
    Hex,	//  10
    Required,	//  20
    Ctor_,	//  40
    NDP_,	//  80
    Series,	// 100
    Index,	// 200
    Delta,	// 400
    Delta2);	// 800

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
  inline constexpr unsigned getCtor(unsigned props) {
    return (props>>CtorShift) & CtorMask;
  }
  // NDP -> flags
  inline constexpr unsigned NDP(unsigned i) {
    return NDP_ | (i<<NDPShift);
  }
  inline constexpr unsigned getNDP(unsigned props) {
    return (props>>NDPShift) & NDPMask;
  }

  // Value<Prop>::N - return bitfield for individual property
  template <typename> struct Value : public ZuUnsigned<0> { };	// default

  template <unsigned I>
  struct Value<ZuUnsigned<I>> : public ZuUnsigned<I> { };	// reduced

  namespace _ = ZtFieldProp;

  template <> struct Value<_::Synthetic> : public ZuUnsigned<Synthetic> { };
  template <> struct Value<_::Update>    : public ZuUnsigned<Update> { };
  template <> struct Value<_::Hidden>    : public ZuUnsigned<Hidden> { };
  template <> struct Value<_::Quote>     : public ZuUnsigned<Quote> { };
  template <> struct Value<_::Hex>       : public ZuUnsigned<Hex> { };
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
  template <typename U> struct Or_<U> {
    using T = Value<U>;
  };
  template <typename L, typename R> struct Or_<L, R> {
    using T = ZuUnsigned<unsigned(Value<L>{}) | unsigned(Value<R>{})>;
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
  constexpr static uint64_t mprops() {
    return ZtMFieldProp::Value<Props>{};
  }
};
// Map is void if neither Enum nor Flags
template <int Code, typename T, typename Map, typename Props>
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

// ZtMFieldEnum encapsulates introspected enum metadata
struct ZtMFieldEnum {
  const char	*(*id)();
  int		(*s2v)(ZuString);
  ZuString	(*v2s)(int);
};
template <typename Map>
struct ZtMFieldEnum_ : public ZtMFieldEnum {
  ZtMFieldEnum_() : ZtMFieldEnum{
    .id = []() -> const char * { return Map::id(); },
    .s2v = [](ZuString s) -> int { return Map::s2v(s); },
    .v2s = [](int i) -> ZuString { return Map::v2s(i); }
  } { }

  static ZtMFieldEnum *instance() {
    return ZmSingleton<ZtMFieldEnum_>::instance();
  }
};

// ZtMFieldFlags encapsulates introspected flags metadata
struct ZtMFieldFlags {
  const char	*(*id)();
  void		(*print)(uint64_t, ZmStream &, const ZtFieldFmt &);
  uint64_t	(*scan)(ZuString, const ZtFieldFmt &);
};
template <typename Map>
struct ZtMFieldFlags_ : public ZtMFieldFlags {
  ZtMFieldFlags_() : ZtMFieldFlags{
    .id = []() -> const char * { return Map::id(); },
    .print = [](uint64_t v, ZmStream &s, const ZtFieldFmt &fmt) -> void {
      s << Map::print(v, fmt.flagsDelim);
    },
    .scan = [](ZuString s, const ZtFieldFmt &fmt) -> uint64_t {
      return Map::template scan<uint64_t>(s, fmt.flagsDelim);
    }
  } { }

  static ZtMFieldFlags *instance() {
    return ZmSingleton<ZtMFieldFlags_>::instance();
  }
};

typedef void (*ZtMFieldPrint)(const void *, ZmStream &, const ZtFieldFmt &);
typedef void (*ZtMFieldScan)(
  void (*)(void *, unsigned, const void *), void *, unsigned,
  ZuString, const ZtFieldFmt &);

// ZtMFieldUDT encapsulates introspected UDT metadata
struct ZtMFieldUDT {
  const std::type_info	*info;
  ZtMFieldPrint		print;
  ZtMFieldScan		scan;
};

// ZtMFieldType encapsulates introspected type metadata
struct ZtMFieldType {
  int			code;		// ZtFieldTypeCode
  uint32_t		props;		// ZtMFieldProp

  union {
    void		*null;
    ZtMFieldEnum *	(*enum_)();	// Enum
    ZtMFieldFlags *	(*flags)();	// Flags
    ZtMFieldUDT *	(*udt)();	// UDT
  } info;
};

// ZtMFieldConstant is used to retrieve field constants
namespace ZtMFieldConstant {
  enum { Null = 0, Deflt, Minimum, Maximum };
}

// monomorphic equivalent of ZtField
struct ZtMField;

// C string quoting
template <bool Quote>
struct ZtFieldType_CString_Print {
  const char *v;
  const ZtFieldFmt &fmt; // unused, but needed at compile-time
  template <typename S, bool Quote_ = Quote>
  friend ZuIfT<Quote_, S &> operator <<(
      S &s, const ZtFieldType_CString_Print &print) {
    const char *v = print.v;
    s << '"';
    if (v)
      for (unsigned i = 0; v[i]; i++) {
	char c = v[i];
	if (ZuUnlikely(c == '"')) s << '\\';
	s << c;
      }
    return s << '"';
  }
  template <typename S, bool Quote_ = Quote>
  friend ZuIfT<!Quote_, S &> operator <<(
      S &s, const ZtFieldType_CString_Print &print) {
    const char *v = print.v;
    s << '"';
    if (v) s << v;
    return s << '"';
  }
};

// C++ string (span) quoting
template <bool Quote>
struct ZtFieldType_String_Print {
  ZuString v;
  const ZtFieldFmt &fmt; // unused, but needed at compile-time
  template <typename S, bool Quote_ = Quote>
  friend ZuIfT<Quote_, S &> operator <<(
      S &s, const ZtFieldType_String_Print &print) {
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
  friend ZuIfT<!Quote_, S &> operator <<(
      S &s, const ZtFieldType_String_Print &print) {
    s << '"';
    s << print.v;
    return s << '"';
  }
};

// monomorphic field get/print
struct ZtMFieldGet {
  union {
    void		*null;
    const char *	(*cstring)(const void *, unsigned);	// CString
    ZuString		(*string)(const void *, unsigned);	// String
    ZuBytes		(*bytes)(const void *, unsigned);	// Bytes
    bool		(*bool_)(const void *, unsigned);	// Bool
    int64_t		(*int_)(const void *, unsigned);	// Int
    uint64_t		(*uint)(const void *, unsigned);	// UInt
    int			(*enum_)(const void *, unsigned);	// Enum
    uint64_t		(*flags)(const void *, unsigned);	// Flags
    double		(*float_)(const void *, unsigned);	// Float
    ZuFixed		(*fixed)(const void *, unsigned);	// Fixed
    ZuDecimal		(*decimal)(const void *, unsigned);	// Decimal
    ZuTime		(*time)(const void *, unsigned);	// Time
    ZuDateTime		(*dateTime)(const void *, unsigned);	// DateTime
    const void *	(*udt)(const void *, unsigned);		// UDT
  } get_;

  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::CString, const char *>
  get(const void *o, unsigned i) const {
    return get_.cstring(o, i);
  }
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::String, ZuString>
  get(const void *o, unsigned i) const {
    return get_.string(o, i);
  }
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Bytes, ZuBytes>
  get(const void *o, unsigned i) const {
    return get_.bytes(o, i);
  }
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Bool, bool>
  get(const void *o, unsigned i) const {
    return get_.bool_(o, i);
  }
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Int, int64_t>
  get(const void *o, unsigned i) const {
    return get_.int_(o, i);
  }
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::UInt, uint64_t>
  get(const void *o, unsigned i) const {
    return get_.uint(o, i);
  }
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Enum, int>
  get(const void *o, unsigned i) const {
    return get_.enum_(o, i);
  }
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Flags, uint64_t>
  get(const void *o, unsigned i) const {
    return get_.flags(o, i);
  }
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Float, double>
  get(const void *o, unsigned i) const {
    return get_.float_(o, i);
  }
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Fixed, ZuFixed>
  get(const void *o, unsigned i) const {
    return get_.fixed(o, i);
  }
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Decimal, ZuDecimal>
  get(const void *o, unsigned i) const {
    return get_.decimal(o, i);
  }
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Time, ZuTime>
  get(const void *o, unsigned i) const {
    return get_.time(o, i);
  }
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::DateTime, ZuDateTime>
  get(const void *o, unsigned i) const {
    return get_.dateTime(o, i);
  }
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::UDT, const void *>
  get(const void *o, unsigned i) const {
    return get_.udt(o, i);
  }

  template <unsigned Code, typename S>
  ZuIfT<Code == ZtFieldTypeCode::CString>
  print(S &, const void *, unsigned, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code, typename S>
  ZuIfT<Code == ZtFieldTypeCode::String>
  print(S &, const void *, unsigned, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code, typename S>
  ZuIfT<Code == ZtFieldTypeCode::Bytes>
  print(S &, const void *, unsigned, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code, typename S>
  ZuIfT<Code == ZtFieldTypeCode::Bool>
  print(S &, const void *, unsigned, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code, typename S>
  ZuIfT<Code == ZtFieldTypeCode::Int>
  print(S &, const void *, unsigned, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code, typename S>
  ZuIfT<Code == ZtFieldTypeCode::UInt>
  print(S &, const void *, unsigned, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code, typename S>
  ZuIfT<Code == ZtFieldTypeCode::Enum>
  print(S &, const void *, unsigned, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code, typename S>
  ZuIfT<Code == ZtFieldTypeCode::Flags>
  print(S & s_, const void *, unsigned, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code, typename S>
  ZuIfT<Code == ZtFieldTypeCode::Float>
  print(S &, const void *, unsigned, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code, typename S>
  ZuIfT<Code == ZtFieldTypeCode::Fixed>
  print(S &, const void *, unsigned, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code, typename S>
  ZuIfT<Code == ZtFieldTypeCode::Decimal>
  print(S &, const void *, unsigned, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code, typename S>
  ZuIfT<Code == ZtFieldTypeCode::Time>
  print(S &, const void *, unsigned, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code, typename S>
  ZuIfT<Code == ZtFieldTypeCode::DateTime>
  print(S &, const void *, unsigned, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code, typename S>
  ZuIfT<Code == ZtFieldTypeCode::UDT>
  print(S & s_, const void *, unsigned, const ZtMField *, const ZtFieldFmt &)
    const;
};

// monomorphic field set/scan
struct ZtMFieldSet {
  union {
    void		*null;
    void		(*cstring)(void *, unsigned, const char *);// CString
    void		(*string)(void *, unsigned, ZuString);	// String
    void		(*bytes)(void *, unsigned, ZuBytes);	// Bytes
    void		(*bool_)(void *, unsigned, bool);	// Bool
    void		(*int_)(void *, unsigned, int64_t);	// Int
    void		(*uint)(void *, unsigned, uint64_t);	// UInt
    void		(*enum_)(void *, unsigned, int);	// Enum
    void		(*flags)(void *, unsigned, uint64_t);	// Flags
    void		(*float_)(void *, unsigned, double);	// Float
    void		(*fixed)(void *, unsigned, ZuFixed);	// Fixed
    void		(*decimal)(void *, unsigned, ZuDecimal);// Decimal
    void		(*time)(void *, unsigned, ZuTime);	// Time
    void		(*dateTime)(void *, unsigned, ZuDateTime);	// DateTime
    void		(*udt)(void *, unsigned, const void *);	// UDT
  } set_;

  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::CString>
  set(void *o, unsigned i, const char *v) const {
    set_.cstring(o, i, v);
  }
  template <unsigned Code, typename U>
  ZuIfT<Code == ZtFieldTypeCode::String>
  set(void *o, unsigned i, U &&v) const {
    set_.string(o, i, ZuString(ZuFwd<U>(v))); // not ZuString{}
  }
  template <unsigned Code, typename U>
  ZuIfT<Code == ZtFieldTypeCode::Bytes>
  set(void *o, unsigned i, U &&v) const {
    set_.bytes(o, i, ZuBytes(ZuFwd<U>(v))); // not ZuBytes{}
  }
  template <unsigned Code, typename U>
  ZuIfT<Code == ZtFieldTypeCode::Bool> set(void *o, unsigned i, U &&v) const {
    set_.bool_(o, i, ZuFwd<U>(v));
  }
  template <unsigned Code, typename U>
  ZuIfT<Code == ZtFieldTypeCode::Int> set(void *o, unsigned i, U &&v) const {
    set_.int_(o, i, ZuFwd<U>(v));
  }
  template <unsigned Code, typename U>
  ZuIfT<Code == ZtFieldTypeCode::UInt> set(void *o, unsigned i, U &&v) const {
    set_.uint(o, i, ZuFwd<U>(v));
  }
  template <unsigned Code, typename U>
  ZuIfT<Code == ZtFieldTypeCode::Enum> set(void *o, unsigned i, U &&v) const {
    set_.enum_(o, i, ZuFwd<U>(v));
  }
  template <unsigned Code, typename U>
  ZuIfT<Code == ZtFieldTypeCode::Flags>
  set(void *o, unsigned i, U &&v) const {
    set_.flags(o, i, ZuFwd<U>(v));
  }
  template <unsigned Code, typename U>
  ZuIfT<Code == ZtFieldTypeCode::Float>
  set(void *o, unsigned i, U &&v) const {
    set_.float_(o, i, ZuFwd<U>(v));
  }
  template <unsigned Code, typename U>
  ZuIfT<Code == ZtFieldTypeCode::Fixed>
  set(void *o, unsigned i, U &&v) const {
    set_.fixed(o, i, ZuFixed{ZuFwd<U>(v)});
  }
  template <unsigned Code, typename U>
  ZuIfT<Code == ZtFieldTypeCode::Decimal>
  set(void *o, unsigned i, U &&v) const {
    set_.decimal(o, i, ZuDecimal{ZuFwd<U>(v)});
  }
  template <unsigned Code, typename U>
  ZuIfT<Code == ZtFieldTypeCode::Time> set(void *o, unsigned i, U &&v) const {
    set_.time(o, i, ZuTime{ZuFwd<U>(v)});
  }
  template <unsigned Code, typename U>
  ZuIfT<Code == ZtFieldTypeCode::DateTime>
  set(void *o, unsigned i, U &&v) const {
    set_.dateTime(o, i, ZuDateTime{ZuFwd<U>(v)});
  }
  template <unsigned Code, typename U>
  ZuIfT<Code == ZtFieldTypeCode::UDT>
  set(void *o, unsigned i, const U &v) const {
    set_.udt(o, i, static_cast<const void *>(&v));
  }

  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::CString>
  scan(void *, unsigned, ZuString, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::String>
  scan(void *, unsigned, ZuString, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Bytes>
  scan(void *, unsigned, ZuString, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Bool>
  scan(void *, unsigned, ZuString, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Int>
  scan(void *, unsigned, ZuString, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::UInt>
  scan(void *, unsigned, ZuString, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Enum>
  scan(void *, unsigned, ZuString, const ZtMField *field, const ZtFieldFmt &)
    const;
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Flags>
  scan(void *, unsigned, ZuString, const ZtMField *field, const ZtFieldFmt &)
    const;
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Float>
  scan(void *, unsigned, ZuString, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Fixed>
  scan(void *, unsigned, ZuString, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Decimal>
  scan(void *, unsigned, ZuString, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::Time>
  scan(void *, unsigned, ZuString, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::DateTime>
  scan(void *, unsigned, ZuString, const ZtMField *, const ZtFieldFmt &)
    const;
  template <unsigned Code>
  ZuIfT<Code == ZtFieldTypeCode::UDT>
  scan(void *, unsigned, ZuString, const ZtMField *field, const ZtFieldFmt &)
    const;
};

// ZtMField is the monomorphic equivalent of ZtField
struct ZtMField {
  ZtMFieldType		*type;
  const char		*id;
  uint32_t		props;			// ZtMFieldProp
  uint64_t		keys;

  ZtMFieldGet		get;
  ZtMFieldSet		set;

  ZtMFieldGet		constant;

  template <typename Field>
  ZtMField(Field) :
      type{Field::Type::vtype()},
      id{Field::id()},
      props{Field::mprops()},
      keys{Field::keys()},
      get{Field::getFn()},
      set{Field::setFn()},
      constant{Field::constantFn()} { }

  // parameter to ZtMFieldGet::get()
  static const void *cget(int c) {
    return reinterpret_cast<void *>(static_cast<uintptr_t>(c));
  }

  // need to de-conflict with print
  template <typename S> void print_(S &s) const {
    s << "id=" << id << " type=" << ZtFieldTypeCode::name(type->code);
    unsigned props_ = props & ZtMFieldProp::Mask;
    props_ &= ~(ZtMFieldProp::Ctor_ | ZtMFieldProp::NDP_);
    s << " props=" << ZtMFieldProp::Map::print(props_);
    if (props & ZtMFieldProp::Ctor_) {
      if (props_) s << '|';
      s << "Ctor(" << ZtMFieldProp::getCtor(props) << ')';
    }
    if (props & ZtMFieldProp::NDP_) {
      if (props & ~ZtMFieldProp::NDP_) s << '|';
      s << "NDP(" << ZtMFieldProp::getNDP(props) << ')';
    }
    s << " keys=" << ZuBoxed(keys).hex();
  }
  friend ZuPrintLambda<[]() {
    return [](auto &s, const auto &v) { v.print_(s); };
  }> ZuPrintType(ZtMField *);
};

// ZtMFieldGet print functions
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::CString> ZtMFieldGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldFmt &fmt
) const {
  auto v = get_.cstring(o, i);
  if (field->props & ZtMFieldProp::Quote)
    s << ZtFieldType_CString_Print<true>{v, fmt};
  else
    s << ZtFieldType_CString_Print<false>{v, fmt};
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::String> ZtMFieldGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldFmt &fmt
) const {
  auto v = get_.string(o, i);
  if (field->props & ZtMFieldProp::Quote)
    s << ZtFieldType_String_Print<true>{v, fmt};
  else
    s << ZtFieldType_String_Print<false>{v, fmt};
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Bytes> ZtMFieldGet::
  print(S &s, const void *o, unsigned i, const ZtMField *, const ZtFieldFmt &)
    const {
  s << ZtHexDump_{get_.bytes(o, i)};
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Bool> ZtMFieldGet::
  print(S &s, const void *o, unsigned i, const ZtMField *, const ZtFieldFmt &)
    const {
  s << (get_.bool_(o, i) ? '1' : '0');
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Int> ZtMFieldGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldFmt &fmt
) const {
  ZuBox<int64_t> v = get_.int_(o, i);
  if (field->props & ZtMFieldProp::Hex)
    s << v.vfmt(fmt.scalar).hex();
  else
    s << v.vfmt(fmt.scalar);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::UInt> ZtMFieldGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldFmt &fmt
) const {
  ZuBox<uint64_t> v = get_.uint(o, i);
  if (field->props & ZtMFieldProp::Hex)
    s << v.vfmt(fmt.scalar).hex();
  else
    s << v.vfmt(fmt.scalar);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Enum> ZtMFieldGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field, const ZtFieldFmt &)
    const {
  s << field->type->info.enum_()->v2s(get_.enum_(o, i));
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Flags> ZtMFieldGet::print(
  S &s_, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldFmt &fmt
) const {
  ZmStream s{s_};
  field->type->info.flags()->print(get_.flags(o, i), s, fmt);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Float> ZtMFieldGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldFmt &fmt
) const {
  ZuBox<double> v = get_.float_(o, i);
  int ndp = ZtMFieldProp::getNDP(field->props);
  if (ndp >= 0)
    s << v.vfmt(fmt.scalar).fp(-ndp);
  else
    s << v.vfmt(fmt.scalar);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Fixed> ZtMFieldGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldFmt &fmt
) const {
  ZuFixed v = get_.fixed(o, i);
  int ndp = ZtMFieldProp::getNDP(field->props);
  if (ndp >= 0)
    s << v.vfmt(fmt.scalar).fp(-ndp);
  else
    s << v.vfmt(fmt.scalar);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Decimal> ZtMFieldGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldFmt &fmt
) const {
  ZuDecimal v = get_.decimal(o, i);
  int ndp = ZtMFieldProp::getNDP(field->props);
  if (ndp >= 0)
    s << v.vfmt(fmt.scalar).fp(-ndp);
  else
    s << v.vfmt(fmt.scalar);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Time> ZtMFieldGet::print(
  S &s, const void *o, unsigned i, const ZtMField *, const ZtFieldFmt &fmt
) const {
  ZuDateTime v{get_.time(o, i)};
  s << v.print(fmt.datePrint);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::DateTime> ZtMFieldGet::print(
  S &s, const void *o, unsigned i, const ZtMField *, const ZtFieldFmt &fmt
) const {
  ZuDateTime v{get_.dateTime(o, i)};
  s << v.print(fmt.datePrint);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::UDT> ZtMFieldGet::print(
  S &s_, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldFmt &fmt
) const {
  ZmStream s{s_};
  field->type->info.udt()->print(get_.udt(o, i), s, fmt);
}

// ZtMFieldSet scan functions
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::CString> ZtMFieldSet::
  scan(void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldFmt &)
    const {
  if (!s) {
    set_.cstring(o, i, nullptr);
    return;
  }
  auto n = s.length();
  auto ptr = static_cast<char *>(::malloc(n + 1));
  if (ptr) {
    memcpy(ptr, s.data(), n);
    ptr[n] = 0;
  }
  set_.cstring(o, i, ptr);
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::String> ZtMFieldSet::
  scan(void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldFmt &)
    const {
  set_.string(o, i, s);
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Bytes> ZtMFieldSet::
  scan(void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldFmt &)
    const {
  set_.bytes(
    o, i, ZuBytes{reinterpret_cast<const uint8_t *>(s.data()), s.length()}
  );
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Bool> ZtMFieldSet::
  scan(void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldFmt &)
    const {
  set_.bool_(o, i, ZtScanBool(s));
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Int> ZtMFieldSet::
  scan(void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldFmt &)
    const {
  set_.int_(o, i, ZuBox<int64_t>{s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::UInt> ZtMFieldSet::
  scan(void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldFmt &)
    const {
  set_.int_(o, i, ZuBox<uint64_t>{s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Enum> ZtMFieldSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *field, const ZtFieldFmt &)
    const {
  set_.enum_(o, i, field->type->info.enum_()->s2v(s));
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Flags> ZtMFieldSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *field,
  const ZtFieldFmt &fmt
) const {
  set_.flags(o, i, field->type->info.flags()->scan(s, fmt));
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Float> ZtMFieldSet::
  scan(void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldFmt &)
    const {
  set_.float_(o, i, ZuBox<double>{s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Fixed> ZtMFieldSet::
  scan(void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldFmt &)
    const {
  set_.fixed(o, i, ZuFixed{s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Decimal> ZtMFieldSet::
  scan(void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldFmt &)
    const {
  set_.decimal(o, i, ZuDecimal{s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Time> ZtMFieldSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldFmt &fmt
) const {
  set_.time(o, i, ZuDateTime{fmt.dateScan, s}.as_zuTime());
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::DateTime> ZtMFieldSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldFmt &fmt
) const {
  set_.dateTime(o, i, ZuDateTime{fmt.dateScan, s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::UDT> ZtMFieldSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *field,
  const ZtFieldFmt &fmt
) const {
  field->type->info.udt()->scan(field->set.set_.udt, o, i, s, fmt);
}

// ZtField_ compile-time encapsulates an individual field, derives from ZuField
template <typename Base_, typename Props_>
struct ZtField_ : public Base_ {
  using Base = Base_;
  using Orig = Base;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = Props_;
  constexpr static uint64_t mprops() {
    return ZtMFieldProp::Value<Props>{};
  }
};

// --- CString

template <typename T, typename Props>
struct ZtFieldType_CString;
template <typename Props_>
struct ZtFieldType_CString<char *, Props_> : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::CString };
  using Props = Props_;
  using Print =
    ZtFieldType_CString_Print<ZuTypeIn<ZtFieldProp::Quote, Props>{}>;
  inline static ZtMFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::CString, T, void, Props> :
    public ZtFieldType_CString<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_CString;
template <typename Props>
struct ZtMFieldType_CString<char *, Props> : public ZtMFieldType {
  using T = char *;
  ZtMFieldType_CString() : ZtMFieldType{
    .code = ZtFieldTypeCode::CString,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename Props>
ZtMFieldType *ZtFieldType_CString<char *, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_CString<char *, Props>>::instance();
}

inline const char *ZtField_CString_Def() { return nullptr; }
template <
  typename Base, typename Props,
  auto Def = ZtField_CString_Def,
  bool = Base::ReadOnly>
struct ZtField_CString : public ZtField_<Base, Props> {
  template <template <typename> typename Override>
  using Adapt = ZtField_CString<Override<Base>, Props>;
  using O = typename Base::O;
  using T = char *;
  using Type =
    ZtFieldType_CString<char *, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.cstring = [](const void *o, unsigned) -> const char * {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.cstring = [](void *, unsigned, const char *) { }}};
  }
  static const char *deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.cstring = [](const void *o, unsigned) -> const char * {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt: return Def();
	default:    return nullptr;
      }
    }}};
  }
};
template <typename Base, typename Props, auto Def>
struct ZtField_CString<Base, Props, Def, false> :
    public ZtField_CString<Base, Props, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.set_ = {.cstring = [](void *o_, unsigned, const char *s) {
      O &o = *static_cast<const O *>(o_);
      auto ptr = Base::get(o);
      if (ptr) ::free(ptr);
      Base::set(o, s ? strdup(s) : static_cast<const char *>(nullptr));
    }}};
  }
};

// --- String

template <typename T_, typename Props_>
struct ZtFieldType_String : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::String };
  using T = T_;
  using Props = Props_;
  using Print =
    ZtFieldType_String_Print<ZuTypeIn<ZtFieldProp::Quote, Props>{}>;
  inline static ZtMFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::String, T, void, Props> :
    public ZtFieldType_String<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_String : public ZtMFieldType {
  ZtMFieldType_String() : ZtMFieldType{
    .code = ZtFieldTypeCode::String,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_String<T, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_String<T, Props>>::instance();
}

inline ZuString ZtField_String_Def() { return {}; }
template <typename Base, typename = void>
struct ZtField_String_Get {
  static ZtMFieldGet getFn() {
    using O = typename Base::O;
    // field get() returns a temporary
    return {.get_ = {.string = [](const void *o, unsigned) -> ZuString {
      auto &v = ZmTLS<ZtString, getFn>();
      v = Base::get(*static_cast<const O *>(o));
      return v;
    }}};
  }
};
template <typename Base>
struct ZtField_String_Get<Base,
    decltype(&Base::get(ZuDeclVal<const typename Base::O &>()), void())> {
  static ZtMFieldGet getFn() {
    using O = typename Base::O;
    // field get() returns a crvalue
    return {.get_ = {.string = [](const void *o, unsigned) -> ZuString {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
};
template <
  typename Base, typename Props,
  auto Def = ZtField_String_Def,
  bool = Base::ReadOnly>
struct ZtField_String :
    public ZtField_<Base, Props>,
    public ZtField_String_Get<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_String<Override<Base>, Props>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_String<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldSet setFn() {
    return {.set_ = {.string = [](void *, ZuString) { }}};
  }
  static ZuString deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.string = [](const void *o, unsigned) -> ZuString {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt: return Def();
	default:    return {};
      }
    }}};
  }
};
template <typename Base, typename Props, auto Def>
struct ZtField_String<Base, Props, Def, false> :
    public ZtField_String<Base, Props, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.set_ = {.string = [](void *o, unsigned, ZuString s) {
      Base::set(*static_cast<O *>(o), s);
    }}};
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
  inline static ZtMFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Bytes, T, void, Props> :
    public ZtFieldType_Bytes<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_Bytes : public ZtMFieldType {
  ZtMFieldType_Bytes() : ZtMFieldType{
    .code = ZtFieldTypeCode::Bytes,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_Bytes<T, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_Bytes<T, Props>>::instance();
}

inline ZuBytes ZtField_Bytes_Def() { return {}; }
template <typename Base, typename = void>
struct ZtField_Bytes_Get {
  static ZtMFieldGet getFn() {
    using O = typename Base::O;
    // field get() returns a temporary
    return {.get_ = {.bytes = [](const void *o, unsigned) -> ZuBytes {
      auto &v = ZmTLS<ZtBytes, getFn>();
      v = Base::get(*static_cast<const O *>(o));
      return v;
    }}};
  }
};
template <typename Base>
struct ZtField_Bytes_Get<Base,
    decltype(&Base::get(ZuDeclVal<const typename Base::O &>()), void())> {
  static ZtMFieldGet getFn() {
    using O = typename Base::O;
    // field get() returns a crvalue
    return {.get_ = {.bytes = [](const void *o, unsigned) -> ZuBytes {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
};
template <
  typename Base, typename Props,
  auto Def = ZtField_Bytes_Def,
  bool = Base::ReadOnly>
struct ZtField_Bytes :
    public ZtField_<Base, Props>,
    public ZtField_Bytes_Get<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Bytes<Override<Base>, Props>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Bytes<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldSet setFn() {
    return {.set_ = {.bytes = [](void *, ZuBytes) { }}};
  }
  static ZuBytes deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.bytes = [](const void *o, unsigned) -> ZuBytes {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt: return Def();
	default:    return {};
      }
    }}};
  }
};
template <typename Base, typename Props, auto Def>
struct ZtField_Bytes<Base, Props, Def, false> :
    public ZtField_Bytes<Base, Props, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.set_ = {.bytes = [](void *o, unsigned, ZuBytes v) {
      Base::set(*static_cast<O *>(o), v);
    }}};
  }
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
  inline static ZtMFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Bool, T, void, Props> :
    public ZtFieldType_Bool<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_Bool : public ZtMFieldType {
  ZtMFieldType_Bool() : ZtMFieldType{
    .code = ZtFieldTypeCode::Bool,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_Bool<T, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_Bool<T, Props>>::instance();
}

inline constexpr bool ZtField_Bool_Def() { return false; }
template <
  typename Base, typename Props,
  auto Def = ZtField_Bool_Def,
  bool = Base::ReadOnly>
struct ZtField_Bool : public ZtField_<Base, Props> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Bool<Override<Base>, Props>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Bool<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.bool_ = [](const void *o, unsigned) -> bool {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.bool_ = [](void *, bool) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.bool_ = [](const void *o, unsigned) -> bool {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	case Minimum: return false;
	case Maximum: return true;
	default:      return false;
      }
    }}};
  }
};
template <typename Base, typename Props, auto Def>
struct ZtField_Bool<Base, Props, Def, false> :
    public ZtField_Bool<Base, Props, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.set_ = {.bool_ = [](void *o, unsigned, bool v) {
      Base::set(*static_cast<O *>(o), v);
    }}};
  }
};

// --- Int

template <typename T_, typename Props_>
struct ZtFieldType_Int : public ZtFieldType_<Props_> {
  ZuAssert(ZuTraits<T_>::IsIntegral);
  ZuAssert(ZuTraits<T_>::IsSigned);
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
  inline static ZtMFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Int, T, void, Props> :
    public ZtFieldType_Int<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_Int : public ZtMFieldType {
  ZtMFieldType_Int() : ZtMFieldType{
    .code = ZtFieldTypeCode::Int,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_Int<T, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_Int<T, Props>>::instance();
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
  auto Max = ZtFieldType_Int_Def<typename Base::T>::maximum,
  bool = Base::ReadOnly>
struct ZtField_Int : public ZtField_<Base, Props> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Int<Override<Base>, Props>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Int<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.int_ = [](const void *o, unsigned) -> int64_t {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.int_ = [](void *, unsigned, int64_t) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.int_ = [](const void *o, unsigned) -> int64_t {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	case Minimum: return Min();
	case Maximum: return Max();
	default:      return ZuCmp<int64_t>::null();
      }
    }}};
  }
};
template <typename Base, typename Props, auto Def, auto Min, auto Max>
struct ZtField_Int<Base, Props, Def, Min, Max, false> :
    public ZtField_Int<Base, Props, Def, Min, Max, true> {
  using O = typename Base::O;
  using T = typename Base::T;
  static ZtMFieldSet setFn() {
    return {.set_ = {.int_ = [](void *o, unsigned, int64_t v) {
      Base::set(*static_cast<O *>(o), v);
    }}};
  }
};

// --- UInt

template <typename T_, typename Props_>
struct ZtFieldType_UInt : public ZtFieldType_<Props_> {
  ZuAssert(ZuTraits<T_>::IsIntegral);
  ZuAssert(!ZuTraits<T_>::IsSigned);
  enum { Code = ZtFieldTypeCode::UInt };
  using T = T_;
  using Props = Props_;
  struct Print {
    ZuBox<uint64_t> v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      if constexpr (ZuTypeIn<ZtFieldProp::Hex, Props>{})
	return s << print.v.vfmt(print.fmt.scalar).hex();
      else
	return s << print.v.vfmt(print.fmt.scalar);
    }
  };
  inline static ZtMFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::UInt, T, void, Props> :
    public ZtFieldType_UInt<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_UInt : public ZtMFieldType {
  ZtMFieldType_UInt() : ZtMFieldType{
    .code = ZtFieldTypeCode::UInt,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_UInt<T, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_UInt<T, Props>>::instance();
}

template <
  typename Base, typename Props,
  auto Def = ZtFieldType_Int_Def<typename Base::T>::deflt,
  auto Min = ZtFieldType_Int_Def<typename Base::T>::minimum,
  auto Max = ZtFieldType_Int_Def<typename Base::T>::maximum,
  bool = Base::ReadOnly>
struct ZtField_UInt : public ZtField_<Base, Props> {
  template <template <typename> typename Override>
  using Adapt = ZtField_UInt<Override<Base>, Props>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_UInt<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.uint = [](const void *o, unsigned) -> uint64_t {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.uint = [](void *, unsigned, uint64_t) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.uint = [](const void *o, unsigned) -> uint64_t {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	case Minimum: return Min();
	case Maximum: return Max();
	default:      return ZuCmp<uint64_t>::null();
      }
    }}};
  }
};
template <typename Base, typename Props, auto Def, auto Min, auto Max>
struct ZtField_UInt<Base, Props, Def, Min, Max, false> :
    public ZtField_UInt<Base, Props, Def, Min, Max, true> {
  using O = typename Base::O;
  using T = typename Base::T;
  static ZtMFieldSet setFn() {
    return {.set_ = {.uint = [](void *o, unsigned, uint64_t v) {
      Base::set(*static_cast<O *>(o), v);
    }}};
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
    template <typename U>
    Print(U v_) : v{int(v_)} { }
    template <typename U>
    Print(U v_, const ZtFieldFmt &fmt_) : v{int(v_)}, fmt{fmt_} { }
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Map::v2s(print.v);
    }
  };
  inline static ZtMFieldType *vtype();
};
template <typename T, typename Map, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Enum, T, Map, Props> :
    public ZtFieldType_Enum<T, Props, Map> { };

template <typename T, typename Map, typename Props>
struct ZtMFieldType_Enum : public ZtMFieldType {
  ZtMFieldType_Enum() : ZtMFieldType{
    .code = ZtFieldTypeCode::Enum,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.enum_ = []() -> ZtMFieldEnum * {
      return ZtMFieldEnum_<Map>::instance();
    }}
  } { }
};
template <typename T, typename Map, typename Props>
ZtMFieldType *ZtFieldType_Enum<T, Map, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_Enum<T, Map, Props>>::instance();
}

inline constexpr int ZtField_Enum_Def() { return -1; }
template <
  typename Base, typename Props, typename Map,
  auto Def = ZtField_Enum_Def,
  bool = Base::ReadOnly>
struct ZtField_Enum : public ZtField_<Base, Props> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Enum<Override<Base>, Props, Map>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Enum<T, Map, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.enum_ = [](const void *o, unsigned) -> int {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.enum_ = [](void *, unsigned, int) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.enum_ = [](const void *o, unsigned) -> int {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return -1;
      }
    }}};
  }
};
template <typename Base, typename Props, typename Map, auto Def>
struct ZtField_Enum<Base, Props, Map, Def, false> :
    public ZtField_Enum<Base, Props, Map, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.set_ = {.enum_ = [](void *o, unsigned, int v) {
      Base::set(*static_cast<O *>(o), v);
    }}};
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
  inline static ZtMFieldType *vtype();
};
template <typename T, typename Map, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Flags, T, Map, Props> :
    public ZtFieldType_Flags<T, Props, Map> { };

template <typename T, typename Map, typename Props>
struct ZtMFieldType_Flags : public ZtMFieldType {
  ZtMFieldType_Flags() : ZtMFieldType{
    .code = ZtFieldTypeCode::Flags,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.flags = []() -> ZtMFieldFlags * {
      return ZtMFieldFlags_<Map>::instance();
    }}
  } { }
};
template <typename T, typename Map, typename Props>
ZtMFieldType *ZtFieldType_Flags<T, Map, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_Flags<T, Map, Props>>::instance();
}

inline constexpr int ZtField_Flags_Def() { return 0; }
template <
  typename Base, typename Props, typename Map,
  auto Def = ZtField_Flags_Def,
  bool = Base::ReadOnly>
struct ZtField_Flags : public ZtField_<Base, Props> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Flags<Override<Base>, Props, Map>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Flags<T, Map, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.flags = [](const void *o, unsigned) -> uint64_t {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.flags = [](void *, unsigned, uint64_t) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.flags = [](const void *o, unsigned) -> uint64_t {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return 0;
      }
    }}};
  }
};
template <typename Base, typename Props, typename Map, auto Def>
struct ZtField_Flags<Base, Props, Map, Def, false> :
    public ZtField_Flags<Base, Props, Map, Def, true> {
  using O = typename Base::O;
  using T = typename Base::T;
  static ZtMFieldSet setFn() {
    return {.set_ = {.flags = [](void *o, unsigned, uint64_t v) {
      Base::set(*static_cast<O *>(o), v);
    }}};
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
  inline static ZtMFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Float, T, void, Props> :
    public ZtFieldType_Float<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_Float : public ZtMFieldType {
  ZtMFieldType_Float() : ZtMFieldType{
    .code = ZtFieldTypeCode::Float,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_Float<T, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_Float<T, Props>>::instance();
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
  auto Max = ZtField_Float_Def<typename Base::T>::maximum,
  bool = Base::ReadOnly>
struct ZtField_Float : public ZtField_<Base, Props> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Float<Override<Base>, Props>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Float<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.float_ = [](const void *o, unsigned) -> double {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.float_ = [](void *, unsigned, double) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.float_ = [](const void *o, unsigned) -> double {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	case Minimum: return Min();
	case Maximum: return Max();
	default:      return ZuCmp<double>::null();
      }
    }}};
  }
};
template <typename Base, typename Props, auto Def, auto Min, auto Max>
struct ZtField_Float<Base, Props, Def, Min, Max, false> :
    public ZtField_Float<Base, Props, Def, Min, Max, true> {
  using O = typename Base::O;
  using T = typename Base::T;
  static ZtMFieldSet setFn() {
    return {.set_ = {.float_ = [](void *o, unsigned, double v) {
      Base::set(*static_cast<O *>(o), v);
    }}};
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
  inline static ZtMFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Fixed, T, void, Props> :
    public ZtFieldType_Fixed<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_Fixed : public ZtMFieldType {
  ZtMFieldType_Fixed() : ZtMFieldType{
    .code = ZtFieldTypeCode::Fixed,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_Fixed<T, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_Fixed<T, Props>>::instance();
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
  template <template <typename> typename Override>
  using Adapt = ZtField_Fixed<Override<Base>, Props>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Fixed<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.fixed = [](const void *o, unsigned) -> ZuFixed {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.fixed = [](void *, unsigned, ZuFixed) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.fixed = [](const void *o, unsigned) -> ZuFixed {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	case Minimum: return Min();
	case Maximum: return Max();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, typename Props, auto Def, auto Min, auto Max>
struct ZtField_Fixed<Base, Props, Def, Min, Max, false> :
    public ZtField_Fixed<Base, Props, Def, Min, Max, true> {
  using O = typename Base::O;
  using T = typename Base::T;
  static ZtMFieldSet setFn() {
    return {.set_ = {.fixed = [](void *o, unsigned, ZuFixed v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
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
  inline static ZtMFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Decimal, T, void, Props> :
    public ZtFieldType_Decimal<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_Decimal : public ZtMFieldType {
  ZtMFieldType_Decimal() : ZtMFieldType{
    .code = ZtFieldTypeCode::Decimal,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_Decimal<T, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_Decimal<T, Props>>::instance();
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
  template <template <typename> typename Override>
  using Adapt = ZtField_Decimal<Override<Base>, Props>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Decimal<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.decimal = [](const void *o, unsigned) -> ZuDecimal {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.decimal = [](void *, unsigned, ZuDecimal) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.decimal = [](const void *o, unsigned) -> ZuDecimal {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	case Minimum: return Min();
	case Maximum: return Max();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, typename Props, auto Def, auto Min, auto Max>
struct ZtField_Decimal<Base, Props, Def, Min, Max, false> :
    public ZtField_Decimal<Base, Props, Def, Min, Max, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.set_ = {.decimal = [](void *o, unsigned, ZuDecimal v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
  }
};

// --- Time

template <typename T_, typename Props_>
struct ZtFieldType_Time : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::Time };
  using T = T_;
  using Props = Props_;
  struct Print {
    ZuTime v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      ZuDateTime v{print.v};
      return s << v.print(print.fmt.datePrint);
    }
  };
  inline static ZtMFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Time, T, void, Props> :
    public ZtFieldType_Time<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_Time : public ZtMFieldType {
  ZtMFieldType_Time() : ZtMFieldType{
    .code = ZtFieldTypeCode::Time,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_Time<T, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_Time<T, Props>>::instance();
}

inline constexpr ZuTime ZtFieldType_Time_Def() { return {}; }
template <
  typename Base, typename Props,
  auto Def = ZtFieldType_Time_Def,
  bool = Base::ReadOnly>
struct ZtField_Time : public ZtField_<Base, Props> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Time<Override<Base>, Props>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Time<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.time = [](const void *o, unsigned) -> ZuTime {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.time = [](void *, unsigned, ZuTime) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.time = [](const void *o, unsigned) -> ZuTime {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, typename Props, auto Def>
struct ZtField_Time<Base, Props, Def, false> :
    public ZtField_Time<Base, Props, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.set_ = {.time = [](void *o, unsigned, ZuTime v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
  }
};

// --- DateTime

template <typename T_, typename Props_>
struct ZtFieldType_DateTime : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::Time };
  using T = T_;
  using Props = Props_;
  struct Print {
    ZuDateTime v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << print.v.print(print.fmt.datePrint);
    }
  };
  inline static ZtMFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::DateTime, T, void, Props> :
    public ZtFieldType_DateTime<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_DateTime : public ZtMFieldType {
  ZtMFieldType_DateTime() : ZtMFieldType{
    .code = ZtFieldTypeCode::DateTime,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_DateTime<T, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_DateTime<T, Props>>::instance();
}

inline constexpr ZuDateTime ZtFieldType_DateTime_Def() { return {}; }
template <
  typename Base, typename Props,
  auto Def = ZtFieldType_DateTime_Def,
  bool = Base::ReadOnly>
struct ZtField_DateTime : public ZtField_<Base, Props> {
  template <template <typename> typename Override>
  using Adapt = ZtField_DateTime<Override<Base>, Props>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_DateTime<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.dateTime = [](const void *o, unsigned) -> ZuDateTime {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.dateTime = [](void *, unsigned, ZuDateTime) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.dateTime = [](const void *o, unsigned) -> ZuDateTime {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, typename Props, auto Def>
struct ZtField_DateTime<Base, Props, Def, false> :
    public ZtField_DateTime<Base, Props, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.set_ = {.dateTime = [](void *o, unsigned, ZuDateTime v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
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
  inline static ZtMFieldType *vtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::UDT, T, void, Props> :
    public ZtFieldType_UDT<T, Props> { };

template <typename T, typename = void>
struct ZtMFieldType_UDT_Print {
  static auto printFn() {
    return [](const void *, ZmStream &, const ZtFieldFmt &) { };
  }
};
template <typename T>
struct ZtMFieldType_UDT_Print<T,
  decltype((ZuDeclVal<ZmStream &>() << ZuDeclVal<const T &>()), void())> {
  static auto printFn() {
    return [](const void *v, ZmStream &s, const ZtFieldFmt &) {
      s << *reinterpret_cast<const T *>(v);
    };
  }
};
template <typename T, typename = void>
struct ZtMFieldType_UDT_Scan {
  static auto scanFn() {
    return [](
      void (*)(void *, unsigned, const void *), void *, unsigned,
      ZuString, const ZtFieldFmt &) { };
  }
};
template <typename T>
struct ZtMFieldType_UDT_Scan<
  T, decltype((ZuDeclVal<T &>() = ZuString{}), void())
> {
  static auto scanFn() {
    return [](
      void (*set)(void *, unsigned, const void *), void *o, unsigned i,
      ZuString s, const ZtFieldFmt &
    ) {
      T v{s};
      set(o, i, reinterpret_cast<const void *>(&v));
    };
  }
};
template <typename T, typename Props>
struct ZtMFieldType_UDT : public ZtMFieldType {
  ZtMFieldType_UDT() : ZtMFieldType{
    .code = ZtFieldTypeCode::UDT,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.udt = []() -> ZtMFieldUDT * {
      static ZtMFieldUDT info{
	.info = &typeid(T),
	.print = ZtMFieldType_UDT_Print<T>::printFn(),
	.scan = ZtMFieldType_UDT_Scan<T>::scanFn()
      };
      return &info;
    }}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_UDT<T, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_UDT<T, Props>>::instance();
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
template <typename, auto, typename = void>
struct ZtField_UDT_Constant {
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.udt = [](const void *o, unsigned) -> const void * {
      return nullptr;
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_UDT_Constant<Base, Def,
    decltype(typename Base::T{Def()}, void())> {
  static ZtMFieldGet constantFn() {
    using T = typename Base::T;
    using namespace ZtMFieldConstant;
    return {.get_ = {.udt = [](const void *o, unsigned) -> const void * {
      static T deflt_{Def()};
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt: return static_cast<const void *>(&deflt_);
	default:    return ZtField_UDT_Null<T>::value();
      }
    }}};
  }
};
template <typename Base, typename = void>
struct ZtField_UDT_Get {
  static ZtMFieldGet getFn() {
    using O = typename Base::O;
    using T = typename Base::T;
    // field get() returns a temporary
    return {.get_ = {.udt = [](const void *o, unsigned) -> const void * {
      auto &v = ZmTLS<T, getFn>();
      v = Base::get(*static_cast<const O *>(o));
      return static_cast<const void *>(&v);
    }}};
  }
};
template <typename Base>
struct ZtField_UDT_Get<Base,
    decltype(&Base::get(ZuDeclVal<const typename Base::O &>()), void())> {
  static ZtMFieldGet getFn() {
    using O = typename Base::O;
    // field get() returns a crvalue
    return {.get_ = {.udt = [](const void *o, unsigned) -> const void * {
      return static_cast<const void *>(&Base::get(*static_cast<const O *>(o)));
    }}};
  }
};
template <
  typename Base, typename Props,
  auto Def = ZtField_UDT_Def<typename Base::T>::value,
  bool = Base::ReadOnly>
struct ZtField_UDT :
    public ZtField_<Base, Props>,
    public ZtField_UDT_Constant<Base, Def>,
    public ZtField_UDT_Get<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_UDT<Override<Base>, Props>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_UDT<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldSet setFn() {
    return {.set_ = {.udt = [](void *, const void *) { }}};
  }
  static auto deflt() { return Def(); }
};
template <typename Base, typename Props, auto Def>
struct ZtField_UDT<Base, Props, Def, false> :
    public ZtField_UDT<Base, Props, Def, true> {
  using O = typename Base::O;
  using T = typename Base::T;
  static ZtMFieldSet setFn() {
    return {.set_ = {.udt = [](void *o, unsigned, const void *p) {
      Base::set(*static_cast<O *>(o), *static_cast<const T *>(p));
    }}};
  }
};

#define ZtField_BaseID__(ID, ...) ID
#define ZtField_BaseID_(Axor, ...) ZuPP_Defer(ZtField_BaseID__)Axor
#define ZtField_BaseID(Base) ZuPP_Defer(ZtField_BaseID_)Base

#define ZtField_TypeName_(Name, ...) Name
#define ZtField_TypeName(Type) ZuPP_Defer(ZtField_TypeName_)Type
#define ZtField_LambdaArg(Arg) []{ return Arg; }

#define ZtField_TypeArgs_CString(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_String(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Bytes(...) \
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
#define ZtField_TypeArgs_DateTime(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_UDT(...) \
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
  O ZuFielded_(O *); \
  ZuFields_::O ZuFieldList_(O *)

template <typename Field>
struct ZtFieldPrint_ {
  using O = typename Field::O;
  using Print = typename Field::Type::Print;
  const O &o;
  const ZtFieldFmt &fmt;
  template <typename S>
  friend S &operator <<(S &s, const ZtFieldPrint_ &print) {
    return
      s << Field::id() << '=' << Print{Field::get(print.o), print.fmt};
  }
};

struct ZtFieldPrint : public ZuPrintDelegate {
  template <typename U> struct Print_Filter :
      public ZuBool<!ZuTypeIn<ZtFieldProp::Hidden, typename U::Props>{}> { };
  static ZtFieldFmt &fmt() { return ZmTLS<ZtFieldFmt, fmt>(); }
  template <typename S, typename O>
  static void print(S &s, const O &o) {
    using FieldList = ZuTypeGrep<Print_Filter, ZuFieldList<O>>;
    s << '{';
    ZuUnroll::all<FieldList>([&s, &o]<typename Field>() {
      if constexpr (ZuTypeIndex<Field, FieldList>{}) s << ' ';
      s << ZtFieldPrint_<Field>{o, fmt()};
    });
    s << '}';
  }
};

// run-time fields

using ZtMFields = ZuArray<const ZtMField *>;

template <typename MField, typename ...Fields>
struct ZtMFieldList_ {
  enum { N = sizeof...(Fields) };

  ZtMFields	fields;

  static ZtMFieldList_ *instance() {
    return ZmSingleton<ZtMFieldList_>::instance();
  }

  ZtMFieldList_() {
    static const MField fields_[N] =
      // std::initializer_list<ZtMField>
    {
      MField{Fields{}}...
    };
    static const ZtMField *ptr_[N];
    ZuUnroll::all<N>([](auto i) {
      ptr_[i] = static_cast<const ZtMField *>(&fields_[i]);
    });
    fields = {&ptr_[0], N};
  }
};
template <typename O, typename MField = ZtMField>
inline ZtMFields ZtMFieldList() {
  using Factory = ZuTypeApply<
    ZtMFieldList_, typename ZuFieldList<O>::template Unshift<MField>>;
  return Factory::instance()->fields;
}

// run-time keys
// - each key is a ZuFieldKeyT<O, KeyID>, i.e. a value tuple of a
//   subset of the values in the object itself, used to identify the
//   object as a primary or secondary key
// - each key tuple has its own field array, which is extracted and
//   transformed from the underlying object field array

using ZtMKeyFields = ZuArray<const ZtMFields>;

template <typename O, typename MField>
struct ZtMKeyFieldList_ {
  ZtMKeyFields	keys;

  static ZtMKeyFieldList_ *instance() {
    return ZmSingleton<ZtMKeyFieldList_>::instance();
  }

  ZtMKeyFieldList_() {
    using KeyIDs = ZuFieldKeyIDs<O>;
    static ZtMFields data_[KeyIDs::N];
    ZuUnroll::all<KeyIDs>([](auto i) {
      data_[i] = ZtMFieldList<ZuFieldKeyT<O, i>, MField>();
    });
    keys = {&data_[0], KeyIDs::N};
  }
};
template <typename O, typename MField = ZtMField>
inline ZtMKeyFields ZtMKeyFieldList() {
  return ZtMKeyFieldList_<O, MField>::instance()->keys;
}

// --- generic run-time data transformation (for ORM, etc.)

namespace ZtField {

// load from external representation
using Importer = ZtArray<ZtMFieldGet>;		// one for each field
struct Import {
  const Importer	&importer;
  const void		*ptr;

  template <unsigned I, int Code, typename U>
  decltype(auto) get() const {
    if constexpr (Code == ZtFieldTypeCode::UDT)
      return
	static_cast<const U &>(*static_cast<const U *>(
	      importer[I].get<Code>(ptr, I)));
    else if constexpr (Code == ZtFieldTypeCode::Int ||
		       Code == ZtFieldTypeCode::UInt ||
		       Code == ZtFieldTypeCode::Float)
      return static_cast<U>(importer[I].get<Code>(ptr, I));
    else
      return importer[I].get<Code>(ptr, I);
  }
};

// save to external representation
using Exporter = ZtArray<ZtMFieldSet>;		// one for each field
struct Export {
  const Exporter	&exporter;
  void			*ptr;

  template <unsigned I, int Code, typename U>
  void set(U &&v) const {
    return exporter[I].set<Code>(ptr, I, ZuFwd<U>(v));
  }
};

namespace TypeCode = ZtFieldTypeCode;
namespace Prop = ZtFieldProp;

template <typename O>
struct Fielded_ {
  using AllFields = ZuFieldList<O>;

  // load fields are all the mutable fields
  template <typename U>
  struct LoadFilter : public ZuBool<!U::ReadOnly> { };
  using LoadFields = ZuTypeGrep<LoadFilter, AllFields>;

  // ctor fields - fields passed to the constructor
  template <typename U> struct CtorFilter :
      public ZuBool<(Prop::GetCtor<typename U::Props>{} >= 0)> { };
  template <typename U>
  struct CtorIndex : public Prop::GetCtor<typename U::Props> { };
  using CtorFields =
    ZuTypeSort<CtorIndex, ZuTypeGrep<CtorFilter, AllFields>>;

  // init fields - fields set post-constructor
  template <typename U> struct InitFilter :
      public ZuBool<(Prop::GetCtor<typename U::Props>{} < 0)> { };
  using InitFields = ZuTypeGrep<InitFilter, LoadFields>;

  // save fields - all the ctor and init fields
  template <typename U> struct SaveFilter :
      public ZuBool<
	(Prop::GetCtor<typename U::Props>{} >= 0) || !U::ReadOnly> { };
  using SaveFields = ZuTypeGrep<SaveFilter, AllFields>;

  // update fields - mutable fields and primary key fields
  template <typename U>
  struct UpdFilter :
      public ZuBool<
	ZuTypeIn<Prop::Update, typename U::Props>{} || (U::keys() & 1)> { };
  using UpdFields = ZuTypeGrep<UpdFilter, LoadFields>;

  // delete fields - primary key fields
  using DelFields = ZuKeyFields<O, 0>;

  template <typename ...Fields>
  struct Exporter {
    static auto fn() {
      return ZtField::Exporter{ { Fields::setFn()... } };
    }
  };
  template <typename ...Fields>
  struct Exporter<ZuTypeList<Fields...>> : public Exporter<Fields...> { };
  static auto exporter() { return Exporter<AllFields>::fn(); }

  template <typename ...Fields>
  struct Importer {
    static auto fn() {
      return ZtField::Importer{ { Fields::getFn()... } };
    }
  };
  template <typename ...Fields>
  struct Importer<ZuTypeList<Fields...>> : public Importer<Fields...> { };
  static auto importer() { return Importer<AllFields>::fn(); }

  template <typename Fields>
  struct Save {
    static void save(const O &o, const Export &export_) {
      ZmAssert(export_.exporter.length() == AllFields::N);
      ZuUnroll::all<Fields>([&o, &export_]<typename Field>() {
	const auto &v = Field::get(o); // temporary lifetime-extension
	export_.set<ZuTypeIndex<Field, AllFields>{}, Field::Type::Code>(v);
      });
    }
  };
  static void save(const O &o, const Export &export_) {
    Save<SaveFields>::save(o, export_);
  }
  static void saveUpd(const O &o, const Export &export_) {
    Save<UpdFields>::save(o, export_);
  }
  static void saveDel(const O &o, const Export &export_) {
    Save<DelFields>::save(o, export_);
  }

  template <typename ...Field>
  struct Ctor {
    static O ctor(const Import &import_) {
      ZmAssert(import_.importer.length() == AllFields::N);
      return O{
	import_.get<
	  ZuTypeIndex<Field, AllFields>{},
	  Field::Type::Code,
	  typename Field::T>()...
      };
    }
    static void ctor(void *o, const Import &import_) {
      ZmAssert(import_.importer.length() == AllFields::N);
      new (o) O{
	import_.get<
	  ZuTypeIndex<Field, AllFields>{},
	  Field::Type::Code,
	  typename Field::T>()...
      };
    }
  };
  static O ctor(const Import &import_) {
    ZmAssert(import_.importer.length() == AllFields::N);
    O o = ZuTypeApply<Ctor, CtorFields>::ctor(import_);
    ZuUnroll::all<InitFields>([&o, &import_]<typename Field>() {
      Field::set(o,
	import_.get<
	  ZuTypeIndex<Field, AllFields>{},
	  Field::Type::Code,
	  typename Field::T>());
    });
    return o;
  }
  static void ctor(void *o_, const Import &import_) {
    ZmAssert(import_.importer.length() == AllFields::N);
    ZuTypeApply<Ctor, CtorFields>::ctor(o_, import_);
    O &o = *static_cast<O *>(o_);
    ZuUnroll::all<InitFields>([&o, &import_]<typename Field>() {
      Field::set(o,
	import_.get<
	  ZuTypeIndex<Field, AllFields>{},
	  Field::Type::Code,
	  typename Field::T>());
    });
  }

  template <typename ...Field>
  struct Load__ : public O {
    Load__() = default;
    Load__(const Import &import_) : O{
      import_.get<
	ZuTypeIndex<Field, AllFields>{},
	Field::Type::Code,
	typename Field::T>()...
    } { }
    template <typename ...Args>
    Load__(Args &&... args) : O{ZuFwd<Args>(args)...} { }
  };
  using Load_ = ZuTypeApply<Load__, CtorFields>;
  struct Load : public Load_ {
    Load() = default;
    Load(const Import &import_) : Load_{import_} {
      ZmAssert(import_.importer.length() == AllFields::N);
      ZuUnroll::all<InitFields>([this, &import_]<typename Field>() {
	Field::set(*this,
	  import_.get<
	    ZuTypeIndex<Field, AllFields>{},
	    Field::Type::Code,
	    typename Field::T>());
      });
    }
    template <typename ...Args>
    Load(Args &&... args) : Load_{ZuFwd<Args>(args)...} { }
  };

  static void load(O &o, const Import &import_) {
    ZmAssert(import_.importer.length() == AllFields::N);
    ZuUnroll::all<LoadFields>([&o, &import_]<typename Field>() {
      Field::set(o,
	import_.get<
	  ZuTypeIndex<Field, AllFields>{},
	  Field::Type::Code,
	  typename Field::T>());
    });
  }
  static void update(O &o, const Import &import_) {
    ZmAssert(import_.importer.length() == AllFields::N);
    ZuUnroll::all<UpdFields>([&o, &import_]<typename Field>() {
      Field::set(o,
	import_.get<
	  ZuTypeIndex<Field, AllFields>{},
	  Field::Type::Code,
	  typename Field::T>());
    });
  }

  template <typename ...Field>
  struct Key {
    using Tuple_ = ZuTuple<typename Field::T...>;
    static decltype(auto) tuple(const Import &import_) {
      ZmAssert(import_.importer.length() == AllFields::N);
      return Tuple_{
	import_.get<
	  ZuTypeIndex<Field, AllFields>{},
	  Field::Type::Code,
	  typename Field::T>()...
      };
    }
  };
  template <typename ...Fields>
  struct Key<ZuTypeList<Fields...>> : public Key<Fields...> { };

  template <int KeyID>
  struct KeyFilter {
    template <typename U> struct T : public ZuBool<U::keys() & (1<<KeyID)> { };
  };
  template <int KeyID = 0>
  static auto key(const Import &import_) {
    using Fields = ZuTypeGrep<KeyFilter<KeyID>::template T, AllFields>;
    return Key<Fields>::tuple(import_);
  }
};
template <typename O>
using Fielded = Fielded_<ZuFielded<O>>;

template <typename O>
inline auto importer() { return Fielded<O>::importer(); }
template <typename O>
inline auto exporter() { return Fielded<O>::exporter(); }

template <typename O>
inline void save(const O &o, const Export &export_) {
  Fielded<O>::save(o, export_);
}
template <typename O>
inline void saveUpd(const O &o, const Export &export_) {
  Fielded<O>::saveUpd(o, export_);
}

template <typename O>
inline void saveDel(const O &o, const Export &export_) {
  Fielded<O>::saveDel(o, export_);
}

template <typename O>
inline O ctor(const Import &import_) {
  return Fielded<O>::ctor(import_);
}
template <typename O>
inline void ctor(void *o_, const Import &import_) {
  Fielded<O>::ctor(o_, import_);
}

template <typename O> using Load = typename Fielded<O>::Load;

template <typename O>
inline void load(O &o, const Import &import_) {
  Fielded<O>::load(o, import_);
}
template <typename O>
inline void update(O &o, const Import &import_) {
  Fielded<O>::update(o, import_);
}

template <typename O, int KeyID = 0>
inline auto key(const Import &import_) {
  return Fielded<O>::template key<KeyID>(import_);
}

// convenient access to various lists of object fields

// LoadList - all fields that can be set (excludes read-only constructor fields)
// CtorList - fields passed to constructor (may be subsequently read-only)
// InitList - fields set post-constructor
// SaveList - fields that save object (excluding synthetic/derived fields)
// UpdList  - fields that are updated and those comprising the primary key
// DelList  - fields that comprise the primary key

template <typename O> using List = ZuFieldList<O>;
template <typename O> using LoadList = typename Fielded_<O>::LoadFields; 
template <typename O> using CtorList = typename Fielded_<O>::CtorFields; 
template <typename O> using InitList = typename Fielded_<O>::InitFields; 
template <typename O> using SaveList = typename Fielded_<O>::SaveFields; 
template <typename O> using UpdList = typename Fielded_<O>::UpdFields; 
template <typename O> using DelList = typename Fielded_<O>::DelFields; 

} // ZtField

#endif /* ZtField_HH */
