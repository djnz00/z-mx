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

// object introspection - compile-time (ZtField*) and monomorphic (ZtMField*)
// - print/scan (CSV, etc.)
// - ORM
// - data series
// - any other application that needs to introspect structured data

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
// ZtField Type	C++ Type		ZtField Args
// ------------	--------		------------
// CString	<char *>		[, default]
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

// Note: Regarding run-time introspection with monomorphic fields (ZtMField),
// virtual polymorphism and RTTI are intentionally avoided:
// 1] if ZtMField were virtually polymorphic, passing it to dynamically
//    loaded libraries (e.g. data store adapters performing serdes) would
//    entail a far more complex type hierarchy with diamond-shaped
//    inheritance, use of dynamic_cast, etc.
// 2] ZtMField (and derived classes) benefit from being POD
// 3] very little syntactic benefit would be obtained

// ZtFieldType is keyed on <Code, T, Map, Props> and provides:
//   Code	- type code
//   T		- underlying type
//   Map	- map (only defined for Enum and Flags)
//   Props	- properties type list
//   Print	- Print{const T &, const ZtFieldFmt &} - formatted printing
//   vtype()	- ZtMFieldType * instance
// 
// ZtField derives from ZuField and adds:
//   O		- object type
//   T		- underlying field type
//   Type	- ZtFieldType
//   Print	- Print{const O &, const ZtFieldFmt &} - formatted printing
//   deflt()	- canonical default value
//
// ZtMFieldType provides:
//   code	- ZtFieldTypeCode
//   props	- ZtMFieldProp
//   info	- enum / flags / UDT metadata
//   get	- run-time get function (const void *o)
//   set	- run-time set function (void *o, <type>)
//   print	- print opaque value
//   scan	- scan opaque value from string
//   cmp	- compare opaque values
//
// ZtMField{ZtField{}} instantiates ZtMField from ZtField
//
// ZtMField provides:
//   type	- ZtMFieldType * instance
//   id		- ZtField::id()
//   props	- ZtMFieldProp properties bitfield
//   keys	- ZtField::keys()
//   get	- run-time get function (const void *o)
//   set	- run-time set function (void *o, <type>)
//   constant	- run-time constant function (ZtMFieldConstant) -> const void *

#ifndef ZtField_HPP
#define ZtField_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZtLib_HPP
#include <zlib/ZtLib.hpp>
#endif

#include <string.h>

#include <typeinfo>

#include <zlib/ZuPrint.hpp>
#include <zlib/ZuBox.hpp>
#include <zlib/ZuArray.hpp>
#include <zlib/ZuString.hpp>
#include <zlib/ZuFixed.hpp>
#include <zlib/ZuDecimal.hpp>
#include <zlib/ZuUnroll.hpp>
#include <zlib/ZuInspect.hpp>
#include <zlib/ZuField.hpp>
#include <zlib/ZuUnroll.hpp>

#include <zlib/ZmStream.hpp>
#include <zlib/ZmTime.hpp>
#include <zlib/ZmSingleton.hpp>

#include <zlib/ZtEnum.hpp>
#include <zlib/ZtDate.hpp>
#include <zlib/ZtString.hpp>

namespace ZtFieldTypeCode {
  ZtEnumValues(ZtFieldTypeCode,
    CString,		// C UTF-8 string (raw pointer), heap-allocated
    String,		// C++ contiguous UTF-8 string
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

// ZtMFieldProp bitfield encapsulates introspected ZtField properties
namespace ZtMFieldProp {
  ZtEnumFlags(ZtMFieldProp,
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
  void		(*scan)(uint64_t &, ZuString, const ZtFieldFmt &);
};
template <typename Map>
struct ZtMFieldFlags_ : public ZtMFieldFlags {
  ZtMFieldFlags_() : ZtMFieldFlags{
    .id = []() -> const char * { return Map::id(); },
    .print = [](uint64_t v, ZmStream &s, const ZtFieldFmt &fmt) -> void {
      s << Map::print(v, fmt.flagsDelim);
    },
    .scan = [](uint64_t &v, ZuString s, const ZtFieldFmt &fmt) -> void {
      v = Map::template scan<uint64_t>(s, fmt.flagsDelim);
    }
  } { }

  static ZtMFieldFlags *instance() {
    return ZmSingleton<ZtMFieldFlags_>::instance();
  }
};

struct ZtMFieldGet {
  union {
    void		*null;
    const char *	(*cstring)(const void *);	// CString
    ZuString		(*string)(const void *);	// String
    ZuBytes		(*bytes)(const void *);		// Bytes
    const void *	(*udt)(const void *);		// UDT
    bool		(*bool_)(const void *);		// Bool
    int64_t		(*int_)(const void *);		// Int
    uint64_t		(*uint)(const void *);		// UInt
    int			(*enum_)(const void *);		// Enum
    uint64_t		(*flags)(const void *);		// Flags
    double		(*float_)(const void *);	// Float
    ZuFixed		(*fixed)(const void *);		// Fixed
    ZuDecimal		(*decimal)(const void *);	// Decimal
    ZmTime		(*time)(const void *);		// Time
  } fn_;

  template <unsigned I>
  ZuIfT<I == ZtFieldTypeCode::CString, const char *> fn(const void *p) const {
    return fn_.cstring(p);
  }
  template <unsigned I>
  ZuIfT<I == ZtFieldTypeCode::String, ZuString> fn(const void *p) const {
    return fn_.string(p);
  }
  template <unsigned I>
  ZuIfT<I == ZtFieldTypeCode::Bytes, ZuBytes> fn(const void *p) const {
    return fn_.bytes(p);
  }
  template <unsigned I>
  ZuIfT<I == ZtFieldTypeCode::UDT, const void *> fn(const void *p) const {
    return fn_.udt(p);
  }
  template <unsigned I>
  ZuIfT<I == ZtFieldTypeCode::Bool, bool> fn(const void *p) const {
    return fn_.bool_(p);
  }
  template <unsigned I>
  ZuIfT<I == ZtFieldTypeCode::Int, int64_t> fn(const void *p) const {
    return fn_.int_(p);
  }
  template <unsigned I>
  ZuIfT<I == ZtFieldTypeCode::UInt, uint64_t> fn(const void *p) const {
    return fn_.uint(p);
  }
  template <unsigned I>
  ZuIfT<I == ZtFieldTypeCode::Enum, int> fn(const void *p) const {
    return fn_.enum_(p);
  }
  template <unsigned I>
  ZuIfT<I == ZtFieldTypeCode::Flags, uint64_t> fn(const void *p) const {
    return fn_.flags(p);
  }
  template <unsigned I>
  ZuIfT<I == ZtFieldTypeCode::Float, double> fn(const void *p) const {
    return fn_.float_(p);
  }
  template <unsigned I>
  ZuIfT<I == ZtFieldTypeCode::Fixed, ZuFixed> fn(const void *p) const {
    return fn_.fixed(p);
  }
  template <unsigned I>
  ZuIfT<I == ZtFieldTypeCode::Decimal, ZuDecimal> fn(const void *p) const {
    return fn_.decimal(p);
  }
  template <unsigned I>
  ZuIfT<I == ZtFieldTypeCode::Time, ZmTime> fn(const void *p) const {
    return fn_.time(p);
  }
};

struct ZtMFieldSet {
  union {
    void		*null;
    void		(*cstring)(void *, const char *);// CString
    void		(*string)(void *, ZuString);	// String
    void		(*bytes)(void *, ZuBytes);	// Bytes
    void		(*udt)(void *, const void *);	// UDT
    void		(*bool_)(void *, bool);		// Bool
    void		(*int_)(void *, int64_t);	// Int
    void		(*uint)(void *, uint64_t);	// UInt
    void		(*enum_)(void *, int);		// Enum
    void		(*flags)(void *, uint64_t);	// Flags
    void		(*float_)(void *, double);	// Float
    void		(*fixed)(void *, ZuFixed);	// Fixed
    void		(*decimal)(void *, ZuDecimal);	// Decimal
    void		(*time)(void *, ZmTime);	// Time
  } fn_;

  template <unsigned I>
  ZuIfT<I == ZtFieldTypeCode::CString> fn(void *p, const char *v) const {
    fn_.cstring(p, v);
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::String> fn(void *p, U &&v) const {
    fn_.string(p, ZuString{ZuFwd<U>(v)});
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Bytes> fn(void *p, U &&v) const {
    fn_.bytes(p, ZuBytes{ZuFwd<U>(v)});
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::UDT> fn(void *p, const U &v) const {
    fn_.udt(p, static_cast<const void *>(&v));
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Bool> fn(void *p, U &&v) const {
    fn_.bool_(p, ZuFwd<U>(v));
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Int> fn(void *p, U &&v) const {
    fn_.int_(p, ZuFwd<U>(v));
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::UInt> fn(void *p, U &&v) const {
    fn_.uint(p, ZuFwd<U>(v));
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Enum> fn(void *p, U &&v) const {
    fn_.enum_(p, ZuFwd<U>(v));
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Flags> fn(void *p, U &&v) const {
    fn_.flags(p, ZuFwd<U>(v));
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Float> fn(void *p, U &&v) const {
    fn_.float_(p, ZuFwd<U>(v));
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Fixed> fn(void *p, U &&v) const {
    fn_.fixed(p, ZuFixed{ZuFwd<U>(v)});
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Decimal> fn(void *p, U &&v) const {
    fn_.decimal(p, ZuDecimal{ZuFwd<U>(v)});
  }
  template <unsigned I, typename U>
  ZuIfT<I == ZtFieldTypeCode::Time> fn(void *p, U &&v) const {
    fn_.time(p, ZmTime{ZuFwd<U>(v)});
  }
};

// ZtMFieldType encapsulates introspected type metadata
struct ZtMFieldType {
  int			code;		// ZtFieldTypeCode
  uint32_t		props;		// ZtMFieldProp

  union {
    void			*null;
    ZtMFieldEnum *		(*enum_)();	// Enum
    ZtMFieldFlags *		(*flags)();	// Flags
    const std::type_info	*udt;		// UDT
  } info;

  ZtMFieldGet		get;
  ZtMFieldSet		set;

  void (*print)(const void *, ZmStream &, const ZtFieldFmt &);
  void (*scan)(void *, ZuString, const ZtFieldFmt &);

  int (*cmp)(const void *, const void *);
};

// ZtMFieldConstant is used to retrieve field constants
namespace ZtMFieldConstant {
  enum { Null = 0, Deflt, Minimum, Maximum };
}

// ZtField_ compile-time encapsulates an individual field, derives from ZuField
template <typename Base, typename Props_>
struct ZtField_ : public Base {
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = Props_;
  constexpr static uint64_t vprops() {
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
  struct Print {
    const char *v;
    const ZtFieldFmt &fmt; // unused, but needed at compile-time
    enum { Quote = ZuTypeIn<ZtFieldProp::Quote, Props>{} };
    template <typename S, bool Quote_ = Quote>
    friend ZuIfT<Quote_, S &> operator <<(S &s, const Print &print) {
      const char *v = print.v;
      s << '"';
      for (unsigned i = 0; v[i]; i++) {
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
    .info = {.null = nullptr},
    .get = {.fn_ = {.cstring = [](const void *ptr) -> const char * {
      return *static_cast<const T *>(ptr);
    }}},
    .set = {.fn_ = {.cstring = [](void *ptr_, const char *s) {
      auto &ptr = *static_cast<T *>(ptr_);
      if (ptr) ::free(ptr);
      ptr = s ? strdup(s) : static_cast<char *>(nullptr);
    }}},
    .print = [](const void *ptr, ZmStream &s, const ZtFieldFmt &fmt) {
      using Print = typename ZtFieldType_CString<T, Props>::Print;
      s << Print{*static_cast<const T *>(ptr), fmt};
    },
    .scan = [](void *ptr_, ZuString s, const ZtFieldFmt &fmt) {
      auto &ptr = *static_cast<T *>(ptr_);
      if (ptr) ::free(ptr);
      ptr = s ? strndup(s.data(), s.length()) : static_cast<char *>(nullptr);
    },
    .cmp = ZtFieldType_Cmp<T>::fn()
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
  using O = typename Base::O;
  using T = char *;
  using Type =
    ZtFieldType_CString<char *, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.fn_ = {.cstring = [](const void *o) -> const char * {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.fn_ = {.cstring = [](void *, const char *) { }}};
  }
  using Print_ = typename Type::Print;
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Base::id() << '=' << Print_{Base::get(print.o), print.fmt};
    }
  };
  static const char *deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtMFieldConstant;
    static const char *deflt_{Def()};
    return [](int i) -> const void * {
      if (ZuLikely(i == Deflt)) return static_cast<const void *>(&deflt_);
      return nullptr;
    };
  }
};
template <typename Base, typename Props, auto Def>
struct ZtField_CString<Base, Props, Def, false> :
    public ZtField_CString<Base, Props, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.fn_ = {.cstring = [](void *o_, const char *s) {
      auto &o = *static_cast<const O *>(o_);
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
    .info = {.null = nullptr},
    .get = {.fn_ = {.string = [](const void *ptr) -> ZuString {
      return *(static_cast<const T *>(ptr));
    }}},
    .set = {.fn_ = {.string = [](void *ptr, ZuString v) {
      *(static_cast<T *>(ptr)) = v;
    }}},
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
ZtMFieldType *ZtFieldType_String<T, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_String<T, Props>>::instance();
}

inline ZuString ZtField_String_Def() { return {}; }
template <typename Base, typename = void>
struct ZtField_String_Get {
  static ZtMFieldGet getFn() {
    using O = typename Base::O;
    // field get() returns a temporary
    return {.fn_ = {.string = [](const void *o) -> ZuString {
      thread_local ZtString v;
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
    return {.fn_ = {.string = [](const void *o) -> ZuString {
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
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_String<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldSet setFn() {
    return {.fn_ = {.string = [](void *, ZuString) { }}};
  }
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
    using namespace ZtMFieldConstant;
    static T deflt_{Def()};
    return [](int i) -> const void * {
      if (ZuLikely(i == Deflt)) return static_cast<const void *>(&deflt_);
      return nullptr;
    };
  }
};
template <typename Base, typename Props, auto Def>
struct ZtField_String<Base, Props, Def, false> :
    public ZtField_String<Base, Props, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.fn_ = {.string = [](void *o, ZuString s) {
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
    .info = {.null = nullptr},
    .get = {.fn_ = {.bytes = [](const void *ptr) -> ZuBytes {
      return *(static_cast<const T *>(ptr));
    }}},
    .set = {.fn_ = {.bytes = [](void *ptr, ZuBytes v) {
      *(static_cast<T *>(ptr)) = v;
    }}},
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
ZtMFieldType *ZtFieldType_Bytes<T, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_Bytes<T, Props>>::instance();
}

inline ZuBytes ZtField_Bytes_Def() { return {}; }
template <typename Base, typename = void>
struct ZtField_Bytes_Get {
  static ZtMFieldGet getFn() {
    using O = typename Base::O;
    // field get() returns a temporary
    return {.fn_ = {.bytes = [](const void *o) -> ZuBytes {
      thread_local ZtBytes v;
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
    return {.fn_ = {.bytes = [](const void *o) -> ZuBytes {
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
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Bytes<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldSet setFn() {
    return {.fn_ = {.bytes = [](void *, ZuBytes) { }}};
  }
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
    using namespace ZtMFieldConstant;
    static T deflt_{Def()};
    return [](int i) -> const void * {
      if (ZuLikely(i == Deflt)) return static_cast<const void *>(&deflt_);
      return nullptr;
    };
  }
};
template <typename Base, typename Props, auto Def>
struct ZtField_Bytes<Base, Props, Def, false> :
    public ZtField_Bytes<Base, Props, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.fn_ = {.bytes = [](void *o, ZuBytes v) {
      Base::set(*static_cast<O *>(o), v);
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

template <typename T, typename Props>
struct ZtMFieldType_UDT : public ZtMFieldType {
  ZtMFieldType_UDT() : ZtMFieldType{
    .code = ZtFieldTypeCode::UDT,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.udt = &typeid(T) },
    .get = {.fn_ = {.udt = [](const void *p) {
      return p;
    }}},
    .set = {.fn_ = {.udt = [](void *p, const void *v) {
      static_cast<T *>(p)->~T();
      new (p) T{*static_cast<const T *>(v)};
    }}},
    .print = [](const void *ptr, ZmStream &s, const ZtFieldFmt &fmt) {
      s << *static_cast<const T *>(ptr);
    },
    .scan = ZtFieldType_Scan<T>::fn(),
    .cmp = ZtFieldType_Cmp<T>::fn()
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
  static auto constantFn() {
    typedef const void *(*Fn)(int);
    return static_cast<Fn>(nullptr);
  }
};
template <typename Base, auto Def>
struct ZtField_UDT_Constant<Base, Def,
    decltype(typename Base::T{Def()}, void())> {
  static auto constantFn() {
    using T = typename Base::T;
    using namespace ZtMFieldConstant;
    static T deflt_{Def()};
    return [](int i) -> const void * {
      if (ZuLikely(i == Deflt)) return static_cast<const void *>(&deflt_);
      return ZtField_UDT_Null<T>::value();
    };
  }
};
template <typename Base, typename = void>
struct ZtField_UDT_Get {
  static ZtMFieldGet getFn() {
    using O = typename Base::O;
    using T = typename Base::T;
    // field get() returns a temporary
    return {.fn_ = {.udt = [](const void *o) -> const void * {
      thread_local T v;
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
    return {.fn_ = {.udt = [](const void *o) -> const void * {
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
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_UDT<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldSet setFn() {
    return {.fn_ = {.udt = [](void *, const void *) { }}};
  }
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
template <typename Base, typename Props, auto Def>
struct ZtField_UDT<Base, Props, Def, false> :
    public ZtField_UDT<Base, Props, Def, true> {
  using O = typename Base::O;
  using T = typename Base::T;
  static ZtMFieldSet setFn() {
    return {.fn_ = {.udt = [](void *o, const void *p) {
      Base::set(*static_cast<O *>(o), *static_cast<const T *>(p));
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
    .info = {.null = nullptr},
    .get = {.fn_ = {.bool_ = [](const void *ptr) -> bool {
      return *(static_cast<const T *>(ptr));
    }}},
    .set = {.fn_ = {.bool_ = [](void *ptr, bool v) {
      *(static_cast<T *>(ptr)) = v;
    }}},
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
ZtMFieldType *ZtFieldType_Bool<T, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_Bool<T, Props>>::instance();
}

inline constexpr bool ZtField_Bool_Def() { return false; }
template <
  typename Base, typename Props,
  auto Def = ZtField_Bool_Def,
  bool = Base::ReadOnly>
struct ZtField_Bool : public ZtField_<Base, Props> {
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Bool<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.fn_ = {.bool_ = [](const void *o) -> bool {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.fn_ = {.bool_ = [](void *, bool) { }}};
  }
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
    using namespace ZtMFieldConstant;
    static T deflt_{Def()};
    static T null_{false};
    return [](int i) -> const void * {
      if (ZuLikely(i == Deflt)) return static_cast<const void *>(&deflt_);
      return static_cast<const void *>(&null_);
    };
  }
};
template <typename Base, typename Props, auto Def>
struct ZtField_Bool<Base, Props, Def, false> :
    public ZtField_Bool<Base, Props, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.fn_ = {.bool_ = [](void *o, bool v) {
      Base::set(*static_cast<O *>(o), v);
    }}};
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
    .info = {.null = nullptr},
    .get = {.fn_ = {.int_ = [](const void *ptr) -> int64_t {
      return *(static_cast<const T *>(ptr));
    }}},
    .set = {.fn_ = {.int_ = [](void *ptr, int64_t v) {
      *(static_cast<T *>(ptr)) = v;
    }}},
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
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Int<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.fn_ = {.int_ = [](const void *o) -> int64_t {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.fn_ = {.int_ = [](void *, int64_t) { }}};
  }
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
    using namespace ZtMFieldConstant;
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
template <typename Base, typename Props, auto Def, auto Min, auto Max>
struct ZtField_Int<Base, Props, Def, Min, Max, false> :
    public ZtField_Int<Base, Props, Def, Min, Max, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.fn_ = {.int_ = [](void *o, int64_t v) {
      Base::set(*static_cast<O *>(o), v);
    }}};
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
    .info = {.null = nullptr},
    .get = {.fn_ = {.uint = [](const void *ptr) -> uint64_t {
      return *(static_cast<const T *>(ptr));
    }}},
    .set = {.fn_ = {.uint = [](void *ptr, uint64_t v) {
      *(static_cast<T *>(ptr)) = v;
    }}},
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
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_UInt<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.fn_ = {.uint = [](const void *o) -> uint64_t {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.fn_ = {.uint = [](void *, uint64_t) { }}};
  }
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
    using namespace ZtMFieldConstant;
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
template <typename Base, typename Props, auto Def, auto Min, auto Max>
struct ZtField_UInt<Base, Props, Def, Min, Max, false> :
    public ZtField_UInt<Base, Props, Def, Min, Max, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.fn_ = {.uint = [](void *o, uint64_t v) {
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
    }},
    .get = {.fn_ = {.enum_ = [](const void *ptr) -> int {
      return *(static_cast<const T *>(ptr));
    }}},
    .set = {.fn_ = {.enum_ = [](void *ptr, int v) {
      *(static_cast<T *>(ptr)) = v;
    }}},
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
ZtMFieldType *ZtFieldType_Enum<T, Map, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_Enum<T, Map, Props>>::instance();
}

inline constexpr int ZtField_Enum_Def() { return -1; }
template <
  typename Base, typename Props, typename Map,
  auto Def = ZtField_Enum_Def,
  bool = Base::ReadOnly>
struct ZtField_Enum : public ZtField_<Base, Props> {
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Enum<T, Map, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.fn_ = {.enum_ = [](const void *o) -> int {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.fn_ = {.enum_ = [](void *, int) { }}};
  }
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
    using namespace ZtMFieldConstant;
    static T deflt_{Def()};
    static T null_{ZuCmp<T>::null()};
    return [](int i) -> const void * {
      if (ZuLikely(i == Deflt)) return static_cast<const void *>(&deflt_);
      return static_cast<const void *>(&null_);
    };
  }
};
template <typename Base, typename Props, typename Map, auto Def>
struct ZtField_Enum<Base, Props, Map, Def, false> :
    public ZtField_Enum<Base, Props, Map, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.fn_ = {.enum_ = [](void *o, int v) {
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
    }},
    .get = {.fn_ = {.flags = [](const void *ptr) -> uint64_t {
      return *(static_cast<const T *>(ptr));
    }}},
    .set = {.fn_ = {.flags = [](void *ptr, uint64_t v) {
      *(static_cast<T *>(ptr)) = v;
    }}},
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
ZtMFieldType *ZtFieldType_Flags<T, Map, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_Flags<T, Map, Props>>::instance();
}

inline constexpr int ZtField_Flags_Def() { return 0; }
template <
  typename Base, typename Props, typename Map,
  auto Def = ZtField_Flags_Def,
  bool = Base::ReadOnly>
struct ZtField_Flags : public ZtField_<Base, Props> {
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Flags<T, Map, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.fn_ = {.flags = [](const void *o) -> uint64_t {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.fn_ = {.flags = [](void *, uint64_t) { }}};
  }
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
    using namespace ZtMFieldConstant;
    static T deflt_{Def()};
    static T null_{ZuCmp<T>::null()};
    return [](int i) -> const void * {
      if (ZuLikely(i == Deflt)) return static_cast<const void *>(&deflt_);
      return static_cast<const void *>(&null_);
    };
  }
};
template <typename Base, typename Props, typename Map, auto Def>
struct ZtField_Flags<Base, Props, Map, Def, false> :
    public ZtField_Flags<Base, Props, Map, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.fn_ = {.flags = [](void *o, uint64_t v) {
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
    .info = {.null = nullptr},
    .get = {.fn_ = {.float_ = [](const void *ptr) -> double {
      return *(static_cast<const T *>(ptr));
    }}},
    .set = {.fn_ = {.float_ = [](void *ptr, double v) {
      *(static_cast<T *>(ptr)) = v;
    }}},
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
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Float<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.fn_ = {.float_ = [](const void *o) -> double {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.fn_ = {.float_ = [](void *, double) { }}};
  }
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
    using namespace ZtMFieldConstant;
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
template <typename Base, typename Props, auto Def, auto Min, auto Max>
struct ZtField_Float<Base, Props, Def, Min, Max, false> :
    public ZtField_Float<Base, Props, Def, Min, Max, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.fn_ = {.float_ = [](void *o, double v) {
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
    .info = {.null = nullptr},
    .get = {.fn_ = {.fixed = [](const void *ptr) -> ZuFixed {
      return *(static_cast<const T *>(ptr));
    }}},
    .set = {.fn_ = {.fixed = [](void *ptr, ZuFixed v) {
      *(static_cast<T *>(ptr)) = v;
    }}},
    .print = [](const void *ptr, ZmStream &s, const ZtFieldFmt &fmt) {
      using Print = typename ZtFieldType_Fixed<T, Props>::Print;
      s << Print{*static_cast<const T *>(ptr), fmt};
    },
    .scan = [](void *ptr, ZuString s, const ZtFieldFmt &fmt) {
      // preserve exponent
      unsigned exponent;
      if constexpr (ZuInspect<T, ZuFixed>::Same)
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
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Fixed<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.fn_ = {.fixed = [](const void *o) -> ZuFixed {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.fn_ = {.fixed = [](void *, ZuFixed) { }}};
  }
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
    using namespace ZtMFieldConstant;
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
template <typename Base, typename Props, auto Def, auto Min, auto Max>
struct ZtField_Fixed<Base, Props, Def, Min, Max, false> :
    public ZtField_Fixed<Base, Props, Def, Min, Max, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.fn_ = {.fixed = [](void *o, ZuFixed v) {
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
    .info = {.null = nullptr},
    .get = {.fn_ = {.decimal = [](const void *ptr) -> ZuDecimal {
      return *(static_cast<const T *>(ptr));
    }}},
    .set = {.fn_ = {.decimal = [](void *ptr, ZuDecimal v) {
      *(static_cast<T *>(ptr)) = v;
    }}},
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
  using O = typename Base::O;
  using T = typename Base::T;
  using Type = ZtFieldType_Decimal<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.fn_ = {.decimal = [](const void *o) -> ZuDecimal {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.fn_ = {.decimal = [](void *, ZuDecimal) { }}};
  }
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
    using namespace ZtMFieldConstant;
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
template <typename Base, typename Props, auto Def, auto Min, auto Max>
struct ZtField_Decimal<Base, Props, Def, Min, Max, false> :
    public ZtField_Decimal<Base, Props, Def, Min, Max, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.fn_ = {.decimal = [](void *o, ZuDecimal v) {
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
    ZmTime v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      ZtDate v{print.v};
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
    .info = {.null = nullptr},
    .get = {.fn_ = {.time = [](const void *ptr) -> ZmTime {
      return *(static_cast<const T *>(ptr));
    }}},
    .set = {.fn_ = {.time = [](void *ptr, ZmTime v) {
      *(static_cast<T *>(ptr)) = v;
    }}},
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
ZtMFieldType *ZtFieldType_Time<T, Props>::vtype() {
  return ZmSingleton<ZtMFieldType_Time<T, Props>>::instance();
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
  static ZtMFieldGet getFn() {
    return {.fn_ = {.time = [](const void *o) -> ZmTime {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.fn_ = {.time = [](void *, ZmTime) { }}};
  }
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
    using namespace ZtMFieldConstant;
    static T deflt_{Def()};
    static T null_{ZuCmp<T>::null()};
    return [](int i) -> const void * {
      if (ZuLikely(i == Deflt)) return static_cast<const void *>(&deflt_);
      return static_cast<const void *>(&null_);
    };
  }
};
template <typename Base, typename Props, auto Def>
struct ZtField_Time<Base, Props, Def, false> :
    public ZtField_Time<Base, Props, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.fn_ = {.time = [](void *o, ZmTime v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
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
    ZuUnroll::all<FieldList>([&o, &s]<typename Field>() {
      if constexpr (ZuTypeIndex<Field, FieldList>{}) s << ' ';
      s << typename Field::Print{o, fmt};
    });
    s << '}';
  }
};

struct ZtMField {
  ZtMFieldType		*type;
  const char		*id;
  uint32_t		props;			// ZtMFieldProp
  uint64_t		keys;

  ZtMFieldGet		get;
  ZtMFieldSet		set;

  const void *		(*constant)(int);

  template <typename Field>
  ZtMField(Field) :
      type{Field::Type::vtype()},
      id{Field::id()},
      props{Field::vprops()},
      keys{Field::keys()},
      get{Field::getFn()},
      set{Field::setFn()},
      constant{Field::constantFn()} { }

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

using ZtMFieldArray = ZuArray<const ZtMField *>;

template <typename MField, typename ...Fields>
struct ZtMFields_ {
  enum { N = sizeof...(Fields) };

  ZtMFieldArray	fields;

  static ZtMFields_ *instance() {
    return ZmSingleton<ZtMFields_>::instance();
  }

  ZtMFields_() {
    static const MField fields_[N] =
      // std::initializer_list<ZtMField>
    {
      MField{Fields{}}...
    };
    static const ZtMField *ptr_[N];
    ZuUnroll::all<ZuMkSeq<N>>([](auto i) {
      ptr_[i] = static_cast<const ZtMField *>(&fields_[i]);
    });
    fields = {&ptr_[0], N};
  }
};
template <typename O, typename MField = ZtMField>
inline ZtMFieldArray ZtMFields() {
  using MFields_ =
    ZuTypeApply<ZtMFields_, typename ZuFieldList<O>::template Prepend<MField>>;
  return MFields_::instance()->fields;
}

// --- generic run-time data transformation (for ORM, etc.)

namespace ZtField {

// load from external representation
struct Importer {
  ZtArray<ZtMFieldGet> get;		// one for each field
};
struct Import {
  const Importer	&importer;

  template <unsigned I, int Code, typename T>
  decltype(auto) get() const {
    auto p = static_cast<const void *>(this);
    if constexpr (Code == ZtFieldTypeCode::UDT)
      return
	static_cast<const T &>(*static_cast<const T *>(
	      importer.get[I].fn<Code>(p)));
    else if constexpr (Code == ZtFieldTypeCode::Int ||
		       Code == ZtFieldTypeCode::UInt ||
		       Code == ZtFieldTypeCode::Float)
      return static_cast<T>(importer.get[I].fn<Code>(p));
    else
      return importer.get[I].fn<Code>(p);
  }
};

// save to external representation
struct Exporter {
  ZtArray<ZtMFieldSet>	set;		// one for each field
};
struct Export {
  const Exporter	&exporter;

  template <unsigned I, int Code, typename U>
  void set(U &&v) {
    auto p = static_cast<void *>(this);
    return exporter.set[I].fn<Code>(p, ZuFwd<U>(v));
  }
};

namespace TypeCode = ZtFieldTypeCode;
namespace Prop = ZtFieldProp;

template <typename O>
struct Fielded_ {
  using AllFields = ZuFieldList<O>;

  template <typename U>
  struct LoadFilter : public ZuBool<!U::ReadOnly> { };
  using LoadFields = ZuTypeGrep<LoadFilter, AllFields>;

  template <typename U>
  struct UpdateFilter : public ZuTypeIn<Prop::Update, typename U::Props> { };
  using UpdateFields = ZuTypeGrep<UpdateFilter, LoadFields>;

  template <typename U> struct CtorFilter :
      public ZuBool<(Prop::GetCtor<typename U::Props>{} >= 0)> { };
  template <typename U>
  struct CtorIndex : public Prop::GetCtor<typename U::Props> { };
  using CtorFields =
    ZuTypeSort<CtorIndex, ZuTypeGrep<CtorFilter, AllFields>>;

  template <typename U> struct InitFilter :
      public ZuBool<(Prop::GetCtor<typename U::Props>{} < 0)> { };
  using InitFields = ZuTypeGrep<InitFilter, LoadFields>;

  template <typename Fields>
  struct Save {
    static void save(const O &o, Export &export_) {
      ZmAssert(export_.exporter.set.length() == AllFields::N);
      ZuUnroll::all<Fields>([&o, &export_]<typename Field>() {
	const auto &v = Field::get(o); // temporary lifetime-extension
	export_.set<ZuTypeIndex<Field, AllFields>{}, Field::Type::Code>(v);
      });
    }
  };
  static void save(const O &o, Export &export_) {
    Save<LoadFields>::save(o, export_);
  }
  static void saveUpdate(const O &o, Export &export_) {
    Save<UpdateFields>::save(o, export_);
  }

  template <typename ...Field>
  struct Ctor {
    static O ctor(const Import &import_) {
      ZmAssert(import_.importer.get.length() == AllFields::N);
      return O{
	import_.get<
	  ZuTypeIndex<Field, AllFields>{},
	  Field::Type::Code,
	  typename Field::T>()...
      };
    }
    static void ctor(void *o, const Import &import_) {
      ZmAssert(import_.importer.get.length() == AllFields::N);
      new (o) O{
	import_.get<
	  ZuTypeIndex<Field, AllFields>{},
	  Field::Type::Code,
	  typename Field::T>()...
      };
    }
  };
  static O ctor(const Import &import_) {
    ZmAssert(import_.importer.get.length() == AllFields::N);
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
    ZmAssert(import_.importer.get.length() == AllFields::N);
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
      ZmAssert(import_.importer.get.length() == AllFields::N);
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
    ZmAssert(import_.importer.get.length() == AllFields::N);
    ZuUnroll::all<LoadFields>([&o, &import_]<typename Field>() {
      Field::set(o,
	import_.get<
	  ZuTypeIndex<Field, AllFields>{},
	  Field::Type::Code,
	  typename Field::T>());
    });
  }
  static void update(O &o, const Import &import_) {
    ZmAssert(import_.importer.get.length() == AllFields::N);
    ZuUnroll::all<UpdateFields>([&o, &import_]<typename Field>() {
      Field::set(o,
	import_.get<
	  ZuTypeIndex<Field, AllFields>{},
	  Field::Type::Code,
	  typename Field::T>());
    });
  }

  template <typename ...Field>
  struct Key {
    using Tuple = ZuTuple<typename Field::T...>;
    static decltype(auto) tuple(const Import &import_) {
      ZmAssert(import_.importer.get.length() == AllFields::N);
      return Tuple{
	import_.get<
	  ZuTypeIndex<Field, AllFields>{},
	  Field::Type::Code,
	  typename Field::T>()...
      };
    }
  };
  template <typename ...Fields>
  struct Key<ZuTypeList<Fields...>> : public Key<Fields...> { };

  template <unsigned KeyID>
  struct KeyFilter {
    template <typename U> struct T : public ZuBool<U::keys() & (1<<KeyID)> { };
  };
  template <unsigned KeyID = 0>
  static auto key(const Import &import_) {
    using Fields = ZuTypeGrep<KeyFilter<KeyID>::template T, AllFields>;
    return Key<Fields>::tuple(import_);
  }
};
template <typename O>
using Fielded = Fielded_<ZuFielded<O>>;

template <typename O>
inline void save(const O &o, Export &export_) {
  Fielded<O>::save(o, export_);
}
template <typename O>
inline void saveUpdate(const O &o, Export &export_) {
  Fielded<O>::saveUpdate(o, export_);
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

template <typename O, unsigned KeyID = 0>
inline auto key(const Import &import_) {
  return Fielded<O>::template key<KeyID>(import_);
}

} // ZtField

#endif /* ZtField_HPP */
