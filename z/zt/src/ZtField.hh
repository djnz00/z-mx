//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// object introspection / reflection
// * extending ZuField
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
// that are injected into the ZuFieldProp namespace
//
// a Field is of the form:
// (((Accessor)[, (Props...)]), (Type[, Args...]))
//
// Example: (((id, Rd), (Keys<0>, Ctor<0>)), (String, "default"))
// Meaning: Read-only string field named "id" with a default
//   value of "default" that is also the containing object's zeroth
//   constructor parameter
//
// ZtField Type  C/C++ Type      ZtField Args
// ------------  ----------      ------------
// CString       char *          [, default]
// String        <String>        [, default]
// Bytes         <uint8_t[]>     [, default]
// Bool          <Integral>      [, default]
// Int<Size>     <Integral>      [, default, min, max]
// UInt<Size>    <Integral>      [, default, min, max]
// Enum, Map     <Integral>      [, default]
// Flags, Map    <Integral>      [, default]
// Float         <FloatingPoint> [, default, min, max]
// Fixed         ZuFixed         [, default, min, max]
// Decimal       ZuDecimal       [, default, min, max]
// Time          ZuTime          [, default]
// DateTime      ZuDateTime      [, default]
// UDT           <UDT>           [, default]
//
// *Vec          ZuArray<T>      [, default]
// CStringVec
// StringVec
// BytesVec
// Int<Size>Vec
// UInt<Size>Vec
// FloatVec
// FixedVec
// DecimalVec
// TimeVec
// DateTimeVec
// 
// Note: ZtMField provides run-time introspection via a monomorphic type -
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
//   Print	- Print<Fmt>{const T &} - compile-time formatted printing
//   mtype()	- ZtMFieldType * instance
// 
// ZtMFieldType provides:
//   code	- ZtFieldTypeCode
//   props	- ZtMFieldProp properties bitfield
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
//   print<Code>(auto &s, const void *o, const ZtMField *, const ZtFieldVFmt &)
//
// ZtMFieldSet provides:
//   set<Code>(void *o, auto &&v)
//   scan<Code>(void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &)
//
// ZtMFieldList<O>() returns the ZtMFields for O
// ZtMKeyFieldList<O>() returns the ZtMKeyFields for O
// ZtMKeyFieldList<O>()[KeyID] == ZtMFieldList<ZuFieldKeyT<O, KeyID>>()

#ifndef ZtField_HH
#define ZtField_HH

#ifndef ZtLib_HH
#include <zlib/ZtLib.hh>
#endif

#include <string.h>

#include <typeinfo>

#include <zlib/ZuArray.hh>
#include <zlib/ZuUnroll.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuString.hh>
#include <zlib/ZuBytes.hh>
#include <zlib/ZuInt.hh>
#include <zlib/ZuDecimal.hh>
#include <zlib/ZuFixed.hh>
#include <zlib/ZuTime.hh>
#include <zlib/ZuDateTime.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuBox.hh>
#include <zlib/ZuBase64.hh>
#include <zlib/ZuField.hh>
#include <zlib/ZuMArray.hh>

#include <zlib/ZmAlloc.hh>
#include <zlib/ZuMStream.hh>
#include <zlib/ZuTime.hh>
#include <zlib/ZmSingleton.hh>

#include <zlib/ZtEnum.hh>
#include <zlib/ZtString.hh>
#include <zlib/ZtRegex.hh>
#include <zlib/ZtScanBool.hh>

namespace ZtFieldTypeCode {
  ZtEnumValues(ZtFieldTypeCode,
    CString,		// C UTF-8 string (raw pointer), heap-allocated
    String,		// C++ contiguous UTF-8 string
    Bytes,		// byte array
    Bool,		// an integral type, interpreted as bool
    Int8,		// 8bit integer
    UInt8,		// 8bit unsigned integer
    Int16,		// 16bit integer
    UInt16,		// 16bit unsigned integer
    Int32,		// 32bit integer
    UInt32,		// 32bit unsigned integer
    Int64,		// 64bit integer
    UInt64,		// 64bit unsigned integer
    Int128,		// 128bit integer
    UInt128,		// 128bit unsigned integer
    Enum,		// an integral enumerated type
    Flags,		// an integral enumerated bitfield type
    Float,		// floating point type
    Fixed,		// ZuFixed
    Decimal,		// ZuDecimal
    Time,		// ZuTime
    DateTime,		// ZuDateTime - Julian date, seconds, nanoseconds
    UDT,		// generic udt type
    
    // XVec - vectors of X

    CStringVec,
    StringVec,
    BytesVec,
    Int8Vec,
    UInt8Vec,
    Int16Vec,
    UInt16Vec,
    Int32Vec,
    UInt32Vec,
    Int64Vec,
    UInt64Vec,
    Int128Vec,
    UInt128Vec,
    FloatVec,
    FixedVec,
    DecimalVec,
    TimeVec,
    DateTimeVec
  );
}

// extended compile-time field property list (see ZuFieldProp)
namespace ZuFieldProp {
  struct Synthetic { };		// synthetic (implies read-only)
  struct Update { };		// include in updates
  struct Hidden { };		// do not print
  struct Hex { };		// print hex value
  struct Required { };		// required - do not default
  struct Series { };		// data series column
  struct Index { };		// - index (e.g. time, nonce, offset, seq#)
  struct Delta { };		// - first derivative
  struct Delta2 { };		// - second derivative

  template <int8_t> struct NDP { }; // NDP for printing float/fixed/decimal

  // default NDP to 0
  template <typename Props, bool = HasValue<int8_t, NDP, Props>{}>
  struct GetNDP_ { using T = GetValue<int8_t, NDP, Props>; };
  template <typename Props>
  struct GetNDP_<Props, false> { using T = ZuInt<ZuCmp<int8_t>::null()>; };
  template <typename Props> using GetNDP = typename GetNDP_<Props>::T;
}

// compile-time formatted field printing/scanning
namespace ZtFieldFmt {
  using namespace ZuFmt;

  struct Default : public ZuFmt::Default {
    static ZuDateTimeScan::Any &DateScan_() {
      return ZmTLS([]{ return ZuDateTimeScan::Any{}; });
    }
    static ZuDateTimeFmt::Any &DatePrint_() {
      return ZmTLS([]{ return ZuDateTimeFmt::Any{}; });
    }
    static ZuString FlagsDelim() { return "|"; }

    // vector formatting
    static ZuString VecPrefix() { return "["; }
    static ZuString VecDelim() { return ", "; }
    static ZuString VecSuffix() { return "]"; }
  };

  // NTP - date/time scan format
  template <auto Scan, typename NTP = Default>
  struct DateScan : public NTP {
    constexpr static auto DateScan_ = Scan;
  };

  // NTP - date/time print format
  template <auto Print, typename NTP = Default>
  struct DatePrint : public NTP {
    constexpr static auto DatePrint_ = Print;
  };

  // NTP - flags formatting
  template <auto Delim, typename NTP = Default>
  struct Flags : public NTP {
    constexpr static auto FlagsDelim = Delim;
  };

  // NTP - vector formatting (none of these should have leading white space)
  template <auto Prefix, auto Delim, auto Suffix, typename NTP = Default>
  struct Vec : public NTP {
    constexpr static auto VecPrefix = Prefix;
    constexpr static auto VecDelim = Delim;
    constexpr static auto VecSuffix = Suffix;
  };
}

// run-time
struct ZtFieldVFmt {
  ZtFieldVFmt() { }

  template <typename Fmt>
  ZtFieldVFmt(Fmt fmt) :
    scalar{fmt},
    dateScan{Fmt::DateScan_()},
    datePrint{Fmt::DatePrint_()},
    flagsDelim{Fmt::FlagsDelim()},
    vecPrefix{Fmt::VecPrefix()},
    vecDelim{Fmt::VecDelim()},
    vecSuffix{Fmt::VecSuffix()} { }

  ZtFieldVFmt(const ZtFieldVFmt &) = default;
  ZtFieldVFmt &operator =(const ZtFieldVFmt &) = default;
  ZtFieldVFmt(ZtFieldVFmt &&) = default;
  ZtFieldVFmt &operator =(ZtFieldVFmt &&) = default;

  ZuVFmt		scalar;			// scalar format (print only)
  ZuDateTimeScan::Any	dateScan;		// date/time scan format
  ZuDateTimeFmt::Any	datePrint;		// date/time print format
  ZuString		flagsDelim = "|";	// flags delimiter

  // none of these should have leading white space
  ZuString		vecPrefix = "[";	// vector prefix
  ZuString		vecDelim = ", ";	// vector delimiter
  ZuString		vecSuffix = "]";	// vector suffix
};

// type properties are a subset of field properties
template <typename Prop> struct ZtFieldType_Props : public ZuFalse { };
template <> struct ZtFieldType_Props<ZuFieldProp::Hidden> : public ZuTrue { };
template <> struct ZtFieldType_Props<ZuFieldProp::Hex> : public ZuTrue { };

// ZtMFieldProp bitfield encapsulates introspected ZtField properties
namespace ZtMFieldProp {
  ZtEnumFlags(ZtMFieldProp, uint16_t,
    Ctor,
    Synthetic,
    Update,
    Hidden,
    Hex,
    Required,
    Series,
    Index,
    Delta,
    Delta2,
    NDP);

  template <T I> using Constant = ZuConstant<T, I>;

  // Value<Prop>::N - return bitfield for individual property
  template <typename> struct Value : public Constant<0> { };	// default

  template <unsigned I>
  struct Value<Constant<I>> : public Constant<I> { };

  namespace _ = ZuFieldProp;

  template <unsigned I>
  struct Value<_::Ctor<I>>               : public Constant<Ctor()> { };
  template <> struct Value<_::Synthetic> : public Constant<Synthetic()> { };
  template <> struct Value<_::Update>    : public Constant<Update()> { };
  template <> struct Value<_::Hidden>    : public Constant<Hidden()> { };
  template <> struct Value<_::Hex>       : public Constant<Hex()> { };
  template <> struct Value<_::Required>  : public Constant<Required()> { };
  template <> struct Value<_::Series>    : public Constant<Series()> { };
  template <> struct Value<_::Index>     : public Constant<Index()> { };
  template <> struct Value<_::Delta>     : public Constant<Delta()> { };
  template <> struct Value<_::Delta2>    : public Constant<Delta2()> { };
  template <int8_t I>
  struct Value<_::NDP<I>>                : public Constant<NDP()> { };

  // Value<List>::N - return bitfield for property list
  template <typename ...> struct Or_;
  template <> struct Or_<> {
    using T = Constant<0>;
  };
  template <typename U> struct Or_<U> {
    using T = Value<U>;
  };
  template <typename L, typename R> struct Or_<L, R> {
    using T = Constant<unsigned(Value<L>{}) | unsigned(Value<R>{})>;
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
  constexpr static ZtMFieldProp::T mprops() {
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
    typedef void (*Fn)(void *, ZuString, const ZtFieldVFmt &);
    return static_cast<Fn>(nullptr);
  }
};
template <typename T>
struct ZtFieldType_Scan<T, decltype((ZuDeclVal<T &>() = ZuString{}), void())> {
  static auto fn() {
    return [](void *ptr, ZuString s, const ZtFieldVFmt &) {
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
  void		(*print)(uint128_t, ZuMStream &, const ZtFieldVFmt &);
  uint128_t	(*scan)(ZuString, const ZtFieldVFmt &);
};
template <typename Map>
struct ZtMFieldFlags_ : public ZtMFieldFlags {
  ZtMFieldFlags_() : ZtMFieldFlags{
    .id = []() -> const char * { return Map::id(); },
    .print = [](uint128_t v, ZuMStream &s, const ZtFieldVFmt &fmt) -> void {
      s << Map::print(v, fmt.flagsDelim);
    },
    .scan = [](ZuString s, const ZtFieldVFmt &fmt) -> uint128_t {
      return Map::template scan<uint128_t>(s, fmt.flagsDelim);
    }
  } { }

  static ZtMFieldFlags *instance() {
    return ZmSingleton<ZtMFieldFlags_>::instance();
  }
};

typedef void (*ZtMFieldPrint)(const void *, ZuMStream &, const ZtFieldVFmt &);
typedef void (*ZtMFieldScan)(
  void (*)(void *, const void *), void *, ZuString, const ZtFieldVFmt &);

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

// monomorphic (untyped) equivalent of ZtField
struct ZtMField;

namespace ZtField_ {

// printing and string quoting
namespace Print {
  // C string quoting
  struct CString {
    const char *v;
    template <typename S>
    friend S &operator <<(S &s, const CString &print) {
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
  };

  // string quoting
  struct String {
    ZuString v;
    template <typename S>
    friend S &operator <<(S &s, const String &print) {
      const auto &v = print.v;
      s << '"';
      for (unsigned i = 0, n = v.length(); i < n; i++) {
	char c = v[i];
	if (ZuUnlikely(c == '"')) s << '\\';
	s << c;
      }
      return s << '"';
    }
  };

  // bytes printing (base64)
  struct Bytes {
    ZuBytes v;
    template <typename S>
    friend S &operator <<(S &s, const Bytes &print) {
      const auto &v = print.v;
      auto n = ZuBase64::enclen(v.length());
      auto buf_ = ZmAlloc(uint8_t, n);
      ZuArray<uint8_t> buf{&buf_[0], n};
      buf.trunc(ZuBase64::encode(buf, v));
      return s << ZuString{buf};
    }
  };
} // Print

// string and string vector element scanning
namespace Scan {
  constexpr static bool isspace__(char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
  }

  ZtExtern unsigned string(ZuArray<char> dst, ZuString &src);

  ZtExtern unsigned strElem(
    ZuArray<char> dst, ZuString &src,
    ZuString delim, ZuString suffix);
} // Scan

using CStringVec = ZuMArray<const char *>;
using StringVec = ZuMArray<ZuString>;
using BytesVec = ZuMArray<ZuBytes>;
using Int8Vec = ZuMArray<int8_t>;
using UInt8Vec = ZuMArray<uint8_t>;
using Int16Vec = ZuMArray<int16_t>;
using UInt16Vec = ZuMArray<uint16_t>;
using Int32Vec = ZuMArray<int32_t>;
using UInt32Vec = ZuMArray<uint32_t>;
using Int64Vec = ZuMArray<int64_t>;
using UInt64Vec = ZuMArray<uint64_t>;
using Int128Vec = ZuMArray<int128_t>;
using UInt128Vec = ZuMArray<uint128_t>;
using FloatVec = ZuMArray<double>;
using FixedVec = ZuMArray<ZuFixed>;
using DecimalVec = ZuMArray<ZuDecimal>;
using TimeVec = ZuMArray<ZuTime>;
using DateTimeVec = ZuMArray<ZuDateTime>;

// monomorphic field get/print
struct MGet {

  union {
    void		*null;

    const char *	(*cstring)(const void *);	// CString
    ZuString		(*string)(const void *);	// String
    ZuBytes		(*bytes)(const void *);		// Bytes
    bool		(*bool_)(const void *);		// Bool
    int8_t		(*int8)(const void *);		// Int8
    uint8_t		(*uint8)(const void *);		// UInt8
    int16_t		(*int16)(const void *);		// Int16
    uint16_t		(*uint16)(const void *);	// UInt16
    int32_t		(*int32)(const void *);		// Int32
    uint32_t		(*uint32)(const void *);	// UInt32
    int64_t		(*int64)(const void *);		// Int64
    uint64_t		(*uint64)(const void *);	// UInt64
    int128_t		(*int128)(const void *);	// Int128
    uint128_t		(*uint128)(const void *);	// UInt128
    int			(*enum_)(const void *);		// Enum
    uint128_t		(*flags)(const void *);		// Flags
    double		(*float_)(const void *);	// Float
    ZuFixed		(*fixed)(const void *);		// Fixed
    ZuDecimal		(*decimal)(const void *);	// Decimal
    ZuTime		(*time)(const void *);		// Time
    ZuDateTime		(*dateTime)(const void *);	// DateTime
    const void *	(*udt)(const void *);		// UDT

    CStringVec		(*cstringVec)(const void *);
    StringVec		(*stringVec)(const void *);
    BytesVec		(*bytesVec)(const void *);
    Int8Vec		(*int8Vec)(const void *);
    UInt8Vec		(*uint8Vec)(const void *);
    Int16Vec		(*int16Vec)(const void *);
    UInt16Vec		(*uint16Vec)(const void *);
    Int32Vec		(*int32Vec)(const void *);
    UInt32Vec		(*uint32Vec)(const void *);
    Int64Vec		(*int64Vec)(const void *);
    UInt64Vec		(*uint64Vec)(const void *);
    Int128Vec		(*int128Vec)(const void *);
    UInt128Vec		(*uint128Vec)(const void *);
    FloatVec		(*floatVec)(const void *);
    FixedVec		(*fixedVec)(const void *);
    DecimalVec		(*decimalVec)(const void *);
    TimeVec		(*timeVec)(const void *);
    DateTimeVec		(*dateTimeVec)(const void *);
  } get_;

#define ZtMField_GetFn(code, type, fn) \
  template <unsigned Code> \
  ZuIfT<Code == ZtFieldTypeCode::code, type> \
  get(const void *o) const { return get_.fn(o); }

  ZtMField_GetFn(CString, const char *, cstring)
  ZtMField_GetFn(String, ZuString, string)
  ZtMField_GetFn(Bytes, ZuBytes, bytes)
  ZtMField_GetFn(Bool, bool, bool_)
  ZtMField_GetFn(Int8, int8_t, int8)
  ZtMField_GetFn(UInt8, uint8_t, uint8)
  ZtMField_GetFn(Int16, int16_t, int16)
  ZtMField_GetFn(UInt16, uint16_t, uint16)
  ZtMField_GetFn(Int32, int32_t, int32)
  ZtMField_GetFn(UInt32, uint32_t, uint32)
  ZtMField_GetFn(Int64, int64_t, int64)
  ZtMField_GetFn(UInt64, uint64_t, uint64)
  ZtMField_GetFn(Int128, int128_t, int128)
  ZtMField_GetFn(UInt128, uint128_t, uint128)
  ZtMField_GetFn(Enum, int, enum_)
  ZtMField_GetFn(Flags, uint64_t, flags)
  ZtMField_GetFn(Float, double, float_)
  ZtMField_GetFn(Fixed, ZuFixed, fixed)
  ZtMField_GetFn(Decimal, ZuDecimal, decimal)
  ZtMField_GetFn(Time, ZuTime, time)
  ZtMField_GetFn(DateTime, ZuDateTime, dateTime)
  ZtMField_GetFn(UDT, const void *, udt)
  ZtMField_GetFn(CStringVec, CStringVec, cstringVec)
  ZtMField_GetFn(StringVec, StringVec, stringVec)
  ZtMField_GetFn(BytesVec, BytesVec, bytesVec)
  ZtMField_GetFn(Int8Vec, Int8Vec, int8Vec)
  ZtMField_GetFn(UInt8Vec, UInt8Vec, uint8Vec)
  ZtMField_GetFn(Int16Vec, Int16Vec, int16Vec)
  ZtMField_GetFn(UInt16Vec, UInt16Vec, uint16Vec)
  ZtMField_GetFn(Int32Vec, Int32Vec, int32Vec)
  ZtMField_GetFn(UInt32Vec, UInt32Vec, uint32Vec)
  ZtMField_GetFn(Int64Vec, Int64Vec, int64Vec)
  ZtMField_GetFn(UInt64Vec, UInt64Vec, uint64Vec)
  ZtMField_GetFn(Int128Vec, Int128Vec, int128Vec)
  ZtMField_GetFn(UInt128Vec, UInt128Vec, uint128Vec)
  ZtMField_GetFn(FloatVec, FloatVec, floatVec)
  ZtMField_GetFn(FixedVec, FixedVec, fixedVec)
  ZtMField_GetFn(DecimalVec, DecimalVec, decimalVec)
  ZtMField_GetFn(TimeVec, TimeVec, timeVec)
  ZtMField_GetFn(DateTimeVec, DateTimeVec, dateTimeVec)

#define ZtMField_PrintFn(Code_) \
  template <unsigned Code, typename S> \
  ZuIfT<Code == ZtFieldTypeCode::Code_> \
  print(S &, const void *, const ZtMField *, const ZtFieldVFmt &) const;

  ZtMField_PrintFn(CString)
  ZtMField_PrintFn(String)
  ZtMField_PrintFn(Bytes)
  ZtMField_PrintFn(Bool)
  ZtMField_PrintFn(Int8)
  ZtMField_PrintFn(UInt8)
  ZtMField_PrintFn(Int16)
  ZtMField_PrintFn(UInt16)
  ZtMField_PrintFn(Int32)
  ZtMField_PrintFn(UInt32)
  ZtMField_PrintFn(Int64)
  ZtMField_PrintFn(UInt64)
  ZtMField_PrintFn(Int128)
  ZtMField_PrintFn(UInt128)
  ZtMField_PrintFn(Enum)
  ZtMField_PrintFn(Flags)
  ZtMField_PrintFn(Float)
  ZtMField_PrintFn(Fixed)
  ZtMField_PrintFn(Decimal)
  ZtMField_PrintFn(Time)
  ZtMField_PrintFn(DateTime)
  ZtMField_PrintFn(UDT)
  ZtMField_PrintFn(CStringVec)
  ZtMField_PrintFn(StringVec)
  ZtMField_PrintFn(BytesVec)
  ZtMField_PrintFn(Int8Vec)
  ZtMField_PrintFn(UInt8Vec)
  ZtMField_PrintFn(Int16Vec)
  ZtMField_PrintFn(UInt16Vec)
  ZtMField_PrintFn(Int32Vec)
  ZtMField_PrintFn(UInt32Vec)
  ZtMField_PrintFn(Int64Vec)
  ZtMField_PrintFn(UInt64Vec)
  ZtMField_PrintFn(Int128Vec)
  ZtMField_PrintFn(UInt128Vec)
  ZtMField_PrintFn(FloatVec)
  ZtMField_PrintFn(FixedVec)
  ZtMField_PrintFn(DecimalVec)
  ZtMField_PrintFn(TimeVec)
  ZtMField_PrintFn(DateTimeVec)
};

// monomorphic field set/scan
struct MSet {
  union {
    void	*null;

    void	(*cstring)(void *, const char *);	// CString
    void	(*string)(void *, ZuString);		// String
    void	(*bytes)(void *, ZuBytes);		// Bytes
    void	(*bool_)(void *, bool);			// Bool
    void	(*int8)(void *, int8_t);		// Int8
    void	(*uint8)(void *, uint8_t);		// UInt8
    void	(*int16)(void *, int16_t);		// Int16
    void	(*uint16)(void *, uint16_t);		// UInt16
    void	(*int32)(void *, int32_t);		// Int32
    void	(*uint32)(void *, uint32_t);		// UInt32
    void	(*int64)(void *, int64_t);		// Int64
    void	(*uint64)(void *, uint64_t);		// UInt64
    void	(*int128)(void *, int128_t);		// Int128
    void	(*uint128)(void *, uint128_t);		// UInt128
    void	(*enum_)(void *, int);			// Enum
    void	(*flags)(void *, uint128_t);		// Flags
    void	(*float_)(void *, double);		// Float
    void	(*fixed)(void *, ZuFixed);		// Fixed
    void	(*decimal)(void *, ZuDecimal);		// Decimal
    void	(*time)(void *, ZuTime);		// Time
    void	(*dateTime)(void *, ZuDateTime);	// DateTime
    void	(*udt)(void *, const void *);		// UDT

    void	(*cstringVec)(void *, CStringVec);
    void	(*stringVec)(void *, StringVec);
    void	(*bytesVec)(void *, BytesVec);
    void	(*int8Vec)(void *, Int8Vec);
    void	(*uint8Vec)(void *, UInt8Vec);
    void	(*int16Vec)(void *, Int16Vec);
    void	(*uint16Vec)(void *, UInt16Vec);
    void	(*int32Vec)(void *, Int32Vec);
    void	(*uint32Vec)(void *, UInt32Vec);
    void	(*int64Vec)(void *, Int64Vec);
    void	(*uint64Vec)(void *, UInt64Vec);
    void	(*int128Vec)(void *, Int128Vec);
    void	(*uint128Vec)(void *, UInt128Vec);
    void	(*floatVec)(void *, FloatVec);
    void	(*fixedVec)(void *, FixedVec);
    void	(*decimalVec)(void *, DecimalVec);
    void	(*timeVec)(void *, TimeVec);
    void	(*dateTimeVec)(void *, DateTimeVec);
  } set_;

#define ZtMField_SetFn(code, type, fn) \
  template <unsigned Code> \
  ZuIfT<Code == ZtFieldTypeCode::code> \
  set(void *o, type v) const { set_.fn(o, v); }

  ZtMField_SetFn(CString, const char *, cstring)
  ZtMField_SetFn(String, ZuString, string)
  ZtMField_SetFn(Bytes, ZuBytes, bytes)
  ZtMField_SetFn(Bool, bool, bool_)
  ZtMField_SetFn(Int8, int8_t, int8)
  ZtMField_SetFn(UInt8, uint8_t, uint8)
  ZtMField_SetFn(Int16, int16_t, int16)
  ZtMField_SetFn(UInt16, uint16_t, uint16)
  ZtMField_SetFn(Int32, int32_t, int32)
  ZtMField_SetFn(UInt32, uint32_t, uint32)
  ZtMField_SetFn(Int64, int64_t, int64)
  ZtMField_SetFn(UInt64, uint64_t, uint64)
  ZtMField_SetFn(Int128, int128_t, int128)
  ZtMField_SetFn(UInt128, uint128_t, uint128)
  ZtMField_SetFn(Enum, int, enum_)
  ZtMField_SetFn(Flags, uint64_t, flags)
  ZtMField_SetFn(Float, double, float_)
  ZtMField_SetFn(Fixed, ZuFixed, fixed)
  ZtMField_SetFn(Decimal, ZuDecimal, decimal)
  ZtMField_SetFn(Time, ZuTime, time)
  ZtMField_SetFn(DateTime, ZuDateTime, dateTime)
  ZtMField_SetFn(UDT, const void *, udt)
  ZtMField_SetFn(CStringVec, CStringVec, cstringVec)
  ZtMField_SetFn(StringVec, StringVec, stringVec)
  ZtMField_SetFn(BytesVec, BytesVec, bytesVec)
  ZtMField_SetFn(Int8Vec, Int8Vec, int8Vec)
  ZtMField_SetFn(UInt8Vec, UInt8Vec, uint8Vec)
  ZtMField_SetFn(Int16Vec, Int16Vec, int16Vec)
  ZtMField_SetFn(UInt16Vec, UInt16Vec, uint16Vec)
  ZtMField_SetFn(Int32Vec, Int32Vec, int32Vec)
  ZtMField_SetFn(UInt32Vec, UInt32Vec, uint32Vec)
  ZtMField_SetFn(Int64Vec, Int64Vec, int64Vec)
  ZtMField_SetFn(UInt64Vec, UInt64Vec, uint64Vec)
  ZtMField_SetFn(Int128Vec, Int128Vec, int128Vec)
  ZtMField_SetFn(UInt128Vec, UInt128Vec, uint128Vec)
  ZtMField_SetFn(FloatVec, FloatVec, floatVec)
  ZtMField_SetFn(FixedVec, FixedVec, fixedVec)
  ZtMField_SetFn(DecimalVec, DecimalVec, decimalVec)
  ZtMField_SetFn(TimeVec, TimeVec, timeVec)
  ZtMField_SetFn(DateTimeVec, DateTimeVec, dateTimeVec)

#define ZtMField_ScanFn(code) \
  template <unsigned Code> \
  ZuIfT<Code == ZtFieldTypeCode::code> \
  scan(void *, ZuString, const ZtMField *, const ZtFieldVFmt &) const;

  ZtMField_ScanFn(CString)
  ZtMField_ScanFn(String)
  ZtMField_ScanFn(Bytes)
  ZtMField_ScanFn(Bool)
  ZtMField_ScanFn(Int8)
  ZtMField_ScanFn(UInt8)
  ZtMField_ScanFn(Int16)
  ZtMField_ScanFn(UInt16)
  ZtMField_ScanFn(Int32)
  ZtMField_ScanFn(UInt32)
  ZtMField_ScanFn(Int64)
  ZtMField_ScanFn(UInt64)
  ZtMField_ScanFn(Int128)
  ZtMField_ScanFn(UInt128)
  ZtMField_ScanFn(Enum)
  ZtMField_ScanFn(Flags)
  ZtMField_ScanFn(Float)
  ZtMField_ScanFn(Fixed)
  ZtMField_ScanFn(Decimal)
  ZtMField_ScanFn(Time)
  ZtMField_ScanFn(DateTime)
  ZtMField_ScanFn(UDT)
  ZtMField_ScanFn(CStringVec)
  ZtMField_ScanFn(StringVec)
  ZtMField_ScanFn(BytesVec)
  ZtMField_ScanFn(Int8Vec)
  ZtMField_ScanFn(UInt8Vec)
  ZtMField_ScanFn(Int16Vec)
  ZtMField_ScanFn(UInt16Vec)
  ZtMField_ScanFn(Int32Vec)
  ZtMField_ScanFn(UInt32Vec)
  ZtMField_ScanFn(Int64Vec)
  ZtMField_ScanFn(UInt64Vec)
  ZtMField_ScanFn(Int128Vec)
  ZtMField_ScanFn(UInt128Vec)
  ZtMField_ScanFn(FloatVec)
  ZtMField_ScanFn(FixedVec)
  ZtMField_ScanFn(DecimalVec)
  ZtMField_ScanFn(TimeVec)
  ZtMField_ScanFn(DateTimeVec)
};

} // ZtField_

using ZtMFieldGet = ZtField_::MGet;
using ZtMFieldSet = ZtField_::MSet;

// ZtMField is the monomorphic (untyped) equivalent of ZtField
struct ZtMField {
  ZtMFieldType		*type;
  const char		*id;
  uint128_t		props;			// ZtMFieldProp
  uint64_t		keys;
  uint16_t		ctor;
  int8_t		ndp;

  ZtMFieldGet		get;
  ZtMFieldSet		set;

  ZtMFieldGet		constant;

  template <typename Field>
  ZtMField(Field) :
      type{Field::Type::mtype()},
      id{Field::id()},
      props{Field::mprops()},
      keys{ZuSeqBitmap<ZuFieldProp::GetKeys<typename Field::Props>>()},
      ctor{ZuFieldProp::GetCtor<typename Field::Props>{}},
      ndp{ZuFieldProp::GetNDP<typename Field::Props>{}},
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
    s << " props=" << ZtMFieldProp::Map::print(props);
    if (props & ZtMFieldProp::Ctor()) {
      if (props & ~(ZtMFieldProp::Ctor() | ZtMFieldProp::NDP())) s << '|';
      s << "Ctor(" << ctor << ')';
    }
    if (props & ZtMFieldProp::NDP()) {
      if (props & ~ZtMFieldProp::NDP()) s << '|';
      s << "NDP(" << int(ndp) << ')';
    }
    s << " keys=" << ZuBoxed(keys).hex();
  }
  friend ZuPrintLambda<[]() {
    return [](auto &s, const auto &v) { v.print_(s); };
  }> ZuPrintType(ZtMField *);
};

namespace ZtField_ {

// ZtMFieldGet print functions
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::CString>
MGet::print(
  S &s, const void *o, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  auto v = get_.cstring(o);
  s << Print::CString{v};
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::String>
MGet::print(
  S &s, const void *o, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  auto v = get_.string(o);
  s << Print::String{v};
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Bytes>
MGet::print(
  S &s, const void *o, const ZtMField *, const ZtFieldVFmt &
) const {
  ZuBytes v = get_.bytes(o);
  auto n = ZuBase64::enclen(v.length());
  auto buf_ = ZmAlloc(uint8_t, n);
  ZuArray<uint8_t> buf{&buf_[0], n};
  buf.trunc(ZuBase64::encode(buf, v));
  s << ZuString{buf};
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Bool>
MGet::print(
  S &s, const void *o, const ZtMField *, const ZtFieldVFmt &
) const {
  s << (get_.bool_(o) ? '1' : '0');
}

#define ZtMField_printInt(width) \
template <unsigned Code, typename S> \
inline ZuIfT<Code == ZtFieldTypeCode::Int##width> \
MGet::print( \
  S &s, const void *o, const ZtMField *field, \
  const ZtFieldVFmt &fmt \
) const { \
  ZuBox<int##width##_t> v = get_.int##width(o); \
  if (field->props & ZtMFieldProp::Hex()) \
    s << v.vfmt(fmt.scalar).hex(); \
  else \
    s << v.vfmt(fmt.scalar); \
} \
template <unsigned Code, typename S> \
inline ZuIfT<Code == ZtFieldTypeCode::UInt##width> \
MGet::print( \
  S &s, const void *o, const ZtMField *field, \
  const ZtFieldVFmt &fmt \
) const { \
  ZuBox<uint##width##_t> v = get_.uint##width(o); \
  if (field->props & ZtMFieldProp::Hex()) \
    s << v.vfmt(fmt.scalar).hex(); \
  else \
    s << v.vfmt(fmt.scalar); \
}
ZtMField_printInt(8)
ZtMField_printInt(16)
ZtMField_printInt(32)
ZtMField_printInt(64)
ZtMField_printInt(128)

template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Enum>
MGet::print(
  S &s, const void *o, const ZtMField *field, const ZtFieldVFmt &
) const {
  s << field->type->info.enum_()->v2s(get_.enum_(o));
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Flags>
MGet::print(
  S &s_, const void *o, const ZtMField *field, const ZtFieldVFmt &fmt
) const {
  ZuMStream s{s_};
  field->type->info.flags()->print(get_.flags(o), s, fmt);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Float>
MGet::print(
  S &s, const void *o, const ZtMField *field, const ZtFieldVFmt &fmt
) const {
  ZuBox<double> v = get_.float_(o);
  auto ndp = field->ndp;
  if (!ZuCmp<decltype(ndp)>::null(ndp))
    s << v.vfmt(fmt.scalar).fp(ndp);
  else
    s << v.vfmt(fmt.scalar);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Fixed>
MGet::print(
  S &s, const void *o, const ZtMField *field, const ZtFieldVFmt &fmt
) const {
  ZuFixed v = get_.fixed(o);
  auto ndp = field->ndp;
  if (!ZuCmp<decltype(ndp)>::null(ndp))
    s << v.vfmt(fmt.scalar).fp(ndp);
  else
    s << v.vfmt(fmt.scalar);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Decimal>
MGet::print(
  S &s, const void *o, const ZtMField *field, const ZtFieldVFmt &fmt
) const {
  ZuDecimal v = get_.decimal(o);
  auto ndp = field->ndp;
  if (!ZuCmp<decltype(ndp)>::null(ndp))
    s << v.vfmt(fmt.scalar).fp(ndp);
  else
    s << v.vfmt(fmt.scalar);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Time>
MGet::print(
  S &s, const void *o, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  ZuDateTime v{get_.time(o)};
  s << v.fmt(fmt.datePrint);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::DateTime>
MGet::print(
  S &s, const void *o, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  ZuDateTime v{get_.dateTime(o)};
  s << v.fmt(fmt.datePrint);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::UDT>
MGet::print(
  S &s_, const void *o, const ZtMField *field, const ZtFieldVFmt &fmt
) const {
  ZuMStream s{s_};
  field->type->info.udt()->print(get_.udt(o), s, fmt);
}

template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::CStringVec>
MGet::print(
  S &s, const void *o, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  CStringVec vec{get_.cstringVec(o)};
  vec.all([&s, &first, &fmt](const char *v) {
    if (!first) s << fmt.vecDelim; else first = false;
    s << Print::CString{v};
  });
  s << fmt.vecSuffix;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::StringVec>
MGet::print(
  S &s, const void *o, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  StringVec vec{get_.stringVec(o)};
  vec.all([&s, &first, &fmt](ZuString v) {
    if (!first) s << fmt.vecDelim; else first = false;
    s << Print::String{v};
  });
  s << fmt.vecSuffix;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::BytesVec>
MGet::print(
  S &s, const void *o, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  BytesVec vec{get_.bytesVec(o)};
  vec.all([&s, &fmt, &first](ZuBytes v) {
    if (!first) s << fmt.vecDelim; else first = false;
    s << Print::Bytes{v};
  });
  s << fmt.vecSuffix;
}

#define ZtMField_printIntVec(width) \
template <unsigned Code, typename S> \
inline ZuIfT<Code == ZtFieldTypeCode::Int##width##Vec> \
MGet::print( \
  S &s, const void *o, const ZtMField *field, const ZtFieldVFmt &fmt \
) const { \
  s << fmt.vecPrefix; \
  Int##width##Vec vec{get_.int##width##Vec(o)}; \
  bool first = true; \
  if (field->props & ZtMFieldProp::Hex()) \
    vec.all([&s, &fmt, &first](ZuBox<int##width##_t> v) { \
      if (!first) s << fmt.vecDelim; else first = false; \
      s << v.vfmt(fmt.scalar).hex(); \
    }); \
  else \
    vec.all([&s, &first, &fmt](ZuBox<int##width##_t> v) { \
      if (!first) s << fmt.vecDelim; else first = false; \
      s << v.vfmt(fmt.scalar); \
    }); \
  s << fmt.vecSuffix; \
} \
template <unsigned Code, typename S> \
inline ZuIfT<Code == ZtFieldTypeCode::UInt##width##Vec> \
MGet::print( \
  S &s, const void *o, const ZtMField *field, const ZtFieldVFmt &fmt \
) const { \
  s << fmt.vecPrefix; \
  UInt##width##Vec vec{get_.uint##width##Vec(o)}; \
  bool first = true; \
  if (field->props & ZtMFieldProp::Hex()) \
    vec.all([&s, &fmt, &first](ZuBox<uint##width##_t> v) { \
      if (!first) s << fmt.vecDelim; else first = false; \
      s << v.vfmt(fmt.scalar).hex(); \
    }); \
  else \
    vec.all([&s, &first, &fmt](ZuBox<uint##width##_t> v) { \
      if (!first) s << fmt.vecDelim; else first = false; \
      s << v.vfmt(fmt.scalar); \
    }); \
  s << fmt.vecSuffix; \
}
ZtMField_printIntVec(8)
ZtMField_printIntVec(16)
ZtMField_printIntVec(32)
ZtMField_printIntVec(64)
ZtMField_printIntVec(128)

template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::FloatVec>
MGet::print(
  S &s, const void *o, const ZtMField *field, const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  FloatVec vec{get_.floatVec(o)};
  auto ndp = field->ndp;
  bool first = true;
  if (!ZuCmp<decltype(ndp)>::null(ndp))
    vec.all([&s, &fmt, ndp, &first](ZuBox<double> v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar).fp(ndp);
    });
  else
    vec.all([&s, &first, &fmt](ZuBox<double> v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar);
    });
  s << fmt.vecSuffix;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::FixedVec>
MGet::print(
  S &s, const void *o, const ZtMField *field, const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  FixedVec vec{get_.fixedVec(o)};
  auto ndp = field->ndp;
  if (!ZuCmp<decltype(ndp)>::null(ndp))
    vec.all([&s, &fmt, ndp, &first](const ZuFixed &v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar).fp(ndp);
    });
  else
    vec.all([&s, &first, &fmt](const ZuFixed &v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar);
    });
  s << fmt.vecSuffix;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::DecimalVec>
MGet::print(
  S &s, const void *o, const ZtMField *field, const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  DecimalVec vec{get_.decimalVec(o)};
  auto ndp = field->ndp;
  bool first = true;
  if (!ZuCmp<decltype(ndp)>::null(ndp))
    vec.all([&s, &fmt, ndp, &first](const ZuDecimal &v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar).fp(ndp);
    });
  else
    vec.all([&s, &first, &fmt](const ZuDecimal &v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar);
    });
  s << fmt.vecSuffix;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::TimeVec>
MGet::print(
  S &s, const void *o, const ZtMField *field, const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  TimeVec vec{get_.timeVec(o)};
  vec.all([&s, &fmt, &first](const ZuTime &v_) {
    ZuDateTime v{v_};
    if (!first) s << fmt.vecDelim; else first = false;
    s << v.fmt(fmt.datePrint);
  });
  s << fmt.vecSuffix;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::DateTimeVec>
MGet::print(
  S &s, const void *o, const ZtMField *field, const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  DateTimeVec vec{get_.dateTimeVec(o)};
  vec.all([&s, &fmt, &first](const ZuDateTime &v) {
    if (!first) s << fmt.vecDelim; else first = false;
    s << v.fmt(fmt.datePrint);
  });
  s << fmt.vecSuffix;
}

// ZtMFieldSet scan functions

template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::CString>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  if (!s) {
    set_.cstring(o, nullptr);
    return;
  }
  unsigned n = s.length() + 1;
  auto buf_ = ZmAlloc(char, n);
  ZuArray<char> buf{&buf_[0], n};
  buf[Scan::string(buf, s)] = 0;
  set_.cstring(o, &buf[0]);
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::String>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  if (!s) {
    set_.string(o, s);
    return;
  }
  unsigned n = s.length();
  auto buf_ = ZmAlloc(char, n);
  ZuArray<char> buf{&buf_[0], n};
  buf.trunc(Scan::string(buf, s));
  set_.string(o, buf);
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Bytes>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  auto n = ZuBase64::declen(s.length());
  auto buf_ = ZmAlloc(uint8_t, n);
  ZuArray<uint8_t> buf{&buf_[0], n};
  buf.trunc(ZuBase64::decode(buf, ZuBytes{s}));
  set_.bytes(o, buf);
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Bool>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  set_.bool_(o, ZtScanBool(s));
}

#define ZtMField_scanInt(width) \
template <unsigned Code> \
inline ZuIfT<Code == ZtFieldTypeCode::Int##width> \
MSet::scan( \
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt & \
) const { \
  set_.int##width(o, ZuBox<int##width##_t>{s}); \
} \
template <unsigned Code> \
inline ZuIfT<Code == ZtFieldTypeCode::UInt##width> \
MSet::scan( \
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt & \
) const { \
  set_.uint##width(o, ZuBox<uint##width##_t>{s}); \
}
ZtMField_scanInt(8)
ZtMField_scanInt(16)
ZtMField_scanInt(32)
ZtMField_scanInt(64)
ZtMField_scanInt(128)

template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Enum>
MSet::scan(
  void *o, ZuString s, const ZtMField *field, const ZtFieldVFmt &
) const {
  set_.enum_(o, field->type->info.enum_()->s2v(s));
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Flags>
MSet::scan(
  void *o, ZuString s, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  set_.flags(o, field->type->info.flags()->scan(s, fmt));
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Float>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  set_.float_(o, ZuBox<double>{s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Fixed>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  set_.fixed(o, ZuFixed{s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Decimal>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  set_.decimal(o, ZuDecimal{s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Time>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  set_.time(o, ZuDateTime{fmt.dateScan, s}.as_time());
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::DateTime>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  set_.dateTime(o, ZuDateTime{fmt.dateScan, s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::UDT>
MSet::scan(
  void *o, ZuString s, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  field->type->info.udt()->scan(field->set.set_.udt, o, s, fmt);
}

namespace VecScan {

  using namespace Scan;

  inline bool match(ZuString &s, ZuString m) {
    unsigned n = m.length();
    if (s.length() < n || memcmp(&s[0], &m[0], n)) return false;
    s.offset(n);
    return true;
  }
  inline void skip(ZuString &s) {
    while (s.length() && isspace__(s[0])) s.offset(1);
  }

  // this is intentionally a 1-pass scan that does NOT validate the suffix;
  // lambda(ZuString &s) should advance s with s.offset()
  template <typename L>
  inline unsigned scan(ZuString &s, const ZtFieldVFmt &fmt, L l) {
    auto begin = &s[0];
    skip(s);
    if (!match(s, fmt.vecPrefix)) return 0;
    skip(s);
    while (l(s)) {
      skip(s);
      if (!match(s, fmt.vecDelim)) {
	match(s, fmt.vecSuffix);
	break;
      }
    }
    return &s[0] - begin;
  }

} // VecScan

template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::CStringVec>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, fmt, [this, o, &fmt](ZuString &s) {
    auto m = s.length();
    auto buf_ = ZmAlloc(char, m + 1);
    ZuArray<char> buf{&buf_[0], m + 1};
    unsigned n = Scan::strElem(buf, s, fmt.vecDelim, fmt.vecSuffix);
    if (n) {
      buf[n] = 0;
      set_.cstring(o, &buf[0]);
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::StringVec>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, fmt, [this, o, &fmt](ZuString &s) {
    auto m = s.length();
    auto buf_ = ZmAlloc(char, m);
    ZuArray<char> buf{&buf_[0], m};
    unsigned n = Scan::strElem(buf, s, fmt.vecDelim, fmt.vecSuffix);
    if (n) {
      buf.trunc(n);
      set_.string(o, ZuString{&buf[0], buf.length()});
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::BytesVec>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, fmt, [this, o](ZuString &s) {
    unsigned n = 0;
    auto m = s.length();
    while (n < m && ZuBase64::is(s[n])) n++;
    n = ZuBase64::declen(m = n);
    if (n) {
      auto buf_ = ZmAlloc(uint8_t, n);
      ZuArray<uint8_t> buf{&buf_[0], n};
      buf.trunc(ZuBase64::decode(buf, ZuBytes{s}));
      set_.bytes(o, ZuBytes{&buf[0], buf.length()});
      s.offset(m);
      return true;
    }
    return false;
  });
}

#define ZtMField_scanIntVec(width) \
template <unsigned Code> \
inline ZuIfT<Code == ZtFieldTypeCode::Int##width##Vec> \
MSet::scan( \
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt \
) const { \
  VecScan::scan(s, fmt, [this, o](ZuString &s) { \
    ZuBox<int##width##_t> v; \
    unsigned n = v.scan(s); \
    if (n) { \
      set_.int##width(o, v); \
      s.offset(n); \
      return true; \
    } \
    return false; \
  }); \
} \
template <unsigned Code> \
inline ZuIfT<Code == ZtFieldTypeCode::UInt##width##Vec> \
MSet::scan( \
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt \
) const { \
  VecScan::scan(s, fmt, [this, o](ZuString &s) { \
    ZuBox<uint##width##_t> v; \
    unsigned n = v.scan(s); \
    if (n) { \
      set_.uint##width(o, v); \
      s.offset(n); \
      return true; \
    } \
    return false; \
  }); \
}
ZtMField_scanIntVec(8)
ZtMField_scanIntVec(16)
ZtMField_scanIntVec(32)
ZtMField_scanIntVec(64)
ZtMField_scanIntVec(128)

template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::FloatVec>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, fmt, [this, o](ZuString &s) {
    ZuBox<double> v;
    unsigned n = v.scan(s);
    if (n) {
      set_.float_(o, v);
      s.offset(n);
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::FixedVec>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, fmt, [this, o](ZuString &s) {
    ZuFixed v;
    unsigned n = v.scan(s);
    if (n) {
      set_.fixed(o, v);
      s.offset(n);
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::DecimalVec>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, fmt, [this, o](ZuString &s) {
    ZuDecimal v;
    unsigned n = v.scan(s);
    if (n) {
      set_.decimal(o, v);
      s.offset(n);
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::TimeVec>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, fmt, [this, o, &fmt](ZuString &s) {
    ZuDateTime v;
    unsigned n = v.scan(fmt.dateScan, s);
    if (n) {
      set_.time(o, v.as_time());
      s.offset(n);
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::DateTimeVec>
MSet::scan(
  void *o, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, fmt, [this, o, &fmt](ZuString &s) {
    ZuDateTime v;
    unsigned n = v.scan(fmt.dateScan, s);
    if (n) {
      set_.dateTime(o, ZuMv(v));
      s.offset(n);
      return true;
    }
    return false;
  });
}

} // ZtField_

// ZtField compile-time encapsulates an individual field, derives from ZuField
template <typename Base_>
struct ZtField : public Base_ {
  using Base = Base_;
  using Orig = Base;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
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
  using T = char *;
  using Props = Props_;
  template <typename = ZtFieldFmt::Default>
  using Print = ZtField_::Print::CString;
  inline static ZtMFieldType *mtype();
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
ZtMFieldType *ZtFieldType_CString<char *, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_CString<char *, Props>>::instance();
}

inline const char *ZtField_CString_Def() { return nullptr; }
template <
  typename Base,
  auto Def = ZtField_CString_Def,
  bool = Base::ReadOnly>
struct ZtField_CString : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_CString<Override<Base>>;
  using O = typename Base::O;
  using T = char *;
  using Props = typename Base::Props;
  using Type =
    ZtFieldType_CString<char *, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.cstring = [](const void *o) -> const char * {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.cstring = [](void *, const char *) { }}};
  }
  static const char *deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.cstring = [](const void *o) -> const char * {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt: return Def();
	default:    return nullptr;
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_CString<Base, Def, false> :
    public ZtField_CString<Base, Def, true> {
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
  template <typename = ZtFieldFmt::Default>
  using Print = ZtField_::Print::String;
  inline static ZtMFieldType *mtype();
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
ZtMFieldType *ZtFieldType_String<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_String<T, Props>>::instance();
}

inline ZuString ZtField_String_Def() { return {}; }
template <typename Base, typename = void>
struct ZtField_String_Get {
  static ZtMFieldGet getFn() {
    using O = typename Base::O;
    // field get() returns a temporary
    return {.get_ = {.string = [](const void *o) -> ZuString {
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
    return {.get_ = {.string = [](const void *o) -> ZuString {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
};
template <
  typename Base,
  auto Def = ZtField_String_Def,
  bool = Base::ReadOnly>
struct ZtField_String :
    public ZtField<Base>,
    public ZtField_String_Get<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_String<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_String<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldSet setFn() {
    return {.set_ = {.string = [](void *, ZuString) { }}};
  }
  static ZuString deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.string = [](const void *o) -> ZuString {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt: return Def();
	default:    return {};
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_String<Base, Def, false> :
    public ZtField_String<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.set_ = {.string = [](void *o, ZuString s) {
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
  template <typename = ZtFieldFmt::Default>
  using Print = ZtField_::Print::Bytes;
  inline static ZtMFieldType *mtype();
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
ZtMFieldType *ZtFieldType_Bytes<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_Bytes<T, Props>>::instance();
}

inline ZuBytes ZtField_Bytes_Def() { return {}; }
template <typename Base, typename = void>
struct ZtField_Bytes_Get {
  static ZtMFieldGet getFn() {
    using O = typename Base::O;
    // field get() returns a temporary
    return {.get_ = {.bytes = [](const void *o) -> ZuBytes {
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
    return {.get_ = {.bytes = [](const void *o) -> ZuBytes {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
};
template <
  typename Base,
  auto Def = ZtField_Bytes_Def,
  bool = Base::ReadOnly>
struct ZtField_Bytes :
    public ZtField<Base>,
    public ZtField_Bytes_Get<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Bytes<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_Bytes<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldSet setFn() {
    return {.set_ = {.bytes = [](void *, ZuBytes) { }}};
  }
  static ZuBytes deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.bytes = [](const void *o) -> ZuBytes {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt: return Def();
	default:    return {};
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_Bytes<Base, Def, false> :
    public ZtField_Bytes<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.set_ = {.bytes = [](void *o, ZuBytes v) {
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
  template <typename = ZtFieldFmt::Default> struct Print {
    bool v;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << (print.v ? '1' : '0');
    }
  };
  inline static ZtMFieldType *mtype();
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
ZtMFieldType *ZtFieldType_Bool<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_Bool<T, Props>>::instance();
}

inline constexpr bool ZtField_Bool_Def() { return false; }
template <
  typename Base,
  auto Def = ZtField_Bool_Def,
  bool = Base::ReadOnly>
struct ZtField_Bool : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Bool<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_Bool<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.bool_ = [](const void *o) -> bool {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.bool_ = [](void *, bool) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.bool_ = [](const void *o) -> bool {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	case Minimum: return false;
	case Maximum: return true;
	default:      return false;
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_Bool<Base, Def, false> :
    public ZtField_Bool<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.set_ = {.bool_ = [](void *o, bool v) {
      Base::set(*static_cast<O *>(o), v);
    }}};
  }
};

// --- {Int,UInt}{8,16,32,64,128}

#define ZtField_Int(Code_, Type_) \
template <typename T_, typename Props_> \
struct ZtFieldType_##Code_ : public ZtFieldType_<Props_> { \
  enum { Code = ZtFieldTypeCode::Code_ }; \
  using T = T_; \
  using Props = Props_; \
  template <typename Fmt = ZtFieldFmt::Default> struct Print { \
    ZuBox<Type_##_t> v; \
    template <typename S> \
    friend S &operator <<(S &s, const Print &print) { \
      if constexpr (ZuTypeIn<ZuFieldProp::Hex, Props>{}) \
	return s << print.v.template hex<false, Fmt>(); \
      else \
	return s << print.v.template fmt<Fmt>(); \
    } \
  }; \
  inline static ZtMFieldType *mtype(); \
}; \
template <typename T, typename Props> \
struct ZtFieldType<ZtFieldTypeCode::Code_, T, void, Props> : \
    public ZtFieldType_##Code_<T, Props> { }; \
 \
template <typename T, typename Props> \
struct ZtMFieldType_##Code_ : public ZtMFieldType { \
  ZtMFieldType_##Code_() : ZtMFieldType{ \
    .code = ZtFieldTypeCode::Code_, \
    .props = ZtMFieldProp::Value<Props>{}, \
    .info = {.null = nullptr} \
  } { } \
}; \
template <typename T, typename Props> \
ZtMFieldType *ZtFieldType_##Code_<T, Props>::mtype() { \
  return ZmSingleton<ZtMFieldType_##Code_<T, Props>>::instance(); \
} \
 \
template <typename T> \
struct ZtFieldType_##Code_##_Def { \
  constexpr static auto deflt() { return ZuCmp<T>::null(); } \
  constexpr static auto minimum() { return ZuCmp<T>::minimum(); } \
  constexpr static auto maximum() { return ZuCmp<T>::maximum(); } \
}; \
template < \
  typename Base, \
  auto Def = ZtFieldType_##Code_##_Def<typename Base::T>::deflt, \
  auto Min = ZtFieldType_##Code_##_Def<typename Base::T>::minimum, \
  auto Max = ZtFieldType_##Code_##_Def<typename Base::T>::maximum, \
  bool = Base::ReadOnly> \
struct ZtField_##Code_ : public ZtField<Base> { \
  template <template <typename> typename Override> \
  using Adapt = ZtField_##Code_<Override<Base>>; \
  using O = typename Base::O; \
  using T = typename Base::T; \
  using Props = typename Base::Props; \
  using Type = ZtFieldType_##Code_<T, ZuTypeGrep<ZtFieldType_Props, Props>>; \
  enum { Code = Type::Code }; \
  static ZtMFieldGet getFn() { \
    return {.get_ = {.Type_= [](const void *o) -> Type_##_t { \
      return Base::get(*static_cast<const O *>(o)); \
    }}}; \
  } \
  static ZtMFieldSet setFn() { \
    return {.set_ = {.Type_= [](void *, Type_##_t) { }}}; \
  } \
  constexpr static auto deflt() { return Def(); } \
  static ZtMFieldGet constantFn() { \
    using namespace ZtMFieldConstant; \
    return {.get_ = {.Type_= [](const void *o) -> Type_##_t { \
      switch (int(reinterpret_cast<uintptr_t>(o))) { \
	case Deflt:   return Def(); \
	case Minimum: return Min(); \
	case Maximum: return Max(); \
	default:      return ZuCmp<Type_##_t>::null(); \
      } \
    }}}; \
  } \
}; \
template <typename Base, auto Def, auto Min, auto Max> \
struct ZtField_##Code_<Base, Def, Min, Max, false> : \
    public ZtField_##Code_<Base, Def, Min, Max, true> { \
  using O = typename Base::O; \
  using T = typename Base::T; \
  static ZtMFieldSet setFn() { \
    return {.set_ = {.Type_= [](void *o, Type_##_t v) { \
      Base::set(*static_cast<O *>(o), v); \
    }}}; \
  } \
};

ZtField_Int(Int8, int8);
ZtField_Int(UInt8, uint8);
ZtField_Int(Int16, int16);
ZtField_Int(UInt16, uint16);
ZtField_Int(Int32, int32);
ZtField_Int(UInt32, uint32);
ZtField_Int(Int64, int64);
ZtField_Int(UInt64, uint64);
ZtField_Int(Int128, int128);
ZtField_Int(UInt128, uint128);

// --- Enum

template <typename T_, typename Map_, typename Props_>
struct ZtFieldType_Enum : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::Enum };
  using T = T_;
  using Map = Map_;
  using Props = Props_;
  template <typename = ZtFieldFmt::Default> struct Print {
    int v;
    template <typename U> Print(U v_) : v{int(v_)} { }
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Map::v2s(print.v);
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Map, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Enum, T, Map, Props> :
    public ZtFieldType_Enum<T, Map, Props> { };

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
ZtMFieldType *ZtFieldType_Enum<T, Map, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_Enum<T, Map, Props>>::instance();
}

inline constexpr int ZtField_Enum_Def() { return -1; }
template <
  typename Base, typename Map,
  auto Def = ZtField_Enum_Def,
  bool = Base::ReadOnly>
struct ZtField_Enum : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Enum<Override<Base>, Map>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_Enum<T, Map, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.enum_ = [](const void *o) -> int {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.enum_ = [](void *, int) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.enum_ = [](const void *o) -> int {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return -1;
      }
    }}};
  }
};
template <typename Base, typename Map, auto Def>
struct ZtField_Enum<Base, Map, Def, false> :
    public ZtField_Enum<Base, Map, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.set_ = {.enum_ = [](void *o, int v) {
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
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    uint128_t v;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Map::print(print.v, Fmt::FlagsDelim());
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Map, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Flags, T, Map, Props> :
    public ZtFieldType_Flags<T, Map, Props> { };

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
ZtMFieldType *ZtFieldType_Flags<T, Map, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_Flags<T, Map, Props>>::instance();
}

inline constexpr uint128_t ZtField_Flags_Def() { return 0; }
template <
  typename Base, typename Map,
  auto Def = ZtField_Flags_Def,
  bool = Base::ReadOnly>
struct ZtField_Flags : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Flags<Override<Base>, Map>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_Flags<T, Map, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.flags = [](const void *o) -> uint128_t {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.flags = [](void *, uint64_t) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.flags = [](const void *o) -> uint128_t {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return 0;
      }
    }}};
  }
};
template <typename Base, typename Map, auto Def>
struct ZtField_Flags<Base, Map, Def, false> :
    public ZtField_Flags<Base, Map, Def, true> {
  using O = typename Base::O;
  using T = typename Base::T;
  static ZtMFieldSet setFn() {
    return {.set_ = {.flags = [](void *o, uint128_t v) {
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
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZuBox<double> v;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      constexpr int8_t NDP = ZuFieldProp::GetNDP<Props>{};
      if constexpr (!ZuCmp<int8_t>::null(NDP))
	return s << print.v.template fp<NDP, '\0', Fmt>();
      else
	return s << print.v.template fmt<Fmt>();
    }
  };
  inline static ZtMFieldType *mtype();
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
ZtMFieldType *ZtFieldType_Float<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_Float<T, Props>>::instance();
}

template <typename T>
struct ZtField_Float_Def {
  constexpr static auto deflt() { return ZuCmp<T>::null(); }
  constexpr static auto minimum() { return -ZuFP<T>::inf(); }
  constexpr static auto maximum() { return ZuFP<T>::inf(); }
};
template <
  typename Base,
  auto Def = ZtField_Float_Def<typename Base::T>::deflt,
  auto Min = ZtField_Float_Def<typename Base::T>::minimum,
  auto Max = ZtField_Float_Def<typename Base::T>::maximum,
  bool = Base::ReadOnly>
struct ZtField_Float : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Float<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_Float<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.float_ = [](const void *o) -> double {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.float_ = [](void *, double) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.float_ = [](const void *o) -> double {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	case Minimum: return Min();
	case Maximum: return Max();
	default:      return ZuCmp<double>::null();
      }
    }}};
  }
};
template <typename Base, auto Def, auto Min, auto Max>
struct ZtField_Float<Base, Def, Min, Max, false> :
    public ZtField_Float<Base, Def, Min, Max, true> {
  using O = typename Base::O;
  using T = typename Base::T;
  static ZtMFieldSet setFn() {
    return {.set_ = {.float_ = [](void *o, double v) {
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
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZuFixed v;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      constexpr int8_t NDP = ZuFieldProp::GetNDP<Props>{};
      if constexpr (!ZuCmp<int8_t>::null(NDP))
	return s << print.v.template fp<NDP, '\0', Fmt>();
      else
	return s << print.v.template fmt<Fmt>();
    }
  };
  inline static ZtMFieldType *mtype();
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
ZtMFieldType *ZtFieldType_Fixed<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_Fixed<T, Props>>::instance();
}

struct ZtField_Fixed_Def {
  constexpr static ZuFixed deflt() { return {}; }
  constexpr static ZuFixed minimum() { return {ZuFixedMin, 0}; }
  constexpr static ZuFixed maximum() { return {ZuFixedMax, 0}; }
};
template <
  typename Base,
  auto Def = ZtField_Fixed_Def::deflt,
  auto Min = ZtField_Fixed_Def::minimum,
  auto Max = ZtField_Fixed_Def::maximum,
  bool = Base::ReadOnly>
struct ZtField_Fixed : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Fixed<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_Fixed<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.fixed = [](const void *o) -> ZuFixed {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.fixed = [](void *, ZuFixed) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.fixed = [](const void *o) -> ZuFixed {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	case Minimum: return Min();
	case Maximum: return Max();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, auto Def, auto Min, auto Max>
struct ZtField_Fixed<Base, Def, Min, Max, false> :
    public ZtField_Fixed<Base, Def, Min, Max, true> {
  using O = typename Base::O;
  using T = typename Base::T;
  static ZtMFieldSet setFn() {
    return {.set_ = {.fixed = [](void *o, ZuFixed v) {
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
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZuDecimal v;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      constexpr int8_t NDP = ZuFieldProp::GetNDP<Props>{};
      if constexpr (!ZuCmp<int8_t>::null(NDP))
	return s << print.v.template fp<NDP, '\0', Fmt>();
      else
	return s << print.v.template fmt<Fmt>();
    }
  };
  inline static ZtMFieldType *mtype();
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
ZtMFieldType *ZtFieldType_Decimal<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_Decimal<T, Props>>::instance();
}

struct ZtField_Decimal_Def {
  constexpr static ZuDecimal deflt() {
    return ZuCmp<ZuDecimal>::null();
  }
  constexpr static ZuDecimal minimum() {
    return {ZuDecimal::Unscaled{ZuDecimal::minimum()}};
  }
  constexpr static ZuDecimal maximum() {
    return {ZuDecimal::Unscaled{ZuDecimal::maximum()}};
  }
};
template <
  typename Base,
  auto Def = ZtField_Decimal_Def::deflt,
  auto Min = ZtField_Decimal_Def::minimum,
  auto Max = ZtField_Decimal_Def::maximum,
  bool = Base::ReadOnly>
struct ZtField_Decimal : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Decimal<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_Decimal<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.decimal = [](const void *o) -> ZuDecimal {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.decimal = [](void *, ZuDecimal) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.decimal = [](const void *o) -> ZuDecimal {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	case Minimum: return Min();
	case Maximum: return Max();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, auto Def, auto Min, auto Max>
struct ZtField_Decimal<Base, Def, Min, Max, false> :
    public ZtField_Decimal<Base, Def, Min, Max, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.set_ = {.decimal = [](void *o, ZuDecimal v) {
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
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZuTime v;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      ZuDateTime v{print.v};
      return s << v.fmt(Fmt::DatePrint_());
    }
  };
  inline static ZtMFieldType *mtype();
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
ZtMFieldType *ZtFieldType_Time<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_Time<T, Props>>::instance();
}

inline constexpr ZuTime ZtField_Time_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtField_Time_Def,
  bool = Base::ReadOnly>
struct ZtField_Time : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Time<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_Time<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.time = [](const void *o) -> ZuTime {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.time = [](void *, ZuTime) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.time = [](const void *o) -> ZuTime {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_Time<Base, Def, false> :
    public ZtField_Time<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.set_ = {.time = [](void *o, ZuTime v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
  }
};

// --- DateTime

template <typename T_, typename Props_>
struct ZtFieldType_DateTime : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::DateTime };
  using T = T_;
  using Props = Props_;
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZuDateTime v;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << print.v.fmt(Fmt::DatePrint_());
    }
  };
  inline static ZtMFieldType *mtype();
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
ZtMFieldType *ZtFieldType_DateTime<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_DateTime<T, Props>>::instance();
}

inline constexpr ZuDateTime ZtField_DateTime_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtField_DateTime_Def,
  bool = Base::ReadOnly>
struct ZtField_DateTime : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_DateTime<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_DateTime<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.dateTime = [](const void *o) -> ZuDateTime {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.dateTime = [](void *, ZuDateTime) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.dateTime = [](const void *o) -> ZuDateTime {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_DateTime<Base, Def, false> :
    public ZtField_DateTime<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.set_ = {.dateTime = [](void *o, ZuDateTime v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
  }
};

// --- UDT

template <typename T, typename Fmt, typename = void>
struct ZtFieldType_UDT_HasFmt : public ZuFalse { };
template <typename T, typename Fmt>
struct ZtFieldType_UDT_HasFmt<T, Fmt,
  decltype(ZuDeclVal<const T &>().template fmt<Fmt>(), void())> :
    public ZuTrue { };

template <typename T_, typename Props_>
struct ZtFieldType_UDT : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::UDT };
  using T = T_;
  using Props = Props_;
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    const T &v;
    template <typename S, typename U = T>
    friend ZuIfT<ZtFieldType_UDT_HasFmt<U, Fmt>{}, S &>
    operator <<(S &s, const Print &print) {
      return s << print.v.template fmt<Fmt>();
    }
    template <typename S, typename U = T>
    friend ZuIfT<!ZtFieldType_UDT_HasFmt<U, Fmt>{}, S &>
    operator <<(S &s, const Print &print) {
      return s << print.v;
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::UDT, T, void, Props> :
    public ZtFieldType_UDT<T, Props> { };

template <typename T, typename = void>
struct ZtMFieldType_UDT_Print {
  static auto printFn() {
    return [](const void *, ZuMStream &, const ZtFieldVFmt &) { };
  }
};
template <typename T>
struct ZtMFieldType_UDT_Print<T,
  decltype((ZuDeclVal<ZuMStream &>() << ZuDeclVal<const T &>()), void())> {
  static auto printFn() {
    return [](const void *v, ZuMStream &s, const ZtFieldVFmt &) {
      s << *reinterpret_cast<const T *>(v);
    };
  }
};
template <typename T, typename = void>
struct ZtMFieldType_UDT_Scan {
  static auto scanFn() {
    return [](
      void (*)(void *, const void *), void *,
      ZuString, const ZtFieldVFmt &) { };
  }
};
template <typename T>
struct ZtMFieldType_UDT_Scan<
  T, decltype((ZuDeclVal<T &>() = ZuString{}), void())
> {
  static auto scanFn() {
    return [](
      void (*set)(void *, const void *), void *o,
      ZuString s, const ZtFieldVFmt &
    ) {
      T v{s};
      set(o, reinterpret_cast<const void *>(&v));
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
ZtMFieldType *ZtFieldType_UDT<T, Props>::mtype() {
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
    return {.get_ = {.udt = [](const void *o) -> const void * {
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
    return {.get_ = {.udt = [](const void *o) -> const void * {
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
    return {.get_ = {.udt = [](const void *o) -> const void * {
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
    return {.get_ = {.udt = [](const void *o) -> const void * {
      return static_cast<const void *>(&Base::get(*static_cast<const O *>(o)));
    }}};
  }
};
template <
  typename Base,
  auto Def = ZtField_UDT_Def<typename Base::T>::value,
  bool = Base::ReadOnly>
struct ZtField_UDT :
    public ZtField<Base>,
    public ZtField_UDT_Constant<Base, Def>,
    public ZtField_UDT_Get<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_UDT<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_UDT<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldSet setFn() {
    return {.set_ = {.udt = [](void *, const void *) { }}};
  }
  static auto deflt() { return Def(); }
};
template <typename Base, auto Def>
struct ZtField_UDT<Base, Def, false> :
    public ZtField_UDT<Base, Def, true> {
  using O = typename Base::O;
  using T = typename Base::T;
  static ZtMFieldSet setFn() {
    return {.set_ = {.udt = [](void *o, const void *p) {
      Base::set(*static_cast<O *>(o), *static_cast<const T *>(p));
    }}};
  }
};

// --- CStringVec

template <typename T_, typename Props_>
struct ZtFieldType_CStringVec : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::CStringVec };
  using T = T_;
  using Props = Props_;
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZtField_::CStringVec vec;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      s << Fmt::VecPrefix();
      bool first = true;
      print.vec.all([&s, &first](const char *v) {
	if (!first) s << Fmt::VecDelim(); else first = false;
	s << ZtField_::Print::CString{v};
      });
      return s << Fmt::VecSuffix();
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::CStringVec, T, void, Props> :
    public ZtFieldType_CStringVec<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_CStringVec : public ZtMFieldType {
  ZtMFieldType_CStringVec() : ZtMFieldType{
    .code = ZtFieldTypeCode::CStringVec,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_CStringVec<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_CStringVec<T, Props>>::instance();
}

inline ZtField_::CStringVec ZtField_CStringVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtField_CStringVec_Def,
  bool = Base::ReadOnly>
struct ZtField_CStringVec : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_CStringVec<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Elem = typename ZuTraits<T>::Elem;
  using Props = typename Base::Props;
  using Type = ZtFieldType_CStringVec<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  using CStringVec = ZtField_::CStringVec;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.cstringVec = [](const void *o) -> CStringVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.cstringVec = [](void *, CStringVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.cstringVec = [](const void *o) -> CStringVec {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_CStringVec<Base, Def, false> :
    public ZtField_CStringVec<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.cstringVec = [](void *o, CStringVec v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
  }
};

// --- StringVec

template <typename T_, typename Props_>
struct ZtFieldType_StringVec : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::StringVec };
  using T = T_;
  using Props = Props_;
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZtField_::StringVec vec;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      s << Fmt::VecPrefix();
      bool first = true;
      print.vec.all([&s, &first](ZuString v) {
	if (!first) s << Fmt::VecDelim(); else first = false;
	s << ZtField_::Print::String{v};
      });
      return s << Fmt::VecSuffix();
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::StringVec, T, void, Props> :
    public ZtFieldType_StringVec<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_StringVec : public ZtMFieldType {
  ZtMFieldType_StringVec() : ZtMFieldType{
    .code = ZtFieldTypeCode::StringVec,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_StringVec<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_StringVec<T, Props>>::instance();
}

inline ZtField_::StringVec ZtField_StringVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtField_StringVec_Def,
  bool = Base::ReadOnly>
struct ZtField_StringVec : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_StringVec<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_StringVec<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  using StringVec = ZtField_::StringVec;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.stringVec = [](const void *o) -> StringVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.stringVec = [](void *, StringVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.stringVec = [](const void *o) -> StringVec {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_StringVec<Base, Def, false> :
    public ZtField_StringVec<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.stringVec = [](void *o, StringVec v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
  }
};

// --- BytesVec

template <typename T_, typename Props_>
struct ZtFieldType_BytesVec : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::BytesVec };
  using T = T_;
  using Props = Props_;
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZtField_::BytesVec vec;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      s << Fmt::VecPrefix();
      bool first = true;
      print.vec.all([&s, &first](ZuBytes v) {
	if (!first) s << Fmt::VecDelim(); else first = false;
	s << ZtField_::Print::Bytes{v};
      });
      return s << Fmt::VecSuffix();
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::BytesVec, T, void, Props> :
    public ZtFieldType_BytesVec<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_BytesVec : public ZtMFieldType {
  ZtMFieldType_BytesVec() : ZtMFieldType{
    .code = ZtFieldTypeCode::BytesVec,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_BytesVec<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_BytesVec<T, Props>>::instance();
}

inline ZtField_::BytesVec ZtField_BytesVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtField_BytesVec_Def,
  bool = Base::ReadOnly>
struct ZtField_BytesVec : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_BytesVec<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_BytesVec<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  using BytesVec = ZtField_::BytesVec;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.bytesVec = [](const void *o) -> BytesVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.bytesVec = [](void *, BytesVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.bytesVec = [](const void *o) -> BytesVec {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_BytesVec<Base, Def, false> :
    public ZtField_BytesVec<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.bytesVec = [](void *o, BytesVec v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
  }
};

// --- {Int,UInt}{8,16,32,64,128}Vec

#define ZtField_IntVec(Code_, Type_) \
template <typename T_, typename Props_> \
struct ZtFieldType_##Code_##Vec : public ZtFieldType_<Props_> { \
  enum { Code = ZtFieldTypeCode::Code_##Vec }; \
  using T = T_; \
  using Props = Props_; \
  template <typename Fmt = ZtFieldFmt::Default> struct Print { \
    ZtField_::Code_##Vec vec; \
    template <typename S> \
    friend S &operator <<(S &s, const Print &print) { \
      s << Fmt::VecPrefix(); \
      bool first = true; \
      print.vec.all([&s, &first](ZuBox<Type_##_t> v) { \
	if (!first) s << Fmt::VecDelim(); else first = false; \
	s << v.template fmt<Fmt>(); \
      }); \
      return s << Fmt::VecSuffix(); \
    } \
  }; \
  inline static ZtMFieldType *mtype(); \
}; \
template <typename T, typename Props> \
struct ZtFieldType<ZtFieldTypeCode::Code_##Vec, T, void, Props> : \
    public ZtFieldType_##Code_##Vec<T, Props> { }; \
 \
template <typename T, typename Props> \
struct ZtMFieldType_##Code_##Vec : public ZtMFieldType { \
  ZtMFieldType_##Code_##Vec() : ZtMFieldType{ \
    .code = ZtFieldTypeCode::Code_##Vec, \
    .props = ZtMFieldProp::Value<Props>{}, \
    .info = {.null = nullptr} \
  } { } \
}; \
template <typename T, typename Props> \
ZtMFieldType *ZtFieldType_##Code_##Vec<T, Props>::mtype() { \
  return ZmSingleton<ZtMFieldType_##Code_##Vec<T, Props>>::instance(); \
} \
 \
inline ZtField_::Code_##Vec ZtFieldType_##Code_##Vec_Def() { return {}; } \
template < \
  typename Base, \
  auto Def = ZtFieldType_##Code_##Vec_Def, \
  bool = Base::ReadOnly> \
struct ZtField_##Code_##Vec : public ZtField<Base> { \
  template <template <typename> typename Override> \
  using Adapt = ZtField_##Code_##Vec<Override<Base>>; \
  using O = typename Base::O; \
  using T = typename Base::T; \
  using Props = typename Base::Props; \
  using Type = \
    ZtFieldType_##Code_##Vec<T, ZuTypeGrep<ZtFieldType_Props, Props>>; \
  using Code_##Vec = ZtField_::Code_##Vec; \
  enum { Code = Type::Code }; \
  static ZtMFieldGet getFn() { \
    return {.get_ = {.Type_##Vec = [](const void *o) -> Code_##Vec { \
      return Base::get(*static_cast<const O *>(o)); \
    }}}; \
  } \
  static ZtMFieldSet setFn() { \
    using namespace ZtField_; \
    return {.set_ = {.Type_##Vec = [](void *, Code_##Vec) { }}}; \
  } \
  constexpr static auto deflt() { return Def(); } \
  static ZtMFieldGet constantFn() { \
    using namespace ZtMFieldConstant; \
    return {.get_ = {.Type_##Vec = [](const void *o) -> Code_##Vec { \
      switch (int(reinterpret_cast<uintptr_t>(o))) { \
	case Deflt:   return Def(); \
	default:      return {}; \
      } \
    }}}; \
  } \
}; \
template <typename Base, auto Def> \
struct ZtField_##Code_##Vec<Base, Def, false> : \
    public ZtField_##Code_##Vec<Base, Def, true> { \
  using O = typename Base::O; \
  static ZtMFieldSet setFn() { \
    using namespace ZtField_; \
    return {.set_ = {.Type_##Vec = [](void *o, Code_##Vec v) { \
      Base::set(*static_cast<O *>(o), ZuMv(v)); \
    }}}; \
  } \
};

ZtField_IntVec(Int8, int8);
ZtField_IntVec(UInt8, uint8);
ZtField_IntVec(Int16, int16);
ZtField_IntVec(UInt16, uint16);
ZtField_IntVec(Int32, int32);
ZtField_IntVec(UInt32, uint32);
ZtField_IntVec(Int64, int64);
ZtField_IntVec(UInt64, uint64);
ZtField_IntVec(Int128, int128);
ZtField_IntVec(UInt128, uint128);

// --- FloatVec

template <typename T_, typename Props_>
struct ZtFieldType_FloatVec : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::FloatVec };
  using T = T_;
  using Props = Props_;
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZtField_::FloatVec vec;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      s << Fmt::VecPrefix();
      bool first = true;
      print.vec.all([&s, &first](ZuBox<double> v) {
	if (!first) s << Fmt::VecDelim(); else first = false;
	s << v.template fmt<Fmt>();
      });
      return s << Fmt::VecSuffix();
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::FloatVec, T, void, Props> :
    public ZtFieldType_FloatVec<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_FloatVec : public ZtMFieldType {
  ZtMFieldType_FloatVec() : ZtMFieldType{
    .code = ZtFieldTypeCode::FloatVec,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_FloatVec<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_FloatVec<T, Props>>::instance();
}

inline ZtField_::FloatVec ZtField_FloatVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtField_FloatVec_Def,
  bool = Base::ReadOnly>
struct ZtField_FloatVec : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_FloatVec<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_FloatVec<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  using FloatVec = ZtField_::FloatVec;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.floatVec = [](const void *o) -> FloatVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.floatVec = [](void *, FloatVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.floatVec = [](const void *o) -> FloatVec {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_FloatVec<Base, Def, false> :
    public ZtField_FloatVec<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.floatVec = [](void *o, FloatVec v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
  }
};

// --- FixedVec

template <typename T_, typename Props_>
struct ZtFieldType_FixedVec : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::FixedVec };
  using T = T_;
  using Props = Props_;
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZtField_::FixedVec vec;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      s << Fmt::VecPrefix();
      bool first = true;
      print.vec.all([&s, &first](const auto &v) {
	if (!first) s << Fmt::VecDelim(); else first = false;
	s << v.template fmt<Fmt>();
      });
      return s << Fmt::VecSuffix();
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::FixedVec, T, void, Props> :
    public ZtFieldType_FixedVec<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_FixedVec : public ZtMFieldType {
  ZtMFieldType_FixedVec() : ZtMFieldType{
    .code = ZtFieldTypeCode::FixedVec,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_FixedVec<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_FixedVec<T, Props>>::instance();
}

inline ZtField_::FixedVec ZtField_FixedVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtField_FixedVec_Def,
  bool = Base::ReadOnly>
struct ZtField_FixedVec : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_FixedVec<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_FixedVec<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  using FixedVec = ZtField_::FixedVec;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.fixedVec = [](const void *o) -> FixedVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.fixedVec = [](void *, FixedVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.fixedVec = [](const void *o) -> FixedVec {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_FixedVec<Base, Def, false> :
    public ZtField_FixedVec<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.fixedVec = [](void *o, FixedVec v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
  }
};

// --- DecimalVec

template <typename T_, typename Props_>
struct ZtFieldType_DecimalVec : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::DecimalVec };
  using T = T_;
  using Props = Props_;
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZtField_::DecimalVec vec;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      s << Fmt::VecPrefix();
      bool first = true;
      print.vec.all([&s, &first](const auto &v) {
	if (!first) s << Fmt::VecDelim(); else first = false;
	s << v.template fmt<Fmt>();
      });
      return s << Fmt::VecSuffix();
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::DecimalVec, T, void, Props> :
    public ZtFieldType_DecimalVec<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_DecimalVec : public ZtMFieldType {
  ZtMFieldType_DecimalVec() : ZtMFieldType{
    .code = ZtFieldTypeCode::DecimalVec,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_DecimalVec<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_DecimalVec<T, Props>>::instance();
}

inline ZtField_::DecimalVec ZtField_DecimalVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtField_DecimalVec_Def,
  bool = Base::ReadOnly>
struct ZtField_DecimalVec : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_DecimalVec<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_DecimalVec<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  using DecimalVec = ZtField_::DecimalVec;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.decimalVec = [](const void *o) -> DecimalVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.decimalVec = [](void *, DecimalVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.decimalVec = [](const void *o) -> DecimalVec {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_DecimalVec<Base, Def, false> :
    public ZtField_DecimalVec<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.decimalVec = [](void *o, DecimalVec v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
  }
};

// --- TimeVec

template <typename T_, typename Props_>
struct ZtFieldType_TimeVec : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::TimeVec };
  using T = T_;
  using Props = Props_;
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZtField_::TimeVec vec;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      s << Fmt::VecPrefix();
      bool first = true;
      print.vec.all([&s, &first](ZuDateTime v) {
	if (!first) s << Fmt::VecDelim(); else first = false;
	s << v.fmt(Fmt::DatePrint_());
      });
      return s << Fmt::VecSuffix();
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::TimeVec, T, void, Props> :
    public ZtFieldType_TimeVec<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_TimeVec : public ZtMFieldType {
  ZtMFieldType_TimeVec() : ZtMFieldType{
    .code = ZtFieldTypeCode::TimeVec,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_TimeVec<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_TimeVec<T, Props>>::instance();
}

inline ZtField_::TimeVec ZtField_TimeVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtField_TimeVec_Def,
  bool = Base::ReadOnly>
struct ZtField_TimeVec : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_TimeVec<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_TimeVec<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  using TimeVec = ZtField_::TimeVec;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.timeVec = [](const void *o) -> TimeVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.timeVec = [](void *, TimeVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.timeVec = [](const void *o) -> TimeVec {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_TimeVec<Base, Def, false> :
    public ZtField_TimeVec<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.timeVec = [](void *o, TimeVec v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
  }
};

// --- DateTimeVec

template <typename T_, typename Props_>
struct ZtFieldType_DateTimeVec : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::DateTimeVec };
  using T = T_;
  using Props = Props_;
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZtField_::DateTimeVec vec;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      s << Fmt::VecPrefix();
      bool first = true;
      print.vec.all([&s, &first](const auto &v) {
	if (!first) s << Fmt::VecDelim(); else first = false;
	s << v.fmt(Fmt::DatePrint_());
      });
      return s << Fmt::VecSuffix();
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::DateTimeVec, T, void, Props> :
    public ZtFieldType_DateTimeVec<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_DateTimeVec : public ZtMFieldType {
  ZtMFieldType_DateTimeVec() : ZtMFieldType{
    .code = ZtFieldTypeCode::DateTimeVec,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_DateTimeVec<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_DateTimeVec<T, Props>>::instance();
}

inline ZtField_::DateTimeVec ZtField_DateTimeVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtField_DateTimeVec_Def,
  bool = Base::ReadOnly>
struct ZtField_DateTimeVec : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_DateTimeVec<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_DateTimeVec<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  using DateTimeVec = ZtField_::DateTimeVec;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.dateTimeVec = [](const void *o) -> DateTimeVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.dateTimeVec = [](void *, DateTimeVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.dateTimeVec = [](const void *o) -> DateTimeVec {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_DateTimeVec<Base, Def, false> :
    public ZtField_DateTimeVec<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.dateTimeVec = [](void *o, DateTimeVec v) {
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
#define ZtField_TypeArgs_Bool(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Int8(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_UInt8(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Int16(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_UInt16(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Int32(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_UInt32(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Int64(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_UInt64(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Int128(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_UInt128(...) \
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

#define ZtField_TypeArgs_CStringVec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_StringVec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_BytesVec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Int8Vec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_UInt8Vec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Int16Vec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_UInt16Vec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Int32Vec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_UInt32Vec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Int64Vec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_UInt64Vec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Int128Vec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_UInt128Vec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_FloatVec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_FixedVec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_DecimalVec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_TimeVec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_DateTimeVec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)

#define ZtField_TypeArgs_(Name, ...) \
  __VA_OPT__(, ZtField_TypeArgs_##Name(__VA_ARGS__))
#define ZtField_TypeArgs(Type) ZuPP_Defer(ZtField_TypeArgs_)Type

#define ZtFieldTypeName(O, ID) ZtField_##O##_##ID

#define ZtField_Decl__(O, ID, Base, TypeName, Type) \
  ZuField_Decl(O, Base) \
  using ZtFieldTypeName(O, ID) = \
  ZtField_##TypeName<ZuFieldTypeName(O, ID) ZtField_TypeArgs(Type)>;
#define ZtField_Decl_(O, Base, Type) \
  ZuPP_Defer(ZtField_Decl__)(O, \
      ZuPP_Nest(ZtField_BaseID(Base)), Base, \
      ZuPP_Nest(ZtField_TypeName(Type)), Type)
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
  using Print = typename Field::Type::Print<ZtFieldFmt::Default>;
  const O &o;
  template <typename S>
  friend S &operator <<(S &s, const ZtFieldPrint_ &print) {
    return
      s << Field::id() << '=' << Print{Field::get(print.o)};
  }
};

struct ZtFieldPrint : public ZuPrintDelegate {
  template <typename U> struct Print_Filter :
      public ZuBool<!ZuTypeIn<ZuFieldProp::Hidden, typename U::Props>{}> { };
  template <typename S, typename O>
  static void print(S &s, const O &o) {
    using FieldList = ZuTypeGrep<Print_Filter, ZuFieldList<O>>;
    s << '{';
    ZuUnroll::all<FieldList>([&s, &o]<typename Field>() {
      if constexpr (ZuTypeIndex<Field, FieldList>{}) s << ' ';
      s << ZtFieldPrint_<Field>{o};
    });
    s << '}';
  }
};

// run-time fields

using ZtMFields = ZuArray<const ZtMField *>;

template <typename MField, typename ...Fields>
struct ZtMFieldFactory {
  enum { N = sizeof...(Fields) };

  ZtMFields	fields;

  static ZtMFieldFactory *instance() {
    return ZmSingleton<ZtMFieldFactory>::instance();
  }

  ZtMFieldFactory() {
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
template <typename Fields, typename MField = ZtMField>
inline ZtMFields ZtMFieldList_() {
  using Factory = ZuTypeApply<
    ZtMFieldFactory, typename Fields::template Unshift<MField>>;
  return Factory::instance()->fields;
}
template <typename O, typename MField = ZtMField>
inline ZtMFields ZtMFieldList() {
  return ZtMFieldList_<ZuFieldList<O>, MField>();
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

#endif /* ZtField_HH */
