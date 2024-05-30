//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// object introspection / reflection
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
// that are injected into the ZuFieldProp namespace
//
// a Field is of the form:
// (((Accessor)[, (Props...)]), (Type[, Args...]))
//
// Example: (((id, Rd), (Keys<0>, Ctor<0>)), (String, "default"))
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

#include <zlib/ZmAlloc.hh>
#include <zlib/ZmStream.hh>
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
    Int,		// an integral type <= 64bits
    UInt,		// an unsigned integral type <= 64bits
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
    IntVec,
    UIntVec,
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
  ZtEnumFlags(ZtMFieldProp,
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

  // Value<Prop>::N - return bitfield for individual property
  template <typename> struct Value : public ZuUnsigned<0> { };	// default

  template <unsigned I>
  struct Value<ZuUnsigned<I>> : public ZuUnsigned<I> { };

  namespace _ = ZuFieldProp;

  template <unsigned I>
  struct Value<_::Ctor<I>>               : public ZuUInt128<Ctor()> { };
  template <> struct Value<_::Synthetic> : public ZuUInt128<Synthetic()> { };
  template <> struct Value<_::Update>    : public ZuUInt128<Update()> { };
  template <> struct Value<_::Hidden>    : public ZuUInt128<Hidden()> { };
  template <> struct Value<_::Hex>       : public ZuUInt128<Hex()> { };
  template <> struct Value<_::Required>  : public ZuUInt128<Required()> { };
  template <> struct Value<_::Series>    : public ZuUInt128<Series()> { };
  template <> struct Value<_::Index>     : public ZuUInt128<Index()> { };
  template <> struct Value<_::Delta>     : public ZuUInt128<Delta()> { };
  template <> struct Value<_::Delta2>    : public ZuUInt128<Delta2()> { };
  template <int8_t I>
  struct Value<_::NDP<I>>                : public ZuUInt128<NDP()> { };

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
  void		(*print)(uint128_t, ZmStream &, const ZtFieldVFmt &);
  uint128_t	(*scan)(ZuString, const ZtFieldVFmt &);
};
template <typename Map>
struct ZtMFieldFlags_ : public ZtMFieldFlags {
  ZtMFieldFlags_() : ZtMFieldFlags{
    .id = []() -> const char * { return Map::id(); },
    .print = [](uint128_t v, ZmStream &s, const ZtFieldVFmt &fmt) -> void {
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

typedef void (*ZtMFieldPrint)(const void *, ZmStream &, const ZtFieldVFmt &);
typedef void (*ZtMFieldScan)(
  void (*)(void *, unsigned, const void *), void *, unsigned,
  ZuString, const ZtFieldVFmt &);

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

using CStringVec = ZuArray<const char *>;
using StringVec = ZuArray<ZuString>;
using BytesVec = ZuArray<ZuBytes>;
using IntVec = ZuArray<int64_t>;
using UIntVec = ZuArray<uint64_t>;
using Int128Vec = ZuArray<int128_t>;
using UInt128Vec = ZuArray<uint128_t>;
using FloatVec = ZuArray<double>;
using FixedVec = ZuArray<ZuFixed>;
using DecimalVec = ZuArray<ZuDecimal>;
using TimeVec = ZuArray<ZuTime>;
using DateTimeVec = ZuArray<ZuDateTime>;

// monomorphic field get/print
struct MGet {

  union {
    void		*null;

    const char *	(*cstring)(const void *, unsigned);	// CString
    ZuString		(*string)(const void *, unsigned);	// String
    ZuBytes		(*bytes)(const void *, unsigned);	// Bytes
    bool		(*bool_)(const void *, unsigned);	// Bool
    int64_t		(*int_)(const void *, unsigned);	// Int
    uint64_t		(*uint)(const void *, unsigned);	// UInt
    int128_t		(*int128)(const void *, unsigned);	// Int128
    uint128_t		(*uint128)(const void *, unsigned);	// UInt128
    int			(*enum_)(const void *, unsigned);	// Enum
    uint128_t		(*flags)(const void *, unsigned);	// Flags
    double		(*float_)(const void *, unsigned);	// Float
    ZuFixed		(*fixed)(const void *, unsigned);	// Fixed
    ZuDecimal		(*decimal)(const void *, unsigned);	// Decimal
    ZuTime		(*time)(const void *, unsigned);	// Time
    ZuDateTime		(*dateTime)(const void *, unsigned);	// DateTime
    const void *	(*udt)(const void *, unsigned);		// UDT

    CStringVec		(*cstringVec)(const void *, unsigned);
    StringVec		(*stringVec)(const void *, unsigned);
    BytesVec		(*bytesVec)(const void *, unsigned);
    IntVec		(*intVec)(const void *, unsigned);
    UIntVec		(*uintVec)(const void *, unsigned);
    Int128Vec		(*int128Vec)(const void *, unsigned);
    UInt128Vec		(*uint128Vec)(const void *, unsigned);
    FloatVec		(*floatVec)(const void *, unsigned);
    FixedVec		(*fixedVec)(const void *, unsigned);
    DecimalVec		(*decimalVec)(const void *, unsigned);
    TimeVec		(*timeVec)(const void *, unsigned);
    DateTimeVec		(*dateTimeVec)(const void *, unsigned);
  } get_;

#define ZtMField_GetFn(code, type, fn) \
  template <unsigned Code> \
  ZuIfT<Code == ZtFieldTypeCode::code, type> \
  get(const void *o, unsigned i) const { \
    return get_.fn(o, i); \
  }

  ZtMField_GetFn(CString, const char *, cstring)
  ZtMField_GetFn(String, ZuString, string)
  ZtMField_GetFn(Bytes, ZuBytes, bytes)
  ZtMField_GetFn(Bool, bool, bool_)
  ZtMField_GetFn(Int, int64_t, int_)
  ZtMField_GetFn(UInt, uint64_t, uint)
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
  ZtMField_GetFn(IntVec, IntVec, intVec)
  ZtMField_GetFn(UIntVec, UIntVec, uintVec)
  ZtMField_GetFn(IntVec, Int128Vec, int128Vec)
  ZtMField_GetFn(UIntVec, UInt128Vec, uint128Vec)
  ZtMField_GetFn(FloatVec, FloatVec, floatVec)
  ZtMField_GetFn(FixedVec, FixedVec, fixedVec)
  ZtMField_GetFn(DecimalVec, DecimalVec, decimalVec)
  ZtMField_GetFn(TimeVec, TimeVec, timeVec)
  ZtMField_GetFn(DateTimeVec, DateTimeVec, dateTimeVec)

#define ZtMField_PrintFn(code) \
  template <unsigned Code, typename S> \
  ZuIfT<Code == ZtFieldTypeCode::code> \
  print(S &, const void *, unsigned, const ZtMField *, const ZtFieldVFmt &) \
    const;

  ZtMField_PrintFn(CString)
  ZtMField_PrintFn(String)
  ZtMField_PrintFn(Bytes)
  ZtMField_PrintFn(Bool)
  ZtMField_PrintFn(Int)
  ZtMField_PrintFn(UInt)
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
  ZtMField_PrintFn(IntVec)
  ZtMField_PrintFn(UIntVec)
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

    void	(*cstring)(void *, unsigned, const char *);	// CString
    void	(*string)(void *, unsigned, ZuString);		// String
    void	(*bytes)(void *, unsigned, ZuBytes);		// Bytes
    void	(*bool_)(void *, unsigned, bool);		// Bool
    void	(*int_)(void *, unsigned, int64_t);		// Int
    void	(*uint)(void *, unsigned, uint64_t);		// UInt
    void	(*int128)(void *, unsigned, int128_t);		// Int128
    void	(*uint128)(void *, unsigned, uint128_t);	// UInt128
    void	(*enum_)(void *, unsigned, int);		// Enum
    void	(*flags)(void *, unsigned, uint128_t);		// Flags
    void	(*float_)(void *, unsigned, double);		// Float
    void	(*fixed)(void *, unsigned, ZuFixed);		// Fixed
    void	(*decimal)(void *, unsigned, ZuDecimal);	// Decimal
    void	(*time)(void *, unsigned, ZuTime);		// Time
    void	(*dateTime)(void *, unsigned, ZuDateTime);	// DateTime
    void	(*udt)(void *, unsigned, const void *);		// UDT

    void	(*cstringVec)(void *, unsigned, CStringVec);
    void	(*stringVec)(void *, unsigned, StringVec);
    void	(*bytesVec)(void *, unsigned, BytesVec);
    void	(*intVec)(void *, unsigned, IntVec);
    void	(*uintVec)(void *, unsigned, UIntVec);
    void	(*int128Vec)(void *, unsigned, Int128Vec);
    void	(*uint128Vec)(void *, unsigned, UInt128Vec);
    void	(*floatVec)(void *, unsigned, FloatVec);
    void	(*fixedVec)(void *, unsigned, FixedVec);
    void	(*decimalVec)(void *, unsigned, DecimalVec);
    void	(*timeVec)(void *, unsigned, TimeVec);
    void	(*dateTimeVec)(void *, unsigned, DateTimeVec);
  } set_;

#define ZtMField_SetFn(code, type, fn) \
  template <unsigned Code> \
  ZuIfT<Code == ZtFieldTypeCode::code> \
  set(void *o, unsigned i, type v) const { \
    set_.fn(o, i, v); \
  }

  ZtMField_SetFn(CString, const char *, cstring)
  ZtMField_SetFn(String, ZuString, string)
  ZtMField_SetFn(Bytes, ZuBytes, bytes)
  ZtMField_SetFn(Bool, bool, bool_)
  ZtMField_SetFn(Int, int64_t, int_)
  ZtMField_SetFn(UInt, uint64_t, uint)
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
  ZtMField_SetFn(IntVec, IntVec, intVec)
  ZtMField_SetFn(UIntVec, UIntVec, uintVec)
  ZtMField_SetFn(IntVec, Int128Vec, int128Vec)
  ZtMField_SetFn(UIntVec, UInt128Vec, uint128Vec)
  ZtMField_SetFn(FloatVec, FloatVec, floatVec)
  ZtMField_SetFn(FixedVec, FixedVec, fixedVec)
  ZtMField_SetFn(DecimalVec, DecimalVec, decimalVec)
  ZtMField_SetFn(TimeVec, TimeVec, timeVec)
  ZtMField_SetFn(DateTimeVec, DateTimeVec, dateTimeVec)

#define ZtMField_ScanFn(code) \
  template <unsigned Code> \
  ZuIfT<Code == ZtFieldTypeCode::code> \
  scan(void *, unsigned, ZuString, const ZtMField *, const ZtFieldVFmt &) \
    const;

  ZtMField_ScanFn(CString)
  ZtMField_ScanFn(String)
  ZtMField_ScanFn(Bytes)
  ZtMField_ScanFn(Bool)
  ZtMField_ScanFn(Int)
  ZtMField_ScanFn(UInt)
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
  ZtMField_ScanFn(IntVec)
  ZtMField_ScanFn(UIntVec)
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
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  auto v = get_.cstring(o, i);
  s << Print::CString{v};
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::String>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  auto v = get_.string(o, i);
  s << Print::String{v};
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Bytes>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *, const ZtFieldVFmt &
) const {
  ZuBytes v = get_.bytes(o, i);
  auto n = ZuBase64::enclen(v.length());
  auto buf_ = ZmAlloc(uint8_t, n);
  ZuArray<uint8_t> buf{&buf_[0], n};
  buf.trunc(ZuBase64::encode(buf, v));
  s << buf;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Bool>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *, const ZtFieldVFmt &
) const {
  s << (get_.bool_(o, i) ? '1' : '0');
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Int>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  ZuBox<int64_t> v = get_.int_(o, i);
  if (field->props & ZtMFieldProp::Hex())
    s << v.vfmt(fmt.scalar).hex();
  else
    s << v.vfmt(fmt.scalar);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::UInt>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  ZuBox<uint64_t> v = get_.uint(o, i);
  if (field->props & ZtMFieldProp::Hex())
    s << v.vfmt(fmt.scalar).hex();
  else
    s << v.vfmt(fmt.scalar);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Int128>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  ZuBox<int128_t> v = get_.int128(o, i);
  if (field->props & ZtMFieldProp::Hex())
    s << v.vfmt(fmt.scalar).hex();
  else
    s << v.vfmt(fmt.scalar);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::UInt128>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  ZuBox<uint128_t> v = get_.uint128(o, i);
  if (field->props & ZtMFieldProp::Hex())
    s << v.vfmt(fmt.scalar).hex();
  else
    s << v.vfmt(fmt.scalar);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Enum>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field, const ZtFieldVFmt &
) const {
  s << field->type->info.enum_()->v2s(get_.enum_(o, i));
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Flags>
MGet::print(
  S &s_, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  ZmStream s{s_};
  field->type->info.flags()->print(get_.flags(o, i), s, fmt);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Float>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  ZuBox<double> v = get_.float_(o, i);
  auto ndp = field->ndp;
  if (!ZuCmp<decltype(ndp)>::null(ndp))
    s << v.vfmt(fmt.scalar).fp(ndp);
  else
    s << v.vfmt(fmt.scalar);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Fixed>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  ZuFixed v = get_.fixed(o, i);
  auto ndp = field->ndp;
  if (!ZuCmp<decltype(ndp)>::null(ndp))
    s << v.vfmt(fmt.scalar).fp(ndp);
  else
    s << v.vfmt(fmt.scalar);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Decimal>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  ZuDecimal v = get_.decimal(o, i);
  auto ndp = field->ndp;
  if (!ZuCmp<decltype(ndp)>::null(ndp))
    s << v.vfmt(fmt.scalar).fp(ndp);
  else
    s << v.vfmt(fmt.scalar);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Time>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  ZuDateTime v{get_.time(o, i)};
  s << v.fmt(fmt.datePrint);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::DateTime>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  ZuDateTime v{get_.dateTime(o, i)};
  s << v.fmt(fmt.datePrint);
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::UDT>
MGet::print(
  S &s_, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  ZmStream s{s_};
  field->type->info.udt()->print(get_.udt(o, i), s, fmt);
}

template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::CStringVec>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  CStringVec vec{get_.cstringVec(o, i)};
  vec.all([&s, &first, &fmt](const char *v) {
    if (!first) s << fmt.vecDelim; else first = false;
    s << Print::CString{v};
  });
  s << fmt.vecSuffix;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::StringVec>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  StringVec vec{get_.stringVec(o, i)};
  vec.all([&s, &first, &fmt](ZuString v) {
    if (!first) s << fmt.vecDelim; else first = false;
    s << Print::String{v};
  });
  s << fmt.vecSuffix;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::BytesVec>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  BytesVec vec{get_.bytesVec(o, i)};
  vec.all([&s, &fmt, &first](ZuBytes v) {
    if (!first) s << fmt.vecDelim; else first = false;
    auto n = ZuBase64::enclen(v.length());
    auto buf_ = ZmAlloc(uint8_t, n);
    ZuArray<uint8_t> buf{&buf_[0], n};
    buf.trunc(ZuBase64::encode(buf, v));
    s << buf;
  });
  s << fmt.vecSuffix;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::IntVec>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  IntVec vec{get_.intVec(o, i)};
  if (field->props & ZtMFieldProp::Hex())
    vec.all([&s, &fmt, &first](ZuBox<int64_t> v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar).hex();
    });
  else
    vec.all([&s, &first, &fmt](ZuBox<int64_t> v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar);
    });
  s << fmt.vecSuffix;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::UIntVec>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  UIntVec vec{get_.uintVec(o, i)};
  if (field->props & ZtMFieldProp::Hex())
    vec.all([&s, &fmt, &first](ZuBox<uint64_t> v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar).hex();
    });
  else
    vec.all([&s, &fmt, &first](ZuBox<uint64_t> v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar);
    });
  s << fmt.vecSuffix;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::Int128Vec>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  Int128Vec vec{get_.int128Vec(o, i)};
  if (field->props & ZtMFieldProp::Hex())
    vec.all([&s, &fmt, &first](ZuBox<int128_t> v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar).hex();
    });
  else
    vec.all([&s, &first, &fmt](ZuBox<int128_t> v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar);
    });
  s << fmt.vecSuffix;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::UInt128Vec>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  UInt128Vec vec{get_.uint128Vec(o, i)};
  if (field->props & ZtMFieldProp::Hex())
    vec.all([&s, &fmt, &first](ZuBox<uint128_t> v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar).hex();
    });
  else
    vec.all([&s, &first, &fmt](ZuBox<uint128_t> v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar);
    });
  s << fmt.vecSuffix;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::FloatVec>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  FloatVec vec{get_.floatVec(o, i)};
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
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  FixedVec vec{get_.fixedVec(o, i)};
  auto ndp = field->ndp;
  if (!ZuCmp<decltype(ndp)>::null(ndp))
    vec.all([&s, &fmt, ndp, &first](const auto &v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar).fp(ndp);
    });
  else
    vec.all([&s, &first, &fmt](const auto &v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar);
    });
  s << fmt.vecSuffix;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::DecimalVec>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  DecimalVec vec{get_.decimalVec(o, i)};
  auto ndp = field->ndp;
  if (!ZuCmp<decltype(ndp)>::null(ndp))
    vec.all([&s, &fmt, ndp, &first](const auto &v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar).fp(ndp);
    });
  else
    vec.all([&s, &first, &fmt](const auto &v) {
      if (!first) s << fmt.vecDelim; else first = false;
      s << v.vfmt(fmt.scalar);
    });
  s << fmt.vecSuffix;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::TimeVec>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  TimeVec vec{get_.timeVec(o, i)};
  vec.all([&s, &fmt, &first](ZuDateTime v) {
    if (!first) s << fmt.vecDelim; else first = false;
    s << v.fmt(fmt.datePrint);
  });
  s << fmt.vecSuffix;
}
template <unsigned Code, typename S>
inline ZuIfT<Code == ZtFieldTypeCode::DateTimeVec>
MGet::print(
  S &s, const void *o, unsigned i, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  s << fmt.vecPrefix;
  bool first = true;
  DateTimeVec vec{get_.dateTimeVec(o, i)};
  vec.all([&s, &fmt, &first](const auto &v) {
    if (!first) s << fmt.vecDelim; else first = false;
    s << v.fmt(fmt.datePrint);
  });
  s << fmt.vecSuffix;
}

// ZtMFieldSet scan functions

template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::CString>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  if (!s) {
    set_.cstring(o, i, nullptr);
    return;
  }
  unsigned n = s.length() + 1;
  auto buf_ = ZmAlloc(char, n);
  ZuString buf{&buf_[0], n};
  buf_[Scan::string(buf, s)] = 0;
  set_.cstring(o, i, &buf[0]);
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::String>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  if (!s) {
    set_.string(o, i, s);
    return;
  }
  unsigned n = s.length();
  auto buf_ = ZmAlloc(char, n);
  ZuString buf{&buf_[0], n};
  buf.trunc(Scan::string(buf, s));
  set_.string(o, i, buf);
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Bytes>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  auto n = ZuBase64::declen(s.length());
  auto buf_ = ZmAlloc(uint8_t, n);
  ZuArray<uint8_t> buf{&buf_[0], n};
  buf.trunc(ZuBase64::decode(buf, ZuBytes{s}));
  set_.bytes(o, i, buf);
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Bool>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  set_.bool_(o, i, ZtScanBool(s));
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Int>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  set_.int_(o, i, ZuBox<int64_t>{s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::UInt>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  set_.uint(o, i, ZuBox<uint64_t>{s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Int128>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  set_.int128(o, i, ZuBox<int128_t>{s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::UInt128>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  set_.uint128(o, i, ZuBox<uint128_t>{s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Enum>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *field, const ZtFieldVFmt &
) const {
  set_.enum_(o, i, field->type->info.enum_()->s2v(s));
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Flags>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  set_.flags(o, i, field->type->info.flags()->scan(s, fmt));
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Float>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  set_.float_(o, i, ZuBox<double>{s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Fixed>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  set_.fixed(o, i, ZuFixed{s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Decimal>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &
) const {
  set_.decimal(o, i, ZuDecimal{s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Time>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  set_.time(o, i, ZuDateTime{fmt.dateScan, s}.as_time());
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::DateTime>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  set_.dateTime(o, i, ZuDateTime{fmt.dateScan, s});
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::UDT>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *field,
  const ZtFieldVFmt &fmt
) const {
  field->type->info.udt()->scan(field->set.set_.udt, o, i, s, fmt);
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
  inline unsigned scan(ZuString &s,
    ZuString vecPrefix, ZuString vecDelim, ZuString vecSuffix, L l)
  {
    auto begin = &s[0];
    skip(s);
    if (!match(s, vecPrefix)) return 0;
    skip(s);
    while (l(s)) {
      skip(s);
      if (!match(s, vecDelim)) {
	match(s, vecSuffix);
	break;
      }
    }
    return &s[0] - begin;
  }

} // VecScan

template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::CStringVec>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, [this, o, i, &fmt](ZuString &s) {
    auto m = s.length();
    auto buf_ = ZmAlloc(uint8_t, m + 1);
    ZuArray<uint8_t> buf{&buf_[0], m + 1};
    unsigned n = Scan::strElem(buf, s, fmt.vecDelim, fmt.vecSuffix);
    if (n) {
      buf[n] = 0;
      set_.cstring(o, i, &buf[0]);
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::StringVec>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, [this, o, i, &fmt](ZuString &s) {
    auto m = s.length();
    auto buf_ = ZmAlloc(uint8_t, m);
    ZuArray<uint8_t> buf{&buf_[0], m};
    unsigned n = Scan::strElem(buf, s, fmt.vecDelim, fmt.vecSuffix);
    if (n) {
      buf.trunc(n);
      set_.string(o, i, buf);
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::BytesVec>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, [this, o, i](ZuString &s) {
    unsigned n = 0;
    auto m = s.length();
    while (n < m && ZuBase64::is(s[n])) n++;
    n = ZuBase64::declen(m = n);
    if (n) {
      auto buf_ = ZmAlloc(uint8_t, n);
      ZuArray<uint8_t> buf{&buf_[0], n};
      buf.trunc(ZuBase64::decode(buf, ZuBytes{s}));
      set_.bytes(o, i, buf);
      s.offset(m);
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::IntVec>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, [this, o, i](ZuString &s) {
    ZuBox<int64_t> v;
    unsigned n = v.scan(s);
    if (n) {
      set_.int_(o, i, v);
      s.offset(n);
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::UIntVec>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, [this, o, i](ZuString &s) {
    ZuBox<uint64_t> v;
    unsigned n = v.scan(s);
    if (n) {
      set_.uint(o, i, v);
      s.offset(n);
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::Int128Vec>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, [this, o, i](ZuString &s) {
    ZuBox<int128_t> v;
    unsigned n = v.scan(s);
    if (n) {
      set_.int128(o, i, v);
      s.offset(n);
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::UInt128Vec>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, [this, o, i](ZuString &s) {
    ZuBox<uint128_t> v;
    unsigned n = v.scan(s);
    if (n) {
      set_.uint128(o, i, v);
      s.offset(n);
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::FloatVec>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, [this, o, i](ZuString &s) {
    ZuBox<double> v;
    unsigned n = v.scan(s);
    if (n) {
      set_.float_(o, i, v);
      s.offset(n);
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::FixedVec>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, [this, o, i](ZuString &s) {
    ZuFixed v;
    unsigned n = v.scan(s);
    if (n) {
      set_.fixed(o, i, v);
      s.offset(n);
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::DecimalVec>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, [this, o, i](ZuString &s) {
    ZuDecimal v;
    unsigned n = v.scan(s);
    if (n) {
      set_.decimal(o, i, v);
      s.offset(n);
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::TimeVec>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, [this, o, i, &fmt](ZuString &s) {
    ZuDateTime v;
    unsigned n = v.scan(fmt.dateScan, s);
    if (n) {
      set_.time(o, i, v.as_time());
      s.offset(n);
      return true;
    }
    return false;
  });
}
template <unsigned Code>
inline ZuIfT<Code == ZtFieldTypeCode::DateTimeVec>
MSet::scan(
  void *o, unsigned i, ZuString s, const ZtMField *, const ZtFieldVFmt &fmt
) const {
  VecScan::scan(s, [this, o, i, &fmt](ZuString &s) {
    ZuDateTime v;
    unsigned n = v.scan(fmt.dateScan, s);
    if (n) {
      set_.dateTime(o, i, ZuMv(v));
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
    return {.get_ = {.string = [](const void *o, unsigned) -> ZuString {
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
  template <typename = ZtFieldFmt::Default> struct Print {
    ZuBytes v;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      auto n = ZuBase64::enclen(print.v.length());
      auto buf_ = ZmAlloc(uint8_t, n);
      ZuArray<uint8_t> buf{&buf_[0], n};
      buf.trunc(ZuBase64::encode(buf, print.v));
      return s << buf;
    }
  };
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
    return {.get_ = {.bytes = [](const void *o, unsigned) -> ZuBytes {
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
template <typename Base, auto Def>
struct ZtField_Bool<Base, Def, false> :
    public ZtField_Bool<Base, Def, true> {
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
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZuBox<int64_t> v;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      if constexpr (ZuTypeIn<ZuFieldProp::Hex, Props>{})
	return s << print.v.template hex<false, Fmt>();
      else
	return s << print.v.template fmt<Fmt>();
    }
  };
  inline static ZtMFieldType *mtype();
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
ZtMFieldType *ZtFieldType_Int<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_Int<T, Props>>::instance();
}

template <typename T>
struct ZtFieldType_Int_Def {
  constexpr static auto deflt() { return ZuCmp<T>::null(); }
  constexpr static auto minimum() { return ZuCmp<T>::minimum(); }
  constexpr static auto maximum() { return ZuCmp<T>::maximum(); }
};
template <
  typename Base,
  auto Def = ZtFieldType_Int_Def<typename Base::T>::deflt,
  auto Min = ZtFieldType_Int_Def<typename Base::T>::minimum,
  auto Max = ZtFieldType_Int_Def<typename Base::T>::maximum,
  bool = Base::ReadOnly>
struct ZtField_Int : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Int<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
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
template <typename Base, auto Def, auto Min, auto Max>
struct ZtField_Int<Base, Def, Min, Max, false> :
    public ZtField_Int<Base, Def, Min, Max, true> {
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
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZuBox<uint64_t> v;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      if constexpr (ZuTypeIn<ZuFieldProp::Hex, Props>{})
	return s << print.v.template hex<false, Fmt>();
      else
	return s << print.v.template fmt<Fmt>();
    }
  };
  inline static ZtMFieldType *mtype();
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
ZtMFieldType *ZtFieldType_UInt<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_UInt<T, Props>>::instance();
}

template <
  typename Base,
  auto Def = ZtFieldType_Int_Def<typename Base::T>::deflt,
  auto Min = ZtFieldType_Int_Def<typename Base::T>::minimum,
  auto Max = ZtFieldType_Int_Def<typename Base::T>::maximum,
  bool = Base::ReadOnly>
struct ZtField_UInt : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_UInt<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
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
template <typename Base, auto Def, auto Min, auto Max>
struct ZtField_UInt<Base, Def, Min, Max, false> :
    public ZtField_UInt<Base, Def, Min, Max, true> {
  using O = typename Base::O;
  using T = typename Base::T;
  static ZtMFieldSet setFn() {
    return {.set_ = {.uint = [](void *o, unsigned, uint64_t v) {
      Base::set(*static_cast<O *>(o), v);
    }}};
  }
};

// --- Int128

template <typename T_, typename Props_>
struct ZtFieldType_Int128 : public ZtFieldType_<Props_> {
  ZuAssert(ZuTraits<T_>::IsInt128egral);
  ZuAssert(ZuTraits<T_>::IsSigned);
  enum { Code = ZtFieldTypeCode::Int128 };
  using T = T_;
  using Props = Props_;
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZuBox<int128_t> v;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      if constexpr (ZuTypeIn<ZuFieldProp::Hex, Props>{})
	return s << print.v.template hex<false, Fmt>();
      else
	return s << print.v.template fmt<Fmt>();
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Int128, T, void, Props> :
    public ZtFieldType_Int128<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_Int128 : public ZtMFieldType {
  ZtMFieldType_Int128() : ZtMFieldType{
    .code = ZtFieldTypeCode::Int128,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_Int128<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_Int128<T, Props>>::instance();
}

template <typename T>
struct ZtFieldType_Int128_Def {
  constexpr static auto deflt() { return ZuCmp<T>::null(); }
  constexpr static auto minimum() { return ZuCmp<T>::minimum(); }
  constexpr static auto maximum() { return ZuCmp<T>::maximum(); }
};
template <
  typename Base,
  auto Def = ZtFieldType_Int128_Def<typename Base::T>::deflt,
  auto Min = ZtFieldType_Int128_Def<typename Base::T>::minimum,
  auto Max = ZtFieldType_Int128_Def<typename Base::T>::maximum,
  bool = Base::ReadOnly>
struct ZtField_Int128 : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Int128<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_Int128<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.int128 = [](const void *o, unsigned) -> int128_t {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.int128 = [](void *, unsigned, int128_t) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.int128 = [](const void *o, unsigned) -> int128_t {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	case Minimum: return Min();
	case Maximum: return Max();
	default:      return ZuCmp<int128_t>::null();
      }
    }}};
  }
};
template <typename Base, auto Def, auto Min, auto Max>
struct ZtField_Int128<Base, Def, Min, Max, false> :
    public ZtField_Int128<Base, Def, Min, Max, true> {
  using O = typename Base::O;
  using T = typename Base::T;
  static ZtMFieldSet setFn() {
    return {.set_ = {.int128 = [](void *o, unsigned, int128_t v) {
      Base::set(*static_cast<O *>(o), v);
    }}};
  }
};

// --- UInt128

template <typename T_, typename Props_>
struct ZtFieldType_UInt128 : public ZtFieldType_<Props_> {
  ZuAssert(ZuTraits<T_>::IsInt128egral);
  ZuAssert(!ZuTraits<T_>::IsSigned);
  enum { Code = ZtFieldTypeCode::UInt128 };
  using T = T_;
  using Props = Props_;
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZuBox<uint128_t> v;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      if constexpr (ZuTypeIn<ZuFieldProp::Hex, Props>{})
	return s << print.v.template hex<false, Fmt>();
      else
	return s << print.v.template fmt<Fmt>();
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::UInt128, T, void, Props> :
    public ZtFieldType_UInt128<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_UInt128 : public ZtMFieldType {
  ZtMFieldType_UInt128() : ZtMFieldType{
    .code = ZtFieldTypeCode::UInt128,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_UInt128<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_UInt128<T, Props>>::instance();
}

template <
  typename Base,
  auto Def = ZtFieldType_Int128_Def<typename Base::T>::deflt,
  auto Min = ZtFieldType_Int128_Def<typename Base::T>::minimum,
  auto Max = ZtFieldType_Int128_Def<typename Base::T>::maximum,
  bool = Base::ReadOnly>
struct ZtField_UInt128 : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_UInt128<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_UInt128<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.uint128 = [](const void *o, unsigned) -> uint128_t {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.uint128 = [](void *, unsigned, uint128_t) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.uint128 = [](const void *o, unsigned) -> uint128_t {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	case Minimum: return Min();
	case Maximum: return Max();
	default:      return ZuCmp<uint128_t>::null();
      }
    }}};
  }
};
template <typename Base, auto Def, auto Min, auto Max>
struct ZtField_UInt128<Base, Def, Min, Max, false> :
    public ZtField_UInt128<Base, Def, Min, Max, true> {
  using O = typename Base::O;
  using T = typename Base::T;
  static ZtMFieldSet setFn() {
    return {.set_ = {.uint128 = [](void *o, unsigned, uint128_t v) {
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
template <typename Base, typename Map, auto Def>
struct ZtField_Enum<Base, Map, Def, false> :
    public ZtField_Enum<Base, Map, Def, true> {
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
    return {.get_ = {.flags = [](const void *o, unsigned) -> uint128_t {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.flags = [](void *, unsigned, uint64_t) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.flags = [](const void *o, unsigned) -> uint128_t {
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
    return {.set_ = {.flags = [](void *o, unsigned, uint128_t v) {
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
template <typename Base, auto Def, auto Min, auto Max>
struct ZtField_Float<Base, Def, Min, Max, false> :
    public ZtField_Float<Base, Def, Min, Max, true> {
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
template <typename Base, auto Def, auto Min, auto Max>
struct ZtField_Fixed<Base, Def, Min, Max, false> :
    public ZtField_Fixed<Base, Def, Min, Max, true> {
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
template <typename Base, auto Def, auto Min, auto Max>
struct ZtField_Decimal<Base, Def, Min, Max, false> :
    public ZtField_Decimal<Base, Def, Min, Max, true> {
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

inline constexpr ZuTime ZtFieldType_Time_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtFieldType_Time_Def,
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
template <typename Base, auto Def>
struct ZtField_Time<Base, Def, false> :
    public ZtField_Time<Base, Def, true> {
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

inline constexpr ZuDateTime ZtFieldType_DateTime_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtFieldType_DateTime_Def,
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
template <typename Base, auto Def>
struct ZtField_DateTime<Base, Def, false> :
    public ZtField_DateTime<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    return {.set_ = {.dateTime = [](void *o, unsigned, ZuDateTime v) {
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
    return [](const void *, ZmStream &, const ZtFieldVFmt &) { };
  }
};
template <typename T>
struct ZtMFieldType_UDT_Print<T,
  decltype((ZuDeclVal<ZmStream &>() << ZuDeclVal<const T &>()), void())> {
  static auto printFn() {
    return [](const void *v, ZmStream &s, const ZtFieldVFmt &) {
      s << *reinterpret_cast<const T *>(v);
    };
  }
};
template <typename T, typename = void>
struct ZtMFieldType_UDT_Scan {
  static auto scanFn() {
    return [](
      void (*)(void *, unsigned, const void *), void *, unsigned,
      ZuString, const ZtFieldVFmt &) { };
  }
};
template <typename T>
struct ZtMFieldType_UDT_Scan<
  T, decltype((ZuDeclVal<T &>() = ZuString{}), void())
> {
  static auto scanFn() {
    return [](
      void (*set)(void *, unsigned, const void *), void *o, unsigned i,
      ZuString s, const ZtFieldVFmt &
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
    return {.set_ = {.udt = [](void *o, unsigned, const void *p) {
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

inline ZtField_::CStringVec ZtFieldType_CStringVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtFieldType_CStringVec_Def,
  bool = Base::ReadOnly>
struct ZtField_CStringVec : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_CStringVec<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_CStringVec<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  using CStringVec = ZtField_::CStringVec;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.cstringVec = [](const void *o, unsigned) -> CStringVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    return {.set_ = {.cstringVec = [](void *, unsigned, CStringVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.cstringVec = [](const void *o, unsigned) -> CStringVec {
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
    return {.set_ = {.cstringVec = [](void *o, unsigned, CStringVec v) {
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

inline ZtField_::StringVec ZtFieldType_StringVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtFieldType_StringVec_Def,
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
    return {.get_ = {.stringVec = [](const void *o, unsigned) -> StringVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.stringVec = [](void *, unsigned, StringVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.stringVec = [](const void *o, unsigned) -> StringVec {
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
    return {.set_ = {.stringVec = [](void *o, unsigned, StringVec v) {
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
	auto n = ZuBase64::enclen(v.length());
	auto buf_ = ZmAlloc(uint8_t, n);
	ZuArray<uint8_t> buf{&buf_[0], n};
	buf.trunc(ZuBase64::encode(buf, v));
	s << buf;
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

inline ZtField_::BytesVec ZtFieldType_BytesVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtFieldType_BytesVec_Def,
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
    return {.get_ = {.bytesVec = [](const void *o, unsigned) -> BytesVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.bytesVec = [](void *, unsigned, BytesVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.bytesVec = [](const void *o, unsigned) -> BytesVec {
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
    return {.set_ = {.bytesVec = [](void *o, unsigned, BytesVec v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
  }
};

// --- IntVec

template <typename T_, typename Props_>
struct ZtFieldType_IntVec : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::IntVec };
  using T = T_;
  using Props = Props_;
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZtField_::IntVec vec;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      s << Fmt::VecPrefix();
      bool first = true;
      print.vec.all([&s, &first](ZuBox<int64_t> v) {
	if (!first) s << Fmt::VecDelim(); else first = false;
	s << v.template fmt<Fmt>();
      });
      return s << Fmt::VecSuffix();
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::IntVec, T, void, Props> :
    public ZtFieldType_IntVec<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_IntVec : public ZtMFieldType {
  ZtMFieldType_IntVec() : ZtMFieldType{
    .code = ZtFieldTypeCode::IntVec,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_IntVec<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_IntVec<T, Props>>::instance();
}

inline ZtField_::IntVec ZtFieldType_IntVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtFieldType_IntVec_Def,
  bool = Base::ReadOnly>
struct ZtField_IntVec : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_IntVec<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_IntVec<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  using IntVec = ZtField_::IntVec;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.intVec = [](const void *o, unsigned) -> IntVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.intVec = [](void *, unsigned, IntVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.intVec = [](const void *o, unsigned) -> IntVec {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_IntVec<Base, Def, false> :
    public ZtField_IntVec<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.intVec = [](void *o, unsigned, IntVec v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
  }
};

// --- UIntVec

template <typename T_, typename Props_>
struct ZtFieldType_UIntVec : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::UIntVec };
  using T = T_;
  using Props = Props_;
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZtField_::UIntVec vec;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      s << Fmt::VecPrefix();
      bool first = true;
      print.vec.all([&s, &first](ZuBox<uint64_t> v) {
	if (!first) s << Fmt::VecDelim(); else first = false;
	s << v.template fmt<Fmt>();
      });
      return s << Fmt::VecSuffix();
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::UIntVec, T, void, Props> :
    public ZtFieldType_UIntVec<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_UIntVec : public ZtMFieldType {
  ZtMFieldType_UIntVec() : ZtMFieldType{
    .code = ZtFieldTypeCode::UIntVec,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_UIntVec<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_UIntVec<T, Props>>::instance();
}

inline ZtField_::UIntVec ZtFieldType_UIntVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtFieldType_UIntVec_Def,
  bool = Base::ReadOnly>
struct ZtField_UIntVec : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_UIntVec<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_UIntVec<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  using UIntVec = ZtField_::UIntVec;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.uintVec = [](const void *o, unsigned) -> UIntVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.uintVec = [](void *, unsigned, UIntVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.uintVec = [](const void *o, unsigned) -> UIntVec {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_UIntVec<Base, Def, false> :
    public ZtField_UIntVec<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.uintVec = [](void *o, unsigned, UIntVec v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
  }
};

// --- Int128Vec

template <typename T_, typename Props_>
struct ZtFieldType_Int128Vec : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::Int128Vec };
  using T = T_;
  using Props = Props_;
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZtField_::Int128Vec vec;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      s << Fmt::VecPrefix();
      bool first = true;
      print.vec.all([&s, &first](ZuBox<int128_t> v) {
	if (!first) s << Fmt::VecDelim(); else first = false;
	s << v.template fmt<Fmt>();
      });
      return s << Fmt::VecSuffix();
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::Int128Vec, T, void, Props> :
    public ZtFieldType_Int128Vec<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_Int128Vec : public ZtMFieldType {
  ZtMFieldType_Int128Vec() : ZtMFieldType{
    .code = ZtFieldTypeCode::Int128Vec,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_Int128Vec<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_Int128Vec<T, Props>>::instance();
}

inline ZtField_::Int128Vec ZtFieldType_Int128Vec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtFieldType_Int128Vec_Def,
  bool = Base::ReadOnly>
struct ZtField_Int128Vec : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_Int128Vec<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_Int128Vec<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  using Int128Vec = ZtField_::Int128Vec;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.int128Vec = [](const void *o, unsigned) -> Int128Vec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.int128Vec = [](void *, unsigned, Int128Vec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.int128Vec = [](const void *o, unsigned) -> Int128Vec {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_Int128Vec<Base, Def, false> :
    public ZtField_Int128Vec<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.int128Vec = [](void *o, unsigned, Int128Vec v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
  }
};

// --- UInt128Vec

template <typename T_, typename Props_>
struct ZtFieldType_UInt128Vec : public ZtFieldType_<Props_> {
  enum { Code = ZtFieldTypeCode::UInt128Vec };
  using T = T_;
  using Props = Props_;
  template <typename Fmt = ZtFieldFmt::Default> struct Print {
    ZtField_::UInt128Vec vec;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      s << Fmt::VecPrefix();
      bool first = true;
      print.vec.all([&s, &first](ZuBox<uint128_t> v) {
	if (!first) s << Fmt::VecDelim(); else first = false;
	s << v.template fmt<Fmt>();
      });
      return s << Fmt::VecSuffix();
    }
  };
  inline static ZtMFieldType *mtype();
};
template <typename T, typename Props>
struct ZtFieldType<ZtFieldTypeCode::UInt128Vec, T, void, Props> :
    public ZtFieldType_UInt128Vec<T, Props> { };

template <typename T, typename Props>
struct ZtMFieldType_UInt128Vec : public ZtMFieldType {
  ZtMFieldType_UInt128Vec() : ZtMFieldType{
    .code = ZtFieldTypeCode::UInt128Vec,
    .props = ZtMFieldProp::Value<Props>{},
    .info = {.null = nullptr}
  } { }
};
template <typename T, typename Props>
ZtMFieldType *ZtFieldType_UInt128Vec<T, Props>::mtype() {
  return ZmSingleton<ZtMFieldType_UInt128Vec<T, Props>>::instance();
}

inline ZtField_::UInt128Vec ZtFieldType_UInt128Vec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtFieldType_UInt128Vec_Def,
  bool = Base::ReadOnly>
struct ZtField_UInt128Vec : public ZtField<Base> {
  template <template <typename> typename Override>
  using Adapt = ZtField_UInt128Vec<Override<Base>>;
  using O = typename Base::O;
  using T = typename Base::T;
  using Props = typename Base::Props;
  using Type = ZtFieldType_UInt128Vec<T, ZuTypeGrep<ZtFieldType_Props, Props>>;
  using UInt128Vec = ZtField_::UInt128Vec;
  enum { Code = Type::Code };
  static ZtMFieldGet getFn() {
    return {.get_ = {.uint128Vec = [](const void *o, unsigned) -> UInt128Vec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.uint128Vec = [](void *, unsigned, UInt128Vec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.uint128Vec = [](const void *o, unsigned) -> UInt128Vec {
      switch (int(reinterpret_cast<uintptr_t>(o))) {
	case Deflt:   return Def();
	default:      return {};
      }
    }}};
  }
};
template <typename Base, auto Def>
struct ZtField_UInt128Vec<Base, Def, false> :
    public ZtField_UInt128Vec<Base, Def, true> {
  using O = typename Base::O;
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.uint128Vec = [](void *o, unsigned, UInt128Vec v) {
      Base::set(*static_cast<O *>(o), ZuMv(v));
    }}};
  }
};

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

inline ZtField_::FloatVec ZtFieldType_FloatVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtFieldType_FloatVec_Def,
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
    return {.get_ = {.floatVec = [](const void *o, unsigned) -> FloatVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.floatVec = [](void *, unsigned, FloatVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.floatVec = [](const void *o, unsigned) -> FloatVec {
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
    return {.set_ = {.floatVec = [](void *o, unsigned, FloatVec v) {
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

inline ZtField_::FixedVec ZtFieldType_FixedVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtFieldType_FixedVec_Def,
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
    return {.get_ = {.fixedVec = [](const void *o, unsigned) -> FixedVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.fixedVec = [](void *, unsigned, FixedVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.fixedVec = [](const void *o, unsigned) -> FixedVec {
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
    return {.set_ = {.fixedVec = [](void *o, unsigned, FixedVec v) {
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

inline ZtField_::DecimalVec ZtFieldType_DecimalVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtFieldType_DecimalVec_Def,
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
    return {.get_ = {.decimalVec = [](const void *o, unsigned) -> DecimalVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.decimalVec = [](void *, unsigned, DecimalVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.decimalVec = [](const void *o, unsigned) -> DecimalVec {
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
    return {.set_ = {.decimalVec = [](void *o, unsigned, DecimalVec v) {
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

inline ZtField_::TimeVec ZtFieldType_TimeVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtFieldType_TimeVec_Def,
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
    return {.get_ = {.timeVec = [](const void *o, unsigned) -> TimeVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.timeVec = [](void *, unsigned, TimeVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.timeVec = [](const void *o, unsigned) -> TimeVec {
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
    return {.set_ = {.timeVec = [](void *o, unsigned, TimeVec v) {
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

inline ZtField_::DateTimeVec ZtFieldType_DateTimeVec_Def() { return {}; }
template <
  typename Base,
  auto Def = ZtFieldType_DateTimeVec_Def,
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
    return {.get_ = {.dateTimeVec = [](const void *o, unsigned) -> DateTimeVec {
      return Base::get(*static_cast<const O *>(o));
    }}};
  }
  static ZtMFieldSet setFn() {
    using namespace ZtField_;
    return {.set_ = {.dateTimeVec = [](void *, unsigned, DateTimeVec) { }}};
  }
  constexpr static auto deflt() { return Def(); }
  static ZtMFieldGet constantFn() {
    using namespace ZtMFieldConstant;
    return {.get_ = {.dateTimeVec = [](const void *o, unsigned) -> DateTimeVec {
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
    return {.set_ = {.dateTimeVec = [](void *o, unsigned, DateTimeVec v) {
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
#define ZtField_TypeArgs_Int(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_UInt(...) \
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
#define ZtField_TypeArgs_IntVec(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_UIntVec(...) \
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
