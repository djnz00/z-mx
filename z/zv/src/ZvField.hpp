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

// flat object introspection
// - print/scan (CSV, etc.)
// - ORM
// - data series

#ifndef ZvField_HPP
#define ZvField_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

#include <zlib/ZuPrint.hpp>
#include <zlib/ZuBox.hpp>
#include <zlib/ZuString.hpp>
#include <zlib/ZuFixed.hpp>
#include <zlib/ZuDecimal.hpp>
#include <zlib/ZuField.hpp>

#include <zlib/ZmStream.hpp>
#include <zlib/ZmTime.hpp>

#include <zlib/ZtEnum.hpp>
#include <zlib/ZtDate.hpp>

#define ZvFieldType(U, ID) ZvField_##U##_##ID

namespace ZvFieldType {
  enum _ {
    String = 0,		// a contiguous UTF-8 string
    Composite,		// composite type
    Bool,		// an integral type, interpreted as bool
    Int,		// an integral type <= 64bits
    Hex,		// an integral type printed in hex
    Enum,		// an integral enumerated type
    Flags,		// an integral bitfield of enumerations type
    Float,		// floating point type, interpreted as double
    Fixed,		// ZuFixed
    Decimal,		// ZuDecimal
    Time,		// ZmTime
    N
  };
}

namespace ZvFieldFlags {
  enum {
    Primary	= 0x00001,	// primary key
    Secondary	= 0x00002,	// secondary key
    ReadOnly	= 0x00004,	// read-only
    Update	= 0x00008,	// include in updates
    Ctor_	= 0x00010,	// constructor parameter
    Series	= 0x00020,	// data series
      Index	= 0x00040,	// - index (e.g. time stamp)
      Delta	= 0x00080,	// - first derivative
      Delta2	= 0x00100	// - second derivative
  };
  enum {
    CtorShift	= 12		// bit-shift for constructor parameter index
  };
  // parameter index -> flags
  inline constexpr unsigned Ctor(unsigned i) {
    return Ctor_ | (i<<CtorShift);
  }
  // flags -> parameter index
  inline constexpr unsigned CtorIndex(unsigned flags) {
    return flags>>CtorShift;
  }
}
#define ZvFieldMkFlags__(Flag) ZvFieldFlags::Flag |
#define ZvFieldMkFlags_(...) (ZuPP_Map(ZvFieldMkFlags__, __VA_ARGS__) 0)
#define ZvFieldMkFlags(Args) ZuPP_Defer(ZvFieldMkFlags_)(ZuPP_Strip(Args))

struct ZvTimeFmt_Null : public ZuPrintable {
  template <typename S> void print(S &) const { }
};
using ZvTimeFmt_FIX = ZtDateFmt::FIX<-9, TimeFmt_Null>;
ZuDeclUnion(ZvTimeFmt,
    (ZtDateFmt::CSV, csv),
    (TimeFmt_FIX, fix),
    (ZtDateFmt::ISO, iso));

struct ZvFieldFmt {
  Fmt() { new (time.init_csv()) ZtDateFmt::CSV{}; }

  ZuVFmt	scalar;			// scalar format (print only)
  ZvTimeFmt	time;			// date/time format
  char		flagsDelim = '|';	// flags delimiter
};

namespace ZvField_ {

template <typename Base, unsigned Flags_>
struct Field : public Base {
  enum { Flags = Flags_ };
};

template <typename Base>
struct RdString : public Field<Base, Flags> {
  enum { Type = ZvFieldType::String };
  template <typename S>
  static void print(const void *o, S &s, const Fmt &) {
    s << Base::get(o);
  }
  static auto getFn() {
    return [](const void *o) -> ZuString { return Base::get(o); };
  }
  static auto setFn() { return [](void *, ZuString) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const Fmt &) { }; }
};
template <typename Base, unsigned Flags>
struct String : public RdString<Base, Flags> {
  static void scan(void *o, ZuString s, const Fmt &) {
    Base::set(o, s);
  }
  static auto setFn() {
    return [](void *o, ZuString s) { Base::set(o, s); };
  }
  static auto scanFn() { return scan; }
};

template <typename Base>
struct RdComposite : public Field<Base, Flags> {
  enum { Type = ZvFieldType::Composite };
  template <typename S>
  static void print(const void *o, S &s, const Fmt &) {
    s << Base::get(o);
  }
  static auto getFn() {
    return [](const void *o) -> ZmStreamFn {
      return {o, [](const void *o, ZmStream &s) { s << Base::get(o); }};
    };
  }
  static auto setFn() { return [](void *, ZuString) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const Fmt &) { }; }
};
template <typename Base, unsigned Flags>
struct Composite : public RdComposite<Base, Flags> {
  static void scan(void *o, ZuString s, const Fmt &) {
    Base::set(o, s);
  }
  static auto setFn() {
    return [](void *o, ZuString s) { Base::set(o, s); };
  }
  static auto scanFn() { return scan; }
};

template <typename Base, unsigned Flags>
struct RdBool : public Field<Base, Flags> {
  enum { Type = ZvFieldType::Bool };
  template <typename S>
  static void print(const void *o, S &s, const Fmt &) {
    s << (Base::get(o) ? '1' : '0');
  }
  static auto getFn() {
    return [](const void *o) -> int64_t { return Base::get(o); };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const Fmt &) { }; }
};
template <typename Base, unsigned Flags>
struct Bool : public RdBool<Base, Flags> {
  static void scan(void *o, ZuString s, const Fmt &) {
    Base::set(o, s.length() == 1 && s[0] == '1');
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(o, v); };
  }
  static auto scanFn() { return Base::scan; }
};

template <
  typename Base, unsigned Flags,
  bool = ZuTraits<typename Base::T>::IsPrimitive>
struct RdInt : public Field<Base, Flags> {
  enum { Type = ZvFieldType::Int };
  template <typename S>
  static void print(const void *o, S &s, const Fmt &fmt) {
    s << Base::get(o).vfmt(fmt.scalar);
  }
  static auto getFn() {
    return [](const void *o) -> int64_t { return Base::get(o); };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const Fmt &) { }; }
};
template <typename Base, unsigned Flags>
struct Int : public RdInt<Base, Flags> {
  static void scan(void *o, ZuString s, const Fmt &) {
    Base::set(o, s);
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(o, v); };
  }
  static auto scanFn() { return scan; }
};
template <typename Base, unsigned Flags>
struct RdInt<Base, Flags, true> : public Field<Base, Flags> {
  enum { Type = ZvFieldType::Int };
  template <typename S>
  static void print(const void *o, S &s, const Fmt &fmt) {
    s << ZuBoxed(Base::get(o)).vfmt(fmt.scalar);
  }
  static auto getFn() {
    return [](const void *o) -> int64_t { return Base::get(o); };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const Fmt &) { }; }
};
template <typename Base, unsigned Flags>
struct Int : public RdInt<Base, Flags> {
  static void scan(void *o, ZuString s, const Fmt &) {
    using T = typename Base::T;
    Base::set(o, ZuBoxT<T>{s});
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(o, v); };
  }
  static auto scanFn() { return scan; }
};

template <typename Base, unsigned Flags>
struct RdHex : public Field<Base, Flags> {
  enum { Type = ZvFieldType::Hex };
  template <typename S>
  static void print(const void *o, S &s, const Fmt &fmt) {
    s << ZuBoxed(Base::get(o)).vfmt(fmt.scalar).hex();
  }
  static auto getFn() {
    return [](const void *o) -> int64_t { return Base::get(o); };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const Fmt &) { }; }
};
template <typename Base, unsigned Flags>
struct Hex : public RdHex<Base, Flags> {
  static void scan(void *o, ZuString s, const Fmt &) {
    using T = typename Base::T;
    Base::set(o, ZuBoxT<T>{ZuFmt::Hex<>{}, s});
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(o, v); };
  }
  static auto scanFn() { return Base::scan; }
};

template <typename Base, unsigned Flags, typename Map_>
struct RdEnum : public Field<Base, Flags> {
  enum { Type = ZvFieldType::Enum };
  using Map = Map_;
  template <typename S>
  static void print(const void *o, S &s, const Fmt &) {
    s << Map::v2s(Base::get(o));
  }
  static auto enumFn() {
    return []() -> ZvVFieldEnum * {
      return ZvVFieldEnum_<Map>::instance();
    };
  }
  static auto getFn() {
    return [](const void *o) -> int64_t { return Base::get(o); };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const Fmt &) { }; }
};
template <typename Base, unsigned Flags, typename Map>
struct Enum : public RdEnum<Base, Flags, Map> {
  static void scan(void *o, ZuString s, const Fmt &) {
    Base::set(o, Map::s2v(s));
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(o, v); };
  }
  static auto scanFn() { return Base::scan; }
};

template <typename Base, unsigned Flags, typename Map_>
struct RdFlags : public Field<Base, Flags> {
  enum { Type = ZvFieldType::Flags };
  using Map = Map_;
  template <typename S>
  static void print(const void *o, S &s, const Fmt &fmt) {
    Map::print(s, Base::get(o), fmt.flagsDelim);
  }
  static auto flagsFn() {
    return []() -> ZvVFieldFlags * {
      return ZvVFieldFlags_<Map>::instance();
    };
  }
  static auto getFn() {
    return [](const void *o) -> int64_t { return Base::get(o); };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const Fmt &) { }; }
};
template <typename Base, unsigned Flags, typename Map>
struct Flags : public RdFlags<Base, Flags, Map> {
  static void scan(void *o, ZuString s, const Fmt &fmt) {
    using T = typename Base::T;
    Base::set(o, Map::template scan<T>(s, fmt.flagsDelim));
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(o, v); };
  }
  static auto scanFn() { return Base::scan; }
};

template <
  typename Base, unsigned Flags, typename Exponent,
  typename T = typename Base::T,
  bool = ZuTraits<T>::IsPrimitive>
struct RdFloat : public Field<Base, Flags> {
  enum { Type = ZvFieldType::Float };
  template <typename S>
  static void print(const void *o, S &s, const Fmt &fmt) {
    s << Base::get(o).vfmt(fmt).fp(-Exponent::get(o));
  }
  static auto expFn() { return [](const void *o) { return Exponent::get(o); }; }
  static auto getFn() {
    return [](const void *o) -> double { return Base::get(o); };
  }
  static auto setFn() { return [](void *, double) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const Fmt &) { }; }
};
template <typename Base, unsigned Flags, typename Exponent>
struct Float : public RdFloat<Base, Flags, Exponent> {
  static void scan(void *o, ZuString s, const Fmt &) {
    Base::set(o, s);
  }
  static auto setFn() {
    return [](void *o, double v) { Base::set(o, v); };
  }
  static auto scanFn() { return scan; }
};
template <typename Base, unsigned Flags, typename Exponent, typename T>
struct RdFloat<Base, Flags, Exponent, T, true> :
    public Field<Base, Flags> {
  enum { Type = ZvFieldType::Float };
  template <typename S>
  static void print(const void *o, S &s, const Fmt &fmt) {
    s << ZuBoxed(Base::get(o)).vfmt(fmt.scalar).fp(-Exponent::get(o));
  }
  static auto expFn() { return [](const void *o) { return Exponent::get(o); }; }
  static auto getFn() {
    return [](const void *o) -> double { return Base::get(o); };
  }
  static auto setFn() { return [](void *, double) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const Fmt &) { }; }
};
template <typename Base, unsigned Flags, typename Exponent, typename T>
struct Float<Base, Flags, Exponent, T, true> :
    public RdFloat<Base, Flags, Exponent> {
  static void scan(void *o, ZuString s, const Fmt &) {
    Base::set(o, ZuBoxT<T>{s});
  }
  static auto setFn() {
    return [](void *o, double v) { Base::set(o, v); };
  }
  static auto scanFn() { return scan; }
};
template <typename Base, unsigned Flags, typename Exponent>
struct RdFloat<Base, Flags, Exponent, ZuFixed, false> :
    public Field<Base, Flags> {
  enum { Type = ZvFieldType::Fixed };
  template <typename S>
  static void print(const void *o, S &s, const Fmt &fmt) {
    s << Base::get(o).vfmt(fmt.scalar).fp(-Exponent::get(o));
  }
  static auto expFn() { return [](const void *o) { return Exponent::get(o); }; }
  static auto getFn() {
    return [](const void *o) -> ZuFixed { return Base::get(o); };
  }
  static auto setFn() { return [](void *, ZuFixed) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const Fmt &) { }; }
};
template <typename Base, unsigned Flags, typename Exponent>
struct Float<Base, Flags, Exponent, ZuFixed, false> :
    public RdFloat<Base, Flags, Exponent> {
  static void scan(void *o, ZuString s, const Fmt &) {
    Base::set(o, ZuFixed{s, Exponent::get(o)});
  }
  static auto setFn() {
    return [](void *o, ZuFixed v) { Base::set(o, v); };
  }
  static auto scanFn() { return :scan; }
};
template <typename Base, unsigned Flags, typename Exponent>
struct RdFloat<Base, Flags, Exponent, ZuDecimal, false> :
    public Field<Base, Flags> {
  enum { Type = ZvFieldType::Decimal };
  template <typename S>
  static void print(const void *o, S &s, const Fmt &fmt) {
    s << Base::get(o).vfmt(fmt.scalar).fp(-Exponent::get(o));
  }
  static auto expFn() { return [](const void *o) { return Exponent::get(o); }; }
  static auto getFn() {
    return [](const void *o) -> ZuDecimal { return Base::get(o); };
  }
  static auto setFn() { return [](void *, ZuDecimal) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const Fmt &) { }; }
};
template <typename Base, unsigned Flags, typename Exponent>
struct Float<Base, Flags, Exponent, ZuDecimal, false> :
    public RdFloat<Base, Flags, Exponent> {
  static void scan(void *o, ZuString s, const Fmt &) {
    Base::set(o, s);
  }
  static auto setFn() {
    return [](void *o, ZuDecimal v) { Base::set(o, v); };
  }
  static auto scanFn() { return scan; }
};

template <typename Base, unsigned Flags>
struct RdTime : public Field<Base, Flags> {
  enum { Type = ZvFieldType::Time };
  template <typename S>
  static void print(const void *o, S &s, const Fmt &fmt) {
    ZtDate v{Base::get(o)};
    switch (fmt.time.type()) {
      default:
      case TimeFmt::Index<ZtDateFmt::CSV>::I:
	s << v.csv(fmt.time.csv());
	break;
      case TimeFmt::Index<TimeFmt_FIX>::I:
	s << v.fix(fmt.time.fix());
	break;
      case TimeFmt::Index<ZtDateFmt::ISO>::I:
	s << v.iso(fmt.time.iso());
	break;
    }
  }
  static auto getFn() {
    return [](const void *o) -> ZmTime { return Base::get(o); };
  }
  static auto setFn() { return [](void *, ZmTime) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const Fmt &) { }; }
};
template <typename Base, unsigned Flags>
struct Time : public RdTime<Base, Flags> {
  static void scan(void *o, ZuString s, const Fmt &fmt) {
    switch (fmt.time.type()) {
      default:
      case TimeFmt::Index<ZtDateFmt::CSV>::I:
	Base::set(o, ZtDate{ZtDate::CSV, s});
	break;
      case TimeFmt::Index<TimeFmt_FIX>::I:
	Base::set(o, ZtDate{ZtDate::FIX, s});
	break;
      case TimeFmt::Index<ZtDateFmt::ISO>::I:
	Base::set(o, ZtDate{s});
	break;
    }
  }
  static auto setFn() {
    return [](void *o, ZmTime v) { Base::set(o, v); };
  }
  static auto scanFn() { return Base::scan; }
};

} // namespace ZvField_

#define ZvFieldXRd(U, Type, ID, Member, Flags_. ...) \
  using ZvFieldType(U, ID) = \
  ZvField_::Rd##Type<ZuFieldXRd(U, ID, Member), \
    (ZvFieldMkFlags(Flags_) | ZvFieldFlags::ReadOnly) \
    __VA_OPT__(, __VA_ARGS__)>;
#define ZvFieldRd(U, Type, Member, Flags) \
  ZvFieldXRd(U, Type, Member, Member, Flags)
#define ZvFieldX(U, Type, ID, Member, Flags, ...) \
  using ZvFieldType(U, ID) = \
  ZvField_::Type<ZuFieldX(U, ID, Member), ZvFieldMkFlags(Flags), \
    __VA_OPT__(, __VA_ARGS__)>;
#define ZvField(U, Type, Member, Flags) \
  ZvFieldX(U, Type, Member, Member, Flags)

#define ZvFieldXRdFn(U, Type, ID, Get, Flags_) \
  using ZvFieldType(U, ID) = \
  ZvField_::Rd##Type<ZuFieldXRdFn(U, ID, Get), \
    (ZvFieldMkFlags(Flags_) | ZvFieldFlags::ReadOnly) \
    __VA_OPT__(, __VA_ARGS__)>;
#define ZvFieldRdFn(U, Type, Fn, Flags) \
  ZvFieldXRdFn(U, Type, Fn, Fn, Flags)
#define ZvFieldXFn(U, Type, ID, Get, Set, Flags, ...) \
  using ZvFieldType(U, ID) = \
  ZvField_::Type<ZuFieldXFn(U, ID, Get, Set), ZvFieldMkFlags(Flags), \
    __VA_OPT__(, __VA_ARGS__)>;
#define ZvFieldFn(U, Type, Fn, Flags) \
  ZvFieldXFn(U, Type, Fn, Fn, Fn, Flags)

#define ZvFieldRdLambda(U, Type, ID, Get, Flags_) \
  using ZvFieldType(U, ID) = \
  ZvField_::Rd##Type<ZuFieldRdLambda(U, ID, Get), \
    (ZvFieldMkFlags(Flags_) | ZvFieldFlags::ReadOnly) \
    __VA_OPT__(, __VA_ARGS__)>;
#define ZvFieldLambda(U, Type, ID, Get, Set, Flags, ...) \
  using ZvFieldType(U, ID) = \
  ZvField_::Type<ZuFieldLambda(U, ID, Get, Set), ZvFieldMkFlags(Flags), \
    __VA_OPT__(, __VA_ARGS__)>;

#define ZvField_Decl_(U, Method, ...) ZvField##Method(U, __VA_ARGS__)
#define ZvField_Decl(U, Args) ZuPP_Defer(ZvField_Decl_)(U, ZuPP_Strip(Args))

#define ZvField_Type_(U, Method, Type, ID, ...) ZvFieldType(U, ID)
#define ZvField_Type(U, Args) ZuPP_Defer(ZvField_Type_)(U, ZuPP_Strip(Args))

#define ZvFields(U, ...)  \
  namespace { \
    ZuPP_Eval(ZuPP_MapArg(ZuField_Decl, U, __VA_ARGS__)) \
    ZuPP_Eval(ZuPP_MapArg(ZvField_Decl, U, __VA_ARGS__)) \
    using ZvFields_##U = \
      ZuTypeList<ZuPP_Eval(ZuPP_MapArgComma(ZvField_Type, U, __VA_ARGS__))>; \
  } \
  U *ZuFielded_(U *); \
  ZuFields_##U ZuFieldList_(U *)

template <typename Impl> struct ZuFieldPrint : public ZuPrintable {
  const Impl *impl() const { return static_cast<const Impl *>(this); }
  Impl *impl() { return static_cast<Impl *>(this); }

  template <typename T>
  struct Print_Filter { enum { OK = !(T::Flags & Flags::ReadOnly) }; };
  template <typename S> void print(S &s) const {
    using FieldList = ZuTypeGrep<Print_Filter, ZuFieldList<Impl>>;
    thread_local Fmt fmt;
    ZuTypeAll<FieldList>::invoke([o = impl(), &s]<typename Field>() {
      if constexpr (ZuTypeIndex<Field, FieldList>::I) s << ' ';
      s << Field::id() << '=';
      Field::print(o, s, fmt);
    });
  }
};

template <typename V, typename FieldList>
struct ZvFieldKey__ {
  template <typename ...Fields>
  struct Mk_ {
    using Tuple = ZuTuple<typename Fields::T...>;
    static auto tuple(const V &v) { return Tuple{Fields::get(&v)...}; }
  };
  using Mk = ZuTypeApply<Mk_, FieldList>;
};
template <typename V> struct ZvFieldKey_ {
  template <typename T>
  struct Filter { enum { OK = T::Flags & Flags::Primary }; };
  using FieldList = ZuTypeGrep<Filter, ZuFieldList<V>>;
  using Mk = typename ZvFieldKey__<V, FieldList>::Mk;
};
template <typename V>
auto ZvFieldKey(const V &v) {
  return ZvFieldKey_<V>::Mk::tuple(v);
}

// run-time introspection

struct ZvVFieldEnum {
  const char	*(*id)();
  int		(*s2v)(ZuString);
  ZuString	(*v2s)(int);
};
template <typename Map>
struct ZvVFieldEnum_ : public ZvVFieldEnum {
  ZvVFieldEnum_() : ZvVFieldEnum{
    .id = []() -> const char * { return Map::id(); },
    .s2v = [](ZuString s) -> int { return Map::s2v(s); },
    .v2s = [](int i) -> ZuString { return Map::v2s(i); }
  } { }

  static ZvVFieldEnum_ *instance() {
    return ZmSingleton<ZvVFieldEnum_>::instance();
  }
};

struct ZvVFieldFlags {
  const char	*(*id)();
  void		(*print)(uint64_t, ZmStream &, const ZvFieldFmt &);
  void		(*scan)(uint64_t &, ZuString, const ZvFieldFmt &);
};
template <typename Map>
struct ZvVFieldFlags_ : public ZvVFieldFlags {
  ZvVFieldFlags_() : ZvVFieldFlags{
    .id = []() -> const char * { return Map::id(); },
    .print = [](uint64_t v, ZmStream &s, const ZvFieldFmt &fmt) -> void {
      Map::print(s, v, fmt.flagsDelim);
    },
    .scan = [](uint64_t &v, ZuString s, const ZvFieldFmt &fmt) -> void {
      v = Map::template scan<uint64_t>(s, fmt.flagsDelim);
    }
  } { }

  static ZvVFieldFlags_ *instance() {
    return ZmSingleton<ZvVFieldFlags_>::instance();
  }
};

struct ZvVField {
  const char	*id;
  uint32_t	type;		// ZvFieldType
  uint32_t	flags;		// ZvFieldFlags

  int		(*cmp)(const void *, const void *);

  union {
    unsigned		(*exponent)(const void *);	// Float|Fixed|Decimal
    ZvVFieldEnum	*(*enum_)();			// Enum
    ZvVFieldFlags	*(*flags)();			// Flags
  } info;

  void		(*print)(const void *, ZmStream &, const ZvFieldFmt &);
  void		(*scan)(void *, ZuString, const ZvFieldFmt &);
  union {
    ZuString	(*string)(const void *);	// String
    ZmStreamFn	(*composite)(const void *);	// Composite
    int64_t	(*int_)(const void *);		// Bool|Int|Hex|Enum|Flags
    double	(*float_)(const void *);	// Float
    ZuFixed	(*fixed)(const void *);		// Fixed
    ZuDecimal	(*decimal)(const void *);	// Decimal
    ZmTime	(*time)(const void *);		// Time
  } getFn;
  union {
    void	(*string)(void *, ZuString);	// String|Composite
    void	(*int_)(void *, int64_t);	// Bool|Int|Hex|Enum|Flags
    void	(*float_)(void *, double);	// Float
    void	(*fixed)(void *, ZuFixed);	// Fixed
    void	(*decimal)(void *, ZuDecimal);	// Decimal
    void	(*time)(void *, ZmTime);	// Time
  } setFn;

  template <typename Field>
  ZvVField(Field,
        ZuIfT<Field::Type == ZvFieldType::String> *_ = 0) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{[](const void *o1, const void *o2) {
	return ZuCmp<typename Field::T>::cmp(Field::get(o1), Field::get(o2));
      }},
      info{.exponent = nullptr},
      print{Field::template print<ZmStream>},
      scan{Field::scanFn()},
      getFn{.string = Field::getFn()},
      setFn{.string = Field::setFn()} { }

  template <typename Field>
  ZvVField(Field,
        ZuIfT<Field::Type == ZvFieldType::Composite> *_ = 0) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{[](const void *o1, const void *o2) {
	return ZuCmp<typename Field::T>::cmp(Field::get(o1), Field::get(o2));
      }},
      info{.exponent = nullptr},
      print{Field::template print<ZmStream>},
      scan{Field::scanFn()},
      getFn{.composite = Field::getFn()},
      setFn{.string = Field::setFn()} { }

  template <typename Field>
  ZvVField(Field,
        ZuIfT<
	  Field::Type == ZvFieldType::Bool ||
	  Field::Type == ZvFieldType::Int ||
	  Field::Type == ZvFieldType::Hex> *_ = 0) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{[](const void *o1, const void *o2) {
	return ZuCmp<typename Field::T>::cmp(Field::get(o1), Field::get(o2));
      }},
      info{.exponent = nullptr},
      print{Field::template print<ZmStream>},
      scan{Field::scanFn()},
      getFn{.int_ = Field::getFn()},
      setFn{.int_ = Field::setFn()} { }

  template <typename Field>
  ZvVField(Field,
        ZuIfT<Field::Type == ZvFieldType::Enum> *_ = 0) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{[](const void *o1, const void *o2) {
	return ZuCmp<typename Field::T>::cmp(Field::get(o1), Field::get(o2));
      }},
      info{.enum_ = Field::enumFn()},
      print{Field::template print<ZmStream>},
      scan{Field::scanFn()},
      getFn{.int_ = Field::getFn()},
      setFn{.int_ = Field::setFn()} { }

  template <typename Field>
  ZvVField(Field,
        ZuIfT<Field::Type == ZvFieldType::Flags> *_ = 0) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{[](const void *o1, const void *o2) {
	return ZuCmp<typename Field::T>::cmp(Field::get(o1), Field::get(o2));
      }},
      info{.flags = Field::flagsFn()},
      print{Field::template print<ZmStream>},
      scan{Field::scanFn()},
      getFn{.int_ = Field::getFn()},
      setFn{.int_ = Field::setFn()} { }

  template <typename Field>
  ZvVField(Field,
        ZuIfT<Field::Type == ZvFieldType::Float> *_ = 0) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{[](const void *o1, const void *o2) {
	return ZuCmp<typename Field::T>::cmp(Field::get(o1), Field::get(o2));
      }},
      info{.exponent = Field::expFn()},
      print{Field::template print<ZmStream>},
      scan{Field::scanFn()},
      getFn{.float_ = Field::getFn()},
      setFn{.float_ = Field::setFn()} { }

  template <typename Field>
  ZvVField(Field,
        ZuIfT<Field::Type == ZvFieldType::Fixed> *_ = 0) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{[](const void *o1, const void *o2) {
	return ZuCmp<typename Field::T>::cmp(Field::get(o1), Field::get(o2));
      }},
      info{.exponent = Field::expFn()},
      print{Field::template print<ZmStream>},
      scan{Field::scanFn()},
      getFn{.fixed = Field::getFn()},
      setFn{.fixed = Field::setFn()} { }

  template <typename Field>
  ZvVField(Field,
        ZuIfT<Field::Type == ZvFieldType::Decimal> *_ = 0) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{[](const void *o1, const void *o2) {
	return ZuCmp<typename Field::T>::cmp(Field::get(o1), Field::get(o2));
      }},
      info{.exponent = Field::expFn()},
      print{Field::template print<ZmStream>},
      scan{Field::scanFn()},
      getFn{.decimal = Field::getFn()},
      setFn{.decimal = Field::setFn()} { }

  template <typename Field>
  ZvVField(Field,
        ZuIfT<Field::Type == ZvFieldType::Time> *_ = 0) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{[](const void *o1, const void *o2) {
	return ZuCmp<typename Field::T>::cmp(Field::get(o1), Field::get(o2));
      }},
      info{.exponent = nullptr},
      print{Field::template print<ZmStream>},
      scan{Field::scanFn()},
      getFn{.decimal = Field::getFn()},
      setFn{.decimal = Field::setFn()} { }

  template <unsigned I, typename L>
  ZuIfT<I == ZvFieldType::String> get_(const void *o, L l) {
    l(getFn.string(o));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZvFieldType::Composite> get_(const void *o, L l) {
    l(getFn.composite(o));
  }
  template <unsigned I, typename L>
  ZuIfT<
    I == ZvFieldType::Bool || I == ZvFieldType::Int ||
    I == ZvFieldType::Fixed || I == ZvFieldType::Hex ||
    I == ZvFieldType::Enum || I == ZvFieldType::Flags> get_(
	const void *o, L l) {
    l(getFn.int_(o));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZvFieldType::Float> get_(const void *o, L l) {
    l(getFn.float_(o));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZvFieldType::Decimal> get_(const void *o, L l) {
    l(getFn.decimal(o));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZvFieldType::Time> get_(const void *o, L l) {
    l(getFn.time(o));
  }
  template <typename L> void get(const void *o, L l) {
    ZuSwitch::dispatch<ZvFieldType::N>(type,
	[this, o, l = ZuMv(l)](auto i) mutable { get_<i>(o, ZuMv(l)); });
  }

  template <unsigned I, typename L>
  ZuIfT<
    I == ZvFieldType::String || I == ZvFieldType::Composite> set_(
	void *o, L l) {
    setFn.string(o, l.template operator ()<ZuString>());
  }
  template <unsigned I, typename L>
  ZuIfT<
    I == ZvFieldType::Bool || I == ZvFieldType::Int ||
    I == ZvFieldType::Fixed || I == ZvFieldType::Hex ||
    I == ZvFieldType::Enum || I == ZvFieldType::Flags> set_(
	void *o, L l) {
    setFn.int_(o, l.template operator ()<int64_t>());
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZvFieldType::Float> set_(void *o, L l) {
    setFn.float_(o, l.template operator ()<double>());
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZvFieldType::Decimal> set_(void *o, L l) {
    setFn.decimal(o, l.template operator ()<ZuDecimal>());
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZvFieldType::Time> set_(void *o, L l) {
    setFn.time(o, l.template operator ()<ZmTime>());
  }
  template <typename L>
  void set(void *o, L l) {
    ZuSwitch::dispatch<ZvFieldType::N>(type,
	[this, o, l = ZuMv(l)](auto i) mutable { set_<i>(o, ZuMv(l)); });
  }
};

using ZvVFieldArray = ZuArray<const ZvVField>;

template <typename ...Fields>
struct ZvVFields__ {
  static ZvVFieldArray fields() {
    static const ZvVField fields_[] =
      // std::initializer_list<ZvVField>
    {
      ZvVField{Fields{}}...
    };
    return {&fields_[0], sizeof(fields_) / sizeof(fields_[0])};
  }
};
template <typename FieldList>
inline ZvVFieldArray ZvVFields_() {
  return ZuTypeApply<ZvVFields__, FieldList>::fields();
}
template <typename T>
inline ZvVFieldArray ZvVFields() {
  return ZvVFields_<ZvFieldList<T>>();
}

#endif /* ZvField_HPP */
