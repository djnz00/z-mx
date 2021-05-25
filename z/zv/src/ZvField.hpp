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
    NDP_	= 0x00020,	// NDP for printing float/fixed/decimal
    Series	= 0x00040,	// data series
      Index	= 0x00080,	// - index (e.g. time stamp)
      Delta	= 0x00100,	// - first derivative
      Delta2	= 0x00200	// - second derivative
  };
  enum {
    CtorShift	= 10,		// bit-shift for constructor parameter index
    CtorMask	= 0x3f		// 6 bits
  };
  enum {
    NDPShift	= 16,
    NDPMask	= 0x1f		// 0-31
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
}
#define ZvFieldMkFlags__(Flag) ZvFieldFlags::Flag |
#define ZvFieldMkFlags_(...) (ZuPP_Map(ZvFieldMkFlags__, __VA_ARGS__) 0)
#define ZvFieldMkFlags(Args) ZuPP_Defer(ZvFieldMkFlags_)(ZuPP_Strip(Args))

struct ZvTimeFmt_Null : public ZuPrintable {
  template <typename S> void print(S &) const { }
};
using ZvTimeFmt_FIX = ZtDateFmt::FIX<-9, ZvTimeFmt_Null>;
ZuDeclUnion(ZvTimeFmt,
    (ZtDateFmt::CSV, csv),
    (ZvTimeFmt_FIX, fix),
    (ZtDateFmt::ISO, iso));

struct ZvFieldFmt {
  ZuVFmt	scalar;			// scalar format (print only)
  ZvTimeFmt	time;			// date/time format
  char		flagsDelim = '|';	// flags delimiter

  ZvFieldFmt() { new (time.init_csv()) ZtDateFmt::CSV{}; }
};

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
      s << Map::print(v, fmt.flagsDelim);
    },
    .scan = [](uint64_t &v, ZuString s, const ZvFieldFmt &fmt) -> void {
      v = Map::template scan<uint64_t>(s, fmt.flagsDelim);
    }
  } { }

  static ZvVFieldFlags_ *instance() {
    return ZmSingleton<ZvVFieldFlags_>::instance();
  }
};

template <typename Base, unsigned Flags_>
struct ZvField_ : public Base {
  enum { Flags = Flags_ };
};

template <typename Base, unsigned Flags>
struct ZvField_RdString : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::String };
  template <typename S>
  static void print(const void *o, S &s, const ZvFieldFmt &) {
    s << Base::get(o);
  }
  static auto getFn() {
    return [](const void *o) -> ZuString { return Base::get(o); };
  }
  static auto setFn() { return [](void *, ZuString) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvField_String : public ZvField_RdString<Base, Flags> {
  static void scan(void *o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, s);
  }
  static auto setFn() {
    return [](void *o, ZuString s) { Base::set(o, s); };
  }
  static auto scanFn() { return scan; }
};

template <typename Base, unsigned Flags>
struct ZvField_RdComposite : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Composite };
  template <typename S>
  static void print(const void *o, S &s, const ZvFieldFmt &) {
    s << Base::get(o);
  }
  static auto getFn() {
    return [](const void *o) -> ZmStreamFn {
      return {o, [](const void *o, ZmStream &s) { s << Base::get(o); }};
    };
  }
  static auto setFn() { return [](void *, ZuString) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvField_Composite : public ZvField_RdComposite<Base, Flags> {
  static void scan(void *o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, s);
  }
  static auto setFn() {
    return [](void *o, ZuString s) { Base::set(o, s); };
  }
  static auto scanFn() { return scan; }
};

template <typename Base, unsigned Flags>
struct ZvField_RdBool : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Bool };
  template <typename S>
  static void print(const void *o, S &s, const ZvFieldFmt &) {
    s << (Base::get(o) ? '1' : '0');
  }
  static auto getFn() {
    return [](const void *o) -> int64_t { return Base::get(o); };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvField_Bool : public ZvField_RdBool<Base, Flags> {
  static void scan(void *o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, s.length() == 1 && s[0] == '1');
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(o, v); };
  }
  static auto scanFn() { return scan; }
};

template <
  typename Base, unsigned Flags,
  bool = ZuTraits<typename Base::T>::IsPrimitive>
struct ZvField_RdInt : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Int };
  template <typename S>
  static void print(const void *o, S &s, const ZvFieldFmt &fmt) {
    s << Base::get(o).vfmt(fmt.scalar);
  }
  static auto getFn() {
    return [](const void *o) -> int64_t { return Base::get(o); };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const ZvFieldFmt &) { }; }
};
template <
  typename Base, unsigned Flags,
  bool = ZuTraits<typename Base::T>::IsPrimitive>
struct ZvField_Int : public ZvField_RdInt<Base, Flags, false> {
  static void scan(void *o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, s);
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(o, v); };
  }
  static auto scanFn() { return scan; }
};
template <typename Base, unsigned Flags>
struct ZvField_RdInt<Base, Flags, true> : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Int };
  template <typename S>
  static void print(const void *o, S &s, const ZvFieldFmt &fmt) {
    s << ZuBoxed(Base::get(o)).vfmt(fmt.scalar);
  }
  static auto getFn() {
    return [](const void *o) -> int64_t { return Base::get(o); };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvField_Int<Base, Flags, true> : public ZvField_RdInt<Base, Flags, true> {
  static void scan(void *o, ZuString s, const ZvFieldFmt &) {
    using T = typename Base::T;
    Base::set(o, ZuBoxT<T>{s});
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(o, v); };
  }
  static auto scanFn() { return scan; }
};

template <typename Base, unsigned Flags>
struct ZvField_RdHex : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Hex };
  template <typename S>
  static void print(const void *o, S &s, const ZvFieldFmt &fmt) {
    s << ZuBoxed(Base::get(o)).vfmt(fmt.scalar).hex();
  }
  static auto getFn() {
    return [](const void *o) -> int64_t { return Base::get(o); };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvField_Hex : public ZvField_RdHex<Base, Flags> {
  static void scan(void *o, ZuString s, const ZvFieldFmt &) {
    using T = typename Base::T;
    Base::set(o, ZuBoxT<T>{ZuFmt::Hex<>{}, s});
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(o, v); };
  }
  static auto scanFn() { return scan; }
};

template <typename Base, unsigned Flags, typename Map_>
struct ZvField_RdEnum : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Enum };
  using Map = Map_;
  template <typename S>
  static void print(const void *o, S &s, const ZvFieldFmt &) {
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
  static auto scanFn() { return [](void *o, ZuString s, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags, typename Map>
struct ZvField_Enum : public ZvField_RdEnum<Base, Flags, Map> {
  static void scan(void *o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, Map::s2v(s));
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(o, v); };
  }
  static auto scanFn() { return scan; }
};

template <typename Base, unsigned Flags, typename Map_>
struct ZvField_RdFlags : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Flags };
  using Map = Map_;
  template <typename S>
  static void print(const void *o, S &s, const ZvFieldFmt &fmt) {
    s << Map::print(Base::get(o), fmt.flagsDelim);
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
  static auto scanFn() { return [](void *o, ZuString s, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags_, typename Map>
struct ZvField_Flags : public ZvField_RdFlags<Base, Flags_, Map> {
  static void scan(void *o, ZuString s, const ZvFieldFmt &fmt) {
    using T = typename Base::T;
    Base::set(o, Map::template scan<T>(s, fmt.flagsDelim));
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(o, v); };
  }
  static auto scanFn() { return scan; }
};

template <
  typename Base, unsigned Flags,
  typename T = typename Base::T,
  bool = ZuTraits<T>::IsPrimitive>
struct ZvField_RdFloat : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Float };
  template <typename S>
  static void print(const void *o, S &s, const ZvFieldFmt &fmt) {
    s << Base::get(o).vfmt(fmt.scalar);
  }
  static auto getFn() {
    return [](const void *o) -> double { return Base::get(o); };
  }
  static auto setFn() { return [](void *, double) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const ZvFieldFmt &) { }; }
};
template <
  typename Base, unsigned Flags,
  typename T = typename Base::T,
  bool = ZuTraits<T>::IsPrimitive>
struct ZvField_Float : public ZvField_RdFloat<Base, Flags, T, false> {
  static void scan(void *o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, s);
  }
  static auto setFn() {
    return [](void *o, double v) { Base::set(o, v); };
  }
  static auto scanFn() { return scan; }
};
template <typename Base, unsigned Flags, typename T>
struct ZvField_RdFloat<Base, Flags, T, true> :
    public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Float };
  template <typename S>
  static void print(const void *o, S &s, const ZvFieldFmt &fmt) {
    auto v = ZuBoxed(Base::get(o));
    if (Flags & ZvFieldFlags::NDP_)
      s << v.vfmt(fmt.scalar).fp(-ZvFieldFlags::getNDP(Flags));
    else
      s << v.vfmt(fmt.scalar);
  }
  static auto getFn() {
    return [](const void *o) -> double { return Base::get(o); };
  }
  static auto setFn() { return [](void *, double) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags, typename T>
struct ZvField_Float<Base, Flags, T, true> :
    public ZvField_RdFloat<Base, Flags, T, true> {
  static void scan(void *o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, ZuBoxT<T>{s});
  }
  static auto setFn() {
    return [](void *o, double v) { Base::set(o, v); };
  }
  static auto scanFn() { return scan; }
};
template <typename Base, unsigned Flags>
struct ZvField_RdFloat<Base, Flags, ZuFixed, false> :
    public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Fixed };
  template <typename S>
  static void print(const void *o, S &s, const ZvFieldFmt &fmt) {
    if (Flags & ZvFieldFlags::NDP_)
      s << Base::get(o).vfmt(fmt.scalar).fp(-ZvFieldFlags::getNDP(Flags));
    else
      s << Base::get(o).vfmt(fmt.scalar);
  }
  static auto getFn() {
    return [](const void *o) -> ZuFixed { return Base::get(o); };
  }
  static auto setFn() { return [](void *, ZuFixed) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvField_Float<Base, Flags, ZuFixed, false> :
    public ZvField_RdFloat<Base, Flags, ZuFixed, false> {
  static void scan(void *o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, ZuFixed{s, Base::get(o).exponent()});
  }
  static auto setFn() {
    return [](void *o, ZuFixed v) { Base::set(o, v); };
  }
  static auto scanFn() { return scan; }
};
template <typename Base, unsigned Flags>
struct ZvField_RdFloat<Base, Flags, ZuDecimal, false> :
    public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Decimal };
  template <typename S>
  static void print(const void *o, S &s, const ZvFieldFmt &fmt) {
    if (Flags & ZvFieldFlags::NDP_)
      s << Base::get(o).vfmt(fmt.scalar).fp(-ZvFieldFlags::getNDP(Flags));
    else
      s << Base::get(o).vfmt(fmt.scalar);
  }
  static auto getFn() {
    return [](const void *o) -> ZuDecimal { return Base::get(o); };
  }
  static auto setFn() { return [](void *, ZuDecimal) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvField_Float<Base, Flags, ZuDecimal, false> :
    public ZvField_RdFloat<Base, Flags, ZuDecimal, false> {
  static void scan(void *o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, s);
  }
  static auto setFn() {
    return [](void *o, ZuDecimal v) { Base::set(o, v); };
  }
  static auto scanFn() { return scan; }
};

template <typename Base, unsigned Flags>
struct ZvField_RdTime : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Time };
  template <typename S>
  static void print(const void *o, S &s, const ZvFieldFmt &fmt) {
    ZtDate v{Base::get(o)};
    switch (fmt.time.type()) {
      default:
      case ZvTimeFmt::Index<ZtDateFmt::CSV>::I:
	s << v.csv(fmt.time.csv());
	break;
      case ZvTimeFmt::Index<ZvTimeFmt_FIX>::I:
	s << v.fix(fmt.time.fix());
	break;
      case ZvTimeFmt::Index<ZtDateFmt::ISO>::I:
	s << v.iso(fmt.time.iso());
	break;
    }
  }
  static auto getFn() {
    return [](const void *o) -> ZmTime { return Base::get(o); };
  }
  static auto setFn() { return [](void *, ZmTime) { }; }
  static auto scanFn() { return [](void *o, ZuString s, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvField_Time : public ZvField_RdTime<Base, Flags> {
  static void scan(void *o, ZuString s, const ZvFieldFmt &fmt) {
    switch (fmt.time.type()) {
      default:
      case ZvTimeFmt::Index<ZtDateFmt::CSV>::I:
	Base::set(o, ZtDate{ZtDate::CSV, s});
	break;
      case ZvTimeFmt::Index<ZvTimeFmt_FIX>::I:
	Base::set(o, ZtDate{ZtDate::FIX, s});
	break;
      case ZvTimeFmt::Index<ZtDateFmt::ISO>::I:
	Base::set(o, ZtDate{s});
	break;
    }
  }
  static auto setFn() {
    return [](void *o, ZmTime v) { Base::set(o, v); };
  }
  static auto scanFn() { return scan; }
};

#define ZvFieldXRd(U, Type, ID, Member, Flags_, ...) \
  ZuFieldXRd(U, ID, Member) \
  using ZvFieldType(U, ID) = \
  ZvField_##Rd##Type<ZuFieldType(U, ID), \
    (ZvFieldMkFlags(Flags_) | ZvFieldFlags::ReadOnly) \
    __VA_OPT__(,) __VA_ARGS__>;
#define ZvFieldRd(U, Type, Member, Flags, ...) \
  ZvFieldXRd(U, Type, Member, Member, Flags __VA_OPT__(,) __VA_ARGS__)
#define ZvFieldX(U, Type, ID, Member, Flags, ...) \
  ZuFieldX(U, ID, Member) \
  using ZvFieldType(U, ID) = \
  ZvField_##Type<ZuFieldType(U, ID), ZvFieldMkFlags(Flags) \
    __VA_OPT__(,) __VA_ARGS__>;
#define ZvField(U, Type, Member, Flags, ...) \
  ZvFieldX(U, Type, Member, Member, Flags __VA_OPT__(,) __VA_ARGS__)

#define ZvFieldXRdFn(U, Type, ID, Get, Flags_, ...) \
  ZuFieldXRdFn(U, ID, Get) \
  using ZvFieldType(U, ID) = \
  ZvField_##Rd##Type<ZuFieldType(U, ID), \
    (ZvFieldMkFlags(Flags_) | ZvFieldFlags::ReadOnly) \
    __VA_OPT__(,) __VA_ARGS__>;
#define ZvFieldRdFn(U, Type, Fn, Flags, ...) \
  ZvFieldXRdFn(U, Type, Fn, Fn, Flags __VA_OPT__(,) __VA_ARGS__)
#define ZvFieldXFn(U, Type, ID, Get, Set, Flags, ...) \
  ZuFieldXFn(U, ID, Get, Set) \
  using ZvFieldType(U, ID) = \
  ZvField_##Type<ZuFieldType(U, ID), ZvFieldMkFlags(Flags) \
    __VA_OPT__(,) __VA_ARGS__>;
#define ZvFieldFn(U, Type, Fn, Flags, ...) \
  ZvFieldXFn(U, Type, Fn, Fn, Fn, Flags __VA_OPT__(,) __VA_ARGS__)

#define ZvFieldRdLambda(U, Type, ID, Get, Flags_, ...) \
  ZuFieldRdLambda(U, ID, Get) \
  using ZvFieldType(U, ID) = \
  ZvField_##Rd##Type<ZuFieldType(U, ID), \
    (ZvFieldMkFlags(Flags_) | ZvFieldFlags::ReadOnly) \
    __VA_OPT__(,) __VA_ARGS__>;
#define ZvFieldLambda(U, Type, ID, Get, Set, Flags, ...) \
  ZuFieldLambda(U, ID, Get, Set) \
  using ZvFieldType(U, ID) = \
  ZvField_##Type<ZuFieldType(U, ID), ZvFieldMkFlags(Flags) \
    __VA_OPT__(,) __VA_ARGS__>;

#define ZvField_Decl_(U, Method, ...) ZvField##Method(U, __VA_ARGS__)
#define ZvField_Decl(U, Args) ZuPP_Defer(ZvField_Decl_)(U, ZuPP_Strip(Args))

#define ZvField_Type_(U, Method, Type, ID, ...) ZvFieldType(U, ID)
#define ZvField_Type(U, Args) ZuPP_Defer(ZvField_Type_)(U, ZuPP_Strip(Args))

#define ZvFields(U, ...)  \
  namespace { \
    ZuPP_Eval(ZuPP_MapArg(ZvField_Decl, U, __VA_ARGS__)) \
    using ZvFields_##U = \
      ZuTypeList<ZuPP_Eval(ZuPP_MapArgComma(ZvField_Type, U, __VA_ARGS__))>; \
  } \
  U *ZuFielded_(U *); \
  ZvFields_##U ZuFieldList_(U *)

template <typename Impl> struct ZvFieldPrint : public ZuPrintable {
  const Impl *impl() const { return static_cast<const Impl *>(this); }
  Impl *impl() { return static_cast<Impl *>(this); }

  template <typename T>
  struct Print_Filter { enum { OK = !(T::Flags & ZvFieldFlags::ReadOnly) }; };
  template <typename S> void print(S &s) const {
    using FieldList = ZuTypeGrep<Print_Filter, ZuFieldList<Impl>>;
    thread_local ZvFieldFmt fmt;
    ZuTypeAll<FieldList>::invoke([o = impl(), &s]<typename Field>() {
      if constexpr (ZuTypeIndex<Field, FieldList>::I) s << ' ';
      s << Field::id() << '=';
      Field::print(o, s, fmt);
    });
  }
  friend ZuPrintFn ZuPrintType(Impl *);
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
  struct Filter { enum { OK = T::Flags & ZvFieldFlags::Primary }; };
  using FieldList = ZuTypeGrep<Filter, ZuFieldList<V>>;
  using Mk = typename ZvFieldKey__<V, FieldList>::Mk;
};
template <typename V>
auto ZvFieldKey(const V &v) {
  return ZvFieldKey_<V>::Mk::tuple(v);
}

// run-time introspection

struct ZvVField {
  const char	*id;
  uint32_t	type;		// ZvFieldType
  uint32_t	flags;		// ZvFieldFlags

  int		(*cmp)(const void *, const void *);

  union {
    void		*null;
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
      info{.null = nullptr},
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
      info{.null = nullptr},
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
      info{.null = nullptr},
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
      info{.null = nullptr},
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
      info{.null = nullptr},
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
      info{.null = nullptr},
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
      info{.null = nullptr},
      print{Field::template print<ZmStream>},
      scan{Field::scanFn()},
      getFn{.time = Field::getFn()},
      setFn{.time = Field::setFn()} { }

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
  return ZvVFields_<ZuFieldList<T>>();
}

#endif /* ZvField_HPP */
