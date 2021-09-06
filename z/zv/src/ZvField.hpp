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

#define ZvFieldType(O, ID) ZvField_##O##_##ID

// Metadata macro DSL for identifying and accessing data fields and keys:
//
// Syntax
// ------
// (((Accessor)[, (Keys...)]), (Type[, Args...])[, (Flags...)])
//
// (((id, Rd), (0)), (String), (Ctor(0)))
//
// ZvField Type	C++ Type
// ------------	--------
// String	<String>
// Bool		<Integral>
// Int		<Integral>
// Hex		<Integral>
// Enum, Map	<Integral>
// Flags, Map	<Integral>
// Float	<FloatingPoint>
// Fixed	ZuFixed
// Decimal	ZuDecimal
// Time		ZmTime
// Composite	<Composite>

namespace ZvFieldType {
  enum _ {
    String = 0,		// a contiguous UTF-8 string
    Composite,		// composite type
    Bool,		// an integral type, interpreted as bool
    Int,		// an integral type <= 64bits
    Hex,		// an integral type printed in hex
    Enum,		// an integral enumerated type
    Flags,		// an integral enumerated bitfield type
    Float,		// floating point type
    Fixed,		// ZuFixed
    Decimal,		// ZuDecimal
    Time,		// ZmTime
    N
  };
}

namespace ZvFieldFlags {
  enum {
    Synthetic	= 0x00001,	// synthetic and read-only
    Update	= 0x00002,	// include in updates
    Ctor_	= 0x00004,	// constructor parameter
    NDP_	= 0x00008,	// NDP for printing float/fixed/decimal
    Series	= 0x00010,	// data series
      Index	= 0x00020,	// - index (e.g. time stamp)
      Delta	= 0x00040,	// - first derivative
      Delta2	= 0x00080	// - second derivative
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
#define ZvField_Flags__(Flag) ZvFieldFlags::Flag |
#define ZvField_Flags_(...) (ZuPP_Map(ZvField_Flags__, __VA_ARGS__) 0)
#define ZvField_Flags(Args) ZuPP_Defer(ZvField_Flags_)(ZuPP_Strip(Args))

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
  using O = typename Base::O;
  static auto cmpFn() {
    return [](const void *o1, const void *o2) {
      return ZuCmp<typename Base::T>::cmp(
	  Base::get(*static_cast<const O *>(o1)),
	  Base::get(*static_cast<const O *>(o2)));
    };
  }
};

template <typename Base, unsigned Flags, bool = Base::ReadOnly>
struct ZvFieldType_String : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::String };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZvFieldFmt &) {
    s << Base::get(ZuFwd<P>(o));
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZvFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> ZuString {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() {
    return [](void *, ZuString) { };
  }
  static auto scanFn() {
    return [](void *, ZuString, const ZvFieldFmt &) { };
  }
};
template <typename Base, unsigned Flags>
struct ZvFieldType_String<Base, Flags, false> :
    public ZvFieldType_String<Base, Flags, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZvFieldFmt &) { Base::set(o, s); }
  static auto setFn() {
    return [](void *o, ZuString s) {
      Base::set(*static_cast<O *>(o), s);
    };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZvFieldFmt &) {
      Base::set(*static_cast<O *>(o), s);
    };
  }
};

template <typename Base, unsigned Flags, bool = Base::ReadOnly>
struct ZvFieldType_Composite : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Composite };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZvFieldFmt &) {
    s << Base::get(ZuFwd<P>(o));
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZvFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> ZmStreamFn {
      return {
	static_cast<const O *>(o),
	[](const O *o, ZmStream &s) { s << Base::get(*o); }};
    };
  }
  static auto setFn() { return [](void *, ZuString) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvFieldType_Composite<Base, Flags, false> :
    public ZvFieldType_Composite<Base, Flags, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZvFieldFmt &) { Base::set(o, s); }
  static auto setFn() {
    return [](void *o, ZuString s) {
      Base::set(*static_cast<O *>(o), s);
    };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZvFieldFmt &) {
      Base::set(*static_cast<O *>(o), s);
    };
  }
};

template <typename Base, unsigned Flags, bool = Base::ReadOnly>
struct ZvFieldType_Bool : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Bool };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZvFieldFmt &) {
    s << (Base::get(o) ? '1' : '0');
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZvFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> int64_t {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvFieldType_Bool<Base, Flags, false> :
    public ZvFieldType_Bool<Base, Flags, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, s.length() == 1 && s[0] == '1');
  }
  static auto setFn() {
    return [](void *o, int64_t v) {
      Base::set(*static_cast<O *>(o), v);
    };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZvFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

template <
  typename Base, unsigned Flags,
  bool = Base::ReadOnly,
  bool = ZuTraits<typename Base::T>::IsPrimitive>
struct ZvFieldType_Int : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Int };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZvFieldFmt &fmt) {
    s << ZuBoxed(Base::get(ZuFwd<P>(o))).vfmt(fmt.scalar);
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZvFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> int64_t {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvFieldType_Int<Base, Flags, false, true> :
    public ZvFieldType_Int<Base, Flags, true, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, ZuBoxT<typename Base::T>{s});
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZvFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};
template <typename Base, unsigned Flags>
struct ZvFieldType_Int<Base, Flags, true, false> :
    public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Int };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZvFieldFmt &fmt) {
    s << ZuBoxed(Base::get(ZuFwd<P>(o))).vfmt(fmt.scalar);
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZvFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> int64_t {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvFieldType_Int<Base, Flags, false, false> :
    public ZvFieldType_Int<Base, Flags, true, false> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, ZuBoxT<typename Base::T>{s});
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZvFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

template <typename Base, unsigned Flags, bool = Base::ReadOnly>
struct ZvFieldType_Hex : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Hex };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZvFieldFmt &fmt) {
    s << ZuBoxed(Base::get(ZuFwd<P>(o))).vfmt(fmt.scalar).hex();
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZvFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> int64_t {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvFieldType_Hex<Base, Flags, false> :
    public ZvFieldType_Hex<Base, Flags, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, ZuBoxT<typename Base::T>{ZuFmt::Hex<>{}, s});
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZvFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

template <typename Base, unsigned Flags, typename Map_, bool = Base::ReadOnly>
struct ZvFieldType_Enum : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Enum };
  using O = typename Base::O;
  using Map = Map_;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZvFieldFmt &) {
    s << Map::v2s(Base::get(ZuFwd<P>(o)));
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZvFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto enumFn() {
    return []() -> ZvVFieldEnum * {
      return ZvVFieldEnum_<Map>::instance();
    };
  }
  static auto getFn() {
    return [](const void *o) -> int64_t {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags, typename Map>
struct ZvFieldType_Enum<Base, Flags, Map, false> :
    public ZvFieldType_Enum<Base, Flags, Map, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, Map::s2v(s));
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZvFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

template <typename Base, unsigned Flags, typename Map_, bool = Base::ReadOnly>
struct ZvFieldType_Flags : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Flags };
  using O = typename Base::O;
  using Map = Map_;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZvFieldFmt &fmt) {
    s << Map::print(Base::get(ZuFwd<P>(o)), fmt.flagsDelim);
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZvFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto flagsFn() {
    return []() -> ZvVFieldFlags * {
      return ZvVFieldFlags_<Map>::instance();
    };
  }
  static auto getFn() {
    return [](const void *o) -> int64_t {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags, typename Map>
struct ZvFieldType_Flags<Base, Flags, Map, false> :
    public ZvFieldType_Flags<Base, Flags, Map, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZvFieldFmt &fmt) {
    Base::set(o, Map::template scan<typename Base::T>(s, fmt.flagsDelim));
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZvFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

template <
  typename Base, unsigned Flags, bool = Base::ReadOnly,
  bool = ZuTraits<typename Base::T>::IsPrimitive>
struct ZvFieldType_Float : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Float };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZvFieldFmt &fmt) {
    s << Base::get(ZuFwd<P>(o)).vfmt(fmt.scalar);
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZvFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> double {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, double) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvFieldType_Float<Base, Flags, false, false> :
    public ZvFieldType_Float<Base, Flags, true, false> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, s);
  }
  static auto setFn() {
    return [](void *o, double v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZvFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};
template <typename Base, unsigned Flags>
struct ZvFieldType_Float<Base, Flags, true, true> :
    public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Float };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZvFieldFmt &fmt) {
    auto v = ZuBoxed(Base::get(ZuFwd<P>(o)));
    if (Flags & ZvFieldFlags::NDP_)
      s << v.vfmt(fmt.scalar).fp(-ZvFieldFlags::getNDP(Flags));
    else
      s << v.vfmt(fmt.scalar);
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZvFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> double {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, double) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvFieldType_Float<Base, Flags, false, true> :
    public ZvFieldType_Float<Base, Flags, true, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, ZuBoxT<typename Base::T>{s});
  }
  static auto setFn() {
    return [](void *o, double v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZvFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

template <typename Base, unsigned Flags, bool = Base::ReadOnly>
struct ZvFieldType_Fixed : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Fixed };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZvFieldFmt &fmt) {
    if (Flags & ZvFieldFlags::NDP_)
      s << Base::get(ZuFwd<P>(o)).vfmt(fmt.scalar).fp(
	  -ZvFieldFlags::getNDP(Flags));
    else
      s << Base::get(ZuFwd<P>(o)).vfmt(fmt.scalar);
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZvFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> ZuFixed {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, ZuFixed) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvFieldType_Fixed<Base, Flags, false> :
    public ZvFieldType_Fixed<Base, Flags, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, ZuFixed{s, Base::get(ZuFwd<P>(o)).exponent()});
  }
  static auto setFn() {
    return [](void *o, ZuFixed v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZvFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

template <typename Base, unsigned Flags, bool = Base::ReadOnly>
struct ZvFieldType_Decimal : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Decimal };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZvFieldFmt &fmt) {
    if (Flags & ZvFieldFlags::NDP_)
      s << Base::get(ZuFwd<P>(o)).vfmt(fmt.scalar).fp(
	  -ZvFieldFlags::getNDP(Flags));
    else
      s << Base::get(ZuFwd<P>(o)).vfmt(fmt.scalar);
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZvFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> ZuDecimal {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, ZuDecimal) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvFieldType_Decimal<Base, Flags, false> :
    public ZvFieldType_Decimal<Base, Flags, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZvFieldFmt &) {
    Base::set(o, s);
  }
  static auto setFn() {
    return [](void *o, ZuDecimal v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZvFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

template <typename Base, unsigned Flags, bool = Base::ReadOnly>
struct ZvFieldType_Time : public ZvField_<Base, Flags> {
  enum { Type = ZvFieldType::Time };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZvFieldFmt &fmt) {
    ZtDate v{Base::get(ZuFwd<P>(o))};
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
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZvFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> ZmTime {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, ZmTime) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZvFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZvFieldType_Time<Base, Flags, false> :
    public ZvFieldType_Time<Base, Flags, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZvFieldFmt &fmt) {
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
    return [](void *o, ZmTime v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZvFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

#define ZvField_BaseID__(ID, ...) ID
#define ZvField_BaseID_(Axor, ...) ZuPP_Defer(ZvField_BaseID__)Axor
#define ZvField_BaseID(Base) ZuPP_Defer(ZvField_BaseID_)Base

#define ZvField_TypeName_(Name, ...) Name
#define ZvField_TypeName(Type) ZuPP_Defer(ZvField_TypeName_)Type
#define ZvField_TypeArgs_(Name, ...) __VA_OPT__(,) __VA_ARGS__
#define ZvField_TypeArgs(Type) ZuPP_Defer(ZvField_TypeArgs_)Type

#define ZvField_Decl_4(O, ID, Base, TypeName, Type) \
  ZuField_Decl(O, Base) \
  using ZvFieldType(O, ID) = \
  ZvFieldType_##TypeName<ZuFieldType(O, ID), \
      0 ZvField_TypeArgs(Type)>;
#define ZvField_Decl_5(O, ID, Base, TypeName, Type, Flags) \
  ZuField_Decl(O, Base) \
  using ZvFieldType(O, ID) = \
  ZvFieldType_##TypeName<ZuFieldType(O, ID), \
      ZvField_Flags(Flags) ZvField_TypeArgs(Type)>;
#define ZvField_Decl_N(O, _0, _1, _2, _3, _4, Fn, ...) Fn
#define ZvField_Decl__(O, ...) \
  ZvField_Decl_N(O, __VA_ARGS__, \
      ZvField_Decl_5(O, __VA_ARGS__), \
      ZvField_Decl_4(O, __VA_ARGS__))
#define ZvField_Decl_(O, Base, Type, ...) \
  ZuPP_Defer(ZvField_Decl__)(O, \
      ZuPP_Nest(ZvField_BaseID(Base)), Base, \
      ZuPP_Nest(ZvField_TypeName(Type)), Type __VA_OPT__(,) __VA_ARGS__)
#define ZvField_Decl(O, Args) ZuPP_Defer(ZvField_Decl_)(O, ZuPP_Strip(Args))

#define ZvField_Type_(O, Base, ...) \
  ZuPP_Defer(ZvFieldType)(O, ZuPP_Nest(ZvField_BaseID(Base)))
#define ZvField_Type(O, Args) ZuPP_Defer(ZvField_Type_)(O, ZuPP_Strip(Args))

#define ZvFields(O, ...)  \
  namespace ZuFields_ { \
    ZuPP_Eval(ZuPP_MapArg(ZvField_Decl, O, __VA_ARGS__)) \
    using O = \
      ZuTypeList<ZuPP_Eval(ZuPP_MapArgComma(ZvField_Type, O, __VA_ARGS__))>; \
  } \
  O *ZuFielded_(O *); \
  ZuFields_::O ZuFieldList_(O *)

template <typename Impl> struct ZvFieldPrint : public ZuPrintable {
  const Impl *impl() const { return static_cast<const Impl *>(this); }
  Impl *impl() { return static_cast<Impl *>(this); }

  template <typename U>
  struct Print_Filter { enum { OK = !U::ReadOnly }; };
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
        ZuIfT<Field::Type == ZvFieldType::String> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.string = Field::getFn()},
      setFn{.string = Field::setFn()} { }

  template <typename Field>
  ZvVField(Field,
        ZuIfT<Field::Type == ZvFieldType::Composite> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.composite = Field::getFn()},
      setFn{.string = Field::setFn()} { }

  template <typename Field>
  ZvVField(Field,
        ZuIfT<
	  Field::Type == ZvFieldType::Bool ||
	  Field::Type == ZvFieldType::Int ||
	  Field::Type == ZvFieldType::Hex> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.int_ = Field::getFn()},
      setFn{.int_ = Field::setFn()} { }

  template <typename Field>
  ZvVField(Field,
        ZuIfT<Field::Type == ZvFieldType::Enum> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.enum_ = Field::enumFn()},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.int_ = Field::getFn()},
      setFn{.int_ = Field::setFn()} { }

  template <typename Field>
  ZvVField(Field,
        ZuIfT<Field::Type == ZvFieldType::Flags> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.flags = Field::flagsFn()},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.int_ = Field::getFn()},
      setFn{.int_ = Field::setFn()} { }

  template <typename Field>
  ZvVField(Field,
        ZuIfT<Field::Type == ZvFieldType::Float> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.float_ = Field::getFn()},
      setFn{.float_ = Field::setFn()} { }

  template <typename Field>
  ZvVField(Field,
        ZuIfT<Field::Type == ZvFieldType::Fixed> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.fixed = Field::getFn()},
      setFn{.fixed = Field::setFn()} { }

  template <typename Field>
  ZvVField(Field,
        ZuIfT<Field::Type == ZvFieldType::Decimal> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.decimal = Field::getFn()},
      setFn{.decimal = Field::setFn()} { }

  template <typename Field>
  ZvVField(Field,
        ZuIfT<Field::Type == ZvFieldType::Time> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      print{Field::printFn()},
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
template <typename O>
inline ZvVFieldArray ZvVFields() {
  return ZvVFields_<ZuFieldList<O>>();
}

#endif /* ZvField_HPP */
