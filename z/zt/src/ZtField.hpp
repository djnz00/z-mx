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

#ifndef ZtField_HPP
#define ZtField_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZtLib_HPP
#include <zlib/ZtLib.hpp>
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

#define ZtFieldType(O, ID) ZtField_##O##_##ID

// Metadata macro DSL for identifying and accessing data fields and keys:
//
// Syntax
// ------
// (((Accessor)[, (Keys...)]), (Type[, Args...])[, (Flags...)])
//
// Example: (((id, Rd), (0)), (String), (Ctor(0)))
//
// ZtField Type	C++ Type
// ------------	--------
// String	<String>
// Composite	<Composite>
// Bool		<Integral>
// Int		<Integral>
// Hex		<Integral>
// Enum, Map	<Integral>
// Flags, Map	<Integral>
// Float	<FloatingPoint>
// Fixed	ZuFixed
// Decimal	ZuDecimal
// Time		ZmTime

namespace ZtFieldType {
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

namespace ZtFieldFlags {
  enum {
    Synthetic	= 0x00001,	// synthetic and read-only
    Update	= 0x00002,	// include in updates
    DoNotPrint	= 0x00004,	// do not print
    Ctor_	= 0x00008,	// constructor parameter
    NDP_	= 0x00010,	// NDP for printing float/fixed/decimal
    Series	= 0x00020,	// data series
      Index	= 0x00040,	// - index (e.g. time stamp)
      Delta	= 0x00080,	// - first derivative
      Delta2	= 0x00100	// - second derivative
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
#define ZtField_Flags__(Flag) ZtFieldFlags::Flag |
#define ZtField_Flags_(...) (ZuPP_Map(ZtField_Flags__, __VA_ARGS__) 0)
#define ZtField_Flags(Args) ZuPP_Defer(ZtField_Flags_)(ZuPP_Strip(Args))

struct ZtFieldFmt {
  ZuVFmt		scalar;			// scalar format (print only)
  ZtDateScan::Any	dateScan;		// date/time scan format
  ZtDateFmt::Any	datePrint;		// date/time print format
  char			flagsDelim = '|';	// flags delimiter
};

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

  static ZtVFieldEnum_ *instance() {
    return ZmSingleton<ZtVFieldEnum_>::instance();
  }
};

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

  static ZtVFieldFlags_ *instance() {
    return ZmSingleton<ZtVFieldFlags_>::instance();
  }
};

template <typename Base, unsigned Flags_>
struct ZtField_ : public Base {
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
struct ZtFieldType_String : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::String };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZtFieldFmt &) {
    s << '"' << Base::get(ZuFwd<P>(o)) << '"';
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
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
    return [](void *, ZuString, const ZtFieldFmt &) { };
  }
};
template <typename Base, unsigned Flags>
struct ZtFieldType_String<Base, Flags, false> :
    public ZtFieldType_String<Base, Flags, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &) { Base::set(o, s); }
  static auto setFn() {
    return [](void *o, ZuString s) {
      Base::set(*static_cast<O *>(o), s);
    };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZtFieldFmt &) {
      Base::set(*static_cast<O *>(o), s);
    };
  }
};

template <typename Base, unsigned Flags, bool = Base::ReadOnly>
struct ZtFieldType_Composite : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Composite };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZtFieldFmt &) {
    s << '{' << Base::get(ZuFwd<P>(o)) << '}';
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
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
  static auto scanFn() { return [](void *, ZuString, const ZtFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZtFieldType_Composite<Base, Flags, false> :
    public ZtFieldType_Composite<Base, Flags, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &) { Base::set(o, s); }
  static auto setFn() {
    return [](void *o, ZuString s) {
      Base::set(*static_cast<O *>(o), s);
    };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZtFieldFmt &) {
      Base::set(*static_cast<O *>(o), s);
    };
  }
};

template <typename Base, unsigned Flags, bool = Base::ReadOnly>
struct ZtFieldType_Bool : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Bool };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZtFieldFmt &) {
    s << (Base::get(ZuFwd<P>(o)) ? '1' : '0');
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> int64_t {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZtFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZtFieldType_Bool<Base, Flags, false> :
    public ZtFieldType_Bool<Base, Flags, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &) {
    Base::set(o, s.length() == 1 && s[0] == '1');
  }
  static auto setFn() {
    return [](void *o, int64_t v) {
      Base::set(*static_cast<O *>(o), v);
    };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZtFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

template <
  typename Base, unsigned Flags,
  bool = Base::ReadOnly,
  bool = ZuTraits<typename Base::T>::IsPrimitive>
struct ZtFieldType_Int : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Int };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZtFieldFmt &fmt) {
    s << ZuBoxed(Base::get(ZuFwd<P>(o))).vfmt(fmt.scalar);
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> int64_t {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZtFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZtFieldType_Int<Base, Flags, false, true> :
    public ZtFieldType_Int<Base, Flags, true, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &) {
    Base::set(o, ZuBoxT<typename Base::T>{s});
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZtFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};
template <typename Base, unsigned Flags>
struct ZtFieldType_Int<Base, Flags, true, false> :
    public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Int };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZtFieldFmt &fmt) {
    s << ZuBoxed(Base::get(ZuFwd<P>(o))).vfmt(fmt.scalar);
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> int64_t {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZtFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZtFieldType_Int<Base, Flags, false, false> :
    public ZtFieldType_Int<Base, Flags, true, false> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &) {
    Base::set(o, ZuBoxT<typename Base::T>{s});
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZtFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

template <typename Base, unsigned Flags, bool = Base::ReadOnly>
struct ZtFieldType_Hex : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Hex };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZtFieldFmt &fmt) {
    s << ZuBoxed(Base::get(ZuFwd<P>(o))).vfmt(fmt.scalar).hex();
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> int64_t {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZtFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZtFieldType_Hex<Base, Flags, false> :
    public ZtFieldType_Hex<Base, Flags, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &) {
    Base::set(o, ZuBoxT<typename Base::T>{ZuFmt::Hex<>{}, s});
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZtFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

template <typename Base, unsigned Flags, typename Map_, bool = Base::ReadOnly>
struct ZtFieldType_Enum : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Enum };
  using O = typename Base::O;
  using Map = Map_;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZtFieldFmt &) {
    s << Map::v2s(Base::get(ZuFwd<P>(o)));
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto enumFn() {
    return []() -> ZtVFieldEnum * {
      return ZtVFieldEnum_<Map>::instance();
    };
  }
  static auto getFn() {
    return [](const void *o) -> int64_t {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZtFieldFmt &) { }; }
};
template <typename Base, unsigned Flags, typename Map>
struct ZtFieldType_Enum<Base, Flags, Map, false> :
    public ZtFieldType_Enum<Base, Flags, Map, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &) {
    Base::set(o, Map::s2v(s));
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZtFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

template <typename Base, unsigned Flags, typename Map_, bool = Base::ReadOnly>
struct ZtFieldType_Flags : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Flags };
  using O = typename Base::O;
  using Map = Map_;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZtFieldFmt &fmt) {
    s << Map::print(Base::get(ZuFwd<P>(o)), fmt.flagsDelim);
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto flagsFn() {
    return []() -> ZtVFieldFlags * {
      return ZtVFieldFlags_<Map>::instance();
    };
  }
  static auto getFn() {
    return [](const void *o) -> int64_t {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, int64_t) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZtFieldFmt &) { }; }
};
template <typename Base, unsigned Flags, typename Map>
struct ZtFieldType_Flags<Base, Flags, Map, false> :
    public ZtFieldType_Flags<Base, Flags, Map, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &fmt) {
    Base::set(o, Map::template scan<typename Base::T>(s, fmt.flagsDelim));
  }
  static auto setFn() {
    return [](void *o, int64_t v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZtFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

template <
  typename Base, unsigned Flags, bool = Base::ReadOnly,
  bool = ZuTraits<typename Base::T>::IsPrimitive>
struct ZtFieldType_Float : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Float };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZtFieldFmt &fmt) {
    s << Base::get(ZuFwd<P>(o)).vfmt(fmt.scalar);
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> double {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, double) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZtFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZtFieldType_Float<Base, Flags, false, false> :
    public ZtFieldType_Float<Base, Flags, true, false> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &) {
    Base::set(o, s);
  }
  static auto setFn() {
    return [](void *o, double v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZtFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};
template <typename Base, unsigned Flags>
struct ZtFieldType_Float<Base, Flags, true, true> :
    public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Float };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZtFieldFmt &fmt) {
    auto v = ZuBoxed(Base::get(ZuFwd<P>(o)));
    if (Flags & ZtFieldFlags::NDP_)
      s << v.vfmt(fmt.scalar).fp(-ZtFieldFlags::getNDP(Flags));
    else
      s << v.vfmt(fmt.scalar);
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> double {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, double) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZtFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZtFieldType_Float<Base, Flags, false, true> :
    public ZtFieldType_Float<Base, Flags, true, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &) {
    Base::set(o, ZuBoxT<typename Base::T>{s});
  }
  static auto setFn() {
    return [](void *o, double v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZtFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

template <typename Base, unsigned Flags, bool = Base::ReadOnly>
struct ZtFieldType_Fixed : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Fixed };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZtFieldFmt &fmt) {
    if (Flags & ZtFieldFlags::NDP_)
      s << Base::get(ZuFwd<P>(o)).vfmt(fmt.scalar).fp(
	  -ZtFieldFlags::getNDP(Flags));
    else
      s << Base::get(ZuFwd<P>(o)).vfmt(fmt.scalar);
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> ZuFixed {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, ZuFixed) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZtFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZtFieldType_Fixed<Base, Flags, false> :
    public ZtFieldType_Fixed<Base, Flags, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &) {
    Base::set(o, ZuFixed{s, Base::get(ZuFwd<P>(o)).exponent()});
  }
  static auto setFn() {
    return [](void *o, ZuFixed v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZtFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

template <typename Base, unsigned Flags, bool = Base::ReadOnly>
struct ZtFieldType_Decimal : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Decimal };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZtFieldFmt &fmt) {
    if (Flags & ZtFieldFlags::NDP_)
      s << Base::get(ZuFwd<P>(o)).vfmt(fmt.scalar).fp(
	  -ZtFieldFlags::getNDP(Flags));
    else
      s << Base::get(ZuFwd<P>(o)).vfmt(fmt.scalar);
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> ZuDecimal {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, ZuDecimal) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZtFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZtFieldType_Decimal<Base, Flags, false> :
    public ZtFieldType_Decimal<Base, Flags, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &) {
    Base::set(o, s);
  }
  static auto setFn() {
    return [](void *o, ZuDecimal v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZtFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

template <typename Base, unsigned Flags, bool = Base::ReadOnly>
struct ZtFieldType_Time : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Time };
  using O = typename Base::O;
  template <typename P, typename S>
  static void print(P &&o, S &s, const ZtFieldFmt &fmt) {
    ZtDate v{Base::get(ZuFwd<P>(o))};
    s << v.print(fmt.datePrint);
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      print(*static_cast<const O *>(o), s, fmt);
    };
  }
  static auto getFn() {
    return [](const void *o) -> ZmTime {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() { return [](void *, ZmTime) { }; }
  static auto scanFn() { return [](void *, ZuString, const ZtFieldFmt &) { }; }
};
template <typename Base, unsigned Flags>
struct ZtFieldType_Time<Base, Flags, false> :
    public ZtFieldType_Time<Base, Flags, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &fmt) {
    Base::set(o, ZtDate{fmt.dateScan, s});
  }
  static auto setFn() {
    return [](void *o, ZmTime v) { Base::set(*static_cast<O *>(o), v); };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZtFieldFmt &fmt) {
      scan(*static_cast<O *>(o), s, fmt);
    };
  }
};

#define ZtField_BaseID__(ID, ...) ID
#define ZtField_BaseID_(Axor, ...) ZuPP_Defer(ZtField_BaseID__)Axor
#define ZtField_BaseID(Base) ZuPP_Defer(ZtField_BaseID_)Base

#define ZtField_TypeName_(Name, ...) Name
#define ZtField_TypeName(Type) ZuPP_Defer(ZtField_TypeName_)Type
#define ZtField_TypeArgs_(Name, ...) __VA_OPT__(,) __VA_ARGS__
#define ZtField_TypeArgs(Type) ZuPP_Defer(ZtField_TypeArgs_)Type

#define ZtField_Decl_4(O, ID, Base, TypeName, Type) \
  ZuField_Decl(O, Base) \
  using ZtFieldType(O, ID) = \
  ZtFieldType_##TypeName<ZuFieldType(O, ID), \
      0 ZtField_TypeArgs(Type)>;
#define ZtField_Decl_5(O, ID, Base, TypeName, Type, Flags) \
  ZuField_Decl(O, Base) \
  using ZtFieldType(O, ID) = \
  ZtFieldType_##TypeName<ZuFieldType(O, ID), \
      ZtField_Flags(Flags) ZtField_TypeArgs(Type)>;
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
  ZuPP_Defer(ZtFieldType)(O, ZuPP_Nest(ZtField_BaseID(Base)))
#define ZtField_Type(O, Args) ZuPP_Defer(ZtField_Type_)(O, ZuPP_Strip(Args))

#define ZtFields(O, ...)  \
  namespace ZuFields_ { \
    ZuPP_Eval(ZuPP_MapArg(ZtField_Decl, O, __VA_ARGS__)) \
    using O = \
      ZuTypeList<ZuPP_Eval(ZuPP_MapArgComma(ZtField_Type, O, __VA_ARGS__))>; \
  } \
  O *ZuFielded_(O *); \
  ZuFields_::O ZuFieldList_(O *)

struct ZtFieldPrint {
  enum { OK = 1, String = 0, Delegate = 1, Buffer = 0 };
  template <typename U>
  struct Print_Filter { enum { OK = !(U::Flags & ZtFieldFlags::DoNotPrint) }; };
  template <typename S, typename O>
  static void print(S &s, const O &o) {
    using FieldList = ZuTypeGrep<Print_Filter, ZuFieldList<O>>;
    thread_local ZtFieldFmt fmt;
    ZuTypeAll<FieldList>::invoke([&o, &s]<typename Field>() {
      if constexpr (ZuTypeIndex<Field, FieldList>::I) s << ' ';
      s << Field::id() << '=';
      Field::print(o, s, fmt);
    });
  }
};

// run-time introspection

struct ZtVField {
  const char	*id;
  uint32_t	type;		// ZtFieldType
  uint32_t	flags;		// ZtFieldFlags

  int		(*cmp)(const void *, const void *);

  union {
    void		*null;
    ZtVFieldEnum	*(*enum_)();		// Enum
    ZtVFieldFlags	*(*flags)();		// Flags
  } info;

  void		(*print)(const void *, ZmStream &, const ZtFieldFmt &);
  void		(*scan)(void *, ZuString, const ZtFieldFmt &);
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
  ZtVField(Field,
        ZuIfT<Field::Type == ZtFieldType::String> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.string = Field::getFn()},
      setFn{.string = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field,
        ZuIfT<Field::Type == ZtFieldType::Composite> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.composite = Field::getFn()},
      setFn{.string = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field,
        ZuIfT<
	  Field::Type == ZtFieldType::Bool ||
	  Field::Type == ZtFieldType::Int ||
	  Field::Type == ZtFieldType::Hex> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.int_ = Field::getFn()},
      setFn{.int_ = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field,
        ZuIfT<Field::Type == ZtFieldType::Enum> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.enum_ = Field::enumFn()},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.int_ = Field::getFn()},
      setFn{.int_ = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field,
        ZuIfT<Field::Type == ZtFieldType::Flags> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.flags = Field::flagsFn()},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.int_ = Field::getFn()},
      setFn{.int_ = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field,
        ZuIfT<Field::Type == ZtFieldType::Float> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.float_ = Field::getFn()},
      setFn{.float_ = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field,
        ZuIfT<Field::Type == ZtFieldType::Fixed> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.fixed = Field::getFn()},
      setFn{.fixed = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field,
        ZuIfT<Field::Type == ZtFieldType::Decimal> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.decimal = Field::getFn()},
      setFn{.decimal = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field,
        ZuIfT<Field::Type == ZtFieldType::Time> *_ = nullptr) :
      id{Field::id()}, type{Field::Type}, flags{Field::Flags},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.time = Field::getFn()},
      setFn{.time = Field::setFn()} { }

  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldType::String> get_(const void *o, L l) {
    l(getFn.string(o));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldType::Composite> get_(const void *o, L l) {
    l(getFn.composite(o));
  }
  template <unsigned I, typename L>
  ZuIfT<
    I == ZtFieldType::Bool || I == ZtFieldType::Int ||
    I == ZtFieldType::Fixed || I == ZtFieldType::Hex ||
    I == ZtFieldType::Enum || I == ZtFieldType::Flags> get_(
	const void *o, L l) {
    l(getFn.int_(o));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldType::Float> get_(const void *o, L l) {
    l(getFn.float_(o));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldType::Decimal> get_(const void *o, L l) {
    l(getFn.decimal(o));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldType::Time> get_(const void *o, L l) {
    l(getFn.time(o));
  }
  template <typename L> void get(const void *o, L l) {
    ZuSwitch::dispatch<ZtFieldType::N>(type,
	[this, o, l = ZuMv(l)](auto i) mutable { get_<i>(o, ZuMv(l)); });
  }

  template <unsigned I, typename L>
  ZuIfT<
    I == ZtFieldType::String || I == ZtFieldType::Composite> set_(
	void *o, L l) {
    setFn.string(o, l.template operator ()<ZuString>());
  }
  template <unsigned I, typename L>
  ZuIfT<
    I == ZtFieldType::Bool || I == ZtFieldType::Int ||
    I == ZtFieldType::Fixed || I == ZtFieldType::Hex ||
    I == ZtFieldType::Enum || I == ZtFieldType::Flags> set_(
	void *o, L l) {
    setFn.int_(o, l.template operator ()<int64_t>());
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldType::Float> set_(void *o, L l) {
    setFn.float_(o, l.template operator ()<double>());
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldType::Decimal> set_(void *o, L l) {
    setFn.decimal(o, l.template operator ()<ZuDecimal>());
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldType::Time> set_(void *o, L l) {
    setFn.time(o, l.template operator ()<ZmTime>());
  }
  template <typename L>
  void set(void *o, L l) {
    ZuSwitch::dispatch<ZtFieldType::N>(type,
	[this, o, l = ZuMv(l)](auto i) mutable { set_<i>(o, ZuMv(l)); });
  }
};

using ZtVFieldArray = ZuArray<const ZtVField>;

template <typename ...Fields>
struct ZtVFields__ {
  static ZtVFieldArray fields() {
    static const ZtVField fields_[] =
      // std::initializer_list<ZtVField>
    {
      ZtVField{Fields{}}...
    };
    return {&fields_[0], sizeof(fields_) / sizeof(fields_[0])};
  }
};
template <typename FieldList>
inline ZtVFieldArray ZtVFields_() {
  return ZuTypeApply<ZtVFields__, FieldList>::fields();
}
template <typename O>
inline ZtVFieldArray ZtVFields() {
  return ZtVFields_<ZuFieldList<O>>();
}

#endif /* ZtField_HPP */
