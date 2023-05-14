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
#include <zlib/ZmSingleton.hpp>

#include <zlib/ZtEnum.hpp>
#include <zlib/ZtDate.hpp>

#define ZtFieldType_(O, ID) ZtField_##O##_##ID

// Metadata macro DSL for identifying and accessing data fields and keys:
//
// ZtFields(Type, Fields...)
//
// a Field is of the form:
// (((Accessor)[, (Keys...)]), (Type[, Args...])[, (Flags...)])
//
// Example: (((id, Rd), (0)), (String, "default"), (Ctor(0)))
//
// ZtField Type	C++ Type		ZtField Args
// ------------	--------		------------
// String	<String>		[, default]
// Composite	<Composite>		[, default]
// Bool		<Integral>		[, default]
// Int		<Integral>		[, min, max, default]
// Hex		<Integral>		[, default]
// Enum, Map	<Integral>		[, default]
// Flags, Map	<Integral>		[, default]
// Float	<FloatingPoint>		[, min, max, default]
// Fixed	ZuFixed			[, min, max, default]
// Decimal	ZuDecimal		[, min, max, default]
// Time		ZmTime			[, default]

namespace ZtFieldType {
  enum _ {
    String = 0,		// a contiguous UTF-8 string
    Composite,		// generic composite type
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

inline const char *ZtFieldType_String_Def() { return ""; }
template <
  typename Base, unsigned Flags,
  auto Def = ZtFieldType_String_Def,
  bool = Base::ReadOnly>
struct ZtFieldType_String : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::String };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print {
    const T &v;
    const ZtFieldFmt &fmt; // unused
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      ZuString v{print.v};
      s << '"';
      for (unsigned i = 0, n = v.length(); i < n; i++) {
	char c = v[i];
	if (ZuUnlikely(c == '"')) s << '\\';
	s << c;
      }
      return s << '"';
    }
  };
  const O &o;
  const ZtFieldFmt &fmt;
  template <typename S>
  friend S &operator <<(S &s, const ZtFieldType_String &field) {
    return s << field.id() << '=' << Print{Base::get(field.o), field.fmt};
  }
  ZuInline static const char *deflt() { return Def(); }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << ZtFieldType_String{*static_cast<const O *>(o), fmt};
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
template <typename Base, unsigned Flags, auto Def>
struct ZtFieldType_String<Base, Flags, Def, false> :
    public ZtFieldType_String<Base, Flags, Def, true> {
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

template <typename Base>
struct ZtFieldType_Composite_ {
  using T = ZuUnbox<typename Base::T>;
  constexpr static T deflt() { return {}; };
};
template <
  typename Base, unsigned Flags,
  auto Def = ZtFieldType_Composite_<Base>::deflt,
  bool = Base::ReadOnly>
struct ZtFieldType_Composite : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Composite };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print {
    const T &v;
    const ZtFieldFmt &fmt; // unused
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << '{' << print.v << '}';
    }
  };
  const O &o;
  const ZtFieldFmt &fmt;
  template <typename S>
  friend S &operator <<(S &s, const ZtFieldType_Composite &field) {
    return s << field.id() << '=' << Print{Base::get(field.o), field.fmt};
  }
  ZuInline static constexpr auto deflt() { return Def(); }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << ZtFieldType_Composite{*static_cast<const O *>(o), fmt};
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
template <typename Base, unsigned Flags, auto Def>
struct ZtFieldType_Composite<Base, Flags, Def, false> :
    public ZtFieldType_Composite<Base, Flags, Def, true> {
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

inline constexpr bool ZtFieldType_Bool_Def() { return false; }
template <
  typename Base, unsigned Flags,
  auto Def = ZtFieldType_Bool_Def,
  bool = Base::ReadOnly>
struct ZtFieldType_Bool : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Bool };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << (print.v ? '1' : '0');
    }
  };
  const O &o;
  const ZtFieldFmt &fmt;
  template <typename S>
  friend S &operator <<(S &s, const ZtFieldType_Bool &field) {
    return s << field.id() << '=' << Print{Base::get(field.o), field.fmt};
  }
  ZuInline constexpr static auto deflt() { return Def(); }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << ZtFieldType_Bool{*static_cast<const O *>(o), fmt};
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
template <typename Base, unsigned Flags, auto Def>
struct ZtFieldType_Bool<Base, Flags, Def, false> :
    public ZtFieldType_Bool<Base, Flags, Def, true> {
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

template <typename Base>
struct ZtFieldType_Int_ {
  using T = ZuUnbox<typename Base::T>;
  constexpr static auto minimum() { return ZuCmp<T>::minimum(); };
  constexpr static auto maximum() { return ZuCmp<T>::maximum(); };
  constexpr static auto deflt() { return ZuCmp<T>::null(); };
};
template <
  typename Base, unsigned Flags,
  auto Min = ZtFieldType_Int_<Base>::minimum,
  auto Max = ZtFieldType_Int_<Base>::maximum,
  auto Def = ZtFieldType_Int_<Base>::deflt,
  bool = Base::ReadOnly>
struct ZtFieldType_Int : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Int };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << ZuBoxed(print.v).vfmt(print.fmt.scalar);
    }
  };
  const O &o;
  const ZtFieldFmt &fmt;
  template <typename S>
  friend S &operator <<(S &s, const ZtFieldType_Int &field) {
    return s << field.id() << '=' << Print{Base::get(field.o), field.fmt};
  }
  ZuInline constexpr static auto minimum() { return Min(); }
  ZuInline constexpr static auto maximum() { return Max(); }
  ZuInline constexpr static auto deflt() { return Def(); }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << ZtFieldType_Int{*static_cast<const O *>(o), fmt};
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
template <typename Base, unsigned Flags, auto Min, auto Max, auto Def>
struct ZtFieldType_Int<Base, Flags, Min, Max, Def, false> :
    public ZtFieldType_Int<Base, Flags, Min, Max, Def, true> {
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

inline constexpr int ZtFieldType_Hex_Def() { return 0; }
template <
  typename Base, unsigned Flags,
  auto Def = ZtFieldType_Hex_Def,
  bool = Base::ReadOnly>
struct ZtFieldType_Hex : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Hex };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << ZuBoxed(print.v).vfmt(print.fmt.scalar).hex();
    }
  };
  const O &o;
  const ZtFieldFmt &fmt;
  template <typename S>
  friend S &operator <<(S &s, const ZtFieldType_Hex &field) {
    return s << field.id() << '=' << Print{Base::get(field.o), field.fmt};
  }
  ZuInline constexpr static auto deflt() { return Def(); }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << ZtFieldType_Hex{*static_cast<const O *>(o), fmt};
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
template <typename Base, unsigned Flags, auto Def>
struct ZtFieldType_Hex<Base, Flags, Def, false> :
    public ZtFieldType_Hex<Base, Flags, Def, true> {
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

inline constexpr int ZtFieldType_Enum_Def() { return -1; }
template <
  typename Base, unsigned Flags, typename Map_,
  auto Def = ZtFieldType_Enum_Def,
  bool = Base::ReadOnly>
struct ZtFieldType_Enum : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Enum };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Map::v2s(print.v);
    }
  };
  const O &o;
  const ZtFieldFmt &fmt;
  template <typename S>
  friend S &operator <<(S &s, const ZtFieldType_Enum &field) {
    return s << field.id() << '=' << Print{Base::get(field.o), field.fmt};
  }
  using Map = Map_;
  ZuInline constexpr static auto deflt() { return Def(); }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << ZtFieldType_Enum{*static_cast<const O *>(o), fmt};
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
template <typename Base, unsigned Flags, typename Map, auto Def>
struct ZtFieldType_Enum<Base, Flags, Map, Def, false> :
    public ZtFieldType_Enum<Base, Flags, Map, Def, true> {
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

inline constexpr int ZtFieldType_Flags_Def() { return 0; }
template <
  typename Base, unsigned Flags, typename Map_,
  auto Def = ZtFieldType_Flags_Def,
  bool = Base::ReadOnly>
struct ZtFieldType_Flags : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Flags };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      return s << Map::print(print.v, print.fmt.flagsDelim);
    }
  };
  const O &o;
  const ZtFieldFmt &fmt;
  template <typename S>
  friend S &operator <<(S &s, const ZtFieldType_Flags &field) {
    return s << field.id() << '=' << Print{Base::get(field.o), field.fmt};
  }
  using Map = Map_;
  ZuInline constexpr static auto deflt() { return Def(); }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << ZtFieldType_Flags{*static_cast<const O *>(o), fmt};
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

template <typename Base>
struct ZtFieldType_Float_ {
  using T = ZuUnbox<typename Base::T>;
  constexpr static auto minimum() { return -ZuFP<T>::inf(); };
  constexpr static auto maximum() { return ZuFP<T>::inf(); };
  constexpr static auto deflt() { return ZuCmp<T>::null(); };
};
template <
  typename Base, unsigned Flags,
  auto Min = ZtFieldType_Float_<Base>::minimum,
  auto Max = ZtFieldType_Float_<Base>::maximum,
  auto Def = ZtFieldType_Float_<Base>::deflt,
  bool = Base::ReadOnly>
struct ZtFieldType_Float : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Float };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      if (Flags & ZtFieldFlags::NDP_)
	return s << ZuBoxed(print.v).vfmt(print.fmt.scalar).fp(-ZtFieldFlags::getNDP(Flags));
      else
	return s << ZuBoxed(print.v).vfmt(print.fmt.scalar);
    }
  };
  const O &o;
  const ZtFieldFmt &fmt;
  template <typename S>
  friend S &operator <<(S &s, const ZtFieldType_Float &field) {
    return s << field.id() << '=' << Print{Base::get(field.o), field.fmt};
  }
  ZuInline constexpr static auto minimum() { return Min(); }
  ZuInline constexpr static auto maximum() { return Max(); }
  ZuInline constexpr static auto deflt() { return Def(); }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << ZtFieldType_Float{*static_cast<const O *>(o), fmt};
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
template <typename Base, unsigned Flags, auto Min, auto Max, auto Def>
struct ZtFieldType_Float<Base, Flags, Min, Max, Def, false> :
    public ZtFieldType_Float<Base, Flags, Min, Max, Def, true> {
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

struct ZtFieldType_Fixed_ {
  constexpr static ZuFixed minimum() { return {ZuFixedMin, 0}; }
  constexpr static ZuFixed maximum() { return {ZuFixedMax, 0}; }
  constexpr static ZuFixed deflt() { return {}; }
};
template <
  typename Base, unsigned Flags,
  auto Min = ZtFieldType_Fixed_::minimum,
  auto Max = ZtFieldType_Fixed_::maximum,
  auto Def = ZtFieldType_Fixed_::deflt,
  bool = Base::ReadOnly>
struct ZtFieldType_Fixed : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Fixed };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      if (Flags & ZtFieldFlags::NDP_)
	return s << print.v.vfmt(print.fmt.scalar).fp(-ZtFieldFlags::getNDP(Flags));
      else
	return s << print.v.vfmt(print.fmt.scalar);
    }
  };
  const O &o;
  const ZtFieldFmt &fmt;
  template <typename S>
  friend S &operator <<(S &s, const ZtFieldType_Fixed &field) {
    return s << field.id() << '=' << Print{Base::get(field.o), field.fmt};
  }
  ZuInline constexpr static auto minimum() { return Min(); }
  ZuInline constexpr static auto maximum() { return Max(); }
  ZuInline constexpr static auto deflt() { return Def(); }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << ZtFieldType_Fixed{*static_cast<const O *>(o), fmt};
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
template <typename Base, unsigned Flags, auto Min, auto Max, auto Def>
struct ZtFieldType_Fixed<Base, Flags, Min, Max, Def, false> :
    public ZtFieldType_Fixed<Base, Flags, Min, Max, Def, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &) {
    Base::set(o, ZuFixed{s, Base::get(o).exponent()});
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

struct ZtFieldType_Decimal_ {
  constexpr static ZuDecimal minimum() {
    return {ZuDecimal::Unscaled, ZuDecimal::minimum()};
  }
  constexpr static ZuDecimal maximum() {
    return {ZuDecimal::Unscaled, ZuDecimal::maximum()};
  }
  constexpr static ZuDecimal deflt() { return ZuCmp<ZuDecimal>::null(); }
};
template <
  typename Base, unsigned Flags,
  auto Min = ZtFieldType_Decimal_::minimum,
  auto Max = ZtFieldType_Decimal_::maximum,
  auto Def = ZtFieldType_Decimal_::deflt,
  bool = Base::ReadOnly>
struct ZtFieldType_Decimal : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Decimal };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      if (Flags & ZtFieldFlags::NDP_)
	return s << print.v.vfmt(print.fmt.scalar).fp(-ZtFieldFlags::getNDP(Flags));
      else
	return s << print.v.vfmt(print.fmt.scalar);
    }
  };
  const O &o;
  const ZtFieldFmt &fmt;
  template <typename S>
  friend S &operator <<(S &s, const ZtFieldType_Decimal &field) {
    return s << field.id() << '=' << Print{Base::get(field.o), field.fmt};
  }
  ZuInline constexpr static auto minimum() { return Min(); }
  ZuInline constexpr static auto maximum() { return Max(); }
  ZuInline constexpr static auto deflt() { return Def(); }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << ZtFieldType_Decimal{*static_cast<const O *>(o), fmt};
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
template <
  typename Base, unsigned Flags,
  ZuDecimal Min, ZuDecimal Max, ZuDecimal Def>
struct ZtFieldType_Decimal<Base, Flags, Min, Max, Def, false> :
    public ZtFieldType_Decimal<Base, Flags, Min, Max, Def, true> {
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

inline constexpr ZmTime ZtFieldType_Time_Def() { return {}; }
template <
  typename Base, unsigned Flags,
  auto Def = ZtFieldType_Time_Def,
  bool = Base::ReadOnly>
struct ZtFieldType_Time : public ZtField_<Base, Flags> {
  enum { Type = ZtFieldType::Time };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &print) {
      ZtDate v{print.v};
      return s << v.print(print.fmt.datePrint);
    }
  };
  const O &o;
  const ZtFieldFmt &fmt;
  template <typename S>
  friend S &operator <<(S &s, const ZtFieldType_Time &field) {
    return s << field.id() << '=' << Print{Base::get(field.o), field.fmt};
  }
  ZuInline constexpr static auto deflt() { return Def(); }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << ZtFieldType_Time{*static_cast<const O *>(o), fmt};
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
#define ZtField_LambdaArg(Arg) []{ return Arg; }

#define ZtField_TypeArgs_String(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Composite(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Bool(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Int(...) \
  ZuPP_MapComma(ZtField_LambdaArg, __VA_ARGS__)
#define ZtField_TypeArgs_Hex(...) \
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

#define ZtField_Decl_4(O, ID, Base, TypeName, Type) \
  ZuField_Decl(O, Base) \
  using ZtFieldType_(O, ID) = \
  ZtFieldType_##TypeName<ZuFieldType(O, ID), \
      0 ZtField_TypeArgs(Type)>;
#define ZtField_Decl_5(O, ID, Base, TypeName, Type, Flags) \
  ZuField_Decl(O, Base) \
  using ZtFieldType_(O, ID) = \
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
  ZuPP_Defer(ZtFieldType_)(O, ZuPP_Nest(ZtField_BaseID(Base)))
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
  template <typename U>
  struct Print_Filter { enum { OK = !(U::Flags & ZtFieldFlags::DoNotPrint) }; };
  template <typename S, typename O>
  static void print(S &s, const O &o) {
    using FieldList = ZuTypeGrep<Print_Filter, ZuFieldList<O>>;
    thread_local ZtFieldFmt fmt;
    ZuTypeAll<FieldList>::invoke([&o, &s]<typename Field>() {
      if constexpr (ZuTypeIndex<Field, FieldList>::I) s << ' ';
      s << Field{o, fmt};
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
  ZuIfT<I == ZtFieldType::String> get_(const void *o, L l) const {
    l(getFn.string(o));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldType::Composite> get_(const void *o, L l) const {
    l(getFn.composite(o));
  }
  template <unsigned I, typename L>
  ZuIfT<
    I == ZtFieldType::Bool || I == ZtFieldType::Int ||
    I == ZtFieldType::Fixed || I == ZtFieldType::Hex ||
    I == ZtFieldType::Enum || I == ZtFieldType::Flags> get_(
	const void *o, L l) const {
    l(getFn.int_(o));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldType::Float> get_(const void *o, L l) const {
    l(getFn.float_(o));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldType::Decimal> get_(const void *o, L l) const {
    l(getFn.decimal(o));
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldType::Time> get_(const void *o, L l) const {
    l(getFn.time(o));
  }
  template <typename L> void get(const void *o, L l) const {
    ZuSwitch::dispatch<ZtFieldType::N>(type,
	[this, o, l = ZuMv(l)](auto i) mutable { this->get_<i>(o, ZuMv(l)); });
  }

  template <unsigned I, typename L>
  ZuIfT<
    I == ZtFieldType::String || I == ZtFieldType::Composite> set_(
	void *o, L l) const {
    setFn.string(o, l.template operator ()<ZuString>());
  }
  template <unsigned I, typename L>
  ZuIfT<
    I == ZtFieldType::Bool || I == ZtFieldType::Int ||
    I == ZtFieldType::Fixed || I == ZtFieldType::Hex ||
    I == ZtFieldType::Enum || I == ZtFieldType::Flags> set_(
	void *o, L l) const {
    setFn.int_(o, l.template operator ()<int64_t>());
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldType::Float> set_(void *o, L l) const {
    setFn.float_(o, l.template operator ()<double>());
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldType::Decimal> set_(void *o, L l) const {
    setFn.decimal(o, l.template operator ()<ZuDecimal>());
  }
  template <unsigned I, typename L>
  ZuIfT<I == ZtFieldType::Time> set_(void *o, L l) const {
    setFn.time(o, l.template operator ()<ZmTime>());
  }
  template <typename L>
  void set(void *o, L l) const {
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
