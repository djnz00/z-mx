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
// Composite	<Composite>		[, default]
// Bool		<Integral>		[, default]
// Int		<Integral>		[, default, min, max]
// Hex		<Integral>		[, default]
// Enum, Map	<Integral>		[, default]
// Flags, Map	<Integral>		[, default]
// Float	<FloatingPoint>		[, default, min, max]
// Fixed	ZuFixed			[, default, min, max]
// Decimal	ZuDecimal		[, default, min, max]
// Time		ZmTime			[, default]

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
#include <zlib/ZuField.hpp>

#include <zlib/ZmStream.hpp>
#include <zlib/ZmTime.hpp>
#include <zlib/ZmSingleton.hpp>

#include <zlib/ZtEnum.hpp>
#include <zlib/ZtDate.hpp>
#include <zlib/ZtString.hpp>

#define ZtFieldType(O, ID) ZtField_##O##_##ID

namespace ZtFieldType {
  enum {
    String = 0,		// a contiguous UTF-8 string
    Bytes,		// byte array
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

// compile-time field property list is a typelist of individual properties
// - each type is declared in the ZtFieldProp namespace
// - additional properties can be injected into the ZtFieldProp namespace
namespace ZtFieldProp {
  struct Synthetic { };		// synthetic and read-only
  struct Update { };		// include in updates
  struct Hidden { };		// do not print
  struct Quote { };		// print quoted string
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

// run-time property list is a bitfield of ZtField layer properties
namespace ZtVFieldProp {
  enum {
    Synthetic	= 0x00001,
    Update	= 0x00002,
    Hidden	= 0x00004,
    Quote	= 0x00008,
    Required	= 0x00010,
    Ctor_	= 0x00020,
    NDP_	= 0x00040,
    Series	= 0x00080,
    Index	= 0x00100,
    Delta	= 0x00200,
    Delta2	= 0x00400
  };
  enum {
    CtorShift	= 11,		// bit-shift for constructor parameter index
    CtorMask	= 0x3f		// 6 bits, 0-63
  };
  enum {
    NDPShift	= 17,
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

  // Bits<Prop>::N - return bitfield for individual property
  template <typename> struct Bits { enum { N = 0 }; }; // default

  template <> struct Bits<ZtFieldProp::Synthetic> { enum { N = Synthetic }; };
  template <> struct Bits<ZtFieldProp::Update>    { enum { N = Update }; };
  template <> struct Bits<ZtFieldProp::Hidden>    { enum { N = Hidden }; };
  template <> struct Bits<ZtFieldProp::Quote>     { enum { N = Quote }; };
  template <> struct Bits<ZtFieldProp::Required>  { enum { N = Required }; };
  template <> struct Bits<ZtFieldProp::Series>    { enum { N = Series }; };
  template <> struct Bits<ZtFieldProp::Index>     { enum { N = Index }; };
  template <> struct Bits<ZtFieldProp::Delta>     { enum { N = Delta }; };
  template <> struct Bits<ZtFieldProp::Delta2>    { enum { N = Delta2 }; };

  template <unsigned I>
  struct Bits<ZtFieldProp::Ctor<I>> { enum { N = Ctor(I) }; };

  template <unsigned I>
  struct Bits<ZtFieldProp::NDP<I>>  { enum { N = NDP(I) }; };

  // Bits<List>::N - return bitfield value for property list
  template <typename ...> struct Or_;
  template <typename Prop> struct Or_<Prop> {
    struct T { enum { N = Bits<Prop>::N }; };
  };
  template <typename Prop1, typename Prop2> struct Or_<Prop1, Prop2> {
    struct T { enum { N = Bits<Prop1>::N | Bits<Prop2>::N }; };
  };
  template <typename ...Props> using Or = typename Or_<Props...>::T;
  template <typename ...Props>
  struct Bits<ZuTypeList<Props...>> :
      public ZuTypeReduce<Or, ZuTypeList<Props...>> { };
}

#define ZtField_Props__(Prop) ZtFieldProp::Prop
#define ZtField_Props_(...) ZuPP_MapComma(ZtField_Props__, __VA_ARGS__)
#define ZtField_Props(Args) ZuPP_Defer(ZtField_Props_)(ZuPP_Strip(Args))

struct ZtFieldFmt {
  ZuVFmt		scalar;			// scalar format (print only)
  ZtDateScan::Any	dateScan;		// date/time scan format
  ZtDateFmt::Any	datePrint;		// date/time print format
  char			flagsDelim = '|';	// flags delimiter
};

namespace ZtVFieldConstant {
  enum { Deflt = 0, Minimum, Maximum };		// (*constantFn) parameter
}

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

template <typename Base, typename Props_, typename = void>
struct ZtField_ : public Base {
  using Props = Props_;
  using O = typename Base::O;
  static auto cmpFn() {
    return [](const void *o1, const void *o2) { return 0; };
  }
};
template <typename Base, typename Props_>
struct ZtField_<Base, Props_,
    decltype(&ZuCmp<typename Base::T>::cmp, void())> : public Base {
  using Props = Props_;
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
  typename Base, typename Props,
  auto Def = ZtFieldType_String_Def,
  bool = Base::ReadOnly>
struct ZtFieldType_String : public ZtField_<Base, Props> {
  enum { Type = ZtFieldType::String };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print_ {
    const T &v;
    const ZtFieldFmt &fmt; // unused, but needed at compile-time
    enum { Quote =
      ZuTypeIn<ZtFieldProp::Quote, Props>{} &&
      ZuTraits<T>::IsString && !ZuTraits<T>::IsWString };
    template <typename S, bool Quote_ = Quote>
    friend ZuIfT<Quote_, S &> operator <<(S &s, const Print_ &print) {
      ZuString v{print.v};
      s << '"';
      for (unsigned i = 0, n = v.length(); i < n; i++) {
	char c = v[i];
	if (ZuUnlikely(c == '"')) s << '\\';
	s << c;
      }
      return s << '"';
    }
    template <typename S, bool Quote_ = Quote>
    friend ZuIfT<!Quote_, S &> operator <<(S &s, const Print_ &print) {
      s << '"';
      s << print.v;
      return s << '"';
    }
  };
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt; // unused, but needed at compile-time
    template <typename S>
    friend S &operator <<(S &s, const Print &self) {
      return s << Base::id() << '=' << Print_{Base::get(self.o), self.fmt};
    }
  };
  ZuInline static const char *deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    return [](int i) -> ZuString {
      if (ZuLikely(i == Deflt)) return deflt();
      return ZtFieldType_String_Def();
    };
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << Print{*static_cast<const O *>(o), fmt};
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
template <typename Base, typename Props, auto Def>
struct ZtFieldType_String<Base, Props, Def, false> :
    public ZtFieldType_String<Base, Props, Def, true> {
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

inline ZuBytes ZtFieldType_Bytes_Def() { return {}; }
template <
  typename Base, typename Props,
  auto Def = ZtFieldType_Bytes_Def,
  bool = Base::ReadOnly>
struct ZtFieldType_Bytes : public ZtField_<Base, Props> {
  enum { Type = ZtFieldType::Bytes };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print_ {
    const T &v;
    const ZtFieldFmt &fmt; // unused, but needed at compile-time
    template <typename S>
    friend S &operator <<(S &s, const Print_ &print) {
      return s << ZtHexDump_{ZuBytes{print.v}};
    }
  };
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt; // unused, but needed at compile-time
    template <typename S>
    friend S &operator <<(S &s, const Print &self) {
      return s << Base::id() << '=' << Print_{Base::get(self.o), self.fmt};
    }
  };
  ZuInline static ZuBytes deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    return [](int i) -> ZuBytes {
      if (ZuLikely(i == Deflt)) return deflt();
      return ZtFieldType_Bytes_Def();
    };
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << Print{*static_cast<const O *>(o), fmt};
    };
  }
  static auto getFn() {
    return [](const void *o) -> ZuBytes {
      return Base::get(*static_cast<const O *>(o));
    };
  }
  static auto setFn() {
    return [](void *, ZuBytes) { };
  }
  static auto scanFn() {
    return [](void *, ZuString, const ZtFieldFmt &) { };
  }
};
template <typename Base, typename Props, auto Def>
struct ZtFieldType_Bytes<Base, Props, Def, false> :
    public ZtFieldType_Bytes<Base, Props, Def, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &) {
    Base::set(o, ZuBytes{
      reinterpret_cast<const uint8_t *>(s.data()), s.length()});
  }
  static auto setFn() {
    return [](void *o, ZuBytes s) {
      Base::set(*static_cast<O *>(o), s);
    };
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZtFieldFmt &) {
      Base::set(*static_cast<O *>(o), ZuBytes{
	reinterpret_cast<const uint8_t *>(s.data()), s.length()});
    };
  }
};

template <typename Base, typename Props,
  bool = ZuTypeIn<ZtFieldProp::Synthetic, Props>{},
  typename = void>
struct ZtFieldType_Composite_ {
  constexpr static void deflt() { } // unused
};
// only default non-synthetic fields that are default-constructible
template <typename Base, typename Props>
struct ZtFieldType_Composite_<
    Base, Props, false, decltype(typename Base::T{}, void())> {
  using T = typename Base::T;
  constexpr static T deflt() { return {}; }
};
template <
  typename Base, typename Props,
  auto Def = ZtFieldType_Composite_<Base, Props>::deflt,
  bool = Base::ReadOnly>
struct ZtFieldType_Composite : public ZtField_<Base, Props> {
  enum { Type = ZtFieldType::Composite };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print_ {
    const T &v;
    const ZtFieldFmt &fmt; // unused
    template <typename S>
    friend S &operator <<(S &s, const Print_ &print) {
      return s << '{' << print.v << '}';
    }
  };
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &self) {
      return s << Base::id() << '=' << Print_{Base::get(self.o), self.fmt};
    }
  };
  ZuInline constexpr static auto deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    return [](int i) -> ZmStreamFn {
      if (ZuLikely(i == Deflt)) return {[](ZmStream &s) { s << deflt(); }};
      return {};
    };
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << Print{*static_cast<const O *>(o), fmt};
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
template <typename Base, typename Props, auto Def, typename = void>
struct ZtFieldType_Composite_Scan :
    public ZtFieldType_Composite<Base, Props, Def, true> {
  template <typename P>
  static void scan(P &, ZuString, const ZtFieldFmt &) { }
  static auto scanFn() {
    return [](void *, ZuString, const ZtFieldFmt &) { };
  }
};
template <typename Base, typename Props, auto Def>
struct ZtFieldType_Composite_Scan<Base, Props, Def, decltype(
      Base::set(ZuDeclVal<typename Base::O *>(), ZuDeclVal<ZuString>()),
      void())> :
    public ZtFieldType_Composite<Base, Props, Def, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &) {
    Base::set(o, s);
  }
  static auto scanFn() {
    return [](void *o, ZuString s, const ZtFieldFmt &) {
      Base::set(*static_cast<O *>(o), s);
    };
  }
};
template <typename Base, typename Props, auto Def>
struct ZtFieldType_Composite<Base, Props, Def, false> :
    public ZtFieldType_Composite_Scan<Base, Props, Def> {
  using O = typename Base::O;
  using T = typename Base::T;
  static auto setFn() {
    return [](void *o, const void *p) {
      Base::set(*static_cast<O *>(o), *static_cast<const T *>(p));
    };
  }
};

inline constexpr bool ZtFieldType_Bool_Def() { return false; }
template <
  typename Base, typename Props,
  auto Def = ZtFieldType_Bool_Def,
  bool = Base::ReadOnly>
struct ZtFieldType_Bool : public ZtField_<Base, Props> {
  enum { Type = ZtFieldType::Bool };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print_ {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print_ &print) {
      return s << (print.v ? '1' : '0');
    }
  };
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &self) {
      return s << Base::id() << '=' << Print_{Base::get(self.o), self.fmt};
    }
  };
  ZuInline constexpr static auto deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    return [](int i) -> int64_t {
      if (ZuLikely(i == Deflt)) return deflt();
      return ZtFieldType_Bool_Def();
    };
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << Print{*static_cast<const O *>(o), fmt};
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
template <typename Base, typename Props, auto Def>
struct ZtFieldType_Bool<Base, Props, Def, false> :
    public ZtFieldType_Bool<Base, Props, Def, true> {
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
  constexpr static auto deflt() { return ZuCmp<T>::null(); }
  constexpr static auto minimum() { return ZuCmp<T>::minimum(); }
  constexpr static auto maximum() { return ZuCmp<T>::maximum(); }
};
template <
  typename Base, typename Props,
  auto Def = ZtFieldType_Int_<Base>::deflt,
  auto Min = ZtFieldType_Int_<Base>::minimum,
  auto Max = ZtFieldType_Int_<Base>::maximum,
  bool = Base::ReadOnly>
struct ZtFieldType_Int : public ZtField_<Base, Props> {
  enum { Type = ZtFieldType::Int };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print_ {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print_ &print) {
      return s << ZuBoxed(print.v).vfmt(print.fmt.scalar);
    }
  };
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &self) {
      return s << Base::id() << '=' << Print_{Base::get(self.o), self.fmt};
    }
  };
  ZuInline constexpr static auto deflt() { return Def(); }
  ZuInline constexpr static auto minimum() { return Min(); }
  ZuInline constexpr static auto maximum() { return Max(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    return [](int i) -> int64_t {
      switch (i) {
	case Deflt:	return deflt();
	case Minimum:	return minimum();
	case Maximum:	return maximum();
      }
      return ZtFieldType_Int_<Base>::deflt();
    };
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << Print{*static_cast<const O *>(o), fmt};
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
template <typename Base, typename Props, auto Def, auto Min, auto Max>
struct ZtFieldType_Int<Base, Props, Def, Min, Max, false> :
    public ZtFieldType_Int<Base, Props, Def, Min, Max, true> {
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
  typename Base, typename Props,
  auto Def = ZtFieldType_Hex_Def,
  bool = Base::ReadOnly>
struct ZtFieldType_Hex : public ZtField_<Base, Props> {
  enum { Type = ZtFieldType::Hex };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print_ {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print_ &print) {
      return s << ZuBoxed(print.v).vfmt(print.fmt.scalar).hex();
    }
  };
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &self) {
      return s << Base::id() << '=' << Print_{Base::get(self.o), self.fmt};
    }
  };
  ZuInline constexpr static auto deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    return [](int i) -> int64_t {
      if (ZuLikely(i == Deflt)) return deflt();
      return ZtFieldType_Hex_Def();
    };
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << Print{*static_cast<const O *>(o), fmt};
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
template <typename Base, typename Props, auto Def>
struct ZtFieldType_Hex<Base, Props, Def, false> :
    public ZtFieldType_Hex<Base, Props, Def, true> {
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
  typename Base, typename Props, typename Map_,
  auto Def = ZtFieldType_Enum_Def,
  bool = Base::ReadOnly>
struct ZtFieldType_Enum : public ZtField_<Base, Props> {
  enum { Type = ZtFieldType::Enum };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print_ {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print_ &print) {
      return s << Map::v2s(print.v);
    }
  };
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &self) {
      return s << Base::id() << '=' << Print_{Base::get(self.o), self.fmt};
    }
  };
  using Map = Map_;
  ZuInline constexpr static auto deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    return [](int i) -> int64_t {
      if (ZuLikely(i == Deflt)) return deflt();
      return ZtFieldType_Enum_Def();
    };
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << Print{*static_cast<const O *>(o), fmt};
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
template <typename Base, typename Props, typename Map, auto Def>
struct ZtFieldType_Enum<Base, Props, Map, Def, false> :
    public ZtFieldType_Enum<Base, Props, Map, Def, true> {
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
  typename Base, typename Props, typename Map_,
  auto Def = ZtFieldType_Flags_Def,
  bool = Base::ReadOnly>
struct ZtFieldType_Flags : public ZtField_<Base, Props> {
  enum { Type = ZtFieldType::Flags };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print_ {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print_ &print) {
      return s << Map::print(print.v, print.fmt.flagsDelim);
    }
  };
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &self) {
      return s << Base::id() << '=' << Print_{Base::get(self.o), self.fmt};
    }
  };
  using Map = Map_;
  ZuInline constexpr static auto deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    return [](int i) -> int64_t {
      if (ZuLikely(i == Deflt)) return deflt();
      return ZtFieldType_Flags_Def();
    };
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << Print{*static_cast<const O *>(o), fmt};
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
template <typename Base, typename Props, typename Map, auto Def>
struct ZtFieldType_Flags<Base, Props, Map, Def, false> :
    public ZtFieldType_Flags<Base, Props, Map, Def, true> {
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
  constexpr static auto deflt() { return ZuCmp<T>::null(); }
  constexpr static auto minimum() { return -ZuFP<T>::inf(); }
  constexpr static auto maximum() { return ZuFP<T>::inf(); }
};
template <
  typename Base, typename Props,
  auto Def = ZtFieldType_Float_<Base>::deflt,
  auto Min = ZtFieldType_Float_<Base>::minimum,
  auto Max = ZtFieldType_Float_<Base>::maximum,
  bool = Base::ReadOnly>
struct ZtFieldType_Float : public ZtField_<Base, Props> {
  enum { Type = ZtFieldType::Float };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print_ {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S> S &print(S &s) const {
      constexpr int NDP = ZtFieldProp::GetNDP<Props>{};
      if constexpr (NDP >= 0)
	return s << ZuBoxed(v).vfmt(fmt.scalar).fp(-NDP);
      else
	return s << ZuBoxed(v).vfmt(fmt.scalar);
    }
    template <typename S>
    friend S &operator <<(S &s, const Print_ &self) { return self.print(s); }
  };
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &self) {
      return s << Base::id() << '=' << Print_{Base::get(self.o), self.fmt};
    }
  };
  ZuInline constexpr static auto deflt() { return Def(); }
  ZuInline constexpr static auto minimum() { return Min(); }
  ZuInline constexpr static auto maximum() { return Max(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    return [](int i) -> double {
      switch (i) {
	case Deflt:	return deflt();
	case Minimum:	return minimum();
	case Maximum:	return maximum();
      }
      return ZtFieldType_Float_<Base>::deflt();
    };
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << Print{*static_cast<const O *>(o), fmt};
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
template <typename Base, typename Props, auto Def, auto Min, auto Max>
struct ZtFieldType_Float<Base, Props, Def, Min, Max, false> :
    public ZtFieldType_Float<Base, Props, Def, Min, Max, true> {
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

struct ZtFieldType_Fixed_ {
  constexpr static ZuFixed deflt() { return {}; }
  constexpr static ZuFixed minimum() { return {ZuFixedMin, 0}; }
  constexpr static ZuFixed maximum() { return {ZuFixedMax, 0}; }
};
template <
  typename Base, typename Props,
  auto Def = ZtFieldType_Fixed_::deflt,
  auto Min = ZtFieldType_Fixed_::minimum,
  auto Max = ZtFieldType_Fixed_::maximum,
  bool = Base::ReadOnly>
struct ZtFieldType_Fixed : public ZtField_<Base, Props> {
  enum { Type = ZtFieldType::Fixed };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print_ {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S> S &print(S &s) const {
      constexpr int NDP = ZtFieldProp::GetNDP<Props>{};
      if constexpr (NDP >= 0)
	return s << v.vfmt(fmt.scalar).fp(-NDP);
      else
	return s << v.vfmt(fmt.scalar);
    }
    template <typename S>
    friend S &operator <<(S &s, const Print_ &self) { return self.print(s); }
  };
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &self) {
      return s << Base::id() << '=' << Print_{Base::get(self.o), self.fmt};
    }
  };
  ZuInline constexpr static auto deflt() { return Def(); }
  ZuInline constexpr static auto minimum() { return Min(); }
  ZuInline constexpr static auto maximum() { return Max(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    return [](int i) -> ZuFixed {
      switch (i) {
	case Deflt:	return deflt();
	case Minimum:	return minimum();
	case Maximum:	return maximum();
      }
      return ZtFieldType_Fixed_::deflt();
    };
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << Print{*static_cast<const O *>(o), fmt};
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
template <typename Base, typename Props, auto Def, auto Min, auto Max>
struct ZtFieldType_Fixed<Base, Props, Def, Min, Max, false> :
    public ZtFieldType_Fixed<Base, Props, Def, Min, Max, true> {
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
  auto Def = ZtFieldType_Decimal_::deflt,
  auto Min = ZtFieldType_Decimal_::minimum,
  auto Max = ZtFieldType_Decimal_::maximum,
  bool = Base::ReadOnly>
struct ZtFieldType_Decimal : public ZtField_<Base, Props> {
  enum { Type = ZtFieldType::Decimal };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print_ {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S> S &print(S &s) const {
      constexpr int NDP = ZtFieldProp::GetNDP<Props>{};
      if constexpr (NDP >= 0)
	return s << v.vfmt(fmt.scalar).fp(-NDP);
      else
	return s << v.vfmt(fmt.scalar);
    }
    template <typename S>
    friend S &operator <<(S &s, const Print_ &self) { return self.print(s); }
  };
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &self) {
      return s << Base::id() << '=' << Print_{Base::get(self.o), self.fmt};
    }
  };
  ZuInline constexpr static auto deflt() { return Def(); }
  ZuInline constexpr static auto minimum() { return Min(); }
  ZuInline constexpr static auto maximum() { return Max(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    return [](int i) -> ZuDecimal {
      switch (i) {
	case Deflt:	return deflt();
	case Minimum:	return minimum();
	case Maximum:	return maximum();
      }
      return ZtFieldType_Decimal_::deflt();
    };
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << Print{*static_cast<const O *>(o), fmt};
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
template <typename Base, typename Props, auto Def, auto Min, auto Max>
struct ZtFieldType_Decimal<Base, Props, Def, Min, Max, false> :
    public ZtFieldType_Decimal<Base, Props, Def, Min, Max, true> {
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
  typename Base, typename Props,
  auto Def = ZtFieldType_Time_Def,
  bool = Base::ReadOnly>
struct ZtFieldType_Time : public ZtField_<Base, Props> {
  enum { Type = ZtFieldType::Time };
  using O = typename Base::O;
  using T = typename Base::T;
  struct Print_ {
    const T &v;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print_ &print) {
      ZtDate v{print.v};
      return s << v.print(print.fmt.datePrint);
    }
  };
  struct Print {
    const O &o;
    const ZtFieldFmt &fmt;
    template <typename S>
    friend S &operator <<(S &s, const Print &self) {
      return s << Base::id() << '=' << Print_{Base::get(self.o), self.fmt};
    }
  };
  ZuInline constexpr static auto deflt() { return Def(); }
  static auto constantFn() {
    using namespace ZtVFieldConstant;
    return [](int i) -> ZmTime {
      if (ZuLikely(i == Deflt)) return deflt();
      return ZtFieldType_Time_Def();
    };
  }
  static auto printFn() {
    return [](const void *o, ZmStream &s, const ZtFieldFmt &fmt) {
      s << Print{*static_cast<const O *>(o), fmt};
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
template <typename Base, typename Props, auto Def>
struct ZtFieldType_Time<Base, Props, Def, false> :
    public ZtFieldType_Time<Base, Props, Def, true> {
  using O = typename Base::O;
  template <typename P>
  static void scan(P &o, ZuString s, const ZtFieldFmt &fmt) {
    Base::set(o, ZtDate{fmt.dateScan, s}.zmTime());
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
#define ZtField_TypeArgs_Bytes(...) \
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
  using ZtFieldType(O, ID) = \
  ZtFieldType_##TypeName<ZuFieldType(O, ID), \
      ZuTypeList<> ZtField_TypeArgs(Type)>;
#define ZtField_Decl_5(O, ID, Base, TypeName, Type, Props) \
  ZuField_Decl(O, Base) \
  using ZtFieldType(O, ID) = \
  ZtFieldType_##TypeName<ZuFieldType(O, ID), \
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

struct ZtFieldPrint : public ZuPrintDelegate {
  template <typename U> struct Print_Filter :
      public ZuBool<!ZuTypeIn<ZtFieldProp::Hidden, typename U::Props>{}> { };
  template <typename S, typename O>
  static void print(S &s, const O &o) {
    using FieldList = ZuTypeGrep<Print_Filter, ZuFieldList<O>>;
    thread_local ZtFieldFmt fmt;
    ZuTypeAll<FieldList>::invoke([&o, &s]<typename Field>() {
      if constexpr (ZuTypeIndex<Field, FieldList>{}) s << ' ';
      s << typename Field::Print{o, fmt};
    });
  }
};

// run-time introspection
//
// Note: virtual polymorphism and RTTI are avoided here because:
// 1] If ZtVField were virtually polymorphic, deriving from it to enrich it
//    with capabilities such as data-store serdes would entail a much
//    more complex type hierarchy with diamond-shaped inheritance
// 2] ZtVField (and derived classes) need to be POD
// 3] Very little syntactic benefit would be obtained

struct ZtVField {
  const char		*id;
  uint32_t		type;		// ZtFieldType
  uint32_t		props;		// ZtVFieldProp
  uint64_t		keys;

  int		(*cmp)(const void *, const void *);

  union {
    void		*null;
    ZtVFieldEnum	*(*enum_)();		// Enum
    ZtVFieldFlags	*(*flags)();		// Flags
  } info;

  union {
    ZuString	(*string)(int);			// String
    ZuBytes	(*bytes)(int);			// Bytes
    ZmStreamFn	(*composite)(int);		// Composite
    int64_t	(*int_)(int);			// Bool|Int|Hex|Enum|Flags
    double	(*float_)(int);			// Float
    ZuFixed	(*fixed)(int);			// Fixed
    ZuDecimal	(*decimal)(int);		// Decimal
    ZmTime	(*time)(int);			// Time
  } constantFn;

  void		(*print)(const void *, ZmStream &, const ZtFieldFmt &);
  void		(*scan)(void *, ZuString, const ZtFieldFmt &);
  union {
    ZuString	(*string)(const void *);	// String
    ZuBytes	(*bytes)(const void *);		// Bytes
    ZmStreamFn	(*composite)(const void *);	// Composite
    int64_t	(*int_)(const void *);		// Bool|Int|Hex|Enum|Flags
    double	(*float_)(const void *);	// Float
    ZuFixed	(*fixed)(const void *);		// Fixed
    ZuDecimal	(*decimal)(const void *);	// Decimal
    ZmTime	(*time)(const void *);		// Time
  } getFn;
  union {
    void	(*string)(void *, ZuString);	// String
    void	(*bytes)(void *, ZuBytes);	// Bytes
    void	(*composite)(void *, const void *);// Composite
    void	(*int_)(void *, int64_t);	// Bool|Int|Hex|Enum|Flags
    void	(*float_)(void *, double);	// Float
    void	(*fixed)(void *, ZuFixed);	// Fixed
    void	(*decimal)(void *, ZuDecimal);	// Decimal
    void	(*time)(void *, ZmTime);	// Time
  } setFn;

  template <typename Field>
  ZtVField(Field, ZuIfT<Field::Type == ZtFieldType::String> *_ = nullptr) :
      id{Field::id()}, type{Field::Type},
      props{ZtVFieldProp::Bits<typename Field::Props>::N},
      keys{Field::keys()},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      constantFn{.string = Field::constantFn()},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.string = Field::getFn()},
      setFn{.string = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field, ZuIfT<Field::Type == ZtFieldType::Bytes> *_ = nullptr) :
      id{Field::id()}, type{Field::Type},
      props{ZtVFieldProp::Bits<typename Field::Props>::N},
      keys{Field::keys()},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      constantFn{.bytes = Field::constantFn()},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.bytes = Field::getFn()},
      setFn{.bytes = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field, ZuIfT<Field::Type == ZtFieldType::Composite> *_ = nullptr) :
      id{Field::id()}, type{Field::Type},
      props{ZtVFieldProp::Bits<typename Field::Props>::N},
      keys{Field::keys()},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      constantFn{.composite = Field::constantFn()},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.composite = Field::getFn()},
      setFn{.composite = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field,
        ZuIfT<
	  Field::Type == ZtFieldType::Bool ||
	  Field::Type == ZtFieldType::Int ||
	  Field::Type == ZtFieldType::Hex> *_ = nullptr) :
      id{Field::id()}, type{Field::Type},
      props{ZtVFieldProp::Bits<typename Field::Props>::N},
      keys{Field::keys()},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      constantFn{.int_ = Field::constantFn()},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.int_ = Field::getFn()},
      setFn{.int_ = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field, ZuIfT<Field::Type == ZtFieldType::Enum> *_ = nullptr) :
      id{Field::id()}, type{Field::Type},
      props{ZtVFieldProp::Bits<typename Field::Props>::N},
      keys{Field::keys()},
      cmp{Field::cmpFn()},
      info{.enum_ = Field::enumFn()},
      constantFn{.int_ = Field::constantFn()},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.int_ = Field::getFn()},
      setFn{.int_ = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field, ZuIfT<Field::Type == ZtFieldType::Flags> *_ = nullptr) :
      id{Field::id()}, type{Field::Type},
      props{ZtVFieldProp::Bits<typename Field::Props>::N},
      keys{Field::keys()},
      cmp{Field::cmpFn()},
      info{.flags = Field::flagsFn()},
      constantFn{.int_ = Field::constantFn()},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.int_ = Field::getFn()},
      setFn{.int_ = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field, ZuIfT<Field::Type == ZtFieldType::Float> *_ = nullptr) :
      id{Field::id()}, type{Field::Type},
      props{ZtVFieldProp::Bits<typename Field::Props>::N},
      keys{Field::keys()},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      constantFn{.float_ = Field::constantFn()},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.float_ = Field::getFn()},
      setFn{.float_ = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field, ZuIfT<Field::Type == ZtFieldType::Fixed> *_ = nullptr) :
      id{Field::id()}, type{Field::Type},
      props{ZtVFieldProp::Bits<typename Field::Props>::N},
      keys{Field::keys()},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      constantFn{.fixed = Field::constantFn()},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.fixed = Field::getFn()},
      setFn{.fixed = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field, ZuIfT<Field::Type == ZtFieldType::Decimal> *_ = nullptr) :
      id{Field::id()}, type{Field::Type},
      props{ZtVFieldProp::Bits<typename Field::Props>::N},
      keys{Field::keys()},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      constantFn{.decimal = Field::constantFn()},
      print{Field::printFn()},
      scan{Field::scanFn()},
      getFn{.decimal = Field::getFn()},
      setFn{.decimal = Field::setFn()} { }

  template <typename Field>
  ZtVField(Field, ZuIfT<Field::Type == ZtFieldType::Time> *_ = nullptr) :
      id{Field::id()}, type{Field::Type},
      props{ZtVFieldProp::Bits<typename Field::Props>::N},
      keys{Field::keys()},
      cmp{Field::cmpFn()},
      info{.null = nullptr},
      constantFn{.time = Field::constantFn()},
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
