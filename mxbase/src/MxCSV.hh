//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Mx CSV common definitions

#ifndef MxCSV_HH
#define MxCSV_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxBaseLib_HH
#include <mxbase/MxBaseLib.hh>
#endif

#include <zlib/ZiIP.hh>

#include <zlib/ZvCSV.hh>

#include <mxbase/MxBase.hh>

typedef ZvCSVColumn<ZvCSVColType::Int, MxBool> MxBoolCol;
typedef ZvCSVColumn<ZvCSVColType::Int, MxInt> MxIntCol;
typedef ZvCSVColumn<ZvCSVColType::Int, MxInt64> MxInt64Col;
typedef ZvCSVColumn<ZvCSVColType::Int, MxUInt> MxUIntCol;
typedef ZvCSVColumn<ZvCSVColType::Int, MxUInt64> MxUInt64Col;
typedef ZvCSVColumn<ZvCSVColType::Float, MxFloat> MxFloatCol;
typedef ZvCSVColumn<ZvCSVColType::Int, MxNDP> MxNDPCol;
typedef ZvCSVColumn<ZvCSVColType::Int, MxRatio> MxRatioCol;
typedef ZvCSVColumn<ZvCSVColType::Time, MxDateTime> MxTimeCol;
typedef ZvCSVColumn<ZvCSVColType::String, MxIDString> MxIDStrCol;
template <typename Map> using MxEnumCol = ZvCSVEnumColumn<MxEnum, Map>;
template <typename Map> using MxFlagsCol = ZvCSVFlagsColumn<MxFlags, Map>;
template <typename Map> using MxFlags64Col = ZvCSVFlagsColumn<MxFlags64, Map>;

class MxIPCol : public ZvCSVColumn<ZvCSVColType::Func, ZiIP> {
  typedef ZvCSVColumn<ZvCSVColType::Func, ZiIP> Base;
  typedef typename Base::ParseFn ParseFn;
  typedef typename Base::PlaceFn PlaceFn;
public:
  template <typename ID> MxIPCol(ID &&id, int offset) :
      Base(ZuFwd<ID>(id), offset,
	  ParseFn::Ptr<&MxIPCol::parse_>::fn(),
	  PlaceFn::Ptr<&MxIPCol::place_>::fn()) { }
  static void parse_(ZiIP *i, ZuString b) { *i = b; }
  static void place_(ZtArray<char> &b, const ZiIP *i) { b << *i; }
};

typedef ZvCSVColumn<ZvCSVColType::Int, ZuBox0(uint16_t)> MxPortCol;

class MxIDCol : public ZvCSVColumn<ZvCSVColType::Func, MxID>  {
  typedef ZvCSVColumn<ZvCSVColType::Func, MxID> Base;
  typedef typename Base::ParseFn ParseFn;
  typedef typename Base::PlaceFn PlaceFn;
public:
  template <typename ID> MxIDCol(ID &&id, int offset) :
      Base(ZuFwd<ID>(id), offset,
	  ParseFn::Ptr<&MxIDCol::parse_>::fn(),
	  PlaceFn::Ptr<&MxIDCol::place_>::fn()) { }
  static void parse_(MxID *i, ZuString b) { *i = b; }
  static void place_(ZtArray<char> &b, const MxID *i) { b << *i; }
};

class MxHHMMSSCol : public ZvCSVColumn<ZvCSVColType::Func, MxDateTime>  {
  typedef ZvCSVColumn<ZvCSVColType::Func, MxDateTime> Base;
  typedef typename Base::ParseFn ParseFn;
  typedef typename Base::PlaceFn PlaceFn;
public:
  template <typename ID>
  MxHHMMSSCol(ID &&id, int offset, unsigned yyyymmdd, int tzOffset) :
      Base(ZuFwd<ID>(id), offset,
	  ParseFn::Member<&MxHHMMSSCol::parse_>::fn(this),
	  PlaceFn::Member<&MxHHMMSSCol::place_>::fn(this)),
      m_yyyymmdd(yyyymmdd), m_tzOffset(tzOffset) { }
  virtual ~MxHHMMSSCol() { }

  void parse_(MxDateTime *t, ZuString b) {
    new (t) MxDateTime(
	MxDateTime::YYYYMMDD, m_yyyymmdd, MxDateTime::HHMMSS, MxUInt(b));
    *t -= ZmTime(m_tzOffset);
  }
  void place_(ZtArray<char> &b, const MxDateTime *t) {
    MxDateTime l = *t + m_tzOffset;
    b << MxUInt(l.hhmmss()).fmt<ZuFmt::Right<6>>();
  }

private:
  ZtString	m_tz;
  unsigned	m_yyyymmdd;
  int		m_tzOffset;
};
class MxNSecCol : public ZvCSVColumn<ZvCSVColType::Func, MxDateTime> {
  typedef ZvCSVColumn<ZvCSVColType::Func, MxDateTime> Base;
  typedef typename Base::ParseFn ParseFn;
  typedef typename Base::PlaceFn PlaceFn;
public:
  template <typename ID>
  MxNSecCol(ID &&id, int offset) :
      Base(ZuFwd<ID>(id), offset,
	  ParseFn::Ptr<&MxNSecCol::parse_>::fn(),
	  PlaceFn::Ptr<&MxNSecCol::place_>::fn()) { }

  static void parse_(MxDateTime *t, ZuString b) {
    t->nsec() = MxUInt(b);
  }
  static void place_(ZtArray<char> &b, const MxDateTime *t) {
    b << MxUInt(t->nsec()).fmt(ZuFmt::Right<9>());
  }
};

class MxValueCol : public ZvCSVColumn<ZvCSVColType::Func, MxValue> {
  typedef ZvCSVColumn<ZvCSVColType::Func, MxValue> Base;
  typedef typename Base::ParseFn ParseFn;
  typedef typename Base::PlaceFn PlaceFn;
public:
  template <typename ID>
  MxValueCol(ID &&id, int offset, int ndpOffset) :
      Base(ZuFwd<ID>(id), offset,
	  ParseFn::Member<&MxValueCol::parse_>::fn(this),
	  PlaceFn::Member<&MxValueCol::place_>::fn(this)),
      m_ndpOffset(ndpOffset - offset) { }
  virtual ~MxValueCol() { }

  void parse_(MxValue *f, ZuString b) {
    *f = MxValNDP{b, ndp(f)}.value;
  }
  void place_(ZtArray<char> &b, const MxValue *f) {
    if (**f) b << MxValNDP{*f, ndp(f)};
  }

private:
  ZuInline MxNDP ndp(const MxValue *f) {
    return *(MxNDP *)((const char *)(const void *)f + m_ndpOffset);
  }

  int	m_ndpOffset;
};

class MxDecimalCol : public ZvCSVColumn<ZvCSVColType::Func, MxDecimal> {
  typedef ZvCSVColumn<ZvCSVColType::Func, MxDecimal> Base;
  typedef typename Base::ParseFn ParseFn;
  typedef typename Base::PlaceFn PlaceFn;
public:
  template <typename ID>
  MxDecimalCol(ID &&id, int offset) :
      Base(ZuFwd<ID>(id), offset,
	  ParseFn::Member<&MxDecimalCol::parse_>::fn(this),
	  PlaceFn::Member<&MxDecimalCol::place_>::fn(this)) { }
  virtual ~MxDecimalCol() { }

  void parse_(MxDecimal *f, ZuString b) {
    *f = MxDecimal{b};
  }
  void place_(ZtArray<char> &b, const MxDecimal *f) {
    if (**f) b << *f;
  }
};

struct MxCSVApp {
  static bool hhmmss() { return false; }
  static unsigned yyyymmdd() { return 0; }
  static int tzOffset() { return 0; }

  static bool raw() { return false; }
};

template <typename CSV> struct MxCSV {
  template <typename App, typename ID>
  void addValCol(App *app, ID &&id, int offset, int ndpOffset) {
    if (app && app->raw())
      static_cast<CSV *>(this)->add(
	  new ZvCSVColumn<ZvCSVColType::Int, MxValue>(ZuFwd<ID>(id), offset));
    else
      static_cast<CSV *>(this)->add(
	  new MxValueCol(ZuFwd<ID>(id), offset, ndpOffset));
  }
};

class MxUniKeyCSV : public ZvCSV, public MxCSV<MxUniKeyCSV> {
public:
  typedef MxUniKey Data;
  typedef ZuPOD<Data> POD;

  typedef ZvCSVColumn<ZvCSVColType::Int, MxValue> RawValueCol;

  template <typename App = MxCSVApp>
  MxUniKeyCSV(App *app = 0) {
    new ((m_pod = new POD())->ptr()) Data{};
#ifdef Offset
#undef Offset
#endif
#define Offset(x) offsetof(Data, x)
    add(new MxIDStrCol("id", Offset(id)));
    add(new MxIDCol("venue", Offset(venue)));
    add(new MxIDCol("segment", Offset(segment)));
    add(new RawValueCol("strike", Offset(strike)));
    add(new MxUIntCol("mat", Offset(mat)));
    add(new MxEnumCol<MxInstrIDSrc::CSVMap>("src", Offset(src)));
    add(new MxEnumCol<MxPutCall::CSVMap>("putCall", Offset(putCall)));
#undef Offset
  }

  void alloc(ZuRef<ZuAnyPOD> &pod) { pod = m_pod; }

  template <typename File>
  void read(const File &file, ZvCSVReadFn fn) {
    ZvCSV::readFile(file,
	ZvCSVAllocFn::Member<&MxUniKeyCSV::alloc>::fn(this), fn);
  }

  ZuInline POD *pod() { return m_pod.ptr(); }
  ZuInline Data *ptr() { return m_pod->ptr(); }

  ZuInline static MxUniKey key(const ZuAnyPOD *pod) { return pod->as<Data>(); }

private:
  ZuRef<POD>	m_pod;
};

#endif /* MxCSV_HH */
