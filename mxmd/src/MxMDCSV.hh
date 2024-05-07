//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxMD CSV common definitions

#ifndef MxMDCSV_HH
#define MxMDCSV_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxMDLib_HH
#include <mxmd/MxMDLib.hh>
#endif

#include <zlib/ZiIP.hh>

#include <zlib/ZvCSV.hh>

#include <mxbase/MxBase.hh>
#include <mxbase/MxCSV.hh>

#include <mxmd/MxMDTypes.hh>
#include <mxmd/MxMDStream.hh>

template <typename Flags>
class MxMDVenueFlagsCol : public ZvCSVColumn<ZvCSVColType::Func, MxFlags> {
  typedef ZvCSVColumn<ZvCSVColType::Func, MxFlags> Base;
  typedef typename Base::ParseFn ParseFn;
  typedef typename Base::PlaceFn PlaceFn;
public:
  template <typename ID>
  MxMDVenueFlagsCol(ID &&id, int offset, int venueOffset) :
    Base(ZuFwd<ID>(id), offset,
	ParseFn::Member<&MxMDVenueFlagsCol::parse_>::fn(this),
	PlaceFn::Member<&MxMDVenueFlagsCol::place_>::fn(this)),
    m_venueOffset(venueOffset - offset) { }
  virtual ~MxMDVenueFlagsCol() { }

  void parse_(MxFlags *f, ZuString b) {
    unsigned i, n = b.length(); if (n > 9) n = 9;
    for (i = 0;; i++) {
      if (ZuUnlikely(i == n)) { *f = 0; return; }
      if (b[i] == ':') break;
    }
    MxID venue = ZuString(&b[0], i++);
    *(MxID *)((char *)(void *)f + m_venueOffset) = venue;
    MxMDFlagsStr in = ZuString(&b[i], b.length() - i);
    Flags::scan(in, venue, *f);
  }
  void place_(ZtArray<char> &b, const MxFlags *f) {
    if (!*f) return;
    MxID venue =
      *(const MxID *)((const char *)(const void *)f + m_venueOffset);
    MxMDFlagsStr out;
    Flags::print(out, venue, *f);
    if (!out) return;
    b << venue << ':' << out;
  }

private:
  int	m_venueOffset;
};

class MxMDVenueCSV : public ZvCSV, public MxCSV<MxMDVenueCSV> {
public:
  struct Data {
    MxID		id;
    MxEnum		orderIDScope;
    MxFlags		venueFlags;
  };
  typedef ZuPOD<Data> POD;

  template <typename App = MxCSVApp>
  MxMDVenueCSV(App *app = 0) {
    new ((m_pod = new POD())->ptr()) Data{};
#ifdef Offset
#undef Offset
#endif
#define Offset(x) offsetof(Data, x)
    add(new MxIDCol("id", Offset(id)));
    add(new MxEnumCol<MxMDOrderIDScope::CSVMap>(
	  "orderIDScope", Offset(orderIDScope)));
    add(new MxFlagsCol<MxMDVenueFlags::Flags>(
	  "venueFlags", Offset(venueFlags)));
#undef Offset
  }

  void alloc(ZuRef<ZuAnyPOD> &pod) { pod = m_pod; }

  template <typename File>
  void read(const File &file, ZvCSVReadFn fn) {
    ZvCSV::readFile(file,
	ZvCSVAllocFn::Member<&MxMDVenueCSV::alloc>::fn(this), fn);
  }

  ZuAnyPOD *row(const MxMDStream::Msg *msg) {
    Data *data = ptr();

    {
      using namespace MxMDStream;
      const Hdr &hdr = msg->hdr();
      switch ((int)hdr.type) {
	case Type::AddVenue:
	  {
	    const AddVenue &obj = hdr.as<AddVenue>();
	    new (data) Data{obj.id, obj.orderIDScope, obj.flags};
	  }
	  break;
	default:
	  return 0;
      }
    }

    return pod();
  }

  ZuInline POD *pod() { return m_pod.ptr(); }
  ZuInline Data *ptr() { return m_pod->ptr(); }

private:
  ZuRef<POD>	m_pod;
};

class MxMDTickSizeCSV : public ZvCSV, public MxCSV<MxMDTickSizeCSV> {
public:
  struct Data {
    MxEnum		event;
    MxID		venue;
    MxIDString		id;
    MxNDP		pxNDP;
    MxValue		minPrice;
    MxValue		maxPrice;
    MxValue		tickSize;
  };
  typedef ZuPOD<Data> POD;

  template <typename App = MxCSVApp>
  MxMDTickSizeCSV(App *app = 0) {
    new ((m_pod = new POD())->ptr()) Data{};
#ifdef Offset
#undef Offset
#endif
#define Offset(x) offsetof(Data, x)
    add(new MxEnumCol<MxMDStream::Type::CSVMap>("event", Offset(event)));
    add(new MxIDCol("venue", Offset(venue)));
    add(new MxIDStrCol("id", Offset(id)));
    add(new MxNDPCol("pxNDP", Offset(pxNDP)));
    this->addValCol(app, "minPrice", Offset(minPrice), Offset(pxNDP));
    this->addValCol(app, "maxPrice", Offset(maxPrice), Offset(pxNDP));
    this->addValCol(app, "tickSize", Offset(tickSize), Offset(pxNDP));
#undef Offset
  }

  void alloc(ZuRef<ZuAnyPOD> &pod) { pod = m_pod; }

  template <typename File>
  void read(const File &file, ZvCSVReadFn fn) {
    ZvCSV::readFile(file,
	ZvCSVAllocFn::Member<&MxMDTickSizeCSV::alloc>::fn(this), fn);
  }

  ZuAnyPOD *row(const MxMDStream::Msg *msg) {
    Data *data = ptr();

    {
      using namespace MxMDStream;
      const Hdr &hdr = msg->hdr();
      switch ((int)hdr.type) {
	case Type::AddTickSizeTbl:
	case Type::ResetTickSizeTbl:
	  {
	    const AddTickSizeTbl &obj = hdr.as<AddTickSizeTbl>();
	    new (data) Data{hdr.type, obj.venue, obj.id, obj.pxNDP};
	  }
	  break;
	case Type::AddTickSize:
	  {
	    const AddTickSize &obj = hdr.as<AddTickSize>();
	    new (data) Data{hdr.type,
	      obj.venue, obj.id, obj.pxNDP,
	      obj.minPrice, obj.maxPrice, obj.tickSize};
	  }
	  break;
	default:
	  return 0;
      }
    }

    return pod();
  }

  ZuInline POD *pod() { return m_pod.ptr(); }
  ZuInline Data *ptr() { return m_pod->ptr(); }

private:
  ZuRef<POD>	m_pod;
};

class MxMDInstrumentCSV : public ZvCSV, public MxCSV<MxMDInstrumentCSV> {
public:
  struct Data {
    MxUInt		shard;
    MxEnum		event;
    MxDateTime		transactTime;
    MxID		venue;
    MxID		segment;
    MxIDString		id;
    MxMDInstrRefData	refData;
  };
  typedef ZuPOD<Data> POD;

  template <typename App = MxCSVApp>
  MxMDInstrumentCSV(App *app = 0) {
    new ((m_pod = new POD())->ptr()) Data{};
#ifdef Offset
#undef Offset
#endif
#define Offset(x) offsetof(Data, x)
    add(new MxUIntCol("shard", Offset(shard)));
    add(new MxEnumCol<MxMDStream::Type::CSVMap>("event", Offset(event)));
    if (app && app->hhmmss())
      add(new MxHHMMSSCol("transactTime", Offset(transactTime),
	    app->yyyymmdd(), app->tzOffset()));
    else
      add(new MxTimeCol("transactTime", Offset(transactTime),
	    app ? app->tzOffset() : 0));
    add(new MxIDCol("venue", Offset(venue)));
    add(new MxIDCol("segment", Offset(segment)));
    add(new MxIDStrCol("id", Offset(id)));
#undef Offset
#define Offset(x) offsetof(Data, refData) + offsetof(MxMDInstrRefData, x)
    add(new MxBoolCol("tradeable", Offset(tradeable), -1, 1));
    add(new MxIDCol("baseAsset", Offset(baseAsset)));
    add(new MxIDCol("quoteAsset", Offset(quoteAsset)));
    add(new MxEnumCol<MxInstrIDSrc::CSVMap>("idSrc", Offset(idSrc)));
    add(new MxIDStrCol("symbol", Offset(symbol)));
    add(new MxEnumCol<MxInstrIDSrc::CSVMap>("altIDSrc", Offset(altIDSrc)));
    add(new MxIDStrCol("altSymbol", Offset(altSymbol)));
    add(new MxIDCol("underVenue", Offset(underVenue)));
    add(new MxIDCol("underSegment", Offset(underSegment)));
    add(new MxIDStrCol("underlying", Offset(underlying)));
    add(new MxUIntCol("mat", Offset(mat)));
    add(new MxEnumCol<MxPutCall::CSVMap>("putCall", Offset(putCall)));
    add(new MxNDPCol("pxNDP", Offset(pxNDP)));
    add(new MxNDPCol("qtyNDP", Offset(qtyNDP)));
    this->addValCol(app, "strike", Offset(strike), Offset(pxNDP));
    add(new MxUIntCol("outstandingUnits", Offset(outstandingUnits)));
    this->addValCol(app, "adv", Offset(adv), Offset(pxNDP));
#undef Offset
  }

  void alloc(ZuRef<ZuAnyPOD> &pod) { pod = m_pod; }

  template <typename File>
  void read(const File &file, ZvCSVReadFn fn) {
    ZvCSV::readFile(file,
	ZvCSVAllocFn::Member<&MxMDInstrumentCSV::alloc>::fn(this), fn);
  }

  ZuAnyPOD *row(const MxMDStream::Msg *msg) {
    Data *data = ptr();

    {
      using namespace MxMDStream;
      const Hdr &hdr = msg->hdr();
      switch ((int)hdr.type) {
	case Type::AddInstrument:
	  {
	    const AddInstrument &obj = hdr.as<AddInstrument>();
	    new (data) Data{hdr.shard, hdr.type, obj.transactTime,
	      obj.key.venue, obj.key.segment, obj.key.id,
	      obj.refData};
	  }
	  break;
	case Type::UpdateInstrument:
	  {
	    const UpdateInstrument &obj = hdr.as<UpdateInstrument>();
	    new (data) Data{hdr.shard, hdr.type, obj.transactTime,
	      obj.key.venue, obj.key.segment, obj.key.id,
	      obj.refData};
	  }
	  break;
	default:
	  return 0;
      }
    }

    return pod();
  }

  ZuInline POD *pod() { return m_pod.ptr(); }
  ZuInline Data *ptr() { return m_pod->ptr(); }

private:
  ZuRef<POD>	m_pod;
};

class MxMDOrderBookCSV : public ZvCSV, public MxCSV<MxMDOrderBookCSV> {
public:
  struct Data {
    MxUInt		shard;
    MxEnum		event;
    MxDateTime		transactTime;
    MxID		venue;
    MxID		segment;
    MxIDString		id;
    MxNDP		pxNDP;
    MxNDP		qtyNDP;
    MxUInt		legs;
    MxIDString		tickSizeTbl;
    MxMDLotSizes	lotSizes;
    MxID		instrVenues[MxMDNLegs];
    MxID		instrSegments[MxMDNLegs];
    MxIDString		instruments[MxMDNLegs];
    MxEnum		sides[MxMDNLegs];
    MxRatio		ratios[MxMDNLegs];
  };
  typedef ZuPOD<Data> POD;

  template <typename App = MxCSVApp>
  MxMDOrderBookCSV(App *app = 0) {
    new ((m_pod = new POD())->ptr()) Data{};
#ifdef Offset
#undef Offset
#endif
#define Offset(x) offsetof(Data, x)
    add(new MxUIntCol("shard", Offset(shard)));
    add(new MxEnumCol<MxMDStream::Type::CSVMap>("event", Offset(event)));
    if (app && app->hhmmss())
      add(new MxHHMMSSCol("transactTime", Offset(transactTime),
	    app->yyyymmdd(), app->tzOffset()));
    else
      add(new MxTimeCol("transactTime", Offset(transactTime),
	    app ? app->tzOffset() : 0));
    add(new MxIDCol("venue", Offset(venue)));
    add(new MxIDCol("segment", Offset(segment)));
    add(new MxIDStrCol("id", Offset(id)));
    add(new MxNDPCol("pxNDP", Offset(pxNDP)));
    int qtyNDP = Offset(qtyNDP);
    add(new MxNDPCol("qtyNDP", qtyNDP));
    add(new MxUIntCol("legs", Offset(legs)));
    for (unsigned i = 0; i < MxMDNLegs; i++) {
      ZuStringN<32> instrVenue = "instrVenue"; instrVenue << ZuBoxed(i);
      ZuStringN<32> instrSegment = "instrSegment"; instrSegment << ZuBoxed(i);
      ZuStringN<32> instrument = "instrument"; instrument << ZuBoxed(i);
      ZuStringN<32> side = "side"; side << ZuBoxed(i);
      ZuStringN<32> ratio = "ratio"; ratio << ZuBoxed(i);
      add(new MxIDCol(instrVenue, Offset(instrVenues) + (i * sizeof(MxID))));
      add(new MxIDCol(instrSegment, Offset(instrSegments) + (i * sizeof(MxID))));
      add(new MxIDStrCol(instrument,
	    Offset(instruments) + (i * sizeof(MxIDString))));
      add(new MxEnumCol<MxSide::CSVMap>(side,
	    Offset(sides) + (i * sizeof(MxEnum))));
      add(new MxRatioCol(ratio,
	    Offset(ratios) + (i * sizeof(MxRatio))));
    }
    add(new MxIDStrCol("tickSizeTbl", Offset(tickSizeTbl)));
#undef Offset
#define Offset(x) offsetof(Data, lotSizes) + offsetof(MxMDLotSizes, x)
    this->addValCol(app, "oddLotSize", Offset(oddLotSize), qtyNDP);
    this->addValCol(app, "lotSize", Offset(lotSize), qtyNDP);
    this->addValCol(app, "blockLotSize", Offset(blockLotSize), qtyNDP);
#undef Offset
  }

  void alloc(ZuRef<ZuAnyPOD> &pod) { pod = m_pod; }

  template <typename File>
  void read(const File &file, ZvCSVReadFn fn) {
    ZvCSV::readFile(file,
	ZvCSVAllocFn::Member<&MxMDOrderBookCSV::alloc>::fn(this), fn);
  }

  ZuAnyPOD *row(const MxMDStream::Msg *msg) {
    Data *data = ptr();

    {
      using namespace MxMDStream;
      const Hdr &hdr = msg->hdr();
      switch ((int)hdr.type) {
	case Type::AddOrderBook:
	  {
	    const AddOrderBook &obj = hdr.as<AddOrderBook>();
	    new (data) Data{hdr.shard, hdr.type, obj.transactTime,
	      obj.key.venue, obj.key.segment, obj.key.id,
	      {}, obj.qtyNDP, 1, obj.tickSizeTbl, obj.lotSizes,
	      { obj.instrument.venue },
	      { obj.instrument.segment },
	      { obj.instrument.id },
	      {}, {} };
	  }
	  break;
	case Type::DelOrderBook:
	  {
	    const DelOrderBook &obj = hdr.as<DelOrderBook>();
	    new (data) Data{hdr.shard, hdr.type, obj.transactTime,
	      obj.key.venue, obj.key.segment, obj.key.id};
	  }
	  break;
	case Type::AddCombination:
	  {
	    const AddCombination &obj = hdr.as<AddCombination>();
	    new (data) Data{hdr.shard, hdr.type, obj.transactTime,
	      obj.key.venue, obj.key.segment, obj.key.id,
	      obj.pxNDP, obj.qtyNDP, obj.legs, obj.tickSizeTbl, obj.lotSizes,
	      {}, {}, {}, {}, {} };
	    for (unsigned i = 0, n = obj.legs; i < n; i++) {
	      data->instrVenues[i] = obj.instruments[i].venue;
	      data->instrSegments[i] = obj.instruments[i].segment;
	      data->instruments[i] = obj.instruments[i].id;
	      data->sides[i] = obj.sides[i];
	      data->ratios[i] = obj.ratios[i];
	    }
	  }
	  break;
	case Type::DelCombination:
	  {
	    const DelCombination &obj = hdr.as<DelCombination>();
	    new (data) Data{hdr.shard, hdr.type, obj.transactTime,
	      obj.key.venue, obj.key.segment, obj.key.id };
	  }
	  break;
	case Type::UpdateOrderBook:
	  {
	    const UpdateOrderBook &obj = hdr.as<UpdateOrderBook>();
	    new (data) Data{hdr.shard, hdr.type, obj.transactTime,
	      obj.key.venue, obj.key.segment, obj.key.id,
	      {}, {}, MxUInt(), obj.tickSizeTbl, obj.lotSizes,
	      {}, {}, {}, {}, {} };
	  }
	  break;
	default:
	  return 0;
      }
    }

    return pod();
  }

  ZuInline POD *pod() { return m_pod.ptr(); }
  ZuInline Data *ptr() { return m_pod->ptr(); }

private:
  ZuRef<POD>	m_pod;
};

#endif /* MxMDCSV_HH */
