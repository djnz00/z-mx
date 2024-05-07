//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxT Vocabulary Types

#ifndef MxTTypes_HH
#define MxTTypes_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxTLib_HH
#include <mxt/MxTLib.hh>
#endif

#include <zlib/ZuTuple.hh>

#include <mxbase/MxBase.hh>

namespace MxTMktNoticeType {
  MxEnumerate(
    DFD,
    MktSuspend,
    MktRelease);
}

namespace MxOrdType {
  MxEnumerate(
      Market,
      Limit,
      Stop,
      StopLimit,
      Funari,		// limit to market on close (closing auction)
      MIT,		// market if touched
      Mkt2Limit,	// market during auction, unfilled becomes limit at AP
      Pegged,
      BestLimit,
      StopBL,		// stop best limit
      LIT,		// limit if touched
      BLIT);		// best limit if touched
  MxEnumMapAlias(Map, CSVMap);
  MxEnumMap(FixMap,
      "1", Market,
      "2", Limit,
      "3", Stop,
      "4", StopLimit,
      "I", Funari,
      "J", MIT,
      "K", Mkt2Limit,
      "P", Pegged,
      "U", BestLimit,
      "W", StopBL,
      "X", LIT,
      "Y", BLIT);

  bool isLimit(int ordType) {
    if (ZuUnlikely(ordType < 0 || ordType >= N)) return 0;
    static bool isLimit_[N] = { 0, 1, 0, 1, 1, 0, 0, 1, 1, 1, 1, 1 };
    return isLimit_[ordType];
  }

  bool isMkt(int ordType) { return !isLimit(ordType); }
}

namespace MxTimeInForce {
  MxEnumerate(Normal, IOC, FOK, AtOpen, AtClose, GTC, GTD);
  MxEnumMapAlias(Map, CSVMap);
  MxEnumMap(FixMap,
      "0", Normal,
      "1", GTC,
      "2", AtOpen,
      "3", IOC,
      "4", FOK,
      "6", GTD,
      "7", AtClose);
}

namespace MxPegType { // peg type
  MxEnumerate(
      Last, Mid, Open, Mkt, Primary, VWAP, TrailingStop, AltMid, Short);
  MxEnumMapAlias(Map, CSVMap);
  MxEnumMap(FixMap,
      "1", Last,
      "2", Mid,
      "3", Open,	// base price for continously traded instruments
      "4", Mkt,		// aggressive; buy - best ask; sell - best bid
      "5", Primary,	// passive;    buy - best bid; sell - best ask
      "7", VWAP,
      "8", TrailingStop,// last; buy - ratchets down; sell - ratchets up
      "A", AltMid,	// mid +/- one tick
      "S", Short);	// short-sell limit pegging (e.g. JPX/TSE)
}

namespace MxTQtyType {
  MxEnumerate(Unit, Lot);
  MxEnumMapAlias(Map, CSVMap);
  MxEnumMap(FixMap,
      "0", Unit,
      "1", Lot);
}

namespace MxOrderCapacity {
  MxEnumerate(Agency, Principal, Mixed);
  MxEnumMapAlias(Map, CSVMap);
  MxEnumMap(FixMap,
      "A", Agency,
      "P", Principal,
      "M", Mixed);
}

namespace MxCashMargin {
  MxEnumerate(Cash, MarginOpen, MarginClose);
  MxEnumMapAlias(Map, CSVMap);
  MxEnumMap(FixMap,
      "1", Cash,
      "2", MarginOpen,
      "3", MarginClose);
}

namespace MxTFillLiquidity {
  MxEnumerate(Added, Removed, RoutedOut, Auction);
  MxEnumMapAlias(Map, CSVMap);
  MxEnumMap(FixMap,
      "1", Added,
      "2", Removed,
      "3", RoutedOut,
      "4", Auction);
}

namespace MxTFillCapacity {
  MxEnumerate(Agent, CrossAsAgent, CrossAsPrincipal, Principal);
  MxEnumMapAlias(Map, CSVMap);
  MxEnumMap(FixMap,
      "1", Agent,
      "2", CrossAsAgent,
      "3", CrossAsPrincipal,
      "4", Principal);
}

namespace MxTPosImpact {
  MxEnumerate(
      Open,	// order opens or extends a position
      Close);	// order closes or reduces a position
}

namespace MxTRejReason {
  MxEnumerate(
    UnknownOrder,		// unknown order
    DuplicateOrder,		// duplicate order
    BadList,			// bad list order information
    ModifyPending,		// modify pending (in response to modify)
    CancelPending,		// cancel pending (in response to cancel)
    OrderClosed,		// order closed (in response to modify/cancel)
    PxNotRoundTick,		// price not round tick
    PxOutOfRange,		// price out of range
    QtyNotRoundLot,		// qty not round lot
    QtyOutOfRange,		// qty out of range
    BadSide,			// bad side
    BadOrderType,		// bad order type
    BadTimeInForce,		// bad time in force
    BadPrice,			// bad price (inconsistent with order type)
    BadLocate,			// bad locate
    BadOrderCapacity,		// bad order capacity
    BadCashMargin,		// bad cash margin
    BadExpireTime,		// bad expire time
    BadInstrument,		// bad instrument
    BadMarket,			// bad market (destination)
    BadQtyType,			// bad qty type
    BadNumberOfLegs,		// bad number of legs
    BadMinimumQty,		// bad minimum qty
    BadMaximumFloor,		// bad maximum floor
    BadPegType,			// bad peg type
    BadPegOffset,		// bad peg offset
    BadPegPx,			// bad peg price
    BadTriggerPx,		// bad trigger price
    TriggerPxNotRoundTick,	// trigger price not round tick
    TriggerPxOutOfRange,	// trigger price out of range
    TooManyTriggers,		// too many open trigger orders
    BadCrossType,		// bad cross type
    BadBookingType,		// bad booking type
    BadContraBroker,		// bad contra broker
    BadClient,			// bad client
    BadAccount,			// bad account
    BadInvestorID,		// bad investor ID
    BrokerReject,		// broker-specific reject
    MarketReject,		// market-specific reject
    OSM,			// order state management
    InstrRestricted,		// instrument restricted
    AcctDisabled,		// account disabled
    NoAssets,			// cash trading - insufficient assets/funds
    NoCollateral,		// margining - insufficient collateral
    RiskBreach,			// risk limit breached
    BadComAsset,		// bad commission asset
    NoMktPx			// no market price
    );
  enum { OK = Invalid };	// OK == Invalid == -1
}

namespace MxTCrossType {
  MxEnumerate(
      OnBook,	// crossed on a listed book (on-exchange)
      SI,	// by a systematic internalizer (dark pool / crossing engine)
      Broker,	// by the broker's trading desk (not using an SI)
      MTF,	// by a multilateral trading facility
      OTC,	// buying broker crossed with selling broker "over the counter"
      Direct);	// buyer directly crossed with seller
}

namespace MxBookingType {
  MxEnumerate(Normal, CFD, Swap);
  MxEnumMapAlias(Map, CSVMap);
  MxEnumMap(FixMap,
      "0", Normal,
      "1", CFD,
      "2", Swap);
}

struct MxTTimeFmt {
  static ZtDateFmt::FIX<-9> &fix() {
    return ZmTLS<ZtDateFmt::FIX<-9, Null>, fix>();
  }
  static ZtDateFmt::CSV &csv() {
    return ZmTLS<ZtDateFmt::CSV, csv>();
  }
  // friend ZuPrintFn ZuPrintType(MxTTimeFmt::Null *);
};

struct MxTCSVTimeFmt {
  static ZtDateFmt::CSV &fmt() {
    return ZmTLS<ZtDateFmt::CSV, fmt>();
  }
};

struct MxTBoolFmt {
  MxBool	v;

  template <typename S> void print(S &s) const {
    if (!*v) return;
    s << (v ? '1' : '0');
  }
  friend ZuPrintFn ZuPrintType(MxTBoolFmt *);
};

#endif /* MxTTypes_HH */
