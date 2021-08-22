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

// MxMD library

#ifndef MxMDTypes_HPP
#define MxMDTypes_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxMDLib_HPP
#include <mxmd/MxMDLib.hpp>
#endif

#include <mxbase/MxBase.hpp>

#ifndef MxMDNLegs
#define MxMDNLegs 4	// up to MxNLegs legs per order
#endif
#ifndef MxMDNSessions
#define MxMDNSessions 3	// no market has >3 continuous trading sessions per day
#endif

struct MxMDSegment { // venue segment
  MxID		id;
  MxEnum	session;	// MxTradingSession
  MxDateTime	stamp;		// session start time stamp

  ZuInline bool operator !() const { return !id; }
  ZuOpBool
};

ZuDeclTuple(MxMDTickSize,
    (MxValue, minPrice),
    (MxValue, maxPrice),
    (MxValue, tickSize));
struct MxMDTickSize_MinPxAccessor {
  static MxValue get(const MxMDTickSize &t) { return t.minPrice(); }
};

#pragma pack(push, 4)

struct MxMDInstrRefData {	// instrument reference data ("static data")
  MxValue	strike;		// strike (null if not option)
  MxValue	adv;		// average daily volume (often null)
  MxID		baseAsset;	// base asset (often same as symbol)
  MxID		quoteAsset;	// quote asset (currency)
  MxID		underVenue;	// underlying venue (null if no underlying)
  MxID		underSegment;	// underlying segment (can be null)
  MxUInt	mat;		// maturity (null if not future/option)
  MxUInt	outstandingUnits; // (null if not an issued security/asset)
  MxIDString	symbol;		// symbol
  MxIDString	altSymbol;	// alternative symbol
  MxIDString	underlying;	// underlying ID (null if no underlying)
  MxNDP		pxNDP;		// price NDP
  MxNDP		qtyNDP;		// qty NDP
  MxBool	tradeable;	// usually true, is false e.g. for an index
  MxEnum	idSrc;		// symbol ID source
  MxEnum	altIDSrc;	// altSymbol ID source
  MxEnum	putCall;	// put/call (null if not option)
};

// Note: mat is, by industry convention, in YYYYMMDD format
//
// the mat field is NOT to be used for time-to-maturity calculations; it
// is for instrument identification only
//
// DD is normally 00 since listed derivatives maturities/expiries are
// normally uniquely identified by the month; the actual day varies
// and is not required for instrument identification

struct MxMDLotSizes {
  MxValue	oddLotSize;
  MxValue	lotSize;
  MxValue	blockLotSize;

  ZuInline bool operator !() const { return !*lotSize; }
  ZuOpBool

  template <typename S> void print(S &s) const {
    s << '[' << oddLotSize << ", " << lotSize << ", " << blockLotSize << ']';
  }
};

template <> struct ZuPrint<MxMDLotSizes> : public ZuPrintFn { };

struct MxMDL1Data {
  MxDateTime	stamp;
  // Note: all px/qty are integers scaled by 10^ndp
  MxValue	base;			// aka adjusted previous day's close
  MxValue	open[MxMDNSessions];	// [0] is open of first session
  MxValue	close[MxMDNSessions];	// [0] is close of first session
  MxValue	last;
  MxValue	lastQty;
  MxValue	bid;			// best bid
  MxValue	bidQty;
  MxValue	ask;			// best ask
  MxValue	askQty;
  MxValue	high;
  MxValue	low;
  MxValue	accVol;
  MxValue	accVolQty;	// VWAP = accVol / accVolQty
  MxValue	match;		// auction - indicative match/IAP/equilibrium
  MxValue	matchQty;	// auction - indicative match volume
  MxValue	surplusQty;	// auction - surplus volume
  MxFlags	flags;
  MxNDP		pxNDP;			// price NDP
  MxNDP		qtyNDP;			// qty NDP
  MxEnum	status;			// MxTradingStatus
  MxEnum	tickDir;		// MxTickDir
};

typedef MxString<12> MxMDFlagsStr;

#pragma pack(pop)

// venue order ID scope

namespace MxMDOrderIDScope {
  MxEnumValues("MxMDOrderIDScope", Venue, OrderBook, OBSide);
  enum { Default = Venue };
  using CSVMap = Map;
}

// venue flags

namespace MxMDVenueFlags {
  MxEnumValues("MxMDVenueFlags_",
      UniformRanks,		// order ranks are uniformly distributed
      Dark,			// lit if not dark
      Synthetic);		// synthetic (aggregated from input venues)
  MxEnumFlags("MxMDVenueFlags", Flags,
      "UniformRanks", UniformRanks,
      "Dark", Dark,
      "Synthetic", Synthetic);
}

#endif /* MxMDTypes_HPP */
