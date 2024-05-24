//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxBase position keeping

#ifndef MxPosition_HH
#define MxPosition_HH

#ifndef MxBaseLib_HH
#include <mxbase/MxBaseLib.hh>
#endif

#include <mxbase/MxBase.hh>

// balance (deposited assets, loaned assets, traded/confirmed assets)
struct MxBalance {
  MxValue	deposited;	// deposited
  MxValue	loanAvail;	// loan available
  MxValue	loaned;		// loan used (should be <= loanAvail)
  MxValue	confirmed;	// confirmed / settled
  MxValue	traded;		// traded / realized
  MxValue	marginTraded;	// traded / realized on margin
  MxValue	marginFunded;	// used to fund margin trading (other assets)
  MxValue	comFunded;	// used to fund trading costs
};

// exposure (open orders, unexpired derivatives)
struct MxExpSide {
  MxValue	open;		// total qty of open/working orders
  MxValue	futures;	// total qty of futures (on this underlying)
  MxValue	options;	// total qty of options (on this underlying)
};

// can be aggregated across instruments/venues
struct MxExpAsset {
  MxExpSide	longExp;
  MxExpSide	shortExp;
};

// exposure for an instrument - i.e. an asset pair
struct MxExposure {
  MxExpAsset	base;
  MxExpAsset	quote;
};

#endif /* MxPosition_HH */
