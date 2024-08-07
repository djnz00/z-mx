//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#ifndef zdbtest_HH
#define zdbtest_HH

#include <zlib/ZuLib.hh>
#include <zlib/ZuStringN.hh>
#include <zlib/ZuInt.hh>

#include <zlib/ZtField.hh>

#include <zlib/Zfb.hh>
#include <zlib/ZfbField.hh>

#include "zdbtest_fbs.h"

namespace zdbtest {

namespace Side {
  ZfbEnumValues(Side, Buy, Sell);
};

struct Order {
  ZuStringN<32>		symbol;
  ZuNBox<uint64_t>	orderID;
  ZuStringN<32>		link;
  ZuStringN<32>		clOrdID;
  ZuNBox<uint64_t>	seqNo;
  int8_t		side;
  ZtArray<int>		prices;
  ZtArray<int>		qtys;
  ZtBitmap		flags;

  friend ZtFieldPrint ZuPrintType(Order *);
};

ZfbFieldTbl(Order,
  (((symbol),	(Ctor<0>, Keys<0>)),				(String)),
  (((orderID),	(Ctor<1>, Keys<0>, Mutable)),			(UInt64)),
  (((link),	(Ctor<2>, (Keys<1, 2>), Group<2>, Descend<2>)),	(String)),
  (((clOrdID),	(Ctor<3>, Keys<1>, Mutable)),			(String)),
  (((seqNo),	(Ctor<4>, Keys<2>, Descend<2>, Mutable)),	(UInt64)),
  (((side),	(Ctor<5>, Enum<Side::Map>)),			(Int8)),
  (((prices),	(Ctor<6>, Mutable)),				(Int32Vec)),
  (((qtys),	(Ctor<7>, Mutable)),				(Int32Vec)),
  (((flags),	(Ctor<8>, Mutable)),	(Bitmap, ZtBitmap{"4,8,16-42"})));

ZfbRoot(Order);	// bind Order to flatbuffer schema

}

#endif /* zdbtest_HH */
