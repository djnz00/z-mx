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
  int			side;
  int			price;
  int			quantity;

  friend ZtFieldPrint ZuPrintType(Order *);
};

ZfbFields(Order,
  (((symbol), (Keys<0>, Ctor<0>)), (String)),
  (((orderID), (Keys<0>, Ctor<1>, Update)), (UInt64)),
  (((link), ((Keys<1, 2>), Ctor<2>)), (String)),
  (((clOrdID), (Keys<1>, Ctor<3>, Update)), (String)),
  (((seqNo), (Keys<2>, Ctor<4>, Series, Index, Update)), (UInt64)),
  (((side), (Ctor<5>)), (Enum, Side::Map)),
  (((price), (Ctor<6>, Update)), (Int32)),
  (((quantity), (Ctor<7>, Update)), (Int32)));

ZfbRoot(Order);	// bind Order to flatbuffer schema

}

#endif /* zdbtest_HH */
