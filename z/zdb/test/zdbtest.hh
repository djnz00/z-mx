//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

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
  uint64_t		orderID;
  ZuStringN<32>		link;
  ZuStringN<32>		clOrdID;
  uint64_t		seqNo;
  int			side;
  int			price;
  int			quantity;

  friend ZtFieldPrint ZuPrintType(Order *);
};

ZfbFields(Order,
  (((symbol), (0)), (String), (Ctor<0>)),
  (((orderID), (0)), (UInt), (Ctor<1>)),
  (((link), (1, 2)), (String), (Ctor<2>)),
  (((clOrdID), (1)), (String), (Ctor<3>)),
  (((seqNo), (2)), (UInt), (Ctor<4>, Series, Index)),
  (((side)), (Enum, Side::Map), (Ctor<5>)),
  (((price)), (Int), (Ctor<6>)),
  (((quantity)), (Int), (Ctor<7>)));

ZfbRoot(Order);	// bind Order to flatbuffer schema

}

#endif /* zdbtest_HH */
