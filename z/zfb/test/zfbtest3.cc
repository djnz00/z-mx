//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <iostream>
#include <string>

#include <zlib/ZuBox.hh>

#include <zlib/ZmDemangle.hh>

#include <zlib/Zfb.hh>
#include <zlib/ZfbField.hh>

#include <flatbuffers/reflection.h>
#include <flatbuffers/reflection_generated.h>

#include "zfbtest3_fbs.h"

namespace zfbtest3 {

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

}

inline void out(const char *s) { std::cout << s << '\n'; }

#define CHECK(x) ((x) ? out("OK  " #x) : out("NOK " #x))

using IOBuilder = Zfb::IOBuilder<>;
using IOBuf = IOBuilder::IOBuf;

int main()
{
  using namespace zfbtest3;

  Order order{"IBM", 0, "FIX0", "order0", 0, Side::Buy, 100, 100};

  {
    IOBuilder fbb;
    fbb.Finish(Zfb::Save::object(fbb, order));
    auto buf = fbb.buf();
    auto fbo = ZfbField::root<Order>(buf->data());
    std::cout << *fbo << '\n';
  }

  using Key = ZuFieldKeyT<Order>;
  auto key = ZuFieldKey(order);

  {
    IOBuilder fbb;
    fbb.Finish(ZfbField::save<Key>(fbb, key));
    auto buf = fbb.buf();
    auto fbo = ZfbField::root<Order>(buf->data());
    std::cout << *fbo << '\n';
  }
}
