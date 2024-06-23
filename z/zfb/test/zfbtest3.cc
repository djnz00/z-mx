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
  (((symbol), (Keys<0>, Ctor<0>)), (String)),
  (((orderID), (Keys<0>, Ctor<1>)), (UInt64)),
  (((link), ((Keys<1, 2>), Ctor<2>)), (String)),
  (((clOrdID), (Keys<1>, Ctor<3>)), (String)),
  (((seqNo), (Keys<2>, Ctor<4>, Series, Index)), (UInt64)),
  (((side), (Ctor<5>, Enum<Side::Map>)), (Int32)),
  (((price), (Ctor<6>)), (Int32)),
  (((quantity), (Ctor<7>)), (Int32)));

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
