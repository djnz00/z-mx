//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

/* test program */

#include <stdio.h>
#include <stdlib.h>

#include <zlib/ZuHash.hh>
#include <zlib/ZuObject.hh>

#include <zlib/ZmRef.hh>
#include <zlib/ZmHash.hh>
#include <zlib/ZuTime.hh>
#include <zlib/ZmThread.hh>
#include <zlib/ZmSingleton.hh>
#include <zlib/ZmSpecific.hh>

struct Order : public ZuObject {
  static unsigned IDAccessor(const Order *o) { return o->id; }
  Order(unsigned id_) : id(id_) { }
  unsigned id;
};

void dump(Order *o)
{
  printf("order ID: %u\n", o->id);
}

auto HeapID() { return "Orders"; }

using Orders =
  ZmHash<ZmRef<Order>,
    ZmHashKey<Order::IDAccessor,
      ZmHashLock<ZmNoLock,
	ZmHashHeapID<HeapID>>>>;

int main(int argc, char **argv)
{
  ZmHeapMgr::init("Orders", 0, ZmHeapConfig{0, 100});
  ZmRef<Orders> orders = new Orders(ZmHashParams().bits(7).loadFactor(1.0));

  printf("node size: %u\n", (unsigned)sizeof(Orders::Node));
  for (unsigned i = 0; i < 100; i++) orders->add(new Order(i));
  ZmRef<Order> o = orders->findVal(0);
  dump(o);
  Orders::Node *n = orders->del(0);
  dump(n->val());
  delete n;
  o = orders->delVal(1);
  dump(o);
}
