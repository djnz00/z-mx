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

/* test program */

#include <stdio.h>
#include <stdlib.h>

#include <zlib/ZuHash.hpp>
#include <zlib/ZuObject.hpp>

#include <zlib/ZmRef.hpp>
#include <zlib/ZmHash.hpp>
#include <zlib/ZmTime.hpp>
#include <zlib/ZmThread.hpp>
#include <zlib/ZmSingleton.hpp>
#include <zlib/ZmSpecific.hpp>

struct Orders_HeapID {
  static constexpr const char *id() { return "Orders"; }
};

struct Order : public ZuObject {
  struct IDAccessor {
    static unsigned get(const Order *o) { return o->id; }
  };
  Order(unsigned id_) : id(id_) { }
  unsigned id;
};

void dump(Order *o)
{
  printf("order ID: %u\n", o->id);
}

using Orders =
  ZmHash<ZmRef<Order>,
    ZmHashKey<Order::IDAccessor,
      ZmHashObject<ZuNull,
	ZmHashLock<ZmNoLock,
	  ZmHashHeapID<Orders_HeapID> > > > >;

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
