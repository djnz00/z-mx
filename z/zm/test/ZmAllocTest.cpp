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

#include <iostream>

#include <zlib/ZuBox.hpp>

#include <zlib/ZmAlloc.hpp>
#include <zlib/ZmVHeap.hpp>

void test()
{
  auto self = ZmSelf();
  std::cout << "stack: " << ZuBoxPtr(self->stackAddr()).hex() << " +" << ZuBoxed(self->stackSize()).hex() << '\n';
  unsigned n = ZmStackAvail();
  std::cout << "stack available: " << ZuBoxed(n).hex() << '\n';
  auto ptr = ZmAlloc(uint8_t, n/3);
  std::cout << "ZmAlloc(" << ZuBoxed(n/3).hex() << "): " << ZuBoxPtr(ptr.ptr).hex() << '\n';
  std::cout << "stack available: " << ZuBoxed(ZmStackAvail()).hex() << '\n';
  auto ptr2 = ZmAlloc(uint8_t, n);
  std::cout << "ZmAlloc(" << ZuBoxed(n).hex() << "): " << ZuBoxPtr(ptr2.ptr).hex() << '\n';
  std::cout << "stack available: " << ZuBoxed(ZmStackAvail()).hex() << '\n';
  // uint8_t *ptr = static_cast<uint8_t *>(ZmAlloc(n/3));
  // uint8_t *ptr2 = static_cast<uint8_t *>(ZmAlloc(n));
}

int main(int argc, char **argv)
{
  test();
}
