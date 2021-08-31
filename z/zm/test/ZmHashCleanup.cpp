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

#include <zlib/ZmHash.hpp>

struct Object : public ZmObject {
  Object(int val) : m_val(val) { }
  int hash() const { return m_val; }
  int cmp(const Object &i) const { return ZuCmp<int>::cmp(m_val, i.m_val); }
  bool operator !() const { return !m_val; }

  int m_val;
};

inline bool operator ==(const Object &l, const Object &r) {
  return l.m_val == r.m_val;
}

using ObjectHash = ZmHash<ZmRef<Object> >;

int main(int argc, char *argv[])
{
  ZmRef<ObjectHash> hash =
    new ObjectHash(ZmHashParams().bits(4).loadFactor(2));

  for (int i = 0; i < 32; ++i)
    hash->add(ZmRef<Object>(new Object(i)));

  ObjectHash::Iterator it(*hash);

  while (it.iterateKey()) it.del();

  hash = 0;

  return 0;
}
