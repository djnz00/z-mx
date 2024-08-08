//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// join array

#ifndef ZuJoin_HH
#define ZuJoin_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuTraits.hh>
#include <zlib/ZuString.hh>
#include <zlib/ZuPrint.hh>

template <typename Array>
struct ZuJoin {
  const Array	&array;
  ZuString	delimiter;

  template <typename S> void print(S &s) const {
    auto n = ZuTraits<Array>::length(array);
    for (unsigned i = 0; i < n; i++) {
      if (ZuLikely(i)) s << delimiter;
      s << array[i];
    }
  }
  friend ZuPrintFn ZuPrintType(ZuJoin *);
};
template <typename Array, typename Delimiter>
ZuJoin(const Array &, const Delimiter &) -> ZuJoin<Array>;
template <typename T, typename Delimiter>
ZuJoin(std::initializer_list<T>, const Delimiter &) -> ZuJoin<ZuArray<T>>;

#endif /* ZuJoin_HH */
