//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// join array

#ifndef ZtJoin_HH
#define ZtJoin_HH

#ifndef ZtLib_HH
#include <zlib/ZtLib.hh>
#endif

#include <zlib/ZuTraits.hh>
#include <zlib/ZuString.hh>
#include <zlib/ZuPrint.hh>

template <typename Array>
struct ZtJoin {
  const Array	&array;
  ZuString	delimiter;

  template <typename S> void print(S &s) const {
    auto n = ZuTraits<Array>::length(array);
    for (unsigned i = 0; i < n; i++) {
      if (ZuLikely(i)) s << delimiter;
      s << array[i];
    }
  }
  friend ZuPrintFn ZuPrintType(ZtJoin *);
};
template <typename Array, typename Delimiter>
ZtJoin(const Array &, const Delimiter &) -> ZtJoin<Array>;
template <typename T, typename Delimiter>
ZtJoin(std::initializer_list<T>, const Delimiter &) -> ZtJoin<ZuArray<T>>;

#endif /* ZtJoin_HH */
