//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// null type - somewhat equivalent to std::monostate

#ifndef ZuNull_HH
#define ZuNull_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuTraits.hh>

struct ZuNull {
  struct Traits : public ZuBaseTraits<ZuNull> {
    enum { IsEmpty = 1 };
    enum { IsPOD = 1 };
  };
  friend Traits ZuTraitsType(ZuNull *);
};

template <typename T> struct ZuCmp;
template <> struct ZuCmp<ZuNull> {
  static constexpr int cmp(ZuNull, ZuNull) { return 0; }
  static constexpr bool less(ZuNull, ZuNull) { return false; }
  static constexpr bool equals(ZuNull, ZuNull) { return true; }
  static constexpr bool null(ZuNull) { return true; }
  static constexpr ZuNull null() { return {}; }
};

#endif /* ZuNull_HH */
