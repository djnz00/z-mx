//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// variable-size heap-allocated bitmap
// - heap-allocated counterpart to ZuBitmap

#ifndef ZtBitmap_HH
#define ZtBitmap_HH

#ifndef ZtLib_HH
#include <zlib/ZtLib.hh>
#endif

#include <zlib/ZuInt.hh>
#include <zlib/ZuIterator.hh>
#include <zlib/ZuTraits.hh>
#include <zlib/ZuString.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuBox.hh>
#include <zlib/ZuBitmap.hh>

#include <zlib/ZtArray.hh>

namespace ZtBitmap_ {

struct Data {
  enum { Fixed = 0 };

  enum { BitShift = 6 };
  enum { ByteShift = 3 };

public:
  Data() = default;
  Data(const Data &b) = default;
  Data &operator =(const Data &b) = default;
  Data(Data &&b) = default;
  Data &operator =(Data &&b) = default;

  Data(unsigned n) {
    n = (n + 63)>>BitShift;
    data = ZtArray<uint64_t>(n);
    data.length(n);
    memset(&data[0], 0, n<<(BitShift - ByteShift));
  }

  /* return length resulting from combining this with another instance
   * - ZtBitmap grows as necessary to match */
  unsigned combine(const Data &b) {
    unsigned l = data.length();
    unsigned r = b.data.length();
    if (l < r) {
      data.length(r);
      memset(&data[l], 0, (r - l)<<(BitShift - ByteShift));
    }
    return r;
  }

  unsigned length() const {
    return data.length()<<BitShift;
  }
  void length(unsigned n) {
    n = (n + 63)>>BitShift;
    unsigned o = data.length();
    if (n <= o) {
      if (n < o) data.length(n);
      return;
    }
    data.length(n);
    memset(&data[o], 0, (n - o)<<(BitShift - ByteShift));
  }

  ZtArray<uint64_t>	data;
};

struct Bitmap : public ZuBitmap_::Bitmap_<Data> {
  using Base = ZuBitmap_::Bitmap_<Data>;
  using Base::Base;
  using Base::operator =;
  template <typename ...Args>
  Bitmap(Args &&...args) : Base{ZuFwd<Args>(args)...} { }
};

} // ZtBitmap_

using ZtBitmap = ZtBitmap_::Bitmap;

#endif /* ZtBitmap_HH */
