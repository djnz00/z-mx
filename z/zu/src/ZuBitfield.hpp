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

// simple fixed-size bitfield of 1|2|4|8|16|32|64-bit values

#ifndef ZuBitfield_HPP
#define ZuBitfield_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuInt.hpp>
#include <zlib/ZuTraits.hpp>
#include <zlib/ZuString.hpp>
#include <zlib/ZuPrint.hpp>
#include <zlib/ZuBox.hpp>

template <unsigned Width> struct ZuBitfield_;
template <> struct ZuBitfield_<1> {
  constexpr static unsigned word(unsigned i) { return i>>6; }
  constexpr static unsigned shift(unsigned i) { return i & 0x3f; }
  constexpr static uint64_t mask() { return 1; }
};
template <> struct ZuBitfield_<2> {
  constexpr static unsigned word(unsigned i) { return i>>5; }
  constexpr static unsigned shift(unsigned i) { return (i & 0x1f)<<1; }
  constexpr static uint64_t mask() { return 3; }
};
template <> struct ZuBitfield_<4> {
  constexpr static unsigned word(unsigned i) { return i>>4; }
  constexpr static unsigned shift(unsigned i) { return (i & 0xf)<<2; }
  constexpr static uint64_t mask() { return 0xf; }
};
template <> struct ZuBitfield_<8> {
  constexpr static unsigned word(unsigned i) { return i>>3; }
  constexpr static unsigned shift(unsigned i) { return (i & 7)<<3; }
  constexpr static uint64_t mask() { return 0xff; }
};
template <> struct ZuBitfield_<16> {
  constexpr static unsigned word(unsigned i) { return i>>2; }
  constexpr static unsigned shift(unsigned i) { return (i & 3)<<4; }
  constexpr static uint64_t mask() { return 0xffff; }
};
template <> struct ZuBitfield_<32> {
  constexpr static unsigned word(unsigned i) { return i>>1; }
  constexpr static unsigned shift(unsigned i) { return (i & 1)<<5; }
  constexpr static uint64_t mask() { return 0xffffffffULL; }
};
template <> struct ZuBitfield_<64> {
  constexpr static unsigned word(unsigned i) { return i; }
  constexpr static unsigned shift(unsigned i) { return 0; }
  constexpr static uint64_t mask() { return ~static_cast<uint64_t>(0); }
};

template <unsigned N, unsigned Width>
class ZuBitfield : public ZuBitfield_<Width> {
  using Base = ZuBitfield_<Width>;
  using Base::word;
  using Base::shift;
  using Base::mask;

  static uint64_t get_(unsigned i, uint64_t w) {
    return (w>>shift(i)) & mask();
  }
  static uint64_t set_(unsigned i, uint64_t w, uint64_t v) {
    auto s = shift(i);
    return (w & ~(mask()<<s)) | (v<<s);
  }

public:
  enum { Words = word(N); };
  enum { Bytes = Words>>3; };

  ZuBitfield() { zero(); }
  ZuBitfield(const ZuBitfield &b) { memcpy(data, b.data, Bytes; }
  ZuBitfield &operator =(const ZuBitfield &b) {
    if (ZuLikely(this != &b)) memcpy(data, b.data, Bytes;
    return *this;
  }
  ZuBitfield(ZuBitfield &&b) = default;
  ZuBitfield &operator =(ZuBitfield &&b) = default;

  ZuBitfield(ZuString s) { zero(); scan(s); }

  ZuBitfield &zero() { memset(data, 0, Bytes; return *this; }
  ZuBitfield &fill() { memset(data, 0xff, Bytes; return *this; }

  uint64_t get(unsigned i) const { return get_(i, data[word(i)]); }
  ZuBitfield &set(unsigned i, uint64_t v) {
    auto j = word(i);
    data[j] = set_(i, data[j], v);
    return *this;
  }

  struct Field {
    ZuBitfield	&bitmap;
    unsigned	i;

    uint64_t get() const { return bitmap.get(i); }
    operator uint64_t() const { return bitmap.get(i); }

    void set(uint64_t v) { bitmap.set(i, v); }
    Field &operator =(uint64_t v) { bitmap.set(i, v); return *this; }

    bool operator !() const { return !bitmap.get(i); }
  };
  const Field operator [](unsigned i) const {
    return {*const_cast<ZuBitfield *>(this), i};
  }
  Field operator [](unsigned i) { return {*this, i}; }

  bool operator !() const {
    for (unsigned i = 0; i < Words; i++)
      if (data[i]) return false;
    return true;
  }

  uint64_t	data[Words];
};

#endif /* ZuBitfield_HPP */
