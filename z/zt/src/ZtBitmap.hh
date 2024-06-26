//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// variable-size heap-allocated bitmap
// - heap-allocated counterpart to ZuBitmap

#ifndef ZtBitmap_HH
#define ZtBitmap_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
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

class Bitmap : public ZuBitmap_::PrintScan<Bitmap> {
public:
  using Bit = ZuBitmap_::Bit<Bitmap>; // re-use Bit from ZuBitmap

  static constexpr unsigned bits(n) { return (n + 63) & ~63; }
  static constexpr unsigned bytes(unsigned n) { return n>>3; }
  enum { Shift = 6 };
  static constexpr unsigned words(unsigned n) { return n>>Shift; }
  enum { Mask = ((1U<<Shift) - 1) };

  Bitmap() { zero(); }
  Bitmap(const Bitmap &b) = default;
  Bitmap &operator =(const Bitmap &b) = default;
  Bitmap(Bitmap &&b) = default;
  Bitmap &operator =(Bitmap &&b) = default;

  Bitmap(unsigned n) : data{words(n)} {
    data.length(words(n));
    zero();
  }
  Bitmap(unsigned n, ZuString s) : data{words(n)} {
    data.length(words(n));
    zero();
    scan(s);
  }

  unsigned length() const { return data.length()<<Shift; }

  Bitmap &zero() {
    memset(&data[0], 0, data.length()<<(Shift - 3));
    return *this;
  }
  Bitmap &fill() {
    memset(&data[0], 0xff, data.length()<<(Shift - 3));
    return *this;
  }

  bool get(unsigned i) const {
    return data[i>>Shift] & (static_cast<uint64_t>(1)<<(i & Mask));
  }
  Bitmap &set(unsigned i) {
    data[i>>Shift] |= (static_cast<uint64_t>(1)<<(i & Mask));
    return *this;
  }
  Bitmap &clr(unsigned i) {
    data[i>>Shift] &= ~(static_cast<uint64_t>(1)<<(i & Mask));
    return *this;
  }

  const Bit operator [](unsigned i) const {
    return {const_cast<Bitmap &>(*this), i};
  }
  Bit operator [](unsigned i) { return {*this, i}; }

  static void notFn(uint64_t &v1) { v1 = ~v1; }
  static void orFn(uint64_t &v1, const uint64_t v2) { v1 |= v2; }
  static void andFn(uint64_t &v1, const uint64_t v2) { v1 &= v2; }
  static void xorFn(uint64_t &v1, const uint64_t v2) { v1 ^= v2; }

  void flip() {
    unsigned n = data.length();
    for (unsigned i = 0; i < n; i++) data[i] = ~data[i];
  }

  Bitmap &operator |=(const Bitmap &b) {
    unsigned n = data.length();
    for (unsigned i = 0; i < n; i++) data[i] |= b.data[i];
    return *this;
  }
  Bitmap &operator &=(const Bitmap &b) {
    unsigned n = data.length();
    for (unsigned i = 0; i < n; i++) data[i] &= b.data[i];
    return *this;
  }
  Bitmap &operator ^=(const Bitmap &b) {
    unsigned n = data.length();
    for (unsigned i = 0; i < n; i++) data[i] ^= b.data[i];
    return *this;
  }

  void set(unsigned begin, unsigned end) {
    if (begin >= end) return;
    {
      unsigned i = (begin>>Shift);
      uint64_t mask = ~static_cast<uint64_t>(0);
      if (i == (end>>Shift)) mask >>= (64 - (end - begin));
      if (uint64_t begin_ = (begin & Mask)) {
	mask <<= begin_;
	begin -= begin_;
      }
      data[i] |= mask;
      begin += 64;
    }
    {
      unsigned i = (begin>>Shift);
      unsigned j = (end>>Shift);
      if (i < j) {
	memset(&data[i], 0xff, (j - i)<<(Shift - 3));
	begin = end & ~Mask;
      }
    }
    if (begin < end) {
      uint64_t mask = (~static_cast<uint64_t>(0))>>(63 - (end - begin));
      data[begin>>Shift] |= mask;
    }
  }
  void clr(unsigned begin, unsigned end) {
    if (begin >= end) return;
    {
      unsigned i = (begin>>Shift);
      uint64_t mask = ~static_cast<uint64_t>(0);
      if (i == (end>>Shift)) mask >>= (64 - (end - begin));
      if (uint64_t begin_ = (begin & Mask)) {
	mask <<= begin_;
	begin -= begin_;
      }
      data[i] &= ~mask;
      begin += 64;
    }
    {
      unsigned i = (begin>>Shift);
      unsigned j = (end>>Shift);
      if (i < j) {
	memset(&data[i], 0, (j - i)<<(Shift - 3));
	begin = end & ~Mask;
      }
    }
    if (begin < end) {
      uint64_t mask = (~static_cast<uint64_t>(0))>>(63 - (end - begin));
      data[begin>>Shift] &= ~mask;
    }
  }

  bool operator !() const {
    unsigned n = data.length();
    for (unsigned i = 0; i < n; i++)
      if (data[i]) return false;
    return true;
  }

  int first() const {
    unsigned n = data.length();
    for (unsigned i = 0; i < n; i++)
      if (uint64_t w = data[i])
	return (i<<Shift) + ZuIntrin::ctz(w);
    return -1;
  }
  int last() const {
    unsigned n = data.length();
    for (int i = n; --i >= 0; )
      if (uint64_t w = data[i])
	return (i<<Shift) + (63 - ZuIntrin::clz(w));
    return -1;
  }
  int next(int i) const {
    if (ZuUnlikely(i == -1)) return first();
    unsigned n = length();
    do {
      if (++i >= n) return -1;
    } while (!get(i));
    return i;
  }
  int prev(int i) const {
    if (ZuUnlikely(i == -1)) return last();
    do {
      if (--i < 0) return -1;
    } while (!get(i));
    return i;
  }

  friend ZuPrintFn ZuPrintType(Bitmap *);

  using iterator = Iterator<Bitmap, Bit>;
  using const_iterator = Iterator<const Bitmap, const Bit>;
  const_iterator begin() const { return const_iterator{*this, 0}; }
  const_iterator end() const { return const_iterator{*this, length()}; }
  const_iterator cbegin() const { return const_iterator{*this, 0}; }
  const_iterator cend() const { return const_iterator{*this, length()}; }
  iterator begin() { return iterator{*this, 0}; }
  iterator end() { return iterator{*this, length()}; }

  ZtArray<uint64_t>	data;
};

template <typename Bitmap, typename Bit>
inline Bit Iterator<Bitmap, Bit>::operator *() const {
  return m_bitmap[m_i];
}

} // ZtBitmap_

using ZtBitmap = ZtBitmap_::Bitmap;

#endif /* ZtBitmap_HH */

