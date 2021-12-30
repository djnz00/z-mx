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

// simple fixed-size bitmap

#ifndef ZuBitmap_HPP
#define ZuBitmap_HPP

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

template <unsigned Bits_> class ZuBitmap {
public:
  enum { Bits = ((Bits_ + 63) & ~63) };
  enum { Bytes = (Bits>>3) };
  enum { Shift = 6 };
  enum { Mask = ((1U<<Shift) - 1) };
  enum { Words = (Bits>>Shift) };

  ZuBitmap() { zero(); }
  ZuBitmap(const ZuBitmap &b) { memcpy(data, b.data, Bytes); }
  ZuBitmap &operator =(const ZuBitmap &b) {
    if (ZuLikely(this != &b)) memcpy(data, b.data, Bytes);
    return *this;
  }
  ZuBitmap(ZuBitmap &&b) = default;
  ZuBitmap &operator =(ZuBitmap &&b) = default;

  ZuBitmap(ZuString s) { zero(); scan(s); }

  ZuBitmap &zero() { memset(data, 0, Bytes); return *this; }
  ZuBitmap &fill() { memset(data, 0xff, Bytes); return *this; }

  bool get(unsigned i) const {
    return data[i>>Shift] & ((uint64_t)1)<<(i & Mask);
  }
  ZuBitmap &set(unsigned i) {
    data[i>>Shift] |= ((uint64_t)1)<<(i & Mask);
    return *this;
  }
  ZuBitmap &clr(unsigned i) {
    data[i>>Shift] &= ~(((uint64_t)1)<<(i & Mask));
    return *this;
  }

  struct Bit {
    ZuBitmap	&bitmap;
    unsigned	i;
    operator bool() const { return bitmap.get(i); }
    ZuOpBool
    void set() { bitmap.set(i); }
    void clr() { bitmap.clr(i); }
    Bit &operator =(bool v) { v ? set() : clr(); return *this; }
  };
  const Bit operator [](unsigned i) const {
    return {*const_cast<ZuBitmap *>(this), i};
  }
  Bit operator [](unsigned i) {
    return {*this, i};
  }

  template <unsigned I> struct Index { enum { OK = I < Words }; };
  template <typename Fn, unsigned I>
  static ZuIfT<Index<I>::OK>
  opFn(uint64_t *v1) {
    Fn::fn(v1[I]);
    opFn<Fn, I + 1>(v1);
  }
  template <typename Fn, unsigned I>
  static ZuIfT<Index<I>::OK>
  opFn(uint64_t *v1, const uint64_t *v2) {
    Fn::fn(v1[I], v2[I]);
    opFn<Fn, I + 1>(v1, v2);
  }
  template <typename, unsigned I>
  static ZuIfT<!Index<I>::OK>
  opFn(uint64_t *) { }
  template <typename, unsigned I>
  static ZuIfT<!Index<I>::OK>
  opFn(uint64_t *, const uint64_t *) { }

  struct Not {
    static void fn(uint64_t &v1) { v1 = ~v1; }
  };
  struct Or {
    static void fn(uint64_t &v1, const uint64_t &v2) { v1 |= v2; }
  };
  struct And {
    static void fn(uint64_t &v1, const uint64_t &v2) { v1 &= v2; }
  };
  struct Xor {
    static void fn(uint64_t &v1, const uint64_t &v2) { v1 ^= v2; }
  };

  void flip() { opFn<Not, 0>(data); }

  ZuBitmap &operator |=(const ZuBitmap &b) {
    opFn<Or, 0>(data, b.data);
    return *this;
  }
  ZuBitmap &operator &=(const ZuBitmap &b) {
    opFn<And, 0>(data, b.data);
    return *this;
  }
  ZuBitmap &operator ^=(const ZuBitmap &b) {
    opFn<Xor, 0>(data, b.data);
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
    for (unsigned i = 0; i < Words; i++)
      if (data[i]) return false;
    return true;
  }

  int first() const {
    for (unsigned i = 0; i < Words; i++)
      if (uint64_t w = data[i])
	return (i<<Shift) + __builtin_ctzll(w);
    return -1;
  }
  int last() const {
    for (int i = Words; --i >= 0; )
      if (uint64_t w = data[i])
	return (i<<Shift) + (63 - __builtin_clzll(w));
    return -1;
  }
  int next(int i) const {
    if (ZuUnlikely(i == -1)) return first();
    do {
      if (++i >= Bits) return -1;
    } while (!get(i));
    return i;
  }
  int prev(int i) const {
    if (ZuUnlikely(i == -1)) return last();
    do {
      if (--i < 0) return -1;
    } while (!(*this && i));
    return i;
  }

  unsigned scan(ZuString s) {
    const char *data = s.data();
    unsigned length = s.length(), offset = 0;
    if (!length) return 0;
    ZuBox<int> begin, end;
    int j;
    while (offset < length) {
      if (data[offset] == ',') { ++offset; continue; }
      if ((j = begin.scan(data + offset, length - offset)) <= 0) break;
      offset += j;
      if (offset < length && data[offset] == '-') {
	if ((j = end.scan(data + offset + 1, length - offset - 1)) > 0) {
	  ++end;
	  offset += j + 1;
	} else {
	  end = Bits;
	  ++offset;
	}
      } else
	end = begin + 1;
      set(begin, end);
    }
    return offset;
  }
  template <typename S> void print(S &s) const {
    if (!*this) return;
    ZuBox<int> begin = first();
    bool first = true;
    while (begin >= 0) {
      if (!first)
	s << ',';
      else
	first = false;
      ZuBox<int> end = begin, next;
      while ((next = this->next(end)) == end + 1) end = next;
      if (end == begin)
	s << begin;
      else if (end == Bits - 1)
	s << begin << '-';
      else
	s << begin << '-' << end;
      begin = next;
    }
  }

  struct Traits : public ZuBaseTraits<ZuBitmap> { enum { IsComparable = 1 }; };
  friend Traits ZuTraitsType(ZuBitmap *);

  friend ZuPrintFn ZuPrintType(ZuBitmap *);

  uint64_t	data[Words];
};

#endif /* ZuBitmap_HPP */
