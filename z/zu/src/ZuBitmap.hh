//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// fixed-size bitmap

#ifndef ZuBitmap_HH
#define ZuBitmap_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuInt.hh>
#include <zlib/ZuIterator.hh>
#include <zlib/ZuTraits.hh>
#include <zlib/ZuString.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuBox.hh>
#include <zlib/ZuUnroll.hh>

namespace ZuBitmap_ {

template <typename Bitmap_, typename Bit_>
class Iterator : public ZuIterator<Iterator<Bitmap_, Bit_>, Bitmap_, Bit_> {
  using Base = ZuIterator<Iterator<Bitmap_, Bit_>, Bitmap_, Bit_>;
public:
  using Bitmap = Bitmap_;
  using Bit = Bit_;
  using Base::Base;
  using Base::operator =;
  using Base::container;
  using Base::i;

  Bit operator *() const { return container[i]; }
};

template <typename Bitmap_>
class Bit {
public:
  using Bitmap = Bitmap_;

  Bit() = delete;
  Bit(Bitmap &bitmap_, unsigned i_) : bitmap{bitmap_}, i{i_} { }
  Bit(const Bit &) = default;
  Bit &operator =(const Bit &) = default;
  Bit(Bit &&) = default;
  Bit &operator =(Bit &&) = default;

  bool get() const;
  void set();
  void clr();

  operator bool() const { return get(); }

  Bit &operator =(bool v) { v ? set() : clr(); return *this; }

  bool equals(const Bit &r) const { return get() == r.get(); }
  int cmp(const Bit &r) const { return ZuCmp<bool>::cmp(get(), r.get()); }
  friend inline bool
  operator ==(const Bit &l, const Bit &r) { return l.equals(r); }
  friend inline int
  operator <=>(const Bit &l, const Bit &r) { return l.cmp(r); }

  bool operator !() const { return !get(); }

  // traits
  struct Traits : public ZuTraits<bool> {
    enum { IsPrimitive = 0, IsPOD = 0 };
    using Elem = Bit;
  };
  friend Traits ZuTraitsType(Bit *);

  // underlying type
  friend bool ZuUnderType(Bit *);

private:
  Bitmap	&bitmap;
  unsigned	i;
};

template <typename Bitmap_>
struct PrintScan {
  using Bitmap = Bitmap_;

  Bitmap *impl() { return static_cast<Bitmap *>(this); }
  const Bitmap *impl() const { return static_cast<const Bitmap *>(this); }

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
	  end = impl()->length();
	  ++offset;
	}
      } else
	end = begin + 1;
      impl()->set(begin, end);
    }
    return offset;
  }
  template <typename S> void print(S &s) const {
    if (!*impl()) return;
    ZuBox<int> begin = impl()->first();
    bool first = true;
    while (begin >= 0) {
      if (!first)
	s << ',';
      else
	first = false;
      ZuBox<int> end = begin, next;
      while ((next = impl()->next(end)) == end + 1) end = next;
      if (end == begin)
	s << begin;
      else if (end == impl()->length() - 1)
	s << begin << '-';
      else
	s << begin << '-' << end;
      begin = next;
    }
  }
};

template <unsigned Bits_>
class Bitmap : public PrintScan<Bitmap<Bits_>> {
public:
  using Bit = ZuBitmap_::Bit<Bitmap>;
  using PrintScan<Bitmap>::scan;
  using PrintScan<Bitmap>::print;

  enum { Bits = ((Bits_ + 63) & ~63) };
  enum { Bytes = (Bits>>3) };
  enum { Shift = 6 };
  enum { Mask = ((1U<<Shift) - 1) };
  enum { Words = (Bits>>Shift) };
  enum { Unroll = 8 };	// unroll small loops where N <= Unroll

  Bitmap() { zero(); }
  Bitmap(const Bitmap &b) { memcpy(data, b.data, Bytes); }
  Bitmap &operator =(const Bitmap &b) {
    if (ZuLikely(this != &b)) memcpy(data, b.data, Bytes);
    return *this;
  }
  Bitmap(Bitmap &&b) = default;
  Bitmap &operator =(Bitmap &&b) = default;

  Bitmap(ZuString s) { zero(); scan(s); }

  constexpr static unsigned length() { return Bits; }

  Bitmap &zero() { memset(data, 0, Bytes); return *this; }
  Bitmap &fill() { memset(data, 0xff, Bytes); return *this; }

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
    if constexpr (Words <= Unroll)
      ZuUnroll::all<Words>([this](auto i) {
	data[i] = ~data[i];
      });
    else
      for (unsigned i = 0; i < Words; i++) data[i] = ~data[i];
  }

  Bitmap &operator |=(const Bitmap &b) {
    if constexpr (Words <= Unroll)
      ZuUnroll::all<Words>([this, &b](auto i) {
	data[i] |= b.data[i];
      });
    else
      for (unsigned i = 0; i < Words; i++) data[i] |= b.data[i];
    return *this;
  }
  Bitmap &operator &=(const Bitmap &b) {
    if constexpr (Words <= Unroll)
      ZuUnroll::all<Words>([this, &b](auto i) {
	data[i] &= b.data[i];
      });
    else
      for (unsigned i = 0; i < Words; i++) data[i] &= b.data[i];
    return *this;
  }
  Bitmap &operator ^=(const Bitmap &b) {
    if constexpr (Words <= Unroll)
      ZuUnroll::all<Words>([this, &b](auto i) {
	data[i] ^= b.data[i];
      });
    else
      for (unsigned i = 0; i < Words; i++) data[i] ^= b.data[i];
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
	return (i<<Shift) + ZuIntrin::ctz(w);
    return -1;
  }
  int last() const {
    for (int i = Words; --i >= 0; )
      if (uint64_t w = data[i])
	return (i<<Shift) + (63 - ZuIntrin::clz(w));
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
    } while (!get(i));
    return i;
  }

  friend ZuPrintFn ZuPrintType(Bitmap *);

  using iterator = Iterator<Bitmap, Bit>;
  using const_iterator = Iterator<const Bitmap, const Bit>;
  const_iterator begin() const { return const_iterator{*this, 0}; }
  const_iterator end() const { return const_iterator{*this, Bits}; }
  const_iterator cbegin() const { return const_iterator{*this, 0}; }
  const_iterator cend() const { return const_iterator{*this, Bits}; }
  iterator begin() { return iterator{*this, 0}; }
  iterator end() { return iterator{*this, Bits}; }

  uint64_t	data[Words];
};

template <typename Bitmap>
inline bool Bit<Bitmap>::get() const { return bitmap.get(i); }

template <typename Bitmap>
inline void Bit<Bitmap>::set() { bitmap.set(i); }

template <typename Bitmap>
inline void Bit<Bitmap>::clr() { bitmap.clr(i); }

} // ZuBitmap_

template <unsigned Bits> using ZuBitmap = ZuBitmap_::Bitmap<Bits>;

#endif /* ZuBitmap_HH */
