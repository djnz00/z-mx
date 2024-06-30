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
#include <zlib/Zu_ntoa.hh>

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

  auto impl() const { return static_cast<const Bitmap *>(this); }
  auto impl() { return static_cast<Bitmap *>(this); }

  static int scanLast(ZuString s) {
    const char *data = s.data();
    unsigned length = s.length(), offset = 0;
    int last = -1;
    ZuBox<unsigned> begin, end;
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
	  end = begin + 1;
	  ++offset;
	}
      } else
	end = begin + 1;
      if (int(end.val()) > last) last = end;
    }
    return last;
  }
  unsigned scan(ZuString s) {
    const char *data = s.data();
    unsigned length = s.length(), offset = 0;
    if (!length) return 0;
    ZuBox<unsigned> begin, end;
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
  unsigned printLen() const {
    using Log = Zu_ntoa::Log10<4>;
    if (!*impl()) return 0;
    unsigned len = 0;
    int begin = impl()->first(), end;
    bool first = true;
    while (begin >= 0) {
      if (!first)
	++len;
      else
	first = false;
      end = begin;
      int next;
      while ((next = impl()->next(end)) == end + 1) end = next;
      if (end == begin)
	len += Log::log(begin);
      else if (end == impl()->length() - 1)
	len += Log::log(begin) + 1;
      else
	len += Log::log(begin) + 1 + Log::log(end);
      begin = next;
    }
    len = (len - Log::log(end)) + Zu_ntoa::Log10_MaxLog<4>::N;
    return len;
  }
  template <typename S> void print(S &s) const {
    if (!*impl()) return;
    int begin = impl()->first();
    bool first = true;
    while (begin >= 0) {
      if (!first)
	s << ',';
      else
	first = false;
      int end = begin, next;
      while ((next = impl()->next(end)) == end + 1) end = next;
      if (end == begin)
	s << ZuBox<unsigned>{begin};
      else if (end == impl()->length() - 1)
	s << ZuBox<unsigned>{begin} << '-';
      else
	s << ZuBox<unsigned>{begin} << '-' << ZuBox<unsigned>{end};
      begin = next;
    }
  }
};

template <typename Data_>
class Bitmap_ : public Data_, public PrintScan<Bitmap_<Data_>> {
public:
  using Bit = ZuBitmap_::Bit<Bitmap_>;

  using Data = Data_;
  using Data::length;
  using Data::data;

  using PrintScan<Bitmap_>::scan;
  using PrintScan<Bitmap_>::print;

  enum { ByteShift = 3 };
  enum { BitShift = 6 };
  enum { Mask = ((1U<<BitShift) - 1) };

  Bitmap_() = default;
  Bitmap_(const Bitmap_ &) = default;
  Bitmap_ &operator =(const Bitmap_ &) = default;
  Bitmap_(Bitmap_ &&) = default;
  Bitmap_ &operator =(Bitmap_ &&) = default;
  ~Bitmap_() = default;

  template <typename _ = Data, decltype(_{ZuDeclVal<unsigned>()}, int()) = 0>
  Bitmap_(unsigned n) : Data{n} { }
  template <typename _ = Data, decltype(ZuIfT<!_::Fixed>(), int()) = 0>
  Bitmap_(ZuString s) : Data{1U + this->scanLast(s)} {
    this->scan(s);
  }
  template <typename _ = Data, decltype(_{ZuDeclVal<unsigned>()}, int()) = 0>
  Bitmap_(ZuString s, unsigned n) : Data{n} {
    this->scan(s);
  }
  template <typename _ = Data, decltype(ZuIfT<_::Fixed>(), int()) = 0>
  Bitmap_(ZuString s) {
    this->scan(s);
  }

  Bitmap_ &zero() { memset(&data[0], 0, length()>>ByteShift); return *this; }
  Bitmap_ &fill() { memset(&data[0], 0xff, length()>>ByteShift); return *this; }

  bool get(unsigned i) const {
    return data[i>>BitShift] & (uint64_t(1)<<(i & Mask));
  }
  Bitmap_ &set(unsigned i) {
    if (ZuLikely(i >= length())) {
      if constexpr (Data::Fixed)
	return *this;
      else
	length(i + 1);
    }
    data[i>>BitShift] |= (uint64_t(1)<<(i & Mask));
    return *this;
  }
  Bitmap_ &clr(unsigned i) {
    if (ZuLikely(i < length()))
      data[i>>BitShift] &= ~(uint64_t(1)<<(i & Mask));
    return *this;
  }

  const Bit operator [](unsigned i) const {
    return {const_cast<Bitmap_ &>(*this), i};
  }
  Bit operator [](unsigned i) { return {*this, i}; }

  void flip() {
    unsigned n = length()>>BitShift;
    for (unsigned i = 0; i < n; i++) data[i] = ~data[i];
  }

  Bitmap_ &operator |=(const Bitmap_ &b) {
    unsigned n = Data::combine(b);
    for (unsigned i = 0; i < n; i++) data[i] |= b.data[i];
    return *this;
  }
  Bitmap_ &operator &=(const Bitmap_ &b) {
    unsigned n = Data::combine(b);
    for (unsigned i = 0; i < n; i++) data[i] &= b.data[i];
    return *this;
  }
  Bitmap_ &operator ^=(const Bitmap_ &b) {
    unsigned n = Data::combine(b);
    for (unsigned i = 0; i < n; i++) data[i] ^= b.data[i];
    return *this;
  }

  Bitmap_ &set(unsigned begin, unsigned end) {
    if (ZuLikely(end > length())) {
      if constexpr (Data::Fixed) {
	end = length();
	if (begin >= end) return *this;
      } else
	length(end);
    } else {
      if (begin >= end) return *this;
    }
    {
      unsigned i = (begin>>BitShift);
      uint64_t mask = ~static_cast<uint64_t>(0);
      if (i == (end>>BitShift)) mask >>= (64 - (end - begin));
      if (uint64_t begin_ = (begin & Mask)) {
	mask <<= begin_;
	begin -= begin_;
      }
      data[i] |= mask;
      begin += 64;
    }
    {
      unsigned i = (begin>>BitShift);
      unsigned j = (end>>BitShift);
      if (i < j) {
	memset(&data[i], 0xff, (j - i)<<(BitShift - ByteShift));
	begin = end & ~Mask;
      }
    }
    if (begin < end) {
      uint64_t mask = (~static_cast<uint64_t>(0))>>(63 - (end - begin));
      data[begin>>BitShift] |= mask;
    }
    return *this;
  }
  Bitmap_ &clr(unsigned begin, unsigned end) {
    if (end > length()) end = length();
    if (begin >= end) return *this;
    {
      unsigned i = (begin>>BitShift);
      uint64_t mask = ~static_cast<uint64_t>(0);
      if (i == (end>>BitShift)) mask >>= (64 - (end - begin));
      if (uint64_t begin_ = (begin & Mask)) {
	mask <<= begin_;
	begin -= begin_;
      }
      data[i] &= ~mask;
      begin += 64;
    }
    {
      unsigned i = (begin>>BitShift);
      unsigned j = (end>>BitShift);
      if (i < j) {
	memset(&data[i], 0, (j - i)<<(BitShift - ByteShift));
	begin = end & ~Mask;
      }
    }
    if (begin < end) {
      uint64_t mask = (~static_cast<uint64_t>(0))>>(63 - (end - begin));
      data[begin>>BitShift] &= ~mask;
    }
    return *this;
  }

// buffer access

  auto buf() { return ZuArray{&data[0], length()>>BitShift}; }
  auto cbuf() const { return ZuArray{&data[0], length()>>BitShift}; }

// comparison

  bool operator !() const {
    unsigned n = length()>>BitShift;
    for (unsigned i = 0; i < n; i++)
      if (data[i]) return false;
    return true;
  }
  ZuOpBool;

protected:
  bool same(const Bitmap_ &r) const { return this == &r; }

public:
  bool equals(const Bitmap_ &r) const {
    return same(r) || cbuf().equals(r.cbuf());
  }
  int cmp(const Bitmap_ &r) const {
    if (same(r)) return 0;
    return cbuf().cmp(r.cbuf());
  }
  friend inline bool
  operator ==(const Bitmap_ &l, const Bitmap_ &r) { return l.equals(r); }
  friend inline int
  operator <=>(const Bitmap_ &l, const Bitmap_ &r) { return l.cmp(r); }

// hash

  uint32_t hash() const { return cbuf().hash(); }

// iteration

  int first() const {
    unsigned n = length()>>BitShift;
    for (unsigned i = 0; i < n; i++)
      if (uint64_t w = data[i])
	return (i<<BitShift) + ZuIntrin::ctz(w);
    return -1;
  }
  int last() const {
    unsigned n = length()>>BitShift;
    for (int i = n; --i >= 0; )
      if (uint64_t w = data[i])
	return (i<<BitShift) + (63 - ZuIntrin::clz(w));
    return -1;
  }
  int next(int i) const {
    unsigned n = length();
    if (ZuUnlikely(i == -1)) return first();
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

  friend ZuPrintFn ZuPrintType(Bitmap_ *);

  using iterator = Iterator<Bitmap_, Bit>;
  using const_iterator = Iterator<const Bitmap_, const Bit>;
  const_iterator begin() const { return const_iterator{*this, 0}; }
  const_iterator end() const { return const_iterator{*this, length()}; }
  const_iterator cbegin() const { return const_iterator{*this, 0}; }
  const_iterator cend() const { return const_iterator{*this, length()}; }
  iterator begin() { return iterator{*this, 0}; }
  iterator end() { return iterator{*this, length()}; }
};

template <unsigned Bits_>
struct Data {
  enum { Fixed = 1 };

  enum { Bits = ((Bits_ + 63) & ~63) };
  enum { Words = (Bits>>6) };

  static constexpr unsigned length() { return Bits; }

private:
  void copy(const Data &b) { memcpy(&data[0], &b.data[0], sizeof(data)); }

public:
  Data() { memset(&data[0], 0, sizeof(data)); }
  Data(const Data &b) { copy(b); }
  Data &operator =(const Data &b) { copy(b); return *this; }
  Data(Data &&b) { copy(b); }
  Data &operator =(Data &&b) { copy(b); return *this; }

  /* return length resulting from combining this with another instance
   * - ZuBitmap<N> always has the same length */
  static constexpr unsigned combine(const Data &) { return Words; }

  uint64_t	data[Words];
};

template <unsigned Bits_>
struct Bitmap : public Bitmap_<Data<Bits_>> {
  using Base = Bitmap_<Data<Bits_>>;
  using Base::Base;
  using Base::operator =;
  template <typename ...Args>
  Bitmap(Args &&...args) : Base{ZuFwd<Args>(args)...} { }
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
