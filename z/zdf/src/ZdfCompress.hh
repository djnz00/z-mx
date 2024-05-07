//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// 64bit data series compression
// * byte-aligned
// * signed data
// * Huffman-coded length prefix
// * single-byte RLE
// * efficient random-access (seeking) and interpolation searching
// * absolute, delta (first derivative), and delta-of-delta (second derivative)

#ifndef ZdfRW_HH
#define ZdfRW_HH

#ifndef ZdfLib_HH
#include <zlib/ZdfLib.hh>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <ZuInt.hh>
#include <ZuByteSwap.hh>

namespace ZdfCompress {

class Decoder {
public:
  Decoder() : m_pos(nullptr), m_end(nullptr) { }
  Decoder(const Decoder &r) :
      m_pos(r.m_pos), m_end(r.m_end),
      m_prev(r.m_prev), m_rle(r.m_rle), m_count(r.m_count) { }
  Decoder &operator =(const Decoder &r) {
    if (ZuLikely(this != &r)) {
      this->~Decoder(); // nop
      new (this) Decoder{r};
    }
    return *this;
  }
  Decoder(Decoder &&r) :
      m_pos(r.m_pos), m_end(r.m_end),
      m_prev(r.m_prev), m_rle(r.m_rle), m_count(r.m_count) { }
  Decoder &operator =(Decoder &&r) {
    if (ZuLikely(this != &r)) {
      this->~Decoder(); // nop
      new (this) Decoder{ZuMv(r)};
    }
    return *this;
  }
  ~Decoder() = default;

  Decoder(const uint8_t *start, const uint8_t *end) :
      m_pos(start), m_end(end) { }

  bool operator !() const { return !m_pos; }
  ZuOpBool

  const uint8_t *pos() const { return m_pos; }
  const uint8_t *end() const { return m_end; }
  unsigned count() const { return m_count; }

  // seek to a position
  bool seek(unsigned count) {
    while (count) {
      if (m_rle) {
	if (m_rle >= count) {
	  m_count += count;
	  m_rle -= count;
	  return true;
	}
	m_count += m_rle;
	count -= m_rle;
	m_rle = 0;
      } else {
	if (!read_(nullptr)) return false;
	++m_count;
	--count;
      }
    }
    return true;
  }

  // seek to a position, informing upper layer of skipped values
  // l(int64_t value, unsigned count)
  template <typename L>
  bool seek(unsigned count, L l) {
    while (count) {
      if (m_rle) {
	if (m_rle >= count) {
	  l(m_prev, count);
	  m_count += count;
	  m_rle -= count;
	  return true;
	}
	l(m_prev, m_rle);
	m_count += m_rle;
	count -= m_rle;
	m_rle = 0;
      } else {
	int64_t value;
	if (!read_(&value)) return false;
	l(value, 1);
	++m_count;
	--count;
      }
    }
    return true;
  }

  // search for a value
  // l(int64_t value, unsigned count) -> unsigned skipped
  // search ends when skipped < count
  template <typename L>
  bool search(L l) {
    const uint8_t *origPos;
    unsigned count;
    if (m_rle) {
      count = l(m_prev, m_rle);
      m_count += count;
      if (m_rle -= count) return true;
    }
    int64_t value;
    for (;;) {
      origPos = m_pos;
      if (!read_(&value)) return false;
      count = l(value, 1 + m_rle);
      if (!count) {
	m_pos = origPos;
	return true;
      }
      ++m_count;
      --count;
      if (m_rle) {
	m_count += count;
	if (m_rle -= count) return true;
      }
    }
  }

  bool read(int64_t &value) {
    if (m_rle) {
      ++m_count;
      --m_rle;
      value = m_prev;
      return true;
    }
    if (read_(&value)) {
      ++m_count;
      return true;
    }
    return false;
  }

private:
  bool read_(int64_t *value_) {
    if (ZuUnlikely(m_pos >= m_end)) return false;
    unsigned byte = *m_pos;
    if (byte & 0x80) {
      ++m_pos;
      m_rle = byte & 0x7f;
      if (value_) *value_ = m_prev;
      return true;
    }
    int64_t value;
    if (!(byte & 0x20)) {				// 5 bits
      ++m_pos;
      value = byte & 0x1f;
    } else if ((byte & 0x30) == 0x20) {			// 12 bits
      if (m_pos + 2 > m_end) return false;
      ++m_pos;
      value = byte & 0xf;
      value |= static_cast<int64_t>(*m_pos++)<<4;
    } else if ((byte & 0x38) == 0x30) {			// 19 bits
      if (m_pos + 3 > m_end) return false;
      ++m_pos;
      value = byte & 0x7;
      value |= static_cast<int64_t>(*m_pos++)<<3;
      value |= static_cast<int64_t>(*m_pos++)<<11;
    } else if ((byte & 0x3c) == 0x38) {			// 26 bits
      if (m_pos + 4 > m_end) return false;
      ++m_pos;
      value = byte & 0x3;
      value |= static_cast<int64_t>(*m_pos++)<<2;
      value |= static_cast<int64_t>(*m_pos++)<<10;
      value |= static_cast<int64_t>(*m_pos++)<<18;
    } else if ((byte & 0x3e) == 0x3c) {			// 33 bits
      if (m_pos + 5 > m_end) return false;
      ++m_pos;
      value = byte & 0x1;
      value |= static_cast<int64_t>(*m_pos++)<<1;
      value |= static_cast<int64_t>(*m_pos++)<<9;
      value |= static_cast<int64_t>(*m_pos++)<<17;
      value |= static_cast<int64_t>(*m_pos++)<<25;
    } else if ((byte & 0x3f) == 0x3e) {			// 40 bits
      if (m_pos + 6 > m_end) return false;
      ++m_pos;
      value = *m_pos++;
      value |= static_cast<int64_t>(*m_pos++)<<8;
      value |= static_cast<int64_t>(*m_pos++)<<16;
      value |= static_cast<int64_t>(*m_pos++)<<24;
      value |= static_cast<int64_t>(*m_pos++)<<32;
    } else {						// 64 bits
      if (m_pos + 9 > m_end) return false;
      ++m_pos;
#ifdef __x86_64__
      // potentially misaligned
      value = *reinterpret_cast<const ZuLittleEndian<uint64_t> *>(m_pos);
#else
      {
	ZuLittleEndian<uint64_t> value_;
	memcpy(&value_, m_pos, 8);
	value = value_;
      }
#endif
      m_pos += 8;
    }
    if (byte & 0x40) value = ~value;
    if (value_) *value_ = m_prev = value;
    return true;
  }

protected:
  const uint8_t	*m_pos, *m_end;
  int64_t	m_prev = 0;
  unsigned	m_rle = 0;
  unsigned	m_count = 0;
};

class Encoder {
  Encoder(const Encoder &) = delete;
  Encoder &operator =(const Encoder &) = delete;

public:
  Encoder(uint8_t *start, uint8_t *end) : m_pos(start), m_end(end) { }

  Encoder() : m_pos(nullptr), m_end(nullptr) { }
  Encoder(Encoder &&w) :
      m_pos(w.m_pos), m_end(w.m_end),
      m_rle(w.m_rle), m_prev(w.m_prev), m_count(w.m_count) {
    w.m_pos = nullptr;
    w.m_end = nullptr;
    w.m_rle = nullptr;
    w.m_prev = 0;
    w.m_count = 0;
  }
  Encoder &operator =(Encoder &&w) {
    if (ZuLikely(this != &w)) {
      this->~Encoder(); // nop
      new (this) Encoder{ZuMv(w)};
    }
    return *this;
  }
  ~Encoder() = default;

  uint8_t *pos() const { return m_pos; }
  uint8_t *end() const { return m_end; }
  unsigned count() const { return m_count; }

  bool operator !() const { return !m_pos; }
  ZuOpBool

  bool write(int64_t value_) {
    if (ZuLikely(value_ == m_prev)) {
      if (m_rle) {
	if (++*m_rle == 0xff) m_rle = nullptr;
	++m_count;
	return true;
      }
      if (m_pos >= m_end) return false;
      *(m_rle = m_pos++) = 0x80;
      ++m_count;
      return true;
    } else
      m_rle = nullptr;
    unsigned negative = value_ < 0;
    int64_t value = value_;
    if (negative) value = ~value;
    unsigned n = !value ? 0 : 64 - __builtin_clzll(value);
    n = (n + 1) / 7;
    if (n >= 6) n = 8;
    if (m_pos + n >= m_end) return false;
    if (n == 8) n = 6;
    negative <<= 6;
    switch (n) {
      case 0:							// 5 bits
	*m_pos++ = negative | value;
	break;
      case 1:							// 12 bits
	*m_pos++ = negative | 0x20 | (value & 0xf);
	value >>= 4; *m_pos++ = value;
	break;
      case 2:							// 19 bits
	*m_pos++ = negative | 0x30 | (value & 0x7);
	value >>= 3; *m_pos++ = value & 0xff;
	value >>= 8; *m_pos++ = value;
	break;
      case 3:							// 26 bits
	*m_pos++ = negative | 0x38 | (value & 0x3);
	value >>= 2; *m_pos++ = value & 0xff;
	value >>= 8; *m_pos++ = value & 0xff;
	value >>= 8; *m_pos++ = value;
	break;
      case 4:							// 33 bits
	*m_pos++ = negative | 0x3c | (value & 0x1);
	value >>= 1; *m_pos++ = value & 0xff;
	value >>= 8; *m_pos++ = value & 0xff;
	value >>= 8; *m_pos++ = value & 0xff;
	value >>= 8; *m_pos++ = value;
	break;
      case 5:							// 40 bits
	*m_pos++ = negative | 0x3e;
	*m_pos++ = value & 0xff;
	value >>= 8; *m_pos++ = value & 0xff;
	value >>= 8; *m_pos++ = value & 0xff;
	value >>= 8; *m_pos++ = value & 0xff;
	value >>= 8; *m_pos++ = value;
	break;
      case 6:							// 64 bits
	*m_pos++ = negative | 0x3f;
#ifdef __x86_64__
	// potentially misaligned (intentional)
	*reinterpret_cast<ZuLittleEndian<uint64_t> *>(m_pos) = value;
#else
	{
	  ZuLittleEndian<uint64_t> value_ = value;
	  memcpy(m_pos, static_cast<void *>(&value_), 8);
	}
#endif
	m_pos += 8;
	break;
    }
    m_prev = value_;
    ++m_count;
    return true;
  }

  int64_t last() const { return m_prev; }

private:
  uint8_t	*m_pos, *m_end;
  uint8_t	*m_rle = nullptr;
  int64_t	m_prev = 0;
  unsigned	m_count = 0;
};

template <typename Base = Decoder>
class DeltaDecoder : public Base {
public:
  DeltaDecoder() : Base() { }
  DeltaDecoder(const DeltaDecoder &r) : Base(r), m_base(r.m_base) { }
  DeltaDecoder &operator =(const DeltaDecoder &r) {
    if (ZuLikely(this != &r)) {
      this->~DeltaDecoder(); // nop
      new (this) DeltaDecoder{r};
    }
    return *this;
  }
  DeltaDecoder(DeltaDecoder &&r) :
      Base(static_cast<Base &&>(r)), m_base(r.m_base) { }
  DeltaDecoder &operator =(DeltaDecoder &&r) {
    if (ZuLikely(this != &r)) {
      this->~DeltaDecoder(); // nop
      new (this) DeltaDecoder{ZuMv(r)};
    }
    return *this;
  }

  DeltaDecoder(const uint8_t *start, const uint8_t *end) :
      Base(start, end) { }

  bool seek(unsigned count) {
    return Base::seek(count,
	[this](int64_t skip, unsigned count) {
	  m_base += skip * count;
	});
  }

  template <typename L>
  bool seek(unsigned count, L l) {
    return Base::seek(count,
	[this, l = ZuMv(l)](int64_t skip, unsigned count) {
	  for (unsigned i = 0; i < count; i++)
	    l(m_base += skip, 1);
	});
  }

  template <typename L>
  bool search(L l) {
    return Base::search(
	[this, l = ZuMv(l)](int64_t skip, unsigned count) {
	  int64_t value;
	  for (unsigned i = 0; i < count; i++) {
	    value = m_base + skip;
	    if (!l(value, 1)) return i;
	    m_base = value;
	  }
	  return count;
	});
  }

  bool read(int64_t &value_) {
    int64_t value;
    if (ZuUnlikely(!Base::read(value))) return false;
    value_ = (m_base += value);
    return true;
  }

private:
  int64_t		m_base = 0;
};

template <typename Base = Encoder>
class DeltaEncoder : public Base {
  DeltaEncoder(const DeltaEncoder &) = delete;
  DeltaEncoder &operator =(const DeltaEncoder &) = delete;

public:
  DeltaEncoder(uint8_t *start, uint8_t *end) : Base(start, end) { }

  DeltaEncoder() { }
  DeltaEncoder(DeltaEncoder &&w) :
      Base{static_cast<Base &&>(w)}, m_base{w.m_base} {
    w.m_base = 0;
  }
  DeltaEncoder &operator =(DeltaEncoder &&w) {
    if (ZuLikely(this != &w)) {
      this->~DeltaEncoder(); // nop
      new (this) DeltaEncoder{ZuMv(w)};
    }
    return *this;
  }
  ~DeltaEncoder() = default;

  bool write(int64_t value) {
    int64_t delta = value - m_base;
    if (ZuUnlikely(!Base::write(delta))) return false;
    m_base = value;
    return true;
  }

  int64_t last() const { return m_base + Base::last(); }

private:
  int64_t	m_base = 0;
};

} // namespace ZdfCompress

#endif /* ZdfRW_HH */
