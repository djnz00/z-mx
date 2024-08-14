//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// bit stream

#ifndef ZuBitStream_HH
#define ZuBitStream_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuTuple.hh>

class ZuIBitStream {
public:
  ZuIBitStream() = default;
  ZuIBitStream(const ZuIBitStream &) = default;
  ZuIBitStream &operator =(const ZuIBitStream &) = default;
  ZuIBitStream(ZuIBitStream &&) = default;
  ZuIBitStream &operator =(ZuIBitStream &&) = default;

  ZuIBitStream(const uint8_t *start, const uint8_t *end) :
    m_pos{start}, m_end{end} { }

  bool operator !() const { return !m_pos; }
  ZuOpBool

  const uint8_t *pos() const { return m_pos; }
  const uint8_t *end() const { return m_end; }
  unsigned inBits() const { return m_inBits; }

  // save input context
  ZuTuple<const uint8_t *, unsigned> save() const {
    return {m_pos, m_inBits};
  }

  // restore input context
  void load(const ZuTuple<const uint8_t *, unsigned> &saved) {
    m_pos = saved.p<0>();
    m_inBits = saved.p<1>();
  }

  template <unsigned Bits>
  bool avail() {
    return m_pos + ((m_inBits + Bits + 7)>>3) <= m_end;
  }
  bool avail(unsigned bits) {
    return m_pos + ((m_inBits + bits + 7)>>3) <= m_end;
  }

  template <unsigned Bits>
  uint8_t in() {
    uint8_t v;
    if (ZuUnlikely(!m_inBits)) {
      m_inBits = Bits;
      return (*m_pos) & ~(uint8_t(0xff)<<Bits);
    }
    unsigned bits;
    unsigned lbits = 8 - m_inBits;
    if (ZuUnlikely(Bits < lbits)) lbits = Bits;
    v = ((*m_pos)>>m_inBits) & ~(uint8_t(0xff)<<lbits);
    if ((m_inBits += lbits) >= 8) { m_pos++; m_inBits = 0; }
    if (!(bits = Bits - lbits)) return v;
    // m_inBits must be zero here, and bits is < 8
    v |= ((*m_pos) & ~(uint8_t(0xff)<<bits))<<lbits;
    m_inBits = bits;
    return v;
  }
  uint64_t in(unsigned bits) {
    uint64_t v = 0;
    unsigned lbits = 0; // "head" bits
    if (ZuLikely(m_inBits > 0)) {
      lbits = 8 - m_inBits;
      if (ZuUnlikely(bits < lbits)) lbits = bits;
      v = ((*m_pos)>>m_inBits) & ~(uint8_t(0xff)<<lbits);
      if ((m_inBits += lbits) >= 8) { m_pos++; m_inBits = 0; }
      if (!(bits -= lbits)) return v;
      v <<= (64 - lbits);
    }
    switch (bits>>3) {
      case 8: v = uint64_t(*m_pos++)<<56;
      case 7: v = (v>>8) | (uint64_t(*m_pos++)<<56);
      case 6: v = (v>>8) | (uint64_t(*m_pos++)<<56);
      case 5: v = (v>>8) | (uint64_t(*m_pos++)<<56);
      case 4: v = (v>>8) | (uint64_t(*m_pos++)<<56);
      case 3: v = (v>>8) | (uint64_t(*m_pos++)<<56);
      case 2: v = (v>>8) | (uint64_t(*m_pos++)<<56);
      case 1: v = (v>>8) | (uint64_t(*m_pos++)<<56);
    }
    unsigned hbits = bits & 7;
    bits -= hbits;
    v >>= (64 - (bits + lbits));
    if (hbits) {
      m_inBits = hbits;
      v |= ((*m_pos) & ~(uint8_t(0xff)<<hbits))<<(bits + lbits);
    }
    return v;
  }

private:
  const uint8_t	*m_pos = nullptr;
  const uint8_t	*m_end = nullptr;
  unsigned	m_inBits = 0;
};

class ZuOBitStream {
  ZuOBitStream(const ZuOBitStream &) = delete;
  ZuOBitStream &operator =(const ZuOBitStream &) = delete;

public:
  ZuOBitStream(uint8_t *start, uint8_t *end) : m_pos{start}, m_end{end} { }

  ZuOBitStream(ZuOBitStream &&w) :
    m_pos{w.m_pos}, m_end{w.m_end},
    m_outBits{w.m_outBits}
  {
    w.m_pos = nullptr;
    w.m_end = nullptr;
    w.m_outBits = 0;
  }
  ZuOBitStream &operator =(ZuOBitStream &&w) {
    if (ZuLikely(this != &w)) {
      this->~ZuOBitStream(); // nop
      new (this) ZuOBitStream{ZuMv(w)};
    }
    return *this;
  }

  ZuOBitStream(const ZuIBitStream &in, uint8_t *end) :
    m_pos{const_cast<uint8_t *>(in.pos())}, m_end{end}
  {
    m_outBits = in.inBits();
    *m_pos <<= (8 - m_outBits);
  }

  uint8_t *pos() const { return m_pos; }
  uint8_t *end() const { return m_end; }

  template <unsigned Bits>
  void out(uint8_t v) {
    if (ZuUnlikely(m_outBits == 0)) {
      m_outBits = Bits;
      *m_pos = v<<(8 - Bits);
      return;
    }
    unsigned lbits = 8 - m_outBits;
    if (ZuUnlikely(Bits < lbits)) lbits = Bits;
    *m_pos = ((*m_pos)>>lbits) | (v<<(8 - lbits));
    if ((m_outBits += lbits) >= 8) {
      m_pos++;
      m_outBits = 0;
    }
    v >>= lbits;
    if (uint8_t bits = Bits - lbits) {
      m_outBits = bits;
      *m_pos = v<<(8 - bits);
    }
  }
  void out(uint64_t v, unsigned bits) {
    if (ZuLikely(m_outBits > 0)) {
      unsigned lbits = 8 - m_outBits;
      if (ZuUnlikely(bits < lbits)) lbits = bits;
      *m_pos = ((*m_pos)>>lbits) | (v<<(8 - lbits));
      if ((m_outBits += lbits) >= 8) {
	m_pos++;
	m_outBits = 0;
      }
      v >>= lbits;
      if (!(bits -= lbits)) return;
    }
    switch (bits>>3) {
      case 8: *m_pos++ = v; v >>= 8;
      case 7: *m_pos++ = v; v >>= 8;
      case 6: *m_pos++ = v; v >>= 8;
      case 5: *m_pos++ = v; v >>= 8;
      case 4: *m_pos++ = v; v >>= 8;
      case 3: *m_pos++ = v; v >>= 8;
      case 2: *m_pos++ = v; v >>= 8;
      case 1: *m_pos++ = v; v >>= 8;
    }
    bits &= 7;
    if (bits) {
      m_outBits = bits;
      *m_pos = v<<(8 - bits);
    }
  }

  void finish() {
    if (ZuLikely(m_pos < m_end && m_outBits)) {
      (*m_pos) >>= (8 - m_outBits);
      m_pos++;
      m_outBits = 0;
    }
  }

private:
  uint8_t	*m_pos = nullptr;
  uint8_t	*m_end = nullptr;
  unsigned	m_outBits = 0;	// output bits
};

#endif /* ZuBitStream_HH */
