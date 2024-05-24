//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// trailing mean

#ifndef MxValWindow_HH
#define MxValWindow_HH

#ifndef MxBaseLib_HH
#include <mxbase/MxBaseLib.hh>
#endif

#include <zlib/ZtArray.hh>

#include <mxbase/MxBase.hh>

class MxBaseAPI MxValWindow {
public:
  MxValWindow(unsigned size, unsigned interval, unsigned ndp) :
      m_data(size), m_head(0), m_interval(interval), m_offset(0), m_ndp(ndp) {
    m_data.length(size);
  }

  void add(MxValue v, MxValue i) {
    if (ZuUnlikely(i < m_head)) return;
    unsigned length = m_data.length();
    unsigned width = length * m_interval;
    if (ZuLikely(i < m_head + width)) {
  add:
      unsigned j = (i - m_head) / m_interval + m_offset;
      if (j >= length) j -= length;
      m_data[j] += v;
      m_total += v;
      return;
    }
    if (ZuUnlikely(i >= m_head + (width<<1))) {
      if (m_total)
	memset(m_data.data(), 0, m_data.length() * sizeof(MxValue));
      m_head = i - (i % m_interval);
      m_offset = 0;
      m_data[0] = m_total = v;
      return;
    }
    unsigned shift = ((i - m_head) / m_interval - length) + 1;
    for (unsigned j = 0; j < shift; j++) {
      unsigned k = m_offset + j;
      if (k >= length) k -= length;
      m_total -= m_data[k];
      m_data[k] = 0;
    }
    m_offset += shift;
    if (m_offset >= length) m_offset -= length;
    m_head += (shift * m_interval);
    ZmAssert(i < m_head + width);
    goto add;
  }

  ZuInline MxValue total() const { return m_total; }

  MxValue mean() const {
    int64_t width = m_data.length() * m_interval;
    return (MxValNDP{m_total, m_ndp} / MxValNDP{width, 0}).value;
  }

private:
  ZtArray<ZuBox0(int64_t)> m_data;
  MxValue		m_total = 0;
  MxValue		m_head;
  unsigned		m_interval;
  unsigned		m_offset;
  unsigned		m_ndp;
};

#endif /* MxValWindow_HH */
