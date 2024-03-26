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

// hex dump
//
// ZuBytes b{ptr, length};
// std::cout << ZtHexDump_{b} << std::flush;
// std::cout << ZtHexDump{b} << std::flush;
//
// ZtHexDump_ is a low-level hex dumper by reference that does NOT copy
// prefix or data
//
// ZtHexDump takes a copy of the prefix and data - this is used for
// troubleshooting / logging; ZeLog printing is deferred to a later time
// by the logger, which runs in a different thread and stack; both the
// prefix and the data need to reliably remain in scope until dumped

#ifndef ZtHexDump_HPP
#define ZtHexDump_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZtLib_HPP
#include <zlib/ZtLib.hpp>
#endif

#include <zlib/ZtString.hpp>

template <typename T> struct ZtHexDump_Size { enum { N = sizeof(T) }; };
template <> struct ZtHexDump_Size<void> { enum { N = 1 }; };
class ZtAPI ZtHexDump_ {
protected:
  ZtHexDump_() = default;

public:
  template <typename T, typename Cmp>
  ZtHexDump_(ZuArray<T, Cmp> data) :
      m_data{reinterpret_cast<const uint8_t *>(data.data())},
      m_length(data.length() * sizeof(T)) { }

  template <typename T>
  ZtHexDump_(const T *data, unsigned length) :
      m_data{reinterpret_cast<const uint8_t *>(data)},
      m_length{length * ZtHexDump_Size<T>::N} { }

  ZtHexDump_(const ZtHexDump_ &d) : m_data{d.m_data}, m_length{d.m_length} { }
  ZtHexDump_ &operator =(const ZtHexDump_ &d) {
    if (ZuLikely(this != &d)) {
      m_data = d.m_data;
      m_length = d.m_length;
    }
    return *this;
  }

public:
  void print(ZmStream &s) const;
  struct Print : public ZuPrintDelegate {
    template <typename S>
    static void print(S &s_, const ZtHexDump_ &d) {
      ZmStream s{s_};
      d.print(s);
    }
    static void print(ZmStream &s, const ZtHexDump_ &d) {
      d.print(s);
    }
  };
  friend Print ZuPrintType(ZtHexDump_ *);

protected:
  const uint8_t	*m_data = nullptr;
  unsigned	m_length = 0;
};

inline constexpr const char *ZtHexDump_ID() { return "ZtHexDump"; }
class ZtHexDump : private ZmVHeap<ZtHexDump_ID>, public ZtHexDump_ {
  ZtHexDump() = delete;

public:
  template <typename T, typename Cmp>
  ZtHexDump(ZuString prefix, ZuArray<T, Cmp> data) : m_prefix{prefix} {
    m_length = data.length() * sizeof(T);
    init_(reinterpret_cast<const uint8_t *>(data.data()));
  }

  template <typename T>
  ZtHexDump(ZuString prefix, const T *data, unsigned length) :
      m_prefix{prefix} {
    m_length = length * ZtHexDump_Size<T>::N;
    init_(reinterpret_cast<const uint8_t *>(data));
  }

  ~ZtHexDump() {
    if (ZuLikely(m_data)) vfree(m_data);
  }

  ZtHexDump(ZtHexDump &&d) : m_prefix{ZuMv(d.m_prefix)} {
    m_data = d.m_data;
    m_length = d.m_length;
    d.m_prefix = {};
    d.m_data = nullptr;
    d.m_length = 0;
  }
  ZtHexDump &operator =(ZtHexDump &&d) {
    if (ZuLikely(this != &d)) {
      m_prefix = ZuMv(d.m_prefix);
      m_data = d.m_data;
      m_length = d.m_length;
      d.m_prefix = {};
      d.m_data = nullptr;
      d.m_length = 0;
    }
    return *this;
  }
  ZtHexDump(const ZtHexDump &) = delete;
  ZtHexDump &operator =(const ZtHexDump &) = delete;

private:
  void init_(const uint8_t *data_) {
    uint8_t *data;
    if (ZuLikely(data = static_cast<uint8_t *>(valloc(m_length)))) {
      memcpy(data, data_, m_length);
      m_data = data;
    } else {
      m_data = nullptr;
      m_length = 0;
    }
  }

public:
  struct Print : public ZuPrintDelegate {
    template <typename S>
    static void print(S &s, const ZtHexDump &d) {
      s << d.m_prefix << static_cast<const ZtHexDump_ &>(d);
    }
  };
  friend Print ZuPrintType(ZtHexDump *);

private:
  ZtString	m_prefix;
};

#endif /* ZtHexDump_HPP */
