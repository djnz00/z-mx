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

// Union of a 64bit integer and a human-readable string
// (8-byte left-aligned zero-padded)

#ifndef ZuID_HPP
#define ZuID_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuInt.hpp>
#include <zlib/ZuTraits.hpp>
#include <zlib/ZuInspect.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuHash.hpp>
#include <zlib/ZuPrint.hpp>
#include <zlib/ZuString.hpp>

// A ZuID is a union of a 64bit unsigned integer with an 8-byte
// zero-padded string; this permits short human-readable string identifiers
// to be compared and hashed very rapidly using integer 64bit operations
// without needing a map between names and numbers

// Note: the string will not be null-terminated if all 8 bytes are in use

// the implementation uses misaligned 64bit loads where available
// (i.e. any recent x86 64bit)

class ZuID {
public:
  ZuID() : m_val{0} { }

  ZuID(const ZuID &b) : m_val{b.m_val} { }
  ZuID &operator =(const ZuID &b) { m_val = b.m_val; return *this; }

  template <typename S>
  ZuID(S &&s, ZuMatchString<S> *_ = nullptr) {
    init(ZuFwd<S>(s));
  }
  template <typename S>
  ZuMatchString<S, ZuID &> operator =(S &&s) {
    init(ZuFwd<S>(s));
    return *this;
  }

  template <typename V> struct IsUInt64 : public ZuBool<
    !ZuTraits<V>::IsString && ZuInspect<V, uint64_t>::Exists> { };
  template <typename V, typename R = void>
  using MatchUInt64 = ZuIfT<IsUInt64<V>{}, R>;

  template <typename V>
  ZuID(V v, MatchUInt64<V> *_ = nullptr) : m_val{v} { }
  template <typename V>
  MatchUInt64<V, ZuID &> operator =(V v) {
    m_val = v;
    return *this;
  }

  void init(ZuString s) {
    if (ZuLikely(s.length() == 8)) {
      const uint64_t *ZuMayAlias(ptr) =
	reinterpret_cast<const uint64_t *>(s.data());
#ifdef __x86_64__
      m_val = *ptr; // potentially misaligned load
#else
      memcpy(&m_val, ptr, 8);
#endif
      return;
    }
    m_val = 0;
    unsigned n = s.length();
    if (ZuUnlikely(!n)) return;
    if (ZuUnlikely(n > 8)) n = 8;
    memcpy(&m_val, s.data(), n);
  }

  char *data() {
    char *ZuMayAlias(ptr) = reinterpret_cast<char *>(&m_val);
    return ptr;
  }
  const char *data() const {
    const char *ZuMayAlias(ptr) = reinterpret_cast<const char *>(&m_val);
    return ptr;
  }
  unsigned length() const {
    if (!m_val) return 0U;
#if Zu_BIGENDIAN
    return (71U - __builtin_ctzll(m_val))>>3U;
#else
    return (71U - __builtin_clzll(m_val))>>3U;
#endif
  }

  operator ZuString() const { return ZuString{data(), length()}; }
  ZuString string() const { return ZuString{data(), length()}; }

  template <typename S> void print(S &s) const { s << string(); }

  operator uint64_t() const { return m_val; }

  bool equals(ZuID v) const { return m_val == v.m_val; }
  int cmp(ZuID v) const {
    return (m_val > v.m_val) - (m_val < v.m_val);
  }
  template <typename L, typename R>
  friend inline ZuIfT<ZuInspect<ZuID, L>::Is, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline ZuIfT<ZuInspect<ZuID, L>::Is, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

  bool operator !() const { return !m_val; }

  bool operator *() const { return m_val; }

  void null() { m_val = 0; }

  ZuID &update(ZuID id) {
    if (*id) m_val = id.m_val;
    return *this;
  }

  uint32_t hash() const { return ZuHash<uint64_t>::hash(m_val); }

  struct Traits : public ZuTraits<uint64_t> {
    enum { IsPrimitive = 0 };
  };
  friend Traits ZuTraitsType(ZuID *);

  friend ZuPrintFn ZuPrintType(ZuID *);

private:
  uint64_t	m_val;
};

template <> struct ZuCmp<ZuID> : public ZuCmp0<uint64_t> {
  static const ZuID &null() { static const ZuID v; return v; }
  static bool null(uint64_t id) { return !id; }
};

#endif /* ZuID_HPP */
