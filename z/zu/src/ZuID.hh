//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// Union of a 64bit integer and a human-readable string
// (8-byte left-aligned zero-padded)

#ifndef ZuID_HH
#define ZuID_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuInt.hh>
#include <zlib/ZuTraits.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuHash.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuString.hh>

// A ZuID is a union of a 64bit unsigned integer with an 8-byte
// zero-padded string; this permits short human-readable string identifiers
// to be compared and hashed very rapidly using integer 64bit operations
// without needing a map between names and numbers

// Note: the string will not be null-terminated if all 8 bytes are in use

// the implementation uses misaligned 64bit loads where available
// (i.e. any recent x86 64bit)

class ZuID {
public:
  constexpr ZuID() { }

  constexpr ZuID(const ZuID &b) : m_val{b.m_val} { }
  constexpr ZuID &operator =(const ZuID &b) { m_val = b.m_val; return *this; }

  template <typename S>
  ZuID(S &&s, ZuMatchString<S> *_ = nullptr) {
    init(ZuFwd<S>(s));
  }
  template <typename S>
  ZuMatchString<S, ZuID &> operator =(S &&s) {
    init(ZuFwd<S>(s));
    return *this;
  }

  // Note: convertibility and constructibility are the same for uint64_t
  template <typename V> struct IsUInt64 : public ZuBool<
    !ZuTraits<V>::IsString && ZuInspect<V, uint64_t>::Converts> { };
  template <typename V, typename R = void>
  using MatchUInt64 = ZuIfT<IsUInt64<V>{}, R>;

  template <typename V, MatchUInt64<V, int> = 0>
  constexpr ZuID(V v) : m_val{v} { }
  template <typename V>
  constexpr MatchUInt64<V, ZuID &> operator =(V v) {
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

  constexpr operator uint64_t() const { return m_val; }

  constexpr int cmp(ZuID v) const {
    return (m_val > v.m_val) - (m_val < v.m_val);
  }
  template <typename L, typename R>
  friend inline constexpr ZuIs<ZuID, L, bool>
  operator ==(const L &l, const R &r) { return l.m_val == r.m_val; }
  template <typename L, typename R>
  friend inline constexpr ZuIs<ZuID, L, int>
  operator <(const L &l, const R &r) { return l.m_val < r.m_val; }
  template <typename L, typename R>
  friend inline constexpr ZuIs<ZuID, L, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

  constexpr bool operator !() const { return !m_val; }

  constexpr bool operator *() const { return m_val; }

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
  uint64_t	m_val = 0;
};

// override ZuCmp to prevent default string-based comparison
template <> struct ZuCmp<ZuID> {
  template <typename L, typename R>
  constexpr static int cmp(const L &l, const R &r) { return l.cmp(r); }
  template <typename L, typename R>
  constexpr static bool equals(const L &l, const R &r) { return l == r; }
  template <typename L, typename R>
  constexpr static bool less(const L &l, const R &r) { return l < r; }
  constexpr static bool null(ZuID id) { return !id; }
  constexpr static ZuID null() { return {}; }
};

#endif /* ZuID_HH */
