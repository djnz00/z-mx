//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Zv layer enum/flags <-> string conversions, used by ZvCf and ZvCSV

#ifndef ZvEnum_HH
#define ZvEnum_HH

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <zlib/ZmObject.hh>
#include <zlib/ZmRef.hh>

#include <zlib/ZtString.hh>
#include <zlib/ZtEnum.hh>

#include <zlib/ZvError.hh>

namespace ZvEnum {

class Invalid : public ZvError {
public:
  template <typename Key, typename Value>
  Invalid(Key &&key, Value &&value) :
    m_key(ZuFwd<Key>(key)), m_value(ZuFwd<Value>(value)) { }

  const ZtString &key() const { return m_key; }
  const ZtString &value() const { return m_value; }

private:
  ZtString		m_key;
  ZtString		m_value;
};
template <typename Map> class InvalidT : public Invalid {
public:
  template <typename Key, typename Value>
  InvalidT(Key &&key, Value &&value) :
    Invalid{ZuFwd<Key>(key), ZuFwd<Value>(value)} { }

  void print_(ZuVStream &s) const;
};

template <typename Map, bool Throw = true, decltype(ZuIfT<Throw>(), int()) = 0>
inline auto s2v(ZuString key, ZuString s) {
  auto v = Map::s2v(s);
  if (!ZuCmp<decltype(v)>::null(v)) return v;
  throw InvalidT<Map>{key, s};
}
template <typename Map, bool Throw = true, decltype(ZuIfT<!Throw>(), int()) = 0>
inline auto s2v(ZuString key, ZuString s, int deflt = -1) {
  auto v = Map::s2v(s);
  if (!ZuCmp<decltype(v)>::null(v)) return v;
  return decltype(v)(deflt);
}

template <typename Map, typename V>
inline const char *v2s(ZuString key, V v)
{
  const char *s = Map::v2s(v);
  if (ZuLikely(s)) return s;
  throw InvalidT<Map>{key, v};
}

template <typename Map, typename Key, typename Value>
inline void errorMessage(ZuVStream &s, Key &&key, Value &&value)
{
  s << ZuFwd<Key>(key) << ": \"" << ZuFwd<Value>(value) <<
    "\" did not match { ";
  bool first = true;
  Map::all([&s, &first](ZuString s_, auto v) {
    if (ZuLikely(!first)) s << ", ";
    first = false;
    s << s_ << "=" << v;
  });
  s << " }";
}

template <typename Map>
inline void InvalidT<Map>::print_(ZuVStream &s) const
{
  errorMessage<Map>(s, this->key(), this->value());
}

template <typename Map, typename S, typename Flags>
inline unsigned print(
    ZuString key, S &s, const Flags &v, const char *delim = "|")
{
  if (!v) return 0;
  return Map::print(s, v, delim);
}

template <typename Map, typename Flags>
inline Flags scan(ZuString key, ZuString s, const char *delim = "|")
{
  if (!s) return 0;
  if (Flags v = Map::template scan<Flags>(s, delim)) return v;
  throw InvalidT<Map>{key, s};
  // not reached
}

} // ZvEnum

using ZvInvalidEnum = ZvEnum::Invalid;

#endif /* ZvEnum_HH */
