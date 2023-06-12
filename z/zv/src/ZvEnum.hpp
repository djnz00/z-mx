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

// Zv layer enum/flags <-> string conversions, used by ZvCf and ZvCSV

#ifndef ZvEnum_HPP
#define ZvEnum_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

#include <zlib/ZmObject.hpp>
#include <zlib/ZmRef.hpp>

#include <zlib/ZtString.hpp>
#include <zlib/ZtEnum.hpp>

#include <zlib/ZvError.hpp>

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

  void print_(ZmStream &s) const;
};

template <typename Map, bool Throw = true>
inline ZuIfT<Throw, ZtEnum>
s2v(ZuString key, ZuString s)
{
  auto v = Map::s2v(s);
  if (ZuLikely(*v)) return v;
  throw InvalidT<Map>{key, s};
}
template <typename Map, bool Throw = true>
inline ZuIfT<!Throw, ZtEnum>
s2v(ZuString key, ZuString s, int deflt = -1)
{
  auto v = Map::s2v(s);
  if (ZuLikely(*v)) return v;
  return deflt;
}

template <typename Map>
inline const char *v2s(ZuString key, ZtEnum v)
{
  const char *s = Map::v2s(v);
  if (ZuLikely(s)) return s;
  throw InvalidT<Map>{key, v};
}

template <typename Map, typename Key, typename Value>
inline void errorMessage(ZmStream &s, Key &&key, Value &&value)
{
  s << ZuFwd<Key>(key) << ": \"" << ZuFwd<Value>(value) <<
    "\" did not match { ";
  bool first = true;
  Map::all([&s, &first](ZuString s_, ZtEnum v) {
    if (ZuLikely(!first)) s << ", ";
    first = false;
    s << s_ << "=" << v;
  });
  s << " }";
}

template <typename Map>
inline void InvalidT<Map>::print_(ZmStream &s) const
{
  errorMessage<Map>(s, this->key(), this->value());
}

template <typename Map, typename S, typename Flags>
inline unsigned print(
    ZuString key, S &s, const Flags &v, char delim = '|')
{
  if (!v) return 0;
  return Map::print(s, v, delim);
}

template <typename Map, typename Flags>
inline Flags scan(ZuString key, ZuString s, char delim = '|')
{
  if (!s) return 0;
  if (Flags v = Map::template scan<Flags>(s, delim)) return v;
  throw InvalidT<Map>{key, s};
  // not reached
}

} // ZvEnum

using ZvInvalidEnum = ZvEnum::Invalid;

#endif /* ZvEnum_HPP */
