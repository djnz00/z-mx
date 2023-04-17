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

// ZtEnum wrapper, used by configuration and CSV

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

template <typename T> class ZvEnum;

class ZvAPI ZvInvalidEnum : public ZvError {
public:
  template <typename Key, typename Value>
  ZvInvalidEnum(Key &&key, Value &&value) :
    m_key(ZuFwd<Key>(key)), m_value(ZuFwd<Value>(value)) { }

  const ZtString &key() const { return m_key; }
  const ZtString &value() const { return m_value; }

private:
  ZtString			m_key;
  ZtString			m_value;
};
template <typename T> class ZvInvalidEnumT : public ZvInvalidEnum {
public:
  template <typename Key, typename Value>
  ZvInvalidEnumT(
      Key &&key, Value &&value, const ZvEnum<T> *enum_) :
    ZvInvalidEnum(ZuFwd<Key>(key), ZuFwd<Value>(value)), m_enum(enum_) { }

  void print_(ZmStream &s) const;

private:
  const ZvEnum<T>	*m_enum;
};

template <typename T> class ZvEnum : public T {
template <typename> friend class ZvInvalidEnumT;

public:
  static ZvEnum *instance() {
    return static_cast<ZvEnum *>(T::instance());
  }

  ZtEnum s2v(ZuString key, ZuString s,
      int def = ZtEnum{}, bool noThrow = false) const {
    if (ZuUnlikely(!s)) return def;
    ZtEnum v = T::s2v(s);
    if (ZuLikely(*v)) return v;
    if (noThrow) return def;
    throw ZvInvalidEnumT<T>{key, s, this};
  }

  const char *v2s(ZuString key, ZtEnum v) const {
    const char *s = T::v2s(v);
    if (ZuLikely(s)) return s;
    throw ZvInvalidEnumT<T>{key, v, this};
  }

private:
  template <typename Key, typename Value>
  void errorMessage(ZmStream &s, Key &&key, Value &&value) const {
    s << ZuFwd<Key>(key) << ": \"" << ZuFwd<Value>(value) <<
      "\" did not match { ";
    bool first = true;
    this->all([&s, &first](ZuString name, ZtEnum ordinal) {
      if (ZuLikely(!first)) s << ", ";
      first = false;
      s << name << "=" << ordinal;
    });
    s << " }";
  }
};

template <typename T>
void ZvInvalidEnumT<T>::print_(ZmStream &s) const
{
  m_enum->errorMessage(s, this->key(), this->value());
}

template <typename T> class ZvFlags : public ZvEnum<T> {
public:
  static ZvFlags *instance() {
    return static_cast<ZvFlags *>(T::instance());
  }

  template <typename S, typename Flags> unsigned print(
      ZuString key, S &s, const Flags &v, char delim = '|') const {
    if (!v) return 0;
    return T::print(s, v, delim);
  }

  template <typename Flags>
  Flags scan(ZuString key, ZuString s, char delim = '|') const {
    if (!s) return 0;
    if (Flags v = T::template scan<Flags>(s, delim)) return v;
    throw ZvInvalidEnumT<T>{
	key, s, static_cast<const ZvEnum<T> *>(this)};
    // not reached
  }
};

#endif /* ZvEnum_HPP */
