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

// proxy stream using ZmFn to encapsulate arbitrary stream types into
// a single concrete type (ZmStream) that can be used from compiled code

#ifndef ZmStream_HPP
#define ZmStream_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuPrint.hpp>
#include <zlib/ZuString.hpp>

#include <zlib/ZmFn.hpp>

class ZmStreamBuf {
public:
  template <typename T> ZmStreamBuf(const T &v) :
    m_lengthFn{&v, [](const T *v) -> unsigned {
	return ZuPrint<T>::length(*v);
    }},
    m_printFn{&v, [](const T *v, char *buf, unsigned n) -> unsigned {
	return ZuPrint<T>::print(buf, n, *v);
    }} { }

  ZmStreamBuf(const ZmStreamBuf &) = delete;
  ZmStreamBuf &operator =(const ZmStreamBuf &) = delete;

  ZmStreamBuf(ZmStreamBuf &&s) :
    m_lengthFn(ZuMv(s.m_lengthFn)), m_printFn(ZuMv(s.m_printFn)) { }
  ZmStreamBuf &operator =(ZmStreamBuf &&s) {
    if (ZuLikely(this != &s)) {
      m_lengthFn = ZuMv(s.m_lengthFn);
      m_printFn = ZuMv(s.m_printFn);
    }
    return *this;
  }

  unsigned length() const {
    return m_lengthFn();
  }
  unsigned print(char *buf, unsigned n) const {
    return m_printFn(buf, n);
  }

  struct PrintType : public ZuPrintBuffer {
    static unsigned length(const ZmStreamBuf &b) {
      return b.length();
    }
    static unsigned print(char *buf, unsigned n, const ZmStreamBuf &b) {
      return b.print(buf, n);
    }
  };
  friend PrintType ZuPrintType(ZmStreamBuf *);

private:
  ZmFn<>			m_lengthFn;
  ZmFn<char *, unsigned>	m_printFn;
};

class ZmStream {
public:
  template <typename S>
  ZmStream(S &s) :
    m_strFn{&s, [](S *s, const ZuString &v) { *s << v; }},
    m_bufFn{&s, [](S *s, const ZmStreamBuf &v) { *s << v; }} { }

  ZmStream(const ZmStream &) = delete;
  ZmStream &operator =(const ZmStream &) = delete;

  ZmStream(ZmStream &&s) :
    m_strFn(ZuMv(s.m_strFn)), m_bufFn(ZuMv(s.m_bufFn)) { }
  ZmStream &operator =(ZmStream &&s) {
    if (ZuLikely(this != &s)) {
      m_strFn = ZuMv(s.m_strFn);
      m_bufFn = ZuMv(s.m_bufFn);
    }
    return *this;
  }

private:
  template <typename U, typename R = void>
  using MatchChar = ZuIfT<ZuConversion<U, char>::Same, R>;

  template <typename U, typename R = void>
  using MatchReal = ZuIfT<
    ZuTraits<U>::IsPrimitive &&
    ZuTraits<U>::IsReal &&
    !ZuConversion<U, char>::Same, R>;

  template <typename U, typename R = void>
  using MatchString = ZuIfT<
    ZuTraits<U>::IsString &&
    !ZuTraits<U>::IsWString &&
    !ZuConversion<ZuString, U>::Is, R>;

  template <typename U, typename R = void>
  using MatchPDelegate = ZuIfT<ZuPrint<U>::Delegate, R>;

  template <typename U, typename R = void>
  using MatchPBuffer = ZuIfT<ZuPrint<U>::Buffer, R>;

public:
  template <typename C>
  MatchChar<C, ZmStream &> operator <<(C c) {
    m_strFn(ZuString{&c, 1});
    return *this;
  }
  template <typename R>
  MatchReal<R, ZmStream &> operator <<(const R &r) {
    m_bufFn(ZmStreamBuf{ZuBoxed(r)});
    return *this;
  }
  ZmStream &operator <<(ZuString s) {
    m_strFn(s);
    return *this;
  }
  template <typename S>
  MatchString<S, ZmStream &> operator <<(S &&s_) {
    m_strFn(ZuString{ZuFwd<S>(s_)});
    return *this;
  }
  template <typename P>
  MatchPDelegate<P, ZmStream &> operator <<(const P &p) {
    ZuPrint<P>::print(*this, p);
    return *this;
  }
  template <typename P>
  MatchPBuffer<P, ZmStream &> operator <<(const P &p) {
    m_bufFn(ZmStreamBuf{p});
    return *this;
  }

private:
  ZmFn<const ZuString &>	m_strFn;
  ZmFn<const ZmStreamBuf &>	m_bufFn;
};

using ZmStreamFn = ZmFn<ZmStream &>;

template <typename S>
inline S &operator <<(S &s, const ZmStreamFn &fn) {
  ZmStream s_{s};
  fn(s_);
  return s;
}
inline ZmStream &operator <<(ZmStream &s, const ZmStreamFn &fn) {
  fn(s);
  return s;
}

#endif /* ZmStream_HPP */
