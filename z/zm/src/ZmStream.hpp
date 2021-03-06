//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2

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
// a single concrete type (ZmStream) that can be used from cpp code;
// ZmStreamFn can be used to defer printing, e.g. for logging

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
  template <typename T> ZuInline ZmStreamBuf(const T &v) :
    m_lengthFn(&v, [](const T *v) -> unsigned {
	return ZuPrint<T>::length(*v); }),
    m_printFn(&v, [](const T *v, char *buf, unsigned n) -> unsigned {
	return ZuPrint<T>::print(buf, n, *v); }) { }

  ZmStreamBuf(const ZmStreamBuf &) = delete;
  ZmStreamBuf &operator =(const ZmStreamBuf &) = delete;

  ZuInline ZmStreamBuf(ZmStreamBuf &&s) :
    m_lengthFn(ZuMv(s.m_lengthFn)), m_printFn(ZuMv(s.m_printFn)) { }
  ZmStreamBuf &operator =(ZmStreamBuf &&s) {
    m_lengthFn = ZuMv(s.m_lengthFn);
    m_printFn = ZuMv(s.m_printFn);
    return *this;
  }

  ZuInline unsigned length() const {
    return m_lengthFn();
  }
  ZuInline unsigned print(char *buf, unsigned n) const {
    return m_printFn(buf, n);
  }

private:
  ZmFn<>			m_lengthFn;
  ZmFn<char *, unsigned>	m_printFn;
};
template <> struct ZuPrint<ZmStreamBuf> : public ZuPrintBuffer {
  ZuInline static unsigned length(const ZmStreamBuf &b) {
    return b.length();
  }
  ZuInline static unsigned print(char *buf, unsigned n, const ZmStreamBuf &b) {
    return b.print(buf, n);
  }
};

class ZmStream {
public:
  template <typename S> ZuInline ZmStream(S &s) :
    m_strFn(&s, [](S *s, const ZuString &v) { *s << v; }),
    m_bufFn(&s, [](S *s, const ZmStreamBuf &v) { *s << v; }) { }

  ZmStream(const ZmStream &) = delete;
  ZmStream &operator =(const ZmStream &) = delete;

  ZuInline ZmStream(ZmStream &&s) noexcept :
    m_strFn(ZuMv(s.m_strFn)), m_bufFn(ZuMv(s.m_bufFn)) { }
  ZmStream &operator =(ZmStream &&s) noexcept {
    m_strFn = ZuMv(s.m_strFn);
    m_bufFn = ZuMv(s.m_bufFn);
    return *this;
  }

private:
  template <typename U, typename R = void,
    bool B = ZuConversion<U, char>::Same> struct MatchChar;
  template <typename U, typename R>
  struct MatchChar<U, R, true> { typedef R T; };

  template <typename U, typename R = void,
    bool B = ZuTraits<U>::IsPrimitive &&
	     ZuTraits<U>::IsReal &&
	     !ZuConversion<U, char>::Same
    > struct MatchReal;
  template <typename U, typename R>
  struct MatchReal<U, R, true> { typedef R T; };

  template <typename U, typename R = void,
    bool S = ZuTraits<U>::IsString &&
	     !ZuTraits<U>::IsWString &&
	     !ZuConversion<ZuString, U>::Is> struct MatchString;
  template <typename U, typename R>
  struct MatchString<U, R, true> { typedef R T; };

  template <typename U, typename R = void,
    bool B = ZuPrint<U>::Delegate> struct MatchPDelegate;
  template <typename U, typename R>
  struct MatchPDelegate<U, R, true> { typedef R T; };

  template <typename U, typename R = void,
    bool B = ZuPrint<U>::Buffer> struct MatchPBuffer;
  template <typename U, typename R>
  struct MatchPBuffer<U, R, true> { typedef R T; };

public:
  template <typename C>
  ZuInline typename MatchChar<C, ZmStream &>::T operator <<(C c) {
    m_strFn(ZuString(&c, 1));
    return *this;
  }
  template <typename R>
  ZuInline typename MatchReal<R, ZmStream &>::T operator <<(const R &r) {
    m_bufFn(ZmStreamBuf(ZuBoxed(r)));
    return *this;
  }
  ZuInline ZmStream &operator <<(ZuString s) {
    m_strFn(s);
    return *this;
  }
  template <typename S>
  ZuInline typename MatchString<S, ZmStream &>::T operator <<(S &&s_) {
    m_strFn(ZuString(ZuFwd<S>(s_)));
    return *this;
  }
  template <typename P>
  ZuInline typename MatchPDelegate<P, ZmStream &>::T operator <<(const P &p) {
    ZuPrint<P>::print(*this, p);
    return *this;
  }
  template <typename P>
  ZuInline typename MatchPBuffer<P, ZmStream &>::T operator <<(const P &p) {
    m_bufFn(ZmStreamBuf(p));
    return *this;
  }

private:
  ZmFn<const ZuString &>	m_strFn;
  ZmFn<const ZmStreamBuf &>	m_bufFn;
};

#endif /* ZmStream_HPP */
