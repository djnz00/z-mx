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

// IP address

#ifndef ZiIP_HPP
#define ZiIP_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZiLib_HPP
#include <zlib/ZiLib.hpp>
#endif

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuTraits.hpp>
#include <zlib/ZuString.hpp>
#include <zlib/ZuPrint.hpp>

#include <zlib/ZePlatform.hpp>

#include <zlib/ZiPlatform.hpp>

class ZiAPI ZiIP : public in_addr {
public:
  using Hostname = ZiPlatform::Hostname;

  enum Result {
    OK		= Zi::OK,
    IOError	= Zi::IOError
  };

  ZiIP() { s_addr = 0; }

  ZiIP(const ZiIP &a) { s_addr = a.s_addr; }
  ZiIP &operator =(const ZiIP &a) {
    s_addr = a.s_addr;
    return *this;
  }

  explicit ZiIP(struct in_addr ia) { s_addr = ia.s_addr; }
  ZiIP &operator =(struct in_addr ia) {
    s_addr = ia.s_addr;
    return *this;
  }

  ZiIP(uint32_t n) { s_addr = htonl(n); }
  ZiIP &operator =(uint32_t n) { s_addr = htonl(n); return *this; }

  template <typename S>
  ZiIP(S &&s, ZuIsString<S> *_ = nullptr) {
    if (!s) { s_addr = 0; return; }
    ZeError e;
    if (resolve(ZuFwd<S>(s), &e) != OK) throw e;
  }
  template <typename S>
  ZuIsString<S, ZiIP &> &operator =(S &&s) {
    if (!s) { s_addr = 0; return *this; }
    ZeError e;
    if (resolve(ZuFwd<S>(s), &e) != OK) throw e;
    return *this;
  }

  bool operator !() const { return !s_addr; }
  ZuOpBool

  bool equals(const ZiIP &a) const { return s_addr == a.s_addr; }
  int cmp(const ZiIP &a) const {
    return ZuCmp<uint32_t>::cmp(s_addr, a.s_addr);
  }
  friend inline bool operator ==(const ZiIP &l, const ZiIP &r) {
    return l.equals(r);
  }
  friend inline int operator <=>(const ZiIP &l, const ZiIP &r) {
    return l.cmp(r);
  }

  operator uint32_t() const { return ntohl(s_addr); }

  uint32_t hash() const { return s_addr; }

  template <typename S> void print(S &s) const {
    uint32_t addr = (uint32_t)ntohl(s_addr);
    s <<
      ZuBoxed((((uint32_t)addr)>>24) & 0xff) << '.' <<
      ZuBoxed((((uint32_t)addr)>>16) & 0xff) << '.' <<
      ZuBoxed((((uint32_t)addr)>>8 ) & 0xff) << '.' <<
      ZuBoxed((((uint32_t)addr)    ) & 0xff);
  }

  bool multicast() const {
    unsigned i = (((uint32_t)ntohl(s_addr))>>24) & 0xff;
     return i >= 224 && i < 240;
  }

  struct Traits : public ZuBaseTraits<ZiIP> { enum { IsPOD = 1 }; };
  friend Traits ZuTraitsType(ZiIP *);

  friend ZuPrintFn ZuPrintType(ZiIP *);

private:
  int resolve_(ZuString, ZeError *e = 0);
#ifdef _WIN32
  int resolve_(ZuWString, ZeError *e = 0);
#endif
public:
  template <typename S>
  ZuIsString<S, int> resolve(S &&s, ZeError *e = 0) {
    return resolve_(ZuString{ZuFwd<S>(s)}, e);
  }
  Hostname name(ZeError *e = 0);
};

class ZiSockAddr {
public:
  ZiSockAddr() { null(); }
  ZiSockAddr(ZiIP ip, uint16_t port) { init(ip, port); }

  void null() { m_sin.sin_family = AF_UNSPEC; }
  void init(ZiIP ip, uint16_t port) {
    m_sin.sin_family = AF_INET;
    m_sin.sin_port = htons(port);
    m_sin.sin_addr = ip;
    memset(&m_sin.sin_zero, 0, sizeof(m_sin.sin_zero));
  }

  ZiIP ip() const { return ZiIP(m_sin.sin_addr); }
  uint16_t port() const { return ntohs(m_sin.sin_port); }

  struct sockaddr *sa() { return (struct sockaddr *)&m_sin; }
  int len() const { return sizeof(struct sockaddr_in); }

  bool operator !() const { return m_sin.sin_family == AF_UNSPEC; }
  ZuOpBool

  struct sockaddr_in	m_sin;
};

#endif /* ZiIP_HPP */
