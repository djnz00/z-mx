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

#ifndef ZiNetlink_HPP
#define ZiNetlink_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZiLib_HPP
#include <zlib/ZiLib.hpp>
#endif

#include <zlib/ZmAssert.hpp>

#include <zlib/ZeLog.hpp>

#include <zlib/ZiIP.hpp>
#include <zlib/ZiPlatform.hpp>

class ZiNetlinkSockAddr {
  struct sockaddr_nl  m_snl;

public:
  ZiNetlinkSockAddr() {
    m_snl.nl_family = AF_NETLINK;
    memset(&m_snl.nl_pad, 0, sizeof(m_snl.nl_pad));
    // LATER: we always let the kernel specify these...
    m_snl.nl_groups = 0;
    m_snl.nl_pid = 0;
  }

  ZiNetlinkSockAddr(uint32_t portID) {
    m_snl.nl_family = AF_NETLINK;
    memset(&m_snl.nl_pad, 0, sizeof(m_snl.nl_pad));
    m_snl.nl_groups = 0;
    m_snl.nl_pid = portID;
  }

  ZuInline struct sockaddr *sa() { return (struct sockaddr *)&m_snl; }
  ZuInline int len() const { return sizeof(struct sockaddr_nl); }

  template <typename S> void print(S &s) const {
    s << "pid=" << ZuBoxed(m_snl.nl_pid) <<
      " groups=" << ZuBoxed(m_snl.nl_groups);
  }
};
template <> struct ZuPrint<ZiNetlinkSockAddr> : public ZuPrintFn { };

class ZiNetlink {
  friend ZiMultiplex;
  friend ZiConnection;

  using Socket = Zi::Socket;

  // takes the familyName and gets the familyID and a portID from
  // the kernel. The familyID and portID are needed for
  // further communication. Returns 0 on success non-zero on error
  static int connect(Socket sock, ZuString familyName,
      unsigned int &familyID, uint32_t &portID);

  // read 'len' bytes into buffer 'buf' for the given familyID and portID
  // the nlmsghdr and genlmsghdr are read into scratch space and ignored.
  static int recv(Socket sock, unsigned int familyID,
      uint32_t portID, char *buf, int len);

  // send data in 'buf' of length 'len'. This method will prepend
  // an nlmsghdr and genlmsghdr as well as a ZiGNLAttr_Data attribute
  // to the data. The attribute describes the data in 'buf'
  static int send(Socket sock, unsigned int familyID,
      uint32_t portID, const void *buf, int len);

private:
  static int send(Socket sock,
      ZiVec *vecs, int nvecs, int totalBytes, int dataBytes);
  static int recv(Socket sock, struct msghdr *msg, int flags);
};

#endif /* ZiNetlink_HPP */
