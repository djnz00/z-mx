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

// Mx Multiplex

#ifndef ZvMxParams_HPP
#define ZvMxParams_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

#include <zlib/ZuPolymorph.hpp>

#include <zlib/ZiMultiplex.hpp>

#include <zlib/ZvCf.hpp>
#include <zlib/ZvError.hpp>
#include <zlib/ZvSchedParams.hpp>

class ZvAPI ZvInvalidMulticastIP : public ZvError {
public:
  ZvInvalidMulticastIP(ZuString s) : m_addr(s) { }

  void print_(ZmStream &s) const {
    s << "invalid multicast IP \"" << m_addr << '"';
  }

private:
  ZtString	m_addr;
};

struct ZvCxnOptions : public ZiCxnOptions {
  ZvCxnOptions() : ZiCxnOptions() { }

  ZvCxnOptions(const ZiCxnOptions &p) : ZiCxnOptions(p) { }
  ZvCxnOptions &operator =(const ZiCxnOptions &p) {
    ZiCxnOptions::operator =(p);
    return *this;
  }

  ZvCxnOptions(const ZvCf *cf) : ZiCxnOptions() { init(cf); }

  ZvCxnOptions(const ZvCf *cf, const ZiCxnOptions &deflt) :
      ZiCxnOptions(deflt) { init(cf); }

  void init(const ZvCf *cf) {
    if (!cf) return;

    flags(cf->getFlags<ZiCxnFlags::Map>("options", false, 0));

    // multicastInterface is the IP address of the interface used for sending
    // multicastTTL is the TTL (number of hops) used for sending
    // multicastGroups are the groups that are subscribed to for receiving
    // - each group is "addr interface", where addr is the multicast IP
    //   address of the group being subscribed to, and interface is the
    //   IP address of the interface used to receive the messages; interface
    //   can be 0.0.0.0 to subscribe on all interfaces
    // Example: multicastGroups { 239.193.2.51 192.168.1.99 }
    if (multicast()) {
      if (ZuString s = cf->get("multicastInterface", false)) mif(s);
      ttl(cf->getInt("multicastTTL", 0, INT_MAX, false, ttl()));
      if (ZmRef<ZvCf> groups = cf->subset("multicastGroups")) {
	ZvCf::Iterator i(groups);
	ZuString addr_, mif_;
	while (mif_ = i.get(addr_)) {
	  ZiIP addr(addr_), mif(mif_);
	  if (!addr || !addr.multicast()) throw ZvInvalidMulticastIP(addr_);
	  mreq(ZiMReq(addr, mif));
	}
      }
    }
    if (netlink())
      familyName(cf->get("familyName", true));
  }
};

struct ZvMxParams : public ZiMxParams {
  ZvMxParams() : ZiMxParams() { }

  using ZiMxParams::ZiMxParams;

  ZvMxParams(const ZvCf *cf) { init(cf); }

  ZvMxParams(const ZvCf *cf, ZiMxParams &&deflt) :
    ZiMxParams(ZuMv(deflt)) { init(cf); }

  ZvSchedParams &scheduler() {
    return static_cast<ZvSchedParams &>(ZiMxParams::scheduler());
  }

  void init(const ZvCf *cf) {
    if (!cf) return;

    scheduler().init(cf);
    if (ZuString s = cf->get("rxThread")) rxThread(scheduler().tid(s));
    if (ZuString s = cf->get("txThread")) txThread(scheduler().tid(s));
#ifdef ZiMultiplex_EPoll
    epollMaxFDs(cf->getInt("epollMaxFDs", 1, 100000, false, epollMaxFDs()));
    epollQuantum(cf->getInt("epollQuantum", 1, 1024, false, epollQuantum()));
#endif
    rxBufSize(cf->getInt("rcvBufSize", 0, INT_MAX, false, rxBufSize()));
    txBufSize(cf->getInt("sndBufSize", 0, INT_MAX, false, txBufSize()));
#ifdef ZiMultiplex_DEBUG
    trace(cf->getInt("trace", 0, 1, false, trace()));
    debug(cf->getInt("debug", 0, 1, false, debug()));
    frag(cf->getInt("frag", 0, 1, false, frag()));
    yield(cf->getInt("yield", 0, 1, false, yield()));
#endif
  }
};

class ZvMultiplex : public ZuPolymorph, public ZiMultiplex {
public:
  using ID = ZmScheduler::ID;

  static ID IDAxor(const ZvMultiplex *mx) { return mx->params().id(); }

  template <typename ID_>
  ZvMultiplex(const ID_ &id) :
      ZiMultiplex(ZiMxParams().scheduler([&](auto &s) { s.id(id); })) { }
  template <typename ID_>
  ZvMultiplex(const ID_ &id, const ZvCf *cf) :
      ZiMultiplex(ZvMxParams(cf,
	    ZiMxParams().scheduler([&](auto &s) { s.id(id); }))) { }
};

#endif /* ZvMxParams_HPP */
