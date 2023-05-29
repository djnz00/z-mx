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

// multiplexer configuration

#ifndef ZvMxParams_HPP
#define ZvMxParams_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

#include <zlib/ZiMultiplex.hpp>

#include <zlib/ZvCf.hpp>
#include <zlib/ZvError.hpp>
#include <zlib/ZvTelemetry.hpp>

class ZvInvalidMulticastIP : public ZvError {
public:
  ZvInvalidMulticastIP(ZuString s) : m_addr{s} { }

  void print_(ZmStream &s) const {
    s << "invalid multicast IP \"" << m_addr << '"';
  }

private:
  ZtString	m_addr;
};

struct ZvCxnOptions : public ZiCxnOptions {
  ZvCxnOptions() : ZiCxnOptions{} { }

  ZvCxnOptions(const ZiCxnOptions &p) : ZiCxnOptions{p} { }
  ZvCxnOptions &operator =(const ZiCxnOptions &p) {
    ZiCxnOptions::operator =(p);
    return *this;
  }

  ZvCxnOptions(const ZvCf *cf) : ZiCxnOptions{} { init(cf); }

  ZvCxnOptions(const ZvCf *cf, const ZiCxnOptions &deflt) :
      ZiCxnOptions{deflt} { init(cf); }

  void init(const ZvCf *cf) {
    if (!cf) return;

    flags(cf->getFlags<ZiCxnFlags::Map>("options", 0));

    // multicastInterface is the IP address of the interface used for sending
    // multicastTTL is the TTL (number of hops) used for sending
    // multicastGroups are the groups that are subscribed to for receiving
    // - each group is "addr interface", where addr is the multicast IP
    //   address of the group being subscribed to, and interface is the
    //   IP address of the interface used to receive the messages; interface
    //   can be 0.0.0.0 to subscribe on all interfaces
    // Example: multicastGroups { 239.193.2.51 192.168.1.99 }
    if (multicast()) {
      if (ZuString s = cf->get("multicastInterface")) mif(s);
      ttl(cf->getInt("multicastTTL", ttl(), 0, INT_MAX));
      if (ZmRef<ZvCf> groups = cf->getCf("multicastGroups")) {
	groups->all([this](const ZvCfNode *node) {
	  ZiIP addr{node->key}, mif{node->get<true>()};
	  if (!addr || !addr.multicast())
	    throw ZvInvalidMulticastIP{node->key};
	  mreq(ZiMReq(addr, mif));
	});
      }
    }
    if (netlink())
      familyName(cf->get("familyName", true));
  }
};

struct ZvMxParams : public ZiMxParams {
  ZvMxParams() = default;
  ZvMxParams(ZuString id, const ZvCf *cf) { init(id, cf); }
  ZvMxParams(ZuString id, const ZvCf *cf, ZiMxParams &&deflt) :
    ZiMxParams{ZuMv(deflt)} { init(id, cf); }

  void init(ZuString id, const ZvCf *cf) {
    if (!cf) return;

    {
      ZmSchedParams &sched = scheduler();
      static unsigned ncpu = Zm::getncpu();

      sched.id(id);
      sched.nThreads(
	  cf->getInt("nThreads", sched.nThreads(), 1, 1024));
      sched.stackSize(
	  cf->getInt("stackSize", sched.stackSize(), 16384, 2<<20));
      sched.priority(cf->getEnum<ZvTelemetry::ThreadPriority::Map>(
	    "priority", ZmThreadPriority::Normal));
      sched.partition(cf->getInt("partition", 0, 0, ncpu - 1));
      if (ZuString s = cf->get("quantum"))
	sched.quantum((double)ZuBox<double>(s));
      sched.queueSize(
	  cf->getInt("queueSize", sched.queueSize(), 8192, (1U<<30U)));
      sched.ll(cf->getInt("ll", sched.ll(), 0, 1));
      sched.spin(cf->getInt("spin", sched.spin(), 0, INT_MAX));
      sched.timeout(cf->getInt("timeout", sched.timeout(), 0, 3600));
      sched.startTimer(
	  cf->getInt("startTimer", sched.startTimer(), 0, 1));
      if (ZmRef<ZvCf> threadsCf = cf->getCf("threads")) {
	threadsCf->all([&sched](ZvCfNode *node) {
	  if (auto threadCf = node->getCf()) {
	    ZuString id = node->key;
	    ZuBox<unsigned> tid = id;
	    if (id != ZuStringN<12>{tid})
	      throw ZtString{} << "bad thread ID \"" << id << '"';
	    ZmSchedParams::Thread &thread = sched.thread(tid);
	    thread.isolated(threadCf->getInt(
		  "isolated", thread.isolated(), 0, 1));
	    if (ZuString s = threadCf->get("name")) thread.name(s);
	    thread.stackSize(threadCf->getInt(
		  "stackSize", thread.stackSize(), 0, INT_MAX));
	    if (ZuString s = threadCf->get("priority")) {
	      if (s == "RealTime") thread.priority(ZmThreadPriority::RealTime);
	      else if (s == "High") thread.priority(ZmThreadPriority::High);
	      else if (s == "Normal") thread.priority(ZmThreadPriority::Normal);
	      else if (s == "Low") thread.priority(ZmThreadPriority::Low);
	      else throw ZtString{} << "bad thread priority \"" << s << '"';
	    }
	    thread.partition(threadCf->getInt(
		  "partition", thread.partition(), 0, INT_MAX));
	    if (ZuString s = threadCf->get("cpuset")) thread.cpuset(s);
	    thread.detached(
		threadCf->getInt("detached", thread.detached(), 0, 1));
	  }
	});
      }
    }

    if (ZuString s = cf->get("rxThread")) rxThread(scheduler().sid(s));
    if (ZuString s = cf->get("txThread")) txThread(scheduler().sid(s));
#ifdef ZiMultiplex_EPoll
    epollMaxFDs(cf->getInt("epollMaxFDs", epollMaxFDs(), 1, 100000));
    epollQuantum(cf->getInt("epollQuantum", epollQuantum(), 1, 1024));
#endif
    rxBufSize(cf->getInt("rcvBufSize", rxBufSize(), 0, INT_MAX));
    txBufSize(cf->getInt("sndBufSize", txBufSize(), 0, INT_MAX));
#ifdef ZiMultiplex_DEBUG
    trace(cf->getInt("trace", trace(), 0, 1));
    debug(cf->getInt("debug", debug(), 0, 1));
    frag(cf->getInt("frag", frag(), 0, 1));
    yield(cf->getInt("yield", yield(), 0, 1));
#endif
  }
};

#endif /* ZvMxParams_HPP */
