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

// Mx Scheduler

#ifndef ZvScheduler_HPP
#define ZvScheduler_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

#include <zlib/ZmScheduler.hpp>

#include <zlib/ZvCf.hpp>

namespace ZvSchedulerPriorities {
  ZtEnumValues_(
      RealTime = ZmThreadPriority::RealTime,
      High = ZmThreadPriority::High,
      Normal = ZmThreadPriority::Normal,
      Low = ZmThreadPriority::Low);
  ZtEnumMap("ZvSchedulerPriorities", Map,
      "RealTime", RealTime, "High", High, "Normal", Normal, "Low", Low);
}

struct ZvSchedParams : public ZmSchedParams {
  ZvSchedParams() { }

  using ZmSchedParams::ZmSchedParams;

  ZvSchedParams(const ZvCf *cf) { init(cf); }
  inline ZvSchedParams(const ZvCf *cf, ZmSchedParams &&deflt) :
    ZmSchedParams(ZuMv(deflt)) { init(cf); }

  void init(const ZvCf *cf) {
    if (!cf) return;

    static unsigned ncpu = Zm::getncpu();

    if (ZuString s = cf->get("id")) id(s);
    nThreads(cf->getInt("nThreads", 1, 1024, false, nThreads()));
    stackSize(cf->getInt("stackSize", 16384, 2<<20, false, stackSize()));
    priority(cf->getEnum<ZvSchedulerPriorities::Map>(
	  "priority", false, ZmThreadPriority::Normal));
    partition(cf->getInt("partition", 0, ncpu - 1, false, 0));
    if (ZuString s = cf->get("quantum")) quantum((double)ZuBox<double>(s));
    queueSize(cf->getInt("queueSize", 8192, (1U<<30U), false, queueSize()));
    ll(cf->getInt("ll", 0, 1, false, ll()));
    spin(cf->getInt("spin", 0, INT_MAX, false, spin()));
    timeout(cf->getInt("timeout", 0, 3600, false, timeout()));
    startTimer(cf->getInt("startTimer", 0, 1, false, startTimer()));
    if (ZmRef<ZvCf> threadsCf = cf->subset("threads")) {
      ZvCf::Iterator i(threadsCf);
      ZuString id;
      while (ZmRef<ZvCf> threadCf = i.subset(id)) {
	ZuBox<unsigned> tid = id;
	if (id != ZuStringN<12>{tid})
	  throw ZtString() << "bad thread ID \"" << id << '"';
	ZmSchedParams::Thread &thread = this->thread(tid);
	thread.isolated(threadCf->getInt(
	      "isolated", 0, 1, false, thread.isolated()));
	if (ZuString s = threadCf->get("name")) thread.name(s);
	thread.stackSize(threadCf->getInt(
	      "stackSize", 0, INT_MAX, false, thread.stackSize()));
	if (ZuString s = threadCf->get("priority")) {
	  if (s == "RealTime") thread.priority(ZmThreadPriority::RealTime);
	  else if (s == "High") thread.priority(ZmThreadPriority::High);
	  else if (s == "Normal") thread.priority(ZmThreadPriority::Normal);
	  else if (s == "Low") thread.priority(ZmThreadPriority::Low);
	  else throw ZtString() << "bad thread priority \"" << s << '"';
	}
	thread.partition(threadCf->getInt(
	      "partition", 0, INT_MAX, false, thread.partition()));
	if (ZuString s = threadCf->get("cpuset")) thread.cpuset(s);
	thread.detached(
	    threadCf->getInt("detached", 0, 1, false, thread.detached()));
      }
    }
  }
};

class ZvScheduler : public ZuObject, public ZmScheduler {
public:
  template <typename ID>
  ZvScheduler(const ID &id) :
    ZmScheduler(ZmSchedParams().nThreads(1).id(id)) { }
  template <typename ID>
  ZvScheduler(const ID &id, const ZvCf *cf) :
    ZmScheduler(ZvSchedParams(cf,
	  ZmSchedParams().nThreads(1).id(id))) { }
};

#endif /* ZvScheduler_HPP */
