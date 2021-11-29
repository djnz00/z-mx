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

// MxMD library internal API

#ifndef MxMDCore_HPP
#define MxMDCore_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxMDLib_HPP
#include <mxmd/MxMDLib.hpp>
#endif

#include <zlib/ZuPOD.hpp>

#include <zlib/ZmObject.hpp>
#include <zlib/ZmRBTree.hpp>
#include <zlib/ZmRef.hpp>
#include <zlib/ZmThread.hpp>

#include <zlib/ZePlatform.hpp>

#include <zlib/ZvCf.hpp>
#include <zlib/ZvCmdServer.hpp>

#include <mxbase/MxMultiplex.hpp>
#include <mxbase/MxEngine.hpp>

#include <mxmd/MxMD.hpp>
#include <mxmd/MxMDStream.hpp>

#include <mxmd/MxMDBroadcast.hpp>

#include <mxmd/MxMDRecord.hpp>
#include <mxmd/MxMDReplay.hpp>

#include <mxmd/MxMDPublisher.hpp>
#include <mxmd/MxMDSubscriber.hpp>

#include <mxmd/MxMDTelemetry.hpp>

class MxMDCore;

extern "C" {
  typedef void (*MxMDFeedPluginFn)(MxMDCore *md, const ZvCf *cf);
};

class MxMDAPI MxMDCmdServer :
    public ZmPolymorph, public ZvCmdServer<MxMDCmdServer> {
public:
  using Base = ZvCmdServer<MxMDCmdServer>;
  using Link = typename Base::Link;
  using User = typename Base::User;

  using AppFn = ZmFn<
    MxMDCmdServer *, Link *, User *, bool, unsigned, ZuArray<const uint8_t>>;

  void final() { m_appFn = {}; Base::final(); }

  void appFn(AppFn fn) { m_appFn = ZuMv(fn); }

  int processApp(Link *link, User *user, bool interactive,
      ZuID id, ZuArray<const uint8_t> data) {
    return m_appFn(this, link, user, interactive, id, data);
  }

private:
  AppFn		m_appFn;
};

class MxMDAPI MxMDCore : public MxMDLib, public MxEngineMgr {
friend MxMDLib;
friend MxMDCmdServer;

public:
  static unsigned vmajor();
  static unsigned vminor();

  using Mx = MxMultiplex;

private:
  MxMDCore(Mx *mx);

  void init_(const ZvCf *cf);

public:
  ~MxMDCore() { }

  ZuInline ZvCf *cf() { return m_cf.ptr(); }

  ZuInline Mx *mx() const { return m_mx; }

  void start();
  void stop();
  void final();

  bool record(ZuString path);
  ZtString stopRecording();

  bool replay(ZuString path,
      MxDateTime begin = MxDateTime(),
      bool filter = true);
  ZtString stopReplaying();

  void startTimer(MxDateTime begin = MxDateTime());
  void stopTimer();

  void dumpTickSizes(ZuString path, MxID venue = MxID());
  void dumpInstruments(
      ZuString path, MxID venue = MxID(), MxID segment = MxID());
  void dumpOrderBooks(
      ZuString path, MxID venue = MxID(), MxID segment = MxID());

  // null if unconfigured
  ZuInline MxMDCmdServer *cmdServer() { return m_cmdServer; }
  void addCmd(ZuString name, ZuString syntax,
      ZvCmdFn fn, ZtString brief, ZtString usage) {
    if (!m_cmdServer) return;
    m_cmdServer->addCmd(name, syntax, ZuMv(fn), brief, usage);
  }

public:
  // for use by replay

  using Hdr = MxMDStream::Hdr;

  void pad(Hdr &);
  void apply(const Hdr &, bool filter);

private:
  void initCmds();

  void l1(void *, const ZvCf *, ZtString &);
  void l2(void *, const ZvCf *, ZtString &);
  void l2_side(MxMDOBSide *, ZtString &);
  void instrument_(void *, const ZvCf *, ZtString &);

  void ticksizes(void *, const ZvCf *, ZtString &);
  void instruments(void *, const ZvCf *, ZtString &);
  void orderbooks(void *, const ZvCf *, ZtString &);

#if 0
  void tick(void *, const ZvCf *, ZtString &);
  void update(void *, const ZvCf *, ZtString &);

  void feeds(void *, const ZvCf *, ZtString &);
  void venues(void *, const ZvCf *, ZtString &);
#endif

  void addVenueMapping_(ZuAnyPOD *);
  void addTickSize_(ZuAnyPOD *);
  void addInstrument_(ZuAnyPOD *);
  void addOrderBook_(ZuAnyPOD *);

  // Engine Management
  void addEngine(MxEngine *engine) {
    if (m_telemetry) m_telemetry->addEngine(engine);
  }
  void delEngine(MxEngine *) { }
  void engineState(MxEngine *, MxEnum, MxEnum) { }

  // Link Management
  void updateLink(MxAnyLink *) { }
  void delLink(MxAnyLink *) { }
  void linkState(MxAnyLink *link, MxEnum, MxEnum) { }

  // Pool Management
  void updateTxPool(MxAnyTxPool *) { }
  void delTxPool(MxAnyTxPool *) { }

  // Queue Management
  using QueueFn = ZvTelemetry::QueueFn;
  void addQueue(unsigned type, MxID id, QueueFn queueFn) {
    if (m_telemetry) m_telemetry->addQueue(type, id, ZuMv(queueFn));
  }
  void delQueue(unsigned type, MxID id) {
    if (m_telemetry) m_telemetry->delQueue(id, tx);
  }

public:
  // DB Management
  void addDBEnv(ZdbEnv *env) {
    if (m_telemetry) m_telemetry->addDBEnv(env);
  }

private:
  // Traffic Logging (logThread)
  /* Example usage:
  app.log(id, MxTraffic([](const Msg *msg, ZmTime &stamp, ZuString &data) {
    stamp = msg->stamp();
    data = msg->buf();
  }, msg)); */
  void log(MxMsgID, MxTraffic) { }

public:
  MxMDBroadcast &broadcast() { return m_broadcast; }

  bool streaming() { return m_broadcast.active(); }

  template <typename Snapshot>
  bool snapshot(Snapshot &snapshot, MxID id, MxSeqNo seqNo) {
    bool ok = allVenues([&snapshot](MxMDVenue *venue) {
      return MxMDStream::addVenue(
	  snapshot, venue->id(), venue->flags(), venue->orderIDScope()) &&
	venue->allTickSizeTbls([&snapshot, venue](MxMDTickSizeTbl *tbl) {
	  return MxMDStream::addTickSizeTbl(
	      snapshot, venue->id(), tbl->id(), tbl->pxNDP()) &&
	    tbl->allTickSizes(
		[&snapshot, venue, tbl](const MxMDTickSize &ts) {
	    return MxMDStream::addTickSize(snapshot,
		venue->id(), ts.minPrice(), ts.maxPrice(), ts.tickSize(),
		tbl->id(), tbl->pxNDP());
	  });
	});
    }) && allInstruments([&snapshot](MxMDInstrument *instrument) {
      return MxMDStream::addInstrument(snapshot, instrument->shard()->id(),
	  MxDateTime(), instrument->key(), instrument->refData());
    }) && allOrderBooks([&snapshot](MxMDOrderBook *ob) {
      if (!MxMDStream::resetOB(snapshot, ob->shard()->id(),
	  MxDateTime(), ob->key())) return false;
      if (ob->legs() == 1) {
	return MxMDStream::addOrderBook(snapshot, ob->shard()->id(),
	    MxDateTime(), ob->key(), ob->instrument()->key(),
	    ob->lotSizes(), ob->tickSizeTbl()->id(), ob->qtyNDP());
      } else {
	MxInstrKey instrumentKeys[MxMDNLegs];
	MxEnum sides[MxMDNLegs];
	MxRatio ratios[MxMDNLegs];
	for (unsigned i = 0, n = ob->legs(); i < n; i++) {
	  instrumentKeys[i] = ob->instrument(i)->key();
	  sides[i] = ob->side(i);
	  ratios[i] = ob->ratio(i);
	}
	return MxMDStream::addCombination(snapshot, ob->shard()->id(),
	    MxDateTime(), ob->key(), ob->legs(), instrumentKeys,
	    ratios, ob->lotSizes(), ob->tickSizeTbl()->id(),
	    ob->pxNDP(), ob->qtyNDP(), sides);
      }
    }) && allVenues([&snapshot](MxMDVenue *venue) {
      return (venue->loaded() ||
	  MxMDStream::refDataLoaded(snapshot, venue->id())) &&
	venue->allSegments([&snapshot, venue](
	      const MxMDSegment &segment) {
	  return MxMDStream::tradingSession(snapshot, segment.stamp,
	      venue->id(), segment.id, segment.session);
	});
    }) && allOrderBooks([&snapshot](MxMDOrderBook *ob) {
      return
	MxMDStream::l1(snapshot, ob->shard()->id(), ob->key(), ob->l1Data()) &&
	snapshotL2Side(snapshot, ob->bids()) &&
	snapshotL2Side(snapshot, ob->asks());
    }) && MxMDStream::endOfSnapshot(snapshot, id, seqNo, true);
    if (!ok)
      MxMDStream::endOfSnapshot(m_broadcast, id, seqNo, false);
    return ok;
  }

private:
  template <class Snapshot>
  static bool snapshotL2Side(Snapshot &snapshot, MxMDOBSide *side) {
    return (!side->mktLevel() ||
	  snapshotL2PxLvl(snapshot, side->mktLevel())) &&
      side->allPxLevels([&snapshot](MxMDPxLevel *pxLevel) {
	return snapshotL2PxLvl(snapshot, pxLevel);
      });
  }
  template <class Snapshot>
  static bool snapshotL2PxLvl(Snapshot &snapshot, MxMDPxLevel *pxLevel) {
    unsigned orderCount = 0;
    if (!pxLevel->allOrders([&snapshot, &orderCount](MxMDOrder *order) {
      ++orderCount;
      const MxMDOrderData &data = order->data();
      MxMDOrderBook *ob = order->orderBook();
      return MxMDStream::addOrder(snapshot, ob->shard()->id(),
	  data.transactTime, ob->key(),
	  data.price, data.qty, data.rank, data.flags,
	  order->id(), ob->pxNDP(), ob->qtyNDP(), data.side);
    })) return false;
    if (orderCount) return true;
    const MxMDPxLvlData &data = pxLevel->data();
    MxMDOrderBook *ob = pxLevel->obSide()->orderBook();
    return MxMDStream::pxLevel(snapshot, ob->shard()->id(),
	data.transactTime, ob->key(), pxLevel->price(), data.qty,
	data.nOrders, data.flags, ob->pxNDP(), ob->qtyNDP(),
	pxLevel->side(), (uint8_t)false);
  }

private:
  ZmRef<MxMDVenue> venue_(MxID id, MxEnum orderIDScope, MxFlags flags);

private:
  typedef ZmPLock Lock;
  typedef ZmGuard<Lock> Guard;

  void timer();

  Lock			m_stateLock; // prevents overlapping start() / stop()

  ZmRef<ZvCf>		m_cf;

  Mx			*m_mx = 0;

  ZmRef<MxMDTelemetry>	m_telemetry;
  ZmRef<MxMDCmdServer>	m_cmdServer; // FIXME

  MxMDBroadcast		m_broadcast;	// broadcasts updates

  ZmRef<MxMDRecord>	m_record;	// records to file
  ZmRef<MxMDReplay>	m_replay;	// replays from file

  ZmRef<MxMDPublisher>	m_publisher;	// publishes to network
  ZmRef<MxMDSubscriber>	m_subscriber;	// subscribes from network

  ZmRef<MxMDFeed>	m_localFeed;

  ZmScheduler::Timer	m_timer;
  Lock			m_timerLock;
    ZmTime		  m_timerNext;	// time of next timer event
};

ZuInline MxMDCore *MxMDRecord::core() const
  { return static_cast<MxMDCore *>(mgr()); }
ZuInline MxMDCore *MxMDReplay::core() const
  { return static_cast<MxMDCore *>(mgr()); }
ZuInline MxMDCore *MxMDPublisher::core() const
  { return static_cast<MxMDCore *>(mgr()); }
ZuInline MxMDCore *MxMDSubscriber::core() const
  { return static_cast<MxMDCore *>(mgr()); }

#endif /* MxMDCore_HPP */
