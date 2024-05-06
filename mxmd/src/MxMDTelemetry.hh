//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// MxMD telemetry

#ifndef MxMDTelemetry_HH
#define MxMDTelemetry_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxMDLib_HH
#include <mxmd/MxMDLib.hh>
#endif

#include <zlib/ZmRBTree.hh>

#include <mxbase/MxBase.hh>
#include <mxbase/MxQueue.hh>
#include <mxbase/MxTelemetry.hh>

class MxEngine;
class MxAnyLink;
class MxMDCore;

class MxMDAPI MxMDTelemetry : public ZmPolymorph, public MxTelemetry::Server {
  typedef ZmLock Lock;
  typedef ZmGuard<Lock> Guard;
  typedef ZmReadGuard<Lock> ReadGuard;

public:
  MxMDTelemetry() : m_time{ZmTime::Now} { }

  void init(MxMDCore *core, const ZvCf *cf);
  void final();

  void run(MxTelemetry::Server::Cxn *);

  void addEngine(MxEngine *);
  void addQueue(unsigned type, MxID, QueueFn);
  void delQueue(unsigned type, MxID);
  void addDBEnv(ZdbEnv *);

private:
  typedef ZmRBTree<MxID,
	    ZmRBTreeVal<ZmRef<MxEngine>,
	      ZmRBTreeObject<ZuNull,
		ZmRBTreeLock<ZmNoLock> > > > Engines;

  typedef ZmRBTree<ZuTuple<MxID, bool>,
	    ZmRBTreeVal<ZmRef<MxQueue>,
	      ZmRBTreeObject<ZuNull,
		ZmRBTreeLock<ZmNoLock> > > > Queues;

  MxMDCore	*m_core = 0;
  Lock		m_lock;
    Engines	  m_engines;
    Queues	  m_queues;
    ZmRef<ZdbEnv> m_dbEnv = nullptr;
  ZmTime	m_time;
};

#endif /* MxMDTelemetry_HH */
