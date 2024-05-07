//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxMD replay

#ifndef MxMDReplay_HH
#define MxMDReplay_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxMDLib_HH
#include <mxmd/MxMDLib.hh>
#endif

#include <zlib/ZuTime.hh>
#include <zlib/ZmPLock.hh>
#include <zlib/ZmGuard.hh>
#include <zlib/ZmRef.hh>

#include <zlib/ZtString.hh>

#include <zlib/ZiFile.hh>

#include <zlib/ZvCmdHost.hh>

#include <mxbase/MxEngine.hh>

#include <mxmd/MxMDTypes.hh>

class MxMDCore;

class MxMDReplayLink;

class MxMDAPI MxMDReplay : public MxEngine, public MxEngineApp {
public:
  MxMDCore *core() const;

  void init(MxMDCore *core, ZmRef<ZvCf> cf);
  void final();

  bool replay(ZtString path,
      MxDateTime begin = MxDateTime(),
      bool filter = true);
  ZtString stopReplaying();

protected:
  ZmRef<MxAnyLink> createLink(MxID id);

  // commands
  void replayCmd(void *, const ZvCf *args, ZtString &out);

private:
  MxMDReplayLink	*m_link = 0;
};

class MxMDAPI MxMDReplayLink : public MxLink<MxMDReplayLink> {
public:
  MxMDReplayLink(MxID id) : MxLink<MxMDReplayLink>{id} { }

  typedef MxMDReplay Engine;

  ZuInline Engine *engine() const {
    return static_cast<Engine *>(MxAnyLink::engine()); // actually MxAnyTx
  }
  ZuInline MxMDCore *core() const {
    return engine()->core();
  }

  bool ok();
  bool replay(ZtString path, MxDateTime begin, bool filter);
  ZtString stopReplaying();

  // MxAnyLink virtual
  void update(ZvCf *);
  void reset(MxSeqNo rxSeqNo, MxSeqNo txSeqNo);	 // unused

  void connect();
  void disconnect();

  // MxLink CRTP (unused)
  ZuTime reconnInterval(unsigned) { return ZuTime{1}; }

  // MxLink Rx CRTP (unused)
  void process(MxQMsg *) { }
  ZuTime reReqInterval() { return ZuTime{1}; }
  void request(const MxQueue::Gap &prev, const MxQueue::Gap &now) { }
  void reRequest(const MxQueue::Gap &now) { }

  // MxLink Tx CRTP (unused)
  void loaded_(MxQMsg *) { }
  void unloaded_(MxQMsg *) { }

  bool send_(MxQMsg *, bool) { return true; }
  bool resend_(MxQMsg *, bool) { return true; }
  void aborted_(MxQMsg *msg) { }

  bool sendGap_(const MxQueue::Gap &, bool) { return true; }
  bool resendGap_(const MxQueue::Gap &, bool) { return true; }

  void archive_(MxQMsg *msg) { archived(msg->id.seqNo + 1); }
  ZmRef<MxQMsg> retrieve_(MxSeqNo, MxSeqNo) { return nullptr; }

private:
  typedef ZmPLock Lock;
  typedef ZmGuard<Lock> Guard;

  typedef MxMDStream::Msg Msg;

  typedef ZuTuple<ZuBox0(uint16_t), ZuBox0(uint16_t)> Version;

  void read();

private:
  Lock			m_lock;	// serializes replay/stopReplaying
 
  // Rx thread members
  ZtString		m_path;
  ZiFile		m_file;
  ZuRef<Msg>		m_msg;
  ZuTime		m_lastTime;
  ZuTime		m_nextTime;
  bool			m_filter = false;
  Version		m_version;
};

#endif /* MxMDReplay_HH */
