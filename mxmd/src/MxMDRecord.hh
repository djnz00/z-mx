//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxMD recorder

#ifndef MxMDRecord_HH
#define MxMDRecord_HH

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
#include <zlib/ZmAtomic.hh>

#include <zlib/ZtString.hh>

#include <zlib/ZiFile.hh>

#include <zlib/ZvCmdHost.hh>

#include <mxbase/MxEngine.hh>

#include <mxmd/MxMDTypes.hh>

class MxMDCore;

class MxMDRecLink;

class MxMDRecord : public MxEngine, public MxEngineApp {
public:
  MxMDCore *core() const;

  void init(MxMDCore *core, const ZvCf *cf);
  void final();

  ZuInline unsigned snapThread() const { return m_snapThread; }

  bool record(ZtString path);
  ZtString stopRecording();

protected:
  ZmRef<MxAnyLink> createLink(MxID id);

  // commands
  int recordCmd(void *, const ZvCf *args, ZtString &out);

private:
  unsigned	m_snapThread = 0;

  MxMDRecLink	*m_link = 0;
};

class MxMDAPI MxMDRecLink : public MxLink<MxMDRecLink> {
public:
  MxMDRecLink(MxID id) : MxLink<MxMDRecLink>{id} { }

  typedef MxMDRecord Engine;

  ZuInline Engine *engine() const {
    return static_cast<Engine *>(MxAnyLink::engine()); // actually MxAnyTx
  }
  ZuInline MxMDCore *core() const {
    return engine()->core();
  }

  bool ok();
  bool record(ZtString path);
  ZtString stopRecording();

  // MxAnyLink virtual (mostly unused)
  void update(ZvCf *);
  void reset(MxSeqNo rxSeqNo, MxSeqNo txSeqNo);

  void connect();
  void disconnect();

  // MxLink CRTP (unused)
  ZuTime reconnInterval(unsigned) { return ZuTime{1}; }

  // MxLink Rx CRTP
  void process(MxQMsg *);
  ZuTime reReqInterval() { return ZuTime{1}; } // unused
  void request(const MxQueue::Gap &prev, const MxQueue::Gap &now) { } // unused
  void reRequest(const MxQueue::Gap &now) { } // unused

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
  typedef ZmReadGuard<Lock> ReadGuard;

  typedef MxQueueRx<MxMDRecLink> Rx;

  typedef MxMDStream::Msg Msg;

  int write_(const void *ptr, ZeError *e);

  // Rx thread
  void wake();
  void recv(Rx *rx);

public:
  // snap thread
  void snap();
  void *push(unsigned size);
  void *out(void *ptr, unsigned length, unsigned type, int shardID);
  void push2();

private:
  Lock			m_lock;	// serializes record/stopRecording

  MxSeqNo		m_seqNo = 0;

  Lock			m_fileLock;
    ZtString		  m_path;
    ZiFile		  m_file;

  ZuRef<Msg>		m_snapMsg;
};

#endif /* MxMDRecord_HH */
