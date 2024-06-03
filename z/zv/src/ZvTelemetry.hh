//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#ifndef ZvTelemetry_HH
#define ZvTelemetry_HH

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <zlib/Zfb.hh>
#include <zlib/ZfbField.hh>

#include <zlib/zv_telemetry_fbs.h>
#include <zlib/zv_telreq_fbs.h>

namespace ZvTelemetry {

namespace RAG {
  ZfbEnumValues(RAG, Off, Red, Amber, Green);
}

namespace ThreadPriority {
  ZfbEnumMatch(ThreadPriority, ZmThreadPriority,
      RealTime, High, Normal, Low);
}

namespace EngineState {
  ZfbEnumMatch(EngineState, ZmEngineState,
      Stopped, Starting, Running, Stopping, StartPending, StopPending);

  int rag(int i) {
    using namespace RAG;
    if (i < 0 || i >= N) return Off;
    static const int values[N] =
      { Red, Amber, Green, Red, Amber, Red };
    return values[i];
  }
}

namespace SocketType {
  ZfbEnumMatch(SocketType, ZiCxnType, TCPIn, TCPOut, UDP);
}

namespace QueueType {
  ZfbEnumValues(QueueType, Thread, IPC, Rx, Tx);
}

namespace LinkState {
  ZfbEnumValues(LinkState, 
    Down,
    Disabled,
    Deleted,
    Connecting,
    Up,
    ReconnectPending,
    Reconnecting,
    Failed,
    Disconnecting,
    ConnectPending,
    DisconnectPending)

  int rag(int i) {
    using namespace RAG;
    if (i < 0 || i >= N) return Off;
    static const int values[N] =
      { Red, Off, Off, Amber, Green, Amber, Amber, Red, Amber, Amber, Amber };
    return values[i];
  }
}

namespace CacheMode {
  ZfbEnumValues(CacheMode, Normal, All)
}

namespace DBHostState {
  ZfbEnumValues(DBHostState,
      Instantiated,
      Initialized,
      Electing,
      Active,
      Inactive,
      Stopping)

  int rag(int i) {
    using namespace RAG;
    if (i < 0 || i >= N) return Off;
    static const int values[N] = {
      Off, Amber, Amber, Green, Amber, Amber
    };
    return values[i];
  }
}

namespace AppRole {
  ZfbEnumValues(AppRole, Dev, Test, Prod)
}

namespace Severity {
  ZfbEnumValues(Severity, Debug, Info, Warning, Error, Fatal)
}

using Heap_ = ZmHeapTelemetry;
struct Heap : public Heap_ {
  Heap() = default;
  template <typename ...Args>
  Heap(Args &&... args) : Heap_{ZuFwd<Args>(args)...} { }

  uint64_t allocated() const { return (cacheAllocs + heapAllocs) - frees; }
  void allocated(uint64_t) { } // unused

  int rag() const {
    if (!cacheSize) return RAG::Off;
    if (allocated() > cacheSize) return RAG::Red;
    if (heapAllocs) return RAG::Amber;
    return RAG::Green;
  }
  void rag(int) { } // unused

  friend ZtFieldPrint ZuPrintType(Heap *);
};
ZfbFields(Heap,
    (((id), (Keys<0>, Ctor<0>)), (String)),
    (((size), (Keys<0>, Ctor<6>)), (UInt32)),
    (((alignment), (Ctor<9>)), (UInt8)),
    (((partition), (Keys<0>, Ctor<7>)), (UInt16)),
    (((sharded), (Ctor<8>)), (Bool)),
    (((cacheSize), (Ctor<1>)), (UInt64)),
    (((cpuset), (Ctor<2>)), (Bitmap)),
    (((cacheAllocs), (Ctor<3>, Update, Series, Delta)), (UInt64)),
    (((heapAllocs), (Ctor<4>, Update, Series, Delta)), (UInt64)),
    (((frees), (Ctor<5>, Update, Series, Delta)), (UInt64)),
    (((allocated, RdFn), (Synthetic, Series)), (UInt64)),
    (((rag, RdFn), (Series)), (Enum, RAG::Map)));

using HashTbl_ = ZmHashTelemetry;
struct HashTbl : public HashTbl_ {
  HashTbl() = default;
  template <typename ...Args>
  HashTbl(Args &&... args) : HashTbl_{ZuFwd<Args>(args)...} { }

  int rag() const {
    if (resized) return RAG::Red;
    if (effLoadFactor >= loadFactor * 0.8) return RAG::Amber;
    return RAG::Green;
  }
  void rag(int) { } // unused

  friend ZtFieldPrint ZuPrintType(HashTbl *);
};
ZfbFields(HashTbl,
    (((id), (Keys<0>, Ctor<0>)), (String)),
    (((addr), (Keys<0>, Ctor<1>, Hex)), (UInt64)),
    (((linear), (Ctor<9>)), (Bool)),
    (((bits), (Ctor<7>)), (UInt8)),
    (((cBits), (Ctor<8>)), (UInt8)),
    (((loadFactor), (Ctor<2>)), (Float)),
    (((nodeSize), (Ctor<5>)), (UInt32)),
    (((count), (Ctor<4>, Update, Series)), (UInt64)),
    (((effLoadFactor), (Ctor<3>, Update, Series, NDP<2>)), (Float)),
    (((resized), (Ctor<6>)), (UInt32)),
    (((rag, RdFn), (Series)), (Enum, RAG::Map)));

using Thread_ = ZmThreadTelemetry;
struct Thread : public Thread_ {
  Thread() = default;
  template <typename ...Args>
  Thread(Args &&... args) : Thread_{ZuFwd<Args>(args)...} { }

  int rag() const {
    if (cpuUsage >= 0.8) return RAG::Red;
    if (cpuUsage >= 0.5) return RAG::Amber;
    return RAG::Green;
  }
  void rag(int) { } // unused

  friend ZtFieldPrint ZuPrintType(Thread *);
};
// LATER - need to optionally enrich this with thread ring count and overCount
// (i.e. scheduler queue length and DLQ length)
ZfbFields(Thread,
    (((name), (Ctor<0>)), (String)),
    (((sid), (Ctor<8>)), (UInt16)),
    (((tid), (Keys<0>, Ctor<1>)), (UInt64)),
    (((cpuUsage), (Ctor<4>, Update, Series, NDP<2>)), (Float)),
    (((allocStack), (Ctor<5>, Update, Series)), (UInt64)),
    (((allocHeap), (Ctor<6>, Update, Series)), (UInt64)),
    (((cpuset), (Ctor<3>)), (Bitmap)),
    (((priority), (Ctor<10>)), (Enum, ThreadPriority::Map)),
    (((sysPriority), (Ctor<7>)), (Int32)),
    (((stackSize), (Ctor<2>)), (UInt64)),
    (((partition), (Ctor<9>)), (UInt16)),
    (((main), (Ctor<11>)), (Bool)),
    (((detached), (Ctor<12>)), (Bool)),
    (((rag, RdFn), (Series)), (Enum, RAG::Map)));

using Mx_ = ZiMxTelemetry;
struct Mx : public Mx_ {
  Mx() = default;
  template <typename ...Args>
  Mx(Args &&... args) : Mx_{ZuFwd<Args>(args)...} { }

  int rag() const { return EngineState::rag(state); }
  void rag(int) { } // unused

  friend ZtFieldPrint ZuPrintType(Mx *);
};
ZfbFields(Mx,
    (((id), (Keys<0>, Ctor<0>)), (String)),
    (((state), (Ctor<10>, Update, Series)), (Enum, EngineState::Map)),
    (((nThreads), (Ctor<13>)), (UInt8)),
    (((rxThread), (Ctor<7>)), (UInt16)),
    (((txThread), (Ctor<8>)), (UInt16)),
    (((priority), (Ctor<12>)), (UInt8)),
    (((stackSize), (Ctor<1>)), (UInt32)),
    (((partition), (Ctor<9>)), (UInt16)),
    (((rxBufSize), (Ctor<5>)), (UInt32)),
    (((txBufSize), (Ctor<6>)), (UInt32)),
    (((queueSize), (Ctor<2>)), (UInt32)),
    (((ll), (Ctor<11>)), (Bool)),
    (((spin), (Ctor<3>)), (UInt32)),
    (((timeout), (Ctor<4>)), (UInt32)),
    (((rag, RdFn), (Series)), (Enum, RAG::Map)));

using Socket_ = ZiCxnTelemetry;
struct Socket : public Socket_ {
  Socket() = default;
  template <typename ...Args>
  Socket(Args &&... args) : Socket_{ZuFwd<Args>(args)...} { }

  int rag() const {
    if (rxBufLen * 10 >= (rxBufSize<<3) ||
	txBufLen * 10 >= (txBufSize<<3)) return RAG::Red;
    if ((rxBufLen<<1) >= rxBufSize ||
	(txBufLen<<1) >= txBufSize) return RAG::Amber;
    return RAG::Green;
  }
  void rag(int) { } // unused

  friend ZtFieldPrint ZuPrintType(Socket *);
};
ZfbFields(Socket,
    (((mxID), (Ctor<0>)), (String)),
    (((type), (Ctor<15>)), (Enum, SocketType::Map)),
    (((remoteIP), (Ctor<11>)), (IP)),
    (((remotePort), (Ctor<13>)), (UInt16)),
    (((localIP), (Ctor<10>)), (IP)),
    (((localPort), (Ctor<12>)), (UInt16)),
    (((socket), (Keys<0>, Ctor<1>)), (UInt64)),
    (((flags), (Ctor<14>)), (Flags, ZiCxnFlags::Map)),
    (((mreqAddr), (Ctor<6>)), (IP)),
    (((mreqIf), (Ctor<7>)), (IP)),
    (((mif), (Ctor<8>)), (IP)),
    (((ttl), (Ctor<9>)), (UInt32)),
    (((rxBufSize), (Ctor<2>)), (UInt32)),
    (((rxBufLen), (Ctor<3>, Update, Series)), (UInt32)),
    (((txBufSize), (Ctor<4>)), (UInt32)),
    (((txBufLen), (Ctor<5>, Update, Series)), (UInt32)),
    (((rag, RdFn), (Series)), (Enum, RAG::Map)));

// display sequence:
//   id, type, size, full, count, seqNo,
//   inCount, inBytes, outCount, outBytes
struct Queue {
  ZuID		id;		// primary key - same as Link id for Rx/Tx
  uint64_t	seqNo = 0;	// 0 for Thread, IPC
  uint64_t	count = 0;	// dynamic - may not equal in - out
  uint64_t	inCount = 0;	// dynamic (*)
  uint64_t	inBytes = 0;	// dynamic
  uint64_t	outCount = 0;	// dynamic (*)
  uint64_t	outBytes = 0;	// dynamic
  uint32_t	size = 0;	// 0 for Rx, Tx
  uint32_t	full = 0;	// dynamic - how many times queue overflowed
  int8_t	type = -1;	// primary key - QueueType

  // RAG for queues - count > 50% size - amber; 80% - red
  int rag() const {
    if (!size) return RAG::Off;
    if (count * 10 >= (size<<3)) return RAG::Red;
    if ((count<<1) >= size) return RAG::Amber;
    return RAG::Green;
  }
  void rag(int) { } // unused

  friend ZtFieldPrint ZuPrintType(Queue *);
};
ZfbFields(Queue,
    (((id), (Keys<0>, Ctor<0>)), (String)),
    (((type), (Keys<0>, Ctor<9>)), (Enum, QueueType::Map)),
    (((size), (Ctor<7>)), (UInt32)),
    (((full), (Ctor<8>, Update, Series, Delta)), (UInt32)),
    (((count), (Ctor<2>, Update, Series)), (UInt64)),
    (((seqNo), (Ctor<1>)), (UInt64)),
    (((inCount), (Ctor<3>, Update, Series, Delta)), (UInt64)),
    (((inBytes), (Ctor<4>, Update, Series, Delta)), (UInt64)),
    (((outCount), (Ctor<5>, Update, Series, Delta)), (UInt64)),
    (((outBytes), (Ctor<6>, Update, Series, Delta)), (UInt64)),
    (((rag, RdFn), (Series)), (Enum, RAG::Map)));

// display sequence:
//   id, state, reconnects, rxSeqNo, txSeqNo
struct Link {
  ZuID		id;
  ZuID		engineID;
  uint64_t	rxSeqNo = 0;
  uint64_t	txSeqNo = 0;
  uint32_t	reconnects = 0;
  int8_t	state = 0;

  int rag() const { return LinkState::rag(state); }
  void rag(int) { } // unused

  friend ZtFieldPrint ZuPrintType(Link *);
};
ZfbFields(Link,
    (((id), (Keys<0>, Ctor<0>)), (String)),
    (((engineID), (Ctor<1>)), (String)),
    (((state), (Ctor<5>, Update, Series)), (Enum, LinkState::Map)),
    (((reconnects), (Ctor<4>, Update, Series, Delta)), (UInt32)),
    (((rxSeqNo), (Ctor<2>, Update, Series, Delta)), (UInt64)),
    (((txSeqNo), (Ctor<3>, Update, Series, Delta)), (UInt64)),
    (((rag, RdFn), (Series)), (Enum, RAG::Map)));

// display sequence:
//   id, state, nLinks, up, down, disabled, transient, reconn, failed,
//   mxID, rxThread, txThread
struct Engine {
  ZuID		id;		// primary key
  ZuID		type;
  ZuID		mxID;
  uint16_t	down = 0;
  uint16_t	disabled = 0;
  uint16_t	transient = 0;
  uint16_t	up = 0;
  uint16_t	reconn = 0;
  uint16_t	failed = 0;
  uint16_t	nLinks = 0;
  uint16_t	rxThread = 0;
  uint16_t	txThread = 0;
  int8_t	state = -1;

  int rag() const { return EngineState::rag(state); }
  void rag(int) { } // unused

  friend ZtFieldPrint ZuPrintType(Engine *);
};
ZfbFields(Engine,
    (((id), (Keys<0>, Ctor<0>)), (String)),
    (((type), (Ctor<1>)), (String)),
    (((state), (Ctor<12>, Update, Series)), (Enum, EngineState::Map)),
    (((nLinks), (Ctor<9>)), (UInt16)),
    (((up), (Ctor<6>, Update, Series)), (UInt16)),
    (((down), (Ctor<3>, Update, Series)), (UInt16)),
    (((disabled), (Ctor<4>, Update, Series)), (UInt16)),
    (((transient), (Ctor<5>, Update, Series)), (UInt16)),
    (((reconn), (Ctor<7>, Update, Series)), (UInt16)),
    (((failed), (Ctor<8>, Update, Series)), (UInt16)),
    (((mxID), (Ctor<2>)), (String)),
    (((rxThread), (Ctor<10>)), (UInt16)),
    (((txThread), (Ctor<11>)), (UInt16)),
    (((rag, RdFn), (Series)), (Enum, RAG::Map)));

// display sequence: 
//   name, id,
//   path, warmup,
//   count,
//   cacheMode, cacheSize, cacheLoads, cacheMisses,
//   thread
struct DBTable {
  using Name = ZuStringN<28>;

  Name		name;				// primary key
  ZmThreadName	thread;
  uint64_t	count = 0;			// dynamic
  uint64_t	cacheLoads = 0;			// dynamic (*)
  uint64_t	cacheMisses = 0;		// dynamic (*)
  uint32_t	cacheSize = 0;
  int8_t	cacheMode = -1;			// CacheMode
  bool		warmup = 0;

  int rag() const {
    unsigned total = cacheLoads + cacheMisses;
    if (!total) return RAG::Off;
    if (cacheMisses * 10 > (total<<3)) return RAG::Red;
    if ((cacheMisses<<1) > total) return RAG::Amber;
    return RAG::Green;
  }
  void rag(int) { } // unused

  friend ZtFieldPrint ZuPrintType(DBTable *);
};
ZfbFields(DBTable,
    (((name), (Keys<0>, Ctor<0>)), (String)),
    (((cacheMode), (Ctor<7>)), (Enum, CacheMode::Map)),
    (((cacheSize), (Ctor<6>)), (UInt64)),
    (((warmup), (Ctor<8>)), (Bool)),
    (((count), (Ctor<3>, Update, Series, Delta)), (UInt64)),
    (((cacheLoads), (Ctor<4>, Update, Series, Delta)), (UInt64)),
    (((cacheMisses), (Ctor<5>, Update, Series, Delta)), (UInt64)),
    (((thread), (Ctor<1>)), (String)),
    (((rag, RdFn), (Series)), (Enum, RAG::Map)));

// display sequence:
//   id, priority, state, voted, ip, port
struct DBHost {
  ZiIP		ip;
  ZuID		id;
  uint32_t	priority = 0;
  uint16_t	port = 0;
  int8_t	state = 0;// RAG: Instantiated - Red; Active - Green; * - Amber
  uint8_t	voted = 0;

  int rag() const { return DBHostState::rag(state); }
  void rag(int) { } // unused

  friend ZtFieldPrint ZuPrintType(DBHost *);
};
ZfbFields(DBHost,
    (((ip), (Ctor<0>)), (IP)),
    (((id), (Keys<0>, Ctor<1>)), (ID)),
    (((priority), (Ctor<2>)), (UInt32)),
    (((state), (Ctor<4>, Update, Series)), (Enum, DBHostState::Map)),
    (((voted), (Ctor<5>, Update, Series)), (Bool)),
    (((port), (Ctor<3>)), (UInt16)),
    (((rag, RdFn), (Series)), (Enum, RAG::Map)));

// display sequence: 
//   self, leader, prev, next, state, active, recovering, replicating,
//   nDBs, nHosts, nPeers, nCxns,
//   thread,
//   heartbeatFreq, heartbeatTimeout, reconnectFreq, electionTimeout
struct DB {
  ZmThreadName	thread;
  ZuID		self;			// primary key - host ID 
  ZuID		leader;			// host ID
  ZuID		prev;			// ''
  ZuID		next;			// ''
  uint32_t	nCxns = 0;
  uint32_t	heartbeatFreq = 0;
  uint32_t	heartbeatTimeout = 0;
  uint32_t	reconnectFreq = 0;
  uint32_t	electionTimeout = 0;
  uint16_t	nTables = 0;
  uint8_t	nHosts = 0;
  uint8_t	nPeers = 0;
  int8_t	state = -1;		// same as hosts[hostID].state
  uint8_t	active = 0;
  uint8_t	recovering = 0;
  uint8_t	replicating = 0;

  int rag() const { return DBHostState::rag(state); }
  void rag(int) { } // unused

  friend ZtFieldPrint ZuPrintType(DB *);
};
ZfbFields(DB,
    (((self), (Ctor<2>)), (ID)),
    (((leader), (Ctor<3>, Update)), (ID)),
    (((prev), (Ctor<4>, Update)), (ID)),
    (((next), (Ctor<5>, Update)), (ID)),
    (((state), (Ctor<14>, Update, Series)), (Enum, DBHostState::Map)),
    (((active), (Ctor<15>, Update)), (UInt8)),
    (((recovering), (Ctor<16>, Update)), (UInt8)),
    (((replicating), (Ctor<17>, Update)), (UInt8)),
    (((nTables), (Ctor<11>)), (UInt16)),
    (((nHosts), (Ctor<12>)), (UInt8)),
    (((nPeers), (Ctor<13>)), (UInt8)),
    (((nCxns), (Ctor<6>, Update, Series)), (UInt32)),
    (((thread), (Ctor<0>)), (String)),
    (((heartbeatFreq), (Ctor<7>)), (UInt32)),
    (((heartbeatTimeout), (Ctor<8>)), (UInt32)),
    (((reconnectFreq), (Ctor<9>)), (UInt32)),
    (((electionTimeout), (Ctor<10>)), (UInt32)),
    (((rag, RdFn), (Series)), (Enum, RAG::Map)));

// display sequence:
//   id, role, RAG, uptime, version
struct App {
  ZmIDString	id;
  ZmIDString	version;
  ZuDateTime	uptime;
  // LATER - need instanceID (i.e. hostID) for clustered apps
  int8_t	role = -1;
  int8_t	rag = -1;

  friend ZtFieldPrint ZuPrintType(App *);
};
ZfbFields(App,
    (((id), (Keys<0>, Ctor<0>)), (String)),
    (((version), (Ctor<1>)), (String)),
    (((uptime), (Ctor<2>, Update)), (DateTime)),
    (((role), (Ctor<3>)), (Enum, AppRole::Map)),
    (((rag), (Ctor<4>, Update)), (Enum, RAG::Map)));

// display sequence:
//   time, severity, tid, message
struct Alert {
  ZuDateTime	time;
  uint64_t	seqNo = 0;
  uint64_t	tid = 0;
  int8_t	severity = -1;
  ZtString	message;

  friend ZtFieldPrint ZuPrintType(Alert *);
};
ZfbFields(Alert,
    (((time), (Ctor<0>)), (DateTime)),
    (((seqNo), (Ctor<1>)), (UInt64)),
    (((tid), (Ctor<2>)), (UInt64)),
    (((severity), (Ctor<3>)), (Enum, Severity::Map)),
    (((message), (Ctor<4>)), (String)));

namespace ReqType {
  ZfbEnumValues(ReqType,
      Heap, HashTbl, Thread, Mx, Queue, Engine, DB, App, Alert);
}

namespace TelData {
  ZfbEnumUnion(TelData,
      Heap, HashTbl, Thread, Mx, Socket, Queue, Engine, Link,
      DBTable, DBHost, DB, App, Alert);
}

using TypeList = ZuTypeList<
  Heap, HashTbl, Thread, Mx, Socket, Queue, Engine, Link,
  DBTable, DBHost, DB, App, Alert>;

} // ZvTelemetry

#endif /* ZvTelemetry_HH */
