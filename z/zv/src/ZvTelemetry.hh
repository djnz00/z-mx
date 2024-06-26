//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#ifndef ZvTelemetry_HH
#define ZvTelemetry_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <zlib/Zfb.hh>
#include <zlib/ZfbField.hh>

#include <zlib/telemetry_fbs.h>
#include <zlib/telreq_fbs.h>

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
      Off, Amber, Amber, Amber, Red, Amber, Green, Amber, Amber
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
    (((id), (0)), (String), (Ctor<0>)),
    (((size), (0)), (UInt), (Ctor<6>)),
    (((alignment)), (UInt), (Ctor<9>)),
    (((partition), (0)), (UInt), (Ctor<7>)),
    (((sharded)), (Bool), (Ctor<8>)),
    (((cacheSize)), (UInt), (Ctor<1>)),
    (((cpuset)), (Bitmap), (Ctor<2>)),
    (((cacheAllocs)), (UInt), (Ctor<3>, Update, Series, Delta)),
    (((heapAllocs)), (UInt), (Ctor<4>, Update, Series, Delta)),
    (((frees)), (UInt), (Ctor<5>, Update, Series, Delta)),
    (((allocated, RdFn)), (UInt), (Series)),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

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
    (((id), (0)), (String), (Ctor<0>)),
    (((addr), (0)), (UInt), (Ctor<1>, Hex)),
    (((linear)), (Bool), (Ctor<9>)),
    (((bits)), (UInt), (Ctor<7>)),
    (((cBits)), (UInt), (Ctor<8>)),
    (((loadFactor)), (Float), (Ctor<2>)),
    (((nodeSize)), (UInt), (Ctor<4>)),
    (((count)), (UInt), (Ctor<5>, Update, Series)),
    (((effLoadFactor)), (Float), (Ctor<3>, Update, Series, NDP<2>)),
    (((resized)), (UInt), (Ctor<6>)),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

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
    (((name)), (String), (Ctor<0>)),
    (((sid)), (Int), (Ctor<8>)),
    (((tid), (0)), (UInt), (Ctor<1>)),
    (((cpuUsage)), (Float), (Ctor<4>, Update, Series, NDP<2>)),
    (((allocStack)), (UInt), (Ctor<5>, Update, Series)),
    (((allocHeap)), (UInt), (Ctor<6>, Update, Series)),
    (((cpuset)), (Bitmap), (Ctor<3>)),
    (((priority)), (Enum, ThreadPriority::Map), (Ctor<10>)),
    (((sysPriority)), (Int), (Ctor<7>)),
    (((stackSize)), (UInt), (Ctor<2>)),
    (((partition)), (UInt), (Ctor<9>)),
    (((main)), (Bool), (Ctor<11>)),
    (((detached)), (Bool), (Ctor<12>)),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

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
    (((id), (0)), (String), (Ctor<0>)),
    (((state)), (Enum, EngineState::Map), (Ctor<10>, Update, Series)),
    (((nThreads)), (UInt), (Ctor<13>)),
    (((rxThread)), (UInt), (Ctor<7>)),
    (((txThread)), (UInt), (Ctor<8>)),
    (((priority)), (UInt), (Ctor<12>)),
    (((stackSize)), (UInt), (Ctor<1>)),
    (((partition)), (UInt), (Ctor<9>)),
    (((rxBufSize)), (UInt), (Ctor<5>)),
    (((txBufSize)), (UInt), (Ctor<6>)),
    (((queueSize)), (UInt), (Ctor<2>)),
    (((ll)), (Bool), (Ctor<11>)),
    (((spin)), (UInt), (Ctor<3>)),
    (((timeout)), (UInt), (Ctor<4>)),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

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
    (((mxID)), (String), (Ctor<0>)),
    (((type)), (Enum, SocketType::Map), (Ctor<15>)),
    (((remoteIP)), (IP), (Ctor<11>)),
    (((remotePort)), (UInt), (Ctor<13>)),
    (((localIP)), (IP), (Ctor<10>)),
    (((localPort)), (UInt), (Ctor<12>)),
    (((socket), (0)), (UInt), (Ctor<1>)),
    (((flags)), (Flags, ZiCxnFlags::Map), (Ctor<14>)),
    (((mreqAddr)), (IP), (Ctor<6>)),
    (((mreqIf)), (IP), (Ctor<7>)),
    (((mif)), (IP), (Ctor<8>)),
    (((ttl)), (UInt), (Ctor<9>)),
    (((rxBufSize)), (UInt), (Ctor<2>)),
    (((rxBufLen)), (UInt), (Ctor<3>, Update, Series)),
    (((txBufSize)), (UInt), (Ctor<4>)),
    (((txBufLen)), (UInt), (Ctor<5>, Update, Series)),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

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
    (((id), (0)), (String), (Ctor<0>)),
    (((type), (0)), (Enum, QueueType::Map), (Ctor<9>)),
    (((size)), (UInt), (Ctor<7>)),
    (((full)), (UInt), (Ctor<8>, Update, Series, Delta)),
    (((count)), (UInt), (Ctor<2>, Update, Series)),
    (((seqNo)), (UInt), (Ctor<1>)),
    (((inCount)), (UInt), (Ctor<3>, Update, Series, Delta)),
    (((inBytes)), (UInt), (Ctor<4>, Update, Series, Delta)),
    (((outCount)), (UInt), (Ctor<5>, Update, Series, Delta)),
    (((outBytes)), (UInt), (Ctor<6>, Update, Series, Delta)),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

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
    (((id), (0)), (String), (Ctor<0>)),
    (((engineID)), (String), (Ctor<1>)),
    (((state)), (Enum, LinkState::Map), (Ctor<5>, Update, Series)),
    (((reconnects)), (UInt), (Ctor<4>, Update, Series, Delta)),
    (((rxSeqNo)), (UInt), (Ctor<2>, Update, Series, Delta)),
    (((txSeqNo)), (UInt), (Ctor<3>, Update, Series, Delta)),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

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
    (((id), (0)), (String), (Ctor<0>)),
    (((type)), (String), (Ctor<1>)),
    (((state)), (Enum, EngineState::Map), (Ctor<12>, Update, Series)),
    (((nLinks)), (UInt), (Ctor<9>)),
    (((up)), (UInt), (Ctor<6>, Update, Series)),
    (((down)), (UInt), (Ctor<3>, Update, Series)),
    (((disabled)), (UInt), (Ctor<4>, Update, Series)),
    (((transient)), (UInt), (Ctor<5>, Update, Series)),
    (((reconn)), (UInt), (Ctor<7>, Update, Series)),
    (((failed)), (UInt), (Ctor<8>, Update, Series)),
    (((mxID)), (String), (Ctor<2>)),
    (((rxThread)), (UInt), (Ctor<10>)),
    (((txThread)), (UInt), (Ctor<11>)),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

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
  ZmThreadName	writeThread;
  uint64_t	count = 0;			// dynamic
  uint64_t	cacheLoads = 0;			// dynamic (*)
  uint64_t	cacheMisses = 0;		// dynamic (*)
  uint32_t	cacheSize = 0;
  int8_t	cacheMode = -1;			// CacheMode
  uint8_t	warmup = 0;

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
    (((name), (0)), (String), (Ctor<0>)),
    (((cacheMode)), (Enum, CacheMode::Map), (Ctor<7>)),
    (((cacheSize)), (UInt), (Ctor<6>)),
    (((warmup)), (UInt), (Ctor<8>)),
    (((count)), (UInt), (Ctor<3>, Update, Series, Delta)),
    (((cacheLoads)), (UInt), (Ctor<4>, Update, Series, Delta)),
    (((cacheMisses)), (UInt), (Ctor<5>, Update, Series, Delta)),
    (((thread)), (String), (Ctor<1>)),
    (((writeThread)), (String), (Ctor<2>)),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

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
    (((ip)), (IP), (Ctor<0>)),
    (((id), (0)), (ID), (Ctor<1>)),
    (((priority)), (UInt), (Ctor<2>)),
    (((state)), (Enum, DBHostState::Map), (Ctor<4>, Update, Series)),
    (((voted)), (Bool), (Ctor<5>, Update, Series)),
    (((port)), (UInt), (Ctor<3>)),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

// display sequence: 
//   self, leader, prev, next, state, active, recovering, replicating,
//   nDBs, nHosts, nPeers, nCxns,
//   thread, writeThread,
//   heartbeatFreq, heartbeatTimeout, reconnectFreq, electionTimeout
struct DB {
  ZmThreadName	thread;
  ZmThreadName	writeThread;
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
    (((self)), (ID), (Ctor<2>)),
    (((leader)), (ID), (Ctor<3>, Update)),
    (((prev)), (ID), (Ctor<4>, Update)),
    (((next)), (ID), (Ctor<5>, Update)),
    (((state)), (Enum, DBHostState::Map), (Ctor<14>, Update, Series)),
    (((active)), (UInt), (Ctor<15>, Update)),
    (((recovering)), (UInt), (Ctor<16>, Update)),
    (((replicating)), (UInt), (Ctor<17>, Update)),
    (((nTables)), (UInt), (Ctor<11>)),
    (((nHosts)), (UInt), (Ctor<12>)),
    (((nPeers)), (UInt), (Ctor<13>)),
    (((nCxns)), (UInt), (Ctor<6>, Update, Series)),
    (((thread)), (String), (Ctor<0>)),
    (((writeThread)), (String), (Ctor<1>)),
    (((heartbeatFreq)), (UInt), (Ctor<7>)),
    (((heartbeatTimeout)), (UInt), (Ctor<8>)),
    (((reconnectFreq)), (UInt), (Ctor<9>)),
    (((electionTimeout)), (UInt), (Ctor<10>)),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

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
    (((id), (0)), (String), (Ctor<0>)),
    (((version)), (String), (Ctor<1>)),
    (((uptime)), (DateTime), (Ctor<2>, Update)),
    (((role)), (Enum, AppRole::Map), (Ctor<3>)),
    (((rag)), (Enum, RAG::Map), (Ctor<4>, Update)));

// display sequence:
//   time, severity, tid, message
struct Alert {
  ZuDateTime	time;
  uint32_t	seqNo = 0;
  uint32_t	tid = 0;
  int8_t	severity = -1;
  ZtString	message;

  friend ZtFieldPrint ZuPrintType(Alert *);
};
ZfbFields(Alert,
    (((time)), (DateTime), (Ctor<0>)),
    (((seqNo)), (UInt), (Ctor<1>)),
    (((tid)), (UInt), (Ctor<2>)),
    (((severity)), (Enum, Severity::Map), (Ctor<3>)),
    (((message)), (String), (Ctor<4>)));

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
