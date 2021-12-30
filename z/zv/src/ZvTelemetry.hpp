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

#ifndef ZvTelemetry_HPP
#define ZvTelemetry_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

#include <stdlib.h>
#include <time.h>

#include <zlib/ZuPrint.hpp>
#include <zlib/ZuBox.hpp>
#include <zlib/ZuID.hpp>
#include <zlib/ZuString.hpp>
#include <zlib/ZuStringN.hpp>
#include <zlib/ZuAssert.hpp>

#include <zlib/ZmHeap.hpp>
#include <zlib/ZmHashMgr.hpp>
#include <zlib/ZmThread.hpp>
#include <zlib/ZmScheduler.hpp>
#include <zlib/ZmRWLock.hpp>
#include <zlib/ZmRBTree.hpp>

#include <zlib/ZtEnum.hpp>

#include <zlib/ZePlatform.hpp>

#include <zlib/ZiFile.hpp>

#include <zlib/Zfb.hpp>

#include <zlib/ZvMultiplex.hpp>
#include <zlib/ZfbField.hpp>

#include <zlib/telemetry_fbs.h>
#include <zlib/telreq_fbs.h>

namespace ZvTelemetry {

namespace RAG {
  ZfbEnumValues(RAG, Off, Red, Amber, Green);
}

namespace ThreadPriority {
  ZfbEnumValues(ThreadPriority, RealTime, High, Normal, Low);
}

namespace MxState {
  ZfbEnumValues(MxState,
      Stopped, Starting, Running, Draining, Drained, Stopping);

  int rag(int i) {
    using namespace RAG;
    if (i < 0 || i >= N) return Off;
    static const int values[N] =
      { Red, Amber, Green, Amber, Amber, Amber };
    return values[i];
  }
}

namespace SocketType {
  ZfbEnumValues(SocketType, TCPIn, TCPOut, UDP);
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

namespace EngineState {
  ZfbEnumValues(EngineState,
      Stopped, Starting, Running, Stopping, StartPending, StopPending);

  int rag(int i) {
    using namespace RAG;
    if (i < 0 || i >= N) return Off;
    static const int values[N] =
      { Red, Amber, Green, Amber, Amber, Amber };
    return values[i];
  }
}

namespace DBCacheMode {
  ZfbEnumValues(DBCacheMode, Normal, FullCache)
}

namespace DBHostState {
  ZfbEnumValues(DBHostState,
      Instantiated,
      Initialized,
      Stopped,
      Electing,
      Activating,
      Active,
      Deactivating,
      Inactive,
      Stopping)

  int rag(int i) {
    using namespace RAG;
    if (i < 0 || i >= N) return Off;
    static const int values[N] =
      { Off, Amber, Red, Amber, Amber, Green, Amber, Amber, Amber };
    return values[i];
  }
}

namespace AppRole {
  ZfbEnumValues(AppRole, Dev, Test, Prod)
}

namespace Severity {
  ZfbEnumValues(Severity, Debug, Info, Warning, Error, Fatal)
}

template <typename> struct load;

using Heap_ = ZmHeapTelemetry;
struct Heap : public Heap_, public ZtFieldPrint<Heap> {
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
};
ZfbFields(Heap,
    (((id), (0)), (String), (Ctor(0))),
    (((size), (0)), (Int), (Ctor(6))),
    (((alignment)), (Int), (Ctor(9))),
    (((partition), (0)), (Int), (Ctor(7))),
    (((sharded)), (Bool), (Ctor(8))),
    (((cacheSize)), (Int), (Ctor(1))),
    (((cpuset)), (Bitmap), (Ctor(2))),
    (((cacheAllocs)), (Int), (Ctor(3), Update, Series, Delta)),
    (((heapAllocs)), (Int), (Ctor(4), Update, Series, Delta)),
    (((frees)), (Int), (Ctor(5), Update, Series, Delta)),
    (((allocated, RdFn)), (Int), (Series)),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

using HashTbl_ = ZmHashTelemetry;
struct HashTbl : public HashTbl_, public ZtFieldPrint<HashTbl> {
  HashTbl() = default;
  template <typename ...Args>
  HashTbl(Args &&... args) : HashTbl_{ZuFwd<Args>(args)...} { }

  int rag() const {
    if (resized) return RAG::Red;
    if (effLoadFactor >= loadFactor * 0.8) return RAG::Amber;
    return RAG::Green;
  }
  void rag(int) { } // unused
};
ZfbFields(HashTbl,
    (((id), (0)), (String), (Ctor(0))),
    (((addr), (0)), (Hex), (Ctor(1))),
    (((linear)), (Bool), (Ctor(9))),
    (((bits)), (Int), (Ctor(7))),
    (((cBits)), (Int), (Ctor(8))),
    (((loadFactor)), (Int), (Ctor(2))),
    (((nodeSize)), (Int), (Ctor(4))),
    (((count)), (Int), (Ctor(5), Update, Series)),
    (((effLoadFactor)), (Float), (Ctor(3), Update, Series, NDP(2))),
    (((resized)), (Int), (Ctor(6))),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

using Thread_ = ZmThreadTelemetry;
struct Thread : public Thread_, public ZtFieldPrint<Thread> {
  Thread() = default;
  template <typename ...Args>
  Thread(Args &&... args) : Thread_{ZuFwd<Args>(args)...} { }

  int rag() const {
    if (cpuUsage >= 0.8) return RAG::Red;
    if (cpuUsage >= 0.5) return RAG::Amber;
    return RAG::Green;
  }
  void rag(int) { } // unused
};
ZfbFields(Thread,
    (((name)), (String), (Ctor(0))),
    (((index)), (Int), (Ctor(8))),
    (((tid), (0)), (Int), (Ctor(1))),
    (((cpuUsage)), (Float), (Ctor(4), Update, Series, NDP(2))),
    (((allocaStack)), (Int), (Ctor(5), Update, Series)),
    (((allocaHeap)), (Int), (Ctor(6), Update, Series)),
    (((cpuset)), (Bitmap), (Ctor(3))),
    (((priority)), (Enum, ThreadPriority::Map), (Ctor(10))),
    (((sysPriority)), (Int), (Ctor(7))),
    (((stackSize)), (Int), (Ctor(2))),
    (((partition)), (Int), (Ctor(9))),
    (((main)), (Bool), (Ctor(11))),
    (((detached)), (Bool), (Ctor(12))),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

using Mx_ = ZiMxTelemetry;
struct Mx : public Mx_, public ZtFieldPrint<Mx> {
  Mx() = default;
  template <typename ...Args>
  Mx(Args &&... args) : Mx_{ZuFwd<Args>(args)...} { }

  int rag() const { return MxState::rag(state); }
  void rag(int) { } // unused
};
ZfbFields(Mx,
    (((id), (0)), (String), (Ctor(0))),
    (((state)), (Enum, MxState::Map), (Ctor(10), Update, Series)),
    (((nThreads)), (Int), (Ctor(13))),
    (((rxThread)), (Int), (Ctor(7))),
    (((txThread)), (Int), (Ctor(8))),
    (((priority)), (Int), (Ctor(12))),
    (((stackSize)), (Int), (Ctor(1))),
    (((partition)), (Int), (Ctor(9))),
    (((rxBufSize)), (Int), (Ctor(5))),
    (((txBufSize)), (Int), (Ctor(6))),
    (((queueSize)), (Int), (Ctor(2))),
    (((ll)), (Bool), (Ctor(11))),
    (((spin)), (Int), (Ctor(3))),
    (((timeout)), (Int), (Ctor(4))),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

using Socket_ = ZiCxnTelemetry;
struct Socket : public Socket_, public ZtFieldPrint<Socket> {
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
};
ZfbFields(Socket,
    (((mxID)), (String), (Ctor(0))),
    (((type)), (Enum, SocketType::Map), (Ctor(15))),
    (((remoteIP)), (IP), (Ctor(11))),
    (((remotePort)), (Int), (Ctor(13))),
    (((localIP)), (IP), (Ctor(10))),
    (((localPort)), (Int), (Ctor(12))),
    (((socket), (0)), (Int), (Ctor(1))),
    (((flags)), (Int), (Ctor(14))),
    (((mreqAddr)), (IP), (Ctor(6))),
    (((mreqIf)), (IP), (Ctor(7))),
    (((mif)), (IP), (Ctor(8))),
    (((ttl)), (Int), (Ctor(9))),
    (((rxBufSize)), (Int), (Ctor(2))),
    (((rxBufLen)), (Int), (Ctor(3), Update, Series)),
    (((txBufSize)), (Int), (Ctor(4))),
    (((txBufLen)), (Int), (Ctor(5), Update, Series)),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

// display sequence:
//   id, type, size, full, count, seqNo,
//   inCount, inBytes, outCount, outBytes
struct Queue_ {
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
};
struct Queue : public Queue_, public ZtFieldPrint<Queue> {
  Queue() = default;
  template <typename ...Args>
  Queue(Args &&... args) : Queue_{ZuFwd<Args>(args)...} { }

  // RAG for queues - count > 50% size - amber; 80% - red
  int rag() const {
    if (!size) return RAG::Off;
    if (count * 10 >= (size<<3)) return RAG::Red;
    if ((count<<1) >= size) return RAG::Amber;
    return RAG::Green;
  }
  void rag(int) { } // unused
};
ZfbFields(Queue,
    (((id), (0)), (String), (Ctor(0))),
    (((type), (0)), (Enum, QueueType::Map), (Ctor(9))),
    (((size)), (Int), (Ctor(7))),
    (((full)), (Int), (Ctor(8), Update, Series, Delta)),
    (((count)), (Int), (Ctor(2), Update, Series)),
    (((seqNo)), (Int), (Ctor(1))),
    (((inCount)), (Int), (Ctor(3), Update, Series, Delta)),
    (((inBytes)), (Int), (Ctor(4), Update, Series, Delta)),
    (((outCount)), (Int), (Ctor(5), Update, Series, Delta)),
    (((outBytes)), (Int), (Ctor(6), Update, Series, Delta)),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

// display sequence:
//   id, state, reconnects, rxSeqNo, txSeqNo
struct Link_ {
  ZuID		id;
  ZuID		engineID;
  uint64_t	rxSeqNo = 0;
  uint64_t	txSeqNo = 0;
  uint32_t	reconnects = 0;
  int8_t	state = 0;
};
struct Link : public Link_, public ZtFieldPrint<Link> {
  Link() = default;
  template <typename ...Args>
  Link(Args &&... args) : Link_{ZuFwd<Args>(args)...} { }

  int rag() const { return LinkState::rag(state); }
  void rag(int) { } // unused
};
ZfbFields(Link,
    (((id), (0)), (String), (Ctor(0))),
    (((engineID)), (String), (Ctor(1))),
    (((state)), (Enum, LinkState::Map), (Ctor(5), Update, Series)),
    (((reconnects)), (Int), (Ctor(4), Update, Series, Delta)),
    (((rxSeqNo)), (Int), (Ctor(2), Update, Series, Delta)),
    (((txSeqNo)), (Int), (Ctor(3), Update, Series, Delta)),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

struct Engine_ {
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
};
struct Engine : public Engine_, public ZtFieldPrint<Engine> {
  Engine() = default;
  template <typename ...Args>
  Engine(Args &&... args) : Engine_{ZuFwd<Args>(args)...} { }

  int rag() const { return EngineState::rag(state); }
  void rag(int) { } // unused
};
ZfbFields(Engine,
    (((id), (0)), (String), (Ctor(0))),
    (((type)), (String), (Ctor(1))),
    (((state)), (Enum, EngineState::Map), (Ctor(12), Update, Series)),
    (((nLinks)), (Int), (Ctor(9))),
    (((up)), (Int), (Ctor(6), Update, Series)),
    (((down)), (Int), (Ctor(3), Update, Series)),
    (((disabled)), (Int), (Ctor(4), Update, Series)),
    (((transient)), (Int), (Ctor(5), Update, Series)),
    (((reconn)), (Int), (Ctor(7), Update, Series)),
    (((failed)), (Int), (Ctor(8), Update, Series)),
    (((mxID)), (String), (Ctor(2))),
    (((rxThread)), (Int), (Ctor(10))),
    (((txThread)), (Int), (Ctor(11))),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

// display sequence: 
//   name, id, compress,
//   path, warmUp,
//   minRN, nextRN, fileRN,
//   cacheMode, cacheSize, cacheLoads, cacheMisses,
//   fileCacheSize, fileLoads, fileMisses
//   indexBlkCacheSize, indexBlkLoads, indexBlkMisses
struct DB_ {
  using Path = ZuStringN<124>;
  using Name = ZuStringN<28>;

  Path		path;
  Name		name;			// primary key
  uint64_t	minRN = 0;		// dynamic
  uint64_t	nextRN = 0;		// dynamic
  uint64_t	fileRN = 0;		// dynamic
  uint64_t	cacheLoads = 0;		// dynamic (*)
  uint64_t	cacheMisses = 0;	// dynamic (*)
  uint64_t	fileLoads = 0;		// dynamic
  uint64_t	fileMisses = 0;		// dynamic
  uint64_t	indexBlkLoads = 0;	// dynamic
  uint64_t	indexBlkMisses = 0;	// dynamic
  uint32_t	id = 0;
  uint32_t	cacheSize = 0;
  uint32_t	fileCacheSize = 0;
  uint32_t	indexBlkCacheSize = 0;
  int8_t	cacheMode = -1;		// ZdbCacheMode
  uint8_t	warmUp = 0;
  uint8_t	compress = 0;
};
struct DB : public DB_, public ZtFieldPrint<DB> {
  DB() = default;
  template <typename ...Args>
  DB(Args &&... args) : DB_{ZuFwd<Args>(args)...} { }

  int rag() const {
    unsigned total = cacheLoads + cacheMisses;
    if (!total) return RAG::Off;
    if (cacheMisses * 10 > (total<<3)) return RAG::Red;
    if ((cacheMisses<<1) > total) return RAG::Amber;
    return RAG::Green;
  }
  void rag(int) { } // unused
};
ZfbFields(DB,
    (((name), (0)), (String), (Ctor(1))),
    (((id)), (Int), (Ctor(11))),
    (((compress)), (Int), (Ctor(17))),
    (((cacheMode)), (Enum, DBCacheMode::Map), (Ctor(15))),
    (((cacheSize)), (Int), (Ctor(12))),
    (((path)), (String), (Ctor(0))),
    (((fileCacheSize)), (Int), (Ctor(13))),
    (((indexBlkCacheSize)), (Int), (Ctor(14))),
    (((warmUp)), (Int), (Ctor(16))),
    (((minRN)), (Int), (Ctor(2), Update)),
    (((nextRN)), (Int), (Ctor(3), Update, Series, Delta)),
    (((fileRN)), (Int), (Ctor(4), Update, Series, Delta)),
    (((cacheLoads)), (Int), (Ctor(5), Update, Series, Delta)),
    (((cacheMisses)), (Int), (Ctor(6), Update, Series, Delta)),
    (((fileLoads)), (Int), (Ctor(7), Update, Series, Delta)),
    (((fileMisses)), (Int), (Ctor(8), Update, Series, Delta)),
    (((indexBlkLoads)), (Int), (Ctor(9), Update, Series, Delta)),
    (((indexBlkMisses)), (Int), (Ctor(10), Update, Series, Delta)),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

// display sequence:
//   id, priority, state, voted, ip, port
struct DBHost_ {
  ZiIP		ip;
  uint32_t	id = 0;
  uint32_t	priority = 0;
  uint16_t	port = 0;
  int8_t	state = 0;// RAG: Instantiated - Red; Active - Green; * - Amber
  uint8_t	voted = 0;
};
struct DBHost : public DBHost_, public ZtFieldPrint<DBHost> {
  DBHost() = default;
  template <typename ...Args>
  DBHost(Args &&... args) : DBHost_{ZuFwd<Args>(args)...} { }

  int rag() const { return DBHostState::rag(state); }
  void rag(int) { } // unused
};
ZfbFields(DBHost,
    (((ip)), (IP), (Ctor(0))),
    (((id), (0)), (Int), (Ctor(1))),
    (((priority)), (Int), (Ctor(2))),
    (((state)), (Enum, DBHostState::Map), (Ctor(4), Update, Series)),
    (((voted)), (Bool), (Ctor(5), Update, Series)),
    (((port)), (Int), (Ctor(3))),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

// display sequence: 
//   self, master, prev, next, state, active, recovering, replicating,
//   nDBs, nHosts, nPeers, nCxns,
//   heartbeatFreq, heartbeatTimeout, reconnectFreq, electionTimeout,
//   writeThread
struct DBEnv_ {
  uint32_t	nCxns = 0;
  uint32_t	heartbeatFreq = 0;
  uint32_t	heartbeatTimeout = 0;
  uint32_t	reconnectFreq = 0;
  uint32_t	electionTimeout = 0;
  uint32_t	self = 0;		// primary key - host ID 
  uint32_t	master = 0;		// host ID
  uint32_t	prev = 0;		// ''
  uint32_t	next = 0;		// ''
  uint16_t	writeThread = 0;
  uint8_t	nDBs = 0;
  uint8_t	nHosts = 0;
  uint8_t	nPeers = 0;
  int8_t	state = -1;		// same as hosts[hostID].state
  uint8_t	active = 0;
  uint8_t	recovering = 0;
  uint8_t	replicating = 0;
};
struct DBEnv : public DBEnv_, public ZtFieldPrint<DBEnv> {
  DBEnv() = default;
  template <typename ...Args>
  DBEnv(Args &&... args) : DBEnv_{ZuFwd<Args>(args)...} { }

  int rag() const { return DBHostState::rag(state); }
  void rag(int) { } // unused
};
ZfbFields(DBEnv,
    (((self), (0)), (Int), (Ctor(5))),
    (((master)), (Int), (Ctor(6), Update)),
    (((prev)), (Int), (Ctor(7), Update)),
    (((next)), (Int), (Ctor(8), Update)),
    (((state)), (Enum, DBHostState::Map), (Ctor(13), Update, Series)),
    (((active)), (Int), (Ctor(14), Update)),
    (((recovering)), (Int), (Ctor(15), Update)),
    (((replicating)), (Int), (Ctor(16), Update)),
    (((nDBs)), (Int), (Ctor(10))),
    (((nHosts)), (Int), (Ctor(11))),
    (((nPeers)), (Int), (Ctor(12))),
    (((nCxns)), (Int), (Ctor(0), Update, Series)),
    (((heartbeatFreq)), (Int), (Ctor(1))),
    (((heartbeatTimeout)), (Int), (Ctor(2))),
    (((reconnectFreq)), (Int), (Ctor(3))),
    (((electionTimeout)), (Int), (Ctor(4))),
    (((writeThread)), (Int), (Ctor(9))),
    (((rag, RdFn)), (Enum, RAG::Map), (Series)));

// display sequence:
//   id, role, RAG, uptime, version
struct App_ {
  ZmIDString	id;
  ZmIDString	version;
  ZtDate	uptime;
  int8_t	role = -1;
  int8_t	rag = -1;
};
struct App : public App_, public ZtFieldPrint<App> {
  App() = default;
  template <typename ...Args>
  App(Args &&... args) : App_{ZuFwd<Args>(args)...} { }
};
ZfbFields(App,
    (((id), (0)), (String), (Ctor(0))),
    (((version)), (String), (Ctor(1))),
    (((uptime)), (Time), (Ctor(2), Update)),
    (((role)), (Enum, AppRole::Map), (Ctor(3))),
    (((rag)), (Enum, RAG::Map), (Ctor(4), Update)));

// display sequence:
//   time, severity, tid, message
struct Alert_ {
  ZtDate	time;
  uint32_t	seqNo = 0;
  uint32_t	tid = 0;
  int8_t	severity = -1;
  ZtString	message;
};
struct Alert : public Alert_, public ZtFieldPrint<Alert> {
  Alert() = default;
  template <typename ...Args>
  Alert(Args &&... args) : Alert_{ZuFwd<Args>(args)...} { }
};
ZfbFields(Alert,
    (((time)), (Time), (Ctor(0))),
    (((seqNo)), (Int), (Ctor(1))),
    (((tid)), (Int), (Ctor(2))),
    (((severity)), (Enum, Severity::Map), (Ctor(3))),
    (((message)), (String), (Ctor(4))));

namespace ReqType {
  ZfbEnumValues(ReqType,
      Heap, HashTbl, Thread, Mx, Queue, Engine, DBEnv, App, Alert);
}

namespace TelData {
  ZfbEnumUnion(TelData,
      Heap, HashTbl, Thread, Mx, Socket, Queue, Engine, Link,
      DB, DBHost, DBEnv, App, Alert);
}

using TypeList = ZuTypeList<
  Heap, HashTbl, Thread, Mx, Socket, Queue, Engine, Link,
  DB, DBHost, DBEnv, App, Alert>;

} // ZvTelemetry

#endif /* ZvTelemetry_HPP */
