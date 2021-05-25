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
#include <zlib/ZvFBField.hpp>

#include <zlib/telemetry_fbs.h>
#include <zlib/telreq_fbs.h>

class ZvEngine;
class ZvAnyLink;

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
struct Heap : public Heap_, public ZvFieldPrint<Heap> {
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

ZvFBFields(Heap,
    (, String, id, (Ctor(0), Primary)),
    (, Int, size, (Ctor(6), Primary)),
    (, Int, alignment, (Ctor(9))),
    (, Int, partition, (Ctor(7), Primary)),
    (, Bool, sharded, (Ctor(8))),
    (, Int, cacheSize, (Ctor(1))),
    (, Bitmap, cpuset, (Ctor(2))),
    (, Int, cacheAllocs, (Ctor(3), Update, Series, Delta)),
    (, Int, heapAllocs, (Ctor(4), Update, Series, Delta)),
    (, Int, frees, (Ctor(5), Update, Series, Delta)),
    (RdFn, Int, allocated, (Series)),
    (RdFn, Enum, rag, (Series), RAG::Map));

using HashTbl_ = ZmHashTelemetry;
struct HashTbl : public HashTbl_, public ZvFieldPrint<HashTbl> {
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
ZvFBFields(HashTbl,
    (, String, id, (Ctor(0), Primary)),
    (, Hex, addr, (Ctor(1), Primary)),
    (, Bool, linear, (Ctor(9))),
    (, Int, bits, (Ctor(7))),
    (, Int, cBits, (Ctor(8))),
    (, Int, loadFactor, (Ctor(2))),
    (, Int, nodeSize, (Ctor(4))),
    (, Int, count, (Ctor(5), Update, Series)),
    (, Float, effLoadFactor, (Ctor(3), Update, Series, NDP(2))),
    (, Int, resized, (Ctor(6))),
    (RdFn, Enum, rag, (Series), RAG::Map));

using Thread_ = ZmThreadTelemetry;
struct Thread : public Thread_, public ZvFieldPrint<Thread> {
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
ZvFBFields(Thread,
    (, String, name, (Ctor(0))),
    (, Int, index, (Ctor(6))),
    (, Int, tid, (Ctor(1), Primary)),
    (, Float, cpuUsage, (Ctor(4), Update, Series, NDP(2))),
    (, Bitmap, cpuset, (Ctor(3))),
    (, Enum, priority, Ctor(8), ThreadPriority::Map),
    (, Int, sysPriority, (Ctor(5))),
    (, Int, stackSize, (Ctor(2))),
    (, Int, partition, (Ctor(7))),
    (, Bool, main, (Ctor(9))),
    (, Bool, detached, (Ctor(10))),
    (RdFn, Enum, rag, (Series), RAG::Map));

using Mx_ = ZiMxTelemetry;
struct Mx : public Mx_, public ZvFieldPrint<Mx> {
  Mx() = default;
  template <typename ...Args>
  Mx(Args &&... args) : Mx_{ZuFwd<Args>(args)...} { }

  int rag() const { return MxState::rag(state); }
  void rag(int) { } // unused
};
ZvFBFields(Mx,
    (, String, id, (Ctor(0), Primary)),
    (, Enum, state, (Ctor(10), Update, Series), MxState::Map),
    (, Int, nThreads, (Ctor(13))),
    (, Int, rxThread, (Ctor(7))),
    (, Int, txThread, (Ctor(8))),
    (, Int, priority, (Ctor(12))),
    (, Int, stackSize, (Ctor(1))),
    (, Int, partition, (Ctor(9))),
    (, Int, rxBufSize, (Ctor(5))),
    (, Int, txBufSize, (Ctor(6))),
    (, Int, queueSize, (Ctor(2))),
    (, Bool, ll, (Ctor(11))),
    (, Int, spin, (Ctor(3))),
    (, Int, timeout, (Ctor(4))),
    (RdFn, Enum, rag, (Series), RAG::Map));

using Socket_ = ZiCxnTelemetry;
struct Socket : public Socket_, public ZvFieldPrint<Socket> {
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

ZvFBFields(Socket,
    (, String, mxID, (Ctor(0))),
    (, Enum, type, (Ctor(15)), SocketType::Map),
    (, IP, remoteIP, (Ctor(11))),
    (, Int, remotePort, (Ctor(13))),
    (, IP, localIP, (Ctor(10))),
    (, Int, localPort, (Ctor(12))),
    (, Int, socket, (Ctor(1), Primary)),
    (, Int, flags, (Ctor(14))),
    (, IP, mreqAddr, (Ctor(6))),
    (, IP, mreqIf, (Ctor(7))),
    (, IP, mif, (Ctor(8))),
    (, Int, ttl, (Ctor(9))),
    (, Int, rxBufSize, (Ctor(2))),
    (, Int, rxBufLen, (Ctor(3), Update, Series)),
    (, Int, txBufSize, (Ctor(4))),
    (, Int, txBufLen, (Ctor(5), Update, Series)),
    (RdFn, Enum, rag, (Series), RAG::Map));

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
struct Queue : public Queue_, public ZvFieldPrint<Queue> {
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
ZvFBFields(Queue,
    (, String, id, (Ctor(0), Primary)),
    (, Enum, type, (Ctor(9), Primary), QueueType::Map),
    (, Int, size, (Ctor(7))),
    (, Int, full, (Ctor(8), Update, Series, Delta)),
    (, Int, count, (Ctor(2), Update, Series)),
    (, Int, seqNo, (Ctor(1))),
    (, Int, inCount, (Ctor(3), Update, Series, Delta)),
    (, Int, inBytes, (Ctor(4), Update, Series, Delta)),
    (, Int, outCount, (Ctor(5), Update, Series, Delta)),
    (, Int, outBytes, (Ctor(6), Update, Series, Delta)),
    (RdFn, Enum, rag, (Series), RAG::Map));

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
struct Link : public Link_, public ZvFieldPrint<Link> {
  Link() = default;
  template <typename ...Args>
  Link(Args &&... args) : Link_{ZuFwd<Args>(args)...} { }

  int rag() const { return LinkState::rag(state); }
  void rag(int) { } // unused
};
ZvFBFields(Link,
    (, String, id, (Ctor(0), Primary)),
    (, String, engineID, (Ctor(1))),
    (, Enum, state, (Ctor(5), Update, Series), LinkState::Map),
    (, Int, reconnects, (Ctor(4), Update, Series, Delta)),
    (, Int, rxSeqNo, (Ctor(2), Update, Series, Delta)),
    (, Int, txSeqNo, (Ctor(3), Update, Series, Delta)),
    (RdFn, Enum, rag, (Series), RAG::Map));

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
struct Engine : public Engine_, public ZvFieldPrint<Engine> {
  Engine() = default;
  template <typename ...Args>
  Engine(Args &&... args) : Engine_{ZuFwd<Args>(args)...} { }

  int rag() const { return EngineState::rag(state); }
  void rag(int) { } // unused
};
ZvFBFields(Engine,
    (, String, id, (Ctor(0), Primary)),
    (, String, type, (Ctor(1))),
    (, Enum, state, (Ctor(12), Update, Series), EngineState::Map),
    (, Int, nLinks, (Ctor(9))),
    (, Int, up, (Ctor(6), Update, Series)),
    (, Int, down, (Ctor(3), Update, Series)),
    (, Int, disabled, (Ctor(4), Update, Series)),
    (, Int, transient, (Ctor(5), Update, Series)),
    (, Int, reconn, (Ctor(7), Update, Series)),
    (, Int, failed, (Ctor(8), Update, Series)),
    (, String, mxID, (Ctor(2))),
    (, Int, rxThread, (Ctor(10))),
    (, Int, txThread, (Ctor(11))),
    (RdFn, Enum, rag, (Series), RAG::Map));

// display sequence: 
//   name, id, recSize, compress, cacheMode, cacheSize,
//   path, fileSize, fileRecs, filesMax, preAlloc,
//   minRN, nextRN, fileRN,
//   cacheLoads, cacheMisses, fileLoads, fileMisses
struct DB_ {
  using Path = ZuStringN<124>;
  using Name = ZuStringN<28>;

  Path		path;
  Name		name;		// primary key
  uint64_t	fileSize = 0;
  uint64_t	minRN = 0;	// dynamic
  uint64_t	nextRN = 0;	// dynamic
  uint64_t	fileRN = 0;	// dynamic
  uint64_t	cacheLoads = 0;	// dynamic (*)
  uint64_t	cacheMisses = 0;// dynamic (*)
  uint64_t	fileLoads = 0;	// dynamic
  uint64_t	fileMisses = 0;	// dynamic
  uint32_t	id = 0;
  uint32_t	preAlloc = 0;
  uint32_t	recSize = 0;
  uint32_t	fileRecs = 0;
  uint32_t	cacheSize = 0;
  uint32_t	filesMax = 0;
  uint8_t	compress = 0;
  int8_t	cacheMode = -1;	// ZdbCacheMode
};
struct DB : public DB_, public ZvFieldPrint<DB> {
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
ZvFBFields(DB,
    (, String, name, (Ctor(1), Primary)),
    (, Int, id, (Ctor(10))),
    (, Int, recSize, (Ctor(12))),
    (, Int, compress, (Ctor(16))),
    (, Enum, cacheMode, (Ctor(17)), DBCacheMode::Map),
    (, Int, cacheSize, (Ctor(14))),
    (, String, path, (Ctor(0))),
    (, Int, fileSize, (Ctor(2))),
    (, Int, fileRecs, (Ctor(13))),
    (, Int, filesMax, (Ctor(15))),
    (, Int, preAlloc, (Ctor(11))),
    (, Int, minRN, (Ctor(3), Update)),
    (, Int, nextRN, (Ctor(4), Update, Series, Delta)),
    (, Int, fileRN, (Ctor(5), Update, Series, Delta)),
    (, Int, cacheLoads, (Ctor(6), Update, Series, Delta)),
    (, Int, cacheMisses, (Ctor(7), Update, Series, Delta)),
    (, Int, fileLoads, (Ctor(8), Update, Series, Delta)),
    (, Int, fileMisses, (Ctor(9), Update, Series, Delta)),
    (RdFn, Enum, rag, (Series), RAG::Map));

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
struct DBHost : public DBHost_, public ZvFieldPrint<DBHost> {
  DBHost() = default;
  template <typename ...Args>
  DBHost(Args &&... args) : DBHost_{ZuFwd<Args>(args)...} { }

  int rag() const { return DBHostState::rag(state); }
  void rag(int) { } // unused
};
ZvFBFields(DBHost,
    (, IP, ip, (Ctor(0))),
    (, Int, id, (Ctor(1), Primary)),
    (, Int, priority, (Ctor(2))),
    (, Enum, state, (Ctor(4), Update, Series), DBHostState::Map),
    (, Bool, voted, (Ctor(5), Update, Series)),
    (, Int, port, (Ctor(3))),
    (RdFn, Enum, rag, (Series), RAG::Map));

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
struct DBEnv : public DBEnv_, public ZvFieldPrint<DBEnv> {
  DBEnv() = default;
  template <typename ...Args>
  DBEnv(Args &&... args) : DBEnv_{ZuFwd<Args>(args)...} { }

  int rag() const { return DBHostState::rag(state); }
  void rag(int) { } // unused
};
ZvFBFields(DBEnv,
    (, Int, self, (Ctor(5), Primary)),
    (, Int, master, (Ctor(6), Update)),
    (, Int, prev, (Ctor(7), Update)),
    (, Int, next, (Ctor(8), Update)),
    (, Enum, state, (Ctor(13), Update, Series), DBHostState::Map),
    (, Int, active, (Ctor(14), Update)),
    (, Int, recovering, (Ctor(15), Update)),
    (, Int, replicating, (Ctor(16), Update)),
    (, Int, nDBs, (Ctor(10))),
    (, Int, nHosts, (Ctor(11))),
    (, Int, nPeers, (Ctor(12))),
    (, Int, nCxns, (Ctor(0), Update, Series)),
    (, Int, heartbeatFreq, (Ctor(1))),
    (, Int, heartbeatTimeout, (Ctor(2))),
    (, Int, reconnectFreq, (Ctor(3))),
    (, Int, electionTimeout, (Ctor(4))),
    (, Int, writeThread, (Ctor(9))),
    (RdFn, Enum, rag, (Series), RAG::Map));

// display sequence:
//   id, role, RAG, uptime, version
struct App_ {
  ZmIDString	id;
  ZmIDString	version;
  ZtDate	uptime;
  int8_t	role = -1;
  int8_t	rag = -1;
};
struct App : public App_, public ZvFieldPrint<App> {
  App() = default;
  template <typename ...Args>
  App(Args &&... args) : App_{ZuFwd<Args>(args)...} { }
};
ZvFBFields(App,
    (, String, id, (Ctor(0), Primary)),
    (, String, version, (Ctor(1))),
    (, Time, uptime, (Ctor(2), Update)),
    (, Enum, role, (Ctor(3)), AppRole::Map),
    (, Enum, rag, (Ctor(4), Update), RAG::Map));

// display sequence:
//   time, severity, tid, message
struct Alert_ {
  ZtDate	time;
  uint32_t	seqNo = 0;
  uint32_t	tid = 0;
  int8_t	severity = -1;
  ZtString	message;
};
struct Alert : public Alert_, public ZvFieldPrint<Alert> {
  Alert() = default;
  template <typename ...Args>
  Alert(Args &&... args) : Alert_{ZuFwd<Args>(args)...} { }
};
ZvFBFields(Alert,
    (, Time, time, (Ctor(0))),
    (, Int, seqNo, (Ctor(1))),
    (, Int, tid, (Ctor(2))),
    (, Enum, severity, (Ctor(3)), Severity::Map),
    (, String, message, (Ctor(4))));

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
