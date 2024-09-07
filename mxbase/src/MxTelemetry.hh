//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Mx Telemetry

#ifndef MxTelemetry_HH
#define MxTelemetry_HH

#ifndef MxBaseLib_HH
#include <mxbase/MxBaseLib.hh>
#endif

#include <zlib/ZuInspect.hh>

#include <zlib/ZmFn.hh>

#include <zlib/ZiIP.hh>
#include <zlib/ZiMultiplex.hh>

#include <zlib/Zdb.hh>

#include <mxbase/MxBase.hh>
#include <mxbase/MxMultiplex.hh>
#include <mxbase/MxEngine.hh>

#pragma pack(push, 1)

namespace MxTelemetry {
  namespace Type {
    MxEnumerate(Heap, HashTbl, Thread, Multiplexer, Socket, Queue,
	Engine, Link,
	DBEnv, DBHost, DB);
  }

  struct HdrData {
    uint32_t	len = 0;
    uint32_t	type = 0;
  };

  struct Hdr : public HdrData {
    Hdr() = delete;
    Hdr(uint32_t type, uint32_t len) : HdrData{len, type} { }

    void *body() { return (void *)&this[1]; }
    const void *body() const { return (const void *)&this[1]; }
  };

  using Heap = ZmHeapTelemetry;

  using HashTbl = ZmHashTelemetry;

  using Thread = ZmThreadTelemetry;

  using Multiplexer = ZiMxTelemetry;

  using Socket = ZiCxnTelemetry;

  namespace QueueType {
    MxEnumerate(
	Thread,		// inter-thread ring buffers, etc.
	IPC,		// inter-process ring buffers, etc.
	Rx,		// MxQueue (Rx)
	Tx		// MxQueue (Tx)
	);
  }
  // display sequence:
  //   id, type, full, size, count, seqNo,
  //   inCount, inBytes, outCount, outBytes
  // RAG for queues - count > 50% size - amber; 75% - red
  struct Queue {
    MxIDString	id;		// primary key - is same as Link id for Rx/Tx
    uint64_t	seqNo = 0;	// 0 for Thread, IPC
    uint64_t	count = 0;	// graphable - can overlap, may not be in - out
    uint64_t	inCount = 0;	// graphable (*)
    uint64_t	inBytes = 0;	// graphable
    uint64_t	outCount = 0;	// graphable (*)
    uint64_t	outBytes = 0;	// graphable
    uint32_t	full = 0;	// how many times queue overflowed
    uint32_t	size = 0;	// 0 for Rx, Tx
    uint8_t	type = 0;	// primary key - QueueType
  };

  // followed by
  //   Link[nLinks]
  typedef MxEngine::Telemetry Engine;

  typedef MxAnyLink::Telemetry Link;

  typedef ZdbAny::Telemetry DB;

  typedef ZdbHost::Telemetry DBHost;

  // followed by
  //   DBHost[nHosts]
  //   DB[nDBs]
  typedef ZdbEnv::Telemetry DBEnv;

  typedef ZuLargest<
     Heap, HashTbl, Thread, Multiplexer, Socket, Queue,
     Engine, Link,
     DBEnv, DBHost, DB> Largest;

  struct Buf {
    char	data[sizeof(Hdr) + sizeof(Largest)];

    void *ptr() { return (void *)this; }
    const void *ptr() const { return (const void *)this; }

    Hdr &hdr() {
      Hdr *ZuMayAlias(ptr) = reinterpret_cast<Hdr *>(this);
      return *ptr;
    }
    const Hdr &hdr() const {
      const Hdr *ZuMayAlias(ptr) = reinterpret_cast<const Hdr *>(this);
      return *ptr;
    }

    const void *body() const { return hdr().body(); }
    void *body() { return hdr().body(); }

    template <typename T> const T &as() const {
      const T *ZuMayAlias(ptr) = reinterpret_cast<const T *>(body());
      return *ptr;
    }
    template <typename T> T &as() {
      T *ZuMayAlias(ptr) = reinterpret_cast<T *>(body());
      return *ptr;
    }

    bool scan(unsigned length) const {
      return sizeof(Hdr) + hdr().len > length;
    }

    unsigned length() const {
      return sizeof(Hdr) + hdr().len;
    }
  };
}

#pragma pack(pop)

namespace MxTelemetry {

  struct Msg_HeapID {
    static constexpr const char *id() { return "MxTelemetry.Msg"; }
  };

  template <typename Heap>
  struct Msg_ : public Heap, public ZmPolymorph, public Buf {
    void calcLength() { length = this->Buf::length(); }

    constexpr unsigned size() const { return sizeof(Buf); }

    ZiSockAddr	addr;
    unsigned	length = 0;
  };

  typedef Msg_<ZmHeap<Msg_HeapID, Msg_<ZuNull>>> Msg;

#ifdef DeclFn
#undef DeclFn
#endif
#define DeclFn(Fn, T) \
  template <typename ...Args> \
  ZmRef<Msg> Fn(Args &&...args) { \
    ZmRef<Msg> msg = new Msg(); \
    new (msg->ptr()) Hdr{Type::T, sizeof(T)}; \
    new (msg->body()) T{ZuFwd<Args>(args)...}; \
    msg->calcLength(); \
    return msg; \
  }

  DeclFn(queue, Queue)

#undef DeclFn
#define DeclFn(Fn, T) \
  template <typename Arg, typename ...Args> \
  ZmRef<Msg> Fn(const Arg *arg, Args &&...args) { \
    ZmRef<Msg> msg = new Msg(); \
    new (msg->ptr()) Hdr{Type::T, sizeof(T)}; \
    T *ZuMayAlias(body) = new (msg->body()) T{}; \
    arg->telemetry(*body, ZuFwd<Args>(args)...); \
    msg->calcLength(); \
    return msg; \
  }

  DeclFn(heap, Heap)
  DeclFn(hashTbl, HashTbl)
  DeclFn(thread, Thread)
  DeclFn(multiplexer, Multiplexer)
  DeclFn(socket, Socket)
  DeclFn(engine, Engine)
  DeclFn(link, Link)
  DeclFn(db, DB)
  DeclFn(dbHost, DBHost)
  DeclFn(dbEnv, DBEnv)

#undef DeclFn

  template <typename Cxn, typename L> struct IOLambda_ {
    typedef void (*Fn)(Cxn *, ZmRef<Msg>, ZiIOContext &);
    enum { OK = ZuInspect<L, Fn>::Converts };
  };
  template <typename Cxn, typename L, bool = IOLambda_<Cxn, L>::OK>
  struct IOLambda;
  template <typename Cxn, typename L> struct IOLambda<Cxn, L, true> {
    using T = void;
    static void invoke(ZiIOContext &io) {
      // no, this->x does not imply evaluating (*this).x; the reverse is true
      (*reinterpret_cast<const L *>(0))(
	  static_cast<Cxn *>(io.cxn), io.fn.mvObject<Msg>(), io);
    }
  };

  namespace UDP {
    template <typename Cxn>
    void send(Cxn *cxn, ZmRef<Msg> msg, const ZiSockAddr &addr) {
      msg->addr = addr;
      cxn->send(ZiIOFn{ZuMv(msg), [](Msg *msg, ZiIOContext &io) {
	io.init(ZiIOFn{io.fn.mvObject<Msg>(), [](Msg *msg, ZiIOContext &io) {
	  if (ZuUnlikely((io.offset += io.length) < io.size)) return;
	}}, msg->ptr(), msg->length, 0, msg->addr);
      }});
    }

    template <typename Cxn, typename L>
    typename IOLambda<Cxn, L>::T recv(ZmRef<Msg> msg, ZiIOContext &io, L l) {
      Msg *msg_ = msg.ptr();
      io.init(ZiIOFn{ZuMv(msg), [](Msg *msg, ZiIOContext &io) {
	msg->length = (io.offset += io.length);
	msg->addr = io.addr;
	IOLambda<Cxn, L>::invoke(io);
      }}, msg_->ptr(), msg_->size(), 0);
    }
  }

  class MxBaseAPI Client {
    using Lock = ZmPLock;
    typedef ZmGuard<Lock> Guard;
    typedef ZmReadGuard<Lock> ReadGuard;

  public:
    void init(MxMultiplex *mx, const ZvCf *cf);
    void final();

    ZiMultiplex *mx() const { return m_mx; }

    void start();
    void stop();

    class Cxn;
  friend Cxn;
    class MxBaseAPI Cxn : public ZiConnection {
      friend Client;

    public:
      Cxn(Client *client, const ZiCxnInfo &info) :
	ZiConnection(client->mx(), info), m_client(client) { }

      Client *client() const { return m_client; }

    private:
      void connected(ZiIOContext &io) { m_client->connected(this, io); }
      void disconnected() {
	if (auto client = m_client) client->disconnected(this);
      }

      void recv(ZiIOContext &io) {
	ZmRef<Msg> msg = new Msg();
	UDP::recv<Cxn>(ZuMv(msg), io,
	    [](Cxn *cxn, ZmRef<Msg> msg, ZiIOContext &io) mutable {
	      if (!msg->scan(msg->length))
		cxn->client()->process(ZuMv(msg));
	      cxn->recv(io);
	    });
      }

      Client		*m_client = 0;
    };

    virtual void process(ZmRef<Msg> msg) = 0;

    virtual void error(ZeVEvent e) { ZeLogEvent(ZuMv(e)); }

  private:
    void connected(Cxn *, ZiIOContext &);
    void disconnected(Cxn *);

    ZiMultiplex		*m_mx = 0;

    ZiIP		m_interface;
    ZiIP		m_ip;
    uint16_t		m_port = 0;

    Lock		m_connLock;
      ZmRef<Cxn>	  m_cxn;
  };

  class MxBaseAPI Server {
    using Lock = ZmPLock;
    typedef ZmGuard<Lock> Guard;
    typedef ZmReadGuard<Lock> ReadGuard;

  public:
    void init(MxMultiplex *mx, const ZvCf *cf);
    void final();

    ZiMultiplex *mx() const { return m_mx; }

    void start();
    void stop();

    class Cxn;
  friend Cxn;
    class MxBaseAPI Cxn : public ZiConnection {
    public:
      Cxn(Server *server, const ZiCxnInfo &info) :
	ZiConnection(server->mx(), info), m_server(server) { }

      Server *server() const { return m_server; }

      void transmit(ZmRef<Msg> msg) { // named to avoid colliding with send()
	UDP::send(this, ZuMv(msg), m_server->m_addr);
      }

    private:
      void connected(ZiIOContext &) { m_server->connected(this); }
      void disconnected() {
	if (auto server = m_server) server->disconnected(this);
      }

      Server		*m_server = 0;
    };

    virtual void run(Cxn *) = 0; // app should repeatedly call cxn->send(msg)

    virtual void error(ZeVEvent e) { ZeLogEvent(ZuMv(e)); }

  private:
    void run_();
    void scheduleRun();

    void connected(Cxn *);
    void disconnected(Cxn *);

    ZiMultiplex		*m_mx = 0;

    ZiIP		m_interface;
    ZiIP		m_ip;
    uint16_t		m_port = 0;
    unsigned		m_ttl = 0;
    bool		m_loopBack = false;
    unsigned		m_freq = 0;	// in microseconds

    ZiSockAddr		m_addr;
    ZmScheduler::Timer	m_timer;

    Lock		m_connLock;
      ZmRef<Cxn>	  m_cxn;
  };
}

#endif /* MxTelemetry_HH */
