//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxMD TCP/UDP publisher

#ifndef MxMDPublisher_HH
#define MxMDPublisher_HH

#ifndef MxMDLib_HH
#include <mxmd/MxMDLib.hh>
#endif

#include <zlib/ZuTime.hh>
#include <zlib/ZmPLock.hh>
#include <zlib/ZmGuard.hh>
#include <zlib/ZmRef.hh>

#include <zlib/ZiMultiplex.hh>

#include <mxbase/MxMultiplex.hh>
#include <mxbase/MxEngine.hh>

#include <mxmd/MxMDTypes.hh>
#include <mxmd/MxMDCSV.hh>
#include <mxmd/MxMDChannel.hh>
#include <mxmd/MxMDBroadcast.hh>

class MxMDCore;

class MxMDPubLink;

class MxMDAPI MxMDPublisher : public MxEngine, public MxEngineApp {
public:
  MxMDCore *core() const;

  void init(MxMDCore *core, const ZvCf *cf);
  void final();

  ZuInline ZiIP interface_() const { return m_interface; }
  ZuInline unsigned maxQueueSize() const { return m_maxQueueSize; }
  ZuInline ZuTime loginTimeout() const { return ZuTime(m_loginTimeout); }
  ZuInline ZuTime ackInterval() const { return ZuTime(m_ackInterval); }
  ZuInline unsigned reReqMaxGap() const { return m_reReqMaxGap; }
  ZuInline unsigned nAccepts() const { return m_nAccepts; }
  ZuInline unsigned ttl() const { return m_ttl; }
  ZuInline bool loopBack() const { return m_loopBack; }

  void updateLinks(ZuString channels); // update from CSV

private:
  struct ChannelIDAccessor {
    static MxID get(const MxMDChannel &channel) { return channel.id; }
  };
  typedef ZmRBTree<MxMDChannel,
	    ZmRBTreeIndex<ChannelIDAccessor,
	      ZmRBTreeLock<ZmRWLock> > > Channels;
  typedef Channels::Node Channel;

public:
  template <typename L>
  ZuInline void channel(MxID id, L &&l) const {
    if (auto node = m_channels.find(id))
      l(&(node->key()));
    else
      l(nullptr);
  }

  ZmRef<MxAnyLink> createLink(MxID id);

  // broadcast
  bool attach();
  void detach();
  void wake();
  void recv();
  void ack();

  // snapshot
  unsigned snapThread() const { return m_snapThread; }

  // commands
  void statusCmd(void *, const ZvCf *args, ZtString &out);

private:
  unsigned		m_snapThread = 0;
  ZiIP			m_interface;
  unsigned		m_maxQueueSize = 0;
  double		m_loginTimeout = 0.0;
  unsigned		m_reReqMaxGap = 10;
  double		m_ackInterval = 0;
  unsigned		m_nAccepts = 8;
  unsigned		m_ttl = 1;
  bool			m_loopBack = false;

  Channels		m_channels;

  typedef MxMDBroadcast::Ring Ring;

  // Rx thread exclusive
  unsigned		m_attached = 0;
  ZmRef<Ring>		m_ring;

  // Tx queue GC
  ZmScheduler::Timer	m_ackTimer;
};

class MxMDAPI MxMDPubLink : public MxLink<MxMDPubLink> {
  typedef ZmPLock Lock;
  typedef ZmGuard<Lock> Guard;
  typedef ZmReadGuard<Lock> ReadGuard;

  typedef MxMDStream::Msg Msg;

  class TCP;
friend TCP;
  class TCP : public ZiConnection {
  public:
    struct State {
      enum _ {
	Login = 0,
	Sending,
	Disconnect,
	LinkDisconnect,
	N
      };
      static const char *name(int i) {
	static const char *const names[] = {
	  "Login", "Sending", "Disconnect", "LinkDisconnect"
	};
	if (ZuUnlikely(i < 0 || i >= N)) return "Unknown";
	return names[i];
      }
    };

    TCP(MxMDPubLink *link, const ZiCxnInfo &ci);

    ZuInline MxMDPubLink *link() const { return m_link; }

    unsigned state() const { return m_state.load_(); }

    void connected(ZiIOContext &);
    void close();
    void linkDisconnect();
    void disconnect();
    void disconnected();

    void processLogin(ZmRef<MxQMsg>, ZiIOContext &);
    void process(ZmRef<MxQMsg>, ZiIOContext &);

    void snap(MxSeqNo seqNo);
    void *push(unsigned size);
    void *out(void *ptr, unsigned length, unsigned type, int shardID);
    void push2();

  private:
    MxMDPubLink		*m_link;

    ZmScheduler::Timer	m_loginTimer;

    ZmAtomic<unsigned>	m_state;

    ZuRef<Msg>		m_snapMsg;
  };

  struct TCP_HeapID {
    constexpr static const char *id() { return "MxMDPublisher.TCP"; }
  };
  typedef ZmHash<TCP *,
	    ZmHashLock<ZmNoLock,
	      ZmHashObject<ZuObject,
		ZmHashHeapID<TCP_HeapID> > > > TCPTbl;

  class UDP;
friend UDP;
  class UDP : public ZiConnection {
  public:
    struct State {
      enum _ {
	Sending = 0,
	Disconnect
      };
    };

    UDP(MxMDPubLink *link, const ZiCxnInfo &ci);

    ZuInline MxMDPubLink *link() const { return m_link; }

    unsigned state() const { return m_state.load_(); }

    void connected(ZiIOContext &);
    void close();
    void disconnect();
    void disconnected();

    void recv(ZiIOContext &);
    void process(ZmRef<MxQMsg>, ZiIOContext &);

  private:
    MxMDPubLink		*m_link;

    ZmAtomic<unsigned>	m_state;
  };

public:
  typedef MxMDStream::Hdr Hdr;

  MxMDPubLink(MxID id) : MxLink<MxMDPubLink>(id) { }

  typedef MxMDPublisher Engine;

  ZuInline Engine *engine() const {
    return static_cast<Engine *>(MxAnyLink::engine()); // actually MxAnyTx
  }
  ZuInline MxMDCore *core() const {
    return engine()->core();
  }

  ZuInline ZuTime loginTimeout() const { return engine()->loginTimeout(); }

  // MxAnyLink virtual
  void update(ZvCf *);
  void reset(MxSeqNo rxSeqNo, MxSeqNo txSeqNo);

  void connect();		// Rx
  void disconnect();		// Rx

  // MxLink CRTP (unused)
  ZuTime reconnInterval(unsigned) { return ZuTime{1}; }

  // MxLink Rx CRTP (unused)
  void process(MxQMsg *) { }
  ZuTime reReqInterval() { return ZuTime{1}; }
  void request(const MxQueue::Gap &prev, const MxQueue::Gap &now) { }
  void reRequest(const MxQueue::Gap &now) { }

  // MxLink Tx CRTP
  void loaded_(MxQMsg *msg);
  void unloaded_(MxQMsg *msg);

  bool send_(MxQMsg *msg, bool more);
  bool resend_(MxQMsg *msg, bool more);
  void aborted_(MxQMsg *msg) { } // unused

  bool sendGap_(const MxQueue::Gap &gap, bool more);
  bool resendGap_(const MxQueue::Gap &gap, bool more);

  void archive_(MxQMsg *msg) { archived(msg->id.seqNo + 1); } // unused
  ZmRef<MxQMsg> retrieve_(MxSeqNo, MxSeqNo) { return nullptr; } // unused

  // broadcast
  void sendMsg(const Hdr *hdr);
  void ack();

  // command support
  void status(ZtString &out);

private:
  // connection management
  void reconnect(bool immediate);	// any thread

  void reconnect_(bool immediate);	// Rx - calls disconnect_1()
  void disconnect_1();			// Rx
  void disconnect_2();			// Tx
  void disconnect_3();			// Rx

  void tcpListen();
  void tcpListening(const ZiListenInfo &);
  void tcpConnected(TCP *tcp);			// Rx
  void tcpDisconnected(TCP *tcp);		// Rx
  bool tcpLogin(const MxMDStream::Login &);
  void udpConnect();
  void udpConnected(UDP *udp);			// Rx
  void udpConnected_2();			// Tx
  void udpConnected_3();			// Rx
  void udpDisconnected(UDP *udp);		// Rx
  void udpReceived(const MxMDStream::ResendReq &);

  void tcpError(TCP *tcp, ZiIOContext *io);
  void udpError(UDP *udp, ZiIOContext *io);

  // broadcast
  void attach();
  void detach();

  // snapshot
  void snap(ZmRef<TCP> tcp);

private:
  typedef MxQueueRx<MxMDPubLink> Rx;
  typedef MxQueueTx<MxMDPubLink> Tx;

  const MxMDChannel	*m_channel = 0;

  ZiSockAddr		m_udpAddr;

  // Engine Rx thread exclusive
  ZiListenInfo	 	m_listenInfo;
  ZmRef<TCPTbl>	 	m_tcpTbl;
  ZmRef<UDP>		m_udp;
  bool			m_attached = false;
  bool			m_reconnect = false;
  bool			m_immediate = false;	// immediate reconnect

  // Engine Tx thread exclusive
  ZmRef<UDP>		m_udpTx;
};

#endif /* MxMDPublisher_HH */
