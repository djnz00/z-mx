//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxMD TCP/UDP subscriber

#ifndef MxMDSubscriber_HH
#define MxMDSubscriber_HH

#ifndef MxMDLib_HH
#include <mxmd/MxMDLib.hh>
#endif

#include <zlib/ZuTime.hh>
#include <zlib/ZmPLock.hh>
#include <zlib/ZmGuard.hh>
#include <zlib/ZmRef.hh>
#include <zlib/ZmSemaphore.hh>

#include <zlib/ZiMultiplex.hh>

#include <mxbase/MxMultiplex.hh>
#include <mxbase/MxEngine.hh>

#include <mxmd/MxMDTypes.hh>
#include <mxmd/MxMDCSV.hh>
#include <mxmd/MxMDChannel.hh>

class MxMDCore;

class MxMDSubLink;

class MxMDAPI MxMDSubscriber : public MxEngine, public MxEngineApp {
public:
  MxMDCore *core() const;

  void init(MxMDCore *core, const ZvCf *cf);
  void final();

  ZuInline ZiIP interface_() const { return m_interface; }
  ZuInline bool filter() const { return m_filter; }
  ZuInline unsigned maxQueueSize() const { return m_maxQueueSize; }
  ZuInline ZuTime loginTimeout() const { return ZuTime(m_loginTimeout); }
  ZuInline ZuTime timeout() const { return ZuTime(m_timeout); }
  ZuInline ZuTime reconnInterval() const { return ZuTime(m_reconnInterval); }
  ZuInline ZuTime reReqInterval() const { return ZuTime(m_reReqInterval); }
  ZuInline unsigned reReqMaxGap() const { return m_reReqMaxGap; }

  void updateLinks(ZuString channels); // update from CSV

private:
  struct ChannelIDAccessor {
    static MxID get(const MxMDChannel &channel) { return channel.id; }
  };
  typedef ZmRBTree<MxMDChannel,
	    ZmRBTreeIndex<ChannelIDAccessor,
	      ZmRBTreeUnique<true,
		ZmRBTreeLock<ZmRWLock> > > > Channels;
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

  void process(MxQMsg *);

  // commands
  void statusCmd(void *, const ZvCf *args, ZtString &out);
  void resendCmd(void *, const ZvCf *args, ZtString &out);

private:
  ZiIP			m_interface;
  bool			m_filter = false;
  unsigned		m_maxQueueSize = 0;
  double		m_loginTimeout = 0.0;
  double		m_timeout = 0.0;
  double		m_reconnInterval = 0.0;
  double		m_reReqInterval = 0.0;
  unsigned		m_reReqMaxGap = 10;

  Channels		m_channels;
};

class MxMDAPI MxMDSubLink : public MxLink<MxMDSubLink> {
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
	Receiving,
	Disconnect
      };
    };

    TCP(MxMDSubLink *link, const ZiCxnInfo &ci);

    ZuInline MxMDSubLink *link() const { return m_link; }

    unsigned state() const { return m_state.load_(); }

    void connected(ZiIOContext &);
    void close();
    void disconnect();
    void disconnected();

    void sendLogin();

  private:
    void processLoginAck(ZmRef<MxQMsg>, ZiIOContext &);
    void process(ZmRef<MxQMsg>, ZiIOContext &);
    bool endOfSnapshot(MxQMsg *, ZiIOContext &);

  private:
    MxMDSubLink		*m_link;

    ZmScheduler::Timer	m_loginTimer;

    ZmAtomic<unsigned>	m_state;
  };

  class UDP;
friend UDP;
  class UDP : public ZiConnection {
  public:
    struct State {
      enum _ {
	Receiving = 0,
	Disconnect
      };
    };

    UDP(MxMDSubLink *link, const ZiCxnInfo &ci);

    ZuInline MxMDSubLink *link() const { return m_link; }

    unsigned state() const { return m_state.load_(); }

    void connected(ZiIOContext &);
    void close();
    void disconnect();
    void disconnected();

    void recv(ZiIOContext &);
    void process(ZmRef<MxQMsg>, ZiIOContext &);

  private:
    MxMDSubLink		*m_link;

    ZmAtomic<unsigned>	m_state;
  };

public:
  MxMDSubLink(MxID id) : MxLink<MxMDSubLink>(id) { }

  typedef MxMDSubscriber Engine;

  ZuInline Engine *engine() const {
    return static_cast<Engine *>(MxAnyLink::engine());
  }
  ZuInline MxMDCore *core() const {
    return engine()->core();
  }

  // MxAnyLink virtual
  void update(ZvCf *);
  void reset(MxSeqNo rxSeqNo, MxSeqNo txSeqNo);

  void connect();		// Rx
  void disconnect();		// Rx - calls disconnect_1()

  // MxLink CTRP
  ZuTime reconnInterval(unsigned) { return engine()->reconnInterval(); }

  // MxLink Rx CRTP
  void process(MxQMsg *);
  ZuTime reReqInterval() { return engine()->reReqInterval(); }
  void request(const MxQueue::Gap &prev, const MxQueue::Gap &now);
  void reRequest(const MxQueue::Gap &now);

  // MxLink Tx CRTP (unused - TCP login bypasses Tx queue)
  void loaded_(MxQMsg *) { }
  void unloaded_(MxQMsg *) { }

  bool send_(MxQMsg *, bool) { return true; }
  bool resend_(MxQMsg *, bool) { return true; }
  void aborted_(MxQMsg *msg) { }

  bool sendGap_(const MxQueue::Gap &, bool) { return true; }
  bool resendGap_(const MxQueue::Gap &, bool) { return true; }

  void archive_(MxQMsg *msg) { archived(msg->id.seqNo + 1); }
  ZmRef<MxQMsg> retrieve_(MxSeqNo, MxSeqNo) { return nullptr; }

  // command support
  void status(ZtString &out);
  ZmRef<MxQMsg> resend(MxSeqNo seqNo, unsigned count);

private:
  // connection management
  void reconnect(bool immediate);	// any thread

  void reconnect_(bool immediate);	// Rx - calls disconnect_1()
  void disconnect_1();			// Rx

  void tcpConnect();
  void tcpConnected(TCP *tcp);			// Rx
  void tcpDisconnected(TCP *tcp);		// Rx
  ZmRef<MxQMsg> tcpLogin();
  void tcpLoginAck();
  void tcpProcess(MxQMsg *);
  void endOfSnapshot(MxSeqNo seqNo);
  void udpConnect();
  void udpConnected(UDP *udp);			// Rx
  void udpDisconnected(UDP *udp);		// Rx
  void udpReceived(ZmRef<MxQMsg>);

  void tcpError(TCP *tcp, ZiIOContext *io);
  void udpError(UDP *udp, ZiIOContext *io);
  void rxQueueTooBig(uint32_t count, uint32_t max);

  // failover
  ZuInline ZuTime loginTimeout() { return engine()->loginTimeout(); }
  ZuInline ZuTime timeout() { return engine()->timeout(); }

  ZuInline void active() { m_active = true; }
  void hbStart();
  void heartbeat();

public:
  ZuTime lastTime() const { return m_lastTime; }
  void lastTime(ZuTime t) { m_lastTime = t; }

private:
  const MxMDChannel	*m_channel = 0;

  // Rx
  ZmScheduler::Timer	m_timer;
  bool			m_active = false;
  unsigned		m_inactive = 0;
  ZuTime		m_lastTime;

  ZiSockAddr		m_udpResendAddr;

  ZmRef<TCP>		m_tcp;
  ZmRef<UDP>		m_udp;
  MxSeqNo		m_snapshotSeqNo;
  bool			m_reconnect = false;
  bool			m_immediate = false;	// immediate reconnect

  ZmSemaphore		m_resendSem;	// test resend semaphore
  Lock			m_resendLock;
    MxQueue::Gap	  m_resendGap;
    ZmRef<MxQMsg>	  m_resendMsg;
};

#endif /* MxMDSubscriber_HH */
