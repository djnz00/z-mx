//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxMD TCP/UDP publisher

#include <mxmd/MxMDCore.hh>

#include <mxmd/MxMDPublisher.hh>

#include <zlib/ZtHexDump.hh>

void MxMDPublisher::init(MxMDCore *core, const ZvCf *cf)
{
  if (!cf->get("id")) cf->set("id", "publish");

  Mx *mx = core->mx(cf->get("mx", "core"));

  if (!mx) throw ZvCf::Required(cf, "mx");

  MxEngine::init(core, this, mx, cf);

  m_snapThread = mx->tid(cf->get("snapThread", true));

  if (!m_snapThread ||
      rxThread() == mx->rxThread() ||
      m_snapThread == mx->rxThread() ||
      m_snapThread == rxThread())
    throw ZtString{} << "publisher misconfigured - thread conflict -"
      " I/O Rx: " << ZuBoxed(mx->rxThread()) <<
      " IPC Rx: " << ZuBoxed(rxThread()) <<
      " Snapshot: " << ZuBoxed(m_snapThread);

  if (ZuString ip = cf->get("interface")) m_interface = ip;
  m_maxQueueSize = cf->getInt("maxQueueSize", 1000, 1000000, 100000);
  m_loginTimeout = cf->getDbl("loginTimeout", 0, 3600, 3);
  m_ackInterval = cf->getDbl("ackInterval", 0, 3600, 10);
  m_reReqMaxGap = cf->getInt("reReqMaxGap", 0, 1000000, 10);
  m_ttl = cf->getInt("ttl", 0, INT_MAX, 1);
  m_nAccepts = cf->getInt("nAccepts", 1, INT_MAX, 8);
  m_loopBack = cf->getBool("loopBack");

  if (ZuString channels = cf->get("channels"))
    updateLinks(channels);

  core->addCmd(
      "publisher.status", "",
      ZvCmdFn::Member<&MxMDPublisher::statusCmd>::fn(this),
      "publisher status",
      "usage: publisher.status\n");
}

#define engineINFO(code) \
    appException(ZeMkLambdaEvent(Info, \
      ([=](auto &s) { s << code; })))

void MxMDPublisher::final()
{
  engineINFO("MxMDPublisher::final()");
}

void MxMDPublisher::updateLinks(ZuString channels)
{
  MxMDChannelCSV csv;
  csv.read(channels, ZvCSVReadFn{this, [](MxMDPublisher *pub, ZuAnyPOD *pod) {
    const MxMDChannel &channel = pod->as<MxMDChannel>();
    pub->m_channels.del(channel.id);
    pub->m_channels.add(channel);
    pub->updateLink(channel.id, nullptr);
  }});
}

ZmRef<MxAnyLink> MxMDPublisher::createLink(MxID id)
{
  return new MxMDPubLink(id);
}

#define linkINFO(code) \
    engine()->appException(ZeMkLambdaEvent(Info, \
      ([=, id = id()](auto &s) { s << code; })))

void MxMDPubLink::update(ZvCf *)
{
  engine()->channel(id(), [this](MxMDChannel *channel) {
    if (ZuUnlikely(!channel)) return;
    m_channel = channel;
  });
  if (m_channel && m_channel->enabled)
    up();
  else
    down();
}

void MxMDPubLink::reset(MxSeqNo, MxSeqNo txSeqNo)
{
  txRun([txSeqNo](Tx *tx) { tx->txReset(txSeqNo); });
}

#define tcpERROR(tcp, io, code) \
  do { \
    MxMDPubLink *link = tcp->link(); \
    MxMDPublisher *engine = link->engine(); \
    engine->appException(ZeMkLambdaEvent(Error, \
      ([=, engineID = engine->id(), id = link->id()](auto &s) { \
	  s << "MxMDPublisher{" << engineID << ':' << id \
	    << "} " << code; }))); \
    link->tcpError(tcp, io); \
  } while (0)

#define udpERROR(udp, io, code) \
  do { \
    MxMDPubLink *link = udp->link(); \
    MxMDPublisher *engine = link->engine(); \
    engine->appException(ZeMkLambdaEvent(Error, \
      ([=, engineID = engine->id(), id = link->id()](auto &s) { \
	  s << "MxMDPublisher{" << engineID << ':' << id \
	    << "} " << code; }))); \
    link->udpError(udp, io); \
  } while (0)

void MxMDPubLink::tcpError(TCP *tcp, ZiIOContext *io)
{
  if (io)
    io->disconnect();
  else if (tcp)
    tcp->close();

  if (tcp)
    engine()->rxInvoke(ZmMkRef(tcp), [](ZmRef<TCP> tcp) {
      tcp->link()->tcpDisconnected(tcp);
    });
}

void MxMDPubLink::udpError(UDP *udp, ZiIOContext *io)
{
  if (io)
    io->disconnect();
  else if (udp)
    udp->close();

  if (!udp)
    reconnect(false);
  else
    engine()->rxInvoke(ZmMkRef(udp), [](ZmRef<UDP> udp) {
      udp->link()->udpDisconnected(udp);
    });
}

void MxMDPubLink::connect()
{
  linkINFO("MxMDPubLink::connect(" << id << ')');

  reset(0, 0);

  m_tcpTbl = new TCPTbl(ZmHashParams().bits(4).loadFactor(1.0).cBits(3).
      init(ZmIDString{} << "MxMDPublisher." << id() << ".TCPTbl"));

  tcpListen();
  udpConnect();
}

void MxMDPubLink::disconnect()
{
  linkINFO("MxMDPubLink::disconnect(" << id << ')');

  m_reconnect = false;
  disconnect_1();
}

void MxMDPubLink::reconnect(bool immediate)
{
  linkINFO("MxMDPubLink::reconnect(" << id << ')');

  engine()->rxInvoke([this, immediate]() { reconnect_(immediate); });
}

void MxMDPubLink::reconnect_(bool immediate)
{
  m_reconnect = true;
  m_immediate = immediate;
  disconnect_1();
}

// - stop listening; all cxns are closed by link disconnect

void MxMDPubLink::disconnect_1()
{
  detach();

  if (m_listenInfo.port)
    mx()->stopListening(m_listenInfo.ip, m_listenInfo.port);

  if (m_tcpTbl) {
    auto i = m_tcpTbl->readIterator();
    while (TCP *tcp = i.iterateKey()) tcp->linkDisconnect();
  }

  txRun([](Tx *tx) {
    tx->stop(); // stop sending
    tx->impl()->disconnect_2();
  });
}

void MxMDPubLink::disconnect_2()
{
  m_udpTx = nullptr;

  // drain the multiplexer before running disconnect_3
  mx()->txRun(ZmFn<>{this, [](MxMDPubLink *link) {
    link->mx()->rxRun(ZmFn<>{link, [](MxMDPubLink *link) {
      link->engine()->rxRun(ZmFn<>{link, [](MxMDPubLink *link) {
	link->disconnect_3();
      }});
    }});
  }});
}

void MxMDPubLink::disconnect_3()
{
  if (m_udp) m_udp->disconnect();

  m_listenInfo = ZiListenInfo();
  m_tcpTbl = nullptr;
  m_udp = nullptr;

  if (m_reconnect) {
    m_reconnect = false;
    MxLink::reconnect(m_immediate);
  } else
    disconnected();
}

// TCP connect

void MxMDPubLink::tcpListen()
{
  ZiIP ip;
  uint16_t port;
  if (!(reconnects() & 1)) {
    ip = m_channel->tcpIP;
    port = m_channel->tcpPort;
  } else {
    if (!(ip = m_channel->tcpIP2)) ip = m_channel->tcpIP;
    if (!(port = m_channel->tcpPort2)) port = m_channel->tcpPort;
  }

  linkINFO("MxMDPubLink::tcpListen(" << id << ") " <<
      ip << ':' << ZuBoxed(port));

  mx()->listen(
      ZiListenFn(this, [](MxMDPubLink *link, const ZiListenInfo &info) {
	link->tcpListening(info);
      }),
      ZiFailFn(this, [](MxMDPubLink *link, bool transient) {
	if (transient)
	  link->reconnect(false);
	else
	  link->engine()->rxRun(
	      ZmFn<>{link, [](MxMDPubLink *link) { link->disconnect(); }});
      }),
      ZiConnectFn(this,
	[](MxMDPubLink *link, const ZiCxnInfo &ci) -> uintptr_t {
	  if (link->state() != MxLinkState::Up)
	    return 0;
	  return (uintptr_t)(new TCP(link, ci));
	}), ip, port, engine()->nAccepts(), ZiCxnOptions());
}
void MxMDPubLink::tcpListening(const ZiListenInfo &info)
{
  m_listenInfo = info;
  linkINFO("MxMDPubLink::tcpListening(" << id << ") " <<
      info.ip << ':' << ZuBoxed(info.port));
}
MxMDPubLink::TCP::TCP(MxMDPubLink *link, const ZiCxnInfo &ci) :
  ZiConnection(link->mx(), ci), m_link(link), m_state(State::Login)
{
}
void MxMDPubLink::TCP::connected(ZiIOContext &io)
{
  m_link->engine()->rxRun(ZmFn<>{ZmMkRef(this), [](TCP *tcp) {
    tcp->link()->tcpConnected(tcp);
  }});
  MxMDStream::TCP::recv<TCP>(new MxQMsg(new Msg()), io,
      [](TCP *tcp, ZmRef<MxQMsg> msg, ZiIOContext &io) {
	tcp->processLogin(ZuMv(msg), io);
      });
  m_link->engine()->rxRun(ZmFn<>{ZmMkRef(this), [](TCP *tcp) {
    if (tcp->state() != State::Login) return;
    tcpERROR(tcp, 0, "TCP login timeout");
  }}, ZmTimeNow(m_link->loginTimeout()), &m_loginTimer);
}
void MxMDPubLink::tcpConnected(MxMDPubLink::TCP *tcp)
{
  linkINFO("MxMDPubLink::tcpConnected(" << id << ") " <<
      tcp->info().remoteIP << ':' << ZuBoxed(tcp->info().remotePort));
  m_tcpTbl->add(tcp);
}

// TCP disconnect

void MxMDPubLink::TCP::linkDisconnect()
{
  m_state = State::LinkDisconnect;
  ZiConnection::disconnect();
}
void MxMDPubLink::TCP::disconnect()
{
  m_state = State::Disconnect;
  ZiConnection::disconnect();
}
void MxMDPubLink::TCP::close()
{
  m_state = State::Disconnect;
  ZiConnection::close();
}
void MxMDPubLink::TCP::disconnected()
{
  mx()->del(&m_loginTimer);
  if (m_state != State::LinkDisconnect && m_link) {
    // tcpERROR(this, 0, "TCP disconnected");
    m_link->engine()->rxRun(ZmFn<>{ZmMkRef(this), [](ZmRef<TCP> tcp) {
      tcp->link()->tcpDisconnected(tcp);
    }});
  }
}
void MxMDPubLink::tcpDisconnected(MxMDPubLink::TCP *tcp)
{
  if (m_tcpTbl) m_tcpTbl->del(tcp);
}

// TCP login, recv

void MxMDPubLink::TCP::processLogin(ZmRef<MxQMsg> msg, ZiIOContext &io)
{
  if (ZuUnlikely(m_state.load_() != State::Login)) {
    tcpERROR(this, &io, "TCP FSM internal error");
    return;
  }

  mx()->del(&m_loginTimer);

  {
    using namespace MxMDStream;
    const Hdr &hdr = msg->ptr<Msg>()->as<Hdr>();
    if (ZuUnlikely(hdr.type != Type::Login)) {
      tcpERROR(this, &io, "TCP unexpected message type");
      return;
    }
    if (!m_link->tcpLogin(hdr.as<Login>())) {
      tcpERROR(this, &io, "TCP invalid login");
      return;
    }
  }

  m_state = State::Sending;

  MxMDStream::TCP::recv<TCP>(ZuMv(msg), io,
      [](TCP *tcp, ZmRef<MxQMsg> msg, ZiIOContext &io) {
	tcp->process(ZuMv(msg), io);
      });

  m_link->engine()->rxRun(ZmFn<>{ZmMkRef(this),
    [](TCP *tcp) {
      if (auto link = tcp->link()) link->snap(tcp);
    }});
}
void MxMDPubLink::TCP::process(ZmRef<MxQMsg>, ZiIOContext &io)
{
  tcpERROR(this, &io, "TCP unexpected message");
  return;
}
bool MxMDPubLink::tcpLogin(const MxMDStream::Login &login)
{
  if (login.username != m_channel->tcpUsername ||
      login.password != m_channel->tcpPassword) return false;
  linkINFO("MxMDPubLink::tcpLogin(" << id << ')');
  return true;
}

// UDP connect

void MxMDPubLink::udpConnect()
{
  ZiIP ip;
  uint16_t port;
  ZiIP resendIP;
  uint16_t resendPort;
  if (!(reconnects() & 1)) {
    ip = m_channel->udpIP;
    port = m_channel->udpPort;
    resendIP = m_channel->resendIP;
    resendPort = m_channel->resendPort;
  } else {
    if (!(ip = m_channel->udpIP2)) ip = m_channel->udpIP;
    if (!(port = m_channel->udpPort2)) port = m_channel->udpPort;
    if (!(resendIP = m_channel->resendIP2))
      resendIP = m_channel->resendIP;
    if (!(resendPort = m_channel->resendPort2))
      resendPort = m_channel->resendPort;
  }
  m_udpAddr = ZiSockAddr(ip, port);
  ZiCxnOptions options;
  options.udp(true);
  if (ip.multicast()) {
    options.multicast(true);
    options.mif(engine()->interface_());
    options.ttl(engine()->ttl());
    options.loopBack(engine()->loopBack());
  }
  mx()->udp(
      ZiConnectFn(this,
	[](MxMDPubLink *link, const ZiCxnInfo &ci) -> uintptr_t {
	  // link state will not be Up until UDP has connected
	  switch ((int)link->state()) {
	    case MxLinkState::Connecting:
	    case MxLinkState::Reconnecting:
	      return (uintptr_t)(new UDP(link, ci));
	    case MxLinkState::DisconnectPending:
	      link->connected();
	    default:
	      return 0;
	  }
	}),
      ZiFailFn(this, [](MxMDPubLink *link, bool transient) {
	if (transient)
	  link->reconnect(false);
	else
	  link->engine()->rxRun(
	      ZmFn<>{link, [](MxMDPubLink *link) { link->disconnect(); }});
      }), ZiIP(), resendPort, ZiIP(), 0, options);
}
MxMDPubLink::UDP::UDP(MxMDPubLink *link, const ZiCxnInfo &ci) :
    ZiConnection(link->mx(), ci), m_link(link),
    m_state(State::Sending) { }
void MxMDPubLink::UDP::connected(ZiIOContext &io)
{
  m_link->engine()->rxRun(ZmFn<>{ZmMkRef(this),
    [](UDP *udp) {
      if (auto link = udp->link()) link->udpConnected(udp);
    }});
  recv(io); // begin receiving UDP packets (resend requests)
}
void MxMDPubLink::udpConnected(MxMDPubLink::UDP *udp)
{
  linkINFO("MxMDPubLink::udpConnected(" << id << ')');

  if (ZuUnlikely(m_udp)) m_udp->disconnect();
  m_udp = udp;
  txRun([](Tx *tx) {
    tx->impl()->udpConnected_2();
    tx->start();
  });
}
void MxMDPubLink::udpConnected_2()
{
  if (ZuUnlikely(m_udpTx)) m_udpTx->disconnect();
  m_udpTx = m_udp; // m_udp access ok
  engine()->rxRun(
      ZmFn<>{this, [](MxMDPubLink *link) { link->udpConnected_3(); }});
}
void MxMDPubLink::udpConnected_3()
{
  attach();
  connected();
}

// UDP disconnect

void MxMDPubLink::UDP::disconnect()
{
  m_state = State::Disconnect;
  ZiConnection::disconnect();
}
void MxMDPubLink::UDP::close()
{
  m_state = State::Disconnect;
  ZiConnection::close();
}
void MxMDPubLink::UDP::disconnected()
{
  if (m_state != State::Disconnect) udpERROR(this, 0, "UDP disconnected");
}
void MxMDPubLink::udpDisconnected(MxMDPubLink::UDP *udp)
{
  if (udp == m_udp) reconnect(false);
}

// UDP recv

void MxMDPubLink::UDP::recv(ZiIOContext &io)
{
  using namespace MxMDStream;
  ZmRef<MxQMsg> msg = new MxQMsg(new Msg());
  MxMDStream::UDP::recv<UDP>(ZuMv(msg), io,
      [](UDP *udp, ZmRef<MxQMsg> msg, ZiIOContext &io) {
	udp->process(ZuMv(msg), io);
      });
}
void MxMDPubLink::UDP::process(ZmRef<MxQMsg> msg, ZiIOContext &io)
{
  using namespace MxMDStream;
  const Hdr &hdr = msg->ptr<Msg>()->as<Hdr>();
  if (ZuUnlikely(hdr.scan(msg->length))) {
    ZtHexDump msg_{"truncated UDP message",
      msg->ptr<Msg>()->ptr(), msg->length};
    m_link->engine()->appException(ZeMkLambdaEvent(Warning,
      ([=, id = m_link->id(), msg_ = ZuMv(msg_)](auto &s) {
	  s << "MxMDPubLink::UDP::process() link " << id << ' ' << msg_;
	})));
  } else {
    const Hdr &hdr = msg->ptr<Msg>()->as<Hdr>();
    if (ZuUnlikely(hdr.type != Type::ResendReq)) {
      // ignore to prevent (probably accidental) DOS
      // udpERROR(this, &io, "UDP unexpected message type");
      return;
    }
    m_link->udpReceived(hdr.as<ResendReq>());
  }

  MxMDStream::UDP::recv<UDP>(ZuMv(msg), io,
      [](UDP *udp, ZmRef<MxQMsg> msg, ZiIOContext &io) {
	udp->process(ZuMv(msg), io);
      });
}
void MxMDPubLink::udpReceived(const MxMDStream::ResendReq &resendReq)
{
  using namespace MxMDStream;
  MxQueue::Gap gap{resendReq.seqNo, resendReq.count};
  txRun([gap](Tx *tx) {
    if (gap.key() < tx->txQueue()->head()) return;
    if (gap.length() > tx->impl()->engine()->reReqMaxGap()) return;
    tx->resend(gap);
  });
}

// snapshot

void MxMDPubLink::snap(ZmRef<TCP> tcp)
{
  txRun([tcp = ZuMv(tcp)](Tx *tx) mutable {
    tx->impl()->mx()->run(tx->impl()->engine()->snapThread(),
	ZmFn<>{ZuMv(tcp),
	  [seqNo = tx->txQueue()->tail()](MxMDPubLink::TCP *tcp) {
	    tcp->snap(seqNo);
	  }});
  });
}
void MxMDPubLink::TCP::snap(MxSeqNo seqNo)
{
  if (!m_link->core()->snapshot(*this, m_link->id(), seqNo))
    m_link->engine()->rxRun(
	ZmFn<>{m_link, [](MxMDPubLink *link) { link->disconnect(); }});
  else
    disconnect();
  m_snapMsg = nullptr;
}
void *MxMDPubLink::TCP::push(unsigned size)
{
  if (ZuUnlikely(m_link->state() != MxLinkState::Up)) return 0;
  m_snapMsg = new Msg();
  return m_snapMsg->ptr();
}
void *MxMDPubLink::TCP::out(
    void *ptr, unsigned length, unsigned type, int shardID)
{
  using namespace MxMDStream;
  Hdr *hdr = new (ptr) Hdr{(uint64_t)0, (uint32_t)0,
    (uint16_t)length, (uint8_t)type, (uint8_t)shardID};
  return hdr->body();
}
void MxMDPubLink::TCP::push2()
{
  unsigned len = m_snapMsg->length();
  ZmRef<MxQMsg> qmsg = new MxQMsg(ZuMv(m_snapMsg), len);
  MxMDStream::TCP::send(this, ZuMv(qmsg));
}

// broadcast

void MxMDPubLink::attach()
{
  if (!m_attached) {
    if (!engine()->attach()) return;
    m_attached = true;
  }
}

bool MxMDPublisher::attach()
{
  if (m_attached++) return true;

  MxMDBroadcast &broadcast = core()->broadcast();

  if (!(m_ring = broadcast.shadow()) || m_ring->attach() != Zi::OK) {
    m_ring = nullptr;
    m_attached = 0;
    return false;
  }

  mx()->wakeFn(rxThread(), ZmFn<>{this, [](MxMDPublisher *pub) {
    pub->rxPush(ZmFn<>{pub, [](MxMDPublisher *pub) { pub->recv(); }});
    pub->wake();
  }});

  rxPush(ZmFn<>{this, [](MxMDPublisher *pub) { pub->recv(); }});

  txRun(ZmFn<>{this, [](MxMDPublisher *pub) { pub->ack(); }},
      ZmTimeNow(ackInterval()), &m_ackTimer);

  return true;
}

void MxMDPubLink::detach()
{
  if (m_attached) {
    engine()->detach();
    m_attached = false;
  }
}

void MxMDPublisher::detach()
{
  if (!m_attached || --m_attached) return; 

  mx()->del(&m_ackTimer);

  if (!m_ring) return; // paranoia

  m_ring->detach();
  m_ring->close();
  m_ring = nullptr;
}

void MxMDPublisher::wake()
{
  MxMDStream::wake(core()->broadcast(), id());
}

void MxMDPublisher::recv()
{
  using namespace MxMDStream;
  if (ZuUnlikely(!m_ring)) return;
  switch ((int)state()) {
    case MxEngineState::Starting:
    case MxEngineState::Running:
      break;
    default:
      mx()->wakeFn(rxThread(), ZmFn<>{});
      wake();
      return;
  }
  const Hdr *hdr;
  for (;;) {
    if (ZuUnlikely(!(hdr = m_ring->shift()))) {
      if (ZuLikely(m_ring->readStatus() == Zi::EndOfFile)) {
	allLinks<MxMDPubLink>([](MxMDPubLink *link) {
	  link->down();
	  return true;
	});
	return;
      }
      continue;
    }
    if (hdr->len > sizeof(Buf)) {
      m_ring->shift2();
      allLinks<MxMDPubLink>([](MxMDPubLink *link) {
	link->down();
	return true;
      });
      core()->raise(ZeMkLambdaEvent(Fatal,
	    ([name = ZtString(m_ring->params().name())](auto &s) {
	      s << '"' << name << "\": "
		"IPC shared memory ring buffer read error - "
		"message too big / corrupt";
	    })));
      return;
    }
    switch ((int)hdr->type) {
      case Type::Wake:
	{
	  const Wake &wake = hdr->as<Wake>();
	  if (ZuUnlikely(wake.id == id())) {
	    m_ring->shift2();
	    return;
	  }
	  m_ring->shift2();
	}
	break;
      case Type::EndOfSnapshot: // ignored
	m_ring->shift2();
	break;
      default:
	allLinks<MxMDPubLink>([hdr](MxMDPubLink *link) {
	  if (link->state() == MxLinkState::Up) link->sendMsg(hdr);
	  return true;
	});
	m_ring->shift2();
	break;
    }
  }
}

void MxMDPubLink::sendMsg(const Hdr *hdr)
{
  if (m_channel->shardID >= 0 && hdr->shard != 0xff &&
      hdr->shard != m_channel->shardID) return;
  using namespace MxMDStream;
  switch ((int)hdr->type) {
    case Type::Wake:
    case Type::EndOfSnapshot:
    case Type::Login:
    case Type::ResendReq:
      return;
  }
  ZuRef<Msg> msg = new Msg();
  unsigned len = sizeof(Hdr) + hdr->len;
  memcpy((void *)msg->ptr(), (void *)hdr, len);
  send(new MxQMsg(msg, len)); // MxLink::send()
}

void MxMDPubLink::loaded_(MxQMsg *msg)
{
  using namespace MxMDStream;
  auto &hdr = msg->ptr<Msg>()->as<Hdr>();
  hdr.seqNo = msg->id.seqNo;
}

void MxMDPubLink::unloaded_(MxQMsg *msg) { } // unused

bool MxMDPubLink::send_(MxQMsg *msg, bool more)
{
  if (ZuUnlikely(!m_udpTx)) return false;
  MxMDStream::UDP::send(m_udpTx.ptr(), msg, m_udpAddr);
  return true;
}

bool MxMDPubLink::resend_(MxQMsg *msg, bool more)
{
  return send_(msg, more);
}

bool MxMDPubLink::sendGap_(const MxQueue::Gap &gap, bool more)
{
  return true;
}

bool MxMDPubLink::resendGap_(const MxQueue::Gap &gap, bool more)
{
  return true;
}

void MxMDPublisher::ack()
{
  if (state() != MxEngineState::Running) return;
  allLinks<MxMDPubLink>(
      [maxQueueSize = maxQueueSize()](MxMDPubLink *link) {
	if (link->state() == MxLinkState::Up) link->ack();
	return true;
      });
  mx()->run(txThread(), ZmFn<>{this, [](MxMDPublisher *pub) {
	pub->ack();
      }}, ZmTimeNow(ackInterval()), &m_ackTimer);
}

void MxMDPubLink::ack()
{
  txInvoke([maxQueueSize = engine()->maxQueueSize()](Tx *tx) {
    MxSeqNo seqNo = tx->txQueue()->tail();
    if (seqNo < maxQueueSize) return;
    tx->ackd(seqNo - maxQueueSize);
  });
}

// commands

void MxMDPublisher::statusCmd(void *, const ZvCf *args, ZtString &out)
{
  int argc = ZuBox<int>(args->get("#"));
  if (argc != 1) throw ZvCmdUsage();
  out.size(512 * nLinks());
  out << "State: " << MxEngineState::name(state()) << '\n';
  allLinks<MxMDPubLink>([&out](MxMDPubLink *link) {
    out << '\n';
    link->status(out);
    return true;
  });
}

void MxMDPubLink::status(ZtString &out)
{
  out << "Link " << id() << ":\n";
  out << "  TCP:    " <<
    m_channel->tcpIP << ':' << m_channel->tcpPort << " | " <<
    m_channel->tcpIP2 << ':' << m_channel->tcpPort2 << '\n';
  out << "  UDP:    " <<
    m_channel->udpIP << ':' << m_channel->udpPort << " | " <<
    m_channel->udpIP2 << ':' << m_channel->udpPort2 << '\n';
  out << "  Resend: " <<
    m_channel->resendIP << ':' << m_channel->resendPort << " | " <<
    m_channel->resendIP2 << ':' << m_channel->resendPort2 << '\n';
  out << "  TCP Username: " << m_channel->tcpUsername <<
    " Password: " << m_channel->tcpPassword << '\n';

  out << "  State: " << MxLinkState::name(state());
  out << '\n';
  if (ZmRef<TCPTbl> tcpTbl = m_tcpTbl) {
    auto i = tcpTbl->readIterator();
    while (TCP *tcp = i.iterateKey()) {
      const ZiCxnInfo &info = tcp->info();
      out << "  TCP "
	<< info.remoteIP << ':' << ZuBoxed(info.remotePort) << ' '
	<< TCP::State::name(tcp->state()) << '\n';
    }
  } else {
    out << "  TCP Disconnected\n";
  }
  out << "  UDP ";
  if (ZmRef<UDP> udp = m_udp) {
    switch (udp->state()) {
      case UDP::State::Sending: out << "Sending"; break;
      case UDP::State::Disconnect: out << "Disconnect"; break;
      default: out << "Unknown"; break;
    }
  } else {
    out << "Disconnected";
  }
  out << "\n  UDP Queue: ";
  {
    thread_local ZmSemaphore sem; // FIXME
    rxInvoke([&out, sem = &sem](Rx *rx) {
      const MxQueue *queue = rx->rxQueue();
      out << *rx;
      out << "\n    " << *queue;
      sem->post();
    });
    sem.wait();
  }
  out << '\n';
}
