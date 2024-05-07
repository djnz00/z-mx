#include <mxbase/MxEngine.hh>

// MxEngine connectivity framework unit smoke test

class Mgr : public MxEngineMgr {
public:
  virtual ~Mgr() { }

  // Engine Management
  void addEngine(MxEngine *) { }
  void delEngine(MxEngine *) { }
  void engineState(MxEngine *engine, MxEnum prev, MxEnum next) {
    ZeLOG(Info,
      ([id = engine->id(), prev, next](auto &s) {
	s << "engine " << id << ' ' <<
	MxEngineState::name(prev) << "->" << MxEngineState::name(next); }));
  }

  // Link Management
  void updateLink(MxAnyLink *) { }
  void delLink(MxAnyLink *) { }
  void linkState(MxAnyLink *link, MxEnum prev, MxEnum next) {
    ZeLOG(Info,
      ([id = link->id(), prev, next](auto &s) {
	s << "link " << id << ' ' <<
	MxLinkState::name(prev) << "->" << MxLinkState::name(next); }));
  }

  // Pool Management
  void updateTxPool(MxAnyTxPool *) { }
  void delTxPool(MxAnyTxPool *) { }

  // Queue Management
  void addQueue(MxID id, bool tx, MxQueue *) { }
  void delQueue(MxID id, bool tx) { }

  // Traffic Logging
  void log(MxMsgID, MxTraffic) { }
};

class App : public MxEngineApp {
public:
  App() { }
  virtual ~App() { }

  void final() { }

  ZmRef<MxAnyLink> createLink(MxID id);
};

enum { Connected, Disconnected, Reconnect };

class Engine : public MxEngine {
public:
  // Engine() { }

  void init(Mgr *mgr, App *app, Mx *mx, const ZvCf *cf);

  void up() { ZeLOG(Info, "UP"); }
  void down() { ZeLOG(Info, "DOWN"); }

  ZuTime reconnInterval() { return ZuTime(m_reconnInterval); }
  ZuTime reReqInterval() { return ZuTime(m_reReqInterval); }

  int action() const { return m_action; }
  void action(int v) { m_action = v; }

  void connected() { m_connected.post(); }
  void waitConnected() { m_connected.wait(); }
  void disconnected() { m_disconnected.post(); }
  void waitDisconnected() { m_disconnected.wait(); }
  void reconnect() { m_reconnect.post(); }
  void waitReconnect() { m_reconnect.wait(); }

private:
  ZuBox<double>	m_reconnInterval;
  ZuBox<double>	m_reReqInterval;
  
  int		m_action = Connected;

  ZmSemaphore	m_connected;
  ZmSemaphore	m_disconnected;
  ZmSemaphore	m_reconnect;
};

#define linkINFO(code) \
    engine()->appException(ZeEVENT(Info, \
      ([=, id = id()](const ZeEvent &, ZmStream &out) { out << code; })))

class Link : public MxLink<Link> {
public:
  Link(MxID id) : MxLink<Link>(id) { }

  ZuInline Engine *engine() {
    return static_cast<Engine *>(MxAnyLink::engine()); // actually MxAnyTx
  }

  // MxLink CTRP
  ZuTime reconnInterval(unsigned) { return engine()->reconnInterval(); }

  // MxAnyLink virtual
  void update(const ZvCf *cf) { }
  void reset(MxSeqNo rxSeqNo, MxSeqNo txSeqNo) { }

  void connect() {
    linkINFO("connect(): " << id);
    switch (engine()->action()) {
      case Connected:
	connected();
	engine()->connected();
	break;
      case Disconnected:
	disconnected();
	engine()->disconnected();
	break;
      case Reconnect:
	reconnect(false);
	engine()->reconnect();
	break;
    }
  }
  void disconnect() {
    linkINFO("disconnect(): " << id);
    disconnected();
    engine()->disconnected();
  }

  // MxLink Rx CRTP
  void process(MxQMsg *msg) { }
  ZuTime reReqInterval() { return engine()->reReqInterval(); }
  void request(const MxQueue::Gap &prev, const MxQueue::Gap &now) { }
  void reRequest(const MxQueue::Gap &now) { }

  // MxLink Tx CRTP
  void loaded_(MxQMsg *) { }
  void unloaded_(MxQMsg *) { }

  bool send_(MxQMsg *, bool more) { return true; }
  bool resend_(MxQMsg *, bool more) { return true; }
  void aborted_(MxQMsg *msg) { }

  bool sendGap_(const MxQueue::Gap &gap, bool more) { return true; }
  bool resendGap_(const MxQueue::Gap &gap, bool more) { return true; }

  void archive_(MxQMsg *msg) { archived(msg->id.seqNo + 1); }
  ZmRef<MxQMsg> retrieve_(MxSeqNo, MxSeqNo) { return nullptr; }
};

ZmRef<MxAnyLink> App::createLink(MxID id) { return new Link(id); }

void Engine::init(Mgr *mgr, App *app, Mx *mx, const ZvCf *cf)
{
  MxEngine::init(mgr, app, mx, cf);
  m_reconnInterval = cf->getDbl("reconnInterval", 0, 3600, 1);
  m_reReqInterval = cf->getDbl("reReqInterval", 0, 3600, 1);
  if (ZmRef<ZvCf> linksCf = cf->getCf("links")) {
    ZvCf::Iterator i(linksCf);
    ZuString id;
    while (ZmRef<ZvCf> linkCf = i.subset(id))
      MxEngine::updateLink(id, linkCf);
  }
}

int main()
{
  ZeLog::init("MxEngineTest");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::fileSink(ZeSinkOptions{}.path("&2")));
  ZeLog::start();

  ZmRef<ZvCf> cf = new ZvCf();
  cf->fromString(
      "id Engine\n"
      "mx {\n"
	"nThreads 4\n"		// thread IDs are 1-based
	"rxThread 1\n"		// I/O Rx
	"txThread 2\n"		// I/O Tx
	"threads { 1 { isolated 1 } 2 { isolated 1 } 3 { isolated 1 } }\n"
      "}\n"
      "rxThread 3\n"		// App Rx
      "txThread 2\n"		// App Tx (same as I/O Tx)
      "links { link1 { } }\n",
      false);

  App* app = new App();
  Mgr* mgr = new Mgr();
  ZmRef<Engine> engine = new Engine();

  ZmRef<MxMultiplex> mx = new MxMultiplex("mx", cf->getCf("mx"));

  engine->init(mgr, app, mx, cf);

  mx->start();

  engine->start();
  engine->waitConnected();
  engine->stop();
  engine->waitDisconnected();

  engine->action(Reconnect);
  engine->start();
  engine->waitReconnect();
  engine->stop();
  engine->waitDisconnected();

  engine->action(Disconnected);
  engine->start();
  engine->waitDisconnected();
  engine->stop();

  mx->stop();

  engine = 0;
  app->final();
  delete mgr;
  delete app;
}
