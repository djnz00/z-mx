//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#ifndef ZcmdServer_HH
#define ZcmdServer_HH

#ifndef ZcmdLib_HH
#include <zlib/ZcmdLib.hh>
#endif

#include <zlib/ZuString.hh>
#include <zlib/ZuByteSwap.hh>

#include <zlib/ZmObject.hh>
#include <zlib/ZmRef.hh>
#include <zlib/ZmPLock.hh>

#include <zlib/ZtArray.hh>
#include <zlib/ZtString.hh>

#include <zlib/ZeLog.hh>
#include <zlib/ZeAssert.hh>

#include <zlib/ZiMultiplex.hh>
#include <zlib/ZiFile.hh>
#include <zlib/ZiIOBuf.hh>
#include <zlib/ZiRx.hh>

#include <zlib/Zfb.hh>
#include <zlib/Ztls.hh>

#include <zlib/ZvCf.hh>

#include <zlib/Zdb.hh>

#include <zlib/ZumServer.hh>

#include <zlib/Zcmd.hh>
#include <zlib/ZcmdHost.hh>
#include <zlib/ZcmdDispatcher.hh>
#include <zlib/ZtelServer.hh>

template <typename App, typename Link>
class ZcmdServer;

template <
  typename App_,
  typename Impl_,
  typename IOBufAlloc_ = Ztls::IOBufAlloc<>>
class ZcmdSrvLink :
    public Ztls::SrvLink<App_, Impl_, IOBufAlloc_>,
    public ZiRx<ZcmdSrvLink<App_, Impl_, IOBufAlloc_>, IOBufAlloc_> {
public:
  using App = App_;
  using Impl = Impl_;
  using IOBufAlloc = IOBufAlloc_;
  using Base = Ztls::SrvLink<App, Impl, IOBufAlloc>;
  using Rx = ZiRx<ZcmdSrvLink, IOBufAlloc>;

friend Base;
template <typename, typename> friend class ZcmdServer;

  using Session = Zum::Server::Session;

public:
  auto impl() const { return static_cast<const Impl *>(this); }
  auto impl() { return static_cast<Impl *>(this); }

  struct State {
    enum {
      Down = 0,
      Login,
      LoginFailed,
      Up
    };
  };

  ZcmdSrvLink(App *app) : Base{app} { }

  Session *session() const { return m_session; }

  void connected(const char *alpn, int /* tlsver */) {
    scheduleTimeout();

    if (!alpn || strcmp(alpn, "zcmd")) {
      m_state = State::LoginFailed;
      return;
    }

    m_state = State::Login;
  }

  void disconnected() {
    m_state = State::Down;

    cancelTimeout();

    m_rxBuf = nullptr;

    this->app()->disconnected(impl());
  }

private:
  int processLogin(ZmRef<ZiIOBuf> buf) {
    return this->app()->processLogin(
      ZuMv(buf),
      Zum::Server::LoginFn{ZmMkRef(this),
	[](ZcmdSrvLink *this_, ZmRef<Session> session, ZmRef<ZiIOBuf> buf) {
	  this_->processLoginAck(ZuMv(session), ZuMv(buf));
	}});
  }
  void processLoginAck(ZmRef<Session> session, ZmRef<ZiIOBuf> buf) {
    // Note: the app thread is the TLS thread
    this->app()->run([
      this, session = ZuMv(session), buf = ZuMv(buf)
    ]() mutable {
      auto ack = Zfb::GetRoot<Zum::fbs::LoginAck>(buf->data());
      if (ack->ok()) {
	m_session = ZuMv(session);
	m_state = State::Up;
      } else
	m_state = State::LoginFailed;
      this->send_(Zcmd::saveHdr(ZuMv(buf), Zcmd::Type::login()));
    });
  }
  int processUserDB(ZmRef<ZiIOBuf> buf) {
    return this->app()->processUserDB(
      m_session, ZuMv(buf),
      Zum::Server::ResponseFn{ZmMkRef(this),
	[](ZcmdSrvLink *this_, ZmRef<ZiIOBuf> buf) {
	  this_->send(Zcmd::saveHdr(ZuMv(buf), Zcmd::Type::userDB()));
	}});
  }
  int processCmd(ZmRef<ZiIOBuf> buf) {
    return this->app()->processCmd(impl(), m_session, ZuMv(buf));
  }
  int processTelReq(ZmRef<ZiIOBuf> buf) {
    return this->app()->processTelReq(impl(), m_session, ZuMv(buf));
  }
public:
  void sendCmd(ZmRef<ZiIOBuf> buf) {
    this->send(Zcmd::saveHdr(ZuMv(buf), Zcmd::Type::cmd()));
  }
  void sendTelReq(ZmRef<ZiIOBuf> buf) {
    this->send(Zcmd::saveHdr(ZuMv(buf), Zcmd::Type::telReq()));
  }
  void sendTelemetry(ZmRef<ZiIOBuf> buf) {
    this->send(Zcmd::saveHdr(ZuMv(buf), Zcmd::Type::telemetry()));
  }

private:
  static int loadBody(ZmRef<ZiIOBuf> buf) {
    return Zcmd::verifyHdr(ZuMv(buf),
      [](const Zcmd::Hdr *hdr, ZmRef<ZiIOBuf> buf) {
	auto this_ =
	  static_cast<ZcmdSrvLink *>(static_cast<Impl *>(buf->owner));
	auto type = hdr->type;
	if (ZuUnlikely(this_->m_state == State::Login)) {
	  if (type != Zcmd::Type::login()) return -1;
	  return this_->processLogin(ZuMv(buf));
	}
	return this_->app()->dispatch(type, this_->impl(), ZuMv(buf));
      });
  }

public:
  int process(const uint8_t *data, unsigned length) {
    if (ZuUnlikely(m_state == State::Down))
      return -1; // disconnect

    if (ZuUnlikely(m_state == State::LoginFailed))
      return length; // timeout then disc.

    scheduleTimeout();

    // async Rx
    int i = Rx::template recvMem<
      Zcmd::loadHdr, ZcmdSrvLink::loadBody>(data, length, m_rxBuf);

    if (ZuUnlikely(i < 0)) m_state = State::Down;
    return i;
  }

private:
  void scheduleTimeout() {
    if (this->app()->timeout())
      this->app()->mx()->add([this]() { this->disconnect(); },
	  Zm::now(this->app()->timeout()), &m_timer);
  }
  void cancelTimeout() { this->app()->mx()->del(&m_timer); }

private:
  ZmScheduler::Timer	m_timer;
  int			m_state = State::Down;
  ZmRef<ZiIOBuf>	m_rxBuf;
  ZmRef<Session>	m_session;
};

template <typename App_, typename Link_>
class ZcmdServer :
    public ZcmdDispatcher,
    public ZcmdHost,
    public Ztls::Server<App_>,
    public Ztel::Server<App_, Link_> {
public:
  using App = App_;
  using Link = Link_;
  using Host = ZcmdHost;
  using Dispatcher = ZcmdDispatcher;
  using TLS = Ztls::Server<App>;
friend TLS;
  using Session = Zum::Server::Session;
  using TelServer = Ztel::Server<App, Link>;

  using TelServer::run;
  using TelServer::invoked;
  using TelServer::invoke;

  struct UserDB : public ZuObject, public Zum::Server::UserDB {
    using Base = Zum::Server::UserDB;
    using Base::Base;
  };

  const App *app() const { return static_cast<const App *>(this); }
  App *app() { return static_cast<App *>(this); }

  void init(const ZvCf *cf, ZiMultiplex *mx, Zdb *db) {
    Host::init();
    Dispatcher::init();

    map(Zcmd::Type::userDB(),
	[](void *link, ZmRef<ZiIOBuf> buf) {
	  return static_cast<Link *>(link)->processUserDB(ZuMv(buf));
	});
    map(Zcmd::Type::cmd(),
	[](void *link, ZmRef<ZiIOBuf> buf) {
	  return static_cast<Link *>(link)->processCmd(ZuMv(buf));
	});
    map(Zcmd::Type::telReq(),
	[](void *link, ZmRef<ZiIOBuf> buf) {
	  return static_cast<Link *>(link)->processTelReq(ZuMv(buf));
	});

    {
      static const char *alpn[] = { "zcmd", 0 };
      ZmRef<ZvCf> srvCf = cf->getCf<true>("server");

      TLS::init(mx,
	srvCf->get<true>("thread"), srvCf->get<true>("caPath"), alpn,
	srvCf->get<true>("certPath"), srvCf->get<true>("keyPath"));
      m_ip = srvCf->get("localIP", "127.0.0.1");
      m_port = srvCf->getInt("localPort", 1, (1<<16) - 1, 19400);
      m_nAccepts = srvCf->getInt("nAccepts", 1, 1024, 8);
      m_rebindFreq = srvCf->getInt("rebindFreq", 0, 3600, 0);
      m_timeout = srvCf->getInt("timeout", 0, 3600, 0);
    }

    m_userDB = new UserDB(this);
    m_userDB->init(cf->getCf<true>("userdb"), db);

    TelServer::init(mx, cf->getCf("telemetry"));
  }

  void final() {
    TelServer::final();

    m_userDB->final();
    m_userDB = nullptr;

    TLS::final();

    Dispatcher::final();
    Host::final();
  }

  void open(ZtArray<ZtString> perms, Zum::Server::OpenFn fn) {
    perms.push("ZCmd");
    perms.push("ZTel");
    m_userDB->open(ZuMv(perms), [
      this, fn = ZuMv(fn)
    ](bool ok, ZtArray<unsigned> permIDs) mutable {
      run([this, fn = ZuMv(fn), ok, permIDs = ZuMv(permIDs)]() mutable {
	if (!ok) {
	  ZeLOG(Fatal, "userDB open failed");
	  fn(false, ZtArray<unsigned>{});
	} else {
	  unsigned n = permIDs.length();
	  ZeAssert(n >= 2, (n), "n=" << n,
	    fn(false, ZtArray<unsigned>{}); return);
	  permIDs.length(n - 2);
	  m_opened = true;
	  m_cmdPerm = permIDs[n - 2];
	  m_telPerm = permIDs[n - 1];
	  fn(true, ZuMv(permIDs));
	}
      });
    });
  }

  void start() { this->listen(); }
  void stop() { this->stopListening(); }

private:
  using Cxn = typename Link::Cxn;
  Cxn *accepted(const ZiCxnInfo &ci) {
    return new Cxn(new Link{app()}, ci);
  }
public:
  ZiIP localIP() const { return m_ip; }
  unsigned localPort() const { return m_port; }
  unsigned nAccepts() const { return m_nAccepts; }
  unsigned rebindFreq() const { return m_rebindFreq; }
  unsigned timeout() const { return m_timeout; }

  unsigned cmdPerm() const { return m_cmdPerm; }

  bool ok(Session *session, unsigned permID) const {
    return m_userDB->ok(session, permID);
  }

public:
  int processLogin(ZmRef<ZiIOBuf> reqBuf, Zum::Server::LoginFn fn) {
    return m_userDB->loginReq(ZuMv(reqBuf), ZuMv(fn)) ? 1 : -1;
  }
  int processUserDB(
    ZmRef<Session> session, ZmRef<ZiIOBuf> buf, Zum::Server::ResponseFn fn)
  {
    return m_userDB->request(ZuMv(session), ZuMv(buf), ZuMv(fn)) ? 1 : -1;
  }
  int processCmd(Link *link, Session *session, ZmRef<ZiIOBuf> buf) {
    if (!Zfb::Verifier{buf->data(), buf->length}.
	VerifyBuffer<Zcmd::fbs::Request>()) return -1;

    auto req = Zfb::GetRoot<Zcmd::fbs::Request>(buf->data());
    if (m_cmdPerm < 0 || !m_userDB->ok(session, m_cmdPerm)) {
      const auto &user = session->user->data();
      ZtString text = "permission denied";
      if (user.flags & Zum::UserFlags::ChPass())
	text << " (user must change password)\n";
      Zfb::IOBuilder fbb;
      fbb.Finish(Zcmd::fbs::CreateReqAck(fbb,
	  req->seqNo(), __LINE__, Zfb::Save::str(fbb, text)));
      link->send_(Zcmd::saveHdr(fbb.buf(), Zcmd::Type::cmd()));
      return 1;
    }

    auto cmd_ = req->cmd();
    ZtArray<ZtString> args;
    args.length(cmd_->size());
    Zfb::Load::all(cmd_,
	[&args](unsigned i, auto arg_) { args[i] = Zfb::Load::str(arg_); });
    using namespace Zum::Server;
    ZcmdContext ctx{
      .host = this,
      .dest = static_cast<void *>(link),
      .seqNo = req->seqNo(),
      .interactive = bool(session->flags & SessionFlags::Interactive())
    };
    Host::processCmd(&ctx, args);
    return 1;
  }
  void executed(ZcmdContext *ctx) {
    Zfb::IOBuilder fbb;
    fbb.Finish(Zcmd::fbs::CreateReqAck(
	fbb, ctx->seqNo, ctx->code, Zfb::Save::str(fbb, ctx->out)));
    auto link = static_cast<Link *>(ctx->dest.p<void *>());
    link->sendCmd(fbb.buf());
  }
  int processTelReq(Link *link, Session *session, ZmRef<ZiIOBuf> buf) {
    if (!Zfb::Verifier{buf->data(), buf->length}.
	VerifyBuffer<Ztel::fbs::Request>()) return -1;
    auto req = Zfb::GetRoot<Ztel::fbs::Request>(buf->data());
    if (m_telPerm < 0 || !m_userDB->ok(session, m_telPerm)) {
      Zfb::IOBuilder fbb{new Ztel::AckIOBufAlloc{}};
      fbb.Finish(Ztel::fbs::CreateReqAck(fbb, req->seqNo(), false));
      link->send_(Zcmd::saveHdr(fbb.buf(), Zcmd::Type::telemetry()));
      return 1;
    }
    TelServer::process(link, ZuMv(buf));
    return 1;
  }

  void disconnected(Link *link) {
    TelServer::disconnected(link);
  }

  // ZcmdHost virtual functions
  Dispatcher *dispatcher() { return this; }
  /* void send(void *link, ZmRef<ZiIOBuf> buf) {
    return static_cast<Link *>(link)->send(ZuMv(buf));
  } */
  Ztls::Random *rng() { return this; }

private:
  ZiIP			m_ip;
  uint16_t		m_port = 0;
  unsigned		m_nAccepts = 0;
  unsigned		m_rebindFreq = 0;
  unsigned		m_timeout = 0;

  ZuRef<UserDB>		m_userDB;

  bool			m_opened = false;
  unsigned		m_cmdPerm = 0;	// "ZCmd"
  unsigned		m_telPerm = 0;	// "ZTel"
};

#endif /* ZcmdServer_HH */
