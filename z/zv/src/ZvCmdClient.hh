//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

#ifndef ZvCmdClient_HH
#define ZvCmdClient_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <zlib/ZuString.hh>
#include <zlib/ZuUnion.hh>

#include <zlib/ZmObject.hh>
#include <zlib/ZmRef.hh>
#include <zlib/ZmPLock.hh>

#include <zlib/ZtArray.hh>
#include <zlib/ZtString.hh>

#include <zlib/ZeLog.hh>

#include <zlib/ZiMultiplex.hh>
#include <zlib/ZiFile.hh>
#include <zlib/ZiRx.hh>

#include <zlib/Zfb.hh>
#include <zlib/Ztls.hh>

#include <zlib/ZvCf.hh>
#include <zlib/ZvUserDB.hh>
#include <zlib/ZvSeqNo.hh>
#include <zlib/ZvTelemetry.hh>
#include <zlib/ZvCmdDispatcher.hh>

#include <zlib/loginreq_fbs.h>
#include <zlib/loginack_fbs.h>
#include <zlib/userdbreq_fbs.h>
#include <zlib/userdback_fbs.h>
#include <zlib/zcmdreq_fbs.h>
#include <zlib/zcmdack_fbs.h>
#include <zlib/telemetry_fbs.h>
#include <zlib/telreq_fbs.h>
#include <zlib/telack_fbs.h>

#include <zlib/ZvCmdNet.hh>

// userDB response
using ZvCmdUserDBAckFn = ZmFn<const ZvUserDB::fbs::ReqAck *>;
// command response
using ZvCmdAckFn = ZmFn<const ZvCmd::fbs::ReqAck *>;
// telemetry response
using ZvCmdTelAckFn = ZmFn<const ZvTelemetry::fbs::ReqAck *>;

struct ZvCmd_Login {
  ZtString		user;
  ZtString		passwd;
  unsigned		totp = 0;
};
struct ZvCmd_Access {
  ZtString		keyID;
  ZvUserDB::KeyData	token;
  int64_t		stamp;
  ZvUserDB::KeyData	hmac;
};
using ZvCmd_Credentials = ZuUnion<ZvCmd_Login, ZvCmd_Access>;

template <typename App, typename Link> class ZvCmdClient;

template <typename App_, typename Impl_>
class ZvCmdCliLink :
    public Ztls::CliLink<App_, Impl_>,
    public ZiRx<ZvCmdCliLink<App_, Impl_>, Ztls::IOBuf> {
public:
  using App = App_;
  using Impl = Impl_;
  using Base = Ztls::CliLink<App_, Impl_>;
  using IOBuf = Ztls::IOBuf;
  using Rx = ZiRx<ZvCmdCliLink, IOBuf>;

friend Base;
template <typename, typename> friend class ZvCmdClient;

  using FBB = Zfb::IOBuilder<IOBuf>;

public:
  auto impl() const { return static_cast<const Impl *>(this); }
  auto impl() { return static_cast<Impl *>(this); }

private:
  using Credentials = ZvCmd_Credentials;
  using KeyData = ZvUserDB::KeyData;
  using Bitmap = ZvUserDB::Bitmap;

private:
  // containers of pending requests
  using UserDBReqs =
    ZmRBTreeKV<ZvSeqNo, ZvCmdUserDBAckFn,
      ZmRBTreeUnique<true,
	ZmRBTreeLock<ZmPLock> > >;
  using CmdReqs =
    ZmRBTreeKV<ZvSeqNo, ZvCmdAckFn,
      ZmRBTreeUnique<true,
	ZmRBTreeLock<ZmPLock> > >;
  using TelReqs =
    ZmRBTreeKV<ZvSeqNo, ZvCmdTelAckFn,
      ZmRBTreeUnique<true,
	ZmRBTreeLock<ZmPLock> > >;

public:
  struct State {
    enum {
      Down = 0,
      Login,
      Up
    };
  };

  ZvCmdCliLink(App *app, ZtString server, uint16_t port) :
      Base{app, ZuMv(server), port} { }

  // Note: the caller must ensure that calls to login()/access()
  // are not overlapped - until loggedIn()/connectFailed()/disconnected()
  // no further calls must be made
  template <typename User, typename Passwd>
  void login(User &&user, Passwd &&passwd, unsigned totp) {
    new (m_credentials.new_<ZvCmd_Login>())
      ZvCmd_Login{ZuFwd<User>(user), ZuFwd<Passwd>(passwd), totp};
    this->connect();
  }
  template <typename KeyID>
  void access(KeyID &&keyID, ZuString secret_) {
    ZtArray<uint8_t> secret;
    secret.length(Ztls::Base64::declen(secret_.length()));
    Ztls::Base64::decode(secret, secret_);
    secret.length(32);
    ZvUserDB::KeyData token, hmac;
    token.length(token.size());
    hmac.length(hmac.size());
    this->app()->random(token);
    int64_t stamp = ZmTimeNow().sec();
    {
      Ztls::HMAC hmac_(ZvUserDB::Key::keyType());
      hmac_.start(secret);
      hmac_.update(token);
      hmac_.update(
	  {reinterpret_cast<const uint8_t *>(&stamp), sizeof(uint64_t)});
      hmac_.finish(hmac.data());
    }
    new (m_credentials.new_<ZvCmd_Access>())
      ZvCmd_Access{ZuFwd<KeyID>(keyID), ZuMv(token), stamp, ZuMv(hmac)};
    this->connect();
  }
  template <typename KeyID>
  void access_(
      KeyID &&keyID, ZvUserDB::KeyData &&token,
      int64_t stamp, ZvUserDB::KeyData &&hmac) {
    new (m_credentials.new_<ZvCmd_Access>())
      ZvCmd_Access{ZuFwd<KeyID>(keyID), ZuMv(token), stamp, ZuMv(hmac)};
    this->connect();
  }

  int state() const { return m_state; }

  // available once logged in
  uint64_t userID() const { return m_userID; }
  const ZtString &userName() const { return m_userName; }
  const ZtArray<ZtString> &roles() const { return m_roles; }
  const Bitmap &perms() const { return m_perms; }
  uint8_t flags() const { return m_userFlags; }

public:
  // send userDB request
  void sendUserDB(FBB &fbb, ZvSeqNo seqNo, ZvCmdUserDBAckFn fn) {
    using namespace ZvCmd;
    m_userDBReqs.add(seqNo, ZuMv(fn));
    this->send(saveHdr(fbb, Type::userDB()));
  }
  // send command
  void sendCmd(FBB &fbb, ZvSeqNo seqNo, ZvCmdAckFn fn) {
    using namespace ZvCmd;
    m_cmdReqs.add(seqNo, ZuMv(fn));
    this->send(saveHdr(fbb, Type::cmd()));
  }
  // send telemetry request
  void sendTelReq(FBB &fbb, ZvSeqNo seqNo, ZvCmdTelAckFn fn) {
    using namespace ZvCmd;
    m_telReqs.add(seqNo, ZuMv(fn));
    this->send(saveHdr(fbb, Type::telReq()));
  }

  void loggedIn() { } // default

  void connected(const char *alpn) {
    if (!alpn || strcmp(alpn, "zcmd")) {
      this->disconnect();
      return;
    }

    scheduleTimeout();
    m_state = State::Login;

    // send login
    FBB fbb;
    if (m_credentials.type() == Credentials::Index<ZvCmd_Login>{}) {
      using namespace ZvUserDB;
      using namespace Zfb::Save;
      const auto &data = m_credentials.p<ZvCmd_Login>();
      fbb.Finish(fbs::CreateLoginReq(fbb,
	    fbs::LoginReqData::Login,
	    fbs::CreateLogin(fbb,
	      str(fbb, data.user),
	      str(fbb, data.passwd),
	      data.totp).Union()));
    } else {
      using namespace ZvUserDB;
      using namespace Zfb::Save;
      const auto &data = m_credentials.p<ZvCmd_Access>();
      fbb.Finish(fbs::CreateLoginReq(fbb,
	    fbs::LoginReqData::Access,
	    fbs::CreateAccess(fbb,
	      str(fbb, data.keyID),
	      bytes(fbb, data.token),
	      data.stamp,
	      bytes(fbb, data.hmac)).Union()));
    }
    this->send_(ZvCmd::saveHdr(fbb, ZvCmd::Type::login()));
  }

  void disconnected() {
    m_userDBReqs.clean();
    m_cmdReqs.clean();
    m_telReqs.clean();

    m_state = State::Down;

    cancelTimeout();

    m_rxBuf = nullptr;
  }

private:
  int loadBody(const IOBuf *buf, unsigned) {
    return ZvCmd::verifyHdr(buf, [](const ZvCmd::Hdr *hdr, const IOBuf *buf) {
      auto this_ = static_cast<ZvCmdCliLink *>(buf->owner);
      auto type = hdr->type;
      if (ZuUnlikely(this_->m_state.load_() == State::Login)) {
	this_->cancelTimeout();
	if (type != ZvCmd::Type::login()) return -1;
	return this_->processLoginAck(hdr->data(), hdr->length);
      }
      return this_->app()->dispatch(
	  type, this_->impl(), hdr->data(), hdr->length);
    });
  }

public:
  int process(const uint8_t *data, unsigned length) {
    if (ZuUnlikely(m_state.load_() == State::Down))
      return -1; // disconnect

    int i = Rx::template recvMemSync<
      ZvCmd::loadHdr<IOBuf>, &ZvCmdCliLink::loadBody>(data, length, m_rxBuf);

    if (ZuUnlikely(i < 0)) m_state = State::Down;
    return i;
  }

private:
  int processLoginAck(const uint8_t *data, unsigned len) {
    using namespace Zfb;
    using namespace Load;
    using namespace ZvUserDB;
    {
      Verifier verifier{data, len};
      if (!fbs::VerifyLoginAckBuffer(verifier)) return -1;
    }
    auto loginAck = fbs::GetLoginAck(data);
    if (!loginAck->ok()) return false;
    m_userID = loginAck->id();
    m_userName = str(loginAck->name());
    all(loginAck->roles(), [this](unsigned i, auto role_) {
      m_roles.push(str(role_));
    });
    all(loginAck->perms(), [this](unsigned i, uint64_t v) {
      if (i < Bitmap::Words) m_perms.data[i] = v;
    });
    m_userFlags = loginAck->flags();
    m_state = State::Up;
    impl()->loggedIn();
    return len;
  }
  int processUserDB(const uint8_t *data, unsigned len) {
    using namespace Zfb;
    using namespace Load;
    using namespace ZvUserDB;
    {
      Verifier verifier{data, len};
      if (!fbs::VerifyReqAckBuffer(verifier)) return -1;
    }
    auto reqAck = fbs::GetReqAck(data);
    if (ZvCmdUserDBAckFn fn = m_userDBReqs.delVal(reqAck->seqNo()))
      fn(reqAck);
    return len;
  }
  int processCmd(const uint8_t *data, unsigned len) {
    using namespace Zfb;
    using namespace Load;
    using namespace ZvCmd;
    {
      Verifier verifier{data, len};
      if (!fbs::VerifyReqAckBuffer(verifier)) return -1;
    }
    auto reqAck = fbs::GetReqAck(data);
    if (ZvCmdAckFn fn = m_cmdReqs.delVal(reqAck->seqNo()))
      fn(reqAck);
    return len;
  }
  int processTelReq(const uint8_t *data, unsigned len) {
    using namespace Zfb;
    using namespace Load;
    using namespace ZvTelemetry;
    {
      Verifier verifier{data, len};
      if (!fbs::VerifyReqAckBuffer(verifier)) return -1;
    }
    auto reqAck = fbs::GetReqAck(data);
    if (ZvCmdTelAckFn fn = m_telReqs.delVal(reqAck->seqNo()))
      fn(reqAck);
    return len;
  }
  // default telemetry handler does nothing
  int processTelemetry(const uint8_t *data, unsigned len) { return len; }

  void scheduleTimeout() {
    if (this->app()->timeout())
      this->app()->mx()->add([link = ZmMkRef(impl())]() {
	link->disconnect();
      }, ZmTimeNow(this->app()->timeout()), &m_timer);
  }
  void cancelTimeout() {
    this->app()->mx()->del(&m_timer);
  }

private:
  ZmScheduler::Timer	m_timer;
  ZmAtomic<int>		m_state = State::Down;
  ZmRef<IOBuf>		m_rxBuf;
  Credentials		m_credentials;
  UserDBReqs		m_userDBReqs;
  CmdReqs		m_cmdReqs;
  TelReqs		m_telReqs;
  uint64_t		m_userID = 0;
  ZtString		m_userName;
  ZtArray<ZtString>	m_roles;
  Bitmap		m_perms;
  uint8_t		m_userFlags = 0;
};

template <typename App_, typename Link_>
class ZvCmdClient :
    public ZvCmdDispatcher,
    public Ztls::Client<App_> {
public:
  using App = App_;
  using Link = Link_;
  using FBB = typename Link::FBB;
  using Dispatcher = ZvCmdDispatcher;
  using TLS = Ztls::Client<App>;
friend TLS;

  const App *app() const { return static_cast<const App *>(this); }
  App *app() { return static_cast<App *>(this); }

  void init(ZiMultiplex *mx, const ZvCf *cf) {
    static const char *alpn[] = { "zcmd", 0 };

    Dispatcher::init();

    Dispatcher::map(ZvCmd::Type::userDB(),
	[](void *link, const uint8_t *data, unsigned len) {
	  return static_cast<Link *>(link)->processUserDB(data, len);
	});
    Dispatcher::map(ZvCmd::Type::cmd(),
	[](void *link, const uint8_t *data, unsigned len) {
	  return static_cast<Link *>(link)->processCmd(data, len);
	});
    Dispatcher::map(ZvCmd::Type::telReq(),
	[](void *link, const uint8_t *data, unsigned len) {
	  return static_cast<Link *>(link)->processTelReq(data, len);
	});
    Dispatcher::map(ZvCmd::Type::telemetry(),
	[](void *link, const uint8_t *data, unsigned len) {
	  return static_cast<Link *>(link)->processTelemetry(data, len);
	});

    TLS::init(mx, cf->get("thread", true), cf->get("caPath", true), alpn);

    m_reconnFreq = cf->getInt("reconnFreq", 0, 3600, 0);
    m_timeout = cf->getInt("timeout", 0, 3600, 0);
  }
  void final() {
    TLS::final();

    Dispatcher::final();
  }

  unsigned reconnFreq() const { return m_reconnFreq; }
  unsigned timeout() const { return m_timeout; }

private:
  unsigned		m_reconnFreq = 0;
  unsigned		m_timeout = 0;
};

#endif /* ZvCmdClient_HH */
