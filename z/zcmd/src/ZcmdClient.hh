//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#ifndef ZcmdClient_HH
#define ZcmdClient_HH

#ifndef ZcmdLib_HH
#include <zlib/ZcmdLib.hh>
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
#include <zlib/ZcmdTelemetry.hh>
#include <zlib/ZcmdDispatcher.hh>

#include <zlib/zv_loginreq_fbs.h>
#include <zlib/zv_loginack_fbs.h>
#include <zlib/zv_userdbreq_fbs.h>
#include <zlib/zv_userdback_fbs.h>
#include <zlib/zv_zcmdreq_fbs.h>
#include <zlib/zv_zcmdack_fbs.h>
#include <zlib/zv_telemetry_fbs.h>
#include <zlib/zv_telreq_fbs.h>
#include <zlib/zv_telack_fbs.h>

#include <zlib/ZcmdNet.hh>

// userDB response
using ZcmdUserDBAckFn = ZmFn<void(const ZvUserDB::fbs::ReqAck *)>;
// command response
using ZcmdAckFn = ZmFn<void(const Zcmd::fbs::ReqAck *)>;
// telemetry response
using ZcmdTelAckFn = ZmFn<void(const ZcmdTelemetry::fbs::ReqAck *)>;

struct Zcmd_Login {
  ZtString		user;
  ZtString		passwd;
  unsigned		totp = 0;
};
struct Zcmd_Access {
  ZtString		keyID;
  ZvUserDB::KeyData	token;
  int64_t		stamp;
  ZvUserDB::KeyData	hmac;
};
using Zcmd_Credentials = ZuUnion<Zcmd_Login, Zcmd_Access>;

template <typename App, typename Link> class ZcmdClient;

template <typename App_, typename Impl_>
class ZcmdCliLink :
    public Ztls::CliLink<App_, Impl_>,
    public ZiRx<ZcmdCliLink<App_, Impl_>, Ztls::IOBuf> {
public:
  using App = App_;
  using Impl = Impl_;
  using Base = Ztls::CliLink<App_, Impl_>;
  using IOBuf = Ztls::IOBuf;
  using Rx = ZiRx<ZcmdCliLink, IOBuf>;

friend Base;
template <typename, typename> friend class ZcmdClient;

  using FBB = Zfb::IOBuilder<IOBuf>;

public:
  auto impl() const { return static_cast<const Impl *>(this); }
  auto impl() { return static_cast<Impl *>(this); }

private:
  using Credentials = Zcmd_Credentials;
  using KeyData = ZvUserDB::KeyData;
  using Bitmap = ZvUserDB::Bitmap;

private:
  // containers of pending requests
  using UserDBReqs =
    ZmRBTreeKV<ZvSeqNo, ZcmdUserDBAckFn,
      ZmRBTreeUnique<true,
	ZmRBTreeLock<ZmPLock> > >;
  using CmdReqs =
    ZmRBTreeKV<ZvSeqNo, ZcmdAckFn,
      ZmRBTreeUnique<true,
	ZmRBTreeLock<ZmPLock> > >;
  using TelReqs =
    ZmRBTreeKV<ZvSeqNo, ZcmdTelAckFn,
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

  ZcmdCliLink(App *app, ZtString server, uint16_t port) :
      Base{app, ZuMv(server), port} { }

  // Note: the caller must ensure that calls to login()/access()
  // are not overlapped - until loggedIn()/connectFailed()/disconnected()
  // no further calls must be made
  template <typename User, typename Passwd>
  void login(User &&user, Passwd &&passwd, unsigned totp) {
    new (m_credentials.new_<Zcmd_Login>())
      Zcmd_Login{ZuFwd<User>(user), ZuFwd<Passwd>(passwd), totp};
    this->connect();
  }
  template <typename KeyID>
  void access(KeyID &&keyID, ZuString secret_) {
    ZtArray<uint8_t> secret;
    secret.length(ZuBase64::declen(secret_.length()));
    ZuBase64::decode(secret, secret_);
    secret.length(32);
    ZvUserDB::KeyData token, hmac;
    token.length(token.size());
    hmac.length(hmac.size());
    this->app()->random(token);
    int64_t stamp = Zm::now().sec();
    {
      Ztls::HMAC hmac_(ZvUserDB::Key::keyType());
      hmac_.start(secret);
      hmac_.update(token);
      hmac_.update(
	  {reinterpret_cast<const uint8_t *>(&stamp), sizeof(uint64_t)});
      hmac_.finish(hmac.data());
    }
    new (m_credentials.new_<Zcmd_Access>())
      Zcmd_Access{ZuFwd<KeyID>(keyID), ZuMv(token), stamp, ZuMv(hmac)};
    this->connect();
  }
  template <typename KeyID>
  void access_(
      KeyID &&keyID, ZvUserDB::KeyData &&token,
      int64_t stamp, ZvUserDB::KeyData &&hmac) {
    new (m_credentials.new_<Zcmd_Access>())
      Zcmd_Access{ZuFwd<KeyID>(keyID), ZuMv(token), stamp, ZuMv(hmac)};
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
  void sendUserDB(FBB &fbb, ZvSeqNo seqNo, ZcmdUserDBAckFn fn) {
    using namespace Zcmd;
    m_userDBReqs.add(seqNo, ZuMv(fn));
    this->send(saveHdr(fbb, Type::userDB()));
  }
  // send command
  void sendCmd(FBB &fbb, ZvSeqNo seqNo, ZcmdAckFn fn) {
    using namespace Zcmd;
    m_cmdReqs.add(seqNo, ZuMv(fn));
    this->send(saveHdr(fbb, Type::cmd()));
  }
  // send telemetry request
  void sendTelReq(FBB &fbb, ZvSeqNo seqNo, ZcmdTelAckFn fn) {
    using namespace Zcmd;
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
    if (m_credentials.type() == Credentials::Index<Zcmd_Login>{}) {
      using namespace ZvUserDB;
      using namespace Zfb::Save;
      const auto &data = m_credentials.p<Zcmd_Login>();
      fbb.Finish(fbs::CreateLoginReq(fbb,
	    fbs::LoginReqData::Login,
	    fbs::CreateLogin(fbb,
	      str(fbb, data.user),
	      str(fbb, data.passwd),
	      data.totp).Union()));
    } else {
      using namespace ZvUserDB;
      using namespace Zfb::Save;
      const auto &data = m_credentials.p<Zcmd_Access>();
      fbb.Finish(fbs::CreateLoginReq(fbb,
	    fbs::LoginReqData::Access,
	    fbs::CreateAccess(fbb,
	      str(fbb, data.keyID),
	      bytes(fbb, data.token),
	      data.stamp,
	      bytes(fbb, data.hmac)).Union()));
    }
    this->send_(Zcmd::saveHdr(fbb, Zcmd::Type::login()));
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
    return Zcmd::verifyHdr(buf, [](const Zcmd::Hdr *hdr, const IOBuf *buf) {
      auto this_ = static_cast<ZcmdCliLink *>(buf->owner);
      auto type = hdr->type;
      if (ZuUnlikely(this_->m_state.load_() == State::Login)) {
	this_->cancelTimeout();
	if (type != Zcmd::Type::login()) return -1;
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
      Zcmd::loadHdr<IOBuf>, &ZcmdCliLink::loadBody>(data, length, m_rxBuf);

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
    if (ZcmdUserDBAckFn fn = m_userDBReqs.delVal(reqAck->seqNo()))
      fn(reqAck);
    return len;
  }
  int processCmd(const uint8_t *data, unsigned len) {
    using namespace Zfb;
    using namespace Load;
    using namespace Zcmd;
    {
      Verifier verifier{data, len};
      if (!fbs::VerifyReqAckBuffer(verifier)) return -1;
    }
    auto reqAck = fbs::GetReqAck(data);
    if (ZcmdAckFn fn = m_cmdReqs.delVal(reqAck->seqNo()))
      fn(reqAck);
    return len;
  }
  int processTelReq(const uint8_t *data, unsigned len) {
    using namespace Zfb;
    using namespace Load;
    using namespace ZcmdTelemetry;
    {
      Verifier verifier{data, len};
      if (!fbs::VerifyReqAckBuffer(verifier)) return -1;
    }
    auto reqAck = fbs::GetReqAck(data);
    if (ZcmdTelAckFn fn = m_telReqs.delVal(reqAck->seqNo()))
      fn(reqAck);
    return len;
  }
  // default telemetry handler does nothing
  int processTelemetry(const uint8_t *data, unsigned len) { return len; }

  void scheduleTimeout() {
    if (this->app()->timeout())
      this->app()->mx()->add([link = ZmMkRef(impl())]() {
	link->disconnect();
      }, Zm::now(this->app()->timeout()), &m_timer);
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
class ZcmdClient :
    public ZcmdDispatcher,
    public Ztls::Client<App_> {
public:
  using App = App_;
  using Link = Link_;
  using FBB = typename Link::FBB;
  using Dispatcher = ZcmdDispatcher;
  using TLS = Ztls::Client<App>;
friend TLS;

  const App *app() const { return static_cast<const App *>(this); }
  App *app() { return static_cast<App *>(this); }

  void init(ZiMultiplex *mx, const ZvCf *cf) {
    static const char *alpn[] = { "zcmd", 0 };

    Dispatcher::init();

    Dispatcher::map(Zcmd::Type::userDB(),
	[](void *link, const uint8_t *data, unsigned len) {
	  return static_cast<Link *>(link)->processUserDB(data, len);
	});
    Dispatcher::map(Zcmd::Type::cmd(),
	[](void *link, const uint8_t *data, unsigned len) {
	  return static_cast<Link *>(link)->processCmd(data, len);
	});
    Dispatcher::map(Zcmd::Type::telReq(),
	[](void *link, const uint8_t *data, unsigned len) {
	  return static_cast<Link *>(link)->processTelReq(data, len);
	});
    Dispatcher::map(Zcmd::Type::telemetry(),
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

#endif /* ZcmdClient_HH */
