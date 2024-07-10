//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// server-side user DB with MFA, API keys, etc.

#include <zlib/ZumServer.hh>

#include <zlib/ZuBase32.hh>
#include <zlib/ZuBase64.hh>

#include <zlib/ZtQuote.hh>

#include <zlib/ZeLog.hh>
#include <zlib/ZeAssert.hh>

#include <zlib/ZtlsTOTP.hh>

namespace Zum::Server {

Mgr::Mgr(Ztls::Random *rng, unsigned passLen,
    unsigned totpRange, unsigned keyInterval) :
  m_rng{rng},
  m_passLen{passLen},
  m_totpRange{totpRange},
  m_keyInterval{keyInterval}
{
}

Mgr::~Mgr()
{
}

void Mgr::init(ZvCf *cf, ZiMultiplex *mx, Zdb *db)
{
  ZeAssert(m_state == MgrState::Uninitialized,
    (state = m_state), "invalid state=" << state, return);
  unsigned sid = mx->sid(cf->get<true>("thread"));
  if (!sid ||
      sid > mx->params().nThreads() ||
      sid == mx->rxThread() ||
      sid == mx->txThread())
    throw ZeEVENT(Fatal, ([thread = ZtString{cf->get("thread")}](auto &s) {
      s << "ZumServer thread misconfigured: " << thread; }));
  m_mx = mx;
  m_sid = sid;
  m_state = MgrState::Initialized;
  m_userTbl = db->initTable<User>("zum.user");
  m_roleTbl = db->initTable<Role>("zum.role");
  m_keyTbl = db->initTable<Key>("zum.key");
  m_permTbl = db->initTable<Perm>("zum.perm");
}

void Mgr::final()
{
  m_userTbl = nullptr;
  m_roleTbl = nullptr;
  m_keyTbl = nullptr;
  m_permTbl = nullptr;
  m_state = MgrState::Uninitialized;
}

// return permission name for request i
static ZtString permName(unsigned i)
{
  ZtString s{"UserMgmt."};
  auto loginReqEnd = unsigned(fbs::LoginReqData::MAX);
  if (i < loginReqEnd) return s << fbs::EnumNamesLoginReqData()[i + 1];
  return s << fbs::EnumNamesReqData()[(i - loginReqEnd) + 1];
}

// initiate open sequence
void Mgr::open(OpenFn fn)
{
  ZuPtr<Open> context = new Open{
    .fn = ZuMv(fn)
  };
  run([this, context = ZuMv(context)]() mutable { open_(ZuMv(context)); });
}
void Mgr::open_(ZuPtr<Open> context)
{
  // check for overlapping open/bootstrap or already opened
  if (m_state.load_() != MgrState::Initialized) {
    (context->fn)(false);
    return;
  }
  m_state = MgrState::Opening;

  m_userTbl->run([this, context = ZuMv(context)]() mutable {
    open_recoverNextUserID(ZuMv(context));
  });
}
// recover nextUserID
void Mgr::open_recoverNextUserID(ZuPtr<Open> context)
{
  m_userTbl->selectKeys<0>(ZuTuple<>{}, 1, [
    this, context = ZuMv(context)
  ](auto max, unsigned) mutable {
    using Key = ZuFieldKeyT<User, 0>;
    if (max.template is<Key>())
      m_nextUserID = max.template p<Key>().template p<0>() + 1;
    else
      m_permTbl->run([this, context = ZuMv(context)]() mutable {
	open_recoverNextPermID(ZuMv(context));
      });
  });
}
// recover nextPermID
void Mgr::open_recoverNextPermID(ZuPtr<Open> context)
{
  m_permTbl->selectKeys<0>(ZuTuple<>{}, 1, [
    this, context = ZuMv(context)
  ](auto max, unsigned i) mutable {
    using RowKey = ZuFieldKeyT<Perm, 0>;
    if (max.template is<RowKey>())
      m_nextPermID = max.template p<RowKey>().template p<0>() + 1;
    else
      m_permTbl->run([this, context = ZuMv(context)]() mutable {
	open_findAddPerm(ZuMv(context));
      });
  });
}
// find permission and update m_perms[]
void Mgr::open_findAddPerm(ZuPtr<Open> context)
{
  m_permTbl->find<1>(ZuMvTuple(permName(context->perm)), [
    this, context = ZuMv(context)
  ](ZmRef<ZdbObject<Perm>> dbPerm) mutable {
    if (!dbPerm) {
      m_permTbl->insert([
	this, context = ZuMv(context)
      ](ZdbObject<Perm> *dbPerm) mutable {
	if (!dbPerm) { opened(ZuMv(context), false); return; }
	initPerm(dbPerm, permName(context->perm));
	m_perms[context->perm] = dbPerm->data().id;
	open_nextPerm(ZuMv(context));
      });
    } else {
      m_perms[context->perm] = dbPerm->data().id;
      open_nextPerm(ZuMv(context));
    }
  });
}
// iterate to next permission
void Mgr::open_nextPerm(ZuPtr<Open> context)
{
  if (++context->perm < nPerms())
    m_permTbl->run([this, context = ZuMv(context)]() mutable {
      open_findAddPerm(ZuMv(context));
    });
  else
    opened(ZuMv(context), true);
}
// inform app of open result
void Mgr::opened(ZuPtr<Open> context, bool ok)
{
  run([this, context = ZuMv(context), ok]() {
    m_state = ok ? MgrState::Opened : MgrState::OpenFailed;
    (context->fn)(ok);
  });
}

// initiate bootstrap
void Mgr::bootstrap(ZtString userName, ZtString roleName, BootstrapFn fn)
{
  ZuPtr<Bootstrap> context = new Bootstrap{
    .userName = ZuMv(userName),
    .roleName = ZuMv(roleName),
    .fn = ZuMv(fn)
  };
  run([this, context = ZuMv(context)]() mutable { bootstrap_(ZuMv(context)); });
}
void Mgr::bootstrap_(ZuPtr<Bootstrap> context)
{
  // check for overlapping open/bootstrap or failed open
  if (m_state.load_() != MgrState::Opened) {
    (context->fn)(false);
    return;
  }
  m_state = MgrState::Bootstrap;

  m_roleTbl->run([this, context = ZuMv(context)]() mutable {
    bootstrap_findAddRole(ZuMv(context));
  });
}
// idempotent insert role
void Mgr::bootstrap_findAddRole(ZuPtr<Bootstrap> context)
{
  m_roleTbl->find<0>(ZuFwdTuple(context->roleName), [
    this, context = ZuMv(context)
  ](ZmRef<ZdbObject<Role>> dbRole) mutable {
    if (!dbRole)
      m_roleTbl->insert([
	this, context = ZuMv(context)
      ](ZdbObject<Role> *dbRole) mutable {
	if (!dbRole) {
	  bootstrapped(ZuMv(context), BootstrapResult{false});
	  return;
	}
	ZtBitmap perms;
	for (unsigned i = 0, n = nPerms(); i < n; i++) perms.set(m_perms[i]);
	initRole(
	  dbRole, context->roleName, perms, perms, RoleFlags::Immutable());
	m_userTbl->run([this, context = ZuMv(context)]() mutable {
	  bootstrap_findAddUser(ZuMv(context));
	});
      });
    else
      m_userTbl->run([this, context = ZuMv(context)]() mutable {
	bootstrap_findAddUser(ZuMv(context));
      });
  });
}
// idempotent insert admin user
void Mgr::bootstrap_findAddUser(ZuPtr<Bootstrap> context)
{
  m_userTbl->find<1>(ZuFwdTuple(context->userName), [
    this, context = ZuMv(context)
  ](ZmRef<ZdbObject<User>> dbUser) mutable {
    if (!dbUser)
      m_userTbl->insert([
	this, context = ZuMv(context)
      ](ZdbObject<User> *dbUser) mutable {
	if (!dbUser) {
	  bootstrapped(ZuMv(context), BootstrapResult{false});
	  return;
	}
	ZtString passwd;
	initUser(dbUser, m_nextUserID++,
	  ZuMv(context->userName), { ZuMv(context->roleName) },
	  UserFlags::Immutable() | UserFlags::Enabled(),// | UserFlags::ChPass(),
	  passwd);
	auto &user = dbUser->data();
	ZtString secret{ZuBase32::enclen(user.secret.length())};
	secret.length(secret.size());
	secret.length(ZuBase32::encode(secret, user.secret));
	bootstrapped(ZuMv(context),
	  BootstrapResult{BootstrapData{ZuMv(passwd), ZuMv(secret)}});
      });
    else
      bootstrapped(ZuMv(context), BootstrapResult{true});
  });
}
// inform app of bootstrap result
void Mgr::bootstrapped(ZuPtr<Bootstrap> context, BootstrapResult result)
{
  run([this, context = ZuMv(context), result = ZuMv(result)]() mutable {
    m_state = MgrState::Opened;
    (context->fn)(ZuMv(result));
  });
}

// initialize API key
void Mgr::initKey(ZdbObject<Key> *dbKey, UserID userID, KeyIDData keyID)
{
  auto key = new (dbKey->ptr()) Key{.userID = userID, .id = keyID};
  key->secret.length(key->secret.size());
  m_rng->random(key->secret);
  dbKey->commit();
}

// initialize permission
void Mgr::initPerm(ZdbObject<Perm> *dbPerm, ZtString name)
{
  new (dbPerm->ptr()) Perm{m_nextPermID++, ZuMv(name)};
  dbPerm->commit();
}

// initialize role
void Mgr::initRole(
  ZdbObject<Role> *dbRole, ZtString name,
  ZtBitmap perms, ZtBitmap apiperms, RoleFlags::T flags)
{
  new (dbRole->ptr()) Role{
    ZuMv(name),
    ZuMv(perms),
    ZuMv(apiperms),
    flags
  };
  dbRole->commit();
};

// initialize user
void Mgr::initUser(
  ZdbObject<User> *dbUser, UserID id, ZtString name,
  ZtArray<ZtString> roles, UserFlags::T flags,
  ZtString &passwd)
{
  auto &user =
    *(new (dbUser->ptr()) User{
      .id = id,
      .name = ZuMv(name),
      .roles = ZuMv(roles),
      .flags = flags
    });
  {
    passwd.length(m_passLen);
    m_rng->random(passwd);
    ZuArray<uint8_t> passwd_{passwd};
    for (unsigned i = 0; i < m_passLen; i++) {
      unsigned c = passwd_[i];
      c = ((c * 23040)>>16) + 33;	// ASCII 33-122 inclusive
      switch (c) {			// remap quotes and backslash
	case '\'': c = '{'; break;
	case '"':  c = '|'; break;
	case '`':  c = '}'; break;
	case '\\': c = '~'; break;
      }
      passwd_[i] = c;
    }
  }
  user.secret.length(user.secret.size());
  m_rng->random(user.secret);
  {
    Ztls::HMAC hmac(keyType());
    hmac.start(user.secret);
    hmac.update(passwd);
    user.hmac.length(user.hmac.size());
    hmac.finish(user.hmac.data());
  }
  dbUser->commit();
}

// start a new session (a user is logging in)
void Mgr::sessionLoad_login(ZtString userName, SessionFn fn)
{
  ZuPtr<SessionLoad> context =
    new SessionLoad{ZuMv(userName), ZuMv(fn)};
  m_userTbl->run([this, context = ZuMv(context)]() mutable {
    sessionLoad_findUser(ZuMv(context));
  });
}
// start a new session (using an API key)
void Mgr::sessionLoad_access(KeyIDData keyID, SessionFn fn)
{
  ZuPtr<SessionLoad> context = new SessionLoad{ZuMv(keyID), ZuMv(fn)};
  m_userTbl->run([this, context = ZuMv(context)]() mutable {
    sessionLoad_findKey(ZuMv(context));
  });
}
// find and load the user
void Mgr::sessionLoad_findUser(ZuPtr<SessionLoad> context)
{
  m_userTbl->find<1>(ZuFwdTuple(context->cred.p<ZtString>()), [
    this, context = ZuMv(context)
  ](ZmRef<ZdbObject<User>> dbUser) mutable {
    if (!dbUser) { sessionLoaded(ZuMv(context), false); return; }
    const auto &user = dbUser->data();
    context->session = new Session{this, ZuMv(dbUser)};
    if (!user.roles)
      sessionLoaded(ZuMv(context), true);
    else
      m_roleTbl->run([this, context = ZuMv(context)]() mutable {
	sessionLoad_findRole(ZuMv(context));
      });
  });
}
// find and load the key for an API session
void Mgr::sessionLoad_findKey(ZuPtr<SessionLoad> context)
{
  m_keyTbl->find<1>(ZuFwdTuple(context->cred.p<KeyIDData>()), [
    this, context = ZuMv(context)
  ](ZmRef<ZdbObject<Key>> dbKey) mutable {
    if (!dbKey) { sessionLoaded(ZuMv(context), false); return; }
    context->key = ZuMv(dbKey);
    m_userTbl->run([this, context = ZuMv(context)]() mutable {
      sessionLoad_findUserID(ZuMv(context));
    });
  });
}
// find and load the user using the userID from the API key
void Mgr::sessionLoad_findUserID(ZuPtr<SessionLoad> context)
{
  m_userTbl->find<0>(ZuFwdTuple(context->key->data().userID), [
    this, context = ZuMv(context)
  ](ZmRef<ZdbObject<User>> dbUser) mutable {
    if (!dbUser) { sessionLoaded(ZuMv(context), false); return; }
    const auto &user = dbUser->data();
    context->session = new Session{this, ZuMv(dbUser), ZuMv(context->key)};
    if (!user.roles)
      sessionLoaded(ZuMv(context), true);
    else
      m_roleTbl->run([this, context = ZuMv(context)]() mutable {
	sessionLoad_findRole(ZuMv(context));
      });
  });
}
// find and load the user's roles and permissions
void Mgr::sessionLoad_findRole(ZuPtr<SessionLoad> context)
{
  const auto &role = context->session->user->data().roles[context->roleIndex];
  m_roleTbl->find<0>(ZuFwdTuple(role), [
    this, context = ZuMv(context)
  ](ZmRef<ZdbObject<Role>> dbRole) mutable {
    if (!dbRole) { sessionLoaded(ZuMv(context), false); return; }
    if (!context->key)
      context->session->perms |= dbRole->data().perms;
    else
      context->session->perms |= dbRole->data().apiperms;
    if (++context->roleIndex < context->session->user->data().roles.length())
      m_roleTbl->run([this, context = ZuMv(context)]() mutable {
	sessionLoad_findRole(ZuMv(context));
      });
    else
      sessionLoaded(ZuMv(context), true);
  });
}
// inform app (session remains unauthenticated at this point)
void Mgr::sessionLoaded(ZuPtr<SessionLoad> context, bool ok)
{
  run([context = ZuMv(context), ok]() mutable {
    SessionFn fn = ZuMv(context->fn);
    ZmRef<Session> session = ZuMv(context->session);
    context = nullptr;
    if (!ok)
      fn(nullptr);
    else
      fn(ZuMv(session));
  });
}

// login succeeded - zero failure count and inform app
void Mgr::loginSucceeded(ZmRef<Session> session, SessionFn fn)
{
  auto &user = session->user->data();
  if (user.failures) {
    user.failures = 0;
    m_userTbl->run([this, session = ZuMv(session), fn = ZuMv(fn)]() mutable {
      ZmRef<ZdbObject<User>> user = session->user;
      m_userTbl->update(ZuMv(user), [
	session = ZuMv(session), fn = ZuMv(fn)
      ](ZdbObject<User> *dbUser) mutable {
	if (dbUser) dbUser->commit();
	fn(ZuMv(session));
      });
    });
  } else
    fn(ZuMv(session));
}

// login failed - update user and inform app
void Mgr::loginFailed(ZmRef<Session> session, SessionFn fn)
{
  m_userTbl->run([this, session = ZuMv(session), fn = ZuMv(fn)]() mutable {
    ZmRef<ZdbObject<User>> dbUser = session->user;
    m_userTbl->update(ZuMv(dbUser), [fn = ZuMv(fn)](ZdbObject<User> *dbUser) {
      if (dbUser) dbUser->commit();
      fn(nullptr);
    });
  });
}

// interactive login
void Mgr::login(
  ZtString name, ZtString passwd, unsigned totp, SessionFn fn)
{
  sessionLoad_login(ZuMv(name), [
    this, passwd = ZuMv(passwd), totp, fn = ZuMv(fn)
  ](ZmRef<Session> session) {
    if (!session) { fn(nullptr); return; }
    auto &user = session->user->data();
    if (!user.flags & UserFlags::Enabled()) {
      if (++user.failures < 3) {
	ZeLOG(Warning, ([name = user.name](auto &s) {
	  s << "authentication failure: disabled user "
	    << ZtQuote::String{name} << " attempted login"; }));
      }
      loginFailed(ZuMv(session), ZuMv(fn));
      return;
    }
    if (!(session->perms[
	m_perms[loginReqPerm(unsigned(fbs::LoginReqData::Login))]])) {
      if (++user.failures < 3) {
	ZeLOG(Warning, ([name = user.name](auto &s) {
	  s << "authentication failure: user without login permission "
	    << ZtQuote::String{name} << " attempted login"; }));
      }
      loginFailed(ZuMv(session), ZuMv(fn));
      return;
    }
    {
      Ztls::HMAC hmac(keyType());
      KeyData verify;
      hmac.start(user.secret);
      hmac.update(passwd);
      verify.length(verify.size());
      hmac.finish(verify.data());
      if (verify != user.hmac) {
	if (++user.failures < 3) {
	  ZeLOG(Warning, ([name = user.name](auto &s) {
	    s << "authentication failure: user "
	      << ZtQuote::String{name} << " provided invalid password"; }));
	}
	loginFailed(ZuMv(session), ZuMv(fn));
	return;
      }
    }
    if (!Ztls::TOTP::verify(user.secret, totp, m_totpRange)) {
      if (++user.failures < 3) {
	ZeLOG(Warning, ([name = user.name](auto &s) {
	  s << "authentication failure: user "
	    << ZtQuote::String{name} << " provided invalid OTP"; }));
      }
      loginFailed(ZuMv(session), ZuMv(fn));
      return;
    }
    loginSucceeded(ZuMv(session), ZuMv(fn));
  });
}

// non-interactive API access
void Mgr::access(
  KeyIDData keyID, ZtArray<const uint8_t> token, int64_t stamp,
  ZtArray<const uint8_t> hmac, SessionFn fn)
{
  sessionLoad_access(ZuMv(keyID), [
    this, token = ZuMv(token), stamp, hmac = ZuMv(hmac), fn = ZuMv(fn)
  ](ZmRef<Session> session) mutable {
    if (!session) { fn(nullptr); return; }
    auto &user = session->user->data();
    if (!(user.flags & UserFlags::Enabled())) {
      if (++user.failures < 3) {
	ZeLOG(Warning, ([name = user.name](auto &s) {
	  s << "authentication failure: disabled user "
	    << ZtQuote::String{name} << " attempted API key access"; }));
      }
      loginFailed(ZuMv(session), ZuMv(fn));
      return;
    }
    if (!(session->perms[
	m_perms[loginReqPerm(unsigned(fbs::LoginReqData::Access))]])) {
      if (++user.failures < 3) {
	ZeLOG(Warning, ([name = user.name](auto &s) {
	  s << "authentication failure: user without API access permission "
	    << ZtQuote::String{name} << " attempted access"; }));
      }
      loginFailed(ZuMv(session), ZuMv(fn));
      return;
    }
    {
      int64_t delta = Zm::now().sec() - stamp;
      if (delta < 0) delta = -delta;
      if (delta >= m_keyInterval) {
	loginFailed(ZuMv(session), ZuMv(fn));
	return;
      }
    }
    {
      Ztls::HMAC hmac_(keyType());
      KeyData verify;
      hmac_.start(session->key->data().secret);
      hmac_.update(token);
      hmac_.update({
	reinterpret_cast<const uint8_t *>(&stamp),
	sizeof(int64_t)});
      verify.length(verify.size());
      hmac_.finish(verify.data());
      if (verify != hmac) {
	if (++user.failures < 3) {
	  ZeLOG(Warning, ([name = user.name](auto &s) {
	    s << "authentication failure: user "
	      << ZtQuote::String{name}
	      << " provided invalid API key HMAC"; }));
	}
	loginFailed(ZuMv(session), ZuMv(fn));
	return;
      }
    }
    loginSucceeded(ZuMv(session), ZuMv(fn));
  });
}

// login/access request dispatch
void Mgr::loginReq(ZmRef<IOBuf> reqBuf, SessionFn fn)
{
  if (!Zfb::Verifier{reqBuf->data(), reqBuf->length}.
      VerifyBuffer<fbs::LoginReq>()) {
    fn(nullptr);
    return;
  }

  run([this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)]() mutable {
    loginReq_(ZuMv(reqBuf), ZuMv(fn));
  });
}
void Mgr::loginReq_(ZmRef<IOBuf> reqBuf, SessionFn fn)
{
  auto fbLoginReq = Zfb::GetRoot<fbs::LoginReq>(reqBuf->data());

  switch (fbLoginReq->data_type()) {
    case fbs::LoginReqData::Access: {
      auto access = static_cast<const fbs::Access *>(fbLoginReq->data());
      this->access(
	Zfb::Load::str(access->keyID()),
	Zfb::Load::bytes(access->token()),
	access->stamp(),
	Zfb::Load::bytes(access->hmac()),
	ZuMv(fn));
    } break;
    case fbs::LoginReqData::Login: {
      auto login = static_cast<const fbs::Login *>(fbLoginReq->data());
      this->login(
	Zfb::Load::str(login->user()),
	Zfb::Load::str(login->passwd()),
	login->totp(),
	ZuMv(fn));
    } break;
    default:
      fn(nullptr);
      break;
  }
}

// respond to a request
ZmRef<IOBuf> Mgr::respond(
  IOBuilder &fbb, SeqNo seqNo,
  fbs::ReqAckData ackType, Offset<void> ackData)
{
  fbs::ReqAckBuilder fbb_(fbb);
  fbb_.add_seqNo(seqNo);
  fbb_.add_data_type(ackType);
  fbb_.add_data(ackData);
  fbb.Finish(fbb_.Finish());
  return fbb.buf();
}

// reject a request
ZmRef<IOBuf> Mgr::reject(
  IOBuilder &fbb, SeqNo seqNo, unsigned rejCode, ZtString text)
{
  auto text_ = Zfb::Save::str(fbb, text);
  fbs::ReqAckBuilder fbb_(fbb);
  fbb_.add_seqNo(seqNo);
  fbb_.add_rejCode(rejCode);
  fbb_.add_rejText(text_);
  fbb.Finish(fbb_.Finish());
  return fbb.buf();
}

// validate, permission check and dispatch a request
void Mgr::request(ZmRef<Session> session, ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  if (!Zfb::Verifier{reqBuf->data(), reqBuf->length}.
      VerifyBuffer<fbs::Request>()) {
    IOBuilder fbb;
    fn(reject(fbb, 0, __LINE__, "corrupt request"));
    return;
  }

  run([
    this, session = ZuMv(session), reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    request_(ZuMv(session), ZuMv(reqBuf), ZuMv(fn));
  });
}
void Mgr::request_(ZmRef<Session> session, ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
  auto reqType = unsigned(fbRequest->data_type());

  if (ZuUnlikely(!ok(session, m_perms[reqPerm(reqType)]))) {
    IOBuilder fbb;
    using namespace Zfb::Save;
    ZtString text = "permission denied";
    if (session->user->data().flags & UserFlags::ChPass())
      text << " (user must change password)\n";
    fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZuMv(text)));
    return;
  }

  switch (reqType) {
    case int(fbs::ReqData::ChPass):
      chPass(ZuMv(session), ZuMv(reqBuf), ZuMv(fn));
      break;

    case int(fbs::ReqData::OwnKeyGet):
      ownKeyGet(ZuMv(session), ZuMv(reqBuf), ZuMv(fn));
      break;
    case int(fbs::ReqData::OwnKeyAdd):
      ownKeyAdd(ZuMv(session), ZuMv(reqBuf), ZuMv(fn));
      break;
    case int(fbs::ReqData::OwnKeyClr):
      ownKeyClr(ZuMv(session), ZuMv(reqBuf), ZuMv(fn));
      break;
    case int(fbs::ReqData::OwnKeyDel):
      ownKeyDel(ZuMv(session), ZuMv(reqBuf), ZuMv(fn));
      break;

    case int(fbs::ReqData::UserGet): userGet(ZuMv(reqBuf), ZuMv(fn)); break;
    case int(fbs::ReqData::UserAdd): userAdd(ZuMv(reqBuf), ZuMv(fn)); break;
    case int(fbs::ReqData::ResetPass): resetPass(ZuMv(reqBuf), ZuMv(fn)); break;
    case int(fbs::ReqData::UserMod): userMod(ZuMv(reqBuf), ZuMv(fn)); break;
    case int(fbs::ReqData::UserDel): userDel(ZuMv(reqBuf), ZuMv(fn)); break;

    case int(fbs::ReqData::RoleGet): roleGet(ZuMv(reqBuf), ZuMv(fn)); break;
    case int(fbs::ReqData::RoleAdd): roleAdd(ZuMv(reqBuf), ZuMv(fn)); break;
    case int(fbs::ReqData::RoleMod): roleMod(ZuMv(reqBuf), ZuMv(fn)); break;
    case int(fbs::ReqData::RoleDel): roleDel(ZuMv(reqBuf), ZuMv(fn)); break;

    case int(fbs::ReqData::PermGet): permGet(ZuMv(reqBuf), ZuMv(fn)); break;
    case int(fbs::ReqData::PermAdd): permAdd(ZuMv(reqBuf), ZuMv(fn)); break;
    case int(fbs::ReqData::PermMod): permMod(ZuMv(reqBuf), ZuMv(fn)); break;
    case int(fbs::ReqData::PermDel): permDel(ZuMv(reqBuf), ZuMv(fn)); break;

    case int(fbs::ReqData::KeyGet): keyGet(ZuMv(reqBuf), ZuMv(fn)); break;
    case int(fbs::ReqData::KeyAdd): keyAdd(ZuMv(reqBuf), ZuMv(fn)); break;
    case int(fbs::ReqData::KeyClr): keyClr(ZuMv(reqBuf), ZuMv(fn)); break;
    case int(fbs::ReqData::KeyDel): keyDel(ZuMv(reqBuf), ZuMv(fn)); break;
  }
}

// change password
void Mgr::chPass(ZmRef<Session> session, ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  auto &user = session->user->data();
  auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
  auto chPass = static_cast<const fbs::UserChPass *>(fbRequest->data());
  ZuString oldPass = Zfb::Load::str(chPass->oldpass());
  ZuString newPass = Zfb::Load::str(chPass->newpass());
  // verify old password
  Ztls::HMAC hmac(keyType());
  KeyData verify;
  hmac.start(user.secret);
  hmac.update(oldPass);
  verify.length(verify.size());
  hmac.finish(verify.data());
  if (verify != user.hmac) {
    IOBuilder fbb;
    fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	<< "old password did not match"));
    return;
  }
  // clear change password flag and update user with new HMAC
  user.flags &= ~UserFlags::ChPass();
  hmac.reset();
  hmac.update(newPass);
  hmac.finish(user.hmac.data());
  m_userTbl->run([
    this, seqNo = fbRequest->seqNo(), session = ZuMv(session), fn = ZuMv(fn)
  ]() mutable {
    ZdbObjRef<User> dbUser = session->user;
    m_userTbl->update(ZuMv(dbUser), [
      this, seqNo, fn = ZuMv(fn)
    ](ZdbObject<User> *dbUser) {
      if (dbUser) dbUser->commit();
      IOBuilder fbb;
      auto ackData = fbs::CreateAck(fbb);
      fn(respond(fbb, seqNo, fbs::ReqAckData::ChPass, ackData.Union()));
    });
  });
}

// query users
void Mgr::userGet(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
  auto query = static_cast<const fbs::UserQuery *>(fbRequest->data());
  if (query->userKey_type() != fbs::UserKey::ID &&
      query->userKey_type() != fbs::UserKey::Name) {
    IOBuilder fbb;
    fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	<< "unknown query key type (" << int(query->userKey_type()) << ')'));
    return;
  }
  if (query->limit() > MaxQueryLimit) {
    IOBuilder fbb;
    fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	<< "maximum query limit exceeded ("
	<< query->limit() << " > " << MaxQueryLimit << ')'));
    return;
  }
  m_userTbl->run([
    this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
    auto query = static_cast<const fbs::UserQuery *>(fbRequest->data());
    auto tupleFn = [
      this,
      seqNo = fbRequest->seqNo(),
      fbb = IOBuilder{},
      offsets = ZtArray<Offset<fbs::User>>(query->limit()),
      fn = ZuMv(fn)
    ](auto result, unsigned) mutable {
      using Row = ZuFieldTuple<User>;
      if (result.template is<Row>()) {
	offsets.push(ZfbField::save(fbb, result.template p<Row>()));
      } else {
	auto ackData = fbs::CreateUserList(fbb,
	  fbb.CreateVector(&offsets[0], offsets.length()));
	fn(respond(fbb, seqNo, fbs::ReqAckData::UserGet, ackData.Union()));
      }
    };
    if (query->userKey_type() == fbs::UserKey::ID) {
      auto userID = static_cast<const fbs::UserID *>(query->userKey())->id();
      m_userTbl->nextRows<0>(
	ZuMvTuple(ZuMv(userID)),
	query->inclusive(),
	query->limit(),
	ZuMv(tupleFn));
    } else {
      auto userName = Zfb::Load::str(
	static_cast<const fbs::UserName *>(query->userKey())->name());
      m_userTbl->nextRows<1>(
	ZuMvTuple(ZuMv(userName)),
	query->inclusive(),
	query->limit(),
	ZuMv(tupleFn));
    }
  });
}

// add a new user
void Mgr::userAdd(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  m_userTbl->run([
    this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
    auto fbUser = static_cast<const fbs::UserData *>(fbRequest->data());
    m_userTbl->find<1>(ZuMvTuple(Zfb::Load::str(fbUser->name())), [
      this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<User> dbUser) mutable {
      if (dbUser) {
	auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
	auto fbUser = static_cast<const fbs::UserData *>(fbRequest->data());
	auto userName = Zfb::Load::str(fbUser->name());
	IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "user " << ZtQuote::String{userName} << " already exists"));
	return;
      }
      m_userTbl->insert([
	this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
      ](ZdbObject<User> *dbUser) mutable {
	auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
	auto fbUser = static_cast<const fbs::UserData *>(fbRequest->data());
	auto userName = Zfb::Load::str(fbUser->name());
	if (!dbUser) {
	  IOBuilder fbb;
	  fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	      << "user " << ZtQuote::String{userName} << " insert failed"));
	  return;
	}
	ZtArray<ZtString> roles;
	roles.size(fbUser->roles()->size());
	Zfb::Load::all(fbUser->roles(),
	  [&roles](unsigned, const Zfb::String *role) {
	    roles.push(Zfb::Load::str(role));
	  });
	ZtString passwd;
	initUser(
	  dbUser, m_nextUserID++, userName,
	  ZuMv(roles), fbUser->flags(), passwd);
	auto &user = dbUser->data();
	IOBuilder fbb;
	auto ackData = fbs::CreateUserPass(fbb,
	  ZfbField::save(fbb, user), Zfb::Save::str(fbb, passwd));
	fn(respond(fbb, fbRequest->seqNo(),
	    fbs::ReqAckData::UserAdd, ackData.Union()));
      });
    });
  });
}

// delete all API keys for a user
template <typename L>
void Mgr::keyClr__(UserID id, L l)
{
  m_keyTbl->run([this, id, l = ZuMv(l)]() {
    m_keyTbl->selectKeys<0>(ZuMvTuple(ZuMv(id)), MaxAPIKeys, [
      this, l = ZuMv(l)
    ](auto result, unsigned) mutable {
      using KeyID = ZuFieldKeyT<Key, 0>;
      if (result.template is<KeyID>()) {
	m_keyTbl->run([this, id = ZuMv(result).template p<KeyID>()]() mutable {
	  m_keyTbl->findDel<1>(
	    ZuMvTuple(ZuMv(id).template p<1>()),
	    [](ZdbObject<Key> *dbKey) mutable {
	      if (dbKey) dbKey->commit();
	    });
	});
	return;
      }
      // EOR - serialize the completion callback after the key deletions
      m_keyTbl->run([l = ZuMv(l)]() { l(); });
    });
  });
}

// reset password (also clears all API keys)
void Mgr::resetPass(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
  auto userID = static_cast<const fbs::UserID *>(fbRequest->data());
  m_userTbl->run([
    this, seqNo = fbRequest->seqNo(), id = userID->id(), fn = ZuMv(fn)
  ]() mutable {
    m_userTbl->findUpd<0>(ZuMvTuple(ZuMv(id)), [
      this, seqNo, id, fn = ZuMv(fn)
    ](ZdbObjRef<User> dbUser) mutable {
      if (!dbUser) {
	IOBuilder fbb;
	fn(reject(fbb, seqNo, __LINE__, ZtString{}
	    << "user ID " << id << " not found"));
	return;
      }
      auto &user = dbUser->data();
      ZtString passwd;
      {
	KeyData passwd_;
	unsigned passLen_ = ZuBase64::declen(m_passLen);
	if (passLen_ > passwd_.size()) passLen_ = passwd_.size();
	passwd_.length(passLen_);
	m_rng->random(passwd_);
	passwd.length(m_passLen);
	ZuBase64::encode(passwd, passwd_);
      }
      {
	Ztls::HMAC hmac(keyType());
	hmac.start(user.secret);
	hmac.update(passwd);
	user.hmac.length(user.hmac.size());
	hmac.finish(user.hmac.data());
      }
      dbUser->commit();
      keyClr__(id, [
	this, seqNo, dbUser = ZuMv(dbUser),
	passwd = ZuMv(passwd), fn = ZuMv(fn)
      ]() {
	const auto &user = dbUser->data();
	IOBuilder fbb;
	auto ackData = fbs::CreateUserPass(fbb,
	  ZfbField::save(fbb, user), Zfb::Save::str(fbb, passwd));
	fn(respond(fbb, seqNo, fbs::ReqAckData::ResetPass, ackData.Union()));
      });
    });
  });
}

// modify user (name, roles, flags)
void Mgr::userMod(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  m_userTbl->run([
    this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
    auto fbUser = static_cast<const fbs::UserData *>(fbRequest->data());

    auto updateFn = [
      this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<User> dbUser) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
      auto fbUser = static_cast<const fbs::UserData *>(fbRequest->data());
      if (!dbUser) {
	IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "user ID " << fbUser->id() << " not found"));
	return;
      }
      auto &user = dbUser->data();
      if (user.flags & UserFlags::Immutable()) {
	IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "user ID " << fbUser->id() << " is immutable"));
	return;
      }
      if (auto name = Zfb::Load::str(fbUser->name()))
	user.name = name;
      if (fbUser->roles()->size()) {
	user.roles.length(0);
	user.roles.size(fbUser->roles()->size());
	Zfb::Load::all(fbUser->roles(),
	  [&user](unsigned, const Zfb::String *role) {
	    user.roles.push(Zfb::Load::str(role));
	  });
      }
      if (Zfb::IsFieldPresent(fbUser, fbs::UserData::VT_FLAGS))
	user.flags = fbUser->flags();
      dbUser->commit();
      IOBuilder fbb;
      auto ackData = fbs::CreateUserData(fbb,
	user.id,
	Zfb::Save::str(fbb, user.name),
	Zfb::Save::strVecIter(fbb, user.roles.length(), [&user](unsigned i) {
	  return user.roles[i];
	}),
	user.flags);
      fn(respond(fbb, fbRequest->seqNo(),
	  fbs::ReqAckData::UserMod, ackData.Union()));
    };
    if (Zfb::Load::str(fbUser->name())) 
      m_userTbl->findUpd<0, ZuSeq<1>>(ZuMvTuple(fbUser->id()), ZuMv(updateFn));
    else
      m_userTbl->findUpd<0>(ZuMvTuple(fbUser->id()), ZuMv(updateFn));
  });
}

// delete user (and associated API keys)
void Mgr::userDel(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  m_userTbl->run([
    this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
    auto fbUser = static_cast<const fbs::UserID *>(fbRequest->data());
    m_userTbl->findDel<0>(ZuMvTuple(fbUser->id()), [
      this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<User> dbUser) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
      auto fbUser = static_cast<const fbs::UserData *>(fbRequest->data());
      if (!dbUser) {
	IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "user ID " << fbUser->id() << " not found"));
	return;
      }
      dbUser->commit();
      keyClr__(fbUser->id(), [
	this, seqNo = fbRequest->seqNo(), dbUser = ZuMv(dbUser), fn = ZuMv(fn)
      ]() {
	IOBuilder fbb;
	auto ackData = fbs::CreateAck(fbb);
	fn(respond(fbb, seqNo, fbs::ReqAckData::UserDel, ackData.Union()));
      });
    });
  });
}

// query roles
void Mgr::roleGet(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
  auto query = static_cast<const fbs::RoleQuery *>(fbRequest->data());
  if (query->limit() > MaxQueryLimit) {
    IOBuilder fbb;
    fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	<< "maximum query limit exceeded ("
	<< query->limit() << " > " << MaxQueryLimit << ')'));
    return;
  }
  m_roleTbl->run([
    this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
    auto query = static_cast<const fbs::RoleQuery *>(fbRequest->data());
    m_roleTbl->nextRows<0>(
      ZuMvTuple(Zfb::Load::str(query->roleKey())),
      query->inclusive(),
      query->limit(), [
	this,
	seqNo = fbRequest->seqNo(),
	fbb = IOBuilder{},
	offsets = ZtArray<Offset<fbs::Role>>(query->limit()),
	fn = ZuMv(fn)
      ](auto result, unsigned) mutable {
	using Row = ZuFieldTuple<Role>;
	if (result.template is<Row>()) {
	  offsets.push(ZfbField::save(fbb, result.template p<Row>()));
	} else {
	  auto ackData = fbs::CreateRoleList(fbb,
	    fbb.CreateVector(&offsets[0], offsets.length()));
	  fn(respond(fbb, seqNo, fbs::ReqAckData::RoleGet, ackData.Union()));
	}
      });
  });
}

// add new role
void Mgr::roleAdd(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  m_roleTbl->run([
    this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
    auto fbRole = static_cast<const fbs::Role *>(fbRequest->data());
    auto roleName = Zfb::Load::str(fbRole->name());
    m_roleTbl->find<0>(ZuMvTuple(ZuMv(roleName)), [
      this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<Role> dbRole) mutable {
      if (dbRole) {
	auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
	auto fbRole = static_cast<const fbs::Role *>(fbRequest->data());
	auto roleName = Zfb::Load::str(fbRole->name());
	IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "role " << ZtQuote::String{roleName} << " already exists"));
	return;
      }
      m_roleTbl->insert([
	this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
      ](ZdbObject<Role> *dbRole) mutable {
	auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
	auto fbRole = static_cast<const fbs::Role *>(fbRequest->data());
	auto roleName = Zfb::Load::str(fbRole->name());
	if (!dbRole) {
	  IOBuilder fbb;
	  fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	      << "role " << ZtQuote::String{roleName} << " insert failed"));
	  return;
	}
	initRole(dbRole, ZuMv(roleName),
	  Zfb::Load::bitmap<ZtBitmap>(fbRole->perms()),
	  Zfb::Load::bitmap<ZtBitmap>(fbRole->apiperms()),
	  fbRole->flags());
	IOBuilder fbb;
	auto ackData = fbs::CreateAck(fbb);
	fn(respond(fbb, fbRequest->seqNo(),
	    fbs::ReqAckData::RoleAdd, ackData.Union()));
      });
    });
  });
}

// modify role (name, perms, apiperms, flags)
void Mgr::roleMod(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  m_roleTbl->run([
    this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
    auto fbRole = static_cast<const fbs::Role *>(fbRequest->data());
    m_roleTbl->findUpd<0>(ZuMvTuple(Zfb::Load::str(fbRole->name())), [
      this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<Role> dbRole) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
      auto fbRole = static_cast<const fbs::Role *>(fbRequest->data());
      auto roleName = Zfb::Load::str(fbRole->name());
      if (!dbRole) {
	IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "role " << ZtQuote::String{roleName} << " not found"));
	return;
      }
      auto &role = dbRole->data();
      if (role.flags & RoleFlags::Immutable()) {
	IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "role " << ZtQuote::String{roleName} << " is immutable"));
	return;
      }
      if (Zfb::IsFieldPresent(fbRole, fbs::Role::VT_PERMS))
	role.perms = Zfb::Load::bitmap<ZtBitmap>(fbRole->perms());
      if (Zfb::IsFieldPresent(fbRole, fbs::Role::VT_APIPERMS))
	role.apiperms = Zfb::Load::bitmap<ZtBitmap>(fbRole->apiperms());
      if (Zfb::IsFieldPresent(fbRole, fbs::Role::VT_FLAGS))
	role.flags = fbRole->flags();
      dbRole->commit();
      IOBuilder fbb;
      auto ackData = fbs::CreateAck(fbb);
      fn(respond(fbb, fbRequest->seqNo(),
	  fbs::ReqAckData::RoleMod, ackData.Union()));
    });
  });
}

// delete role
void Mgr::roleDel(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  m_roleTbl->run([
    this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
    auto fbRole = static_cast<const fbs::RoleID *>(fbRequest->data());
    auto roleName = Zfb::Load::str(fbRole->name());
    m_roleTbl->findDel<0>(ZuMvTuple(ZuMv(roleName)), [
      this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<Role> dbRole) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
      if (!dbRole) {
	auto fbRole = static_cast<const fbs::RoleID *>(fbRequest->data());
	auto roleName = Zfb::Load::str(fbRole->name());
	IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "role " << ZtQuote::String{roleName} << " not found"));
	return;
      }
      dbRole->commit();
      IOBuilder fbb;
      auto ackData = fbs::CreateAck(fbb);
      fn(respond(fbb, fbRequest->seqNo(),
	  fbs::ReqAckData::RoleMod, ackData.Union()));
    });
  });
}

// query permissions
void Mgr::permGet(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
  auto query = static_cast<const fbs::PermQuery *>(fbRequest->data());
  if (query->permKey_type() != fbs::PermKey::ID &&
      query->permKey_type() != fbs::PermKey::Name) {
    IOBuilder fbb;
    fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	<< "unknown query key type (" << int(query->permKey_type()) << ')'));
    return;
  }
  if (query->limit() > MaxQueryLimit) {
    IOBuilder fbb;
    fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	<< "maximum query limit exceeded ("
	<< query->limit() << " > " << MaxQueryLimit << ')'));
    return;
  }
  m_permTbl->run([
    this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
    auto query = static_cast<const fbs::PermQuery *>(fbRequest->data());
    auto tupleFn = [
      this,
      seqNo = fbRequest->seqNo(),
      fbb = IOBuilder{},
      offsets = ZtArray<Offset<fbs::Perm>>(query->limit()),
      fn = ZuMv(fn)
    ](auto result, unsigned) mutable {
      using Row = ZuFieldTuple<Perm>;
      if (result.template is<Row>()) {
	offsets.push(ZfbField::save(fbb, result.template p<Row>()));
      } else {
	auto ackData = fbs::CreatePermList(fbb,
	  fbb.CreateVector(&offsets[0], offsets.length()));
	fn(respond(fbb, seqNo, fbs::ReqAckData::PermGet, ackData.Union()));
      }
    };
    if (query->permKey_type() == fbs::PermKey::ID) {
      auto permID = static_cast<const fbs::PermID *>(query->permKey())->id();
      m_permTbl->nextRows<0>(
	ZuMvTuple(ZuMv(permID)),
	query->inclusive(),
	query->limit(),
	ZuMv(tupleFn));
    } else {
      auto permName = Zfb::Load::str(
	static_cast<const fbs::PermName *>(query->permKey())->name());
      m_permTbl->nextRows<1>(
	ZuMvTuple(ZuMv(permName)),
	query->inclusive(),
	query->limit(),
	ZuMv(tupleFn));
    }
  });
}

// add new permission
void Mgr::permAdd(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  m_permTbl->run([
    this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
    auto fbPerm = static_cast<const fbs::PermName *>(fbRequest->data());
    m_permTbl->find<1>(ZuMvTuple(Zfb::Load::str(fbPerm->name())), [
      this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<Perm> dbPerm) mutable {
      if (dbPerm) {
	auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
	auto fbPerm = static_cast<const fbs::PermName *>(fbRequest->data());
	auto permName = Zfb::Load::str(fbPerm->name());
	IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "perm " << ZtQuote::String{permName} << " already exists"));
	return;
      }
      m_permTbl->insert([
	this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
      ](ZdbObject<Perm> *dbPerm) mutable {
	auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
	auto fbPerm = static_cast<const fbs::PermName *>(fbRequest->data());
	auto permName = Zfb::Load::str(fbPerm->name());
	if (!dbPerm) {
	  IOBuilder fbb;
	  fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	      << "perm " << ZtQuote::String{permName} << " insert failed"));
	  return;
	}
	initPerm(dbPerm, permName);
	IOBuilder fbb;
	auto ackData = fbs::CreatePermID(fbb, dbPerm->data().id);
	fn(respond(fbb, fbRequest->seqNo(),
	    fbs::ReqAckData::PermAdd, ackData.Union()));
      });
    });
  });
}

// modify permission (name)
void Mgr::permMod(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  m_permTbl->run([
    this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
    auto fbPerm = static_cast<const fbs::Perm *>(fbRequest->data());
    m_permTbl->findUpd<0, ZuSeq<1>>(ZuMvTuple(fbPerm->id()), [
      this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<Perm> dbPerm) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
      auto fbPerm = static_cast<const fbs::Perm *>(fbRequest->data());
      if (!dbPerm) {
	IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "perm ID " << fbPerm->id() << " not found"));
	return;
      }
      auto &perm = dbPerm->data();
      perm.name = Zfb::Load::str(fbPerm->name());
      dbPerm->commit();
      IOBuilder fbb;
      auto ackData = fbs::CreateAck(fbb);
      fn(respond(fbb, fbRequest->seqNo(),
	  fbs::ReqAckData::PermMod, ackData.Union()));
    });
  });
}

// delete permission
void Mgr::permDel(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  m_permTbl->run([
    this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
    auto fbPerm = static_cast<const fbs::PermID *>(fbRequest->data());
    m_permTbl->findDel<0>(ZuMvTuple(fbPerm->id()), [
      this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<Perm> dbPerm) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
      if (!dbPerm) {
	auto fbPerm = static_cast<const fbs::PermID *>(fbRequest->data());
	IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "perm ID " << fbPerm->id() << " not found"));
	return;
      }
      dbPerm->commit();
      IOBuilder fbb;
      auto ackData = fbs::CreateAck(fbb);
      fn(respond(fbb, fbRequest->seqNo(),
	  fbs::ReqAckData::PermMod, ackData.Union()));
    });
  });
}

// query keys
void Mgr::ownKeyGet(ZmRef<Session> session, ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
  keyGet_(
    fbRequest->seqNo(), session->user->data().id,
    fbs::ReqAckData::OwnKeyGet, ZuMv(fn));
}
void Mgr::keyGet(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
  auto query = static_cast<const fbs::UserID *>(fbRequest->data());
  keyGet_(fbRequest->seqNo(), query->id(), fbs::ReqAckData::KeyGet, ZuMv(fn));
}
void Mgr::keyGet_(
  SeqNo seqNo, UserID userID, fbs::ReqAckData ackType, ResponseFn fn)
{
  m_keyTbl->run([
    this, seqNo, userID, ackType, fn = ZuMv(fn)
  ]() mutable {
    m_keyTbl->selectKeys<0>(ZuMvTuple(ZuMv(userID)), MaxAPIKeys, [
      this, 
      seqNo,
      fbb = IOBuilder{},
      offsets = ZtArray<Offset<Zfb::Bytes>>(MaxAPIKeys),
      ackType,
      fn = ZuMv(fn)
    ](auto result, unsigned) mutable {
      using KeyID = ZuFieldKeyT<Key, 0>;
      if (result.template is<KeyID>()) {
	offsets.push(Zfb::CreateBytes(fbb, Zfb::Save::bytes(fbb,
	      result.template p<KeyID>().template p<1>())));
      } else {
	auto ackData = fbs::CreateKeyIDList(fbb,
	  fbb.CreateVector(&offsets[0], offsets.length()));
	fn(respond(fbb, seqNo, ackType, ackData.Union()));
      }
    });
  });
}

// add key
void Mgr::ownKeyAdd(ZmRef<Session> session, ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
  keyAdd_(
    fbRequest->seqNo(), session->user->data().id,
    fbs::ReqAckData::OwnKeyAdd, ZuMv(fn));
}
void Mgr::keyAdd(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
  auto fbUserID = static_cast<const fbs::UserID *>(fbRequest->data());
  keyAdd_(
    fbRequest->seqNo(), fbUserID->id(), fbs::ReqAckData::KeyAdd, ZuMv(fn));
}
void Mgr::keyAdd_(
  SeqNo seqNo, UserID userID, fbs::ReqAckData ackType, ResponseFn fn)
{
  m_keyTbl->run([
    this, seqNo, userID, ackType, fn = ZuMv(fn)
  ]() mutable {
    // generate random key ID
    KeyIDData keyID;
    keyID.length(keyID.size());
    m_rng->random(keyID);
    m_keyTbl->find<1>(ZuFwdTuple(keyID), [
      this, seqNo, userID, keyID, ackType, fn = ZuMv(fn)
    ](ZdbObjRef<Key> dbKey) mutable {
      if (dbKey) {
	// key ID collision - regenerate and retry
	m_keyTbl->run([
	  this, seqNo, userID, ackType, fn = ZuMv(fn)
	]() mutable {
	  keyAdd_(seqNo, userID, ackType, ZuMv(fn));
	});
	return;
      }
      m_keyTbl->insert([
	this, seqNo, userID, keyID, ackType, fn = ZuMv(fn)
      ](ZdbObject<Key> *dbKey) mutable {
	if (!dbKey) {
	  IOBuilder fbb;
	  fn(reject(fbb, seqNo, __LINE__, ZtString{}
	      << "key insert failed for user ID " << userID));
	  return;
	}
	initKey(dbKey, userID, keyID);
	IOBuilder fbb;
	auto ackData = ZfbField::save(fbb, dbKey->data());
	fn(respond(fbb, seqNo, ackType, ackData.Union()));
      });
    });
  });
}

// clear keys
void Mgr::ownKeyClr(ZmRef<Session> session, ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
  keyClr_(
    fbRequest->seqNo(), session->user->data().id,
    fbs::ReqAckData::OwnKeyClr, ZuMv(fn));
}
void Mgr::keyClr(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
  auto fbUserID = static_cast<const fbs::UserID *>(fbRequest->data());
  keyClr_(
    fbRequest->seqNo(), fbUserID->id(), fbs::ReqAckData::KeyClr, ZuMv(fn));
}
void Mgr::keyClr_(
  SeqNo seqNo, UserID userID, fbs::ReqAckData ackType, ResponseFn fn)
{
  m_keyTbl->run([
    this, seqNo, userID, ackType, fn = ZuMv(fn)
  ]() mutable {
    keyClr__(userID, [
      this, seqNo, ackType, fn = ZuMv(fn)
    ]() {
      IOBuilder fbb;
      auto ackData = fbs::CreateAck(fbb);
      fn(respond(fbb, seqNo, ackType, ackData.Union()));
    });
  });
}

// delete key
void Mgr::ownKeyDel(ZmRef<Session> session, ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
  auto fbKeyID = static_cast<const fbs::KeyID *>(fbRequest->data());
  auto userID = session->user->data().id;
  auto keyID = Zfb::Load::bytes(fbKeyID->id());
  m_keyTbl->findDel<0>(ZuMvTuple(ZuMv(userID), ZuMv(keyID)), [
      this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<Key> dbKey) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
      auto fbKeyID = static_cast<const fbs::KeyID *>(fbRequest->data());
      auto keyID = Zfb::Load::bytes(fbKeyID->id());
      if (!dbKey) {
	IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "key " << ZtQuote::Base64{keyID} << " not found"));
	return;
      }
      dbKey->commit();
      IOBuilder fbb;
      auto ackData = fbs::CreateAck(fbb);
      fn(respond(fbb, fbRequest->seqNo(),
	  fbs::ReqAckData::OwnKeyDel, ackData.Union()));
    });
}
void Mgr::keyDel(ZmRef<IOBuf> reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
  auto fbKeyID = static_cast<const fbs::KeyID *>(fbRequest->data());
  auto keyID = Zfb::Load::bytes(fbKeyID->id());
  m_keyTbl->findDel<1>(ZuMvTuple(ZuMv(keyID)), [
      this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<Key> dbKey) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(reqBuf->data());
      auto fbKeyID = static_cast<const fbs::KeyID *>(fbRequest->data());
      auto keyID = Zfb::Load::bytes(fbKeyID->id());
      if (!dbKey) {
	IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "key " << ZtQuote::Base64{keyID} << " not found"));
	return;
      }
      dbKey->commit();
      IOBuilder fbb;
      auto ackData = fbs::CreateAck(fbb);
      fn(respond(fbb, fbRequest->seqNo(),
	  fbs::ReqAckData::KeyDel, ackData.Union()));
    });
}

} // Zum::Server
