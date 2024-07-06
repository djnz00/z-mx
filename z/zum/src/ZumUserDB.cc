//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// server-side user DB with MFA, API keys, etc.

#include <zlib/ZumUserDB.hh>

#include <zlib/ZuBase64.hh>
#include <zlib/ZuQuote.hh>

#include <zlib/ZeLog.hh>

#include <zlib/ZtlsTOTP.hh>

namespace Zum { namespace UserDB {

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

void Mgr::init(Zdb *db)
{
  m_userTbl = db->initTable<User>("user");
  m_roleTbl = db->initTable<Role>("role");
  m_keyTbl = db->initTable<Key>("key");
  m_permTbl = db->initTable<Perm>("perm");
}

void Mgr::final()
{
  m_userTbl = nullptr;
  m_roleTbl = nullptr;
  m_keyTbl = nullptr;
  m_permTbl = nullptr;
}

// return permission name for permission i
inline ZtString permName(unsigned i)
{
  ZtString s{"UserDB."};
  auto loginReqEnd = unsigned(fbs::LoginReqData::MAX);
  if (i < loginReqEnd) return s << fbs::EnumNamesLoginReqData()[i + 1];
  return s << fbs::EnumNamesReqData()[(i - loginReqEnd) + 1];
}

// initiate open sequence
void Mgr::open(OpenFn fn)
{
  // check for overlapping open/bootstrap or already opened
  if (!m_state.is<bool>() || m_state.p<bool>()) { fn(false); return; }

  // save context
  new (m_state.new_<Open>()) Open{ZuMv(fn);};

  m_userTbl->run([this]() { open_recoverNextUserID(); });
}
// recover nextUserID
void Mgr::open_recoverNextUserID()
{
  m_userTbl->selectKeys<0>(ZuTuple<>{}, 1, [this](auto max) {
    using Key = ZuFieldKeyT<User, 0>;
    if (max.template is<Key>())
      m_nextUserID = max.template p<Key>().template p<0>() + 1;
    m_perlTbl->run([this]() { open_recoverNextPermID(); });
  });
}
// recover nextPermID
void Mgr::open_recoverNextPermID()
{
  m_permTbl->selectKeys<0>(ZuTuple<>{}, 1, [this](auto max) {
    using Key = ZuFieldKeyT<Perm, 0>;
    if (max.template is<Key>())
      m_nextPermID = max.template p<Key>().template p<0>() + 1;
    m_perlTbl->run([this]() { open_findPerm(); })
  });
}
// find permission and update m_perms[]
void Mgr::open_findPerm()
{
  auto &context = m_state.p<Bootstrap>();
  m_permTbl->find<1>(ZuMvTuple(permName(context.permID)), [
    this
  ](ZmRef<ZdbObject<Perm>> perm) {
    auto &context = m_state.p<Bootstrap>();
    if (!perm) {
      ZeLOG(Fatal, ([permID = context.permID](auto &s) {
	s << "missing permission " << permName(permID);
      }));
      opened(false);
    } else {
      m_perms[context.permID] = perm->data().id;
      if (++context.permID < nPerms())
	m_permTbl->run([this]() mutable { open_findPerm(); });
      else
	opened(true);
    }
  });
}
// inform app of open result
void Mgr::opened(bool ok)
{
  auto fn = ZuMv(m_state.p<Open>().fn);
  m_state = ok;
  fn(ok);
}

// initiate bootstrap
void Mgr::bootstrap(ZtString userName, ZtString roleName, BootstrapFn fn)
{
  // check for overlapping open/bootstrap or already opened
  if (!m_state.is<bool>() || m_state.p<bool>()) { fn(false); return; }

  // save context
  new (m_state.new_<Bootstrap>()) Bootstrap{
    ZuMv(userName),
    ZuMv(roleName),
    ZuMv(fn)
  };

  m_permTbl->run([this]() mutable { bootstrap_findAddPerm(); });
}
// idempotent insert permission
void Mgr::bootstrap_findAddPerm()
{
  auto &context = m_state.p<Bootstrap>();
  m_permTbl->find<1>(ZuMvTuple(permName(context.permID)), [
    this
  ](ZmRef<ZdbObject<Perm>> perm) mutable {
    if (!perm)
      m_permTbl->insert([this](ZdbObject<Perm> *dbPerm) {
	if (!dbPerm) { bootstrapped(BootstrapResult{false}); return; }
	auto &context = m_state.p<Bootstrap>();
	initPerm(dbPerm, permName(context.permIndex));
	m_perms[context.permIndex] = dbPerm->data().id;
	bootstrap_nextPerm();
      });
    else
      bootstrap_nextPerm();
  });
}
// iterate to next permission
void Mgr::bootstrap_nextPerm()
{
  auto &context = m_state.p<Bootstrap>();
  if (++context.permIndex < nPerms())
    m_permTbl->run([this]() mutable { bootstrap_findAddPerm(); });
  else 
    m_roleTbl->run([this]() mutable { bootstrap_findAddRole(); });
}
// idempotent insert role
void Mgr::bootstrap_findAddRole()
{
  auto &context = m_state.p<Bootstrap>();
  m_roleTbl->find<0>(ZuFwdTuple(context.roleName), [
    this
  ](ZmRef<ZdbObject<Role>> role) mutable {
    if (!role)
      m_roleTbl->insert([this](ZdbObject<Role> *dbRole) mutable {
	if (!dbRole) { bootstrapped(BootstrapResult{false}); return; }
	auto &context = m_state.p<Bootstrap>();
	ZtBitmap perms;
	for (unsigned i = 0, n = nPerms(); i < n; i++) perms.set(m_perms[i]);
	initRole(
	  dbRole, context.roleName, perms, perms, RoleFlags::Immutable());
	bootstrap_findAddUser();
      });
    else
      m_userTbl->run([this]() mutable { bootstrap_findAddUser(); });
  });
}
// idempotent insert admin user
void Mgr::bootstrap_findAddUser()
{
  auto &context = m_state.p<Bootstrap>();
  m_userTbl->find<1>(ZuFwdTuple(context.userName), [
    this
  ](ZmRef<ZdbObject<User>> dbUser) mutable {
    if (!dbUser)
      m_userTbl->insert([this](ZdbObject<User> *dbUser) mutable {
	if (!dbUser) { bootstrapped(BootstrapResult{false}); return; }
	auto &context = m_state.p<Bootstrap>();
	ZtString passwd;
	initUser(dbUser, m_nextUserID++,
	  ZuMv(context.userName),
	  { ZuMv(context.roleName)) },
	  User::Immutable | User::Enabled | User::ChPass,
	  passwd);
	auto &user = dbUser->data();
	bootstrapped(BootstrapResult{BootstrapData{ZuMv(passwd)}});
      });
    else
      bootstrapped(BootstrapResult{true});
  });
}
// inform app of bootstrap result
void Mgr::bootstrapped(BootstrapResult result)
{
  auto fn = ZuMv(m_state.p<Bootstrap>().fn);
  m_state = bookstrapOK(result);
  fn(ZuMv(result));
}

// initialize API key
void Mgr::initKey(ZdbObject<Key> *dbKey, UserID userID, KeyIDData keyID)
{
  new (dbKey->ptr()) Key{.userID = userID, .id = keyID};
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
  ZdbObject<User> *dbUser,
  UserID id, ZtString name,
  ZtArray<ZtString> roles,
  UserFlags::T flags,
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
    KeyData passwd_;
    unsigned passLen_ = ZuBase64::declen(m_passLen);
    if (passLen_ > passwd_.size()) passLen_ = passwd_.size();
    passwd_.length(passLen_);
    m_rng->random(passwd_);
    passwd.length(m_passLen);
    ZuBase64::encode(passwd, passwd_);
  }
  user.secret.length(user.secret.size());
  m_rng->random(user.secret);
  {
    Ztls::HMAC hmac(User::keyType());
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
  ](ZmRef<ZdbObject<User>> user) mutable {
    if (!user) { sessionLoaded(ZuMv(context), false); return; }
    context->session = new Session{this, ZuMv(user)};
    if (!user.data().roles)
      sessionLoaded(ZuMv(context), true);
    else
      roleTbl->run([this, context = ZuMv(context)]() mutable {
	sessionLoad_findRole(ZuMv(context));
      });
  });
}
// find and load the key for an API session
void Mgr::sessionLoad_findKey(ZuPtr<SessionLoad> context)
{
  m_keyTbl->find<1>(ZuFwdTuple(context->cred.p<KeyIDData>()), [
    this, context = ZuMv(context)
  ](ZmRef<ZdbObject<Key>> key) mutable {
    if (!key) { sessionLoaded(ZuMv(context), false); return; }
    context->key = ZuMv(key);
    userTbl->run([this, context = ZuMv(context)]() mutable {
      sessionLoad_findUserID(ZuMv(context));
    });
  });
}
// find and load the user using the userID from the API key
void Mgr::sessionLoad_findUserID(ZuPtr<SessionLoad> context)
{
  m_userTbl->find<0>(ZuFwdTuple(context->key->data().userID), [
    this, context = ZuMv(context)
  ](ZmRef<ZdbObject<User>> user) mutable {
    if (!user) { sessionLoaded(ZuMv(context), false); return; }
    context->session = new Session{this, ZuMv(user), ZuMv(context->key)};
    if (!user.data().roles)
      sessionLoaded(ZuMv(context), true);
    else
      roleTbl->run([this, context = ZuMv(context)]() mutable {
	sessionLoad_findRole(ZuMv(context));
      });
  });
}
// find and load the user's roles and permissions
void Mgr::sessionLoad_findRole(ZuPtr<SessionLoad> context)
{
  const auto &role = context->session->user.data().roles[context->roleIndex];
  m_roleTbl->find<0>(ZuFwdTuple(role), [
    this, context = ZuMv(context)
  ](ZmRef<ZdbObject<Role>> role) mutable {
    if (!role) { sessionLoaded(ZuMv(context), false); return; }
    if (!context->key)
      context->session->perms |= role->data().perms;
    else
      context->session->perms |= role->data().apiperms;
    if (++context->roleIndex < context->session->user->data().roles.length())
      m_roleTbl->run([this, context = ZuMv(context)]() {
	sessionLoad_findRole(ZuMv(context));
      });
    else
      sessionLoaded(ZuMv(context), true);
  });
}
// inform app (session remains unauthenticated at this point)
void Mgr::sessionLoaded(ZuPtr<SessionLoad> context, bool ok)
{
  SessionFn fn = ZuMv(context->fn);
  ZmRef<Session> session = ZuMv(context->session);
  context = nullptr;
  if (!ok)
    fn(nullptr);
  else
    fn(ZuMv(session));
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
	this, fn = ZuMv(fn)
      ](ZdbObject<User> *dbUser) {
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
    ZmRef<ZdbObject<User>> user = session->user;
    m_userTbl->update(ZuMv(user), [
      this, fn = ZuMv(fn)
    ](ZdbObject<User> *dbUser) {
      if (dbUser) dbUser->commit();
      fn(nullptr);
    });
  });
}

// interactive login
ZmRef<User> Mgr::login(
  ZuString name, ZtString passwd, unsigned totp, SessionFn fn)
{
  sessionLoad_login(name, true, [
    this, passwd = ZuMv(passwd), totp, fn = ZuMv(fn)
  ](ZmRef<Session> session) {
    if (!session) { fn(nullptr); return; }
    auto &user = session->user->data();
    if (!user.flags & UserFlags::Enabled()) {
      if (++user.failures < 3) {
	ZeLOG(Warning, ([name = user.name](auto &s) {
	  s << "authentication failure: disabled user "
	    << ZuQuote::String{name} << " attempted login"; }));
      }
      loginFailed(ZuMv(session), ZuMv(fn));
      return;
    }
    if (!(user.perms[m_perms[loginReqPerm(fbs::LoginReqData::Login)]])) {
      if (++user.failures < 3) {
	ZeLOG(Warning, ([name = user.name](auto &s) {
	  s << "authentication failure: user without login permission "
	    << ZuQuote::String{name} << " attempted login"; }));
      }
      loginFailed(ZuMv(session), ZuMv(fn));
      return;
    }
    {
      Ztls::HMAC hmac(User::keyType());
      KeyData verify;
      hmac.start(user.secret);
      hmac.update(passwd);
      verify.length(verify.size());
      hmac.finish(verify.data());
      if (verify != user.hmac) {
	if (++user.failures < 3) {
	  ZeLOG(Warning, ([name = user.name](auto &s) {
	    s << "authentication failure: user "
	      << ZuQuote::String{name} << " provided invalid password"; }));
	}
	loginFailed(ZuMv(session), ZuMv(fn));
	return;
      }
    }
    if (!Ztls::TOTP::verify(user.secret, totp, m_totpRange)) {
      if (++user.failures < 3) {
	ZeLOG(Warning, ([name = user.name](auto &s) {
	  s << "authentication failure: user "
	    << ZuQuote::String{name} << " provided invalid OTP"; }));
      }
      loginFailed(ZuMv(session), ZuMv(fn));
      return;
    }
    loginSucceeded(ZuMv(session), ZuMv(fn));
  });
}

// non-interactive API access
ZmRef<User> Mgr::access(
  KeyIDData keyID, ZtArray<uint8_t> token, int64_t stamp,
  ZtArray<uint8_t> hmac, SessionFn fn)
{
  sessionLoad_access(keyID, false, [
    this, token = ZuMv(token), stamp, hmac = ZuMv(hmac)
  ](ZmRef<Session> session) {
    if (!session) { fn(nullptr); return; }
    auto &user = session->user->data();
    if (!(user.flags & UserFlags::Enabled())) {
      if (++user.failures < 3) {
	ZeLOG(Warning, ([name = user.name](auto &s) {
	  s << "authentication failure: disabled user "
	    << ZuQuote::String{name} << " attempted API key access"; }));
      }
      loginFailed(ZuMv(session), ZuMv(fn));
      return;
    }
    if (!(user.perms[m_perms[loginReqPerm(fbs::LoginReqData::Access)]])) {
      if (++user.failures < 3) {
	ZeLOG(Warning, ([name = user.name](auto &s) {
	  s << "authentication failure: user without API access permission "
	    << ZuQuote::String{name} << " attempted access"; }));
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
      Ztls::HMAC hmac_(Key::keyType());
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
	      << ZuQuote::String{name}
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
void Mgr::loginReq(const fbs::LoginReq *, SessionFn fn)
{
  using namespace Zfb::Load;
  switch (loginReq_->data_type()) {
    case fbs::LoginReqData::Access: {
      auto access = static_cast<const fbs::Access *>(loginReq_->data());
      this->access(
	str(access->keyID()),
	bytes(access->token()),
	access->stamp(),
	bytes(access->hmac()),
	ZuMv(fn));
    } break;
    case fbs::LoginReqData::Login: {
      auto login = static_cast<const fbs::Login *>(loginReq_->data());
      this->login(
	str(login->user()), str(login->passwd()), login->totp(),
	ZuMv(fn));
    } break;
    default:
      fn(nullptr);
      break;
  }
}

// respond to a request
ZmRef<ZiIOBuf<>> Mgr::respond(
  Zfb::IOBuilder &fbb,
  SeqNo seqNo, fbs::ReqAckData ackType, Offset<void> ackData)
{
  fbs::ReqAckBuilder fbb_(fbb);
  fbb_.add_seqNo(seqNo);
  fbb_.add_data_type(ackType);
  fbb_.add_data(ackData);
  fbb.Finish(fbb_.Finish());
  return fbb.buf();
}

// reject a request
ZmRef<ZiIOBuf<>> Mgr::reject(
  Zfb::IOBuilder &fbb, SeqNo seqNo, unsigned rejCode, ZtString text)
{
  auto text_ = str(fbb, text);
  fbs::ReqAckBuilder fbb_(fbb);
  fbb_.add_seqNo(seqNo);
  fbb_.add_rejCode(rejCode);
  fbb_.add_rejText(text_);
  fbb.Finish(fbb_.Finish());
  return fbb.buf();
}

// validate, permission check and dispatch a request
void Mgr::request(ZmRef<Session> session, ZuBytes reqBuf, ResponseFn fn)
{
  if (!Zfb::Verifier{&reqbuf[0], reqbuf.length()}.
      VerifyBuffer<fbs::Request>()) {
    Zfb::IOBuilder fbb;
    fn(reject(fbb, 0, __LINE__, "corrupt request"));
    return;
  }

  auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
  int reqType = int(fbRequest->data_type());

  if (ZuUnlikely(!ok(session, m_perms[reqPerm(reqType)]))) {
    Zfb::IOBuilder fbb;
    using namespace Zfb::Save;
    ZtString text = "permission denied";
    if (session->user->data().flags & UserFlags::ChPass())
      text << " (user must change password)\n";
    fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZuMv(text)));
    return;
  }

  switch (reqType) {
    case int(fbs::ReqData::ChPass):
      chPass(ZuMv(session), reqBuf, ZuMv(fn));
      break;

    case int(fbs::ReqData::OwnKeyGet):
      ownKeyGet(ZuMv(session), reqBuf, ZuMv(fn));
      break;
    case int(fbs::ReqData::OwnKeyAdd):
      ownKeyAdd(ZuMv(session), reqBuf, ZuMv(fn));
      break;
    case int(fbs::ReqData::OwnKeyClr):
      ownKeyClr(ZuMv(session), reqBuf, ZuMv(fn));
      break;
    case int(fbs::ReqData::OwnKeyDel):
      ownKeyDel(ZuMv(session), reqBuf, ZuMv(fn));
      break;

    case int(fbs::ReqData::UserGet):     userGet(reqBuf, ZuMv(fn)); break;
    case int(fbs::ReqData::UserAdd):     userAdd(reqBuf, ZuMv(fn)); break;
    case int(fbs::ReqData::ResetPass): resetPass(reqBuf, ZuMv(fn)); break;
    case int(fbs::ReqData::UserMod):     userMod(reqBuf, ZuMv(fn)); break;
    case int(fbs::ReqData::UserDel):     userDel(reqBuf, ZuMv(fn)); break;

    case int(fbs::ReqData::RoleGet): roleGet(reqBuf, ZuMv(fn)); break;
    case int(fbs::ReqData::RoleAdd): roleAdd(reqBuf, ZuMv(fn)); break;
    case int(fbs::ReqData::RoleMod): roleMod(reqBuf, ZuMv(fn)); break;
    case int(fbs::ReqData::RoleDel): roleDel(reqBuf, ZuMv(fn)); break;

    case int(fbs::ReqData::PermGet): permGet(reqBuf, ZuMv(fn)); break;
    case int(fbs::ReqData::PermAdd): permAdd(reqBuf, ZuMv(fn)); break;
    case int(fbs::ReqData::PermMod): permMod(reqBuf, ZuMv(fn)); break;
    case int(fbs::ReqData::PermDel): permDel(reqBuf, ZuMv(fn)); break;

    case int(fbs::ReqData::KeyGet): keyGet(reqBuf, ZuMv(fn)); break;
    case int(fbs::ReqData::KeyAdd): keyAdd(reqBuf, ZuMv(fn)); break;
    case int(fbs::ReqData::KeyClr): keyClr(reqBuf, ZuMv(fn)); break;
    case int(fbs::ReqData::KeyDel): keyDel(reqBuf, ZuMv(fn)); break;
  }
}

// change password
void Mgr::chPass(ZmRef<Session> session, ZuBytes reqBuf, ResponseFn fn)
{
  auto &user = session->user->data();
  auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
  auto chPass = static_cast<const fbs::UserChPass *>(fbRequest->data());
  ZuString oldPass = Zfb::Load::str(chPass->oldpass());
  ZuString newPass = Zfb::Load::str(chPass->newpass());
  // verify old password
  Ztls::HMAC hmac(User::keyType());
  KeyData verify;
  hmac.start(user.secret);
  hmac.update(oldPass);
  verify.length(verify.size());
  hmac.finish(verify.data());
  if (verify != user.hmac) {
    Zfb::IOBuilder fbb;
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
      Zfb::IOBuilder fbb;
      auto ackData = fbs::CreateAck(fbb);
      fn(respond(fbb, seqNo, fbs::ReqAckData::ChPass, ackData.Union()));
    });
  });
}

// query users
void Mgr::userGet(ZuBytes reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
  auto query = static_cast<const fbs::UserQuery *>(fbRequest->data());
  if (query->userKey_type() != fbs::UserKey::ID &&
      query->userKey_type() != fbs::UserKey::Name) {
    Zfb::IOBuilder fbb;
    fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	<< "unknown query key type (" << int(query->userKey_type()) << ')'));
    return;
  }
  if (query->limit() > MaxQueryLimit) {
    Zfb::IOBuilder fbb;
    fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	<< "maximum query limit exceeded ("
	<< query->limit() << " > " << MaxQueryLimit << ')'));
    return;
  }
  m_userTbl->run([
    this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
    auto query = static_cast<const fbs::UserQuery *>(fbRequest->data());
    auto tupleFn = [
      this,
      seqNo = fbRequest->seqNo(),
      fbb = Zfb::IOBuilder(),
      offsets = ZtArray<Offset<fbs::User>>(limit),
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
void Mgr::userAdd(ZuBytes reqBuf, ResponseFn fn)
{
  m_userTbl->run([
    this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
    auto fbUser = static_cast<const fbs::UserData *>(fbRequest->data());
    m_userTbl->find<1>(ZuMvTuple(Zfb::Load::str(fbUser->name())), [
      this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<User> dbUser) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
      if (dbUser) {
	Zfb::IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "user " << ZuQuote::String{userName} << " already exists"));
	return;
      }
      m_userTbl->insert([
	this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
      ](ZdbObject<User> *dbUser) mutable {
	auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
	auto fbUser = static_cast<const fbs::UserData *>(fbRequest->data());
	auto userName = Zfb::Load::str(fbUser->name());
	if (!dbUser) {
	  Zfb::IOBuilder fbb;
	  fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	      << "user " << ZuQuote::String{userName} << " insert failed"));
	  return;
	}
	ZtArray<ZtString> roles;
	roles.size(fbUser->roles()->size());
	Zfb::Load::all(fbUser->roles(), [&roles](unsigned, Zfb::String *role) {
	  roles.push(Zfb::Load::str(role));
	});
	ZtString passwd;
	initUser(
	  dbUser, m_nextUserID++, userName,
	  ZuMv(roles), fbUser->flags(), passwd);
	auto &user = dbUser->data();
	ZiIOBuilder fbb;
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
      this, id, l = ZuMv(l)
    ](auto result) {
      using KeyID = ZuFieldKeyT<Key, 0>;
      if (result.template is<KeyID>()) {
	m_keyTbl->run([this, id = result.template p<KeyID>()]() mutable {
	  m_keyTbl->findDel<1>(id, [](ZdbObject<Key> *dbKey) mutable {
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
void Mgr::resetPass(ZuBytes reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
  auto userID = static_cast<const fbs::UserID *>(fbRequest->data());
  m_userTbl->run([
    this, seqNo = fbRequest->seqNo(), id = userID->id(), fn = ZuMv(fn)
  ]() mutable {
    m_userTbl->findUpd<0>(ZuMvTuple(ZuMv(id)), [
      this, seqNo, id, fn = ZuMv(fn)
    ](ZdbObjRef<User> dbUser) mutable {
      if (!dbUser) {
	Zfb::IOBuilder fbb;
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
	Ztls::HMAC hmac(User::keyType());
	hmac.start(user.secret);
	hmac.update(passwd);
	user.hmac.length(user.hmac.size());
	hmac.finish(user.hmac.data());
      }
      dbUser->commit();
      keyClr__(id, [this, seqNo, fn = ZuMv(fn)]() {
	Zfb::IOBuilder fbb;
	auto ackData = fbs::CreateUserPass(fbb,
	  ZfbField::save(fbb, user), Zfb::Save::str(fbb, passwd));
	fn(respond(fbb, seqNo, fbs::ReqAckData::ResetPass, ackData.Union()));
      });
    });
  });
}

// modify user (name, roles, flags)
void Mgr::userMod(ZuBytes reqBuf, ResponseFn fn)
{
  m_userTbl->run([
    this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
    auto fbUser = static_cast<const fbs::UserData *>(fbRequest->data());

    auto updateFn = [
      this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<User> dbUser) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
      auto fbUser = static_cast<const fbs::UserData *>(fbRequest->data());
      if (!dbUser) {
	Zfb::IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "user ID " << fbUser-id() << " not found"));
	return;
      }
      auto &user = dbUser->data();
      if (user->flags & UserFlags::Immutable()) {
	Zfb::IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "user ID " << fbUser->id() << " is immutable"));
	return;
      }
      if (auto name = Zfb::Load::str(fbUser->name()))
	user.name = name;
      if (fbUser->roles()->size()) {
	user.roles.length(0);
	user.roles.size(fbUser->roles()->size());
	Zfb::Load::all(fbUser->roles(), [&user](unsigned, Zfb::String *role) {
	  user.roles.push(Zfb::Load::str(role));
	});
      }
      if (Zfb::IsFieldPresent(fbUser, fbs::User::VT_FLAGS))
	user.flags = fbUser->flags();
      dbUser->commit();
      Zfb::IOBuilder fbb;
      auto ackData = fbs::CreateUserData(fbb,
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
void Mgr::userDel(ZuBytes reqBuf, ResponseFn fn)
{
  m_userTbl->run([
    this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
    auto fbUser = static_cast<const fbs::UserID *>(fbRequest->data());
    m_userTbl->findDel<0>(ZuMvTuple(fbUser->id()), [
      this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<User> dbUser) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
      auto fbUser = static_cast<const fbs::UserData *>(fbRequest->data());
      if (!dbUser) {
	Zfb::IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "user ID " << fbUser-id() << " not found"));
	return;
      }
      dbUser->commit();
      keyClr__(id, [
	this, seqNo = fbRequest->seqNo(), dbUser = ZuMv(dbUser), fn = ZuMv(fn)
      ]() {
	Zfb::IOBuilder fbb;
	auto ackData = fbs::CreateAck(fbb);
	fn(respond(fbb, seqNo, fbs::ReqAckData::UserDel, ackData.Union()));
      });
    });
  });
}

// query roles
Mgr::roleGet(ZuBytes reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
  auto query = static_cast<const fbs::RoleQuery *>(fbRequest->data());
  if (query->limit() > MaxQueryLimit) {
    Zfb::IOBuilder fbb;
    fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	<< "maximum query limit exceeded ("
	<< query->limit() << " > " << MaxQueryLimit << ')'));
    return;
  }
  m_roleTbl->run([
    this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
    auto query = static_cast<const fbs::RoleQuery *>(fbRequest->data());
    m_roleTbl->nextRows<0>(
      ZuMvTuple(Zfb::Load::str(query->roleKey())),
      query->inclusive(),
      query->limit(), [
	this,
	seqNo = fbRequest->seqNo(),
	fbb = Zfb::IOBuilder(),
	offsets = ZtArray<Offset<fbs::Role>>(limit),
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
void Mgr::roleAdd(ZuBytes reqBuf, ResponseFn fn)
{
  m_roleTbl->run([
    this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
    auto fbRole = static_cast<const fbs::Role *>(fbRequest->data());
    m_roleTbl->find<0>(ZuMvTuple(Zfb::Load::str(fbRole->name())), [
      this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<Role> dbRole) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
      if (dbRole) {
	Zfb::IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "role " << ZuQuote::String{roleName} << " already exists"));
	return;
      }
      m_roleTbl->insert([
	this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
      ](ZdbObject<Role> *dbRole) mutable {
	auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
	auto fbRole = static_cast<const fbs::Role *>(fbRequest->data());
	auto roleName = Zfb::Load::str(fbRole->name());
	if (!dbRole) {
	  Zfb::IOBuilder fbb;
	  fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	      << "role " << ZuQuote::String{roleName} << " insert failed"));
	  return;
	}
	initRole(dbRole, ZuMv(roleName),
	  Zfb::Load::bitmap<ZtBitmap>(fbRole->perms()),
	  Zfb::Load::bitmap<ZtBitmap>(fbRole->apiperms()),
	  fbRole->flags());
	ZiIOBuilder fbb;
	auto ackData = fbs::CreateAck(fbb);
	fn(respond(fbb, fbRequest->seqNo(),
	    fbs::ReqAckData::RoleAdd, ackData.Union()));
      });
    });
  });
}

// modify role (name, perms, apiperms, flags)
void Mgr::roleMod(ZuBytes reqBuf, ResponseFn fn)
{
  m_roleTbl->run([
    this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
    auto fbRole = static_cast<const fbs::Role *>(fbRequest->data());
    m_roleTbl->findUpd<0>(ZuMvTuple(Zfb::Load::str(fbRole->name())), [
      this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<Role> dbRole) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
      auto fbRole = static_cast<const fbs::Role *>(fbRequest->data());
      auto roleName = Zfb::Load::str(fbRole-name());
      if (!dbRole) {
	Zfb::IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "role " << ZuQuote::String{roleName} << " not found"));
	return;
      }
      auto &role = dbRole->data();
      if (role->flags & RoleFlags::Immutable()) {
	Zfb::IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "role " << ZuQuote::String{roleName} << " is immutable"));
	return;
      }
      if (fbRole->perms()->size())
	role->perms = Zfb::Load::bitmap<ZtBitmap>(fbRole->perms());
      if (fbRole->apiperms()->size())
	role->apiperms = Zfb::Load::bitmap<ZtBitmap>(fbRole->apiperms());
      if (Zfb::IsFieldPresent(fbRole, fbs::Role::VT_FLAGS))
	role->flags = fbRole->flags();
      dbRole->commit();
      Zfb::IOBuilder fbb;
      auto ackData = fbs::CreateAck(fbb);
      fn(respond(fbb, fbRequest->seqNo(),
	  fbs::ReqAckData::RoleMod, ackData.Union()));
    });
  });
}

// delete role
void Mgr::roleDel(ZuBytes reqBuf, ResponseFn fn)
{
  m_roleTbl->run([
    this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
    auto fbRole = static_cast<const fbs::RoleID *>(fbRequest->data());
    m_roleTbl->findDel<0>(ZuMvTuple(Zfb::Load::str(fbRole->name())), [
      this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<Role> dbRole) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
      auto fbRole = static_cast<const fbs::RoleData *>(fbRequest->data());
      auto roleName = Zfb::Load::str(fbRole-name());
      if (!dbRole) {
	Zfb::IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "role " << ZuQuote::String{roleName} << " not found"));
	return;
      }
      dbRole->commit();
      Zfb::IOBuilder fbb;
      auto ackData = fbs::CreateAck(fbb);
      fn(respond(fbb, fbRequest->seqNo(),
	  fbs::ReqAckData::RoleMod, ackData.Union()));
    });
  });
}

// query permissions
void Mgr::permGet(ZuBytes reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
  auto query = static_cast<const fbs::PermQuery *>(fbRequest->data());
  if (query->permKey_type() != fbs::PermKey::ID &&
      query->permKey_type() != fbs::PermKey::Name) {
    Zfb::IOBuilder fbb;
    fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	<< "unknown query key type (" << int(query->permKey_type()) << ')'));
    return;
  }
  if (query->limit() > MaxQueryLimit) {
    Zfb::IOBuilder fbb;
    fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	<< "maximum query limit exceeded ("
	<< query->limit() << " > " << MaxQueryLimit << ')'));
    return;
  }
  m_permTbl->run([
    this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
    auto query = static_cast<const fbs::PermQuery *>(fbRequest->data());
    auto tupleFn = [
      this,
      seqNo = fbRequest->seqNo(),
      fbb = Zfb::IOBuilder(),
      offsets = ZtArray<Offset<fbs::Perm>>(limit),
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
	ZuMvTuple(permName),
	ZuMvTuple(ZuMv(key).p<ZtString>()),
	query->inclusive(),
	query->limit(),
	ZuMv(tupleFn));
    }
  });
}

// add new permission
void Mgr::permAdd(ZuBytes reqBuf, ResponseFn fn)
{
  m_permTbl->run([
    this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
    auto fbPerm = static_cast<const fbs::PermName *>(fbRequest->data());
    m_permTbl->find<1>(ZuMvTuple(Zfb::Load::str(fbPerm->name())), [
      this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<Perm> dbPerm) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
      auto fbPerm = static_cast<const fbs::PermName *>(fbRequest->data());
      auto permName = Zfb::Load::str(fbPerm->name());
      if (dbPerm) {
	Zfb::IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "perm " << ZuQuote::String{permName} << " already exists"));
	return;
      }
      m_permTbl->insert([
	this, reqBuf = ZuMv(reqBuf), fn = ZuMv(fn)
      ](ZdbObject<Perm> *dbPerm) mutable {
	auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
	auto fbPerm = static_cast<const fbs::Perm *>(fbRequest->data());
	auto permName = Zfb::Load::str(fbPerm->name());
	if (!dbPerm) {
	  Zfb::IOBuilder fbb;
	  fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	      << "perm " << ZuQuote::String{permName} << " insert failed"));
	  return;
	}
	initPerm(dbPerm, permName);
	ZiIOBuilder fbb;
	auto ackData = fbs::CreatePermID(fbb, dbPerm->data().id);
	fn(respond(fbb, fbRequest->seqNo(),
	    fbs::ReqAckData::PermAdd, ackData.Union()));
      });
    });
  });
}

// modify permission (name)
void Mgr::permMod(ZuBytes reqBuf, ResponseFn fn)
{
  m_permTbl->run([
    this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
    auto fbPerm = static_cast<const fbs::Perm *>(fbRequest->data());
    m_permTbl->findUpd<0, ZuSeq<1>>(ZuMvTuple(fbPerm->id()), [
      this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<Perm> dbPerm) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
      auto fbPerm = static_cast<const fbs::Perm *>(fbRequest->data());
      if (!dbPerm) {
	Zfb::IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "perm ID " << fbPerm->id() << " not found"));
	return;
      }
      auto &perm = dbPerm->data();
      perm.name = Zfb::Load::str(fbPerm-name());
      dbPerm->commit();
      Zfb::IOBuilder fbb;
      auto ackData = fbs::CreateAck(fbb);
      fn(respond(fbb, fbRequest->seqNo(),
	  fbs::ReqAckData::PermMod, ackData.Union()));
    });
  });
}

// delete permission
void Mgr::permDel(ZuBytes reqBuf, ResponseFn fn)
{
  m_permTbl->run([
    this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
  ]() mutable {
    auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
    auto fbPerm = static_cast<const fbs::PermID *>(fbRequest->data());
    m_permTbl->findDel<0>(ZuMvTuple(fbPerm->id()), [
      this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<Perm> dbPerm) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
      auto fbPerm = static_cast<const fbs::PermData *>(fbRequest->data());
      if (!dbPerm) {
	Zfb::IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "perm ID " << fbPerm->id() << " not found"));
	return;
      }
      dbPerm->commit();
      Zfb::IOBuilder fbb;
      auto ackData = fbs::CreateAck(fbb);
      fn(respond(fbb, fbRequest->seqNo(),
	  fbs::ReqAckData::PermMod, ackData.Union()));
    });
  });
}

// query keys
void Mgr::ownKeyGet(ZmRef<Session> session, ZuBytes reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
  keyGet_(
    fbRequest->seqNo(), session->user->data().id,
    fbs::ReqAckData::OwnKeyGet, ZuMv(fn));
}
void Mgr::keyGet(ZuBytes reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
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
      fbb = Zfb::IOBuilder(),
      offsets = ZtArray<Offset<Vector<uint8_t>>>(limit),
      fn = ZuMv(fn)
    ](auto result, unsigned) mutable {
      using Row = ZuFieldKeyT<Key, 0>;
      if (result.template is<Row>()) {
	offsets.push(Zfb::Save::bytes(result.template p<Row>().p<1>()));
      } else {
	auto ackData = fbs::CreateKeyIDList(fbb,
	  fbb.CreateVector(&offsets[0], offsets.length()));
	fn(respond(fbb, seqNo, ackType, ackData.Union()));
      }
    });
  });
}

// add key
void Mgr::ownKeyAdd(ZmRef<Session> session, ZuBytes reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
  keyAdd_(
    fbRequest->seqNo(), session->user->data().id,
    fbs::ReqAckData::OwnKeyAdd, ZuMv(fn));
}
void Mgr::keyAdd(ZuBytes reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
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
	  Zfb::IOBuilder fbb;
	  fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	      << "key insert failed for user ID " << userID));
	  return;
	}
	initKey(dbKey, userID, keyID);
	ZiIOBuilder fbb;
	auto ackData = ZfbField::save(fbb, dbKey->data());
	fn(respond(fbb, seqNo, ackType, ackData.Union()));
      });
    });
  });
}

// clear keys
void Mgr::ownKeyClr(ZmRef<Session> session, ZuBytes reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
  keyClr_(
    fbRequest->seqNo(), session->user->data().id,
    fbs::ReqAckData::OwnKeyClr, ZuMv(fn));
}
void Mgr::keyClr(ZuBytes reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
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
      Zfb::IOBuilder fbb;
      auto ackData = fbs::CreateAck(fbb);
      fn(respond(fbb, seqNo, ackType, ackData.Union()));
    });
  });
}

// delete key
void Mgr::ownKeyDel(ZmRef<Session> session, ZuBytes reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
  auto fbKeyID = static_cast<const fbs::KeyID *>(fbRequest->data());
  auto userID = session->user->data().id;
  auto keyID = Zfb::Load::bytes(fbKeyID->id());
  m_keyTbl->findDel<0>(ZuMvTuple(ZuMv(userID), ZuMv(keyID)), [
      this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<Key> dbKey) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
      auto fbKeyID = static_cast<const fbs::KeyID *>(fbRequest->data());
      auto keyID = Zfb::Load::bytes(fbKeyID->id());
      if (!dbKey) {
	Zfb::IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "key " << ZuBase64::Print{keyID} << " not found"));
	return;
      }
      dbKey->commit();
      Zfb::IOBuilder fbb;
      auto ackData = fbs::CreateAck(fbb);
      fn(respond(fbb, fbRequest->seqNo(),
	  fbs::ReqAckData::KeyMod, ackData.Union()));
    });
}
void Mgr::keyDel(ZuBytes reqBuf, ResponseFn fn)
{
  auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
  auto fbKeyID = static_cast<const fbs::KeyID *>(fbRequest->data());
  auto keyID = Zfb::Load::bytes(fbKeyID->id());
  m_keyTbl->findDel<1>(ZuMvTuple(ZuMv(keyID)), [
      this, reqBuf = ZtBytes(reqBuf), fn = ZuMv(fn)
    ](ZdbObjRef<Key> dbKey) mutable {
      auto fbRequest = Zfb::GetRoot<fbs::Request>(&reqBuf[0]);
      auto fbKeyID = static_cast<const fbs::KeyID *>(fbRequest->data());
      auto keyID = Zfb::Load::bytes(fbKeyID->id());
      if (!dbKey) {
	Zfb::IOBuilder fbb;
	fn(reject(fbb, fbRequest->seqNo(), __LINE__, ZtString{}
	    << "key " << ZuBase64::Print{keyID} << " not found"));
	return;
      }
      dbKey->commit();
      Zfb::IOBuilder fbb;
      auto ackData = fbs::CreateAck(fbb);
      fn(respond(fbb, fbRequest->seqNo(),
	  fbs::ReqAckData::KeyMod, ackData.Union()));
    });
}

} } // Zum::UserDB
