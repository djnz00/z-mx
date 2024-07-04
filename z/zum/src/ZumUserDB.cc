//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// server-side user DB with MFA, API keys, etc.

#include <zlib/ZumUserDB.hh>

#include <zlib/ZeLog.hh>

#include <zlib/ZuBase32.hh>
#include <zlib/ZtlsTOTP.hh>

namespace ZumUserDB {

Mgr::Mgr(Ztls::Random *rng, unsigned passLen,
    unsigned totpRange, unsigned keyInterval) :
  m_rng{rng},
  m_passLen{passLen},
  m_totpRange{totpRange},
  m_keyInterval{keyInterval}
{
  m_sessions = new Sessions;
}

Mgr::~Mgr()
{
  m_users->clean();
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

// return permission name for permID
inline ZtString permName(unsigned permID)
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

  m_perlTbl->run([this]() { open_recoverNextPermID(); });
}
// recover nextPermID
void Mgr::open_recoverNextPermID()
{
  m_permTbl->glob<0>(ZuTuple<>{}, 0, 1, [this](auto max) {
    using Key = ZuFieldKeyT<Perm, 0>;
    if (max.template is<Key>())
      m_nextPermID = max.template p<Key>().template p<0>() + 1;
    else
      m_nextPermID = 0;
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
      m_permTbl->insert([this](ZdbObject<Perm> *perm) {
	if (!perm) { bootstrapped(BootstrapResult{false}); return; }
	auto &context = m_state.p<Bootstrap>();
	initPerm(perm, context.permID);
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
  if (++context.permID < nPerms())
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
      m_roleTbl->insert([this](ZdbObject<Role> *role) mutable {
	if (!role) { bootstrapped(BootstrapResult{false}); return; }
	auto &context = m_state.p<Bootstrap>();
	initRole(role, context.roleName);
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
  ](ZmRef<ZdbObject<User>> user) mutable {
    if (!user)
      m_userTbl->insert([this](ZdbObject<User> *user) mutable {
	if (!user) { bootstrapped(BootstrapResult{false}); return; }
	ZtString secret;
	auto &context = m_state.p<Bootstrap>();
	initUser(user, 0,
	  ZuMv(context.userName), ZuMv(context.roleName),
	  User::Immutable | User::Enabled | User::ChPass);
	secret.length(ZuBase32::enclen(user->data().secret.length()));
	ZuBase32::encode(secret, user->data().secret);
	bootstrapped(BootstrapResult{BootstrapData{
	  user->data().passwd, secret}});
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

// initialize permission
void Mgr::initPerm(ZdbObject<Perm> *perm, unsigned i)
{
  new (perm->ptr()) Perm{m_perms[i] = m_nextPermID++, permName(i)};
  perm->commit();
}

// initialize role
void Mgr::initRole(ZdbObject<Role> *role, ZtString name)
{
  ZtBitmap perms;
  for (unsigned i = 0, n = nPerms(); i < n; i++) perms.set(m_perms[i]);
  new (role->ptr()) Role{ZuMv(name), RoleFlags::Immutable, perms, perms};
  role->commit();
};

// initialize user
void Mgr::initUser(
  ZdbObject<User> *user_,
  uint64_t id, ZtString name, ZtString role, UserFlags::T flags)
{
  auto user =
    new (user_->ptr()) User{.id = id, .name = ZuMv(name), .flags = flags};
  {
    KeyData passwd_;
    unsigned passLen_ = ZuBase64::declen(m_passLen);
    if (passLen_ > passwd_.size()) passLen_ = passwd_.size();
    passwd_.length(passLen_);
    m_rng->random(passwd_);
    passwd.length(m_passLen);
    ZuBase64::encode(passwd, passwd_);
  }
  user->secret.length(user->secret.size());
  m_rng->random(user->secret);
  {
    Ztls::HMAC hmac(User::keyType());
    hmac.start(user->secret);
    hmac.update(passwd);
    user->hmac.length(user->hmac.size());
    hmac.finish(user->hmac.data());
  }
  if (role) user->roles.push(ZuMv(role));
  user_->commit();
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
// find and load the key
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
// find and load the user using userID
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
  const auto &user = session->user->data();
  if (user.failures) {
    user.failures = 0;
    m_userTbl->run([this, session = ZuMv(session), fn = ZuMv(fn)]() mutable {
      ZmRef<ZdbObject<User>> user = session->user;
      m_userTbl->update(ZuMv(user), [
	this, fn = ZuMv(fn)
      ](ZdbObject<User> *user) {
	if (user) user->commit();
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
    ](ZdbObject<User> *user) {
      if (user) user->commit();
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
    const auto &user = session->user->data();
    if (!user.flags & UserFlags::Enabled()) {
      if (++user.failures < 3) {
	ZeLOG(Warning, ([name = user.name](auto &s) {
	  s << "authentication failure: disabled user \""
	    << name << "\" attempted login"; }));
      }
      loginFailed(ZuMv(session), ZuMv(fn));
      return;
    }
    if (!(user.perms[m_perms[loginReqPerm(fbs::LoginReqData::Login)]])) {
      if (++user.failures < 3) {
	ZeLOG(Warning, ([name = user.name](auto &s) {
	  s << "authentication failure: user without login permission \""
	    << name << "\" attempted login"; }));
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
	    s << "authentication failure: user \""
	      << name << "\" provided invalid password"; }));
	}
	loginFailed(ZuMv(session), ZuMv(fn));
	return;
      }
    }
    if (!Ztls::TOTP::verify(user.secret, totp, m_totpRange)) {
      if (++user.failures < 3) {
	ZeLOG(Warning, ([name = user.name](auto &s) {
	  s << "authentication failure: user \""
	    << name << "\" provided invalid OTP"; }));
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
    const auto &user = session->user->data();
    if (!(user.flags & UserFlags::Enabled())) {
      if (++user.failures < 3) {
	ZeLOG(Warning, ([name = user.name](auto &s) {
	  s << "authentication failure: disabled user \""
	    << name << "\" attempted API key access"; }));
      }
      loginFailed(ZuMv(session), ZuMv(fn));
      return;
    }
    if (!(user.perms[m_perms[loginReqPerm(fbs::LoginReqData::Access)]])) {
      if (++user.failures < 3) {
	ZeLOG(Warning, ([name = user.name](auto &s) {
	  s << "authentication failure: user without API access permission \""
	    << name << "\" attempted access"; }));
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
	    s << "authentication failure: user \""
	      << name << "\" provided invalid API key HMAC"; }));
	}
	loginFailed(ZuMv(session), ZuMv(fn));
	return;
      }
    }
    loginSucceeded(ZuMv(session), ZuMv(fn));
  });
}

// login request
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

void Mgr::respond(
  Zfb::Builder &fbb,
  uint64_t seqNo, fbs::ReqAckData ackType, Offset<void> ackData)
{
  fbs::ReqAckBuilder fbb_(fbb);
  fbb_.add_seqNo(seqNo);
  fbb_.add_data_type(ackType);
  fbb_.add_data(ackData);
  fbb.Finish(fbb_.Finish());
  fn(fbb.buf());
}

void Mgr::reject(
  Zfb::Builder &fbb, uint64_t seqNo, unsigned rejCode, ZtString text)
{
  fn([seqNo, rejCode, text = ZuMv(text)](Zfb::Builder &fbb) {
    auto text_ = str(fbb, text);
    fbs::ReqAckBuilder fbb_(fbb);
    fbb_.add_seqNo(seqNo);
    fbb_.add_rejCode(rejCode);
    fbb_.add_rejText(text_);
    fbb.Finish(fbb_.Finish());
  });
}

void Mgr::request(Session *session, const fbs::Request *request, ResponseFn fn)
{
  uint64_t seqNo = request->seqNo();
  const void *reqData = request->data();
  int reqType = int(request->data_type());

  if (ZuUnlikely(!ok(session, m_perms[reqPerm(reqType)]))) {
    fn([seqNo](Zfb::Builder &fbb) {
      using namespace Zfb::Save;
      ZtString text = "permission denied";
      if (session->user->data().flags & UserFlags::ChPass())
	text << " (user must change password)\n";
      reject(fbb, seqNo, __LINE__, ZuMv(text));
    });
    return;
  }

  switch (reqType) {
    case int(fbs::ReqData::ChPass):
      chPass(
	session, seqNo,
	static_cast<const fbs::UserChPass *>(reqData), ZuMv(fn));
      break;
    case int(fbs::ReqData::OwnKeyGet):
      ackType = fbs::ReqAckData::OwnKeyGet;
      ackData = fbs::CreateKeyIDList(session, ownKeyGet(
	    session, seqNo, static_cast<const fbs::UserID *>(reqData)), ZuMv(fn));
      break;
    case int(fbs::ReqData::OwnKeyAdd):
      ackType = fbs::ReqAckData::KeyAdd;
      ackData = ownKeyAdd(
	  session, seqNo, static_cast<const fbs::UserID *>(reqData), ZuMv(fn));
      break;
    case int(fbs::ReqData::OwnKeyClr):
      ackType = fbs::ReqAckData::KeyClr;
      ackData = ownKeyClr(
	  session, seqNo, static_cast<const fbs::UserID *>(reqData), ZuMv(fn));
      break;
    case int(fbs::ReqData::OwnKeyDel):
      ackType = fbs::ReqAckData::KeyDel;
      ackData = ownKeyDel(
	  session, user, static_cast<const fbs::KeyID *>(reqData), ZuMv(fn));
      break;

    case int(fbs::ReqData::UserGet):
      userGet(
	session, seqNo,
	static_cast<const fbs::UserQuery *>(reqData), ZuMv(fn));
      break;
    case int(fbs::ReqData::UserAdd):
      ackType = fbs::ReqAckData::UserAdd;
      ackData =
	userAdd(session, static_cast<const fbs::User *>(reqData), ZuMv(fn));
      break;
    case int(fbs::ReqData::ResetPass):
      ackType = fbs::ReqAckData::ResetPass;
      ackData =
	resetPass(session, static_cast<const fbs::UserID *>(reqData), ZuMv(fn));
      break;
    case int(fbs::ReqData::UserMod):
      ackType = fbs::ReqAckData::UserMod;
      ackData =
	userMod(session, static_cast<const fbs::User *>(reqData), ZuMv(fn));
      break;
    case int(fbs::ReqData::UserDel):
      ackType = fbs::ReqAckData::UserDel;
      ackData =
	userDel(session, static_cast<const fbs::UserID *>(reqData), ZuMv(fn));
      break;

    case int(fbs::ReqData::RoleGet):
      ackType = fbs::ReqAckData::RoleGet;
      ackData = fbs::CreateRoleList(fbb,
	  roleGet(session, static_cast<const fbs::RoleID *>(reqData)), ZuMv(fn));
      break;
    case int(fbs::ReqData::RoleAdd):
      ackType = fbs::ReqAckData::RoleAdd;
      ackData =
	roleAdd(session, static_cast<const fbs::Role *>(reqData), ZuMv(fn));
      break;
    case int(fbs::ReqData::RoleMod):
      ackType = fbs::ReqAckData::RoleMod;
      ackData =
	roleMod(session, static_cast<const fbs::Role *>(reqData), ZuMv(fn));
      break;
    case int(fbs::ReqData::RoleDel):
      ackType = fbs::ReqAckData::RoleDel;
      ackData =
	roleDel(session, static_cast<const fbs::RoleID *>(reqData), ZuMv(fn));
      break;

    case int(fbs::ReqData::PermGet):
      ackType = fbs::ReqAckData::PermGet;
      ackData = fbs::CreatePermList(fbb,
	  permGet(session, static_cast<const fbs::PermID *>(reqData)), ZuMv(fn));
      break;
    case int(fbs::ReqData::PermAdd):
      ackType = fbs::ReqAckData::PermAdd;
      ackData =
	permAdd(session, static_cast<const fbs::PermAdd *>(reqData), ZuMv(fn));
      break;
    case int(fbs::ReqData::PermMod):
      ackType = fbs::ReqAckData::PermMod;
      ackData =
	permMod(session, static_cast<const fbs::Perm *>(reqData), ZuMv(fn));
      break;
    case int(fbs::ReqData::PermDel):
      ackType = fbs::ReqAckData::PermDel;
      ackData =
	permDel(session, static_cast<const fbs::PermID *>(reqData), ZuMv(fn));
      break;

    case int(fbs::ReqData::KeyGet):
      ackType = fbs::ReqAckData::KeyGet;
      ackData = fbs::CreateKeyIDList(fbb,
	  keyGet(session, static_cast<const fbs::UserID *>(reqData)), ZuMv(fn));
      break;
    case int(fbs::ReqData::KeyAdd):
      ackType = fbs::ReqAckData::KeyAdd;
      ackData =
	keyAdd(session, static_cast<const fbs::UserID *>(reqData), ZuMv(fn));
      break;
    case int(fbs::ReqData::KeyClr):
      ackType = fbs::ReqAckData::KeyClr;
      ackData =
	keyClr(session, static_cast<const fbs::UserID *>(reqData), ZuMv(fn));
      break;
    case int(fbs::ReqData::KeyDel):
      ackType = fbs::ReqAckData::KeyDel;
      ackData =
	keyDel(session, static_cast<const fbs::KeyID *>(reqData), ZuMv(fn));
      break;
  }

}

template <typename T> using Offset = Zfb::Offset<T>;
template <typename T> using Vector = Zfb::Vector<T>;

using namespace Zfb;
using namespace Save;

Offset<fbs::UserAck> Mgr::chPass(
    Session *session, uint64_t seqNo,
    const fbs::UserChPass *userChPass, ResponseFn fn)
{
  const auto &user = session->user->data();
  ZuString oldPass = Load::str(userChPass->oldpass());
  ZuString newPass = Load::str(userChPass->newpass());
  Ztls::HMAC hmac(User::keyType());
  KeyData verify;
  hmac.start(user->secret);
  hmac.update(oldPass);
  verify.length(verify.size());
  hmac.finish(verify.data());
  if (verify != user.hmac) {
    fn([seqNo](Zfb::Builder &fbb) {
      auto ackData = fbs::CreateUserAck(fbb, 0);
      respond(fbb, seqNo, fbs::ReqAckData::ChPass, ackData.Union(), ZuMv(fn));
    });
    return;
  }
  user.flags &= ~UserFlags::ChPass();
  hmac.reset();
  hmac.update(newPass);
  hmac.finish(user.hmac.data());
  m_userTbl->run([
    this, seqNo, session = ZuMv(session), fn = ZuMv(fn)
  ]() mutable {
    ZmRef<ZdbObject<User>> user = session->user;
    m_userTbl->update(ZuMv(user), [
      this, seqNo, fn = ZuMv(fn)
    ](ZdbObject<User> *user) {
      if (user) user->commit();
      fn([seqNo](Zfb::Builder &fbb) {
	auto ackData = fbs::CreateUserAck(fbb, 1);
	respond(fbb, seqNo, fbs::ReqAckData::ChPass, ackData.Union());
      });
    });
  });
}

Offset<Vector<Offset<fbs::User>>> Mgr::userGet(
    Session *session, uint64_t seqNo,
    const fbs::UserQuery *query, ResponseFn fn)
{
  m_userTbl->run([]() {
    switch (unsigned(query->permKey_type())) {
      case unsigned(fbs::PermKey::ID):
	m_userTbl->glob<0>(ZuFwdTuple(query->permKey_as_ID()->id()), query->offset(), query->limit(), [](auto result) {
	  using Key = ZuFieldKeyT<User, 0>;
	  if (result.template is<Key>()) {
	    // FIXME - we want the whole row tuple, not just the key
	  }
	});
      case unsigned(fbs::PermKey::Name):
	m_userTbl->glob<1>(ZuFwdTuple(Zfb::Load::str(query->permKey_as_Name()))
    }
  });
  if (!Zfb::IsFieldPresent(id_, fbs::UserID::VT_ID)) {
    // FIXME - need offset, n, glob
    auto i = m_users->readIterator();
    return keyVecIter<fbs::User>(fbb, i.count(),
	[&i](Builder &fbb, unsigned) { return i.iterate()->save(fbb); });
  } else {
    auto id = id_->id();
    if (auto user = m_users->findPtr(id))
      return keyVec<fbs::User>(fbb, user->save(fbb));
    else
      return keyVec<fbs::User>(fbb);
  }
      ackData = fbs::CreateUserList(fbb,...);
  respond(fbb, seqNo, fbs::ReqAckData::UserGet, ackData.Union());
}

Offset<fbs::UserPass> Mgr::userAdd(
  Session *session, uint64_t seqNo, const fbs::User *user_, ResponseFn fn)
{
  Guard guard(m_lock);
  if (m_users->findPtr(user_->id())) {
    fbs::UserPassBuilder fbb_(fbb);
    fbb_.add_ok(0);
    return fbb_.Finish();
  }
  m_modified = true;
  ZtString passwd;
  ZmRef<User> user = userAdd_(
      user,
      user_->id(), Load::str(user_->name()), ZuString(),
      user_->flags() | User::ChPass, passwd);
  Load::all(user_->roles(), [this, &user](unsigned, auto roleName) {
    if (auto role = m_roles.findPtr(Load::str(roleName))) {
      user->roles.push(role);
      user->perms |= role->perms;
      user->apiperms |= role->apiperms;
    }
  });
  return fbs::CreateUserPass(fbb, user->save(fbb), str(fbb, passwd), 1);
}

Offset<fbs::UserPass> Mgr::resetPass(
    Session *session, uint64_t seqNo, const fbs::UserID *id_, ResponseFn fn)
{
  Guard guard(m_lock);
  auto id = id_->id();
  auto user = m_users->findPtr(id);
  if (!user) {
    fbs::UserPassBuilder fbb_(fbb);
    fbb_.add_ok(0);
    return fbb_.Finish();
  }
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
    hmac.start(user->secret);
    hmac.update(passwd);
    user->hmac.length(user->hmac.size());
    hmac.finish(user->hmac.data());
  }
  {
    auto i = m_keys->iterator();
    while (auto key = i.iterate())
      if (key->userID == id) i.del();
    user->keyList = nullptr;
  }
  return fbs::CreateUserPass(fbb, user->save(fbb), str(fbb, passwd), 1);
}

// only id, name, roles, flags are processed
Offset<fbs::UserUpdAck> Mgr::userMod(
    Session *session, uint64_t seqNo, const fbs::User *user_, ResponseFn fn)
{
  Guard guard(m_lock);
  auto id = user_->id();
  auto user = m_users->findPtr(id);
  if (!user || (user->flags & User::Immutable)) {
    fbs::UserUpdAckBuilder fbb_(fbb);
    fbb_.add_ok(0);
    return fbb_.Finish();
  }
  m_modified = true;
  if (auto name = Load::str(user_->name())) user->name = name;
  if (user_->roles()->size()) {
    user->roles.length(0);
    user->perms.zero();
    user->apiperms.zero();
    Load::all(user_->roles(), [this, &user](unsigned, auto roleName) {
      if (auto role = m_roles.findPtr(Load::str(roleName))) {
	user->roles.push(role);
	user->perms |= role->perms;
	user->apiperms |= role->apiperms;
      }
    });
  }
  if (Zfb::IsFieldPresent(user_, fbs::User::VT_FLAGS))
    user->flags = user_->flags();
  return fbs::CreateUserUpdAck(fbb, user->save(fbb), 1);
}

Offset<fbs::UserUpdAck> Mgr::userDel(
    Session *session, uint64_t seqNo, const fbs::UserID *id_, ResponseFn fn)
{
  Guard guard(m_lock);
  auto id = id_->id();
  ZmRef<User> user = m_users->del(id);
  if (!user || (user->flags & User::Immutable)) {
    fbs::UserUpdAckBuilder fbb_(fbb);
    fbb_.add_ok(0);
    return fbb_.Finish();
  }
  m_modified = true;
  {
    auto i = m_keys->iterator();
    while (auto key = i.iterate())
      if (key->userID == id) i.del();
  }
  return fbs::CreateUserUpdAck(fbb, user->save(fbb), 1);
}

Offset<Vector<Offset<fbs::Role>>> Mgr::roleGet(
    Session *session, uint64_t seqNo, const fbs::RoleID *id_, ResponseFn fn)
{
  ReadGuard guard(m_lock);
  auto name = Load::str(id_->name());
  if (!name) {
    auto i = m_roles.readIterator();
    return keyVecIter<fbs::Role>(fbb, i.count(),
	[&i](Builder &fbb, unsigned) { return i.iterate()->save(fbb); });
  } else {
    if (auto role = m_roles.findPtr(name))
      return keyVec<fbs::Role>(fbb, role->save(fbb));
    else
      return keyVec<fbs::Role>(fbb);
  }
}

Offset<fbs::RoleUpdAck> Mgr::roleAdd(
    Session *session, uint64_t seqNo, const fbs::Role *role_, ResponseFn fn)
{
  Guard guard(m_lock);
  auto name = Load::str(role_->name());
  if (m_roles.findPtr(name)) {
    fbs::RoleUpdAckBuilder fbb_(fbb);
    fbb_.add_ok(0);
    return fbb_.Finish();
  }
  m_modified = true;
  auto role = loadRole(role_);
  m_roles.addNode(role);
  return fbs::CreateRoleUpdAck(fbb, role->save(fbb), 1);
}

// only perms, apiperms, flags are processed
Offset<fbs::RoleUpdAck> Mgr::roleMod(
    Session *session, uint64_t seqNo, const fbs::Role *role_, ResponseFn fn)
{
  Guard guard(m_lock);
  auto name = Load::str(role_->name());
  auto role = m_roles.findPtr(name);
  if (!role || (role->flags & Role::Immutable)) {
    fbs::RoleUpdAckBuilder fbb_(fbb);
    fbb_.add_ok(0);
    return fbb_.Finish();
  }
  m_modified = true;
  if (role_->perms()->size()) {
    role->perms.zero();
    Load::all(role_->perms(), [role](unsigned i, uint64_t v) {
      if (i < Bitmap::Words) role->perms.data[i] = v;
    });
  }
  if (role_->apiperms()->size()) {
    role->apiperms.zero();
    Load::all(role_->apiperms(), [role](unsigned i, uint64_t v) {
      if (i < Bitmap::Words) role->apiperms.data[i] = v;
    });
  }
  if (Zfb::IsFieldPresent(role_, fbs::Role::VT_FLAGS))
    role->flags = role_->flags();
  return fbs::CreateRoleUpdAck(fbb, role->save(fbb), 1);
}

Offset<fbs::RoleUpdAck> Mgr::roleDel(
    Session *session, uint64_t seqNo, const fbs::RoleID *role_, ResponseFn fn)
{
  Guard guard(m_lock);
  auto name = Load::str(role_->name());
  auto role = m_roles.findPtr(name);
  if (!role || (role->flags & Role::Immutable)) {
    fbs::RoleUpdAckBuilder fbb_(fbb);
    fbb_.add_ok(0);
    return fbb_.Finish();
  }
  m_modified = true;
  {
    auto i = m_users->iterator();
    while (auto user = i.iterate())
      user->roles.grep([role](Role *role_) { return role == role_; });
  }
  m_roles.delNode(role);
  return fbs::CreateRoleUpdAck(fbb, role->save(fbb), 1);
}

Offset<Vector<Offset<fbs::Perm>>> Mgr::permGet(
    Session *session, uint64_t seqNo, const fbs::PermID *id_, ResponseFn fn)
{
  ReadGuard guard(m_lock);
  if (!Zfb::IsFieldPresent(id_, fbs::PermID::VT_ID)) {
    return keyVecIter<fbs::Perm>(fbb, m_nPerms,
	[this](Builder &fbb, unsigned i) {
	  return fbs::CreatePerm(fbb, i, str(fbb, m_perms[i]));
	});
  } else {
    auto id = id_->id();
    if (id < m_nPerms)
      return keyVec<fbs::Perm>(fbb,
	  fbs::CreatePerm(fbb, id, str(fbb, m_perms[id])));
    else
      return keyVec<fbs::Perm>(fbb);
  }
}

Offset<fbs::PermUpdAck> Mgr::permAdd(
    Session *session, uint64_t seqNo,
    const fbs::PermAdd *permAdd_, ResponseFn fn)
{
  Guard guard(m_lock);
  if (m_nPerms >= Bitmap::Bits) {
    fbs::PermUpdAckBuilder fbb_(fbb);
    fbb_.add_ok(0);
    return fbb_.Finish();
  }
  auto name = Load::str(permAdd_->name());
  auto id = m_nPerms++;
  m_perms[id] = name;
  m_permNames->add(m_perms[id], id);
  m_modified = true;
  return fbs::CreatePermUpdAck(fbb,
      fbs::CreatePerm(fbb, id, str(fbb, m_perms[id])), 1);
}

Offset<fbs::PermUpdAck> Mgr::permMod(
    Session *session, uint64_t seqNo, const fbs::Perm *perm_, ResponseFn fn)
{
  Guard guard(m_lock);
  auto id = perm_->id();
  if (id >= m_nPerms) {
    fbs::PermUpdAckBuilder fbb_(fbb);
    fbb_.add_ok(0);
    return fbb_.Finish();
  }
  m_modified = true;
  m_permNames->del(m_perms[id]);
  m_permNames->add(m_perms[id] = Load::str(perm_->name()), id);
  return fbs::CreatePermUpdAck(fbb,
      fbs::CreatePerm(fbb, id, str(fbb, m_perms[id])), 1);
}

Offset<fbs::PermUpdAck> Mgr::permDel(
    Session *session, uint64_t seqNo, const fbs::PermID *id_, ResponseFn fn)
{
  Guard guard(m_lock);
  auto id = id_->id();
  if (id >= m_nPerms) {
    fbs::PermUpdAckBuilder fbb_(fbb);
    fbb_.add_ok(0);
    return fbb_.Finish();
  }
  m_modified = true;
  m_permNames->del(m_perms[id]);
  ZtString name = ZuMv(m_perms[id]);
  if (id == m_nPerms - 1) {
    unsigned i = id;
    do { m_nPerms = i; } while (i && !m_perms[--i]);
  }
  return fbs::CreatePermUpdAck(fbb,
      fbs::CreatePerm(fbb, id, str(fbb, name)), 1);
}

Offset<Vector<Offset<Zfb::String>>> Mgr::ownKeyGet(
    Session *session, uint64_t seqNo, ResponseFn fn)
{
  ReadGuard guard(m_lock);
  if (user->id != userID_->id()) user = nullptr;
  return keyGet_(fbb, user);
}
Offset<Vector<Offset<Zfb::String>>> Mgr::keyGet(
    Session *session, uint64_t seqNo,
    const fbs::UserID *userID_, ResponseFn fn)
{
  ReadGuard guard(m_lock);
  return keyGet_(fbb,
      static_cast<const User *>(m_users->findPtr(userID_->id())));
}
Offset<Vector<Offset<Zfb::String>>> Mgr::keyGet_(
    Session *session, uint64_t seqNo, ZdbObjRef<User> user, ResponseFn fn)
{
  if (!user) return strVec(fbb);
  unsigned n = 0;
  for (auto key = user->keyList; key; key = key->next) ++n;
  return strVecIter(fbb, n,
      [key = user->keyList](unsigned) mutable {
	auto id = key->id;
	key = key->next;
	return id;
      });
}

Offset<fbs::KeyUpdAck> Mgr::ownKeyAdd(
    Session *session, uint64_t seqNo, ResponseFn fn)
{
  Guard guard(m_lock);
  if (user->id != userID_->id()) user = nullptr;
  return keyAdd_(fbb, user);
}
Offset<fbs::KeyUpdAck> Mgr::keyAdd(
    Session *session, uint64_t seqNo, const fbs::UserID *userID_, ResponseFn fn)
{
  Guard guard(m_lock);
  return keyAdd_(fbb,
      static_cast<User *>(m_users->findPtr(userID_->id())));
}
Offset<fbs::KeyUpdAck> Mgr::keyAdd_(
    Session *session, uint64_t seqNo, ZdbObjRef<User> user, ResponseFn fn)
{
  if (!user) {
    fbs::KeyUpdAckBuilder fbb_(fbb);
    fbb_.add_ok(0);
    return fbb_.Finish();
  }
  m_modified = true;
  ZtString keyID;
  do {
    Key::IDData keyID_;
    keyID_.length(keyID_.size());
    m_rng->random(keyID_);
    keyID.length(ZuBase64::enclen(keyID_.length()));
    ZuBase64::encode(keyID, keyID_);
  } while (m_keys->findPtr(ZuFwdTuple(keyID)));
  ZmRef<Key> key = new Key{ZuMv(keyID), user->id, user->keyList};
  key->secret.length(key->secret.size());
  m_rng->random(key->secret);
  user->keyList = key;
  m_keys->addNode(key);
  return fbs::CreateKeyUpdAck(fbb, key->save(fbb), 1);
}

Offset<fbs::UserAck> Mgr::ownKeyClr(
    Session *session, uint64_t seqNo, ResponseFn fn)
{
  Guard guard(m_lock);
  if (user->id != userID_->id()) user = nullptr;
  return keyClr_(fbb, user);
}
Offset<fbs::UserAck> Mgr::keyClr(
    Session *session, uint64_t seqNo, const fbs::UserID *userID_, ResponseFn fn)
{
  Guard guard(m_lock);
  return keyClr_(fbb,
      static_cast<User *>(m_users->findPtr(userID_->id())));
}
Offset<fbs::UserAck> Mgr::keyClr_(
    Session *session, uint64_t seqNo, ZdbObjRef<User> user, ResponseFn fn)
{
  if (!user) return fbs::CreateUserAck(fbb, 0);
  m_modified = true;
  auto id = user->id;
  {
    auto i = m_keys->iterator();
    while (auto key = i.iterate())
      if (key->userID == id) i.del();
  }
  user->keyList = nullptr;
  return fbs::CreateUserAck(fbb, 1);
}

Offset<fbs::UserAck> Mgr::ownKeyDel(
    Session *session, uint64_t seqNo, const fbs::KeyID *id_, ResponseFn fn)
{
  Guard guard(m_lock);
  auto keyID = Load::str(id_->id());
  Key *key = m_keys->findPtr(ZuFwdTuple(keyID));
  if (!key || user->id != key->userID) {
    fbs::UserAckBuilder fbb_(fbb);
    fbb_.add_ok(0);
    return fbb_.Finish();
  }
  return keyDel_(fbb, user, keyID);
}
Offset<fbs::UserAck> Mgr::keyDel(
    Session *session,
    uint64_t seqNo, const fbs::KeyID *id_, ResponseFn fn)
{
  Guard guard(m_lock);
  auto keyID = Load::str(id_->id());
  Key *key = m_keys->findPtr(ZuFwdTuple(keyID));
  if (!key) {
    fbs::UserAckBuilder fbb_(fbb);
    fbb_.add_ok(0);
    return fbb_.Finish();
  }
  return keyDel_(fbb,
      static_cast<User *>(m_users->findPtr(key->userID)), keyID);
}
Offset<fbs::UserAck> Mgr::keyDel_(
    Session *session, uint64_t seqNo,
    ZdbObjRef<User> user, ZuString keyID, ResponseFn fn)
{
  m_modified = true;
  ZmRef<Key> key = m_keys->del(ZuFwdTuple(keyID));
  if (!key) {
    fbs::UserAckBuilder fbb_(fbb);
    fbb_.add_ok(0);
    return fbb_.Finish();
  }
  if (user) {
    auto prev = user->keyList;
    if (prev == key)
      user->keyList = key->Key_::next;
    else
      while (prev) {
	if (prev->next == key) {
	  prev->next = key->Key_::next;
	  break;
	}
	prev = prev->next;
      }
  }
  return fbs::CreateUserAck(fbb, 1);
}

} // namespace ZumUserDB
