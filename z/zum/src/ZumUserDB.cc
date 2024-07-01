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
void Mgr::sessionStart(ZtString userName, bool interactive, SessionFn fn)
{
  ZuPtr<SessionStart> context =
    new SessionStart{ZuMv(userName), interactive, ZuMv(fn)};
  m_userTbl->run([this, context = ZuMv(context)]() mutable {
    sessionStart_findUser(ZuMv(context));
  });
}
// find and load the user
void Mgr::sessionStart_findUser(ZuPtr<SessionStart> context)
{
  m_userTbl->find<1>(ZuFwdTuple(userName), [
    this, context = ZuMv(context)
  ](ZmRef<ZdbObject<User>> user) mutable {
    if (!user) { sessionStarted(ZuMv(context), false); return; }
    context->session = new Session{ZuMv(user), context->interactive};
    if (!user.data().roles)
      sessionStarted(ZuMv(context), true);
    else
      roleTbl->run([this, context = ZuMv(context)]() mutable {
	sessionStart_findRole(ZuMv(context));
      });
  });
}
// find and load the user's roles and permissions
void Mgr::sessionStart_findRole(ZuPtr<SessionStart> context)
{
  const auto &role = context->session->user.data().roles[context->roleIndex];
  m_roleTbl->find<0>(ZuFwdTuple(role), [
    this, context = ZuMv(context)
  ](ZmRef<ZdbObject<Role>> role) mutable {
    if (!role) { sessionStarted(ZuMv(context), false); return; }
    if (context->interactive)
      context->session->perms |= role->data().perms;
    else
      context->session->perms |= role->data().apiperms;
    if (++context->roleIndex < context->session->user->data().roles.length())
      m_roleTbl->run([this, context = ZuMv(context)]() {
	sessionStart_findRole(ZuMv(context));
      });
    else
      sessionStarted(ZuMv(context), true);
  });
}
// inform app (session remains unauthenticated at this point)
void Mgr::sessionStarted(ZuPtr<SessionStart> context, bool ok)
{
  SessionFn fn = ZuMv(context->fn);
  ZmRef<Session> session = ZuMv(context->session);
  context = nullptr;
  if (!ok)
    fn(nullptr);
  else
    fn(ZuMv(session));
}

// login request
void Mgr::loginReq(const fbs::LoginReq *, ZmFn<void(ZmRef<Session>)> fn)
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

void Mgr::request(
  Session *session, const fbs::Request *request_,
  ZmFn<void(ZmRef<ZiAnyIOBuf>)> fn)
{
  uint64_t seqNo = request_->seqNo();
  const void *reqData = request_->data();
  fbs::ReqAckData ackType = fbs::ReqAckData::NONE;
  Offset<void> ackData = 0;

  unsigned reqType = unsigned(request_->data_type());

  {
    ReadGuard guard(m_lock);
    auto perm = m_perms[reqPerm(reqType)];
    if (ZuUnlikely(!ok(user, interactive, perm))) {
      using namespace Zfb::Save;
      ZtString text = "permission denied";
      if (user->flags & User::ChPass) text << " (user must change password)\n";
      auto text_ = str(fbb, text);
      fbs::ReqAckBuilder fbb_(fbb);
      fbb_.add_seqNo(seqNo);
      fbb_.add_rejCode(__LINE__);
      fbb_.add_rejText(text_);
      return fbb_.Finish();
    }
  }

  switch (reqType) {
    case int(fbs::ReqData::ChPass):
      ackType = fbs::ReqAckData::ChPass;
      ackData = chPass(
	  fbb, session, static_cast<const fbs::UserChPass *>(reqData)).Union();
      break;
    case int(fbs::ReqData::OwnKeyGet):
      ackType = fbs::ReqAckData::OwnKeyGet;
      ackData = fbs::CreateKeyIDList(fbb, ownKeyGet(
	    fbb, user, static_cast<const fbs::UserID *>(reqData))).Union();
      break;
    case int(fbs::ReqData::OwnKeyAdd):
      ackType = fbs::ReqAckData::KeyAdd;
      ackData = ownKeyAdd(
	  fbb, user, static_cast<const fbs::UserID *>(reqData)).Union();
      break;
    case int(fbs::ReqData::OwnKeyClr):
      ackType = fbs::ReqAckData::KeyClr;
      ackData = ownKeyClr(
	  fbb, user, static_cast<const fbs::UserID *>(reqData)).Union();
      break;
    case int(fbs::ReqData::OwnKeyDel):
      ackType = fbs::ReqAckData::KeyDel;
      ackData = ownKeyDel(
	  fbb, user, static_cast<const fbs::KeyID *>(reqData)).Union();
      break;

    case int(fbs::ReqData::UserGet):
      ackType = fbs::ReqAckData::UserGet;
      ackData = fbs::CreateUserList(fbb,
	  userGet(fbb, static_cast<const fbs::UserID *>(reqData))).Union();
      break;
    case int(fbs::ReqData::UserAdd):
      ackType = fbs::ReqAckData::UserAdd;
      ackData =
	userAdd(fbb, static_cast<const fbs::User *>(reqData)).Union();
      break;
    case int(fbs::ReqData::ResetPass):
      ackType = fbs::ReqAckData::ResetPass;
      ackData =
	resetPass(fbb, static_cast<const fbs::UserID *>(reqData)).Union();
      break;
    case int(fbs::ReqData::UserMod):
      ackType = fbs::ReqAckData::UserMod;
      ackData =
	userMod(fbb, static_cast<const fbs::User *>(reqData)).Union();
      break;
    case int(fbs::ReqData::UserDel):
      ackType = fbs::ReqAckData::UserDel;
      ackData =
	userDel(fbb, static_cast<const fbs::UserID *>(reqData)).Union();
      break;

    case int(fbs::ReqData::RoleGet):
      ackType = fbs::ReqAckData::RoleGet;
      ackData = fbs::CreateRoleList(fbb,
	  roleGet(fbb, static_cast<const fbs::RoleID *>(reqData))).Union();
      break;
    case int(fbs::ReqData::RoleAdd):
      ackType = fbs::ReqAckData::RoleAdd;
      ackData =
	roleAdd(fbb, static_cast<const fbs::Role *>(reqData)).Union();
      break;
    case int(fbs::ReqData::RoleMod):
      ackType = fbs::ReqAckData::RoleMod;
      ackData =
	roleMod(fbb, static_cast<const fbs::Role *>(reqData)).Union();
      break;
    case int(fbs::ReqData::RoleDel):
      ackType = fbs::ReqAckData::RoleDel;
      ackData =
	roleDel(fbb, static_cast<const fbs::RoleID *>(reqData)).Union();
      break;

    case int(fbs::ReqData::PermGet):
      ackType = fbs::ReqAckData::PermGet;
      ackData = fbs::CreatePermList(fbb,
	  permGet(fbb, static_cast<const fbs::PermID *>(reqData))).Union();
      break;
    case int(fbs::ReqData::PermAdd):
      ackType = fbs::ReqAckData::PermAdd;
      ackData =
	permAdd(fbb, static_cast<const fbs::PermAdd *>(reqData)).Union();
      break;
    case int(fbs::ReqData::PermMod):
      ackType = fbs::ReqAckData::PermMod;
      ackData =
	permMod(fbb, static_cast<const fbs::Perm *>(reqData)).Union();
      break;
    case int(fbs::ReqData::PermDel):
      ackType = fbs::ReqAckData::PermDel;
      ackData =
	permDel(fbb, static_cast<const fbs::PermID *>(reqData)).Union();
      break;

    case int(fbs::ReqData::KeyGet):
      ackType = fbs::ReqAckData::KeyGet;
      ackData = fbs::CreateKeyIDList(fbb,
	  keyGet(fbb, static_cast<const fbs::UserID *>(reqData))).Union();
      break;
    case int(fbs::ReqData::KeyAdd):
      ackType = fbs::ReqAckData::KeyAdd;
      ackData =
	keyAdd(fbb, static_cast<const fbs::UserID *>(reqData)).Union();
      break;
    case int(fbs::ReqData::KeyClr):
      ackType = fbs::ReqAckData::KeyClr;
      ackData =
	keyClr(fbb, static_cast<const fbs::UserID *>(reqData)).Union();
      break;
    case int(fbs::ReqData::KeyDel):
      ackType = fbs::ReqAckData::KeyDel;
      ackData =
	keyDel(fbb, static_cast<const fbs::KeyID *>(reqData)).Union();
      break;
  }

  fbs::ReqAckBuilder fbb_(fbb);
  fbb_.add_seqNo(seqNo);
  fbb_.add_data_type(ackType);
  fbb_.add_data(ackData);
  return fbb_.Finish();
}

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

ZmRef<User> Mgr::login(
  ZuString name, ZtString passwd, unsigned totp, SessionFn fn)
{
  sessionStart(name, true, [
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
    if (!(user.perms[m_perms[unsigned(fbs::LoginReqData::Login)]])) {
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
    if (user.failures) {
      user.failures = 0;
      m_userTbl->run([this, fn = ZuMv(fn), session = ZuMv(session)]() mutable {
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
  });
}

ZmRef<User> Mgr::access(
  ZtString keyID, ZtArray<uint8_t> token, int64_t stamp,
  ZtArray<uint8_t> hmac, SessionFn fn)
{
  sessionStart(name, false, [
    this, keyID = ZuMv(keyID), token = ZuMv(token), stamp, hmac = ZuMv(hmac)
  ](ZmRef<Session> session) {
    m_keyTbl->find<0>(ZuFwdTuple(
    // FIXME from here
    Key *key = m_keys->findPtr(ZuFwdTuple(keyID));
    if (!key) {
      failures = -1;
      fn(nullptr);
      return;
    }
    ZmRef<User> user = m_users->find(key->userID);
    if (!user) {
      failures = -1;
      fn(nullptr);
      return;
    }
    if (!(user->flags & User::Enabled)) {
      if (++user->failures < 3) {
	ZeLOG(Warning, ([user](auto &s) {
	  s << "authentication failure: disabled user \""
	    << user->name << "\" attempted login"; }));
      }
      failures = user->failures;
      fn(nullptr);
      return;
    }
    if (!(user->perms[Perm::Access])) {
      if (++user->failures < 3) {
	ZeLOG(Warning, ([user](auto &s) {
	  s << "authentication failure: user without API access permission \""
	    << user->name << "\" attempted access"; }));
      }
      failures = user->failures;
      fn(nullptr);
      return;
    }
    {
      int64_t delta = Zm::now().sec() - stamp;
      if (delta < 0) delta = -delta;
      if (delta >= m_keyInterval) {
	failures = user->failures;
	fn(nullptr);
	return;
      }
    }
    {
      Ztls::HMAC hmac_(Key::keyType());
      KeyData verify;
      hmac_.start(key->secret);
      hmac_.update(token);
      hmac_.update({reinterpret_cast<const uint8_t *>(&stamp), sizeof(int64_t)});
      verify.length(verify.size());
      hmac_.finish(verify.data());
      if (verify != hmac) {
	if (++user->failures < 3) {
	  ZeLOG(Warning, ([user](auto &s) {
	    s << "authentication failure: user \""
	      << user->name << "\" provided invalid API key HMAC"; }));
	}
	failures = user->failures;
	fn(nullptr);
	return;
      }
    }
    failures = 0;
    fn(ZuMv(session));
}

template <typename T> using Offset = Zfb::Offset<T>;
template <typename T> using Vector = Zfb::Vector<T>;

using namespace Zfb;
using namespace Save;

Offset<fbs::UserAck> Mgr::chPass(
    Zfb::Builder &fbb, User *user, const fbs::UserChPass *userChPass_)
{
  Guard guard(m_lock);
  ZuString oldPass = Load::str(userChPass_->oldpass());
  ZuString newPass = Load::str(userChPass_->newpass());
  Ztls::HMAC hmac(User::keyType());
  KeyData verify;
  hmac.start(user->secret);
  hmac.update(oldPass);
  verify.length(verify.size());
  hmac.finish(verify.data());
  if (verify != user->hmac)
    return fbs::CreateUserAck(fbb, 0);
  user->flags &= ~User::ChPass;
  m_modified = true;
  hmac.reset();
  hmac.update(newPass);
  hmac.finish(user->hmac.data());
  return fbs::CreateUserAck(fbb, 1);
}

Offset<Vector<Offset<fbs::User>>> Mgr::userGet(
    Zfb::Builder &fbb, const fbs::UserID *id_)
{
  ReadGuard guard(m_lock);
  if (!Zfb::IsFieldPresent(id_, fbs::UserID::VT_ID)) {
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
}

Offset<fbs::UserPass> Mgr::userAdd(Zfb::Builder &fbb, const fbs::User *user_)
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
    Zfb::Builder &fbb, const fbs::UserID *id_)
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
    Zfb::Builder &fbb, const fbs::User *user_)
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
    Zfb::Builder &fbb, const fbs::UserID *id_)
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
    Zfb::Builder &fbb, const fbs::RoleID *id_)
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
    Zfb::Builder &fbb, const fbs::Role *role_)
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
    Zfb::Builder &fbb, const fbs::Role *role_)
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
    Zfb::Builder &fbb, const fbs::RoleID *role_)
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
    Zfb::Builder &fbb, const fbs::PermID *id_)
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
    Zfb::Builder &fbb, const fbs::PermAdd *permAdd_)
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
    Zfb::Builder &fbb, const fbs::Perm *perm_)
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
    Zfb::Builder &fbb, const fbs::PermID *id_)
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
    Zfb::Builder &fbb, const User *user, const fbs::UserID *userID_)
{
  ReadGuard guard(m_lock);
  if (user->id != userID_->id()) user = nullptr;
  return keyGet_(fbb, user);
}
Offset<Vector<Offset<Zfb::String>>> Mgr::keyGet(
    Zfb::Builder &fbb, const fbs::UserID *userID_)
{
  ReadGuard guard(m_lock);
  return keyGet_(fbb,
      static_cast<const User *>(m_users->findPtr(userID_->id())));
}
Offset<Vector<Offset<Zfb::String>>> Mgr::keyGet_(
    Zfb::Builder &fbb, const User *user)
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
    Zfb::Builder &fbb, User *user, const fbs::UserID *userID_)
{
  Guard guard(m_lock);
  if (user->id != userID_->id()) user = nullptr;
  return keyAdd_(fbb, user);
}
Offset<fbs::KeyUpdAck> Mgr::keyAdd(
    Zfb::Builder &fbb, const fbs::UserID *userID_)
{
  Guard guard(m_lock);
  return keyAdd_(fbb,
      static_cast<User *>(m_users->findPtr(userID_->id())));
}
Offset<fbs::KeyUpdAck> Mgr::keyAdd_(
    Zfb::Builder &fbb, User *user)
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
    Zfb::Builder &fbb, User *user, const fbs::UserID *userID_)
{
  Guard guard(m_lock);
  if (user->id != userID_->id()) user = nullptr;
  return keyClr_(fbb, user);
}
Offset<fbs::UserAck> Mgr::keyClr(
    Zfb::Builder &fbb, const fbs::UserID *userID_)
{
  Guard guard(m_lock);
  return keyClr_(fbb,
      static_cast<User *>(m_users->findPtr(userID_->id())));
}
Offset<fbs::UserAck> Mgr::keyClr_(
    Zfb::Builder &fbb, User *user)
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
    Zfb::Builder &fbb, User *user, const fbs::KeyID *id_)
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
    Zfb::Builder &fbb, const fbs::KeyID *id_)
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
    Zfb::Builder &fbb, User *user, ZuString keyID)
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
