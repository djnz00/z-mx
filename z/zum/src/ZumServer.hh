//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// server-side RBAC user DB with MFA, API keys, etc.

#ifndef ZumServer_HH
#define ZumServer_HH

#ifndef ZumLib_HH
#include <zlib/ZumLib.hh>
#endif

#include <zlib/ZuArrayN.hh>

#include <zlib/ZmHash.hh>

#include <zlib/Zdb.hh>

#include <zlib/Zum.hh>

#include <zlib/zum_loginreq_fbs.h>
#include <zlib/zum_loginack_fbs.h>
#include <zlib/zum_request_fbs.h>
#include <zlib/zum_reqack_fbs.h>

namespace Zum::Server {

class Mgr;

struct Session_ {
  Mgr			*mgr = nullptr;
  ZdbObjRef<User>	user;
  ZdbObjRef<Key>	key;		// if API key access
  ZtBitmap		perms;		// effective permissions
  bool			interactive;

  static auto IDAxor(const Session_ &session) {
    return session.user->data().id;
  }
  static auto NameAxor(const Session_ &session) {
    return session.user->data().name;
  }
};
struct Session : public ZmPolymorph, public Session_ {
  using Session_::Session_;
  using Session_::operator =;
  template <typename ...Args>
  Session(Args &&...args) : Session_{ZuFwd<Args>(args)...} { }
};

// session start callback - nullptr if login/access failed
using SessionFn = ZmFn<void(ZmRef<Session>)>;

// request response callback
using ResponseFn = ZmFn<void(ZmRef<IOBuf>)>;

namespace MgrState {
  ZtEnumValues(MgrState, int,
    Uninitialized = 0, Initialized, Opening, Opened, OpenFailed, Bootstrap);
}

class ZvAPI Mgr {
public:
  Mgr(
    Ztls::Random *rng,
    unsigned passLen,
    unsigned totpRange,
    unsigned keyInterval);
  ~Mgr();

  void init(ZvCf *, ZiMultiplex *, Zdb *);
  void final();

  // mgr thread (may be shared)
  template <typename ...Args>
  void run(Args &&...args) const {
    m_mx->run(m_sid, ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  void invoke(Args &&...args) const {
    m_mx->invoke(m_sid, ZuFwd<Args>(args)...);
  }
  bool invoked() const { return m_mx->invoked(m_sid); }

  // open
  using OpenFn = ZmFn<void(bool)>;
  void open(OpenFn fn);

  // bootstrap
  struct BootstrapData { // bootstrap() result data
    ZtString passwd;
    ZtString secret;
  };
  using BootstrapResult = ZuUnion<bool, BootstrapData>;
  static bool bootstrapOK(const BootstrapResult &result) {
    return !result.is<bool>() || result.p<bool>();
  }
  using BootstrapFn = ZmFn<void(BootstrapResult)>;
  // one-time initialization (idempotent)
  void bootstrap(ZtString userName, ZtString roleName, BootstrapFn);

  // process login/access request
  void loginReq(ZmRef<IOBuf> reqBuf, SessionFn);

  // process user DB request
  void request(ZmRef<Session>, ZmRef<IOBuf> reqBuf, ResponseFn);

  // check permissions - ok(session, perm)
  bool ok(Session *session, unsigned permID) const {
    if ((session->user->data().flags & UserFlags::ChPass()) &&
	!session->key &&
	permID != m_perms[reqPerm(unsigned(fbs::ReqData::ChPass))])
      return false;
    return session->perms[permID];
  }

private:
  struct Open {	// internal open() context
    OpenFn	fn;
    unsigned	perm = 0;
  };
  void open_(ZuPtr<Open>);
  void open_recoverNextUserID(ZuPtr<Open>);
  void open_recoverNextPermID(ZuPtr<Open>);
  void open_findAddPerm(ZuPtr<Open>);
  void open_nextPerm(ZuPtr<Open>);
  void opened(ZuPtr<Open>, bool ok);

  struct Bootstrap { // internal bootstrap() context
    ZtString	userName;
    ZtString	roleName;
    BootstrapFn	fn;
  };
  void bootstrap_(ZuPtr<Bootstrap>);
  void bootstrap_findAddRole(ZuPtr<Bootstrap>);
  void bootstrap_findAddUser(ZuPtr<Bootstrap>);
  void bootstrapped(ZuPtr<Bootstrap>, BootstrapResult);

  // import flatbuffers types
  template <typename T> using Offset = Zfb::Offset<T>;
  template <typename T> using Vector = Zfb::Vector<T>;

  // start new session
  struct SessionLoad {
    using Cred = ZuUnion<ZtString, KeyIDData>;	// username or API key ID

    Cred		cred;
    SessionFn		fn;
    ZdbObjRef<Key>	key;	// null unless non-interactive
    ZmRef<Session>	session;
    unsigned		roleIndex = 0;
  };
  void sessionLoad_login(ZtString userName, SessionFn);
  void sessionLoad_access(KeyIDData keyID, SessionFn);
  void sessionLoad_findUser(ZuPtr<SessionLoad> context);
  void sessionLoad_findKey(ZuPtr<SessionLoad> context);
  void sessionLoad_findUserID(ZuPtr<SessionLoad> context);
  void sessionLoad_findRole(ZuPtr<SessionLoad> context);
  void sessionLoaded(ZuPtr<SessionLoad> context, bool ok);

  // process login/access request
  void loginReq_(ZmRef<IOBuf> reqBuf, SessionFn);

  // process request
  void request_(ZmRef<Session>, ZmRef<IOBuf> reqBuf, ResponseFn);

  // interactive login
  void login(
    ZtString user, ZtString passwd, unsigned totp, SessionFn);
  // API access
  void access(
    KeyIDData keyID, ZtArray<const uint8_t> token, int64_t stamp,
    ZtArray<const uint8_t> hmac, SessionFn);

  void loginSucceeded(ZmRef<Session>, SessionFn);
  void loginFailed(ZmRef<Session>, SessionFn);

  // acknowledge request (positively)
  ZmRef<IOBuf> respond(
    IOBuilder &fbb, SeqNo seqNo,
    fbs::ReqAckData ackType, Offset<void> ackData);
  // reject request
  ZmRef<IOBuf> reject(
    IOBuilder &fbb, SeqNo seqNo, unsigned rejCode, ZtString text);

  // initialize key
  void initKey(ZdbObject<Key> *, UserID, KeyIDData);

  // initialize permission
  void initPerm(ZdbObject<Perm> *, ZtString name);

  // initialize role
  void initRole(
    ZdbObject<Role> *, ZtString name,
    ZtBitmap perms, ZtBitmap apiperms, RoleFlags::T);

  // initialize user
  void initUser(
    ZdbObject<User> *, UserID, ZtString name,
    ZtArray<ZtString> roles, UserFlags::T,
    ZtString &passwd);

  // clear all API keys for a user
  template <typename L> void keyClr__(UserID id, L l);

  // change password
  void chPass(ZmRef<Session>, ZmRef<IOBuf> reqBuf, ResponseFn);

  // query users
  void userGet(ZmRef<IOBuf> reqBuf, ResponseFn);
  // add a new user
  void userAdd(ZmRef<IOBuf> reqBuf, ResponseFn);
  // reset password (also clears all API keys)
  void resetPass(ZmRef<IOBuf> reqBuf, ResponseFn);
  // modify user (name, roles, flags)
  void userMod(ZmRef<IOBuf> reqBuf, ResponseFn);
  // delete user (and associated API keys)
  void userDel(ZmRef<IOBuf> reqBuf, ResponseFn);
  
  // query roles
  void roleGet(ZmRef<IOBuf> reqBuf, ResponseFn);
  // add role
  void roleAdd(ZmRef<IOBuf> reqBuf, ResponseFn);
  // modify role (name, perms, apiperms, flags)
  void roleMod(ZmRef<IOBuf> reqBuf, ResponseFn);
  // delete role
  void roleDel(ZmRef<IOBuf> reqBuf, ResponseFn);
  
  // query permissions 
  void permGet(ZmRef<IOBuf> reqBuf, ResponseFn);
  // add new permission
  void permAdd(ZmRef<IOBuf> reqBuf, ResponseFn);
  // modify permission (name)
  void permMod(ZmRef<IOBuf> reqBuf, ResponseFn);
  // delete permission
  void permDel(ZmRef<IOBuf> reqBuf, ResponseFn);

  // query API keys for user
  void ownKeyGet(ZmRef<Session>, ZmRef<IOBuf> reqBuf, ResponseFn);
  void keyGet(ZmRef<IOBuf> reqBuf, ResponseFn);
  void keyGet_(SeqNo, UserID, fbs::ReqAckData, ResponseFn);
  // add API key for user
  void ownKeyAdd(ZmRef<Session>, ZmRef<IOBuf> reqBuf, ResponseFn);
  void keyAdd(ZmRef<IOBuf> reqBuf, ResponseFn);
  void keyAdd_(SeqNo, UserID, fbs::ReqAckData, ResponseFn);
  // clear all API keys for user
  void ownKeyClr(ZmRef<Session>, ZmRef<IOBuf> reqBuf, ResponseFn);
  void keyClr(ZmRef<IOBuf> reqBuf, ResponseFn);
  void keyClr_(SeqNo, UserID, fbs::ReqAckData, ResponseFn);
  // delete API key
  void ownKeyDel(ZmRef<Session>, ZmRef<IOBuf> reqBuf, ResponseFn);
  void keyDel(ZmRef<IOBuf> reqBuf, ResponseFn);
  void keyDel_(SeqNo, KeyIDData, fbs::ReqAckData, ResponseFn);

private:
  Ztls::Random		*m_rng = nullptr;
  unsigned		m_passLen;
  unsigned		m_totpRange;
  unsigned		m_keyInterval;
  unsigned		m_maxSize;

  ZiMultiplex		*m_mx = nullptr;
  unsigned		m_sid;

  ZmAtomic<int>		m_state = MgrState::Uninitialized;

  ZmRef<ZdbTable<User>>	m_userTbl;
  ZmRef<ZdbTable<Role>>	m_roleTbl;
  ZmRef<ZdbTable<Key>>	m_keyTbl;
  ZmRef<ZdbTable<Perm>>	m_permTbl;

  using NPerms = ZuUnsigned<
    unsigned(fbs::LoginReqData::MAX) + unsigned(fbs::ReqData::MAX)>;

  static constexpr unsigned nPerms() {
    return NPerms{};
  }
  static constexpr unsigned loginReqPerm(unsigned i) { return i - 1; }
  static constexpr unsigned reqPerm(unsigned i) {
    return unsigned(fbs::LoginReqData::MAX) + (i - 1);
  }

  UserID		m_nextUserID = 0;

  PermID		m_nextPermID = 0;
  PermID		m_perms[NPerms{}];
};

} // Zum::Server

#endif /* ZumServer_HH */
