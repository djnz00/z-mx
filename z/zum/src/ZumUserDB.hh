//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// server-side RBAC user DB with MFA, API keys, etc.

#ifndef ZumUserDB_HH
#define ZumUserDB_HH

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <zlib/ZuArrayN.hh>

#include <zlib/ZmHash.hh>

#include <zlib/ZtString.hh>

#include <zlib/Zfb.hh>
#include <zlib/ZfbField.hh>

#include <zlib/Zdb.hh>

#include <zlib/ZtlsBase64.hh>
#include <zlib/ZtlsHMAC.hh>
#include <zlib/ZtlsRandom.hh>

#include <zlib/zum_key_fbs.h>
#include <zlib/zum_perm_fbs.h>
#include <zlib/zum_role_fbs.h>
#include <zlib/zum_user_fbs.h>
#include <zlib/zum_loginreq_fbs.h>
#include <zlib/zum_request_fbs.h>
#include <zlib/zum_reqack_fbs.h>

namespace Zum { namespace UserDB {

using SeqNo = uint64_t;

static constexpr mbedtls_md_type_t keyType() {
  return MBEDTLS_MD_SHA256;
}
enum { KeySize = Ztls::HMAC::Size<MBEDTLS_MD_SHA256>::N }; // 256 bit key
using KeyData = ZuArrayN<uint8_t, KeySize>;
enum { KeyIDSize = 16 };
using KeyIDData = ZuArrayN<uint8_t, KeyIDSize>;

using PermID = uint32_t;
using UserID = uint64_t;

enum { MaxQueryLimit = 1000 };	// maximum batch size for queries

enum { MaxAPIKeys = 10 };	// maximum number of API keys per user

struct Key {
  UserID		userID;
  KeyIDData		id;
  KeyData		secret;

  friend ZtFieldPrint ZuPrintType(Key *);
};
ZfbFields(Key,
  (((userID), (Keys<0>, Group<0>, Ctor<0>)), (UInt64)),
  (((id), (Keys<0, 1>, Ctor<1>)), (String)),
  (((secret), (Ctor<2>, Mutable, Hidden)), (Bytes)));

struct Perm {
  PermID		id;
  ZtString		name;

  friend ZtFieldPrint ZuPrintType(Perm *);
};
ZfbFields(Perm,
  (((id), (Keys<0>, Ctor<0>)), (UInt32)),
  (((name), (Keys<1>, Ctor<1>, Mutable)), (String)));

namespace RoleFlags {
  ZtEnumFlags(RoleFlags, uint8_t, Immutable);
}

struct Role {
  ZtString		name;
  ZtBitmap		perms;
  ZtBitmap		apiperms;
  uint8_t		flags;		// RoleFlags

  friend ZtFieldPrint ZuPrintType(Role *);
};
ZfbFields(Role,
  (((name), (Keys<0>, Ctor<0>)), (String)),
  (((perms), (Ctor<1>, Mutable)), (ZtBitmap)),
  (((apiperms), (Ctor<2>, Mutable)), (ZtBitmap)),
  (((flags), (Ctor<3>, Flags<RoleFlags::Map>)), (UInt8)));

namespace UserFlags {
  ZtEnumFlags(UserFlags, uint8_t,
    Immutable,
    Enabled,
    ChPass);		// user must change password
}

struct User {
  UserID		id;
  ZtString		name;
  KeyData		secret;
  KeyData		hmac;
  ZtArray<ZtString>	roles;
  uint32_t		failures = 0;
  UserFlags::T		flags = 0;	// UserFlags

  friend ZtFieldPrint ZuPrintType(User *);
};
ZfbFields(User,
  (((id), (Keys<0>, Ctor<0>)), (UInt64)),
  (((name), (Keys<1>, Ctor<1>, Mutable)), (String)),
  (((secret), (Ctor<2>, Mutable, Hidden)), (Bytes)),
  (((hmac), (Ctor<3>, Mutable)), (Bytes)),
  (((roles), (Ctor<4>, Mutable)), (StringVec)),
  (((failures), (Ctor<5>, Mutable)), (UInt32, 0)),
  (((flags), (Ctor<6>, Mutable, Flags<UserFlags::Map>)), (UInt8, 0)));

class Mgr;

struct Session : public ZmPolymorph {
  Mgr			*mgr = nullptr;
  ZdbObjRef<User>	user;
  ZdbObjRef<Key>	key;		// if API key access
  ZtBitmap		perms;		// effective permissions
  bool			interactive;

  static auto IDAxor(const Session &session) {
    return session.user->data().id;
  }
  static auto NameAxor(const Session &session) {
    return session.user->data().name;
  }
};

// session start callback - nullptr if login/access failed
using SessionFn = ZmFn<void(ZmRef<Session>)>;

// request response callback
using ResponseFn = ZmFn<void(ZmRef<ZiIOBuf<>>)>;

class ZvAPI Mgr {
public:
  Mgr(
    Ztls::Random *rng,
    unsigned passLen,
    unsigned totpRange,
    unsigned keyInterval);
  ~Mgr();

  void init(Zdb *db);
  void final();

  // open
  using OpenFn = ZmFn<void(bool)>;
  void open(OpenFn fn);

  // bootstrap
  struct BootstrapData { // bootstrap() result data
    ZtString passwd;
  };
  using BootstrapResult = ZuUnion<bool, BootstrapData>;
  static bool bootstrapOK(const BootstrapResult &result) {
    return !result.is<bool>() || result.p<bool>();
  }
  using BootstrapFn = ZmFn<void(BootstrapResult)>;
  // one-time initialization (idempotent)
  void bootstrap(BootstrapFn fn);

  // process login/access request
  void loginReq(ZuBytes reqBuf, SessionFn);

  // process user DB request
  void request(Session *, ZuBytes reqBuf, ResponseFn);

  // check permissions - ok(session, perm)
  bool ok(Session *session, unsigned permID) const {
    if ((session->user->data().flags & UserFlags::ChPass()) &&
	!session->key && perm != reqPerm(fbs::ReqData::ChPass))
      return false;
    return session->perms[permID];
  }

private:
  struct Open {	// internal open() context
    OpenFn	fn;
    unsigned	permIndex = 0;
  };
  void open_recoverNextUserID();
  void open_recoverNextPermID();
  void open_findPerm();
  void opened(bool ok);

  struct Bootstrap { // internal bootstrap() context
    ZtString	userName;
    ZtString	roleName;
    BootstrapFn	fn;
    unsigned	permID = 0;
  };
  void bootstrap_findAddPerm();
  void bootstrap_nextPerm();
  void bootstrap_findAddRole();
  void bootstrap_findAddUser();
  void bootstrapped(BootstrapResult);

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

  // interactive login
  void login(
    ZtString user, ZtString passwd, unsigned totp, SessionFn);
  // API access
  void access(
    ZtString keyID,
    ZtArray<const uint8_t> token, int64_t stamp, ZtArray<const uint8_t> hmac,
    SessionFn);

  // reject request
  void reject(
    SeqNo seqNo, unsigned rejCode, ZuString text, ResponseFn fn);
  // acknowledge request (positively)
  void respond(
    Zfb::Builder &fbb, SeqNo seqNo,
    fbs::ReqAckData ackType, Offset<void> ackData);

  // initialize key
  void initKey(ZdbObject<Key> *, UserID, KeyIDData);

  // initialize permission
  void initPerm(ZdbObject<Perm> *, unsigned i);

  // initialize role
  void initRole(ZdbObject<Role> *, ZtString name);

  // initialize user
  void initUser(
    ZdbObject<User> *,
    UserID id, ZtString name, ZtString role, UserFlags::T flags);

  // clear all API keys for a user
  template <typename L> void keyClr__(UserID id, L l);

  // change password
  void chPass(ZmRef<Session>, ZuBytes reqBuf, ResponseFn);

  // query users
  void userGet(ZuBytes reqBuf, ResponseFn);
  // add a new user
  void userAdd(ZuBytes reqBuf, ResponseFn);
  // reset password (also clears all API keys)
  void resetPass(ZuBytes reqBuf, ResponseFn);
  // modify user (name, roles, flags)
  void userMod(ZuBytes reqBuf, ResponseFn);
  // delete user (and associated API keys)
  void userDel(ZuBytes reqBuf, ResponseFn);
  
  // query roles
  void roleGet(ZuBytes reqBuf, ResponseFn);
  // add role
  void roleAdd(ZuBytes reqBuf, ResponseFn);
  // modify role (name, perms, apiperms, flags)
  void roleMod(ZuBytes reqBuf, ResponseFn);
  // delete role
  void roleDel(ZuBytes reqBuf, ResponseFn);
  
  // query permissions 
  void permGet(ZuBytes reqBuf, ResponseFn);
  // add new permission
  void permAdd(ZuBytes reqBuf, ResponseFn);
  // modify permission (name)
  void permMod(ZuBytes reqBuf, ResponseFn);
  // delete permission
  void permDel(ZuBytes reqBuf, ResponseFn);

  // query API keys for user
  void ownKeyGet(ZmRef<Session>, ZuBytes reqBuf, ResponseFn);
  void keyGet(ZuBytes reqBuf, ResponseFn);
  void keyGet_(SeqNo, UserID, fbs::ReqAckData, ResponseFn);
  // add API key for user
  void ownKeyAdd(ZmRef<Session>, ZuBytes reqBuf, ResponseFn);
  void keyAdd(ZuBytes reqBuf, ResponseFn);
  void keyAdd_(SeqNo, UserID, fbs::ReqAckData, ResponseFn);
  // clear all API keys for user
  void ownKeyClr(ZmRef<Session>, ZuBytes reqBuf, ResponseFn);
  void keyClr(ZuBytes reqBuf, ResponseFn);
  void keyClr_(SeqNo, UserID, fbs::ReqAckData, ResponseFn);
  // delete API key
  void ownKeyDel(ZmRef<Session>, ZuBytes reqBuf, ResponseFn);
  void keyDel(ZuBytes reqBuf, ResponseFn);
  void keyDel_(SeqNo, KeyIDData, fbs::ReqAckData, ResponseFn);

private:
  Ztls::Random		*m_rng;
  unsigned		m_passLen;
  unsigned		m_totpRange;
  unsigned		m_keyInterval;
  unsigned		m_maxSize;

  ZmRef<ZdbTable<User>>	m_userTbl;
  ZmRef<ZdbTable<Role>>	m_roleTbl;
  ZmRef<ZdbTable<Key>>	m_keyTbl;
  ZmRef<ZdbTable<Perm>>	m_permTbl;

  static constexpr unsigned nPerms() {
    return unsigned(fbs::LoginReqData::MAX) + unsigned(fbs::ReqData::MAX);
  }
  static constexpr unsigned loginReqPerm(fbs::LoginReqData i) {
    return unsigned(i) - 1;
  }
  static constexpr unsigned reqPerm(fbs::ReqData i) {
    return unsigned(fbs::LoginReqData::MAX) + (unsigned(i) - 1);
  }

  UserID		m_nextUserID = 0;

  PermID		m_nextPermID = 0;
  PermID		m_perms[nPerms()];

  using State = ZuUnion<bool, Open, Bootstrap>;

  State			m_state;
};

} } // Zum::UserDB

#endif /* ZumUserDB_HH */
