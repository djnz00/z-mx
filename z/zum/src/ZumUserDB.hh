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

#include <zlib/zum_userdb_fbs.h>
#include <zlib/zum_loginreq_fbs.h>
#include <zlib/zum_userdbreq_fbs.h>
#include <zlib/zum_userdback_fbs.h>

namespace Zum { namespace UserDB {

static constexpr mbedtls_md_type_t keyType() {
  return MBEDTLS_MD_SHA256;
}
enum { KeySize = Ztls::HMAC::Size<MBEDTLS_MD_SHA256>::N }; // 256 bit key
using KeyData = ZuArrayN<uint8_t, KeySize>;
enum { KeyIDSize = 16 };
using KeyIDData = ZuArrayN<uint8_t, KeyIDSize>;

using UserID = uint64_t;

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
  uint32_t		id;
  ZtString		name;

  friend ZtFieldPrint ZuPrintType(Perm *);
};
ZfbFields(Perm,
  (((id), (Keys<0>, Ctor<0>)), (UInt32)),
  (((name), (Ctor<1>, Mutable)), (String)));

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
  KeyData		hmac;
  KeyData		secret;
  ZtArray<ZtString>	roles;
  uint32_t		failures = 0;
  uint8_t		flags = 0;	// UserFlags

  friend ZtFieldPrint ZuPrintType(User *);
};
ZfbFields(User,
  (((id), (Keys<0>, Ctor<0>)), (UInt64)),
  (((name), (Keys<1>, Ctor<1>, Mutable)), (String)),
  (((hmac), (Ctor<2>, Mutable)), (Bytes)),
  (((secret), (Ctor<3>, Mutable, Hidden)), (Bytes)),
  (((roles), (Ctor<4>, Mutable)), (StringVec)),
  (((failures), (Ctor<5>, Mutable)), (UInt32, 0)),
  (((flags), (Ctor<6>, Mutable, Flags<UserFlags::Map>)), (UInt8, 0)));

struct Session_ : public ZmPolymorph {
  Mgr			*mgr = nullptr;
  ZdbObjRef<User>	user;
  ZdbObjRef<Key>	key;		// if API key access
  ZtBitmap		perms;		// effective permissions
  bool			interactive;

  static auto NameAxor(const Session &session) {
    return session.user->data().name;
  }
};

inline constexpr const char *Session_HeapID() { return "ZumUserDB.Session"; }
using Sessions =
  ZmHash<Session_,
    ZmHashNode<Session_,
      ZmHashKey<Session_::NameAxor,
	ZmHashHeapID<Session_HeapID>>>>;
using Session = Sessions::Node;

// session start callback - nullptr if login/access failed
using SessionFn = ZmFn<void(ZmRef<Session>)>;

// request response callback
using ResponseFn = ZmFn<ZmFn<void(Zfb::Builder &)>>;

class ZvAPI Mgr {
public:
  Mgr(Ztls::Random *rng, unsigned passLen, unsigned totpRange,
      unsigned keyInterval);
  ~Mgr();

  void init(Zdb *db);
  void final();

  // open
  using OpenFn = ZmFn<void(bool)>;
  void open(OpenFn fn);
private:
  struct Open {	// open() context
    OpenFn	fn;
    unsigned	permID = 0;
  };
  void open_recoverNextPermID();
  void open_findPerm();
  void opened(bool ok);
public:

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
  void bootstrap(BootstrapFn fn);
private:
  struct Bootstrap { // bootstrap() context
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

  // one-time initialization (idempotent)
  // - lambda(ZtString passwd, ZtString secret)
  // import flatbuffers types
  template <typename T> using Offset = Zfb::Offset<T>;
  template <typename T> using Vector = Zfb::Vector<T>;

  void loginReq(const fbs::LoginReq *, SessionFn);

  void request(
    Session *, const fbs::Request *, ZmFn<void(ZmRef<ZiAnyIOBuf>)>);

private:
  // initialize permission
  void initPerm(ZdbObject<Perm> *, unsigned i);

  // initialize role
  void initRole(ZdbObject<Role> *, ZtString name);

  // initialize user
  void initUser(
    ZdbObject<User> *,
    UserID id, ZtString name, ZtString role, UserFlags::T flags);

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

  void reject(
    uint64_t seqNo, unsigned rejCode, ZuString text, ResponseFn fn);
  void respond(
    Zfb::Builder &fbb, uint64_t seqNo,
    fbs::ReqAckData ackType, Offset<void> ackData);

public:
  // ok(user, interactive, perm)
  bool ok(Session *session, unsigned permID) const {
    if ((session->user->data().flags & UserFlags::ChPass()) &&
	!session->key && perm != reqPerm(fbs::ReqData::ChPass))
      return false;
    return session->perms[permID];
  }

private:
  // change password
  void chPass(
    Session *session, uint64_t seqNo,
    const fbs::UserChPass *chPass, ResponseFn);

  // query users
  void userGet(
    Session *session, uint64_t seqNo, const fbs::UserID *id, ResponseFn);
  // add a new user
  void userAdd(
    Session *session, uint64_t seqNo, const fbs::User *user, ResponseFn);
  // reset password (also clears all API keys)
  void resetPass(
    Session *session, uint64_t seqNo, const fbs::UserID *id, ResponseFn);
  // modify user name, roles, flags
  void userMod(
    Session *session, uint64_t seqNo, const fbs::User *user, ResponseFn);
  // delete user
  void userDel(
    Session *session, uint64_t seqNo, const fbs::UserID *id, ResponseFn);
  
  // query roles
  void roleGet(
    Session *session, uint64_t seqNo, const fbs::RoleID *id, ResponseFn);
  // add role
  void roleAdd(
    Session *session, uint64_t seqNo, const fbs::Role *role, ResponseFn);
  // modify role perms, apiperms, flags
  void roleMod(
    Session *session, uint64_t seqNo, const fbs::Role *role, ResponseFn);
  // delete role
  void roleDel(
    Session *session, uint64_t seqNo, const fbs::RoleID *role, ResponseFn);
  
  // query permissions 
  void permGet(
    Session *session, uint64_t seqNo, const fbs::PermID *perm, ResponseFn);
  // add permission
  void permAdd(
    Session *session, uint64_t seqNo, const fbs::PermAdd *permAdd, ResponseFn);
  // modify permission name
  void permMod(
    Session *session, uint64_t seqNo, const fbs::Perm *perm, ResponseFn);
  // delete permission
  void permDel(
    Session *session, uint64_t seqNo, const fbs::PermID *id, ResponseFn);

  // query API keys for user
  void ownKeyGet(
      Session *session, uint64_t seqNo, const fbs::UserID *userID, ResponseFn);
  void keyGet(
      Session *session, uint64_t seqNo, const fbs::UserID *userID, ResponseFn);
  void keyGet_(
      Session *session, uint64_t seqNo, ZdbObjRef<User> user, ResponseFn);
  // add API key for user
  void ownKeyAdd(
      Session *session, uint64_t seqNo, const fbs::UserID *userID, ResponseFn);
  void keyAdd(
      Session *session, uint64_t seqNo, const fbs::UserID *userID, ResponseFn);
  void keyAdd_(
      Session *session, uint64_t seqNo, ZdbObjRef<User> user, ResponseFn);
  // clear all API keys for user
  void ownKeyClr(
      Session *session, uint64_t seqNo, const fbs::UserID *id, ResponseFn);
  void keyClr(
      Session *session, uint64_t seqNo, const fbs::UserID *id, ResponseFn);
  void keyClr_(
      Session *session, uint64_t seqNo, ZdbObjRef<User> user, ResponseFn);
  // delete API key
  void ownKeyDel(
      Session *session, uint64_t seqNo, const fbs::KeyID *id, ResponseFn);
  void keyDel(
      Session *session, uint64_t seqNo, const fbs::KeyID *id, ResponseFn);
  void keyDel_(
      Session *session, uint64_t seqNo,
      ZdbObjRef<User> user, ZuString id, ResponseFn);

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

  uint32_t		m_nextPermID = 0;
  uint32_t		m_perms[nPerms()];

  using State = ZuUnion<bool, Open, Bootstrap>;

  State			m_state;

  ZmRef<Sessions>	m_sessions;
};

} } // Zum::UserDB

#endif /* ZumUserDB_HH */
