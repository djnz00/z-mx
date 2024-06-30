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

struct Key {
  uint64_t		userID;
  ZuStringN<16>		id;
  KeyData		secret;

  friend ZtFieldPrint ZuPrintType(Key *);
};
ZfbFields(Key,
  (((userID), (Keys<0>, Ctor<0>)), (UInt64)),
  (((id), (Keys<0>, Ctor<1>, Grouped)), (String)),
  (((secret), (Ctor<2>, Mutable, Hidden)), (Bytes)));

struct Perm {
  uint32_t		id;
  ZtString		name;

  friend ZtFieldPrint ZuPrintType(Perm *);
};
ZfbFields(Perm,
  (((id), (Keys<0>, Ctor<0>, Grouped)), (UInt32)),
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
  uint64_t		id;
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
  Mgr				*mgr = nullptr;
  ZmRef<ZdbObject<User>>	user;
  unsigned			failures = 0;
  ZtBitmap			perms;		// effective permissions
  bool				interactive;

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
using SessionFn = ZmFn<void(ZmRef<Session>)>;

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
    uint64_t id, ZtString name, ZtString role, UserFlags::T flags);

  // start new session
  struct SessionStart {
    ZtString		userName;
    bool		interactive;
    SessionFn		fn;
    ZmRef<Session>	session;
    unsigned		roleIndex = 0;
  };
  void sessionStart(ZtString userName, SessionFn);
  void sessionStart_findUser(ZuPtr<SessionStart> context);
  void sessionStart_findRole(ZuPtr<SessionStart> context);
  void sessionStarted(ZuPtr<SessionStart> context, bool ok);

  // interactive login
  void login(
    ZuString user, ZuString passwd, unsigned totp,
    SessionFn);
  // API access
  void access(
    ZuString keyID,
    ZuArray<const uint8_t> token, int64_t stamp, ZuArray<const uint8_t> hmac,
    SessionFn);

public:
  // ok(user, interactive, perm)
  bool ok(Session *session, unsigned perm) const {
    if ((session->user->data().flags & User::ChPass) &&
	session->interactive &&
	perm != Perm::Offset + int(fbs::ReqData::ChPass))
      return false;
    return session->perms & (uint128_t(1)<<perm);
  }

private:
  // FIXME from here
  // change password
  Offset<fbs::UserAck> chPass(
      Zfb::Builder &, Session *session, const fbs::UserChPass *chPass);

  // query users
  Offset<Vector<Offset<fbs::User>>> userGet(
      Zfb::Builder &, const fbs::UserID *id);
  // add a new user
  Offset<fbs::UserPass> userAdd(
      Zfb::Builder &, const fbs::User *user);
  // reset password (also clears all API keys)
  Offset<fbs::UserPass> resetPass(
      Zfb::Builder &, const fbs::UserID *id);
  // modify user name, roles, flags
  Offset<fbs::UserUpdAck> userMod(
      Zfb::Builder &, const fbs::User *user);
  // delete user
  Offset<fbs::UserUpdAck> userDel(
      Zfb::Builder &, const fbs::UserID *id);
  
  // query roles
  Offset<Vector<Offset<fbs::Role>>> roleGet(
      Zfb::Builder &, const fbs::RoleID *id);
  // add role
  Offset<fbs::RoleUpdAck> roleAdd(
      Zfb::Builder &, const fbs::Role *role);
  // modify role perms, apiperms, flags
  Offset<fbs::RoleUpdAck> roleMod(
      Zfb::Builder &, const fbs::Role *role);
  // delete role
  Offset<fbs::RoleUpdAck> roleDel(
      Zfb::Builder &, const fbs::RoleID *role);
  
  // query permissions 
  Offset<Vector<Offset<fbs::Perm>>> permGet(
      Zfb::Builder &, const fbs::PermID *perm);
  // add permission
  Offset<fbs::PermUpdAck> permAdd(
      Zfb::Builder &, const fbs::PermAdd *permAdd);
  // modify permission name
  Offset<fbs::PermUpdAck> permMod(
      Zfb::Builder &, const fbs::Perm *perm);
  // delete permission
  Offset<fbs::PermUpdAck> permDel(
      Zfb::Builder &, const fbs::PermID *id);

  // query API keys for user
  Offset<Vector<Offset<Zfb::String>>> ownKeyGet(
      Zfb::Builder &, const Session *session, const fbs::UserID *userID);
  Offset<Vector<Offset<Zfb::String>>> keyGet(
      Zfb::Builder &, const fbs::UserID *userID);
  Offset<Vector<Offset<Zfb::String>>> keyGet_(
      Zfb::Builder &, ZmRef<ZdbObject<User>> user);
  // add API key for user
  Offset<fbs::KeyUpdAck> ownKeyAdd(
      Zfb::Builder &, const Session *session, const fbs::UserID *userID);
  Offset<fbs::KeyUpdAck> keyAdd(
      Zfb::Builder &, const fbs::UserID *userID);
  Offset<fbs::KeyUpdAck> keyAdd_(
      Zfb::Builder &, ZmRef<ZdbObject<User>> user);
  // clear all API keys for user
  Offset<fbs::UserAck> ownKeyClr(
      Zfb::Builder &, const Session *session, const fbs::UserID *id);
  Offset<fbs::UserAck> keyClr(
      Zfb::Builder &, const fbs::UserID *id);
  Offset<fbs::UserAck> keyClr_(
      Zfb::Builder &, ZmRef<ZdbObject<User>> user);
  // delete API key
  Offset<fbs::UserAck> ownKeyDel(
      Zfb::Builder &, const Session *session, const fbs::KeyID *id);
  Offset<fbs::UserAck> keyDel(
      Zfb::Builder &, const fbs::KeyID *id);
  Offset<fbs::UserAck> keyDel_(
      Zfb::Builder &, ZmRef<ZdbObject<User>> user, ZuString id);

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
  static constexpr unsigned loginReqPerm(unsigned i) { return i - 1; }
  static constexpr unsigned reqPerm(unsigned i) {
    return unsigned(fbs::LoginReqData::MAX) + (i - 1);
  }

  uint32_t		m_nextPermID = 0;
  uint32_t		m_perms[nPerms()];

  using Context = ZuUnion<void, Open, Bootstrap>;

  Context		m_context;

  ZmRef<Sessions>	m_sessions;
};

} } // Zum::UserDB

#endif /* ZumUserDB_HH */
