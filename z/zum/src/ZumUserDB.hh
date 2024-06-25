//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// server-side RBAC user DB with MFA, API keys, etc.

#ifndef ZvUserDB_HH
#define ZvUserDB_HH

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <zlib/ZuBitmap.hh>
#include <zlib/ZuObject.hh>
// #include <zlib/ZuPolymorph.hh>
#include <zlib/ZuRef.hh>
#include <zlib/ZuArrayN.hh>

#include <zlib/ZmHash.hh>
#include <zlib/ZmLHash.hh>
#include <zlib/ZmRBTree.hh>
#include <zlib/ZmRWLock.hh>

#include <zlib/ZtString.hh>

#include <zlib/ZiFile.hh>

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

namespace ZvUserDB {

constexpr static mbedtls_md_type_t keyType() {
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
  uint8_t		id;
  ZtString		name;

  friend ZtFieldPrint ZuPrintType(Perm *);
};
ZfbFields(Perm,
  (((id), (Keys<0>, Ctor<0>, Grouped)), (UInt8)),
  (((name), (Ctor<1>, Mutable)), (String)));

namespace RoleFlags {
  ZtEnumFlags(RoleFlags, uint8_t, Immutable);
}

struct Role {
  ZtString		name;
  uint128_t		perms;
  uint128_t		apiperms;
  uint8_t		flags;		// RoleFlags

  friend ZtFieldPrint ZuPrintType(Role *);
};
ZfbFields(Role,
  (((name), (Keys<0>, Ctor<0>)), (String)),
  (((perms), (Ctor<1>, Mutable)), (UInt128)),
  (((apiperms), (Ctor<2>, Mutable)), (UInt128)),
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
  uint8_t		flags = 0;	// UserFlags

  friend ZtFieldPrint ZuPrintType(User *);
};
ZfbFields(User,
  (((id), (Keys<0>, Ctor<0>)), (UInt64)),
  (((name), (Keys<1>, Ctor<1>, Mutable)), (String)),
  (((hmac), (Ctor<2>, Mutable)), (Bytes)),
  (((secret), (Ctor<3>, Mutable, Hidden)), (Bytes)),
  (((roles), (Ctor<4>, Mutable)), (StringVec)),
  (((flags), (Ctor<5>, Mutable, Flags<UserFlags::Map>)), (uint8_t, 0)));

struct Session_ : public ZmPolymorph {
  ZmRef<ZdbObject<User>>	user;
  unsigned			failures = 0;
  uint128_t			perms;		// effective permissions
  uint128_t			apiperms;	// effective API permissions

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

class ZvAPI Mgr {
public:
  using Lock = ZmRWLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  struct Perm {
    enum {
      Login = 0,
      Access,
      N_
    };
    enum {
      Offset = N_ - (int(fbs::ReqData::NONE) + 1)
    };
    enum {
      N = Offset + (int(fbs::ReqData::MAX) + 1)
    };
  };

  Mgr(Ztls::Random *rng, unsigned passLen, unsigned totpRange,
      unsigned keyInterval);
  ~Mgr();

  // one-time initialization (idempotent)
  // - lambda(ZtString passwd, ZtString secret)
  // import flatbuffers types
  template <typename T> using Offset = Zfb::Offset<T>;
  template <typename T> using Vector = Zfb::Vector<T>;

  // request, user, interactive
  int loginReq(const fbs::LoginReq *, ZmRef<Session> &, bool &interactive);

  Offset<fbs::ReqAck> request(Zfb::Builder &,
      Session *, bool interactive, const fbs::Request *);

private:
  // session initialization
  void initSession(ZtString userName, ZmFn<void(ZmRef<Session>)> fn);
  // FIXME - these need to be async
  // interactive login
  ZmRef<Session> login(
    int &failures, ZuString user, ZuString passwd, unsigned totp);
  // API access
  ZmRef<Session> access(
    int &failures, ZuString keyID,
    ZuArray<const uint8_t> token, int64_t stamp,
    ZuArray<const uint8_t> hmac);

public:
  // ok(user, interactive, perm)
  bool ok(Session *session, bool interactive, unsigned perm) const {
    if ((user->flags & User::ChPass) && interactive &&
	perm != m_permIndex[Perm::Offset + int(fbs::ReqData::ChPass)])
      return false;
    return interactive ? user->perms[perm] : user->apiperms[perm];
  }

private:
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

  void permAdd_() { }
  template <typename Arg0, typename ...Args>
  void permAdd_(Arg0 &&arg0, Args &&... args) {
    unsigned id = m_nPerms++;
    m_perms[id] = ZuFwd<Arg0>(arg0);
    m_permNames->add(m_perms[id], id);
    permAdd_(ZuFwd<Args>(args)...);
  }
public:
  template <typename ...Args>
  unsigned permAdd(Args &&... args) {
    Guard guard(m_lock);
    unsigned id = m_nPerms;
    permAdd_(ZuFwd<Args>(args)...);
    return id;
  }
private:
  template <typename ...Args>
  void roleAdd_(ZuString name, Role::Flags flags, Args &&... args) {
    ZmRef<Role> role = new Role(name, flags, ZuFwd<Args>(args)...);
    m_roles.addNode(role);
  }
public:
  template <typename ...Args>
  void roleAdd(Args &&... args) {
    Guard guard(m_lock);
    roleAdd_(ZuFwd<Args>(args)...);
  }
private:
  void userAdd_(
    User *user, uint64_t id, ZuString name, ZuString role, User::Flags flags,
    ZtString &passwd);
public:
  template <typename ...Args>
  ZmRef<User> userAdd(Args &&... args) {
    Guard guard(m_lock);
    return userAdd_(ZuFwd<Args>(args)...);
  }

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

  ZmRef<Sessions>	m_sessions;
};

}

#endif /* ZvUserDB_HH */
