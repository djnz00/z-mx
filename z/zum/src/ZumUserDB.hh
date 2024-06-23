//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// server-side RBAC user DB with MFA, API keys, etc.

// - make ZtField definitions for user, role, perm, key

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

#include <zlib/ZtlsBase64.hh>
#include <zlib/ZtlsHMAC.hh>
#include <zlib/ZtlsRandom.hh>

#include <zlib/ZfbField.hh>

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
};
ZfbFields(Key,
  (((userID), (Keys<0>, Ctor<0>)), (UInt64)),
  (((id), (Keys<0>, Ctor<1>, Grouped)), (String)),
  (((secret), (Ctor<2>, Update)), (Bytes)));

struct Perm {
  uint8_t		id;
  ZtString		name;
};
ZfbFields(Perm,
  (((id), (Keys<0>, Ctor<0>, Grouped)), (UInt8)),
  (((name), (Ctor<1>, Update)), (String)));

namespace RoleFlags {
  ZtEnumFlags(RoleFlags, uint8_t, Immutable);
}

struct Role {
  ZtString		name;
  uint128_t		perms;
  uint128_t		apiperms;
  uint8_t		flags;		// RoleFlags
};
ZfbFields(Role,
  (((name), (Keys<0>, Ctor<0>)), (String)),
  (((perms), (Ctor<1>)), (UInt128)),
  (((apiperms), (Ctor<2>)), (UInt128)),
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

};
ZfbFields(User,
  (((id), (Keys<0>, Ctor<0>)), (UInt64)),
  (((name), (Keys<1>, Ctor<1>)), (String)),
  (((hmac), (Ctor<2>)), (Bytes)),
  (((secret), (Ctor<3>)), (Bytes)),
  (((roles), (Ctor<4>)), (StringVec)),
  (((flags), (Ctor<5>, Flags<UserFlags::Map>)), (uint8_t, 0)));

struct Session_ {
  User		user;
  unsigned	failures = 0;
  uint128_t	perms;		// effective permissions
  uint128_t	apiperms;	// effective API permissions

  static auto NameAxor(const Session &session) { return session.user.name; }
};

inline constexpr const char *Session_HeapID() { return "ZumUserDB.Session"; }
using SessionHash =
  ZmHash<Session_,
    ZmHashNode<Session_,
      ZmHashKey<Session_::NameAxor,
	ZmHashHeapID<Session_HeapID>>>>;
using Session = SessionHash::Node;

ZmRef<Session> initSession(
  using namespace Zfb::Load;
  ZmRef<User> user = new User(user_->id(), str(user_->name()), user_->flags());
  user->hmac = bytes(user_->hmac());
  user->secret = bytes(user_->secret());
  all(user_->roles(), [&roles, &user](unsigned, auto roleName) {
    if (auto role = roles.findPtr(str(roleName))) {
      user->roles.push(role);
      user->perms |= role->perms;
      user->apiperms |= role->apiperms;
    }
  });
  return user;
}

namespace _ {
struct Key {
  ZtString	id;
  KeyData	secret;
  uint64_t	userID;
};
ZfbFields(Key,
    (((id, Rd), (Keys<0>)), (String)),
    (((secret), (Update)), (Bytes)),
    (((userID, Rd)), (UInt64)));
}
using Key__ = ZfbField::Load<_::Key>;
struct Key_ : public ZuObject, public Key__ {
  Key_() = delete;
  Key_(const fbs::Key *key_, Key_ *next_) :
      Key__{key_}, next{next_} { }
  Key_(ZuString id_, uint64_t userID_, Key_ *next_) :
      Key__{id_, KeyData{}, userID_}, next{next_} { }

  using IDData = ZuArrayN<uint8_t, 16>;
  constexpr static const mbedtls_md_type_t keyType() {
    return MBEDTLS_MD_SHA256;
  }

  Key_		*next;	// next in per-user list

  Zfb::Offset<fbs::Key> save(Zfb::Builder &fbb) const {
    using namespace Zfb::Save;
    return fbs::CreateKey(fbb, str(fbb, id), bytes(fbb, secret), userID);
  }
};
inline constexpr const char *KeyHashID() { return "ZvUserDB.Keys"; }
using KeyHash =
  ZmHash<Key_,
    ZmHashNode<Key_,
      ZmHashKey<ZuFieldAxor<Key_, 0>(),
	  ZmHashHeapID<KeyHashID>>>>;
using Key = KeyHash::Node;

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
      N = Perm::Offset + (int(fbs::ReqData::MAX) + 1)
    };
  };

  Mgr(Ztls::Random *rng, unsigned passLen, unsigned totpRange,
      unsigned keyInterval, unsigned maxSize);
  ~Mgr();

  // one-time initialization (idempotent)
  bool bootstrap(
      ZtString user, ZtString role, ZtString &passwd, ZtString &secret);

  bool load_(ZuBytes);
  Zfb::Offset<fbs::UserDB> save_(Zfb::Builder &) const;

  int load(const ZiFile::Path &path, ZeError *e = 0);
  int save(const ZiFile::Path &path, unsigned maxAge = 8, ZeError *e = 0);

  bool modified() const;

  ZtString perm(unsigned i) {
    ReadGuard guard(m_lock);
    if (ZuUnlikely(i >= Bitmap::Bits)) return ZtString();
    return m_perms[i];
  }
  int findPerm(ZuString s) { // returns -1 if not found
    ReadGuard guard(m_lock);
    return findPerm_(s);
  }
private:
  enum { N = Bitmap::Bits };
  ZuAssert(N <= 1024);
  enum { PermBits =
    N <= 2 ? 1 : N <= 4 ? 2 : N <= 8 ? 3 : N <= 16 ? 4 : N <= 32 ? 5 :
    N <= 64 ? 6 : N <= 128 ? 7 : N <= 256 ? 8 : N <= 512 ? 9 : 10
  };
  using PermNames =
    ZmLHashKV<ZuString, ZuBox_1(int),
      ZmLHashStatic<PermBits,
	ZmLHashLock<ZmNoLock> > >;

  int findPerm_(ZuString s) { return m_permNames->findVal(s); }

public:
  template <typename T> using Offset = Zfb::Offset<T>;
  template <typename T> using Vector = Zfb::Vector<T>;

  // request, user, interactive
  int loginReq(const fbs::LoginReq *, ZmRef<User> &, bool &interactive);

  Offset<fbs::ReqAck> request(Zfb::Builder &,
      User *, bool interactive, const fbs::Request *);

private:
  // interactive login
  ZmRef<User> login(int &failures,
      ZuString user, ZuString passwd, unsigned totp);
  // API access
  ZmRef<User> access(int &failures,
      ZuString keyID,
      ZuArray<const uint8_t> token,
      int64_t stamp,
      ZuArray<const uint8_t> hmac);

public:
  // ok(user, interactive, perm)
  bool ok(User *user, bool interactive, unsigned perm) const {
    if ((user->flags & User::ChPass) && interactive &&
	perm != m_permIndex[Perm::Offset + int(fbs::ReqData::ChPass)])
      return false;
    return interactive ? user->perms[perm] : user->apiperms[perm];
  }

private:
  // change password
  Offset<fbs::UserAck> chPass(
      Zfb::Builder &, User *user, const fbs::UserChPass *chPass);

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
      Zfb::Builder &, const User *user, const fbs::UserID *userID);
  Offset<Vector<Offset<Zfb::String>>> keyGet(
      Zfb::Builder &, const fbs::UserID *userID);
  Offset<Vector<Offset<Zfb::String>>> keyGet_(
      Zfb::Builder &, const User *user);
  // add API key for user
  Offset<fbs::KeyUpdAck> ownKeyAdd(
      Zfb::Builder &, User *user, const fbs::UserID *userID);
  Offset<fbs::KeyUpdAck> keyAdd(
      Zfb::Builder &, const fbs::UserID *userID);
  Offset<fbs::KeyUpdAck> keyAdd_(
      Zfb::Builder &, User *user);
  // clear all API keys for user
  Offset<fbs::UserAck> ownKeyClr(
      Zfb::Builder &, User *user, const fbs::UserID *id);
  Offset<fbs::UserAck> keyClr(
      Zfb::Builder &, const fbs::UserID *id);
  Offset<fbs::UserAck> keyClr_(
      Zfb::Builder &, User *user);
  // delete API key
  Offset<fbs::UserAck> ownKeyDel(
      Zfb::Builder &, User *user, const fbs::KeyID *id);
  Offset<fbs::UserAck> keyDel(
      Zfb::Builder &, const fbs::KeyID *id);
  Offset<fbs::UserAck> keyDel_(
      Zfb::Builder &, User *user, ZuString id);

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
  ZmRef<User> userAdd_(
      uint64_t id, ZuString name, ZuString role, User::Flags flags,
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

  mutable Lock		m_lock;
    mutable bool	  m_modified = false;	// cleared by save()
    ZmRef<PermNames>	  m_permNames;
    unsigned		  m_permIndex[Perm::N];
    RoleTree		  m_roles; // name -> permissions
    ZmRef<UserIDHash>	  m_users;
    ZmRef<UserNameHash>	  m_userNames;
    ZmRef<KeyHash>	  m_keys;
    unsigned		  m_nPerms = 0;
    ZtString		  m_perms[Bitmap::Bits]; // indexed by permission ID
};

}

#endif /* ZvUserDB_HH */
