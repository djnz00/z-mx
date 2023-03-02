//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// server-side user DB with MFA, API keys, etc.

#ifndef ZvUserDB_HPP
#define ZvUserDB_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

#include <zlib/ZuBitmap.hpp>
#include <zlib/ZuObject.hpp>
// #include <zlib/ZuPolymorph.hpp>
#include <zlib/ZuRef.hpp>
#include <zlib/ZuArrayN.hpp>

#include <zlib/ZmHash.hpp>
#include <zlib/ZmLHash.hpp>
#include <zlib/ZmRBTree.hpp>
#include <zlib/ZmRWLock.hpp>

#include <zlib/ZtString.hpp>

#include <zlib/ZiFile.hpp>

#include <zlib/Zfb.hpp>

#include <zlib/ZtlsBase64.hpp>
#include <zlib/ZtlsHMAC.hpp>
#include <zlib/ZtlsRandom.hpp>

#include <zlib/ZfbField.hpp>

#include <zlib/userdb_fbs.h>
#include <zlib/loginreq_fbs.h>
#include <zlib/userdbreq_fbs.h>
#include <zlib/userdback_fbs.h>

namespace ZvUserDB {

using Bitmap = ZuBitmap<256>;

struct Role_ : public ZuObject {
public:
  using Flags = uint8_t;
  enum {
    Immutable =	0x01
  };

  Role_(ZuString name_, Flags flags_) :
      name(name_), flags(flags_) { }
  Role_(ZuString name_, Flags flags_,
	const Bitmap &perms_, const Bitmap &apiperms_) :
      name(name_), perms(perms_), apiperms(apiperms_), flags(flags_) { }

  ZtString		name;
  Bitmap		perms;
  Bitmap		apiperms;
  Flags			flags;

  Zfb::Offset<fbs::Role> save(Zfb::Builder &fbb) const {
    using namespace Zfb::Save;
    return fbs::CreateRole(fbb, str(fbb, name),
	fbb.CreateVector(perms.data, Bitmap::Words),
	fbb.CreateVector(apiperms.data, Bitmap::Words));
  }
};
inline constexpr auto RoleNameAxor() {
  return [](const Role_ &r) { return r.name; };
}
using RoleTree =
  ZmRBTree<Role_,
    ZmRBTreeNode<Role_,
      ZmRBTreeKey<RoleNameAxor(),
	ZmRBTreeUnique<true>>>>;
using Role = RoleTree::Node;
ZmRef<Role> loadRole(const fbs::Role *role_) {
  using namespace Zfb::Load;
  ZmRef<Role> role = new Role(str(role_->name()), role_->flags());
  all(role_->perms(), [role](unsigned i, uint64_t v) {
    if (i < Bitmap::Words) role->perms.data[i] = v;
  });
  all(role_->apiperms(), [role](unsigned i, uint64_t v) {
    if (i < Bitmap::Words) role->apiperms.data[i] = v;
  });
  return role;
}

enum { KeySize = Ztls::HMAC::Size<MBEDTLS_MD_SHA256>::N }; // 256 bit key
using KeyData = ZuArrayN<uint8_t, KeySize>;

struct Key_;
struct User__ : public ZuObject {
  using Flags = uint8_t;
  enum {
    Immutable =	0x01,
    Enabled =	0x02,
    ChPass =	0x04	// user must change password
  };

  User__(uint64_t id_, ZuString name_, Flags flags_) :
      id(id_), name(name_), flags(flags_) { }

  constexpr static mbedtls_md_type_t keyType() {
    return MBEDTLS_MD_SHA256;
  }

  uint64_t		id;
  unsigned		failures = 0;
  ZtString		name;
  KeyData		hmac;		// HMAC-SHA256 of secret, password
  KeyData		secret;		// secret (random at user creation)
  ZtArray<Role *>	roles;
  Key_			*keyList = nullptr; // head of list of keys
  Bitmap		perms;		// permissions
  Bitmap		apiperms;	// API permissions
  Flags			flags;

  Zfb::Offset<fbs::User> save(Zfb::Builder &fbb) const {
    using namespace Zfb::Save;
    return fbs::CreateUser(fbb, id,
	str(fbb, name), bytes(fbb, hmac), bytes(fbb, secret),
	strVecIter(fbb, roles.length(), [this](unsigned k) {
	  return roles[k]->name;
	}), flags);
  }
};
inline constexpr auto UserIDHashID() {
  return []() { return "ZvUserDB.UserIDs"; };
}
inline constexpr auto UserIDAxor() {
  return [](const User__ &u) { return u.id; };
}
using UserIDHash =
  ZmHash<User__,
    ZmHashNode<User__,
      ZmHashKey<UserIDAxor(),
	ZmHashHeapID<ZmHeapDisable(),
	  ZmHashID<UserIDHashID()>>>>>;
using User_ = UserIDHash::Node;
inline constexpr auto UserNameHashID() {
  return []() { return "ZvUserDB.UserNames"; };
}
inline constexpr auto UserNameAxor() {
  return [](const User_ &u) { return u.name; };
}
using UserNameHash =
  ZmHash<User_,
    ZmHashNode<User_,
      ZmHashKey<UserNameAxor(),
	ZmHashHeapID<UserNameHashID()>>>>;
using User = UserNameHash::Node;
template <typename Roles>
ZmRef<User> loadUser(const Roles &roles, const fbs::User *user_) {
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
ZfbFields(Key, fbs::Key,
    (((id, Rd), (0)), (String)),
    (((secret)), (Bytes), (Update)),
    (((userID, Rd)), (Int)));
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
inline constexpr auto KeyHashID() {
  return []() { return "ZvUserDB.Keys"; };
}
#if 0
inline constexpr auto KeyIDAxor() {
  return [](const Key_ &k) { return k.id; };
}
#endif
using KeyHash =
  ZmHash<Key_,
    ZmHashNode<Key_,
      ZmHashKey<ZuFieldAxor<Key_, 0>(),
	  ZmHashHeapID<KeyHashID()>>>>;
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
      N
    };
    enum {
      Offset = N - (fbs::ReqData_NONE + 1)
    };
  };

  Mgr(Ztls::Random *rng, unsigned passLen, unsigned totpRange,
      unsigned keyInterval, unsigned maxSize);
  ~Mgr();

  // one-time initialization (idempotent)
  bool bootstrap(
      ZtString user, ZtString role, ZtString &passwd, ZtString &secret);

  bool load_(const uint8_t *buf, unsigned len);
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

  Offset<fbs::ReqAck> request(
      User *, bool interactive, const fbs::Request *, Zfb::Builder &);

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
	perm != m_permIndex[Perm::Offset + fbs::ReqData_ChPass])
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
    unsigned		  m_permIndex[Perm::Offset + fbs::ReqData_MAX + 1];
    RoleTree		  m_roles; // name -> permissions
    ZmRef<UserIDHash>	  m_users;
    ZmRef<UserNameHash>	  m_userNames;
    ZmRef<KeyHash>	  m_keys;
    unsigned		  m_nPerms = 0;
    ZtString		  m_perms[Bitmap::Bits]; // indexed by permission ID
};

}

#endif /* ZvUserDB_HPP */
