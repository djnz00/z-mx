//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// user DB

#ifndef Zum_HH
#define Zum_HH

#ifndef ZumLib_HH
#include <zlib/ZumLib.hh>
#endif

#include <zlib/ZuArrayN.hh>

#include <zlib/ZtString.hh>

#include <zlib/Zfb.hh>
#include <zlib/ZfbField.hh>

#include <zlib/Ztls.hh>
#include <zlib/ZtlsHMAC.hh>
#include <zlib/ZtlsRandom.hh>

#include <zlib/zum_key_fbs.h>
#include <zlib/zum_perm_fbs.h>
#include <zlib/zum_role_fbs.h>
#include <zlib/zum_user_fbs.h>

#include <zlib/zum_loginreq_fbs.h>
#include <zlib/zum_loginack_fbs.h>
#include <zlib/zum_request_fbs.h>
#include <zlib/zum_reqack_fbs.h>

namespace Zum {

enum { IOBufSize = 512 }; // built-in buffer size

using IOBufAlloc = ZiIOBufAlloc<IOBufSize>;

struct IOBuilder : public Zfb::IOBuilder {
  using Zfb::IOBuilder::IOBuilder;
  using Zfb::IOBuilder::operator =;
  IOBuilder() : Zfb::IOBuilder{new IOBufAlloc{}} { }
};

using SeqNo = uint64_t;

static constexpr mbedtls_md_type_t keyType() { return MBEDTLS_MD_SHA256; }
enum { KeySize = Ztls::HMAC::Size<keyType()>::N }; // 256 bit key
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
  (((id), ((Keys<0, 1>), Ctor<1>)), (Bytes)),
  (((secret), (Ctor<2>, Mutable, Hidden)), (Bytes)));

ZfbRoot(Key);

struct Perm {
  PermID		id;
  ZtString		name;

  friend ZtFieldPrint ZuPrintType(Perm *);
};
ZfbFields(Perm,
  (((id), (Keys<0>, Ctor<0>, Descend)), (UInt32)),
  (((name), (Keys<1>, Ctor<1>, Mutable)), (String)));

ZfbRoot(Perm);

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
  (((perms), (Ctor<1>, Mutable)), (Bitmap)),
  (((apiperms), (Ctor<2>, Mutable)), (Bitmap)),
  (((flags), (Ctor<3>, Flags<RoleFlags::Map>, Mutable)), (UInt8)));

ZfbRoot(Role);

namespace UserFlags {
  ZtEnumFlags(UserFlags, uint8_t,
    Immutable,
    Enabled,
    SuperUser,
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
  (((id), (Keys<0>, Ctor<0>, Descend)), (UInt64)),
  (((name), (Keys<1>, Ctor<1>, Mutable)), (String)),
  (((secret), (Ctor<2>, Mutable, Hidden)), (Bytes)),
  (((hmac), (Ctor<3>, Mutable)), (Bytes)),
  (((roles), (Ctor<4>, Mutable)), (StringVec)),
  (((failures), (Ctor<5>, Mutable)), (UInt32, 0)),
  (((flags), (Ctor<6>, Mutable, Flags<UserFlags::Map>)), (UInt8, 0)));

ZfbRoot(User);

} // Zum

#endif /* Zum_HH */
