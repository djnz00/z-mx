//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Zcmd message dispatcher

#ifndef ZcmdDispatcher_HH
#define ZcmdDispatcher_HH

#ifndef ZcmdLib_HH
#include <zlib/ZcmdLib.hh>
#endif

#include <zlib/ZuSpan.hh>
#include <zlib/ZuID.hh>

#include <zlib/ZmFn.hh>
#include <zlib/ZmRef.hh>
#include <zlib/ZmPolymorph.hh>
#include <zlib/ZmNoLock.hh>
#include <zlib/ZmLHash.hh>

#include <zlib/Zcmd.hh>

class ZcmdAPI ZcmdDispatcher {
public:
  using Fn = ZmFn<int(void *, ZmRef<ZiIOBuf>)>;
  using DefltFn = ZmFn<int(void *, ZuID, ZmRef<ZiIOBuf>)>;

  void init();
  void final();

  void map(ZuID id, Fn fn);
  void deflt(DefltFn fn);

  int dispatch(ZuID id, void *link, ZmRef<ZiIOBuf>);

private:
  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;

  static const char *FnMapID() { return "ZcmdDispatcher.FnMap"; }
  using FnMap_ = ZmLHashKV<ZuID, Fn, ZmLHashID<FnMapID, ZmLHashLocal<>>>;
  struct FnMap : public FnMap_ { using FnMap_::FnMap_; };

  Lock			m_lock;
    FnMap		  m_fnMap;
    DefltFn		  m_defltFn;
};

#endif /* ZcmdDispatcher_HH */
