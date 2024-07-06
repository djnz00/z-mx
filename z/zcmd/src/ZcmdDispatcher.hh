//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Zcmd message dispatcher

#ifndef ZcmdDispatcher_HH
#define ZcmdDispatcher_HH

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <zlib/ZuArray.hh>
#include <zlib/ZuID.hh>

#include <zlib/ZmFn.hh>
#include <zlib/ZmRef.hh>
#include <zlib/ZmPolymorph.hh>
#include <zlib/ZmNoLock.hh>
#include <zlib/ZmLHash.hh>

class ZvAPI ZcmdDispatcher {
public:
  using Fn = ZmFn<void(void *, const uint8_t *, unsigned)>;
  using DefltFn = ZmFn<void(void *, ZuID, const uint8_t *, unsigned)>;

  void init();
  void final();

  void map(ZuID id, Fn fn);
  void deflt(DefltFn fn);

  int dispatch(ZuID id, void *link, const uint8_t *data, unsigned len);

private:
  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;

  static const char *FnMapID() { return "ZcmdDispatcher.FnMap"; }
  using FnMap = ZmLHashKV<ZuID, Fn, ZmLHashID<FnMapID, ZmLHashLocal<>>>;

  Lock			m_lock;
    FnMap		  m_fnMap;
    DefltFn		  m_defltFn;
};

#endif /* ZcmdDispatcher_HH */
