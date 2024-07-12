//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#ifndef Zcmd_HH
#define Zcmd_HH

#ifndef ZcmdLib_HH
#include <zlib/ZcmdLib.hh>
#endif

#include <zlib/Zfb.hh>

#include <zlib/Ztls.hh>

namespace Zcmd {

using IOBuf = Ztls::IOBuf;
using IOBuilder = Zfb::IOBuilder<IOBuf>;

using SendFn = ZmFn<void(ZmRef<IOBuf>)>;

} // Zcmd

#endif /* Zcmd_HH */
