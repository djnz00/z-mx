//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// universal string span

#ifndef ZuCSpan_HH
#define ZuCSpan_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuSpan.hh>

using ZuCSpan = ZuSpan<const char>;
using ZuWSpan = ZuSpan<const wchar_t>;

#endif /* ZuCSpan_HH */
