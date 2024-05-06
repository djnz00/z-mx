//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// concrete generic sequence number (uint64)

#ifndef ZvSeqNo_HH
#define ZvSeqNo_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <zlib/ZuInt.hh>
#include <zlib/ZuBox.hh>

using ZvSeqNo = ZuBox<uint64_t>;

#endif /* ZvSeqNo_HH */
