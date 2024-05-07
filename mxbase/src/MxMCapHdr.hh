//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Multicast Packet Capture Header

#ifndef MxMCapHdr_HH
#define MxMCapHdr_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxMDLib_HH
#include <mxbase/MxBaseLib.hh>
#endif

#pragma pack(push, 1)

// multicast capture header
struct MxMCapHdr {
  uint16_t	len = 0;	// exclusive of MxMCapHdr
  uint16_t	group = 0;
  int64_t	sec = 0;
  uint32_t	nsec = 0;
};

#pragma pack(pop)

#endif /* MxMCapHdr_HH */
