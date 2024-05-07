//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxMD library

#ifndef MxMDLib_HH
#define MxMDLib_HH

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuLib.hh>

#ifdef _WIN32

#ifdef MXMD_EXPORTS
#define MxMDAPI ZuExport_API
#define MxMDExplicit ZuExport_Explicit
#else
#define MxMDAPI ZuImport_API
#define MxMDExplicit ZuImport_Explicit
#endif
#define MxMDExtern extern MxMDAPI

#else /* _WIN32 */

#define MxMDAPI
#define MxMDExplicit
#define MxMDExtern extern

#endif /* _WIN32 */

#endif /* MxMDLib_HH */
