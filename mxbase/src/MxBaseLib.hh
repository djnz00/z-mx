//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxBase library

#ifndef MxBaseLib_HH
#define MxBaseLib_HH

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuLib.hh>

#ifdef _WIN32

#ifdef MXBASE_EXPORTS
#define MxBaseAPI ZuExport_API
#define MxBaseExplicit ZuExport_Explicit
#else
#define MxBaseAPI ZuImport_API
#define MxBaseExplicit ZuImport_Explicit
#endif
#define MxBaseExtern extern MxBaseAPI

#else /* _WIN32 */

#define MxBaseAPI
#define MxBaseExplicit
#define MxBaseExtern extern

#endif /* _WIN32 */

#endif /* MxBaseLib_HH */
