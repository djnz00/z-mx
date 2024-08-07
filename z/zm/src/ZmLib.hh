//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Z Multithreading library main header

#ifndef ZmLib_HH
#define ZmLib_HH

#include <zlib/ZuLib.hh>

#ifdef _WIN32

#ifdef ZM_EXPORTS
#define ZmAPI ZuExport_API
#define ZmExplicit ZuExport_Explicit
#else
#define ZmAPI ZuImport_API
#define ZmExplicit ZuImport_Explicit
#endif
#define ZmExtern extern ZmAPI

#else

#define ZmAPI
#define ZmExplicit
#define ZmExtern extern

#endif

#if defined(ZDEBUG) && !defined(ZmObject_DEBUG)
#define ZmObject_DEBUG
#endif

#endif /* ZmLib_HH */
