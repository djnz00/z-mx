// -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
// vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i4

// MxT Library

#ifndef MxTLib_HH
#define MxTLib_HH

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuLib.hh>

#ifdef _WIN32

#ifdef MXTLIB_EXPORTS
#define MxTLibAPI ZuExport_API
#define MxTLibExplicit ZuExport_Explicit
#else
#define MxTLibAPI ZuImport_API
#define MxTLibExplicit ZuImport_Explicit
#endif
#define MxTLibExtern extern MxTLibAPI

#else

#define MxTLibAPI
#define MxTLibExplicit
#define MxTLibExtern extern

#endif

#ifndef MxNLegs
#define MxNLegs 1
#endif

#endif /* MxTLib_HH */
