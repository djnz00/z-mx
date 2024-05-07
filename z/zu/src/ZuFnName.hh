//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// function name macro

#ifndef ZuFnName_HH
#define ZuFnName_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#define ZuFnName nullptr

#ifdef __GNUC__
#undef ZuFnName
#define ZuFnName __PRETTY_FUNCTION__
#endif

#ifdef _MSC_VER
#undef ZuFnName
#define ZuFnName __FUNCSIG__
#endif

#endif /* ZuFnName_HH */
