//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// compile-time assertion

#ifndef ZuAssert_HH
#define ZuAssert_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#ifdef _MSC_VER
#pragma once
#endif

template <bool> struct ZuAssertion_FAILED;
template <> struct ZuAssertion_FAILED<true> { };

template <int N> struct ZuAssert_TEST { };

#ifdef __GNUC__
#define ZuAssert_Unused_Typedef __attribute__((unused))
#else
#define ZuAssert_Unused_Typedef
#endif

// need to indirect via two macros to ensure expansion of __LINE__
#define ZuAssert_Typedef_(p, l) p##l
#define ZuAssert_Typedef(p, l) ZuAssert_Typedef_(p, l)

#define ZuAssert(x) typedef \
	ZuAssert_TEST<sizeof(ZuAssertion_FAILED<static_cast<bool>(x)>)> \
	ZuAssert_Typedef(ZuAssert_, __LINE__) ZuAssert_Unused_Typedef

// compile time C assert
#define ZuCAssert(x) switch (0) { case 0: case (x): ; }

#endif /* ZuAssert_HH */
