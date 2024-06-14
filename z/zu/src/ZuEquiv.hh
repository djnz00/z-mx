//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// type equivalence, using ZuNormChar for character types

#ifndef ZuEquiv_HH
#define ZuEquiv_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuNormChar.hh>

template <typename U1, typename U2>
struct ZuEquiv : public ZuIsExact<ZuNormChar<U1>, ZuNormChar<U2>> { };

#endif /* ZuEquiv_HH */
