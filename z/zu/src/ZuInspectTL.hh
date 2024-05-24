//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// type list inspection for convertibility, constructibility

#ifndef ZuInspectTL_HH
#define ZuInspectTL_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuInspect.hh>
#include <zlib/ZuTL.hh>

// type list convertibility
template <typename Ts, typename Us> // true if Ts converts to Us
struct ZuTLConverts : public ZuFalse { };
template <typename, typename, unsigned, bool>
struct ZuTLConverts_ : public ZuFalse { };
template <>
struct ZuTLConverts_<ZuTypeList<>, ZuTypeList<>, 0, true> : public ZuTrue { };
template <typename T0, typename ...Ts, typename U0, typename ...Us, unsigned N>
struct ZuTLConverts_<ZuTypeList<T0, Ts...>, ZuTypeList<U0, Us...>, N, true> :
  public ZuTLConverts_<
    ZuTypeList<Ts...>, ZuTypeList<Us...>, N - 1,
    ZuInspect<T0, U0>::Converts> { };
template <typename ...Ts, typename ...Us>
struct ZuTLConverts<ZuTypeList<Ts...>, ZuTypeList<Us...>> :
  public ZuTLConverts_<
    ZuTypeList<Ts...>, ZuTypeList<Us...>,
    sizeof...(Us), sizeof...(Ts) == sizeof...(Us)> { };

// type list constructibility
template <typename Ts, typename Us> // true if Us can be constructed from Ts
struct ZuTLConstructs : public ZuFalse { };
template <typename, typename, unsigned, bool>
struct ZuTLConstructs_ : public ZuFalse { };
template <>
struct ZuTLConstructs_<ZuTypeList<>, ZuTypeList<>, 0, true> : public ZuTrue { };
template <typename T0, typename ...Ts, typename U0, typename ...Us, unsigned N>
struct ZuTLConstructs_<ZuTypeList<T0, Ts...>, ZuTypeList<U0, Us...>, N, true> :
  public ZuTLConstructs_<
    ZuTypeList<Ts...>, ZuTypeList<Us...>, N - 1,
    ZuInspect<T0, U0>::Constructs> { };
template <typename ...Ts, typename ...Us>
struct ZuTLConstructs<ZuTypeList<Ts...>, ZuTypeList<Us...>> :
  public ZuTLConstructs_<
    ZuTypeList<Ts...>, ZuTypeList<Us...>,
    sizeof...(Us), sizeof...(Ts) == sizeof...(Us)> { };

#endif /* ZuInspectTL_HH */
