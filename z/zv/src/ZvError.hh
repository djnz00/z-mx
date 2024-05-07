//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// generic Zv error exception

#ifndef ZvError_HH
#define ZvError_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <zlib/ZuPrint.hh>

#include <zlib/ZtString.hh>

class ZvAPI ZvError {
public:
  virtual ~ZvError() { }
  virtual void print_(ZmStream &) const = 0;
  template <typename S>
  void print(S &s_) const { ZmStream s{s_}; print_(s); }
  void print(ZmStream &s) const { print_(s); }
  friend ZuPrintFn ZuPrintType(ZvError *);
};

#endif /* ZvError_HH */
