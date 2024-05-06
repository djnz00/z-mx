//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// command line interface - in-memory history

#ifndef ZrlHistory_HH
#define ZrlHistory_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZrlLib_HH
#include <zlib/ZrlLib.hh>
#endif

#include <zlib/ZuString.hh>

#include <zlib/ZtWindow.hh>

#include <zlib/ZrlApp.hh>

namespace Zrl {

// in-memory history with maximum number of entries
struct ZrlAPI History : public ZtWindow<ZtArray<const uint8_t>> {
  using Base = ZtWindow<ZtArray<const uint8_t>>;

public:
  History() = default;
  History(const History &) = default;
  History &operator =(const History &) = default;
  History(History &&) = default;
  History &operator =(History &&) = default;

  History(unsigned max) : Base{max} { }

  void save(unsigned i, ZuArray<const uint8_t> s);
  bool load(unsigned i, HistFn) const;

  auto saveFn() { return HistSaveFn::Member<&History::save>::fn(this); }
  auto loadFn() { return HistLoadFn::Member<&History::load>::fn(this); }
};

} // Zrl

#endif /* ZrlHistory_HH */
