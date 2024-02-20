//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// command line interface - in-memory history

#ifndef ZrlHistory_HPP
#define ZrlHistory_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZrlLib_HPP
#include <zlib/ZrlLib.hpp>
#endif

#include <zlib/ZuString.hpp>

#include <zlib/ZtWindow.hpp>

#include <zlib/ZrlApp.hpp>

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

#endif /* ZrlHistory_HPP */
