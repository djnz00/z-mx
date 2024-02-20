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

// command line interface

#ifndef ZrlGlobber_HPP
#define ZrlGlobber_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZrlLib_HPP
#include <zlib/ZrlLib.hpp>
#endif

#include <zlib/ZuString.hpp>
#include <zlib/ZuUTF.hpp>

#include <zlib/ZiGlob.hpp>

#include <zlib/ZrlApp.hpp>

namespace Zrl {

// filename glob completion
class ZrlAPI Globber {
  struct QState {	// quoting state
    enum {
      WhiteSpace	= 0x000,
      Unquoted		= 0x001,
      SglQuoted		= 0x002,
      DblQuoted		= 0x003,
      Mask		= 0x003,
      BackQuote		= 0x004		// flag - implies !WhiteSpace
    };
  };
  struct QMode {	// quoting mode
    enum {
      Unset		= 0x000,
      BackQuote		= 0x001,
      SglQuote		= 0x002,
      DblQuote		= 0x003,
      Mask		= 0x003,
      Extant		= 0x004		// flag - implies single/double
    };
  };

private:
  constexpr static bool isspace__(char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
  }
  typedef bool (*QuoteFn)(uint32_t c);
  QuoteFn quoteFn();

public:
  void init(
      ZuArray<const uint8_t> data,	// line data
      unsigned cursor,			// cursor offset (in bytes)
      CompSpliceFn splice);		// line splice callback function
  void final();

  void start();

  bool subst(CompSpliceFn splice, bool next);

  bool next(CompIterFn iter);

  auto initFn() { return CompInitFn::Member<&Globber::init>::fn(this); }
  auto finalFn() { return CompFinalFn::Member<&Globber::final>::fn(this); }
  auto startFn() { return CompStartFn::Member<&Globber::start>::fn(this); }
  auto substFn() { return CompSubstFn::Member<&Globber::subst>::fn(this); }
  auto nextFn() { return CompNextFn::Member<&Globber::next>::fn(this); }

private:
  bool		m_appendSpace = false;	// append space to leafname?
  int		m_qmode = QMode::Unset;	// quoting mode for leafname
  unsigned	m_loff = 0;		// current leafname offset in line
  ZuUTFSpan	m_lspan;		// current leafname span in line
  ZiGlob	m_glob;			// filesystem globber
};

} // Zrl

#endif /* ZrlGlobber_HPP */
