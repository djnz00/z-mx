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

// low-level last-ditch error logging and signal trapping

#ifndef ZmTrap_HPP
#define ZmTrap_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4251)
#endif

#include <ZuString.hpp>

class ZmAPI ZmTrap {
public:
  // trap signals (call once at start of main program)

  static void trap();

  // registering signal handlers is intentionally not MT-safe

  typedef void (*Fn)();

  static void sigintFn(Fn fn);
  static Fn sigintFn();

  static void sighupFn(Fn fn);
  static Fn sighupFn();

  // last-ditch error logging

  static void log(ZuString s);
#ifdef _WIN32
  static void winProgram(ZuString s);
  static void winErrlog(int type, ZuString s);	// Windows error logging
#endif
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZmTrap_HPP */
