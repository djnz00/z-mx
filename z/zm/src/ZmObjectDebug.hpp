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

// intrusively reference-counted object debugging

#ifndef ZmObjectDebug_HPP
#define ZmObjectDebug_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#ifdef ZmObject_DEBUG

#include <stddef.h>
#include <stdlib.h>

#include <zlib/ZmAtomic.hpp>

#include <zlib/ZmBackTrace_.hpp>

class ZmObjectDebug;

extern "C" {
  ZmExtern void ZmObject_ref(const ZmObjectDebug *, const void *);
  ZmExtern void ZmObject_deref(const ZmObjectDebug *, const void *);
}

class ZmAPI ZmObjectDebug {
friend ZmAPI void ZmObject_ref(const ZmObjectDebug *, const void *);
friend ZmAPI void ZmObject_deref(const ZmObjectDebug *, const void *);

public:
  ZmObjectDebug() : m_debug(0) { }
  ~ZmObjectDebug() { ::free(m_debug); }

  void debug() const;

  // context, referrer, backtrace
  typedef void (*DumpFn)(void *, const void *, const ZmBackTrace *);
  void dump(void *context, DumpFn fn) const;

protected:
  bool debugging_() const { return m_debug.load_(); }

  mutable ZmAtomic<void *>	m_debug;
};

#endif /* ZmObject_DEBUG */

#endif /* ZmObjectDebug_HPP */
