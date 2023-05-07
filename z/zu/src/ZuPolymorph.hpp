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

// intrusively reference-counted polymorphic object (base class)

#ifndef ZuPolymorph_HPP
#define ZuPolymorph_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#include <stddef.h>

#include <zlib/ZuObjectTraits.hpp>

class ZuPolymorph {
  ZuPolymorph(const ZuPolymorph &) = delete;
  ZuPolymorph &operator =(const ZuPolymorph &) = delete;

  friend ZuPolymorph ZuObjectType(ZuPolymorph *);

public:
  ZuInline ZuPolymorph() : m_refCount(0) { }
 
  virtual ~ZuPolymorph() { }

  ZuInline void ref() const { m_refCount++; }
  ZuInline bool deref() const { return !--m_refCount; }
  ZuInline int refCount() const { return m_refCount; }

private:
  mutable int		m_refCount;
};

#endif /* ZuPolymorph_HPP */
