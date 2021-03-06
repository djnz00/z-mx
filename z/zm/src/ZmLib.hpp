//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2

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

// Zm library main header

#ifndef ZmLib_HPP
#define ZmLib_HPP

#ifdef _MSC_VER
#pragma once
#endif

#include <zlib/ZuLib.hpp>

#ifdef _WIN32

#ifdef ZM_EXPORTS
#define ZmAPI ZuExport_API
#define ZmExplicit ZuExport_Explicit
#else
#define ZmAPI ZuImport_API
#define ZmExplicit ZuImport_Explicit
#endif
#define ZmExtern extern ZmAPI

#else

#define ZmAPI
#define ZmExplicit
#define ZmExtern extern

#endif

#if defined(ZDEBUG) && !defined(ZmObject_DEBUG)
#define ZmObject_DEBUG
#endif

#endif /* ZmLib_HPP */
