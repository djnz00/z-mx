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

// directory scanning

#ifndef ZiDir_HPP
#define ZiDir_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZiLib_HPP
#include <zlib/ZiLib.hpp>
#endif

#include <zlib/ZmLock.hpp>
#include <zlib/ZmGuard.hpp>

#include <zlib/ZePlatform.hpp>

#include <zlib/ZiPlatform.hpp>

#ifndef _WIN32
#include <sys/types.h>
#include <dirent.h>
#endif

class ZiAPI ZiDir {
  ZiDir(const ZiDir &);
  ZiDir &operator =(const ZiDir &);	// prevent mis-use

public:
  using Path = Zi::Path;

  using Guard = ZmGuard<ZmLock>;

  ZiDir() :
#ifdef _WIN32
      m_handle(INVALID_HANDLE_VALUE)
#else
      m_dir(0)
#endif
  { }

  ~ZiDir() { close(); }

  bool operator !() const {
    return
#ifdef _WIN32
      m_handle == INVALID_HANDLE_VALUE;
#else
      !m_dir;
#endif
  }
  ZuOpBool

  int open(const Path &name, ZeError *e = nullptr);
  int read(Path &name, ZeError *e = nullptr);
  void close();

private:
  ZmLock	m_lock;
#ifdef _WIN32
    Path	  m_match;
    HANDLE	  m_handle;
#else
    DIR		  *m_dir;
#endif
};

#endif /* ZiDir_HPP */
