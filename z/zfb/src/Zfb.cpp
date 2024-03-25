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

// fastbuffers file I/O

#include <zlib/Zfb.hpp>

#include <zlib/ZiFile.hpp>

ZfbExtern int Zfb::Save::save(
    const Zi::Path &path, Builder &fbb, unsigned mode, ZeError *e)
{
  ZiFile f;
  int i;

  if ((i = f.open(path,
	  ZiFile::Create | ZiFile::WriteOnly | ZiFile::GC, mode, e)) != Zi::OK)
    return i;

  const uint8_t *data = fbb.GetBufferPointer();
  int len = fbb.GetSize();

  if (!data || len <= 0) {
    if (e) *e = ZiENOMEM;
    return Zi::IOError;
  }
  return f.write(data, len, e);
}

ZfbExtern int Zfb::Load::load(
    const Zi::Path &path,
    Zfb::Load::LoadFn fn, unsigned maxSize, ZeError *e)
{
  ZiFile f;
  int i;

  if ((i = f.open(path, ZiFile::ReadOnly, 0, e)) != Zi::OK) return i;
  ZiFile::Offset len = f.size();
  if (!len || len >= (ZiFile::Offset)maxSize) {
    f.close();
    if (e) *e = ZiENOMEM;
    return Zi::IOError;
  }
  uint8_t *data = (uint8_t *)::malloc(len);
  if (!data) {
    f.close();
    if (e) *e = ZiENOMEM;
    return Zi::IOError;
  }
  if ((i = f.read(data, len, e)) < len) {
    ::free(data);
    f.close();
    return Zi::IOError;
  }
  f.close();
  if (!fn(ZuBytes{data, static_cast<unsigned>(len)})) {
    ::free(data);
    if (e) *e = ZiEINVAL;
    return Zi::IOError;
  }
  ::free(data);
  return Zi::OK;
}
