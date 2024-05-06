//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// fastbuffers file I/O

#include <zlib/Zfb.hh>

#include <zlib/ZiFile.hh>

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
