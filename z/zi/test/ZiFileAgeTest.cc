//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

#include <zlib/ZeLog.hh>
#include <zlib/ZiFile.hh>

int main()
{
  {
    ZiFile f;
    f.open("foo", ZiFile::Create | ZiFile::WriteOnly | ZiFile::GC, 0777);
  }
  ZiFile::age("foo", 8);
}
