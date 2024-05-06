//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

#include <zlib/ZeLog.hh>
#include <zlib/ZiFile.hh>

int main()
{
  ZeLog::init("ZiFileTest");
  ZeLog::sink(ZeLog::fileSink());
  ZeLog::start();

  ZeError e;

  try {
    {
      ZiFile f;
      if (f.open("foo", ZiFile::Create | ZiFile::Truncate, 0666, &e) != Zi::OK)
	throw e;
      ZtString hw = "Hello World\n";

      if (f.write(hw.data(), hw.length(), &e) != Zi::OK) throw e;

      ZiFile g;

      g.init(f.handle(), 0);

      printf("%d %d\n", f.blkSize(), g.blkSize());
    }

    {
      ZiFile f;
      char buf[1024];
      ZeError e;
      int i;

      if (f.open("foo", ZiFile::ReadOnly, 0777, &e) != Zi::OK) throw e;
      i = f.read(buf, 1024, &e);
      if (i < 0) throw e;
      printf("%d\n", i);
      buf[i] = 0;
      fputs(buf, stdout);
    }

    {
      ZiFile f;
      if (f.open("bar", ZiFile::Create | ZiFile::Truncate, 0666, &e) != Zi::OK)
	throw e;
      uint32_t u = 1;

      if (f.pwrite(4, &u, 4, &e) != Zi::OK) throw e;
      if (f.pread(0, &u, 4, &e) < 4) throw e;
      printf("uninitialized data: %.8x\n", (int)u); fflush(stdout);
    }
  } catch (const ZeError &e) {
    ZeLOG(Fatal, e);
    Zm::exit(1);
  }

  ZeLog::stop();
  return 0;
}
