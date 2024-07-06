//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// server-side user DB bootstrap tool

#include <iostream>

#include <zlib/ZumServer.hh>

using namespace Zum::Server;

void usage()
{
  std::cerr << "usage: zuserdb USER ROLE PASSLEN [PERMS...]\n";
  exit(1);
}

int main(int argc, char **argv)
{
  static ZvOpt opts[] = { { 0 } };

  ZmRef<ZvCf> cf;

  // FIXME - figure out how to configure Zdb more easily
  try {
    cf = inlineCf(
      "thread zdb\n"
      "hostID 0\n"
      "hosts {\n"
      "  0 { priority 100 ip 127.0.0.1 port 9943 }\n"
      "}\n"
      "store {\n"
      "  module ${ZDB_STORE}\n"
      "  connection ${ZDB_CXN}\n"
      "  thread zdb_store\n"
      "  replicated true\n"
      "}\n"
      "tables {\n"
      "  zum.user { }\n"
      "  zum.role { }\n"
      "  zum.key { }\n"
      "  zum.perm { }\n"
      "}\n"
      "debug 1\n"
      "dbMx {\n"
      "  nThreads 4\n"
      "  threads {\n"
      "    1 { name rx isolated true }\n"
      "    2 { name tx isolated true }\n"
      "    3 { name zdb isolated true }\n"
      "    4 { name zdb_store isolated true }\n"
      "  }\n"
      "  rxThread rx\n"
      "  txThread tx\n"
      "}\n"
    );

    if (cf->fromArgs(opts, argc, argv) != 1) usage();

  } catch (const ZvError &e) {
    std::cerr << e << '\n' << std::flush;
    usage();
  } catch (const ZeError &e) {
    std::cerr << e << '\n' << std::flush;
    usage();
  } catch (...) {
    usage();
  }

  if (argc < 4) usage();

  int passlen = atoi(argv[3]);

  if (passlen < 6 || passlen > 60) usage();

  Ztls::Random rng;

  rng.init();

  Mgr mgr(&rng, passlen, 6, 30);

  ZtString passwd, secret;

  // FIXME from here
  mgr.bootstrap(argv[2], argv[3], passwd, secret);

  for (unsigned i = 5; i < (unsigned)argc; i++) mgr.permAdd(argv[i]);

  ZeError e;
  if (mgr.save(path, 0, &e) != Zi::OK) {
    std::cerr << path << ": " << e;
    return 1;
  }

  std::cout << "passwd: " << passwd << "\nsecret: " << secret << '\n';
  return 0;
}
