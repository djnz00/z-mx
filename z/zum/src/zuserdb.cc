//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// server-side user DB bootstrap tool

#include <iostream>

#include <zlib/ZumUserDB.hh>

using namespace ZvUserDB;

void usage()
{
  std::cerr << "usage: zuserdb FILE USER ROLE PASSLEN [PERMS...]\n";
  exit(1);
}

int main(int argc, char **argv)
{
  if (argc < 5) usage();

  int passlen = atoi(argv[4]);

  if (passlen < 6 || passlen > 60) usage();

  const char *path = argv[1];

  Ztls::Random rng;

  rng.init();

  Mgr mgr(&rng, passlen, 6, 30, 1<<20 /* 1Mb */);

  ZtString passwd, secret;

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
