//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <iostream>

#include <zlib/ZeLog.hpp>

#include <zlib/ZiGlob.hpp>

int main(int argc, char **argv)
{
  ZiGlob g;
  for (int i = 1; i < argc; i++) {
    g.init(argv[i]);
    const auto &leafName = g.leafName();
    const auto &dirName = g.dirName();
    while (auto s = g.iterate(true, false)) {
      s.offset(leafName.length());
      std::cout << dirName << '/' << leafName << s << '\n';
    }
  }
}
