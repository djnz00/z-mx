//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

/* test program */

#include <new>

#include <stdio.h>
#include <stdlib.h>

#ifndef _WIN32
#include <alloca.h>
#endif

#include <vector>
#include <list>

#include <zlib/ZmTime.hh>
#include <zlib/ZmHeap.hh>
#include <zlib/ZmAllocator.hh>
#include <zlib/ZmThread.hh>
#include <zlib/ZmSemaphore.hh>
#include <zlib/ZmFn.hh>
#include <zlib/ZmAlloc.hh>

static bool verbose = false;

template <typename Heap> struct S_ : public Heap {
  S_(int i) : m_i(i) { }
  ~S_() { m_i = -1; }
  void doit() {
    if (verbose) { printf("hello world %d\n", m_i); fflush(stdout); }
    if (m_i < 0) __builtin_trap();
  }
  int m_i;
};
constexpr static const char *ID() { return "S"; }
using S = S_<ZmHeap<ID, sizeof(S_<ZuNull>)> >;

static unsigned count = 0;

void doit()
{
  std::cerr << (ZuStringN<80>{} << *ZmSelf() << '\n') << std::flush;
  for (unsigned i = 0; i < count; i++) {
    S *s = new S(i);
    s->doit();
    delete s;
  }
  {
    std::vector<S, ZmAllocator<S, ID>> v;
    std::list<S, ZmAllocator<S>> l;
    for (unsigned i = 0; i < count; i++) {
      v.emplace_back(i);
      l.emplace_back(i);
    }
  }
}

void usage()
{
  fputs(
"usage: ZmHeapTest COUNT SIZE NTHR [VERB]\n\n"
"    COUNT\t- number of iterations\n"
"    SIZE\t- size of heap\n"
"    NTHR\t- number of threads\n"
"    VERB\t- verbose (0 | 1 - defaults to 0)\n"
, stderr);
  Zm::exit(1);
}

int main(int argc, char **argv)
{
  if (argc < 4 || argc > 5) usage();
  {
    std::cout << "ZmGrow sizes:\n";
    unsigned n = 1;
    for (unsigned i = 0; i < 18; i++) {
      auto m = ZmGrow(n, n + 1);
      std::cout <<  n << " -> " << m << '\n';
      n = m;
    }
  }
  count = atoi(argv[1]);
  int size = atoi(argv[2]);
  int nthr = atoi(argv[3]);
  if (argc == 5) verbose = atoi(argv[4]);
  if (!count || !nthr) usage();
  for (int i = 0; i < nthr; i++)
    ZmHeapMgr::init("S", i, ZmHeapConfig{0, static_cast<unsigned>(size)});
  auto threads = ZmAlloc(ZmThread, nthr * sizeof(ZmThread));
  if (!threads) {
    fputs("ZmAlloc() failed\n", stderr);
    Zm::exit(1);
  }
  ZmTime start(ZmTime::Now);
  for (int i = 0; i < nthr; i++)
    new (&threads[i]) ZmThread{doit, ZmThreadParams{}.partition(i), i};
  for (int i = 0; i < nthr; i++) threads[i].join();
  ZmTime end(ZmTime::Now);
  end -= start;
  printf("%u.%09u\n", (unsigned)end.sec(), (unsigned)end.nsec());
  std::cout << ZmHeapMgr::csv();
}
