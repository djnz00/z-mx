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

/* test program */

#include <zlib/ZmTime.hpp>
#include <zlib/ZmHeap.hpp>
#include <zlib/ZmScheduler.hpp>

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

int count;
int size;
int nthr;
ZmSemaphore sem;
ZmScheduler *sched;

int main(int argc, char **argv)
{
  if (argc < 4 || argc > 5) usage();
  count = atoi(argv[1]);
  size = atoi(argv[2]);
  nthr = atoi(argv[3]);
  if (argc == 5) verbose = atoi(argv[4]);
  if (!count || !nthr) usage();
  for (int i = 0; i < nthr; i++)
    ZmHeapMgr::init("S", i, ZmHeapConfig{0, static_cast<unsigned>(size)});
  ZmSchedParams params;
  params.id("sched").nThreads(nthr).startTimer(false);
  for (int i = 0; i < nthr; i++)
    params.thread(i + 1).partition(i);
  ZmScheduler sched_{ZuMv(params)};
  sched = &sched_;
  sched->start();
  ZmTime start(ZmTime::Now);
  for (int j = 0; j < count; j++)
    for (int i = 0; i < nthr; i++)
      sched->run(i + 1, [i, j]() {
	auto s = new S{i + j};
	sched->run(((i + 1) % nthr) + 1, [s]() {
	  delete s;
	  sem.post();
	});
      });
  for (int k = 0, n = count * nthr; k < n; k++) sem.wait();
  sched->stop();
  ZmTime end(ZmTime::Now);
  end -= start;
  printf("%u.%09u\n", (unsigned)end.sec(), (unsigned)end.nsec());
  std::cout << ZmHeapMgr::csv();
}
