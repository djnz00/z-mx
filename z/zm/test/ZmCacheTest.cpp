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

#include <zlib/ZuLib.hpp>

#include <zlib/ZmCache.hpp>
#include <zlib/ZmRBTree.hpp>

struct Z {
  unsigned v;

  struct Traits : public ZuBaseTraits<Z> {
    enum { IsPrimitive = 0, IsReal = 0 };
  };
  friend Traits ZuTraitsType(Z *);
};

template <typename>
struct ZCmp {
  static int cmp(const Z *z1, const Z *z2) { return z1->v - z2->v; }
  static bool less(const Z *z1, const Z *z2) { return z1->v < z2->v; }
  static bool equals(const Z *z1, const Z *z2) { return z1->v == z2->v; }
  static bool null(const Z *z) { return !z; }
  constexpr static const Z *null() { return nullptr; }
};

using ZCache = ZmCacheKV<unsigned, Z>;
using ZNode = ZCache::Node;

using ZTree = ZmRBTreeKV<unsigned, Z>;

void backFill(ZTree &tree, unsigned cacheSize)
{
  for (unsigned i = 0; i < cacheSize; i++) tree.add(i, Z{i});
}

void find(ZCache &cache, ZTree &tree, unsigned offset, unsigned cacheSize)
{
  for (unsigned i = 0; i < cacheSize; i++)
    cache.find(offset + i,
	  [&tree] <typename L> (unsigned key, L l) {
	    if (auto node = tree.find(key))
	      l(new ZNode{key, node->val().v});
	    else
	      l(nullptr);
	  },
	  [](ZNode *) { });
}

void stats(const ZCache &cache)
{
  ZCache::Stats stats;
  cache.stats(stats);
  std::cout <<
    "count=" << stats.count <<
    " loads=" << stats.loads <<
    " misses=" << stats.misses <<
    " evictions=" << stats.evictions <<
    '\n' << std::flush;
}

int main(int argc, char **argv)
{
  unsigned cacheSize = 100;
  unsigned nThreads = 2;
  unsigned nLoops = 2;
  ZmTime overallStart, overallEnd;

  if (argc > 1) cacheSize = atoi(argv[1]);
  if (argc > 2) nThreads = atoi(argv[2]);
  if (argc > 3) nLoops = atoi(argv[3]);

  auto threads = ZmAlloc(ZmThread, nThreads);

  std::cout << "spawning "  << nThreads << " threads...\n";

  overallStart.now();

  ZCache cache{cacheSize};
  ZTree tree;

  backFill(tree, cacheSize);

#if 0
  find(cache, tree, 0, cacheSize);
  find(cache, tree, 0, cacheSize);
#endif

  auto increment = cacheSize / nThreads;

  for (unsigned l = 0; l < nLoops; l++) {
    for (unsigned i = 0, j = 0; i < nThreads; i++, j += increment)
      new (&threads[i]) ZmThread{[&cache, &tree, j, cacheSize]() {
	find(cache, tree, j, cacheSize);
      }};
    for (unsigned i = 0; i < nThreads; i++)
      threads[i].join();
  }

  stats(cache);
}
