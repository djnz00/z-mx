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

// block allocator with affinitized cache (free list) and statistics

#include <stdlib.h>

#include <zlib/ZmHeap.hpp>

#include <zlib/ZuPair.hpp>
#include <zlib/ZuStringN.hpp>

#include <zlib/ZmSingleton.hpp>
#include <zlib/ZmThread.hpp>
#include <zlib/ZmTopology.hpp>
#include <zlib/ZmRBTree.hpp>
#include <zlib/ZmNoLock.hpp>

class ZmHeapMgr;
class ZmHeapCache;

class ZmHeapMgr_ : public ZmObject {
friend ZmSingletonCtor<ZmHeapMgr_>;
friend ZmHeapMgr;
friend ZmHeapCache;

  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  using IDPart = ZuPair<ZmIDString, unsigned>;
  using IDPartSize = ZmHeapCache::IDPartSize;

  using IDPart2Config =
    ZmRBTreeKV<IDPart, ZmHeapConfig,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<ZmHeapDisable(),
	  ZmRBTreeLock<ZmNoLock> > > >;
  using ID2Cache =
    ZmRBTree<ZmHeapCache *,
      ZmRBTreeKey<ZmHeapCache::IDAxor,
	ZmRBTreeHeapID<ZmHeapDisable(),
	  ZmRBTreeLock<ZmNoLock> > > >;
  using IDSize2Cache =
    ZmRBTree<ZmHeapCache *,
      ZmRBTreeKey<ZmHeapCache::IDSizeAxor,
	ZmRBTreeHeapID<ZmHeapDisable(),
	  ZmRBTreeLock<ZmNoLock> > > >;
  using IDPartSize2Cache =
    ZmRBTree<ZmHeapCache *,
      ZmRBTreeKey<ZmHeapCache::IDPartSizeAxor,
	ZmRBTreeHeapID<ZmHeapDisable(),
	  ZmRBTreeLock<ZmNoLock> > > >;

  using StatsFn = ZmHeapCache::StatsFn;
  using AllStatsFn = ZmHeapCache::AllStatsFn;

#ifdef ZmHeap_DEBUG
  using TraceFn = ZmHeapMgr::TraceFn;
#endif

  ZmHeapMgr_() {
    // printf("ZmHeapMgr_() %p\n", this); fflush(stdout);
  }

public:
  ~ZmHeapMgr_() {
    // printf("~ZmHeapMgr_() %p\n", this); fflush(stdout);
    m_caches2.clean();
    m_caches3.clean();
    auto i = m_caches.iterator();
    while (auto node = i.iterate()) {
      auto c = ZuMv(*(i.del(node))).val();
      ZmDEREF(c);
      // delete node;
    }
  }

  friend ZuConstant<ZmCleanup::HeapMgr> ZmCleanupLevel(ZmHeapMgr_ *);

private:
  static ZmHeapMgr_ *instance() {
    return ZmSingleton<ZmHeapMgr_>::instance();
  }

  void init(const char *id, unsigned partition, const ZmHeapConfig &config) {
    Guard guard(m_lock);
    m_configs.del(ZuFwdPair(id, partition));
    m_configs.add(ZuFwdPair(id, partition), config);
    {
      auto i = m_caches.readIterator<ZmRBTreeEqual>(id);
      while (ZmHeapCache *c = i.iterateVal())
	if (c->info().partition == partition) c->init(config);
    }
  }

  void all(ZmFn<ZmHeapCache *> fn) {
    ZmRef<ZmHeapCache> c;
    {
      ReadGuard guard(m_lock);
      c = m_caches3.minimumVal();
    }
    while (c) {
      fn(c);
      {
	ReadGuard guard(m_lock);
	c = m_caches3.readIterator<ZmRBTreeGreater>(
	    ZmHeapCache::IDPartSizeAxor(c)).iterateVal();
      }
    }
  }

  void all(const char *id, ZmFn<ZmHeapCache *> fn) {
    IDPartSize key{id, 0U, 0U};
    ZmRef<ZmHeapCache> c;
    for (;;) {
      {
	ReadGuard guard(m_lock);
	c = m_caches3.readIterator<ZmRBTreeGreater>(key).iterateVal();
      }
      if (!c) return;
      if (strcmp(id, c->info().id)) return;
      key = ZmHeapCache::IDPartSizeAxor(c);
      fn(c);
    }
  }

#ifdef ZmHeap_DEBUG
  void trace(const char *id, TraceFn allocFn, TraceFn freeFn) {
    auto i = m_caches.readIterator<ZmRBTreeEqual>(id);
    while (ZmHeapCache *c = i.iterateVal()) {
      c->m_traceAllocFn = allocFn;
      c->m_traceFreeFn = freeFn;
    }
  }
#endif

  ZmHeapCache *cache(
      const char *id, unsigned size, bool sharded, AllStatsFn allStatsFn) {
    unsigned partition = ZmThreadContext::self()->partition();
    ZmHeapCache *c = 0;
    Guard guard(m_lock);
    if (c = m_caches3.findVal(ZuFwdTuple(id, partition, size)))
      return c;
    ZmHeapCache *n = m_caches2.delVal(ZuFwdTuple(id, size));
    if (IDPart2Config::NodeRef node = 
	  m_configs.find(ZuFwdPair(id, partition)))
      c = new ZmHeapCache(
	  id, size, partition, sharded, node->val(), n, allStatsFn);
    else
      c = new ZmHeapCache(
	  id, size, partition, sharded, ZmHeapConfig{}, n, allStatsFn);
    c->ref();
    m_caches.add(c);
    m_caches2.add(c);
    m_caches3.add(c);
    return c;
  }

  ZmHeapCache *head(const char *id, unsigned size) {
    ZmReadGuard<ZmPLock> guard(m_lock);
    return m_caches2.findVal(ZuFwdTuple(id, size));
  }

  ZmPLock		m_lock;
  IDPart2Config		m_configs;
  ID2Cache		m_caches;
  IDSize2Cache		m_caches2;
  IDPartSize2Cache	m_caches3;
};

void ZmHeapMgr::init(
    const char *id, unsigned partition, const ZmHeapConfig &config)
{
  ZmHeapMgr_::instance()->init(id, partition, config);
}

void ZmHeapMgr::all(ZmFn<ZmHeapCache *> fn)
{
  ZmHeapMgr_::instance()->all(ZuMv(fn));
}

#ifdef ZmHeap_DEBUG
void ZmHeapMgr::trace(const char *id, TraceFn allocFn, TraceFn freeFn)
{
  ZmHeapMgr_::instance()->trace(id, allocFn, freeFn);
}
#endif

ZmHeapCache *ZmHeapMgr::cache(
    const char *id, unsigned size, bool sharded, AllStatsFn allStatsFn)
{
  return ZmHeapMgr_::instance()->cache(id, size, sharded, allStatsFn);
}

void *ZmHeapCache::operator new(size_t s) {
#ifndef _WIN32
  void *p = 0;
  int errNo = posix_memalign(&p, 512, s);
  return (!p || errNo) ? 0 : p;
#else
  return _aligned_malloc(s, 512);
#endif
}
void *ZmHeapCache::operator new(size_t s, void *p)
{
  return p;
}
void ZmHeapCache::operator delete(void *p)
{
#ifndef _WIN32
  ::free(p);
#else
  _aligned_free(p);
#endif
}

ZmHeapCache::ZmHeapCache(
    const char *id, unsigned size, unsigned partition, bool sharded,
    const ZmHeapConfig &config,
    ZmHeapCache *next, AllStatsFn allStatsFn) :
  m_info{id, size, partition, sharded, config},
  m_next(next), m_allStatsFn(allStatsFn),
  m_cache(0), m_end(0)
#ifdef ZmHeap_DEBUG
  , m_traceAllocFn(0), m_traceFreeFn(0)
#endif
  , m_histStats{}, m_stats{}
{
  init_();
}

ZmHeapCache::~ZmHeapCache()
{
  // printf("~ZmHeapCache() 1 %p\n", this); fflush(stdout);
  free_();
  // printf("~ZmHeapCache() 2 %p\n", this); fflush(stdout);
}

void ZmHeapCache::init(const ZmHeapConfig &config)
{
  if (m_info.config.cacheSize) return; // resize is not supported
  m_info.config = config;
  init_();
}

void ZmHeapCache::init_()
{
  ZmHeapConfig &config = m_info.config;
  if (!config.cacheSize) return;
  if (config.alignment <= sizeof(uintptr_t))
    config.alignment = sizeof(uintptr_t);
  else {
    // round up to nearest power of 2, ceiling of 512
    config.alignment = 1U<<(64U - __builtin_clz(config.alignment - 1));
    if (ZuUnlikely(config.alignment > 512)) config.alignment = 512;
  }
  m_info.size = (m_info.size + config.alignment - 1) & ~(config.alignment - 1);
  uint64_t len = config.cacheSize * m_info.size;
  void *cache;
  if (!config.cpuset)
    cache = hwloc_alloc(ZmTopology::hwloc(), len);
  else
    cache = hwloc_alloc_membind(
      ZmTopology::hwloc(), len, config.cpuset, HWLOC_MEMBIND_BIND, 0);
  if (!cache) { config.cacheSize = 0; return; }
  uintptr_t n = 0;
  for (auto p = reinterpret_cast<uintptr_t>(cache) + len;
      (p -= m_info.size) >= reinterpret_cast<uintptr_t>(cache); )
    *reinterpret_cast<uintptr_t *>(p) = n, n = p;
  m_end = reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(cache) + len);
  m_cache = cache;

  m_head = reinterpret_cast<uintptr_t>(cache); // assignment causes release
}

void ZmHeapCache::free_()
{
  if (m_cache)
    hwloc_free(ZmTopology::hwloc(),
	m_cache, m_info.config.cacheSize * m_info.size);
}

void *ZmHeapCache::alloc(ZmHeapStats &stats)
{
#ifdef ZmHeap_DEBUG
  {
    TraceFn fn;
    if (ZuUnlikely(fn = m_traceAllocFn))
      (*fn)(m_info.id, m_info.size);
  }
#endif
  void *p;
  if (ZuLikely(p = alloc_())) {
    ++stats.cacheAllocs;
    return p;
  }
  p = ::malloc(m_info.size);
  ++stats.heapAllocs;
  return p;
}

void ZmHeapCache::free(ZmHeapStats &stats, void *p)
{
  if (ZuUnlikely(!p)) return;
#ifdef ZmHeap_DEBUG
  {
    TraceFn fn;
    if (ZuUnlikely(fn = m_traceFreeFn)) (*fn)(m_info.id, m_info.size);
  }
#endif
  free_(this, p);
  ++stats.frees;
}

void ZmHeapCache::free_(ZmHeapCache *self, void *p)
{
  if (ZuLikely(self->m_info.sharded)) {
    // sharded - no contention, no need to check other partitions
    void *cache = self->m_cache;
    if (ZuLikely(cache && p >= cache && p < self->m_end)) {
      self->free__sharded(p);
      return;
    }
    ::free(p);
    return;
  }
  // check own cache first - optimize for malloc()/free() within same partition
  void *cache = self->m_cache;
  if (ZuLikely(cache && p >= cache && p < self->m_end)) {
    self->free__(p);
    return;
  }
  // check other partitions
  ZmHeapCache *other =
    ZmHeapMgr_::instance()->head(self->m_info.id, self->m_info.size);
  while (ZuLikely(other)) {
    if (ZuLikely(other != self)) {
      cache = other->m_cache;
      if (cache && p >= cache && p < other->m_end) {
	other->free__(p);
	return;
      }
    }
    other = other->m_next;
  }
  ::free(p);
}

void ZmHeapCache::allStats() const
{
  {
    HistReadGuard guard{m_histLock};
    m_stats = m_histStats;
  }
  StatsFn fn = StatsFn::Lambda<ZmHeapDisable()>::fn(
      [this](const ZmHeapStats &s) {
	m_stats.heapAllocs += s.heapAllocs;
	m_stats.cacheAllocs += s.cacheAllocs;
	m_stats.frees += s.frees; });
  m_allStatsFn(ZuMv(fn));
}

void ZmHeapCache::histStats(const ZmHeapStats &s) const
{
  HistGuard guard{m_histLock};
  m_histStats.heapAllocs += s.heapAllocs;
  m_histStats.cacheAllocs += s.cacheAllocs;
  m_histStats.frees += s.frees;
}

void ZmHeapCache::telemetry(ZmHeapTelemetry &data) const
{
  allStats();
  const ZmHeapInfo &info = this->info();
  const ZmHeapStats &stats = this->stats();
  data.id = info.id;
  data.cacheSize = info.config.cacheSize;
  data.cpuset = info.config.cpuset;
  data.cacheAllocs = stats.cacheAllocs;
  data.heapAllocs = stats.heapAllocs;
  data.frees = stats.frees;
  data.size = info.size;
  data.partition = info.partition;
  data.sharded = info.sharded;
  data.alignment = info.config.alignment;
}
