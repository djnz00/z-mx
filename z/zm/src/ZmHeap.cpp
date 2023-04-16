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
#include <zlib/ZmLHash.hpp>

class ZmHeapMgr;
class ZmHeapCache;

class ZmHeapLookup {
public:
  constexpr static unsigned hashSize() { return 8; }

  using Hash = ZmLHashKV<uintptr_t, ZmHeapCache *, ZmLHashLocal<>>;

public:
  ZmHeapLookup() : m_hash{ZmHashParams{hashSize()}} { }

  void add(ZmHeapCache *c) {
    auto begin = reinterpret_cast<uintptr_t>(c->m_begin);
    auto end = reinterpret_cast<uintptr_t>(c->m_end) - 1;
    if (ZuUnlikely(!m_shift))
      m_shift = 32U - __builtin_clz(end - begin);
    begin >>= m_shift;
    end >>= m_shift;
    m_hash.add(begin, c);
    if (end != begin) m_hash.add(end, c);
    c->lookup(this);
  }
  void del(ZmHeapCache *c) {
    auto begin = reinterpret_cast<uintptr_t>(c->m_begin);
    auto end = reinterpret_cast<uintptr_t>(c->m_end) - 1;
    begin >>= m_shift;
    end >>= m_shift;
    m_hash.del(begin, c);
    if (end != begin) m_hash.del(end, c);
    c->lookup(nullptr);
  }

  ZmHeapCache *find(void *p) const {
    if (ZuUnlikely(!m_shift)) return nullptr;
    uintptr_t key = reinterpret_cast<uintptr_t>(p)>>m_shift;
    auto i = m_hash.readIterator(key);
    while (ZmHeapCache *c = i.iterateVal())
      if (ZuLikely(c->owned(p))) return c;
    return nullptr;
  }

private:
  unsigned	  m_shift = 0;
  Hash		  m_hash;
};

class ZmHeapMgr_ : public ZmObject {
friend ZmSingletonCtor<ZmHeapMgr_>;
friend ZmHeapMgr;
friend ZmHeapCache;

  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  using IDPart = ZuPair<ZmIDString, unsigned>;
  using IDSize = ZuPair<const char *, unsigned>;
  using IDPartSize = ZmHeapCache::IDPartSize;

  using IDPart2Config =
    ZmRBTreeKV<IDPart, ZmHeapConfig,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<ZmHeapDisable()>>>;
  using ID2Cache =
    ZmRBTree<ZmHeapCache *,
      ZmRBTreeKey<ZmHeapCache::IDAxor,
	ZmRBTreeHeapID<ZmHeapDisable()>>>;
  using IDSize2Lookup =
    ZmRBTreeKV<IDSize, ZmHeapLookup,
      ZmRBTreeHeapID<ZmHeapDisable()>>;
  using IDPartSize2Cache =
    ZmRBTree<ZmHeapCache *,
      ZmRBTreeKey<ZmHeapCache::IDPartSizeAxor,
	ZmRBTreeHeapID<ZmHeapDisable()>>>;

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
    m_caches.clean([
#ifdef ZmObject_DEBUG
      this
#endif
    ](auto node) { ZmDEREF(node->val()); });
  }

  friend ZuConstant<ZmCleanup::HeapMgr> ZmCleanupLevel(ZmHeapMgr_ *);

private:
  static ZmHeapMgr_ *instance() {
    return ZmSingleton<ZmHeapMgr_>::instance();
  }

  void init(const char *id, unsigned partition, const ZmHeapConfig &config) {
    auto hwloc = ZmTopology::hwloc();
    Guard guard(m_lock);
    m_configs.del(ZuFwdPair(id, partition));
    m_configs.add(ZuFwdPair(id, partition), config);
    {
      auto i = m_caches.readIterator<ZmRBTreeEqual>(id);
      while (ZmHeapCache *c = i.iterateVal())
	if (c->info().partition == partition)
	  c->init(config, hwloc);
    }
  }

  void all(ZmFn<ZmHeapCache *> fn) {
    ZmRef<ZmHeapCache> c;
    {
      ReadGuard guard(m_lock);
      c = m_caches2.minimumVal();
    }
    while (c) {
      fn(c);
      {
	ReadGuard guard(m_lock);
	c = m_caches2.readIterator<ZmRBTreeGreater>(
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
	c = m_caches2.readIterator<ZmRBTreeGreater>(key).iterateVal();
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
    auto hwloc = ZmTopology::hwloc();
    Guard guard(m_lock);
    if (c = m_caches2.findVal(ZuFwdTuple(id, partition, size)))
      return c;
    if (IDPart2Config::NodeRef node = 
	  m_configs.find(ZuFwdPair(id, partition)))
      c = new ZmHeapCache(
	  id, size, partition, sharded, node->val(), allStatsFn, hwloc);
    else
      c = new ZmHeapCache(
	  id, size, partition, sharded, ZmHeapConfig{}, allStatsFn, hwloc);
    ZmREF(c);
    m_caches.add(c);
    m_caches2.add(c);
    if (c->info().config.cacheSize) {
      IDSize2Lookup::Node *lookupNode = m_lookup.find(ZuFwdPair(id, size));
      if (!lookupNode) {
	lookupNode = new IDSize2Lookup::Node{};
	lookupNode->key() = ZuFwdPair(id, size);
	m_lookup.addNode(lookupNode);
      }
      lookupNode->val().add(c);
    }
    return c;
  }

  ZmPLock		m_lock;
  IDPart2Config		m_configs;
  ID2Cache		m_caches;
  IDPartSize2Cache	m_caches2;
  IDSize2Lookup		m_lookup;
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
    const ZmHeapConfig &config, AllStatsFn allStatsFn,
    hwloc_topology_t hwloc) :
  m_info{id, size, partition, sharded, config},
  m_allStatsFn{allStatsFn}
{
  init_(hwloc);
}

ZmHeapCache::~ZmHeapCache()
{
  // printf("~ZmHeapCache() 1 %p\n", this); fflush(stdout);
  final_();
  // printf("~ZmHeapCache() 2 %p\n", this); fflush(stdout);
}

void ZmHeapCache::init(const ZmHeapConfig &config, hwloc_topology_t hwloc)
{
  if (m_info.config.cacheSize) return; // resize is not supported
  m_info.config = config;
  init_(hwloc);
}

void ZmHeapCache::init_(hwloc_topology_t hwloc)
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
  void *begin;
  if (!config.cpuset)
    begin = hwloc_alloc(hwloc, len);
  else
    begin = hwloc_alloc_membind(
      hwloc, len, config.cpuset, HWLOC_MEMBIND_BIND, 0);
  if (!begin) { config.cacheSize = 0; return; }
  uintptr_t n = 0;
  for (auto p = reinterpret_cast<uintptr_t>(begin) + len;
      (p -= m_info.size) >= reinterpret_cast<uintptr_t>(begin); )
    *reinterpret_cast<uintptr_t *>(p) = n, n = p;
  m_begin = begin;
  m_end = reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(begin) + len);
  m_head = reinterpret_cast<uintptr_t>(begin); // assignment causes release
}

void ZmHeapCache::final_()
{
  if (m_lookup)
    m_lookup->del(this);
  if (m_begin)
    hwloc_free(ZmTopology::hwloc(),
	m_begin, m_info.config.cacheSize * m_info.size);
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
  ++stats.frees;
  // sharded - no contention, no need to check other partitions
  if (ZuLikely(m_info.sharded)) {
    if (ZuLikely(owned(p))) {
      free_sharded(p);
      return;
    }
    ::free(p);
    return;
  }
  // check own cache first - optimize for malloc()/free() within same partition
  if (ZuLikely(owned(p))) {
    free_(p);
    return;
  }
  if (auto lookup = this->lookup())
    if (auto other = lookup->find(p)) {
      other->free_(p);
      return;
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
