//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// block allocator with affinitized cache (free list) and statistics

#include <stdlib.h>

#include <zlib/ZmHeap.hh>

#include <zlib/ZuTuple.hh>
#include <zlib/ZuCArray.hh>

#include <zlib/ZmSingleton.hh>
#include <zlib/ZmThread.hh>
#include <zlib/ZmTopology.hh>
#include <zlib/ZmRBTree.hh>
#include <zlib/ZmLHash.hh>

class ZmHeapMgr;
class ZmHeapCache;

class ZmHeapLookup {
  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmGuard<Lock>;

public:
  static constexpr unsigned hashSize() { return 8; }

  using Hash_ = ZmLHashKV<uintptr_t, ZmHeapCache *, ZmLHashLocal<>>;
  struct Hash : public Hash_ { using Hash_::Hash_; };

public:
  ZmHeapLookup() : m_hash{ZmHashParams{hashSize()}} { }

  void add(ZmHeapCache *c) {
    auto begin = reinterpret_cast<uintptr_t>(c->m_begin);
    auto end = reinterpret_cast<uintptr_t>(c->m_end) - 1;
    Guard guard(m_lock);
    if (ZuUnlikely(!m_shift))
      m_shift = (sizeof(end)<<3) - ZuIntrin::clz(end - begin);
    begin >>= m_shift;
    end >>= m_shift;
    m_hash.add(begin, c);
    if (end != begin) m_hash.add(end, c);
    c->lookup(this);
  }
  void del(ZmHeapCache *c) {
    auto begin = reinterpret_cast<uintptr_t>(c->m_begin);
    auto end = reinterpret_cast<uintptr_t>(c->m_end) - 1;
    Guard guard(m_lock);
    begin >>= m_shift;
    end >>= m_shift;
    m_hash.del(begin, c);
    if (end != begin) m_hash.del(end, c);
    c->lookup(nullptr);
  }

  ZmHeapCache *find(ZmHeapCache *skip, void *p) const {
    ReadGuard guard(m_lock);
    if (ZuUnlikely(!m_shift)) return nullptr;
    uintptr_t key = reinterpret_cast<uintptr_t>(p)>>m_shift;
    auto i = m_hash.readIterator(key);
    while (ZmHeapCache *c = i.iterateVal())
      if (ZuLikely(c != skip && c->owned(p))) return c;
    return nullptr;
  }

private:
  mutable Lock		m_lock;
    unsigned		  m_shift = 0;
    Hash		  m_hash;
};

class ZmHeapMgr_ : public ZmObject {
friend ZmHeapMgr;
friend ZmHeapCache;

  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  using IDPart = ZuTuple<ZmIDString, unsigned>;
  using IDSize = ZuTuple<const char *, unsigned>;
  using Key = ZmHeapCache::Key;

  // primary key for heap configurations is {ID, partition}
  using IDPart2Config_ =
    ZmRBTreeKV<IDPart, ZmHeapConfig,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<ZmHeapDisable()>>>;
  struct IDPart2Config : public IDPart2Config_ { using IDPart2Config_::IDPart2Config_; };
  // id2Cache is non-unique map used to find and configure heaps that were
  // constructed prior to configuration, and enable/disable tracing by apps
  using ID2Cache_ =
    ZmRBTree<ZmHeapCache *,
      ZmRBTreeKey<ZmHeapCache::IDAxor,
	ZmRBTreeHeapID<ZmHeapDisable()>>>;
  struct ID2Cache : public ID2Cache_ { using ID2Cache_::ID2Cache_; };
  // key2Cache is unique map from primary key to individual heap cache;
  // primary key for a heap is {ID, partition, size, sharded}
  using Key2Cache_ =
    ZmRBTree<ZmHeapCache *,
      ZmRBTreeKey<ZmHeapCache::KeyAxor,
	ZmRBTreeUnique<true,
	  ZmRBTreeHeapID<ZmHeapDisable()>>>>;
  struct Key2Cache : public Key2Cache_ { using Key2Cache_::Key2Cache_; };
  // lookups are only used for non-sharded heaps; primary key is {ID, size};
  // IDSize2Lookup maps direct from {ID, size, address} to individual heap
  // for free()
  using IDSize2Lookup_ =
    ZmRBTreeKV<IDSize, ZmHeapLookup,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<ZmHeapDisable()>>>;
  struct IDSize2Lookup : public IDSize2Lookup_ { using IDSize2Lookup_::IDSize2Lookup_; };

  using StatsFn = ZmHeapStatsFn;

#ifdef ZmHeap_DEBUG
  using TraceFn = ZmHeapMgr::TraceFn;
#endif

public:
  ZmHeapMgr_() = default;
  ~ZmHeapMgr_() {
    m_key2Cache.clean();
    m_id2Cache.clean([
#ifdef ZmObject_DEBUG
      this
#endif
    ](auto node) {
      ZmDEREF(node->val());
    });
  }

private:
  static ZmHeapMgr_ *instance() {
    return
      ZmSingleton<ZmHeapMgr_,
	ZmSingletonCleanup<ZmCleanup::HeapMgr>>::instance();
  }

  void init(const char *id, unsigned partition, const ZmHeapConfig &config) {
    auto hwloc = ZmTopology::hwloc();
    Guard guard(m_lock);
    m_configs.del(ZuFwdTuple(id, partition));
    m_configs.add(ZuFwdTuple(id, partition), config);
    {
      auto i = m_id2Cache.readIterator<ZmRBTreeEqual>(id);
      while (ZmHeapCache *c = i.iterateVal())
	if (c->info().partition == partition)
	  c->init(config, hwloc);
    }
  }

  void all(ZmFn<void(ZmHeapCache *)> fn) {
    ZmRef<ZmHeapCache> c;
    {
      ReadGuard guard(m_lock);
      c = m_key2Cache.minimumVal();
    }
    while (c) {
      fn(c);
      {
	ReadGuard guard(m_lock);
	c = m_key2Cache.readIterator<ZmRBTreeGreater>(
	    ZmHeapCache::KeyAxor(c)).iterateVal();
      }
    }
  }

  void all(const char *id, ZmFn<void(ZmHeapCache *)> fn) {
    Key key{id, 0U, 0U, false};
    ZmRef<ZmHeapCache> c;
    for (;;) {
      {
	ReadGuard guard(m_lock);
	c = m_key2Cache.readIterator<ZmRBTreeGreater>(key).iterateVal();
      }
      if (!c) return;
      if (strcmp(id, c->info().id)) return;
      key = ZmHeapCache::KeyAxor(c);
      fn(c);
    }
  }

#ifdef ZmHeap_DEBUG
  void trace(const char *id, TraceFn allocFn, TraceFn freeFn) {
    auto i = m_id2Cache.readIterator<ZmRBTreeEqual>(id);
    while (ZmHeapCache *c = i.iterateVal()) {
      c->m_traceAllocFn = allocFn;
      c->m_traceFreeFn = freeFn;
    }
  }
#endif

  ZmHeapCache *cache(
    const char *id, unsigned size, unsigned alignment, bool sharded,
    StatsFn statsFn)
  {
    unsigned partition = ZmSelf()->partition();
    ZmHeapCache *c = 0;
    auto hwloc = ZmTopology::hwloc();
    Guard guard(m_lock);
    if (c = m_key2Cache.findVal(
	ZuFwdTuple(id, partition, size, alignment, sharded)))
      return c;
    if (IDPart2Config::NodeRef node =
	m_configs.find(ZuFwdTuple(id, partition)))
      c = new ZmHeapCache(
	id, size, alignment, partition, sharded, node->val(), statsFn, hwloc);
    else
      c = new ZmHeapCache(
	id, size, alignment, partition, sharded, ZmHeapConfig{
	  .cacheSize = 0
	}, statsFn, hwloc);
    ZmREF(c);
    m_id2Cache.add(c);
    m_key2Cache.add(c);
    if (!sharded && c->info().config.cacheSize) {
      IDSize2Lookup::Node *lookupNode = m_lookups.find(ZuFwdTuple(id, size));
      if (!lookupNode) {
	lookupNode = new IDSize2Lookup::Node{};
	lookupNode->key() = ZuFwdTuple(id, size);
	m_lookups.addNode(lookupNode);
      }
      lookupNode->val().add(c);
    }
    return c;
  }

  ZmPLock		m_lock;
    IDPart2Config	  m_configs;
    ID2Cache		  m_id2Cache;
    Key2Cache		  m_key2Cache;
    IDSize2Lookup	  m_lookups;
};

void ZmHeapMgr::init(
    const char *id, unsigned partition, const ZmHeapConfig &config)
{
  ZmHeapMgr_::instance()->init(id, partition, config);
}

void ZmHeapMgr::all(ZmFn<void(ZmHeapCache *)> fn)
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
  const char *id, unsigned size, unsigned alignment, bool sharded,
  StatsFn statsFn)
{
  return ZmHeapMgr_::instance()->cache(id, size, alignment, sharded, statsFn);
}

void *ZmHeapCache::operator new(size_t size) {
  void *ptr = Zm::alignedAlloc<512>(size);
  if (ZuUnlikely(!ptr)) throw std::bad_alloc{};
  return ptr;
}
void *ZmHeapCache::operator new(size_t, void *ptr)
{
  return ptr;
}
void ZmHeapCache::operator delete(void *ptr)
{
  Zm::alignedFree(ptr);
}

ZmHeapCache::ZmHeapCache(
    const char *id, unsigned size, unsigned alignment,
    unsigned partition, bool sharded,
    const ZmHeapConfig &config, StatsFn statsFn, hwloc_topology_t hwloc) :
  m_info{id, size, alignment, partition, sharded, config}, m_statsFn{statsFn}
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
  m_info.size = (m_info.size + m_info.alignment - 1) & ~(m_info.alignment - 1);
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

void ZmHeapCache::free(ZmHeapStats &stats, void *ptr)
{
  if (ZuUnlikely(!ptr)) return;
#ifdef ZmHeap_DEBUG
  {
    TraceFn fn;
    if (ZuUnlikely(fn = m_traceFreeFn)) (*fn)(m_info.id, m_info.size);
  }
#endif
  ++stats.frees;
  // sharded - no contention, no need to check other partitions
  if (ZuLikely(m_info.sharded)) {
    if (ZuLikely(owned(ptr))) {
      free_sharded(ptr);
      return;
    }
    goto heapfree;
  }
  // check own cache first - optimize for malloc()/free() within same partition
  if (ZuLikely(owned(ptr))) {
    free_(ptr);
    return;
  }
  if (auto lookup = this->lookup())
    if (auto other = lookup->find(this, ptr)) {
      other->free_(ptr);
      return;
    }
heapfree:
  Zm::alignedFree(ptr);
}

// stats() iterates over the ZmHeapCacheT instances using
// ZmSpecific::all, compiling aggregate statistics from the
// thread-specific instances
void ZmHeapCache::stats() const
{
  {
    HistReadGuard guard{m_histLock};
    m_stats = m_histStats;
  }
  m_statsFn(); // calls ZmHeapCacheT::stats() { TLS::all(...) }
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
  stats();
  data.id = m_info.id;
  data.cacheSize = m_info.config.cacheSize;
  data.cpuset = m_info.config.cpuset;
  data.cacheAllocs = m_stats.cacheAllocs;
  data.heapAllocs = m_stats.heapAllocs;
  data.frees = m_stats.frees;
  data.size = m_info.size;
  data.partition = m_info.partition;
  data.sharded = m_info.sharded;
  data.alignment = m_info.alignment;
}
