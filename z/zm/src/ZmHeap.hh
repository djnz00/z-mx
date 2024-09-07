//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// recycling zero-overhead block allocator
// - compile-time determination of fixed object size
// - intentionally recycles without zero-filling
// - arenas with CPU core and NUMA affinity
// - cache-aware
// - optional partitions / sharding
//   - fast partition lookup
// - TLS free list
// - efficient statistics and telemetry (Ztel)
// - globally configured
//   - supports profile-guided optimization of heap configuration

// Note: classes using ZmHeap benefit from empty base optimization
// - with MSVC, only the FIRST base class is optimized by default
// - to ensure ZmHeap EBO with multiple inheritance, always
//   place the ZmHeap first in the list of base classes

#ifndef ZmHeap_HH
#define ZmHeap_HH

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <new>

#include <zlib/ZuTuple.hh>
#include <zlib/ZuPrint.hh>

#include <zlib/ZmPlatform.hh>
#include <zlib/ZmObject.hh>
#include <zlib/ZmBitmap.hh>
#include <zlib/ZmSpecific.hh>
#include <zlib/ZmPLock.hh>
#include <zlib/ZmGuard.hh>
#include <zlib/ZmFn_.hh>	// avoid circular dependency

#if defined(ZDEBUG) && !defined(ZmHeap_DEBUG)
#define ZmHeap_DEBUG
#endif

class ZmHeapMgr;
class ZmHeapMgr_;
class ZmHeapCache;
template <auto ID, unsigned Size, unsigned Algnment, bool Sharded>
class ZmHeap__;
template <auto ID, unsigned Size, unsigned Algnment, bool Sharded>
class ZmHeapCacheT;

struct ZmHeapConfig {
  uint32_t	alignment;
  uint64_t	cacheSize;
  ZmBitmap	cpuset;
};

struct ZmHeapInfo {
  const char	*id;
  unsigned	size;
  unsigned	partition;
  bool		sharded;
  ZmHeapConfig	config;
};

struct ZmHeapStats {
  uint64_t	heapAllocs;
  uint64_t	cacheAllocs;
  uint64_t	frees;
};

// display sequence:
//   id, size, alignment, partition, sharded,
//   cacheSize, cpuset, cacheAllocs, heapAllocs, frees, allocated (*)
// derived display fields:
//   allocated = (heapAllocs + cacheAllocs) - frees
struct ZmHeapTelemetry {
  ZmIDString	id;		// primary key
  uint64_t	cacheSize = 0;
  ZmBitmap	cpuset;
  uint64_t	cacheAllocs = 0;// graphable (*)
  uint64_t	heapAllocs = 0;	// graphable (*)
  uint64_t	frees = 0;	// graphable
  uint32_t	size = 0;
  uint16_t	partition = 0;
  uint8_t	sharded = 0;
  uint8_t	alignment = 0;
};

class ZmHeapLookup;

typedef void (*ZmHeapStatsFn)();

// cache (LIFO free list) of fixed-size blocks; one per CPU set / NUMA node
class ZmAPI ZmHeapCache : public ZmObject {
friend ZmHeapMgr;
friend ZmHeapMgr_;
friend ZmHeapLookup;
template <auto, unsigned, unsigned, bool> friend class ZmHeap__;
template <auto, unsigned, unsigned, bool> friend class ZmHeapCacheT;

  enum { CacheLineSize = Zm::CacheLineSize };

  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;

  using StatsFn = ZmHeapStatsFn;

  void stats(const ZmHeapStats &s) { // aggregate statistics from ZmHeapCacheT
    m_stats.heapAllocs += s.heapAllocs;
    m_stats.cacheAllocs += s.cacheAllocs;
    m_stats.frees += s.frees;
  }

  static const char *IDAxor(const ZmHeapCache *this_) {
    return this_->m_info.id;
  }
  using IDSize = ZuTuple<const char *, unsigned>;
  // primary key for a heap is {ID, partition, size, sharded}
  using Key = ZuTuple<const char *, unsigned, unsigned, bool>;
  static Key KeyAxor(const ZmHeapCache *this_) {
    const auto &info = this_->m_info;
    return {info.id, info.partition, info.size, info.sharded};
  }

  void *operator new(size_t s);
  void *operator new(size_t s, void *p);
public:
  void operator delete(void *p);

private:
  ZmHeapCache(
      const char *id, unsigned size, unsigned partition, bool sharded,
      const ZmHeapConfig &, StatsFn, hwloc_topology_t);

  void lookup(ZmHeapLookup *l) { m_lookup = l; }
  ZmHeapLookup *lookup() const { return m_lookup; }

public:
  ~ZmHeapCache();

  const ZmHeapInfo &info() const { return m_info; }

  void telemetry(ZmHeapTelemetry &data) const;

#ifdef ZmHeap_DEBUG
  typedef void (*TraceFn)(const char *, unsigned);
#endif

private:
  void init(const ZmHeapConfig &, hwloc_topology_t);
  void init_(hwloc_topology_t);
  void final_();

  void *alloc(ZmHeapStats &stats);
  void free(ZmHeapStats &stats, void *p);

  // lock-free MPMC LIFO slist

  void *alloc_() {
    uintptr_t p;
  loop:
    p = m_head.load_();
    if (ZuUnlikely(!p)) return nullptr;
    if (ZuLikely(m_info.sharded)) { // sharded - no contention
      m_head.store_(*reinterpret_cast<uintptr_t *>(p));
      return reinterpret_cast<void *>(p);
    }
    if (ZuUnlikely(p & 1)) { ZmAtomic_acquire(); goto loop; }
    if (ZuUnlikely(m_head.cmpXch(p | 1, p) != p)) goto loop;
    m_head = reinterpret_cast<ZmAtomic<uintptr_t> *>(p)->load_();
    return reinterpret_cast<void *>(p);
  }
  void free_(void *p) {
    uintptr_t n;
  loop:
    n = m_head.load_();
    if (n & 1) { ZmAtomic_acquire(); goto loop; }
    reinterpret_cast<ZmAtomic<uintptr_t> *>(p)->store_(n);
    if (m_head.cmpXch(reinterpret_cast<uintptr_t>(p), n) != n) goto loop;
  }
  void free_sharded(void *p) { // sharded - no contention
    *reinterpret_cast<uintptr_t *>(p) = m_head.load_();
    m_head.store_(reinterpret_cast<uintptr_t>(p));
  }

  bool owned(void *p) const {
    return p >= m_begin && p < m_end;
  }

  void stats() const;
  void histStats(const ZmHeapStats &stats) const;

  // cache, end, lookup are guarded by ZmHeapMgr

  ZmAtomic<uintptr_t>	m_head;		// free list (contended atomic)
  char			m__pad[CacheLineSize - sizeof(uintptr_t)];

  ZmHeapInfo		m_info;
  ZmHeapLookup		*m_lookup = nullptr;
  StatsFn		m_statsFn;	// aggregates stats from TLS

  void			*m_begin = nullptr;	// bound memory region
  void			*m_end = nullptr;	// end of memory region

#ifdef ZmHeap_DEBUG
  TraceFn		m_traceAllocFn = nullptr;
  TraceFn		m_traceFreeFn = nullptr;
#endif

  using HistLock = ZmPLock;
  using HistGuard = ZmGuard<HistLock>;
  using HistReadGuard = ZmReadGuard<HistLock>;

  mutable HistLock	m_histLock;
    mutable ZmHeapStats	  m_histStats{};// stats from exited threads
  mutable ZmHeapStats	m_stats{};	// aggregated on demand
};

class ZmAPI ZmHeapMgr {
friend ZmHeapCache;
template <auto, unsigned, unsigned, bool> friend class ZmHeapCacheT; 

  template <class S> struct CSV_ {
    CSV_(S &stream) : m_stream(stream) { }
    void print() {
      m_stream <<
	"ID,size,partition,sharded,alignment,cacheSize,cpuset,"
	"cacheAllocs,heapAllocs,frees\n";
      ZmHeapMgr::all(
	ZmFn<void(ZmHeapCache *)>::Member<&CSV_::print_>::fn(this));
    }
    void print_(ZmHeapCache *cache) {
      ZmHeapTelemetry data;
      cache->telemetry(data);
      m_stream <<
	'"' << data.id << "\"," <<	// assume no need to quote embedded "
	ZuBoxed(data.size) << ',' <<
	ZuBoxed(data.partition) << ',' <<
	ZuBoxed(data.sharded) << ',' <<
	ZuBoxed(data.alignment) << ',' <<
	ZuBoxed(data.cacheSize) << ',' <<
	data.cpuset << ',' <<
	ZuBoxed(data.cacheAllocs) << ',' <<
	ZuBoxed(data.heapAllocs) << ',' <<
	ZuBoxed(data.frees) << '\n';
    }

  private:
    S	&m_stream;
  };

public:
  static void init(
      const char *id, unsigned partition, const ZmHeapConfig &config);

  static void all(ZmFn<void(ZmHeapCache *)> fn);

  struct CSV {
    template <typename S> void print(S &s) const {
      ZmHeapMgr::CSV_<S>(s).print();
    }
    friend ZuPrintFn ZuPrintType(CSV *);
  };
  static CSV csv() { return CSV(); }

#ifdef ZmHeap_DEBUG
  using TraceFn = ZmHeapCache::TraceFn;

  static void trace(const char *id, TraceFn allocFn, TraceFn freeFn);
#endif

private:
  using StatsFn = ZmHeapStatsFn;

  static ZmHeapCache *cache(
    const char *id, unsigned size, unsigned alignment, bool sharded, StatsFn);
};

// TLS heap cache, specific to ID+size; maintains TLS heap statistics
template <auto ID, unsigned Size, unsigned Alignment, bool Sharded>
class ZmHeapCacheT : public ZmObject {
  using TLS = ZmSpecific<ZmHeapCacheT, ZmSpecificCleanup<ZmCleanup::Heap>>;

public:
  ZmHeapCacheT() :
    m_cache{ZmHeapMgr::cache(ID(), Size, Alignment, Sharded, &stats)},
    m_stats{} { }
  ~ZmHeapCacheT() {
    m_cache->histStats(m_stats);
  }

  // stats() uses ZmSpecific::all to iterate over all threads and
  // collect/aggregate statistics for each TLS instance
  static void stats();

  static ZmHeapCacheT *instance() { return TLS::instance(); }
  static void *alloc() {
    ZmHeapCacheT *this_ = instance();
    return this_->m_cache->alloc(this_->m_stats);
  }
  static void free(void *p) {
    ZmHeapCacheT *this_ = instance();
    this_->m_cache->free(this_->m_stats, p);
  }

private:
  ZmHeapCache	*m_cache;
  ZmHeapStats	m_stats;
};

// ZmHeapAllocSize returns a size that is minimum sizeof(uintptr_t),
// or the smallest power of 2 greater than the passed size yet smaller
// than the cache line size, or the size rounded up to the nearest multiple
// of the cache line size
template <unsigned Size_,
	  bool Small = (Size_ <= sizeof(uintptr_t)),
	  unsigned RShift = 0,
	  bool Big = (Size_ > (Zm::CacheLineSize>>RShift))>
  struct ZmHeapAllocSize;
template <unsigned Size_, unsigned RShift, bool Big> // smallest
struct ZmHeapAllocSize<Size_, true, RShift, Big> {
  enum { N = sizeof(uintptr_t) };
};
template <unsigned Size_, unsigned RShift> // smaller
struct ZmHeapAllocSize<Size_, false, RShift, false> {
  enum { N = ZmHeapAllocSize<Size_, false, RShift + 1>::N };
};
template <unsigned Size_, unsigned RShift> // larger
struct ZmHeapAllocSize<Size_, false, RShift, true> {
  enum { N = (Zm::CacheLineSize>>(RShift - 1)) };
};
template <unsigned Size_>
struct ZmHeapAllocSize<Size_, false, 0, true> { // larger than cache line size
  enum { N = ((Size_ + Zm::CacheLineSize - 1) & ~(Zm::CacheLineSize - 1)) };
};

template <typename Heap> class ZmHeap_Init {
template <auto, unsigned, unsigned, bool> friend class ZmHeap__;
  ZmHeap_Init();
};

template <auto ID_, unsigned Size_, unsigned Alignment_, bool Sharded_>
class ZmHeap__ {
public:
  static constexpr auto HeapID = ID_;
  enum { AllocSize = ZmHeapAllocSize<Size_>::N };
  enum { Alignment = Alignment_ };
  enum { Sharded = Sharded_ };

private:
  using Cache = ZmHeapCacheT<HeapID, AllocSize, Alignment, Sharded>;

public:
  void *operator new(size_t) { return Cache::alloc(); }
  void *operator new(size_t, void *p) noexcept { return p; }
  void operator delete(void *p) noexcept {
    if (ZuUnlikely(!p)) return;
    Cache::free(p);
  }

private:
  static ZmHeap_Init<ZmHeap__>	m_init;
};

template <typename Heap>
ZmHeap_Init<Heap>::ZmHeap_Init() { delete new Heap(); }

template <auto ID, unsigned Size, unsigned Alignment, bool Sharded>
ZmHeap_Init<ZmHeap__<ID, Size, Alignment, Sharded>>
ZmHeap__<ID, Size, Alignment, Sharded>::m_init;

// sentinel heap ID used to disable ZmHeap
inline constexpr auto ZmHeapDisable() {
  return []() -> const char * { return nullptr; };
};

template <auto ID, unsigned Size, unsigned Alignment, bool Sharded>
struct ZmHeap_ { using T = ZmHeap__<ID, Size, Alignment, Sharded>; };
template <unsigned Size, unsigned Alignment, bool Sharded>
struct ZmHeap_<ZmHeapDisable(), Size, Alignment, Sharded> { using T = ZuNull; };
template <auto ID, typename T, bool Sharded = false>
using ZmHeap = typename ZmHeap_<ID, sizeof(T), alignof(T), Sharded>::T;

#include <zlib/ZmFn.hh>

template <auto ID, unsigned Size, unsigned Alignment, bool Sharded>
inline void ZmHeapCacheT<ID, Size, Alignment, Sharded>::stats()
{
  // aggregate heap cache statistics
  TLS::all([](ZmHeapCacheT *this_) { this_->m_cache->stats(this_->m_stats); });
}

#endif /* ZmHeap_HH */
