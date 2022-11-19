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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// recycling block allocator with affinitized cache (free list) and statistics

#ifndef ZmHeap_HPP
#define ZmHeap_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <new>

#include <zlib/ZuTuple.hpp>
#include <zlib/ZuPrint.hpp>
#include <zlib/ZuSwitch.hpp>

#include <zlib/ZmPlatform.hpp>
#include <zlib/ZmObject.hpp>
#include <zlib/ZmBitmap.hpp>
#include <zlib/ZmSpecific.hpp>
#include <zlib/ZmPLock.hpp>
#include <zlib/ZmGuard.hpp>
#include <zlib/ZmFn_.hpp>	// avoid circular dependency

#if defined(ZDEBUG) && !defined(ZmHeap_DEBUG)
#define ZmHeap_DEBUG
#endif

class ZmHeapMgr;
class ZmHeapMgr_;
class ZmHeapCache;
template <typename ID, unsigned Size> class ZmHeap;
template <typename ID, unsigned Size> class ZmHeapCacheT;

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

// cache (LIFO free list) of fixed-size blocks; one per CPU set / NUMA node
class ZmAPI ZmHeapCache : public ZmObject {
friend ZmHeapMgr;
friend ZmHeapMgr_;
template <class, unsigned> friend class ZmHeap;
template <class, unsigned> friend class ZmHeapCacheT;

  enum { CacheLineSize = Zm::CacheLineSize };

  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;

  using StatsFn = ZmFn<const ZmHeapStats &>;
  using AllStatsFn = ZmFn<StatsFn>;

  static const char *IDAxor(const ZmHeapCache *c) {
    return c->info().id;
  }
  using IDSize = ZuPair<const char *, unsigned>;
  static IDSize IDSizeAxor(const ZmHeapCache *c) {
    return {c->info().id, c->info().size};
  }
  using IDPartSize = ZuTuple<const char *, unsigned, unsigned>;
  static IDPartSize IDPartSizeAxor(const ZmHeapCache *c) {
    return {c->info().id, c->info().partition, c->info().size};
  }

  void *operator new(size_t s);
  void *operator new(size_t s, void *p);
public:
  void operator delete(void *p);

private:
  ZmHeapCache(
      const char *id, unsigned size, unsigned partition, bool sharded,
      const ZmHeapConfig &config,
      ZmHeapCache *next, AllStatsFn allStatsFn);

public:
  ~ZmHeapCache();

  const ZmHeapInfo &info() const { return m_info; }
  const ZmHeapStats &stats() const { return m_stats; }

  void telemetry(ZmHeapTelemetry &data) const;

#ifdef ZmHeap_DEBUG
  typedef void (*TraceFn)(const char *, unsigned);
#endif

private:
  void init(const ZmHeapConfig &);
  void init_();
  void free_();

  void *alloc(ZmHeapStats &stats);
  void free(ZmHeapStats &stats, void *p);

  static void free_(ZmHeapCache *, void *p);

  // lock-free MPMC LIFO slist

  void *alloc_() {
    uintptr_t p;
  loop:
    p = m_head.load_();
    if (ZuUnlikely(!p)) return nullptr;
    if (ZuLikely(m_info.sharded)) { // sharded - no contention
      m_head.store_(*(uintptr_t *)p);
      return (void *)p;
    }
    if (ZuUnlikely(p & 1)) { ZmAtomic_acquire(); goto loop; }
    if (ZuUnlikely(m_head.cmpXch(p | 1, p) != p)) goto loop;
    m_head = ((ZmAtomic<uintptr_t> *)p)->load_();
    return (void *)p;
  }
  void free__(void *p) {
    uintptr_t n;
  loop:
    n = m_head.load_();
    if (n & 1) { ZmAtomic_acquire(); goto loop; }
    ((ZmAtomic<uintptr_t> *)p)->store_(n);
    if (m_head.cmpXch((uintptr_t)p, n) != n) goto loop;
  }
  void free__sharded(void *p) { // sharded - no contention
    *(uintptr_t *)p = m_head.load_();
    m_head.store_((uintptr_t)p);
  }

  void allStats() const;
  void histStats(const ZmHeapStats &stats) const;

  // cache, end, next are guarded by ZmHeapMgr

  ZmAtomic<uintptr_t>	m_head;		// free list (contended atomic)
  char			m__pad[CacheLineSize - sizeof(uintptr_t)];

  ZmHeapInfo		m_info;
  ZmHeapCache		*m_next;	// next in partition list
  AllStatsFn		m_allStatsFn;	// aggregates stats from TLS

  void			*m_cache = 0;	// bound memory region
  void			*m_end = 0;	// end of memory region

#ifdef ZmHeap_DEBUG
  TraceFn		m_traceAllocFn;
  TraceFn		m_traceFreeFn;
#endif

  using HistLock = ZmPLock;
  using HistGuard = ZmGuard<HistLock>;
  using HistReadGuard = ZmReadGuard<HistLock>;

  mutable HistLock	m_histLock;
    mutable ZmHeapStats	  m_histStats;	// historical stats from exited threads
  mutable ZmHeapStats	m_stats;	// aggregated on demand
};

class ZmAPI ZmHeapMgr {
friend ZmHeapCache;
template <class, unsigned> friend class ZmHeapCacheT; 

  template <class S> struct CSV_ {
    CSV_(S &stream) : m_stream(stream) { }
    void print() {
      m_stream <<
	"ID,size,partition,sharded,alignment,cacheSize,cpuset,"
	"cacheAllocs,heapAllocs,frees\n";
      ZmHeapMgr::all(ZmFn<ZmHeapCache *>::Member<&CSV_::print_>::fn(this));
    }
    void print_(ZmHeapCache *c) {
      ZmHeapTelemetry data;
      c->telemetry(data);
      m_stream <<
	data.id << ',' <<
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

  static void all(ZmFn<ZmHeapCache *> fn);

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
  using AllStatsFn = ZmHeapCache::AllStatsFn;

  static ZmHeapCache *cache(
      const char *id, unsigned size, bool sharded, AllStatsFn);
};

// derive ID from ZmHeapSharded to declare a sharded heap
struct ZmHeapSharded { };

template <typename ID, unsigned Size>
struct ZmCleanup<ZmHeapCacheT<ID, Size> > {
  enum { Level = ZmCleanupLevel::Heap };
};

// TLS heap cache, specific to ID+size; maintains TLS heap statistics
template <typename ID, unsigned Size>
class ZmHeapCacheT : public ZmObject {
friend ZmSpecificCtor<ZmHeapCacheT<ID, Size> >;
  using TLS = ZmSpecific<ZmHeapCacheT>;

  using StatsFn = ZmHeapCache::StatsFn;
  using AllStatsFn = ZmHeapCache::AllStatsFn;

public:
  // allStats uses ZmSpecific::all to iterate over all threads and
  // collect/aggregate statistics for each TLS instance
  static void allStats(StatsFn fn);

private:
  ZmHeapCacheT() :
    m_cache(ZmHeapMgr::cache(ID::id(), Size,
	  ZuConversion<ZmHeapSharded, ID>::Base,
	  AllStatsFn::Ptr<&allStats>::fn())), m_stats{} { }
public:
  ~ZmHeapCacheT() {
    m_cache->histStats(m_stats);
  }

  static ZmHeapCacheT *instance() { return TLS::instance(); }
  static void *alloc() {
    ZmHeapCacheT *self = instance();
    return self->m_cache->alloc(self->m_stats);
  }
  static void free(void *p) {
    ZmHeapCacheT *self = instance();
    self->m_cache->free(self->m_stats, p);
  }

private:
  ZmHeapCache	*m_cache;
  ZmHeapStats	m_stats;
};

// ZmHeap_Size returns a size that is minimum sizeof(uintptr_t),
// or the smallest power of 2 greater than the passed size yet smaller
// than the cache line size, or the size rounded up to the nearest multiple
// of the cache line size
template <unsigned Size_,
	  bool Small = (Size_ <= sizeof(uintptr_t)),
	  unsigned RShift = 0,
	  bool Big = (Size_ > (Zm::CacheLineSize>>RShift))>
  struct ZmHeap_Size;
template <unsigned Size_, unsigned RShift, bool Big> // smallest
struct ZmHeap_Size<Size_, true, RShift, Big> {
  enum { Size = sizeof(uintptr_t) };
};
template <unsigned Size_, unsigned RShift> // smaller
struct ZmHeap_Size<Size_, false, RShift, false> {
  enum { Size = ZmHeap_Size<Size_, false, RShift + 1>::Size };
};
template <unsigned Size_, unsigned RShift> // larger
struct ZmHeap_Size<Size_, false, RShift, true> {
  enum { CacheLineSize = Zm::CacheLineSize };
  enum { Size = (CacheLineSize>>(RShift - 1)) };
};
template <unsigned Size_>
struct ZmHeap_Size<Size_, false, 0, true> { // larger than cache line size
  enum { CacheLineSize = Zm::CacheLineSize };
  enum { Size = ((Size_ + CacheLineSize - 1) & ~(CacheLineSize - 1)) };
};

template <typename Heap> class ZmHeap_Init {
template <typename, unsigned> friend class ZmHeap;
  ZmHeap_Init();
};

template <typename ID_, unsigned Size_>
class ZmHeap {
  enum { Size = ZmHeap_Size<Size_>::Size };

public:
  using ID = ID_;
private:
  using Cache = ZmHeapCacheT<ID, Size>;

public:
  void *operator new(size_t) { return Cache::alloc(); }
  void *operator new(size_t, void *p) noexcept { return p; }
  void operator delete(void *p) noexcept {
    if (ZuUnlikely(!p)) return;
    Cache::free(p);
  }

private:
  static ZmHeap_Init<ZmHeap>	m_init;
};

template <typename Heap>
ZmHeap_Init<Heap>::ZmHeap_Init() { delete new Heap(); }

template <typename ID, unsigned Size_>
ZmHeap_Init<ZmHeap<ID, Size_> > ZmHeap<ID, Size_>::m_init;

template <unsigned Size> class ZmHeap<ZuNull, Size> { };

#include <zlib/ZmFn.hpp>

template <class ID, unsigned Size>
inline void ZmHeapCacheT<ID, Size>::allStats(StatsFn fn)
{
  TLS::all(ZmFn<ZmHeapCacheT *>::template Lambda<ZuNull>::fn(
	[&fn](ZmHeapCacheT *c) { fn(c->m_stats); }));
}

#endif /* ZmHeap_HPP */
