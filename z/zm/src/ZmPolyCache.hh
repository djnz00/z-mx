//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// LRU cache of ZuField objects (combination of ZmList and ZmPolyHash)

#ifndef ZmPolyCache_HH
#define ZmPolyCache_HH

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <zlib/ZuField.hh>

#include <zlib/ZmLockTraits.hh>
#include <zlib/ZmPLock.hh>
#include <zlib/ZmGuard.hh>
#include <zlib/ZmAlloc.hh>
#include <zlib/ZmList.hh>
#include <zlib/ZmPolyHash.hh>
#include <zlib/ZmBlock.hh>
#include <zlib/ZmCacheStats.hh>

// NTP defaults
struct ZmPolyCache_Defaults : public ZmPolyHash_Defaults {
  static const char *HeapID() { return "ZmPolyCache"; }
  static constexpr auto ID = HeapID;
  enum { Evict = 1 };
};

// most NTP parameters are identical to ZmPolyHash
template <typename Lock, typename NTP = ZmPolyCache_Defaults>
using ZmPolyCacheLock = ZmPolyHashLock<Lock, NTP>;
template <bool Shadow, typename NTP = ZmPolyCache_Defaults>
using ZmPolyCacheShadow = ZmPolyHashShadow<Shadow, NTP>;
template <auto HeapID, typename NTP = ZmPolyCache_Defaults>
using ZmPolyCacheHeapID = ZmPolyHashHeapID<HeapID, NTP>;
template <auto ID, typename NTP = ZmPolyCache_Defaults>
using ZmPolyCacheID = ZmPolyHashID<ID, NTP>;
template <bool Sharded, typename NTP = ZmPolyCache_Defaults>
using ZmPolyCacheSharded = ZmPolyHashSharded<Sharded, NTP>;

// ZmPolyCacheEvict - enable/disable eviction
template <bool Evict_, typename NTP = ZmPolyCache_Defaults>
struct ZmPolyCacheEvict : public NTP {
  enum { Evict = Evict_ };
};

template <typename T_, typename NTP = ZmPolyCache_Defaults>
class ZmPolyCache {
public:
  using T = T_;
  using Lock = typename NTP::Lock;
  enum { Evict = NTP::Evict };

private:
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;
  using LRUList = ZmList<T, ZmListNode<T, ZmListShadow<true>>>;
  struct LRUDisable { // LRU list is not needed if eviction is disabled
    using Node = T;
    Node *delNode(Node *node) { return node; }
    Node *shift() { return nullptr; }
    void pushNode(Node *) { }
  };
  using LRU = ZuIf<Evict, LRUList, LRUDisable>;
  using PolyHash =
    ZmPolyHash<typename LRU::Node,
      ZmPolyHashLock<ZmNoLock, NTP>>; // overrides NTP::Lock

public:
  static constexpr auto ID = PolyHash::ID;
  static constexpr auto HeapID = PolyHash::HeapID;

  using Node = typename PolyHash::Node;
  using NodeRef = typename PolyHash::NodeRef;
  using NodeMvRef = typename PolyHash::NodeMvRef;

private:
  using FindFn = ZmFn<void(Node *)>;
  using FindFnList = ZmList<FindFn>;
  // key IDs as a type list
  using KeyIDs = ZuSeqTL<ZuFieldKeyIDs<T>>;
  // key types, each a tuple
  template <int KeyID> using Key = ZuFieldKeyT<T, KeyID>;
  template <typename KeyID> using KeyT = ZuFieldKeyT<T, KeyID{}>;
  using Keys = ZuTypeMap<KeyT, KeyIDs>;
  // load hash tables, mapping keys to pending find() operations for each KeyID
  template <typename KeyID>
  using LoadHash = ZmHashKV<KeyT<KeyID>, FindFnList, ZmHashHeapID<HeapID>>;
  // hash table node type
  template <int KeyID>
  using LoadHashNode = typename LoadHash<ZuUnsigned<KeyID>>::Node;
  // type list of load hash tables
  using LoadHashTL = ZuTypeMap<LoadHash, KeyIDs>;
  // tuple of load hash tables
  using LoadHashes = ZuTypeApply<ZuTuple, ZuTypeMap<ZmRef, LoadHashTL>>;

public:
  ZmPolyCache(ZmHashParams params = ZmHashParams{ID()}) : m_hash{params} {
    m_size = m_hash.size();
    ZuUnroll::all<KeyIDs>([this, &params]<typename KeyID>() {
      m_loadHashes.template p<KeyID{}>(new ZuType<KeyID{}, LoadHashTL>{params});
    });
  }

  ZmPolyCache(ZmPolyCache &) = delete;
  ZmPolyCache &operator =(ZmPolyCache &) = delete;
  // the move operators are intentionally unlocked
  // - these are intended for use exclusively in initialization
  ZmPolyCache(ZmPolyCache &&c) :
    m_size{c.m_size},
    m_hash{ZuMv(c.m_hash)},
    m_lru{ZuMv(c.m_lru)},
    m_loadHashes{ZuMv(c.m_loadHashes)},
    m_loads{c.m_loads},
    m_misses{c.m_misses},
    m_evictions{c.m_evictions}
  {
    c.m_size = 0;
    c.m_hash = PolyHash{};
    c.m_lru = LRU{};
    c.m_loadHashes = LoadHashes{};
    c.m_loads = c.m_misses = c.m_evictions = 0;
  }
  ZmPolyCache &operator =(ZmPolyCache &&c) {
    this->~ZmPolyCache();
    new (this) ZmPolyCache{ZuMv(c)};
    return *this;
  }

  unsigned size() const { return m_size; }

  using Stats = ZmCacheStats;

private:
  void stats_(Stats &r) const {
    r.size = m_hash.size();
    r.count = m_hash.count_();
    r.loads = m_loads;
    r.misses = m_misses;
    r.evictions = m_evictions;
  }
public:
  template <bool Reset = false>
  ZuIfT<!Reset> stats(Stats &r) const {
    ReadGuard guard{m_lock};
    stats_(r);
  }
  template <bool Reset = false>
  ZuIfT<Reset> stats(Stats &r) {
    Guard guard{m_lock};
    stats_(r);
    m_loads = m_misses = m_evictions = 0;
  }

  template <int KeyID = 0, bool UpdateLRU = Evict, typename Key>
  NodeRef find(const Key &key) {
    Guard guard{m_lock};
    ++m_loads;
    if (NodeRef node = find_<KeyID, UpdateLRU>(key)) return node;
    ++m_misses;
    return nullptr;
  }

  template <
    int KeyID = 0,
    bool UpdateLRU = Evict, bool Evict_ = Evict,
    typename FindFn_, typename LoadFn>
  void find(Key<KeyID> key, FindFn_ findFn, LoadFn loadFn) {
    Guard guard{m_lock};
    ++m_loads;
    if (NodeRef node = find_<KeyID, UpdateLRU>(key)) {
      findFn(ZuMv(node));
      return;
    }
    ++m_misses;
    const auto &loadHash = m_loadHashes.template p<KeyID>();
    LoadHashNode<KeyID> *load = loadHash->find(key);
    bool pending = load;
    if (!pending)
      loadHash->addNode(load = new LoadHashNode<KeyID>{key, FindFnList{}});
    load->val().push(FindFn{ZuMv(findFn)});
    guard.unlock();
    if (!pending)
      loadFn(ZuMv(key), [this, key](NodeRef node) {
	Guard guard{m_lock};
	if (node) add_(node);
	const auto &loadHash = m_loadHashes.template p<KeyID>();
	if (auto load = loadHash->del(key)) {
	  guard.unlock();
	  while (auto findFn = load->val().shiftVal()) findFn(node);
	}
      });
  }

  template <
    int KeyID = 0,
    bool UpdateLRU = Evict,
    typename FindFn_, typename LoadFn, typename EvictFn>
  void find(Key<KeyID> key, FindFn_ findFn, LoadFn loadFn, EvictFn evictFn) {
    Guard guard{m_lock};
    ++m_loads;
    if (NodeRef node = find_<KeyID, UpdateLRU>(key)) {
      findFn(ZuMv(node)); return;
    }
    ++m_misses;
    const auto &loadHash = m_loadHashes.template p<KeyID>();
    LoadHashNode<KeyID> *load = loadHash->find(key);
    bool pending = load;
    if (!pending)
      loadHash->addNode(load = new LoadHashNode<KeyID>{key, FindFnList{}});
    load->val().push(FindFn{ZuMv(findFn)});
    guard.unlock();
    if (!pending)
      loadFn(key, [this, key, evictFn = ZuMv(evictFn)](NodeRef node) {
	Guard guard{m_lock};
	if (node)
	  if (auto evicted = add_<true>(node)) evictFn(ZuMv(evicted));
	const auto &loadHash = m_loadHashes.template p<KeyID>();
	if (auto load = loadHash->del(key)) {
	  guard.unlock();
	  while (auto findFn = load->val().shiftVal()) findFn(node);
	}
      });
  }

  template <bool Evict_ = Evict>
  ZuIfT<!Evict_ || !Evict> add(NodeRef node) {
    Guard guard{m_lock};
    add_<false>(ZuMv(node));
  }

  template <bool Evict_ = Evict>
  ZuIfT<Evict_ && Evict, NodeRef> add(NodeRef node) {
    Guard guard{m_lock};
    return add_<true>(ZuMv(node));
  }

  template <bool Evict_ = Evict, typename EvictFn>
  ZuIfT<Evict_ && Evict, NodeRef> add(NodeRef node, EvictFn evictFn) {
    Guard guard{m_lock};
    if (auto evicted = add_<true>(ZuMv(node)))
      evictFn(ZuMv(evicted));
  }

  // update keys lambda - l(node)
  template <typename KeyIDs_ = ZuSeq<>, typename L>
  void update(Node *node, L l) const {
    Guard guard{m_lock};
    m_hash.template update<KeyIDs_>(node,
      [this, &guard, l = ZuMv(l)](Node *node) mutable {
	guard = Guard{};
	l(node);
	guard = Guard{m_lock};
      });
  }

  template <int KeyID, typename Key>
  NodeMvRef del(const Key &key) {
    Guard guard{m_lock};
    NodeMvRef node = m_hash.template del<KeyID>(key);
    if constexpr (Evict) if (node) m_lru.delNode(node);
    return node;
  }

  NodeMvRef delNode(Node *node_) {
    Guard guard{m_lock};
    NodeMvRef node = m_hash.delNode(node_);
    if constexpr (Evict) if (node) m_lru.delNode(node);
    return node;
  }

private:
  template <int KeyID, bool UpdateLRU = Evict, typename Key>
  NodeRef find_(const Key &key) {
    if (auto node = m_hash.template find<KeyID>(key)) {
      if constexpr (UpdateLRU && Evict)
	m_lru.pushNode(m_lru.delNode(node));
      return node;
    }
    return nullptr;
  }

  template <bool Evict_ = Evict>
  ZuIfT<!Evict_ || !Evict> add_(NodeRef node) {
    Node *nodePtr = node;
    m_hash.addNode(ZuMv(node));
    if constexpr (Evict) m_lru.pushNode(nodePtr);
  }

  template <bool Evict_ = Evict>
  ZuIfT<Evict_ && Evict, NodeMvRef> add_(NodeRef node) {
    Node *nodePtr = node;
    NodeMvRef evicted = nullptr;
    if (m_hash.count_() >= m_size) {
      auto evicted_ = m_lru.shift();
      if constexpr (ZuTraits<ZuDecay<decltype(evicted_)>>::IsPrimitive)
	evicted = static_cast<NodeMvRef>(evicted_);
      else
	evicted = NodeMvRef{ZuMv(evicted_)};
      if (evicted) {
	++m_evictions;
	m_hash.delNode(ZuMv(evicted));
      }
    }
    m_hash.add(ZuMv(node));
    m_lru.pushNode(nodePtr);
    return evicted;
  }

public:
  // all() iterates over the cache asynchronously
  template <typename L>
  void all(L l) const {
    m_lock.lock();
    const_cast<ZmPolyCache *>(this)->all_<false>(ZuMv(l));
  }

  // allSync() synchronously blocks
  template <typename L>
  void allSync(L l) const {
    m_lock.lock();
    const_cast<ZmPolyCache *>(this)->all_<true>(ZuMv(l));
  }

private:
  template <bool Sync, typename L>
  bool all_(L l) {
    unsigned n = m_hash.count_();
    auto buf = ZmAlloc(NodeRef, n);
    if (!buf) return false;
    all__<Sync>(ZuMv(l), buf, n);
    return true;
  }
  template <typename NodeRef>
  struct NodeRefFn {
    static void ctor(NodeRef *ptr, NodeRef ref) {
      new (ptr) NodeRef{ZuMv(ref)};
    }
    static void dtor(NodeRef &ref) { ref.~NodeRef(); }
  };
  template <typename Node>
  struct NodeRefFn<Node *> {
    using NodeRef = Node *;
    static void ctor(NodeRef *ptr, NodeRef ref) { *ptr = ref; }
    static constexpr void dtor(NodeRef &) { }
  };
  template <bool Sync, typename L>
  void all__(L l, NodeRef *buf, unsigned n) {
    using Fn = NodeRefFn<NodeRef>;
    {
      auto i = m_hash.iterator();
      unsigned j = 0;
      for (j = 0; j < n; j++) {
	Fn::ctor(&buf[j], i.iterate());
	if (ZuUnlikely(!buf[j])) { Fn::dtor(buf[j]); break; }
      }
      n = j;
    }
    m_lock.unlock();
    if constexpr (Sync)
      ZmBlock<>{}(n, [&l, buf](unsigned j, auto wake) {
	l(ZuMv(buf[j]), ZuMv(wake));
	Fn::dtor(buf[j]);
      });
    else
      for (unsigned j = 0; j < n; j++) {
	l(ZuMv(buf[j]));
	Fn::dtor(buf[j]);
      }
  }

private:
  unsigned		m_size;
 
  mutable Lock		m_lock;
    PolyHash		  m_hash;
    LRU			  m_lru;
    LoadHashes		  m_loadHashes;
    uint64_t		  m_loads = 0;
    uint64_t		  m_misses = 0;
    uint64_t		  m_evictions = 0;
};

#endif /* ZmPolyCache_HH */
