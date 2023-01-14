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

// LRU cache (combination of ZmList and ZmCache)

#ifndef ZmCache_HPP
#define ZmCache_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZmLockTraits.hpp>
#include <zlib/ZmRWLock.hpp>
#include <zlib/ZmGuard.hpp>
#include <zlib/ZmAlloc.hpp>
#include <zlib/ZmList.hpp>
#include <zlib/ZmHash.hpp>

// NTP defaults
struct ZmCache_Defaults : public ZmHash_Defaults {
  constexpr static auto KeyAxor = ZuDefaultAxor();
  constexpr static auto ValAxor = ZuDefaultAxor();
  enum { Evict = 1 };
  using Lock = ZmPRWLock;
};

// ZmCacheKey - key accessor
template <auto KeyAxor_, typename NTP = ZmCache_Defaults>
struct ZmCacheKey : public NTP {
  constexpr static auto KeyAxor = KeyAxor_;
};

// ZmCacheKeyVal - key and optional value accessors
template <auto KeyAxor_, auto ValAxor_, typename NTP = ZmCache_Defaults>
struct ZmCacheKeyVal : public NTP {
  constexpr static auto KeyAxor = KeyAxor_;
  constexpr static auto ValAxor = ValAxor_;
};

// ZmCacheEvict - enable/disable eviction
template <bool Evict_, typename NTP = ZmCache_Defaults>
struct ZmCacheEvict : public NTP {
  enum { Evict = Evict_ };
};

// ZmCacheLock - the lock type used
template <typename Lock_, typename NTP = ZmCache_Defaults>
struct ZmCacheLock : public NTP {
  using Lock = Lock_;
};

template <typename T, typename NTP = ZmCache_Defaults>
class ZmCache {
public:
  using Lock = typename NTP::Lock;
  enum { Evict = NTP::Evict };

private:
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;
  using LRUList = ZmList<T, ZmListNode<T, ZmListShadow<true>>>;
  struct LRUDisable { // LRU list is not needed if eviction is disabled
    using Node = T;
    Node *delNode(Node *node) { return node; }
    Node *shiftNode() { return nullptr; }
    void pushNode(Node *) { }
  };
  using LRU = ZuIf<Evict, LRUList, LRUDisable>;
  using Hash =
    ZmHash<typename LRU::Node,
      ZmHashNode<typename LRU::Node, NTP>>;

public:
  using Key = typename Hash::Key;
  using Node = typename Hash::Node;
  using NodeRef = typename Hash::NodeRef;

  ZmCache(uint32_t size) : m_size{size} {
    m_hash = new Hash{ZmHashParams{size}};
  }

  unsigned count_() const { return m_hash->count_(); }
  uint64_t loads() const { return m_loads; }
  uint64_t misses() const { return m_misses; }

  // intentional DRY violation below

  template <typename P, typename Add, bool Evict_ = Evict>
  ZuIfT<!Evict_, Node *> find(const P &key, Add add) {
    if constexpr (ZmLockTraits<Lock>::RWLock) {
      ReadGuard guard{m_lock};
      ++m_loads;
      if (Node *node = find_(key)) return node;
    }
    {
      Node *nodePtr;
      {
	Guard guard(m_lock);
	if constexpr (!ZmLockTraits<Lock>::RWLock) ++m_loads;
	if (Node *node = find_(key)) return node;
	++m_misses;
	NodeRef node = add(key);
	if (ZuUnlikely(!node)) return nullptr;
	nodePtr = node;
	add_(ZuMv(node));
      }
      return nodePtr;
    }
  }

  template <typename P, typename Add, bool Evict_ = Evict>
  ZuIfT<Evict_, Node *> find(const P &key, Add add) {
    if constexpr (ZmLockTraits<Lock>::RWLock) {
      ReadGuard guard{m_lock};
      ++m_loads;
      if (Node *node = find_(key)) return node;
    }
    {
      Node *nodePtr;
      {
	Guard guard(m_lock);
	if constexpr (!ZmLockTraits<Lock>::RWLock) ++m_loads;
	if (Node *node = find_(key)) return node;
	++m_misses;
	NodeRef node = add(key);
	if (ZuUnlikely(!node)) return nullptr;
	nodePtr = node;
	add_(ZuMv(node));
      }
      return nodePtr;
    }
  }

  template <typename P, typename Add, typename Evicted, bool Evict_ = Evict>
  ZuIfT<Evict_, Node *> find(const P &key, Add add, Evicted evicted) {
    if constexpr (ZmLockTraits<Lock>::RWLock) {
      ReadGuard guard{m_lock};
      ++m_loads;
      if (Node *node = find_(key)) return node;
    }
    {
      Node *nodePtr;
      NodeRef evictedNode;
      {
	Guard guard(m_lock);
	if constexpr (!ZmLockTraits<Lock>::RWLock) ++m_loads;
	if (Node *node = find_(key)) return node;
	++m_misses;
	NodeRef node = add(key);
	if (ZuUnlikely(!node)) return nullptr;
	nodePtr = node;
	evictedNode = add_(ZuMv(node));
      }
      if (evictedNode) evicted(ZuMv(evictedNode));
      return nodePtr;
    }
  }

private:
  template <typename P>
  Node *find_(const P &key) {
    if (Node *node = m_hash->findPtr(key)) {
      if constexpr (Evict) m_lru.pushNode(m_lru.delNode(node));
      return node;
    }
    return nullptr;
  }

  template <bool Evict_ = Evict>
  ZuIfT<!Evict_> add_(NodeRef node) {
    m_hash->addNode(ZuMv(node));
  }

  template <bool Evict_ = Evict>
  ZuIfT<Evict_, NodeRef> add_(NodeRef node) {
    Node *nodePtr = node;
    NodeRef evicted = nullptr;
    if (m_hash->count_() >= m_size)
      if (evicted = static_cast<NodeRef>(m_lru.shiftNode()))
	m_hash->delNode(evicted);
    m_hash->addNode(ZuMv(node));
    m_lru.pushNode(nodePtr);
    return evicted;
  }

public:
  // all() is const by default, but all<true>() empties the cache
  template <bool Delete = false, typename L>
  ZuIfT<!Delete> all(L l) const {
    m_lock.lock();
    const_cast<ZmCache *>(this)->all_<Delete>(ZuMv(l));
  }
  template <bool Delete, typename L>
  ZuIfT<Delete> all(L l) {
    m_lock.lock();
    all_<Delete>(ZuMv(l));
  }

private:
  template <bool Delete, typename L>
  bool all_(L l) {
    unsigned n = m_hash.count_();
    auto buf = ZmAlloc(NodeRef, n);
    if (!buf) return false;
    all__<Delete>(ZuMv(l), buf, n);
    return true;
  }
  template <bool Delete>
  ZuIfT<!Delete, decltype(ZuDeclVal<Hash &>().iterator())>
  allIterator() {
    return m_hash.iterator();
  }
  template <bool Delete>
  ZuIfT<Delete, decltype(ZuDeclVal<const Hash &>().readIterator())>
  allIterator() {
    return m_hash.readIterator();
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
    constexpr static void dtor(NodeRef &) { }
  };
  template <bool Delete, typename L>
  void all__(L l, NodeRef *buf, unsigned n) {
    using Fn = NodeRefFn<NodeRef>;
    {
      auto i = allIterator<Delete>();
      unsigned j = 0;
      for (j = 0; j < n; j++) {
	Fn::ctor(&buf[j], i.iterate());
	if (ZuUnlikely(!buf[j])) { Fn::dtor(buf[j]); break; }
	if constexpr (Delete) i.del();
      }
      n = j;
    }
    m_lock.unlock();
    for (unsigned j = 0; j < n; j++) {
      l(ZuMv(buf[j]));
      Fn::dtor(buf[j]);
    }
  }

private:
  unsigned		m_size;

  Lock			m_lock;
    ZmRef<Hash>		  m_hash;
    LRU			  m_lru;
    uint64_t		  m_loads = 0;
    uint64_t		  m_misses = 0;
};

template <typename P0, typename P1, typename NTP = ZmCache_Defaults>
using ZmCacheKV =
  ZmCache<ZuPair<P0, P1>,
    ZmCacheKeyVal<ZuPairAxor<0>(), ZuPairAxor<1>(), NTP>>;

#endif /* ZmCache_HPP */
