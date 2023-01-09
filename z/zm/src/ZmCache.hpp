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

#include <ZmGuard.hpp>
#include <ZmList.hpp>
#include <ZmHash.hpp>

// NTP defaults
struct ZmCache_Defaults : public ZmHash_Defaults {
  enum { Evict = 1 };
};

// ZmCacheEvict - enable/disable eviction
template <bool Evict_, typename NTP = ZmCache_Defaults>
struct ZmCacheEvict : public NTP {
  enum { Evict = Evict_ };
};

template <typename T, auto KeyAxor, typename NTP = ZmCache_Defaults>
class ZmCache {
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;
  enum { Evict = NTP::Evict };
  using LRUList = ZmList<T, ZmListNode<T, ZmListShadow<true>>>;
  struct LRUDisable { // LRU list is not needed if eviction is disabled
    using Node = T;
    Node *delNode(Node *node) { return node; }
    Node *shiftNode() { return nullptr; }
    void pushNode(Node *) { }
  };
  using LRU = ZuIf<Evict, LRUList, LRUDisable>;
  using Hash =
    ZmHash<LRU::Node,
      ZmHashNode<LRU::Node,
	ZmHashKey<KeyAxor, NTP>>>;
  using Key = typename Hash::Key;
  using Node = typename Hash::Node;
  using NodeRef = typename Hash::NodeRef;

public:
  ZmCache(uint32_t size) : m_size{size} {
    m_hash = new Hash{ZmHashParams{size}};
  }

  uint64_t loads() const { return m_loads; }
  uint64_t misses() const { return m_misses; }

  template <typename P>
  Node *find(const P &key) const {
    ReadGuard guard{m_lock};
    ++m_loads;
    if (Node *node = m_hash.findPtr(key)) {
      if constexpr (Evict) m_lru.pushNode(m_lru.delNode(node));
      return node;
    }
    ++m_misses;
    return nullptr;
  }

  template <bool Evict_ = Evict>
  ZuIfT<!Evict_> add(NodeRef node) {
    Guard guard{m_lock};
    m_hash->addNode(ZuMv(node));
  }

  template <bool Evict_ = Evict>
  ZuIfT<Evict_, NodeRef> add(NodeRef node) {
    auto ptr = node.ptr();
    NodeRef lru = nullptr;
    Guard guard{m_lock};
    if (m_hash.count_() >= m_size)
      if (lru = m_lru.shiftNode()) m_hash->delNode(lru);
    m_hash->addNode(ZuMv(node));
    m_lru.pushNode(ptr);
    return lru;
  }

public:
  // all() is const by default, but all<true>() empties the cache
  template <bool Delete = false, typename L>
  ZuIfT<!Delete> all(L l) const {
    m_lock.lock();
    const_cast<ZmCache *>(this)->all_<Delete>(ZuMv(map), ZuMv(reduce));
  }
  template <bool Delete, typename Map, typename Reduce>
  ZuIfT<Delete> all(L l) {
    m_lock.lock();
    all_<Delete>(ZuMv(map), ZuMv(reduce));
  }
private:
  template <bool Delete, typename L>
  void all_(L l, Reduce reduce) {
    unsigned n = m_hash.count_();
    auto buf = ZmAlloc(NodeRef, n);
    if (!buf) return false;
    all__<Delete>(ZuMv(l), buf, n);
  }
  template <bool Delete>
  ZuIfT<!Delete, decltype(m_hash.iterator())> allIterator() {
    return m_hash.iterator();
  }
  template <bool Delete>
  ZuIfT<Delete, decltype(m_hash.readIterator())> allIterator() {
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
    LRU			  m_lru;
    ZmRef<Hash>		  m_hash;
    mutable uint64_t	  m_loads = 0;
    mutable uint64_t	  m_misses = 0;
};

#endif /* ZmCache_HPP */
