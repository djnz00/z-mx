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

// intrusive policy-based double-linked list

#ifndef ZmList_HPP
#define ZmList_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZuNull.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuConversion.hpp>
#include <zlib/ZuObject.hpp>

#include <zlib/ZmNoLock.hpp>
#include <zlib/ZmGuard.hpp>
#include <zlib/ZmAssert.hpp>
#include <zlib/ZmRef.hpp>
#include <zlib/ZmHeap.hpp>
#include <zlib/ZmNode.hpp>
#include <zlib/ZmNodeFn.hpp>

// NTP (named template parameters) convention:
//
// ZmList<ZtString,				// list of ZtStrings
//   ZmListLock<ZmRWLock,			// lock with R/W lock
//     ZmListCmp<ZtICmp> > >			// case-insensitive comparison

// NTP defaults
struct ZmList_Defaults {
  constexpr static auto KeyAxor = ZuDefaultAxor();
  constexpr static auto ValAxor = ZuDefaultAxor();
  template <typename T> using CmpT = ZuCmp<T>;
  template <typename T> using ValCmpT = ZuCmp<T>;
  using Lock = ZmNoLock;
  using Node = ZuNull;
  enum { Shadow = 0 };
  static const char *HeapID() { return "ZmList"; }
  enum { Sharded = 0 };
};

// ZmListKey - key accessor
template <auto KeyAxor_, typename NTP = ZmList_Defaults>
struct ZmListKey : public NTP {
  constexpr static auto KeyAxor = KeyAxor_;
};

// ZmListKeyVal - key and optional value accessors
template <
  auto KeyAxor_, auto ValAxor_,
  typename NTP = ZmList_Defaults>
struct ZmListKeyVal : public NTP {
  constexpr static auto KeyAxor = KeyAxor_;
  constexpr static auto ValAxor = ValAxor_;
};

// ZmListCmp - the comparator
template <template <typename> typename Cmp_, typename NTP = ZmList_Defaults>
struct ZmListCmp : public NTP {
  template <typename T> using CmpT = Cmp_<T>;
};

// ZmListLock - the lock type used (ZmRWLock will permit concurrent reads)
template <class Lock_, class NTP = ZmList_Defaults>
struct ZmListLock : public NTP {
  using Lock = Lock_;
};

// ZmListNode - the base type for nodes
template <typename Node_, typename NTP = ZmList_Defaults>
struct ZmListNode : public NTP {
  using Node = Node_;
};

// ZmListShadow - shadow nodes, do not manage ownership
template <bool Shadow_, typename NTP = ZmList_Defaults>
struct ZmListShadow : public NTP {
  enum { Shadow = Shadow_ };
  constexpr static auto HeapID = ZmHeapDisable();
};

// ZmListHeapID - the heap ID
template <auto HeapID_, class NTP = ZmList_Defaults>
struct ZmListHeapID : public NTP {
  constexpr static auto HeapID = HeapID_;
};

// ZmListSharded - heap is sharded
template <bool Sharded_, typename NTP = ZmList_Defaults>
struct ZmListSharded : public NTP {
  enum { Sharded = Sharded_ };
};

template <typename T_, class NTP = ZmList_Defaults>
class ZmList : public ZmNodeFn<NTP::Shadow, typename NTP::Node> {
public:
  using T = T_;
  constexpr static auto KeyAxor = NTP::KeyAxor;
  constexpr static auto ValAxor = NTP::ValAxor;
  using Key = ZuDecay<decltype(KeyAxor(ZuDeclVal<const T &>()))>;
  using Val = ZuDecay<decltype(ValAxor(ZuDeclVal<const T &>()))>;
  using Cmp = typename NTP::template CmpT<T>;
  using ValCmp = typename NTP::template ValCmpT<Val>;
  using Lock = typename NTP::Lock;
  using NodeBase = typename NTP::Node;
  enum { Shadow = NTP::Shadow };
  constexpr static auto HeapID = NTP::HeapID;
  enum { Sharded = NTP::Sharded };

private:
  using NodeFn = ZmNodeFn<Shadow, NodeBase>;

  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

private:
  template <typename I> class Iterator_;
template <typename> friend class Iterator_;

private:
  // node in a list

  template <typename Node>
  class NodeExt_ {
  friend ZmList<T, NTP>;
  template <typename> friend class ZmList<T, NTP>::Iterator_;

    Node	*next = nullptr, *prev = nullptr;
  };

public:
  using Node = ZmNode<T, KeyAxor, ValAxor, NodeBase, NodeExt_, HeapID, Sharded>;
  using NodeExt = NodeExt_<Node>;
  using NodeRef = typename NodeFn::template Ref<Node>;
  using NodeMvRef = typename NodeFn::template MvRef<Node>;
  using NodePtr = Node *;

private:
  using NodeFn::nodeRef;
  using NodeFn::nodeDeref;
  using NodeFn::nodeDelete;
  using NodeFn::nodeAcquire;

  static const Key &key(Node *node) {
    if (ZuLikely(node)) return node->Node::key();
    return ZuNullRef<T, Cmp>();
  }
  static Key keyMv(NodeMvRef node) {
    if (ZuLikely(node)) return ZuMv(*node).Node::key();
    return ZuNullRef<T, Cmp>();
  }
  static const Val &val(Node *node) {
    if (ZuLikely(node)) return node->Node::val();
    return ZuNullRef<Val, ValCmp>();
  }
  static Val valMv(NodeMvRef node) {
    if (ZuLikely(node)) return ZuMv(*node).Node::val();
    return ZuNullRef<Val, ValCmp>();
  }

private:
  template <typename I> struct Iterator__ {
    decltype(auto) iterateKey() {
      return key(static_cast<I *>(this)->iterate());
    }
    decltype(auto) iterateVal() {
      return val(static_cast<I *>(this)->iterate());
    }
  };

  template <typename I> class Iterator_ : public Iterator__<I> {
    Iterator_(const Iterator_ &) = delete;
    Iterator_ &operator =(const Iterator_ &) = delete;

    using List = ZmList<T, NTP>;
  friend List;

  protected:
    Iterator_(Iterator_ &&) = default;
    Iterator_ &operator =(Iterator_ &&) = default;

    Iterator_(List &list) : m_list(list) { }

  public:
    void reset() {
      m_list.startIterate(static_cast<I &>(*this));
    }
    Node *iterate() {
      return m_list.iterate(static_cast<I &>(*this));
    }

    unsigned count() const { return m_list.count_(); }

  protected:
    List	&m_list;
    Node	*m_node;
  };

public:
  class Iterator : public Iterator_<Iterator> {
    Iterator(const Iterator &) = delete;
    Iterator &operator =(const Iterator &) = delete;

    using List = ZmList<T, NTP>;
  friend List;
    using Base = Iterator_<Iterator>;

    using Base::m_list;

  public:
    Iterator(Iterator &&) = default;
    Iterator &operator =(Iterator &&) = default;

    Iterator(List &list) : Base{list} { list.startIterate(*this); }
    ~Iterator() { m_list.endIterate(); }

    template <typename P>
    NodeRef push(P &&data) {
      return m_list.pushIterate(*this, ZuFwd<P>(data));
    }
    template <typename P0, typename P1>
    NodeRef push(P0 &&p0, P1 &&p1) {
      return push(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
    }
    void pushNode(Node *node) {
      m_list.pushIterateNode(*this, node);
    }

    template <typename P>
    NodeRef unshift(P &&data) {
      return m_list.unshiftIterate(*this, ZuFwd<P>(data));
    }
    template <typename P0, typename P1>
    NodeRef unshift(P0 &&p0, P1 &&p1) {
      return unshift(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
    }
    void unshiftNode(Node *node) {
      m_list.unshiftIterateNode(*this, node);
    }

    NodeMvRef del() { return this->m_list.delIterate(*this); }
  };

  class ReadIterator : public Iterator_<ReadIterator> {
    ReadIterator(const ReadIterator &) = delete;
    ReadIterator &operator =(const ReadIterator &) = delete;

    using List = ZmList<T, NTP>;
  friend List;
    using Base = Iterator_<ReadIterator>;

    using Base::m_list;

  public:
    ReadIterator(ReadIterator &&) = default;
    ReadIterator &operator =(ReadIterator &&) = default;

    ReadIterator(const List &list) : Base{const_cast<List &>(list)} {
      const_cast<List &>(list).startIterate(*this);
    }
    ~ReadIterator() { m_list.endIterate(); }
  };

  ZmList() = default;

  ZmList(const ZmList &) = delete;
  ZmList &operator =(const ZmList &) = delete;

  ZmList(ZmList &&list) {
    Guard guard(list.m_lock);
    m_head = list.m_head, m_tail = list.m_tail;
    m_count = list.m_count;
    list.m_head = list.m_tail = nullptr;
    list.m_count = 0;
  }
  ZmList &operator =(ZmList &&list) {
    unsigned count;
    Node *head, *tail;
    {
      Guard guard(list.m_lock);
      head = list.m_head, tail = list.m_tail;
      count = list.m_count;
      list.m_head = list.m_tail = nullptr;
      list.m_count = 0;
    }
    {
      Guard guard(m_lock);
      clean_();
      m_head = head, m_tail = tail;
      m_count = count;
    }
    return *this;
  }
  ZmList &operator +=(ZmList &&list) {
    unsigned count;
    Node *head, *tail;
    {
      Guard guard(list.m_lock);
      head = list.m_head, tail = list.m_tail;
      count = list.m_count;
      list.m_head = list.m_tail = nullptr;
      list.m_count = 0;
    }
    if (head) {
      Guard guard(m_lock);
      if (m_tail) {
	m_tail->NodeExt::next = head;
	head->NodeExt::prev = m_tail;
	m_tail = tail;
	m_count += count;
      } else {
	m_head = head, m_tail = tail;
	m_count = count;
      }
    }
    return *this;
  }

  ~ZmList() { clean_(); }

  // unsigned count() const { ReadGuard guard(m_lock); return m_count; }
  bool empty() const { ReadGuard guard(m_lock); return !m_count; }
  unsigned count_() const { return m_count; }
  bool empty_() const { return !m_count; }

  template <typename P> void add(P &&data) { push(ZuFwd<P>(data)); }
  template <typename P> void addNode(P &&node) { pushNode(ZuFwd<P>(node)); }

private:
  template <typename U, typename V = Key>
  struct IsKey {
    enum { OK = ZuConversion<U, V>::Exists };
  };
  template <typename U, typename R = void>
  using MatchKey = ZuIfT<IsKey<U>::OK, R>;
  template <typename U, typename V = T, bool = ZuConversion<NodeBase, V>::Is>
  struct IsData {
    enum { OK = !IsKey<U>::OK && ZuConversion<U, V>::Exists };
  };
  template <typename U, typename V>
  struct IsData<U, V, true> {
    enum { OK = 0 };
  };
  template <typename U, typename R = void>
  using MatchData = ZuIfT<IsData<U>::OK, R>;

  template <typename P>
  static auto matchKey(const P &key) {
    return [&key](const Node *node) -> bool {
      return Cmp::equals(node->Node::key(), key);
    };
  }
  template <typename P>
  static auto matchData(const P &data) {
    return [&data](const Node *node) -> bool {
      return node->Node::data() == data;
    };
  }

public:
  template <typename P>
  MatchKey<P, NodeRef> find(const P &key) {
    return find_(matchKey(key));
  }
  template <typename P>
  MatchData<P, NodeRef> find(const P &data) {
    return find_(matchData(data));
  }
  template <typename P0, typename P1>
  NodeRef find(P0 &&p0, P1 &&p1) {
    return find(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <typename P>
  MatchKey<P, Node *> findPtr(const P &key) {
    return find_(matchKey(key));
  }
  template <typename P>
  MatchData<P, Node *> findPtr(const P &data) {
    return find_(matchData(data));
  }

  template <typename P>
  MatchKey<P, Key> findKey(const P &key) {
    return key(find_(matchKey(key)));
  }
  template <typename P>
  MatchData<P, Key> findKey(const P &data) {
    return key(find_(matchData(data)));
  }
  template <typename P0, typename P1>
  Key findKey(P0 &&p0, P1 &&p1) {
    return findKey(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <typename P>
  MatchKey<P, Val> findVal(const P &key) {
    return val(find_(matchKey(key)));
  }
  template <typename P>
  MatchData<P, Val> findVal(const P &data) {
    return val(find_(matchData(data)));
  }
  template <typename P0, typename P1>
  Val findVal(P0 &&p0, P1 &&p1) {
    return findVal(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

private:
  template <typename Match>
  NodeRef find_(Match match) {
    Node *node;
    Guard guard(m_lock);
    if (!m_count) return nullptr;
    for (node = m_head; node && !match(node); node = node->NodeExt::next);
    return node;
  }

public:
  template <typename P>
  MatchKey<P, NodeRef> del(const P &key) { return del_(matchKey(key)); }
  template <typename P>
  MatchData<P, NodeRef> del(const P &data) { return del_(matchData(data)); }
  template <typename P0, typename P1>
  NodeMvRef del(P0 &&p0, P1 &&p1) {
    return del(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }
  NodeMvRef delNode(Node *node) {
    Guard guard(m_lock);
    if (ZuLikely(node)) del__(node);
    return nodeAcquire(node);
  }

  template <typename P>
  MatchKey<P, Key> delKey(const P &key) {
    return keyMv(del_(matchKey(key)));
  }
  template <typename P>
  MatchData<P, Key> delKey(const P &data) {
    return keyMv(del_(matchData(data)));
  }
  template <typename P0, typename P1>
  Key delKey(P0 &&p0, P1 &&p1) {
    return delKey(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }
  template <typename P>
  Key delNodeKey(Node *node) {
    Guard guard(m_lock);
    if (ZuLikely(node)) del__(node);
    return nodeAcquire(node);
  }

  template <typename P>
  MatchKey<P, Val> delVal(const P &key) {
    return valMv(del_(matchKey(key)));
  }
  template <typename P>
  MatchData<P, Val> delVal(const P &data) {
    return valMv(del_(matchData(data)));
  }
  template <typename P0, typename P1>
  Val delVal(P0 &&p0, P1 &&p1) {
    return delVal(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }
  template <typename P>
  Val delNodeVal(Node *node) {
    Guard guard(m_lock);
    if (ZuLikely(node)) del__(node);
    return nodeAcquire(node);
  }

private:
  template <typename Match>
  NodeMvRef del_(Match match) {
    Guard guard(m_lock);
    if (!m_count) return nullptr;
    Node *node;
    for (node = m_head; node && !match(node); node = node->NodeExt::next);
    if (ZuLikely(node)) del__(node);
    return nodeAcquire(node);
  }

public:
  template <typename P>
  NodeRef push(P &&data) {
    NodeRef node = new Node{ZuFwd<P>(data)};
    pushNode(node);
    return node;
  }
  template <typename P0, typename P1>
  NodeRef push(P0 &&p0, P1 &&p1) {
    return push(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }
  template <bool _ = !ZuConversion<NodeRef, Node *>::Same>
  ZuIfT<_> pushNode(const NodeRef &node_) { pushNode(node_.ptr()); }
  template <bool _ = !ZuConversion<NodeRef, Node *>::Same>
  ZuIfT<_> pushNode(NodeRef &&node_) {
    Node *node = node_.release();
    Guard guard(m_lock);
    pushNode_(node);
  }
  void pushNode(Node *node) {
    nodeRef(node);
    Guard guard(m_lock);
    pushNode_(node);
  }
private:
  void pushNode_(Node *node) {
    node->NodeExt::next = nullptr;
    node->NodeExt::prev = m_tail;
    if (!m_tail)
      m_head = node;
    else
      m_tail->NodeExt::next = node;
    m_tail = node;
    ++m_count;
  }
public:
  NodeMvRef pop() {
    Guard guard(m_lock);
    Node *node;

    if (!(node = m_tail)) return nullptr;

    if (!(m_tail = node->NodeExt::prev))
      m_head = nullptr;
    else
      m_tail->NodeExt::next = nullptr;

    NodeRef ret = node;

    nodeDeref(node);
    --m_count;

    return ret;
  }
  Key popKey() { return keyMv(pop()); }
  Val popVal() { return valMv(pop()); }
  NodeRef rpop() {
    Guard guard(m_lock);
    Node *node;

    if (!(node = m_tail)) return nullptr;

    if (!(m_tail = node->NodeExt::prev))
      m_tail = node;
    else {
      node->NodeExt::next(m_head);
      m_head->NodeExt::prev = node;
      (m_head = node)->NodeExt::prev = nullptr;
      m_tail->NodeExt::next = nullptr;
    }

    return node;
  }
  Key rpopKey() { return keyMv(rpop()); }
  Val rpopVal() { return valMv(rpop()); }

  template <typename P>
  NodeRef unshift(P &&data) {
    NodeRef node = new Node{ZuFwd<P>(data)};
    unshiftNode(node);
    return node;
  }
  template <typename P0, typename P1>
  NodeRef unshift(P0 &&p0, P1 &&p1) {
    return unshift(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }
  void unshiftNode(Node *node) {
    Guard guard(m_lock);

    nodeRef(node);
    node->NodeExt::prev = nullptr;
    node->NodeExt::next = m_head;
    if (!m_head)
      m_tail = node;
    else
      m_head->NodeExt::prev = node;
    m_head = node;
    ++m_count;
  }

  NodeMvRef shift() {
    Guard guard(m_lock);
    Node *node;

    if (!(node = m_head)) return nullptr;

    if (!(m_head = node->NodeExt::next))
      m_tail = nullptr;
    else
      m_head->NodeExt::prev = nullptr;

    NodeRef ret = node;

    nodeDeref(node);
    --m_count;

    return ret;
  }
  Key shiftKey() { return keyMv(shift()); }
  Val shiftVal() { return valMv(shift()); }
  NodeRef rshift() {
    Guard guard(m_lock);
    Node *node;

    if (!(node = m_head)) return nullptr;

    if (!(m_head = node->NodeExt::next))
      m_head = node;
    else {
      node->NodeExt::prev = m_tail;
      m_tail->NodeExt::next = node;
      (m_tail = node)->NodeExt::next = nullptr;
      m_head->NodeExt::prev = nullptr;
    }

    return node;
  }
  Key rshiftKey() { return keyMv(rshift()); }
  Val rshiftVal() { return valMv(rshift()); }

  T head() const {
    ReadGuard guard(m_lock);
    if (ZuUnlikely(!m_head)) return T{};
    return m_head->Node::data();
  }
  NodeRef headNode() const {
    ReadGuard guard(m_lock);
    return m_head;
  }
  T tail() const {
    ReadGuard guard(m_lock);
    if (ZuUnlikely(!m_tail)) return T{};
    return m_tail->Node::data();
  }
  NodeRef tailNode() const {
    ReadGuard guard(m_lock);
    return m_tail;
  }

  void clean() {
    Guard guard(m_lock);
    clean_();
    m_head = m_tail = nullptr;
    m_count = 0;
  }

  auto iterator() { return Iterator{*this}; }
  auto readIterator() const { return ReadIterator{*this}; }

protected:
  template <typename I>
  void startIterate(I &iterator) {
    m_lock.lock();
    iterator.m_node = nullptr;
  }
  template <typename I>
  Node *iterate(I &iterator) {
    Node *node = iterator.m_node;

    if (!node)
      node = m_head;
    else
      node = node->NodeExt::next;

    if (!node) return nullptr;

    return iterator.m_node = node;
  }
  template <typename I, typename P>
  NodeRef pushIterate(I &iterator, P &&data) {
    pushIterateNode(iterator, new Node{ZuFwd<P>(data)});
  }
  template <typename I>
  void pushIterateNode(I &iterator, Node *node) {
    Node *prevNode = iterator.m_node;

    if (!prevNode) { push(node); return; }

    nodeRef(node);
    if (Node *nextNode = prevNode->NodeExt::next) {
      node->NodeExt::next = nextNode;
      nextNode->NodeExt::prev = node;
    } else {
      m_tail = node;
      node->NodeExt::next = nullptr;
    }
    node->NodeExt::prev = prevNode;
    prevNode->NodeExt::next = node;
    ++m_count;
  }

  template <typename I, typename P>
  NodeRef unshiftIterate(I &iterator, P &&data) {
    unshiftIterateNode(iterator, new Node{ZuFwd<P>(data)});
  }
  template <typename I>
  void unshiftIterateNode(I &iterator, Node *node) {
    Node *nextNode = iterator.m_node;

    if (!nextNode) { unshift(node); return; }

    nodeRef(node);
    if (Node *prevNode = nextNode->NodeExt::prev) {
      node->NodeExt::prev = prevNode;
      prevNode->NodeExt::next = node;
    } else {
      m_head = node;
      node->NodeExt::prev = nullptr;
    }
    node->NodeExt::next = nextNode;
    nextNode->NodeExt::prev(node);
    ++m_count;
  }

  template <typename I>
  NodeMvRef delIterate(I &iterator) {
    if (!m_count) return nullptr;

    Node *node = iterator.m_node;

    if (ZuLikely(node)) {
      iterator.m_node = node->NodeExt::prev;
      del__(node);
    }

    return nodeAcquire(node);
  }

  void endIterate() {
    m_lock.unlock();
  }

  void del__(Node *node) {
    Node *prevNode = node->NodeExt::prev;
    Node *nextNode = node->NodeExt::next;

    ZmAssert(prevNode || nextNode || (m_head == node && m_tail == node));

    if (!prevNode)
      m_head = nextNode;
    else
      prevNode->NodeExt::next = nextNode;

    if (!nextNode)
      m_tail = prevNode;
    else
      nextNode->NodeExt::prev = prevNode;

    --m_count;
    
    node->NodeExt::next = node->NodeExt::prev = nullptr;
  }

  void clean_() {
    if (!m_count) return;

    Node *node = m_head, *prevNode;

    while (prevNode = node) {
      node = prevNode->NodeExt::next;
      nodeDeref(prevNode);
      nodeDelete(prevNode);
    }
  }

  Lock		m_lock;
    unsigned	  m_count = 0;
    Node	  *m_head = nullptr;
    Node	  *m_tail = nullptr;
};

template <typename P0, typename P1, typename NTP = ZmList_Defaults>
using ZmListKV =
  ZmList<ZuPair<P0, P1>,
    ZmListKeyVal<ZuPairAxor<0>(), ZuPairAxor<1>(), NTP>>;

#endif /* ZmList_HPP */
