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

#include <zlib/ZmLock.hpp>
#include <zlib/ZmGuard.hpp>
#include <zlib/ZmObject.hpp>
#include <zlib/ZmAssert.hpp>
#include <zlib/ZmRef.hpp>
#include <zlib/ZmHeap.hpp>
#include <zlib/ZmNode.hpp>

// NTP (named template parameters) convention:
//
// ZmList<ZtString,				// list of ZtStrings
//   ZmListLock<ZmRWLock,			// lock with R/W lock
//     ZmListCmp<ZtICmp> > >			// case-insensitive comparison

struct ZmList_Defaults {
  using KeyAxor = ZuDefaultAxor;
  using ValAxor = ZuDefaultAxor;
  template <typename T> using CmpT = ZuCmp<T>;
  template <typename T> using ValCmpT = ZuCmp<T>;
  enum { NodeDerive = 0 };
  using Lock = ZmLock;
  using Object = ZmObject;
  struct HeapID { static constexpr const char *id() { return "ZmList"; } };
};

// ZmListKey - key accessor
template <typename KeyAxor_, typename NTP = ZmList_Defaults>
struct ZmListKey : public NTP {
  using KeyAxor = KeyAxor_;
};

// ZmListKeyVal - key and optional value accessors
template <
  typename KeyAxor_, typename ValAxor_,
  typename NTP = ZmList_Defaults>
struct ZmListKeyVal : public NTP {
  using KeyAxor = KeyAxor_;
  using ValAxor = ValAxor_;
};

// ZmListCmp - the comparator
template <template <typename> typename Cmp_, typename NTP = ZmList_Defaults>
struct ZmListCmp : public NTP {
  template <typename T> using CmpT = Cmp_<T>;
};

// ZmListNodeDerive - derive ZmList::Node from T instead of containing it
template <bool NodeDerive_, typename NTP = ZmList_Defaults>
struct ZmListNodeDerive : public NTP {
  enum { NodeDerive = NodeDerive_ };
};

// ZmListLock - the lock type used (ZmRWLock will permit concurrent reads)
template <class Lock_, class NTP = ZmList_Defaults>
struct ZmListLock : public NTP {
  using Lock = Lock_;
};

// ZmListObject - the reference-counted object type used
template <class Object_, class NTP = ZmList_Defaults>
struct ZmListObject : public NTP {
  using Object = Object_;
};

// ZmListHeapID - the heap ID
template <class HeapID_, class NTP = ZmList_Defaults>
struct ZmListHeapID : public NTP {
  using HeapID = HeapID_;
};

template <typename T_, class NTP = ZmList_Defaults>
class ZmList : public ZmNodePolicy<typename NTP::Object> {
  using NodePolicy = ZmNodePolicy<typename NTP::Object>;

public:
  using T = T_;
  using KeyAxor = typename NTP::KeyAxor;
  using ValAxor = typename NTP::ValAxor;
  using Key = ZuDecay<decltype(KeyAxor::get(ZuDeclVal<const T &>()))>;
  using Val = ZuDecay<decltype(ValAxor::get(ZuDeclVal<const T &>()))>;
  using Cmp = typename NTP::template CmpT<Key>;
  using ValCmp = typename NTP::template ValCmpT<Val>;
  enum { NodeDerive = NTP::NodeDerive };
  using Lock = typename NTP::Lock;
  using Object = typename NodePolicy::Object;
  using HeapID = typename NTP::HeapID;

  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

protected:
  class Iterator_;
friend Iterator_;

public:
  // node in a list

  struct NullObject { }; // deconflict with ZuNull
  template <typename Node, typename Heap, bool NodeDerive> class NodeFn :
      public ZuIf<
	ZuConversion<ZuNull, Object>::Is ||
	ZuConversion<ZuShadow, Object>::Is ||
	(NodeDerive && ZuConversion<Object, T>::Is),
	NullObject, Object>,
      public Heap {
    NodeFn(const NodeFn &);
    NodeFn &operator =(const NodeFn &);	// prevent mis-use

  friend ZmList<T, NTP>;
  friend ZmList<T, NTP>::Iterator_;

  protected:
    NodeFn() { init(); }

  private:
    void init() { next = prev = nullptr; }

    Node	*next, *prev;
  };

  template <typename Heap>
  using Node_ = ZmNode<T, KeyAxor, ValAxor, Heap, NodeDerive, NodeFn>;
  struct NullHeap { }; // deconflict with ZuNull
  using NodeHeap = ZmHeap<HeapID, sizeof(Node_<NullHeap>)>;
  using Node = Node_<NodeHeap>;
  using Fn = typename Node::Fn;

  using NodeRef = typename NodePolicy::template Ref<Node>;

private:
  using NodePolicy::nodeRef;
  using NodePolicy::nodeDeref;
  using NodePolicy::nodeDelete;
  using NodePolicy::nodeAcquire;

  static const Key &key(Node *node) {
    if (ZuLikely(node)) return node->Node::key();
    return Cmp::null();
  }
  static Key keyMv(NodeRef &&node) {
    if (ZuLikely(node)) {
      Key key = ZuMv(*node).Node::key();
      nodeDelete(node);
      return key;
    }
    return Cmp::null();
  }
  static const Val &val(Node *node) {
    if (ZuLikely(node)) return node->Node::val();
    return ValCmp::null();
  }
  static Val valMv(NodeRef &&node) {
    if (ZuLikely(node)) {
      Val val = ZuMv(*node).Node::val();
      nodeDelete(node);
      return val;
    }
    return ValCmp::null();
  }

protected:
  class Iterator_ {			// iterator
    Iterator_(const Iterator_ &) = delete;
    Iterator_ &operator =(const Iterator_ &) = delete;

    using List = ZmList<T, NTP>;
  friend List;

  protected:
    Iterator_(Iterator_ &&) = default;
    Iterator_ &operator =(Iterator_ &&) = default;

    Iterator_(List &list) : m_list(list) {
      list.startIterate(*this);
    }

  public:
    void reset() { m_list.startIterate(*this); }

    Node *iterateNode() { return m_list.iterate(*this); }

    const T &iterate() {
      Node *node = m_list.iterate(*this);
      if (ZuLikely(node)) return node->Node::data();
      return Cmp::null();
    }

    unsigned count() const { return m_list.count_(); }

  protected:
    List	&m_list;
    Node	*m_node;
  };

public:
  class Iterator;
friend Iterator;
  class Iterator : private Guard, public Iterator_ {
    Iterator(const Iterator &) = delete;
    Iterator &operator =(const Iterator &) = delete;

    using List = ZmList<T, NTP>;

  public:
    Iterator(Iterator &&) = default;
    Iterator &operator =(Iterator &&) = default;

    Iterator(List &list) : Guard(list.m_lock), Iterator_(list) { }

    template <typename P>
    NodeRef push(P &&data) {
      return this->m_list.pushIterate(*this, ZuFwd<P>(data));
    }
    void pushNode(Node *node) {
      this->m_list.pushIterateNode(*this, node);
    }

    template <typename P>
    NodeRef unshift(P &&data) {
      return this->m_list.unshiftIterate(*this, ZuFwd<P>(data));
    }
    void unshiftNode(Node *node) {
      this->m_list.unshiftIterateNode(*this, node);
    }

    NodeRef del() { return this->m_list.delIterate(*this); }
  };

  class ReadIterator;
friend ReadIterator;
  class ReadIterator : private ReadGuard, public Iterator_ {
    ReadIterator(const ReadIterator &) = delete;
    ReadIterator &operator =(const ReadIterator &) = delete;

    using List = ZmList<T, NTP>;

  public:
    ReadIterator(ReadIterator &&) = default;
    ReadIterator &operator =(ReadIterator &&) = default;

    ReadIterator(const List &list) :
      ReadGuard(list.m_lock), Iterator_(const_cast<List &>(list)) { }
  };

  ZmList() = default;

  ZmList(const ZmList &) = delete;
  ZmList &operator =(const ZmList &) = delete;

  ZmList(ZmList &&list) noexcept {
    Guard guard(list.m_lock);
    m_head = list.m_head, m_tail = list.m_tail;
    m_count = list.m_count;
    list.m_head = list.m_tail = nullptr;
    list.m_count = 0;
  }
  ZmList &operator =(ZmList &&list) noexcept {
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
  ZmList &operator +=(ZmList &&list) noexcept {
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
	m_tail->Fn::next = head;
	head->Fn::prev = m_tail;
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

  unsigned count() const { ReadGuard guard(m_lock); return m_count; }
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
  template <typename U, typename V = T, bool = NodeDerive>
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
    for (node = m_head; node && !match(node); node = node->Fn::next);
    return node;
  }

public:
  template <typename P>
  MatchKey<P, NodeRef> del(const P &key) { return del_(matchKey(key)); }
  template <typename P>
  MatchData<P, NodeRef> del(const P &data) { return del_(matchData(data)); }
  template <typename P0, typename P1>
  NodeRef del(P0 &&p0, P1 &&p1) {
    return del(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }
  NodeRef delNode(Node *node) {
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
  NodeRef del_(Match match) {
    Guard guard(m_lock);
    if (!m_count) return nullptr;
    Node *node;
    for (node = m_head; node && !match(node); node = node->Fn::next);
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
    node->Fn::next = nullptr;
    node->Fn::prev = m_tail;
    if (!m_tail)
      m_head = node;
    else
      m_tail->Fn::next = node;
    m_tail = node;
    ++m_count;
  }
public:
  NodeRef popNode() {
    Guard guard(m_lock);
    Node *node;

    if (!(node = m_tail)) return nullptr;

    if (!(m_tail = node->Fn::prev))
      m_head = nullptr;
    else
      m_tail->Fn::next = nullptr;

    NodeRef ret = node;

    nodeDeref(node);
    --m_count;

    return ret;
  }
  T pop() {
    NodeRef node = popNode();
    if (ZuUnlikely(!node)) return T{};
    return ZuMv(*node).Node::data();
  }
  NodeRef rpopNode() {
    Guard guard(m_lock);
    Node *node;

    if (!(node = m_tail)) return nullptr;

    if (!(m_tail = node->Fn::prev))
      m_tail = node;
    else {
      node->Fn::next(m_head);
      m_head->Fn::prev = node;
      (m_head = node)->Fn::prev = nullptr;
      m_tail->Fn::next = nullptr;
    }

    return node;
  }
  T rpop() {
    NodeRef node = rpopNode();
    if (ZuUnlikely(!node)) return T{};
    return node->Node::data();
  }
  template <typename P>
  NodeRef unshift(P &&data) {
    NodeRef node = new Node{ZuFwd<P>(data)};
    unshiftNode(node);
    return node;
  }
  void unshiftNode(Node *node) {
    Guard guard(m_lock);

    nodeRef(node);
    node->Fn::prev = nullptr;
    node->Fn::next = m_head;
    if (!m_head)
      m_tail = node;
    else
      m_head->Fn::prev = node;
    m_head = node;
    ++m_count;
  }
  NodeRef shiftNode() {
    Guard guard(m_lock);
    Node *node;

    if (!(node = m_head)) return nullptr;

    if (!(m_head = node->Fn::next))
      m_tail = nullptr;
    else
      m_head->Fn::prev = nullptr;

    NodeRef ret = node;

    nodeDeref(node);
    --m_count;

    return ret;
  }
  T shift() {
    NodeRef node = shiftNode();
    if (ZuUnlikely(!node)) return T{};
    T data = ZuMv(*node).Node::data();
    nodeDelete(node);
    return data;
  }
  NodeRef rshiftNode() {
    Guard guard(m_lock);
    Node *node;

    if (!(node = m_head)) return nullptr;

    if (!(m_head = node->Fn::next))
      m_head = node;
    else {
      node->Fn::prev = m_tail;
      m_tail->Fn::next = node;
      (m_tail = node)->Fn::next = nullptr;
      m_head->Fn::prev = nullptr;
    }

    return node;
  }
  T rshift() {
    NodeRef node = rshiftNode();
    if (ZuUnlikely(!node)) return T{};
    return node->Node::data();
  }

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
  }

  auto iterator() { return Iterator(*this); }
  auto readIterator() const { return ReadIterator(*this); }

protected:
  void startIterate(Iterator_ &iterator) {
    iterator.m_node = nullptr;
  }
  Node *iterate(Iterator_ &iterator) {
    Node *node = iterator.m_node;

    if (!node)
      node = m_head;
    else
      node = node->Fn::next;

    if (!node) return nullptr;

    return iterator.m_node = node;
  }
  template <typename P>
  NodeRef pushIterate(Iterator_ &iterator, P &&data) {
    pushIterateNode(iterator, new Node{ZuFwd<P>(data)});
  }
  void pushIterateNode(Iterator_ &iterator, Node *node) {
    Node *prevNode = iterator.m_node;

    if (!prevNode) { push(node); return; }

    nodeRef(node);
    if (Node *nextNode = prevNode->Fn::next) {
      node->Fn::next = nextNode;
      nextNode->Fn::prev = node;
    } else {
      m_tail = node;
      node->Fn::next = nullptr;
    }
    node->Fn::prev = prevNode;
    prevNode->Fn::next = node;
    ++m_count;
  }

  template <typename P>
  NodeRef unshiftIterate(Iterator_ &iterator, P &&data) {
    unshiftIterateNode(iterator, new Node{ZuFwd<P>(data)});
  }
  void unshiftIterateNode(Iterator_ &iterator, Node *node) {
    Node *nextNode = iterator.m_node;

    if (!nextNode) { unshift(node); return; }

    nodeRef(node);
    if (Node *prevNode = nextNode->Fn::prev) {
      node->Fn::prev = prevNode;
      prevNode->Fn::next = node;
    } else {
      m_head = node;
      node->Fn::prev = nullptr;
    }
    node->Fn::next = nextNode;
    nextNode->Fn::prev(node);
    ++m_count;
  }

  NodeRef delIterate(Iterator_ &iterator) {
    if (!m_count) return nullptr;

    Node *node = iterator.m_node;

    if (ZuLikely(node)) {
      iterator.m_node = node->Fn::prev;
      del__(node);
    }

    return nodeAcquire(node);
  }

  void del__(Node *node) {
    Node *prevNode = node->Fn::prev;
    Node *nextNode = node->Fn::next;

    ZmAssert(prevNode || nextNode || (m_head == node && m_tail == node));

    if (!prevNode)
      m_head = nextNode;
    else
      prevNode->Fn::next = nextNode;

    if (!nextNode)
      m_tail = prevNode;
    else
      nextNode->Fn::prev = prevNode;

    --m_count;
  }

  void clean_() {
    if (!m_count) return;

    Node *node = m_head, *prevNode;

    while (prevNode = node) {
      node = prevNode->Fn::next;
      nodeDeref(prevNode);
      nodeDelete(prevNode);
    }

    m_head = m_tail = nullptr;
    m_count = 0;
  }

  Lock		m_lock;
    unsigned	  m_count = 0;
    Node	  *m_head = nullptr;
    Node	  *m_tail = nullptr;
};

#endif /* ZmList_HPP */
