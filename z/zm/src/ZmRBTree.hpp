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

// red/black tree (policy-based)

#ifndef ZmRBTree_HPP
#define ZmRBTree_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZuNull.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuConversion.hpp>

#include <zlib/ZmGuard.hpp>
#include <zlib/ZmLock.hpp>
#include <zlib/ZmObject.hpp>
#include <zlib/ZmRef.hpp>
#include <zlib/ZmHeap.hpp>
#include <zlib/ZmNode.hpp>

// uses NTP (named template parameters):
//
// ZmRBTreeKV<ZtString, ZtString,	// key, value pair of ZtStrings
//     ZmRBTreeValCmp<ZtICmp> >		// case-insensitive comparison

struct ZmRBTree_Defaults {
  using KeyAxor = ZuDefaultAxor;
  using ValAxor = ZuDefaultAxor;
  template <typename T> using CmpT = ZuCmp<T>;
  template <typename T> using ValCmpT = ZuCmp<T>;
  enum { NodeDerive = 0 };
  using Lock = ZmLock;
  using Object = ZmObject;
  struct HeapID { static constexpr const char *id() { return "ZmRBTree"; } };
  enum { Unique = 0 };
};

// ZmRBTreeKey - key accessor
template <typename KeyAxor_, typename NTP = ZmRBTree_Defaults>
struct ZmRBTreeKey : public NTP {
  using KeyAxor = KeyAxor_;
};

// ZmRBTreeKeyVal - key and optional value accessors
template <
  typename KeyAxor_, typename ValAxor_,
  typename NTP = ZmRBTree_Defaults>
struct ZmRBTreeKeyVal : public NTP {
  using KeyAxor = KeyAxor_;
  using ValAxor = ValAxor_;
};

// ZmRBTreeCmp - the comparator
template <
  template <typename> typename Cmp_,
  typename NTP = ZmRBTree_Defaults>
struct ZmRBTreeCmp : public NTP {
  template <typename T> using CmpT = Cmp_<T>;
};

// ZmRBTreeValCmp - the optional value comparator
template <
  template <typename> typename ValCmp_,
  typename NTP = ZmRBTree_Defaults>
struct ZmRBTreeValCmp : public NTP {
  template <typename T> using ValCmpT = ValCmp_<T>;
};

// ZmRBTreeNodeDerive - derive ZmRBTree::Node from T instead of containing it
template <bool NodeDerive_, typename NTP = ZmRBTree_Defaults>
struct ZmRBTreeNodeDerive : public NTP {
  enum { NodeDerive = NodeDerive_ };
};

// ZmRBTreeLock - the lock type used (ZmRWLock will permit concurrent reads)
template <class Lock_, class NTP = ZmRBTree_Defaults>
struct ZmRBTreeLock : public NTP {
  using Lock = Lock_;
};

// ZmRBTreeObject - the reference-counted object type used
template <class Object_, class NTP = ZmRBTree_Defaults>
struct ZmRBTreeObject : public NTP {
  using Object = Object_;
};

// ZmRBTreeHeapID - the heap ID
template <class HeapID_, class NTP = ZmRBTree_Defaults>
struct ZmRBTreeHeapID : public NTP {
  using HeapID = HeapID_;
};

// ZmRBTreeUnique - key is unique
template <bool Unique_, class NTP = ZmRBTree_Defaults>
struct ZmRBTreeUnique : public NTP {
  enum { Unique = Unique_ };
};

enum {
  ZmRBTreeEqual = 0,
  ZmRBTreeGreaterEqual = 1,
  ZmRBTreeLessEqual = -1,
  ZmRBTreeGreater = 2,
  ZmRBTreeLess = -2
};

template <typename Tree_, int Direction_>
class ZmRBTreeIterator_ { // red/black tree iterator
  ZmRBTreeIterator_(const ZmRBTreeIterator_ &) = delete;
  ZmRBTreeIterator_ &operator =(const ZmRBTreeIterator_ &) = delete;

friend Tree_;

public:
  using Tree = Tree_;
  enum { Direction = Direction_ };
  using T = typename Tree::T;
  using Key = typename Tree::Key;
  using Val = typename Tree::Val;
  using Cmp = typename Tree::Cmp;
  using ValCmp = typename Tree::ValCmp;
  using Node = typename Tree::Node;
  using NodeRef = typename Tree::NodeRef;

protected:
  ZmRBTreeIterator_(ZmRBTreeIterator_ &&) = default;
  ZmRBTreeIterator_ &operator =(ZmRBTreeIterator_ &&) = default;

  ZmRBTreeIterator_(Tree &tree) : m_tree(tree) {
    tree.startIterate(*this);
  }
  template <typename P>
  ZmRBTreeIterator_(Tree &tree, const P &key) : m_tree(tree) {
    tree.startIterate(*this, key);
  }

public:
  void reset() { m_tree.startIterate(*this); }
  template <typename P>
  void reset(const P &key) {
    m_tree.startIterate(*this, key);
  }

  Node *iterate() { return m_tree.iterate(*this); }

  const Key &iterateKey() {
    Node *node = m_tree.iterate(*this);
    if (ZuLikely(node)) return node->Node::key();
    return Cmp::null();
  }
  const Val &iterateVal() {
    Node *node = m_tree.iterate(*this);
    if (ZuLikely(node)) return node->Node::val();
    return ValCmp::null();
  }

  unsigned count() const { return m_tree.count_(); }

protected:
  Tree		&m_tree;
  Node		*m_node;
};

template <typename Tree_, int Direction_ = ZmRBTreeGreaterEqual>
class ZmRBTreeIterator :
    public Tree_::Guard,
    public ZmRBTreeIterator_<Tree_, Direction_> {
  ZmRBTreeIterator(const ZmRBTreeIterator &) = delete;
  ZmRBTreeIterator &operator =(const ZmRBTreeIterator &) = delete;

  using Tree = Tree_;
  enum { Direction = Direction_ };
  using Guard = typename Tree::Guard;
  using Node = typename Tree::Node;
  using NodeRef = typename Tree::NodeRef;

public:
  ZmRBTreeIterator(ZmRBTreeIterator &&) = default;
  ZmRBTreeIterator &operator =(ZmRBTreeIterator &&) = default;

  ZmRBTreeIterator(Tree &tree) :
      Guard{tree.lock()},
      ZmRBTreeIterator_<Tree, Direction>{tree} { }
  template <typename P>
  ZmRBTreeIterator(Tree &tree, const P &key) :
    Guard{tree.lock()},
    ZmRBTreeIterator_<Tree, Direction>{tree, key} { }

  NodeRef del(Node *node) { return this->m_tree.delIterate(node); }
};

template <typename Tree_, int Direction_ = ZmRBTreeGreaterEqual>
class ZmRBTreeReadIterator :
    public Tree_::ReadGuard,
    public ZmRBTreeIterator_<Tree_, Direction_> {
  ZmRBTreeReadIterator(const ZmRBTreeReadIterator &) = delete;
  ZmRBTreeReadIterator &operator =(const ZmRBTreeReadIterator &) = delete;

  using Tree = Tree_;
  enum { Direction = Direction_ };
  using ReadGuard = typename Tree::ReadGuard;

public:
  ZmRBTreeReadIterator(ZmRBTreeReadIterator &&) = default;
  ZmRBTreeReadIterator &operator =(ZmRBTreeReadIterator &&) = default;

  ZmRBTreeReadIterator(const Tree &tree) :
    ReadGuard(tree.lock()),
    ZmRBTreeIterator_<Tree, Direction>(const_cast<Tree &>(tree)) { }
  template <typename P>
  ZmRBTreeReadIterator(const Tree &tree, const P &key) :
    ReadGuard(tree.lock()),
    ZmRBTreeIterator_<Tree, Direction>(const_cast<Tree &>(tree), key) { }
};

template <typename T_, class NTP = ZmRBTree_Defaults>
class ZmRBTree : public ZmNodePolicy<typename NTP::Object> {
  template <typename, int> friend class ZmRBTreeIterator_;
  template <typename, int> friend class ZmRBTreeIterator;

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
  enum { Unique = NTP::Unique };

  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  template <int Direction = ZmRBTreeGreaterEqual>
  using Iterator = ZmRBTreeIterator<ZmRBTree, Direction>;
  template <int Direction = ZmRBTreeGreaterEqual>
  using ReadIterator = ZmRBTreeReadIterator<ZmRBTree, Direction>;

  // node in a red/black tree

private:
  struct NullObject { }; // deconflict with ZuNull
  template <typename Node>
  class NodeFn_Dup {
  public:
    void init() { m_dup = nullptr; }
    Node *dup() const { return m_dup; }
    void dup(Node *n) { m_dup = n; }
  private:
    Node		*m_dup;
  };
  template <typename Node>
  class NodeFn_Unique {
  public:
    void init() { }
    constexpr Node * const dup() const { return nullptr; }
    void dup(Node *) { }
  };
  template <
    typename Node, typename Heap, bool NodeDerive, bool Unique>
  class NodeFn_ :
      public ZuIf<
	ZuConversion<ZuNull, Object>::Is ||
	ZuConversion<ZuShadow, Object>::Is ||
	(NodeDerive && ZuConversion<Object, T>::Is),
	NullObject, Object>,
      public ZuIf<Unique, NodeFn_Unique<Node>, NodeFn_Dup<Node>>,
      public Heap {
    using Base = ZuIf<Unique, NodeFn_Unique<Node>, NodeFn_Dup<Node>>;

  public:
    NodeFn_() { }

    void init() {
      Base::init();
      m_right = m_left = nullptr;
      m_parent = 0;
    }

    bool black() { return m_parent & static_cast<uintptr_t>(1); }
    void black(bool black) {
      black ?
	(m_parent |= static_cast<uintptr_t>(1)) :
	(m_parent &= ~static_cast<uintptr_t>(1));
    }

    Node *right() const { return m_right; }
    Node *left() const { return m_left; }
    Node *parent() const {
      return reinterpret_cast<Node *>(m_parent & ~static_cast<uintptr_t>(1));
    }

    void right(Node *n) { m_right = n; }
    void left(Node *n) { m_left = n; }
    void parent(Node *n) {
      m_parent =
	reinterpret_cast<uintptr_t>(n) | (m_parent & static_cast<uintptr_t>(1));
    }

  private:
    Node		*m_right;
    Node		*m_left;
    uintptr_t		m_parent;
  };

  template <typename Node, typename Heap, bool NodeDerive>
  using NodeFn = NodeFn_<Node, Heap, NodeDerive, Unique>;
  template <typename Heap>
  using Node_ = ZmNode<T, KeyAxor, ValAxor, Heap, NodeDerive, NodeFn>;
  struct NullHeap { }; // deconflict with ZuNull
  using NodeHeap = ZmHeap<HeapID, sizeof(Node_<NullHeap>)>;

public:
  using Node = Node_<NodeHeap>;
  using Fn = typename Node::Fn;

  using NodeRef = typename NodePolicy::template Ref<Node>;

private:
  using NodePolicy::nodeRef;
  using NodePolicy::nodeDeref;
  using NodePolicy::nodeDelete;

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

public:
  ZmRBTree() = default;

  ZmRBTree(const ZmRBTree &) = delete;
  ZmRBTree &operator =(const ZmRBTree &) = delete;

  ZmRBTree(ZmRBTree &&tree) noexcept {
    Guard guard(tree.m_lock);
    m_root = tree.m_root;
    m_minimum = tree.m_minimum, m_maximum = tree.m_maximum;
    m_count = tree.m_count;
    tree.m_root = tree.m_minimum = tree.m_maximum = nullptr;
    tree.m_count = 0;
  }
  ZmRBTree &operator =(ZmRBTree &&tree) noexcept {
    unsigned count;
    Node *root, *minimum, *maximum;
    {
      Guard guard(tree.m_lock);
      root = tree.m_root, minimum = tree.m_minimum, maximum = tree.m_maximum;
      count = tree.m_count;
      tree.m_root = tree.m_minimum = tree.m_maximum = nullptr;
      tree.m_count = 0;
    }
    {
      Guard guard(m_lock);
      clean_();
      m_root = root, m_minimum = minimum, m_maximum = maximum;
      m_count = count;
    }
    return *this;
  }

  ~ZmRBTree() { clean_(); }

  Lock &lock() const { return m_lock; }

  unsigned count() const { ReadGuard guard(m_lock); return m_count; }
  unsigned count_() const { return m_count; }

  template <typename P>
  NodeRef add(P &&data) {
    Node *node = new Node(ZuFwd<P>(data));
    addNode(node);
    return node;
  }
  template <typename P0, typename P1>
  NodeRef add(P0 &&p0, P1 &&p1) {
    return add(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }
  template <bool _ = !ZuConversion<NodeRef, Node *>::Same>
  ZuIfT<_> addNode(NodeRef &&node_) {
    Node *node;
    *reinterpret_cast<NodeRef *>(&node) = ZuMv(node_);
    node->init();
    Guard guard(m_lock);
    addNode_(node);
  }
  void addNode(Node *node) {
    nodeRef(node);
    node->init();
    Guard guard(m_lock);
    addNode_(node);
  }
private:
  void addNode_(Node *newNode) {
    Node *node;


    if (!(node = m_root)) {
      newNode->black(1);
      m_root = m_minimum = m_maximum = newNode;
      ++m_count;
      return;
    }

    bool minimum = true, maximum = true;
    const Key &key = newNode->Node::key();

    for (;;) {
      int c = Cmp::cmp(node->Node::key(), key);

      if constexpr (!Unique) {
	if (!c) {
	  Node *child;

	  newNode->Fn::dup(child = node->Fn::dup());
	  if (child) child->Fn::parent(newNode);
	  node->Fn::dup(newNode);
	  newNode->Fn::parent(node);
	  ++m_count;
	  return;
	}
      }

      if (c >= 0) {
	if (!node->Fn::left()) {
	  node->Fn::left(newNode);
	  newNode->Fn::parent(node);
	  if (minimum) m_minimum = newNode;
	  break;
	}

	node = node->Fn::left();
	maximum = false;
      } else {
	if (!node->Fn::right()) {
	  node->Fn::right(newNode);
	  newNode->Fn::parent(node);
	  if (maximum) m_maximum = newNode;
	  break;
	}

	node = node->Fn::right();
	minimum = false;
      }
    }

    rebalance(newNode);
    ++m_count;
  }

  template <typename U, typename V = Key>
  struct IsKey {
    enum { OK = ZuConversion<U, V>::Exists };
  };
  template <typename U, typename R = void>
  using MatchKey = ZuIfT<IsKey<U>::OK, R>;
  template <typename U, typename V = T>
  struct IsData {
    enum { OK = !IsKey<U>::OK && ZuConversion<U, V>::Exists };
  };
  template <typename U, typename R = void>
  using MatchData = ZuIfT<IsData<U>::OK, R>;

  template <typename P>
  struct MatchKeyFn {
    const P &key;
    int cmp(Node *node) const {
      return Cmp::cmp(node->Node::key(), key);
    }
    static constexpr bool equals(Node *) { return true; }
  };
  template <typename P> MatchKeyFn(const P &) -> MatchKeyFn<P>;
  template <typename P>
  struct MatchDataFn {
    const P &data;
    int cmp(Node *node) const {
      return Cmp::cmp(node->Node::key(), KeyAxor::get(data));
    }
    bool equals(Node *node) const {
      return node->Node::data() == data;
    }
  };
  template <typename P> MatchDataFn(const P &) -> MatchDataFn<P>;

  template <int Direction, typename Match>
  ZuIfT<Direction == ZmRBTreeEqual, Node *> find_(Match match) const {
    Node *node = m_root;
    for (;;) {
      if (!node) return nullptr;
      int c = match.cmp(node);
      if (!c) {
	if constexpr (Unique) {
	  if (match.equals(node)) return node;
	  return nullptr;
	} else {
	  while (!match.equals(node)) if (!(node = node->Fn::dup())) break;
	  return node;
	}
      } else if (c > 0) {
	node = node->Fn::left();
      } else {
	node = node->Fn::right();
      }
    }
  }
  template <int Direction, typename Match>
  ZuIfT<Direction == ZmRBTreeGreaterEqual, Node *> find_(Match match) const {
    Node *node = m_root, *foundNode = nullptr;
    for (;;) {
      if (!node) return foundNode;
      int c = match.cmp(node);
      if (!c) {
	return node;
      } else if (c > 0) {
	foundNode = node;
	node = node->Fn::left();
      } else {
	node = node->Fn::right();
      }
    }
  }
  template <int Direction, typename Match>
  ZuIfT<Direction == ZmRBTreeGreater, Node *> find_(Match match) const {
    Node *node = m_root, *foundNode = nullptr;
    for (;;) {
      if (!node) return foundNode;
      int c = match.cmp(node);
      if (!c) {
	node = node->Fn::right();
      } else if (c > 0) {
	foundNode = node;
	node = node->Fn::left();
      } else {
	node = node->Fn::right();
      }
    }
  }
  template <int Direction, typename Match>
  ZuIfT<Direction == ZmRBTreeLessEqual, Node *> find_(Match match) const {
    Node *node = m_root, *foundNode = nullptr;
    for (;;) {
      if (!node) return foundNode;
      int c = match.cmp(node);
      if (!c) {
	return node;
      } else if (c > 0) {
	node = node->Fn::left();
      } else {
	foundNode = node;
	node = node->Fn::right();
      }
    }
  }
  template <int Direction, typename Match>
  ZuIfT<Direction == ZmRBTreeLess, Node *> find_(Match match) const {
    Node *node = m_root, *foundNode = nullptr;
    for (;;) {
      if (!node) return foundNode;
      int c = match.cmp(node);
      if (!c) {
	node = node->Fn::left();
      } else if (c > 0) {
	node = node->Fn::left();
      } else {
	foundNode = node;
	node = node->Fn::right();
      }
    }
  }

public:
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchKey<P, NodeRef> find(const P &key) const {
    ReadGuard guard(m_lock);
    return find_<Direction>(MatchKeyFn{key});
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, NodeRef> find(const P &data) const {
    ReadGuard guard(m_lock);
    return find_<Direction>(MatchDataFn{data});
  }
  template <int Direction = ZmRBTreeEqual, typename P0, typename P1>
  NodeRef find(P0 &&p0, P1 &&p1) {
    return find<Direction>(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <int Direction = ZmRBTreeEqual, typename P>
  MatchKey<P, Node *> findPtr(const P &key) const {
    ReadGuard guard(m_lock);
    return find_<Direction>(MatchKeyFn{key});
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, Node *> findPtr(const P &data) const {
    ReadGuard guard(m_lock);
    return find_<Direction>(MatchDataFn{data});
  }

  template <int Direction = ZmRBTreeEqual, typename P>
  MatchKey<P, Key> findKey(const P &key) const {
    ReadGuard guard(m_lock);
    return key(find_<Direction>(MatchKeyFn{key}));
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, Key> findKey(const P &data) const {
    ReadGuard guard(m_lock);
    return key(find_<Direction>(MatchDataFn{data}));
  }
  template <int Direction = ZmRBTreeEqual, typename P0, typename P1>
  Key findKey(P0 &&p0, P1 &&p1) {
    return findKey<Direction>(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <int Direction = ZmRBTreeEqual, typename P>
  MatchKey<P, Val> findVal(const P &key) const {
    ReadGuard guard(m_lock);
    return val(find_<Direction>(MatchKeyFn{key}));
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, Val> findVal(const P &data) const {
    ReadGuard guard(m_lock);
    return val(find_<Direction>(MatchDataFn{data}));
  }
  template <int Direction = ZmRBTreeEqual, typename P0, typename P1>
  Val findVal(P0 &&p0, P1 &&p1) {
    return findVal<Direction>(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

public:
  NodeRef minimum() const {
    ReadGuard guard(m_lock);
    return m_minimum;
  }
  Node *minimumPtr() const {
    ReadGuard guard(m_lock);
    return m_minimum;
  }
  Key minimumKey() const {
    ReadGuard guard(m_lock);
    Node *node = m_minimum;
    if (ZuUnlikely(!node)) return Cmp::null();
    return key(node);
  }
  Val minimumVal() const {
    ReadGuard guard(m_lock);
    Node *node = m_minimum;
    if (ZuUnlikely(!node)) return ValCmp::null();
    return val(node);
  }

  NodeRef maximum() const {
    ReadGuard guard(m_lock);
    return m_maximum;
  }
  Node *maximumPtr() const {
    ReadGuard guard(m_lock);
    return m_maximum;
  }
  Key maximumKey() const {
    ReadGuard guard(m_lock);
    Node *node = m_maximum;
    if (ZuUnlikely(!node)) return Cmp::null();
    return key(node);
  }
  Val maximumVal() const {
    ReadGuard guard(m_lock);
    Node *node = m_maximum;
    if (ZuUnlikely(!node)) return ValCmp::null();
    return val(node);
  }

  template <int Direction = ZmRBTreeEqual, typename P>
  MatchKey<P, NodeRef> del(const P &key) {
    ReadGuard guard(m_lock);
    Node *node = find_<Direction>(MatchKeyFn{key});
    if (!node) return nullptr;
    delNode_(node);
    NodeRef *ZuMayAlias(ptr) = reinterpret_cast<NodeRef *>(&node);
    return ZuMv(*ptr);
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, NodeRef> del(const P &data) {
    ReadGuard guard(m_lock);
    Node *node = find_<Direction>(MatchDataFn{data});
    if (!node) return nullptr;
    delNode_(node);
    NodeRef *ZuMayAlias(ptr) = reinterpret_cast<NodeRef *>(&node);
    return ZuMv(*ptr);
  }
  template <int Direction = ZmRBTreeEqual, typename P0, typename P1>
  NodeRef del(P0 &&p0, P1 &&p1) {
    return del<Direction>(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <int Direction = ZmRBTreeEqual, typename P>
  MatchKey<P, Key> delKey(const P &key) {
    ReadGuard guard(m_lock);
    Node *node = find_<Direction>(MatchKeyFn{key});
    if (!node) return Cmp::null();
    delNode_(node);
    Key key_ = ZuMv(*node).Node::key();
    nodeDelete(node);
    return key_;
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, Key> delKey(const P &data) {
    ReadGuard guard(m_lock);
    Node *node = find_<Direction>(MatchDataFn{data});
    if (!node) return Cmp::null();
    delNode_(node);
    Key key = ZuMv(*node).Node::key();
    nodeDelete(node);
    return key;
  }
  template <int Direction = ZmRBTreeEqual, typename P0, typename P1>
  Key delKey(P0 &&p0, P1 &&p1) {
    return delKey<Direction>(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <int Direction = ZmRBTreeEqual, typename P>
  MatchKey<P, Val> delVal(const P &key) {
    ReadGuard guard(m_lock);
    Node *node = find_<Direction>(MatchKeyFn{key});
    if (!node) return ValCmp::null();
    delNode_(node);
    Val val = ZuMv(*node).Node::val();
    nodeDelete(node);
    return val;
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, Val> delVal(const P &data) {
    ReadGuard guard(m_lock);
    Node *node = find_<Direction>(MatchDataFn{data});
    if (!node) return ValCmp::null();
    delNode_(node);
    Val val = ZuMv(*node).Node::val();
    nodeDelete(node);
    return val;
  }
  template <int Direction = ZmRBTreeEqual, typename P0, typename P1>
  Val delVal(P0 &&p0, P1 &&p1) {
    return delVal<Direction>(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  NodeRef delNode(Node *node) {
    if (ZuUnlikely(!node)) return nullptr;
    Guard guard(m_lock);
    delNode_(node);
    NodeRef *ZuMayAlias(ptr) = reinterpret_cast<NodeRef *>(&node);
    return ZuMv(*ptr);
  }

private:
  void delNode_(Node *node) {
    if constexpr (!Unique) {
      Node *parent = node->Fn::parent();
      Node *dup = node->Fn::dup();

      if (parent && parent->Fn::dup() == node) {
	parent->Fn::dup(dup);
	if (dup) dup->Fn::parent(parent);
	--m_count;
	return;
      }
      if (dup) {
	{
	  Node *child;

	  dup->Fn::left(child = node->Fn::left());
	  if (child) child->Fn::parent(dup);
	  dup->Fn::right(child = node->Fn::right());
	  if (child) child->Fn::parent(dup);
	}
	if (!parent) {
	  m_root = dup;
	  dup->Fn::parent(0);
	} else if (node == parent->Fn::right()) {
	  parent->Fn::right(dup);
	  dup->Fn::parent(parent);
	} else {
	  parent->Fn::left(dup);
	  dup->Fn::parent(parent);
	}
	dup->black(node->black());
	if (node == m_minimum) m_minimum = dup;
	if (node == m_maximum) m_maximum = dup;
	--m_count;
	return;
      }
    }
    delRebalance(node);
    --m_count;
  }

public:
  template <int Direction = ZmRBTreeGreaterEqual>
  auto iterator() {
    return Iterator<Direction>{*this};
  }
  template <int Direction = ZmRBTreeGreaterEqual, typename P>
  auto iterator(P &&key) {
    return Iterator<Direction>{*this, ZuFwd<P>(key)};
  }
  template <int Direction = ZmRBTreeGreaterEqual>
  auto readIterator() const {
    return ReadIterator<Direction>{*this};
  }
  template <int Direction = ZmRBTreeGreaterEqual, typename P>
  auto readIterator(P &&key) const {
    return ReadIterator<Direction>{*this, ZuFwd<P>(key)};
  }

// clean tree

  void clean() {
    Guard guard(m_lock);
    clean_();
  }
  void clean_() {
    auto i = iterator();
    while (auto node = i.iterate())
      nodeDelete(i.del(node));
    m_minimum = m_maximum = m_root = nullptr;
    m_count = 0;
  }

  void rotateRight(Node *node, Node *parent) {
    Node *left = node->Fn::left();
    Node *mid = left->Fn::right();

  // move left to the right, under node's parent
  // (make left the root if node is the root)

    if (parent) {
      if (parent->Fn::left() == node)
	parent->Fn::left(left);
      else
	parent->Fn::right(left);
    } else
      m_root = left;
    left->Fn::parent(parent);

  // node descends to left's right

    left->Fn::right(node), node->Fn::parent(left);

  // mid switches from left's right to node's left

    node->Fn::left(mid); if (mid) mid->Fn::parent(node);
  }

  void rotateLeft(Node *node, Node *parent) {
    Node *right = node->Fn::right();
    Node *mid = right->Fn::left();

  // move right to the left, under node's parent
  // (make right the root if node is the root)

    if (parent) {
      if (parent->Fn::right() == node)
	parent->Fn::right(right);
      else
	parent->Fn::left(right);
    } else
      m_root = right;
    right->Fn::parent(parent);

  // node descends to right's left

    right->Fn::left(node), node->Fn::parent(right);

  // mid switches from right's left to node's right

    node->Fn::right(mid); if (mid) mid->Fn::parent(node);
  }

  void rebalance(Node *node) {

  // rebalance until we hit a black node (the root is always black)

    for (;;) {
      Node *parent = node->Fn::parent();

      if (!parent) { node->black(1); return; }	// force root to black

      if (parent->black()) return;

      Node *gParent = parent->Fn::parent();

      if (parent == gParent->Fn::left()) {
	Node *uncle = gParent->Fn::right();

	if (uncle && !uncle->black()) {
	  parent->black(1);
	  uncle->black(1);
	  (node = gParent)->black(0);
	} else {
	  if (node == parent->Fn::right()) {
	    rotateLeft(node = parent, gParent);
	    gParent = (parent = node->Fn::parent())->Fn::parent();
	  }
	  parent->black(1);
	  gParent->black(0);
	  rotateRight(gParent, gParent->Fn::parent());
	  m_root->black(1);				// force root to black
	  return;
	}
      } else {
	Node *uncle = gParent->Fn::left();

	if (uncle && !uncle->black()) {
	  parent->black(1);
	  uncle->black(1);
	  (node = gParent)->black(0);
	} else {
	  if (node == parent->Fn::left()) {
	    rotateRight(node = parent, gParent);
	    gParent = (parent = node->Fn::parent())->Fn::parent();
	  }
	  parent->black(1);
	  gParent->black(0);
	  rotateLeft(gParent, gParent->Fn::parent());
	  m_root->black(1);				// force root to black
	  return;
	}
      }
    }
  }

  void delRebalance(Node *node) {
    Node *successor = node;
    Node *child, *parent;

    if (!successor->Fn::left())
      child = successor->Fn::right();
    else if (!successor->Fn::right())
      child = successor->Fn::left();
    else {
      successor = successor->Fn::right();
      while (successor->Fn::left()) successor = successor->Fn::left();
      child = successor->Fn::right();
    }

    if (successor != node) {
      node->Fn::left()->Fn::parent(successor);
      successor->Fn::left(node->Fn::left());
      if (successor != node->Fn::right()) {
	parent = successor->Fn::parent();
	if (child) child->Fn::parent(parent);
	successor->Fn::parent()->Fn::left(child);
	successor->Fn::right(node->Fn::right());
	node->Fn::right()->Fn::parent(successor);
      } else
	parent = successor;

      Node *childParent = parent;

      parent = node->Fn::parent();

      if (!parent)
	m_root = successor;
      else if (node == parent->Fn::left())
	parent->Fn::left(successor);
      else
	parent->Fn::right(successor);
      successor->Fn::parent(parent);

      bool black = node->black();

      node->black(successor->black());
      successor->black(black);

      successor = node;

      parent = childParent;
    } else {
      parent = node->Fn::parent();

      if (child) child->Fn::parent(parent);

      if (!parent)
	m_root = child;
      else if (node == parent->Fn::left())
	parent->Fn::left(child);
      else
	parent->Fn::right(child);

      if (node == m_minimum) {
	if (!node->Fn::right())
	  m_minimum = parent;
	else {
	  Node *minimum = child;

	  do {
	    m_minimum = minimum;
	  } while (minimum = minimum->Fn::left());
	}
      }

      if (node == m_maximum) {
	if (!node->Fn::left())
	  m_maximum = parent;
	else {
	  Node *maximum = child;

	  do {
	    m_maximum = maximum;
	  } while (maximum = maximum->Fn::right());
	}
      }
    }

    if (successor->black()) {
      Node *sibling;

      while (parent && (!child || child->black()))
	if (child == parent->Fn::left()) {
	  sibling = parent->Fn::right();
	  if (!sibling->black()) {
	    sibling->black(1);
	    parent->black(0);
	    rotateLeft(parent, parent->Fn::parent());
	    sibling = parent->Fn::right();
	  }
	  if ((!sibling->Fn::left() || sibling->Fn::left()->black()) &&
	      (!sibling->Fn::right() || sibling->Fn::right()->black())) {
	    sibling->black(0);
	    child = parent;
	    parent = child->Fn::parent();
	  } else {
	    if (!sibling->Fn::right() || sibling->Fn::right()->black()) {
	      if (sibling->Fn::left()) sibling->Fn::left()->black(1);
	      sibling->black(0);
	      rotateRight(sibling, parent);
	      sibling = parent->Fn::right();
	    }
	    sibling->black(parent->black());
	    parent->black(1);
	    if (sibling->Fn::right()) sibling->Fn::right()->black(1);
	    rotateLeft(parent, parent->Fn::parent());
	    break;
	  }
	} else {
	  sibling = parent->Fn::left();
	  if (!sibling->black()) {
	    sibling->black(1);
	    parent->black(0);
	    rotateRight(parent, parent->Fn::parent());
	    sibling = parent->Fn::left();
	  }
	  if ((!sibling->Fn::right() || sibling->Fn::right()->black()) &&
	      (!sibling->Fn::left() || sibling->Fn::left()->black())) {
	    sibling->black(0);
	    child = parent;
	    parent = child->Fn::parent();
	  } else {
	    if (!sibling->Fn::left() || sibling->Fn::left()->black()) {
	      if (sibling->Fn::right()) sibling->Fn::right()->black(1);
	      sibling->black(0);
	      rotateLeft(sibling, parent);
	      sibling = parent->Fn::left();
	    }
	    sibling->black(parent->black());
	    parent->black(1);
	    if (sibling->Fn::left()) sibling->Fn::left()->black(1);
	    rotateRight(parent, parent->Fn::parent());
	    break;
	  }
	}
      if (child) child->black(1);
    }
  }

  Node *next(Node *node) {
    Node *next;

    if constexpr (!Unique) {
      if (next = node->Fn::dup()) return next;

      if (next = node->Fn::parent())
	while (node == next->Fn::dup()) {
	  node = next;
	  if (!(next = node->Fn::parent())) break;
	}
    }

    if (next = node->Fn::right()) {
      node = next;
      while (node = node->Fn::left()) next = node;
      return next;
    }

    if (!(next = node->Fn::parent())) return nullptr;

    while (node == next->Fn::right()) {
      node = next;
      if (!(next = node->Fn::parent())) return nullptr;
    }

    return next;
  }

  Node *prev(Node *node) {
    Node *prev;

    if constexpr (!Unique) {
      if (prev = node->Fn::dup()) return prev;

      if (prev = node->Fn::parent())
	while (node == prev->Fn::dup()) {
	  node = prev;
	  if (!(prev = node->Fn::parent())) break;
	}
    }

    if (prev = node->Fn::left()) {
      node = prev;
      while (node = node->Fn::right()) prev = node;
      return prev;
    }

    if (!(prev = node->Fn::parent())) return nullptr;

    while (node == prev->Fn::left()) {
      node = prev;
      if (!(prev = node->Fn::parent())) return nullptr;
    }

    return prev;
  }

// iterator functions

  template <int Direction>
  using Iterator_ = ZmRBTreeIterator_<ZmRBTree, Direction>;

  template <int Direction>
  ZuIfT<(Direction >= 0)> startIterate(Iterator_<Direction> &iterator) {
    iterator.m_node = m_minimum;
  }
  template <int Direction>
  ZuIfT<(Direction < 0)> startIterate(Iterator_<Direction> &iterator) {
    iterator.m_node = m_maximum;
  }
  template <int Direction, typename P>
  void startIterate(Iterator_<Direction> &iterator, const P &key) {
    iterator.m_node = find_<Direction>(MatchKeyFn{key});
  }

  template <int Direction>
  ZuIfT<(Direction > 0), Node *> iterate(Iterator_<Direction> &iterator) {
    Node *node = iterator.m_node;
    if (!node) return nullptr;
    iterator.m_node = next(node);
    return node;
  }
  template <int Direction>
  ZuIfT<(!Direction), Node *> iterate(Iterator_<Direction> &iterator) {
    Node *node = iterator.m_node;
    if (!node) return nullptr;
    iterator.m_node = node->Fn::dup();
    return node;
  }
  template <int Direction>
  ZuIfT<(Direction < 0), Node *> iterate(Iterator_<Direction> &iterator) {
    Node *node = iterator.m_node;
    if (!node) return nullptr;
    iterator.m_node = prev(node);
    return node;
  }

  NodeRef delIterate(Node *node) {
    if (ZuUnlikely(!node)) return nullptr;
    delNode_(node);
    NodeRef *ZuMayAlias(ptr) = reinterpret_cast<NodeRef *>(&node);
    return ZuMv(*ptr);
  }

  mutable Lock	m_lock;
    Node	  *m_root = nullptr;
    Node	  *m_minimum = nullptr;
    Node	  *m_maximum = nullptr;
    unsigned	  m_count = 0;
};

template <typename P0, typename P1, typename NTP = ZmRBTree_Defaults>
using ZmRBTreeKV =
  ZmRBTree<ZuPair<P0, P1>,
    ZmRBTreeKeyVal<ZuPairAxor<0>, ZuPairAxor<1>, NTP> >;

#endif /* ZmRBTree_HPP */
