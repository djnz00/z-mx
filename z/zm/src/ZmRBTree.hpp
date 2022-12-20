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

#include <zlib/ZmAssert.hpp>
#include <zlib/ZmGuard.hpp>
#include <zlib/ZmNoLock.hpp>
#include <zlib/ZmRef.hpp>
#include <zlib/ZmHeap.hpp>
#include <zlib/ZmNode.hpp>
#include <zlib/ZmNodeContainer.hpp>

// uses NTP (named template parameters):
//
// ZmRBTreeKV<ZtString, ZtString,	// key, value pair of ZtStrings
//     ZmRBTreeValCmp<ZtICmp> >		// case-insensitive comparison

// NTP defaults
struct ZmRBTree_Defaults {
  constexpr static auto KeyAxor = ZuDefaultAxor();
  constexpr static auto ValAxor = ZuDefaultAxor();
  template <typename T> using CmpT = ZuCmp<T>;
  template <typename T> using ValCmpT = ZuCmp<T>;
  enum { Unique = 0 };
  using Lock = ZmNoLock;
  using Node = ZuNull;
  enum { Shadow = 0 };
  static const char *HeapID() { return "ZmRBTree"; }
  enum { Sharded = 0 };
};

// ZmRBTreeKey - key accessor
template <auto KeyAxor_, typename NTP = ZmRBTree_Defaults>
struct ZmRBTreeKey : public NTP {
  constexpr static auto KeyAxor = KeyAxor_;
};

// ZmRBTreeKeyVal - key and optional value accessors
template <
  auto KeyAxor_, auto ValAxor_,
  typename NTP = ZmRBTree_Defaults>
struct ZmRBTreeKeyVal : public NTP {
  constexpr static auto KeyAxor = KeyAxor_;
  constexpr static auto ValAxor = ValAxor_;
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

// ZmRBTreeUnique - key is unique
template <bool Unique_, class NTP = ZmRBTree_Defaults>
struct ZmRBTreeUnique : public NTP {
  enum { Unique = Unique_ };
};

// ZmRBTreeLock - the lock type used (ZmRWLock will permit concurrent reads)
template <class Lock_, class NTP = ZmRBTree_Defaults>
struct ZmRBTreeLock : public NTP {
  using Lock = Lock_;
};

// ZmRBTreeNode - the base type for nodes
template <typename Node_, typename NTP = ZmRBTree_Defaults>
struct ZmRBTreeNode : public NTP {
  using Node = Node_;
};

// ZmRBTreeShadow - shadow nodes, do not manage ownership
template <bool Shadow_, typename NTP = ZmRBTree_Defaults>
struct ZmRBTreeShadow : public NTP {
  enum { Shadow = Shadow_ };
};

// ZmRBTreeHeapID - the heap ID
template <auto HeapID_, class NTP = ZmRBTree_Defaults>
struct ZmRBTreeHeapID : public NTP {
  constexpr static auto HeapID = HeapID_;
};

// ZmRBTreeSharded - heap is sharded
template <bool Sharded_, typename NTP = ZmRBTree_Defaults>
struct ZmRBTreeSharded : public NTP {
  enum { Sharded = Sharded_ };
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

  decltype(auto) iterateKey() { return Tree::key(m_tree.iterate(*this)); }
  decltype(auto) iterateVal() { return Tree::val(m_tree.iterate(*this)); }

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
class ZmRBTree :
    public ZmNodeContainer<NTP::Shadow, T_, typename NTP::Node> {
  template <typename, int> friend class ZmRBTreeIterator_;
  template <typename, int> friend class ZmRBTreeIterator;
  template <typename, int> friend class ZmRBTreeReadIterator;

public:
  using T = T_;
  constexpr static auto KeyAxor = NTP::KeyAxor;
  constexpr static auto ValAxor = NTP::ValAxor;
  using KeyRet = decltype(KeyAxor(ZuDeclVal<const T &>()));
  using ValRet = decltype(ValAxor(ZuDeclVal<const T &>()));
  using Key = ZuDecay<KeyRet>;
  using Val = ZuDecay<ValRet>;
  using Cmp = typename NTP::template CmpT<Key>;
  using ValCmp = typename NTP::template ValCmpT<Val>;
  enum { Unique = NTP::Unique };
  using Lock = typename NTP::Lock;
  using NodeBase = typename NTP::Node;
  enum { Shadow = NTP::Shadow };
  constexpr static auto HeapID = NTP::HeapID;
  enum { Sharded = NTP::Sharded };

private:
  using NodeContainer = ZmNodeContainer<Shadow, T, NodeBase>;

  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

public:
  template <int Direction = ZmRBTreeGreaterEqual>
  using Iterator = ZmRBTreeIterator<ZmRBTree, Direction>;
  template <int Direction = ZmRBTreeGreaterEqual>
  using ReadIterator = ZmRBTreeReadIterator<ZmRBTree, Direction>;

private:
  // node in a red/black tree

  template <typename Node>
  class NodeFn_Dup {
  friend ZmRBTree<T, NTP>;
  template <typename, int> friend class ZmRBTreeIterator_;

    ZuInline Node *dup() const { return m_dup; }
    ZuInline void dup(Node *n) { m_dup = n; }

    ZuInline void clear() { m_dup = nullptr; }

    Node	*m_dup = nullptr;
  };
  template <typename Node>
  class NodeFn_Unique {
  friend ZmRBTree<T, NTP>;
  template <typename, int> friend class ZmRBTreeIterator_;

    ZuInline constexpr Node * const dup() const { return nullptr; }
    ZuInline void dup(Node *) { }

    ZuInline void clear() { }
  };
  template <typename Node>
  class NodeFn_ : public ZuIf<Unique, NodeFn_Unique<Node>, NodeFn_Dup<Node>> {
  friend ZmRBTree<T, NTP>;
  template <typename, int> friend class ZmRBTreeIterator_;
    // 64bit pointer-packing - uses bit 63
    constexpr static uintptr_t Black() {
      return static_cast<uintptr_t>(1)<<63;
    }
    constexpr static uintptr_t Black(bool b) {
      return static_cast<uintptr_t>(b)<<63;
    }

    using Base = ZuIf<Unique, NodeFn_Unique<Node>, NodeFn_Dup<Node>>;

    ZuInline bool black() { return m_parent & Black(); }
    ZuInline void black(bool b) {
      m_parent = (m_parent & ~Black()) | Black(b);
    }
    ZuInline void black(const NodeFn_ *node) {
      m_parent = (m_parent & ~Black()) | (node->m_parent & Black());
    }
    ZuInline void setBlack() { m_parent |= Black(); }
    ZuInline void clrBlack() { m_parent &= ~Black(); }

    ZuInline Node *right() const { return m_right; }
    ZuInline Node *left() const { return m_left; }
    ZuInline Node *parent() const {
      return reinterpret_cast<Node *>(m_parent & ~Black());
    }

    ZuInline void right(Node *n) { m_right = n; }
    ZuInline void left(Node *n) { m_left = n; }
    ZuInline void parent(Node *n) {
      m_parent = reinterpret_cast<uintptr_t>(n) | (m_parent & Black());
    }

    void clearDup() {
      Base::clear();
      m_parent = 0;
    }
    void clear() {
      Base::clear();
      m_right = nullptr;
      m_left = nullptr;
      m_parent = 0;
    }

    Node		*m_right = nullptr;
    Node		*m_left = nullptr;
    uintptr_t		m_parent = 0;
  };

public:
  using Node = ZmNode<T, KeyAxor, ValAxor, NodeBase, NodeFn_, HeapID, Sharded>;
  using NodeFn = NodeFn_<Node>;
  using NodeRef = typename NodeContainer::template Ref<Node>;
  using NodePtr = Node *;

private:
  using NodeContainer::nodeRef;
  using NodeContainer::nodeDeref;
  using NodeContainer::nodeDelete;
  using NodeContainer::nodeAcquire;

  static KeyRet key(Node *node) {
    if (ZuLikely(node)) return node->Node::key();
    return ZuNullRef<Key, Cmp>();
  }
  static Key keyMv(NodeRef &&node) {
    if (ZuLikely(node)) {
      Key key = ZuMv(*node).Node::key();
      nodeDelete(node);
      return key;
    }
    return ZuNullRef<Key, Cmp>();
  }
  static ValRet val(Node *node) {
    if (ZuLikely(node)) return node->Node::val();
    return ZuNullRef<Val, ValCmp>();
  }
  static Val valMv(NodeRef &&node) {
    if (ZuLikely(node)) {
      Val val = ZuMv(*node).Node::val();
      nodeDelete(node);
      return val;
    }
    return ZuNullRef<Val, ValCmp>();
  }

public:
  ZmRBTree() = default;

  ZmRBTree(const ZmRBTree &) = delete;
  ZmRBTree &operator =(const ZmRBTree &) = delete;

  ZmRBTree(ZmRBTree &&tree) {
    Guard guard(tree.m_lock);
    m_root = tree.m_root;
    m_minimum = tree.m_minimum, m_maximum = tree.m_maximum;
    m_count = tree.m_count;
    tree.m_root = tree.m_minimum = tree.m_maximum = nullptr;
    tree.m_count = 0;
  }
  ZmRBTree &operator =(ZmRBTree &&tree) {
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

  // unsigned count() const { ReadGuard guard(m_lock); return m_count; }
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
  ZuIfT<_> addNode(const NodeRef &node_) { addNode(node_.ptr()); }
  template <bool _ = !ZuConversion<NodeRef, Node *>::Same>
  ZuIfT<_> addNode(NodeRef &&node_) {
    Node *node = node_.release();
    Guard guard(m_lock);
    addNode_(node);
  }
  void addNode(Node *node) {
    nodeRef(node);
    Guard guard(m_lock);
    addNode_(node);
  }
private:
  void addNode_(Node *newNode) {
    if constexpr (Unique) ZmAssert(!newNode->NodeFn::dup());
    ZmAssert(!newNode->NodeFn::left());
    ZmAssert(!newNode->NodeFn::right());
    ZmAssert(!newNode->NodeFn::parent());
    ZmAssert(!newNode->NodeFn::black());

    Node *node;

    if (!(node = m_root)) {
      newNode->NodeFn::setBlack();
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

	  newNode->NodeFn::dup(child = node->NodeFn::dup());
	  if (child) child->NodeFn::parent(newNode);
	  node->NodeFn::dup(newNode);
	  newNode->NodeFn::parent(node);
	  ++m_count;
	  return;
	}
      }

      if (c >= 0) {
	if (!node->NodeFn::left()) {
	  node->NodeFn::left(newNode);
	  newNode->NodeFn::parent(node);
	  if (minimum) m_minimum = newNode;
	  break;
	}

	node = node->NodeFn::left();
	maximum = false;
      } else {
	if (!node->NodeFn::right()) {
	  node->NodeFn::right(newNode);
	  newNode->NodeFn::parent(node);
	  if (maximum) m_maximum = newNode;
	  break;
	}

	node = node->NodeFn::right();
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
  struct MatchKeyFn {
    const P &key;
    int cmp(Node *node) const {
      return Cmp::cmp(node->Node::key(), key);
    }
    constexpr static bool equals(Node *) { return true; }
  };
  template <typename P>
  struct MatchDataFn {
    const P &data;
    int cmp(Node *node) const {
      return Cmp::cmp(node->Node::key(), KeyAxor(data));
    }
    bool equals(Node *node) const {
      return node->Node::data() == data;
    }
  };

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
	  while (!match.equals(node)) if (!(node = node->NodeFn::dup())) break;
	  return node;
	}
      } else if (c > 0) {
	node = node->NodeFn::left();
      } else {
	node = node->NodeFn::right();
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
	node = node->NodeFn::left();
      } else {
	node = node->NodeFn::right();
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
	node = node->NodeFn::right();
      } else if (c > 0) {
	foundNode = node;
	node = node->NodeFn::left();
      } else {
	node = node->NodeFn::right();
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
	node = node->NodeFn::left();
      } else {
	foundNode = node;
	node = node->NodeFn::right();
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
	node = node->NodeFn::left();
      } else if (c > 0) {
	node = node->NodeFn::left();
      } else {
	foundNode = node;
	node = node->NodeFn::right();
      }
    }
  }

public:
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchKey<P, NodeRef> find(const P &key) const {
    ReadGuard guard(m_lock);
    return find_<Direction>(MatchKeyFn<P>{key});
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, NodeRef> find(const P &data) const {
    ReadGuard guard(m_lock);
    return find_<Direction>(MatchDataFn<P>{data});
  }
  template <int Direction = ZmRBTreeEqual, typename P0, typename P1>
  NodeRef find(P0 &&p0, P1 &&p1) {
    return find<Direction>(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <int Direction = ZmRBTreeEqual, typename P>
  MatchKey<P, Node *> findPtr(const P &key) const {
    ReadGuard guard(m_lock);
    return find_<Direction>(MatchKeyFn<P>{key});
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, Node *> findPtr(const P &data) const {
    ReadGuard guard(m_lock);
    return find_<Direction>(MatchDataFn<P>{data});
  }

  template <int Direction = ZmRBTreeEqual, typename P>
  MatchKey<P, Key> findKey(const P &key) const {
    ReadGuard guard(m_lock);
    return key(find_<Direction>(MatchKeyFn<P>{key}));
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, Key> findKey(const P &data) const {
    ReadGuard guard(m_lock);
    return key(find_<Direction>(MatchDataFn<P>{data}));
  }
  template <int Direction = ZmRBTreeEqual, typename P0, typename P1>
  Key findKey(P0 &&p0, P1 &&p1) {
    return findKey<Direction>(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <int Direction = ZmRBTreeEqual, typename P>
  MatchKey<P, Val> findVal(const P &key) const {
    ReadGuard guard(m_lock);
    return val(find_<Direction>(MatchKeyFn<P>{key}));
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, Val> findVal(const P &data) const {
    ReadGuard guard(m_lock);
    return val(find_<Direction>(MatchDataFn<P>{data}));
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
    if (ZuUnlikely(!node)) return ZuNullRef<Key, Cmp>();
    return key(node);
  }
  Val minimumVal() const {
    ReadGuard guard(m_lock);
    Node *node = m_minimum;
    if (ZuUnlikely(!node)) return ZuNullRef<Val, ValCmp>();
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
    if (ZuUnlikely(!node)) return ZuNullRef<Key, Cmp>();
    return key(node);
  }
  Val maximumVal() const {
    ReadGuard guard(m_lock);
    Node *node = m_maximum;
    if (ZuUnlikely(!node)) return ZuNullRef<Val, ValCmp>();
    return val(node);
  }

  template <int Direction = ZmRBTreeEqual, typename P>
  MatchKey<P, NodeRef> del(const P &key) {
    ReadGuard guard(m_lock);
    Node *node = find_<Direction>(MatchKeyFn<P>{key});
    if (!node) return nullptr;
    delNode_(node);
    return nodeAcquire(node);
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, NodeRef> del(const P &data) {
    ReadGuard guard(m_lock);
    Node *node = find_<Direction>(MatchDataFn<P>{data});
    if (!node) return nullptr;
    delNode_(node);
    return nodeAcquire(node);
  }
  template <int Direction = ZmRBTreeEqual, typename P0, typename P1>
  NodeRef del(P0 &&p0, P1 &&p1) {
    return del<Direction>(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <int Direction = ZmRBTreeEqual, typename P>
  MatchKey<P, Key> delKey(const P &key) {
    ReadGuard guard(m_lock);
    Node *node = find_<Direction>(MatchKeyFn<P>{key});
    if (!node) return ZuNullRef<Key, Cmp>();
    delNode_(node);
    Key key_ = ZuMv(*node).Node::key();
    nodeDelete(node);
    return key_;
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, Key> delKey(const P &data) {
    ReadGuard guard(m_lock);
    Node *node = find_<Direction>(MatchDataFn<P>{data});
    if (!node) return ZuNullRef<Key, Cmp>();
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
    Node *node = find_<Direction>(MatchKeyFn<P>{key});
    if (!node) return ZuNullRef<Val, ValCmp>();
    delNode_(node);
    Val val = ZuMv(*node).Node::val();
    nodeDelete(node);
    return val;
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, Val> delVal(const P &data) {
    ReadGuard guard(m_lock);
    Node *node = find_<Direction>(MatchDataFn<P>{data});
    if (!node) return ZuNullRef<Val, ValCmp>();
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
    return nodeAcquire(node);
  }

private:
  void delNode_(Node *node) {
    if constexpr (!Unique) {
      Node *parent = node->NodeFn::parent();
      Node *dup = node->NodeFn::dup();

      if (parent && parent->NodeFn::dup() == node) {
	parent->NodeFn::dup(dup);
	if (dup) dup->NodeFn::parent(parent);
	--m_count;
	node->NodeFn::clearDup();
	return;
      }
      if (dup) {
	{
	  Node *child;

	  dup->NodeFn::left(child = node->NodeFn::left());
	  if (child) {
	    node->NodeFn::left(nullptr);
	    child->NodeFn::parent(dup);
	  }
	  dup->NodeFn::right(child = node->NodeFn::right());
	  if (child) {
	    node->NodeFn::right(nullptr);
	    child->NodeFn::parent(dup);
	  }
	}
	if (!parent) {
	  m_root = dup;
	  dup->NodeFn::parent(0);
	} else if (node == parent->NodeFn::right()) {
	  parent->NodeFn::right(dup);
	  dup->NodeFn::parent(parent);
	} else {
	  parent->NodeFn::left(dup);
	  dup->NodeFn::parent(parent);
	}
	dup->NodeFn::black(node);
	if (node == m_minimum) m_minimum = dup;
	if (node == m_maximum) m_maximum = dup;
	--m_count;
	node->NodeFn::clearDup();
	return;
      }
    }
    delRebalance(node);
    node->NodeFn::clear();
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
    Node *left = node->NodeFn::left();
    Node *mid = left->NodeFn::right();

  // move left to the right, under node's parent
  // (make left the root if node is the root)

    if (parent) {
      if (parent->NodeFn::left() == node)
	parent->NodeFn::left(left);
      else
	parent->NodeFn::right(left);
    } else
      m_root = left;
    left->NodeFn::parent(parent);

  // node descends to left's right

    left->NodeFn::right(node), node->NodeFn::parent(left);

  // mid switches from left's right to node's left

    node->NodeFn::left(mid); if (mid) mid->NodeFn::parent(node);
  }

  void rotateLeft(Node *node, Node *parent) {
    Node *right = node->NodeFn::right();
    Node *mid = right->NodeFn::left();

  // move right to the left, under node's parent
  // (make right the root if node is the root)

    if (parent) {
      if (parent->NodeFn::right() == node)
	parent->NodeFn::right(right);
      else
	parent->NodeFn::left(right);
    } else
      m_root = right;
    right->NodeFn::parent(parent);

  // node descends to right's left

    right->NodeFn::left(node), node->NodeFn::parent(right);

  // mid switches from right's left to node's right

    node->NodeFn::right(mid); if (mid) mid->NodeFn::parent(node);
  }

  void rebalance(Node *node) {

  // rebalance until we hit a black node (the root is always black)

    for (;;) {
      Node *parent = node->NodeFn::parent();

      if (!parent) { node->NodeFn::setBlack(); return; }// force root to black

      if (parent->NodeFn::black()) return;

      Node *gParent = parent->NodeFn::parent();

      if (parent == gParent->NodeFn::left()) {
	Node *uncle = gParent->NodeFn::right();

	if (uncle && !uncle->NodeFn::black()) {
	  parent->NodeFn::setBlack();
	  uncle->NodeFn::setBlack();
	  (node = gParent)->NodeFn::clrBlack();
	} else {
	  if (node == parent->NodeFn::right()) {
	    rotateLeft(node = parent, gParent);
	    gParent = (parent = node->NodeFn::parent())->NodeFn::parent();
	  }
	  parent->NodeFn::setBlack();
	  gParent->NodeFn::clrBlack();
	  rotateRight(gParent, gParent->NodeFn::parent());
	  m_root->NodeFn::setBlack();			// force root to black
	  return;
	}
      } else {
	Node *uncle = gParent->NodeFn::left();

	if (uncle && !uncle->NodeFn::black()) {
	  parent->NodeFn::setBlack();
	  uncle->NodeFn::setBlack();
	  (node = gParent)->NodeFn::clrBlack();
	} else {
	  if (node == parent->NodeFn::left()) {
	    rotateRight(node = parent, gParent);
	    gParent = (parent = node->NodeFn::parent())->NodeFn::parent();
	  }
	  parent->NodeFn::setBlack();
	  gParent->NodeFn::clrBlack();
	  rotateLeft(gParent, gParent->NodeFn::parent());
	  m_root->NodeFn::setBlack();			// force root to black
	  return;
	}
      }
    }
  }

  void delRebalance(Node *node) {
    Node *successor = node;
    Node *child, *parent;

    if (!successor->NodeFn::left())
      child = successor->NodeFn::right();
    else if (!successor->NodeFn::right())
      child = successor->NodeFn::left();
    else {
      successor = successor->NodeFn::right();
      while (successor->NodeFn::left()) successor = successor->NodeFn::left();
      child = successor->NodeFn::right();
    }

    if (successor != node) {
      node->NodeFn::left()->NodeFn::parent(successor);
      successor->NodeFn::left(node->NodeFn::left());
      if (successor != node->NodeFn::right()) {
	parent = successor->NodeFn::parent();
	if (child) child->NodeFn::parent(parent);
	successor->NodeFn::parent()->NodeFn::left(child);
	successor->NodeFn::right(node->NodeFn::right());
	node->NodeFn::right()->NodeFn::parent(successor);
      } else
	parent = successor;

      Node *childParent = parent;

      parent = node->NodeFn::parent();

      if (!parent)
	m_root = successor;
      else if (node == parent->NodeFn::left())
	parent->NodeFn::left(successor);
      else
	parent->NodeFn::right(successor);
      successor->NodeFn::parent(parent);

      bool black = node->NodeFn::black();

      node->NodeFn::black(successor);
      successor->NodeFn::black(black);

      successor = node;

      parent = childParent;
    } else {
      parent = node->NodeFn::parent();

      if (child) child->NodeFn::parent(parent);

      if (!parent)
	m_root = child;
      else if (node == parent->NodeFn::left())
	parent->NodeFn::left(child);
      else
	parent->NodeFn::right(child);

      if (node == m_minimum) {
	if (!node->NodeFn::right())
	  m_minimum = parent;
	else {
	  Node *minimum = child;

	  do {
	    m_minimum = minimum;
	  } while (minimum = minimum->NodeFn::left());
	}
      }

      if (node == m_maximum) {
	if (!node->NodeFn::left())
	  m_maximum = parent;
	else {
	  Node *maximum = child;

	  do {
	    m_maximum = maximum;
	  } while (maximum = maximum->NodeFn::right());
	}
      }
    }

    if (successor->NodeFn::black()) {
      Node *sibling;

      while (parent && (!child || child->NodeFn::black()))
	if (child == parent->NodeFn::left()) {
	  sibling = parent->NodeFn::right();
	  if (!sibling->NodeFn::black()) {
	    sibling->NodeFn::setBlack();
	    parent->NodeFn::clrBlack();
	    rotateLeft(parent, parent->NodeFn::parent());
	    sibling = parent->NodeFn::right();
	  }
	  if ((!sibling->NodeFn::left() ||
		sibling->NodeFn::left()->NodeFn::black()) &&
	      (!sibling->NodeFn::right() ||
		sibling->NodeFn::right()->NodeFn::black())) {
	    sibling->NodeFn::clrBlack();
	    child = parent;
	    parent = child->NodeFn::parent();
	  } else {
	    if (!sibling->NodeFn::right() ||
		sibling->NodeFn::right()->NodeFn::black()) {
	      if (sibling->NodeFn::left())
		sibling->NodeFn::left()->NodeFn::setBlack();
	      sibling->NodeFn::clrBlack();
	      rotateRight(sibling, parent);
	      sibling = parent->NodeFn::right();
	    }
	    sibling->NodeFn::black(parent);
	    parent->NodeFn::setBlack();
	    if (sibling->NodeFn::right())
	      sibling->NodeFn::right()->NodeFn::setBlack();
	    rotateLeft(parent, parent->NodeFn::parent());
	    break;
	  }
	} else {
	  sibling = parent->NodeFn::left();
	  if (!sibling->NodeFn::black()) {
	    sibling->NodeFn::setBlack();
	    parent->NodeFn::clrBlack();
	    rotateRight(parent, parent->NodeFn::parent());
	    sibling = parent->NodeFn::left();
	  }
	  if ((!sibling->NodeFn::right() ||
		sibling->NodeFn::right()->NodeFn::black()) &&
	      (!sibling->NodeFn::left() ||
		sibling->NodeFn::left()->NodeFn::black())) {
	    sibling->NodeFn::clrBlack();
	    child = parent;
	    parent = child->NodeFn::parent();
	  } else {
	    if (!sibling->NodeFn::left() ||
		sibling->NodeFn::left()->NodeFn::black()) {
	      if (sibling->NodeFn::right())
		sibling->NodeFn::right()->NodeFn::setBlack();
	      sibling->NodeFn::clrBlack();
	      rotateLeft(sibling, parent);
	      sibling = parent->NodeFn::left();
	    }
	    sibling->NodeFn::black(parent);
	    parent->NodeFn::setBlack();
	    if (sibling->NodeFn::left())
	      sibling->NodeFn::left()->NodeFn::setBlack();
	    rotateRight(parent, parent->NodeFn::parent());
	    break;
	  }
	}
      if (child) child->NodeFn::setBlack();
    }
  }

  Node *next(Node *node) {
    Node *next;

    if constexpr (!Unique) {
      if (next = node->NodeFn::dup()) return next;

      if (next = node->NodeFn::parent())
	while (node == next->NodeFn::dup()) {
	  node = next;
	  if (!(next = node->NodeFn::parent())) break;
	}
    }

    if (next = node->NodeFn::right()) {
      node = next;
      while (node = node->NodeFn::left()) next = node;
      return next;
    }

    if (!(next = node->NodeFn::parent())) return nullptr;

    while (node == next->NodeFn::right()) {
      node = next;
      if (!(next = node->NodeFn::parent())) return nullptr;
    }

    return next;
  }

  Node *prev(Node *node) {
    Node *prev;

    if constexpr (!Unique) {
      if (prev = node->NodeFn::dup()) return prev;

      if (prev = node->NodeFn::parent())
	while (node == prev->NodeFn::dup()) {
	  node = prev;
	  if (!(prev = node->NodeFn::parent())) break;
	}
    }

    if (prev = node->NodeFn::left()) {
      node = prev;
      while (node = node->NodeFn::right()) prev = node;
      return prev;
    }

    if (!(prev = node->NodeFn::parent())) return nullptr;

    while (node == prev->NodeFn::left()) {
      node = prev;
      if (!(prev = node->NodeFn::parent())) return nullptr;
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
    iterator.m_node = find_<Direction>(MatchKeyFn<P>{key});
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
    iterator.m_node = node->NodeFn::dup();
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
    return nodeAcquire(node);
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
    ZmRBTreeKeyVal<ZuPairAxor<0>(), ZuPairAxor<1>(), NTP> >;

#endif /* ZmRBTree_HPP */
