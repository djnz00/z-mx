//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// red/black tree (compile-time policy-based)
// * intrusive
// * policy-based control of key, value, locking, heap, etc.
// * intentionally disdains range-based for() and structured binding

#ifndef ZmRBTree_HH
#define ZmRBTree_HH

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <zlib/ZuNull.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuInspect.hh>

#include <zlib/ZmAssert.hh>
#include <zlib/ZmGuard.hh>
#include <zlib/ZmNoLock.hh>
#include <zlib/ZmRef.hh>
#include <zlib/ZmHeap.hh>
#include <zlib/ZmNode.hh>
#include <zlib/ZmNodeFn.hh>

// uses NTP (named template parameters):
//
// ZmRBTreeKV<ZtString, ZtString,	// key, value pair of ZtStrings
//     ZmRBTreeValCmp<ZuICmp> >		// case-insensitive comparison

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
struct ZmRBTreeShadow;
template <typename NTP>
struct ZmRBTreeShadow<true, NTP> : public NTP {
  enum { Shadow = true };
  constexpr static auto HeapID = ZmHeapDisable();
};
template <typename NTP>
struct ZmRBTreeShadow<false, NTP> : public NTP {
  enum { Shadow = false };
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

// ZmRBTree node
template <typename Node>
struct ZmRBTree_NodeExt_Dup {
  ZuInline Node *dup() const { return m_dup; }
  ZuInline void dup(Node *n) { m_dup = n; }

  ZuInline void clear() { m_dup = nullptr; }

private:
  Node	*m_dup = nullptr;
};
template <typename Node>
struct ZmRBTree_NodeExt_Unique {
  constexpr Node * const dup() const { return nullptr; }
  void dup(Node *) { }

  void clear() { }
};
template <typename Node, bool Unique>
struct ZmRBTree_NodeExt :
    public ZuIf<Unique,
      ZmRBTree_NodeExt_Unique<Node>,
      ZmRBTree_NodeExt_Dup<Node>> {
  // 64bit pointer-packing - uses bit 63
  constexpr static uintptr_t Black() {
    return uintptr_t(1)<<63;
  }
  constexpr static uintptr_t Black(bool b) {
    return uintptr_t(b)<<63;
  }

  using Base = ZuIf<Unique,
    ZmRBTree_NodeExt_Unique<Node>,
    ZmRBTree_NodeExt_Dup<Node>>;

  ZuInline bool black() { return m_parent & Black(); }
  ZuInline void black(bool b) {
    m_parent = (m_parent & ~Black()) | Black(b);
  }
  ZuInline void black(const ZmRBTree_NodeExt *node) {
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

private:
  Node		*m_right = nullptr;
  Node		*m_left = nullptr;
  uintptr_t	m_parent = 0;
};

// ZmRBTree search and iteration comparators
enum {
  ZmRBTreeEqual = 0,
  ZmRBTreeGreaterEqual = 1,
  ZmRBTreeLessEqual = -1,
  ZmRBTreeGreater = 2,
  ZmRBTreeLess = -2
};

// red/black tree iterator - base
template <typename Tree_, int Direction_>
class ZmRBTreeIterator_ {
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

// read-write tree iterator
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
  using NodeMvRef = typename Tree::NodeMvRef;

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

  NodeMvRef del(Node *node) { return this->m_tree.delIterate(node); }
};

// read-only tree iterator
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

// red/black tree
template <typename T_, class NTP = ZmRBTree_Defaults>
class ZmRBTree : public ZmNodeFn<NTP::Shadow, typename NTP::Node> {
  template <typename, int> friend class ZmRBTreeIterator_;
  template <typename, int> friend class ZmRBTreeIterator;
  template <typename, int> friend class ZmRBTreeReadIterator;

public:
  using T = T_;
  constexpr static auto KeyAxor = NTP::KeyAxor;
  constexpr static auto ValAxor = NTP::ValAxor;
  using KeyRet = decltype(KeyAxor(ZuDeclVal<const T &>()));
  using ValRet = decltype(ValAxor(ZuDeclVal<const T &>()));
  using Key = ZuRDecay<KeyRet>;
  using Val = ZuRDecay<ValRet>;
  using Cmp = typename NTP::template CmpT<Key>;
  using ValCmp = typename NTP::template ValCmpT<Val>;
  enum { Unique = NTP::Unique };
  using Lock = typename NTP::Lock;
  using NodeBase = typename NTP::Node;
  enum { Shadow = NTP::Shadow };
  constexpr static auto HeapID = NTP::HeapID;
  enum { Sharded = NTP::Sharded };

private:
  using NodeFn = ZmNodeFn<Shadow, NodeBase>;

  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

public:
  template <int Direction = ZmRBTreeGreaterEqual>
  using Iterator = ZmRBTreeIterator<ZmRBTree, Direction>;
  template <int Direction = ZmRBTreeGreaterEqual>
  using ReadIterator = ZmRBTreeReadIterator<ZmRBTree, Direction>;

private:
public:
  struct Node;
  using Node_ = ZmNode<
    T, KeyAxor, ValAxor, NodeBase, ZmRBTree_NodeExt<Node, Unique>,
    HeapID, Sharded>;
  struct Node : public Node_ {
    using Node_::Node_;
    using Node_::operator =;
  };
  using NodeExt = ZmRBTree_NodeExt<Node, Unique>;
  using NodeRef = typename NodeFn::template Ref<Node>;
  using NodeMvRef = typename NodeFn::template MvRef<Node>;
  using NodePtr = Node *;

private:
  using NodeFn::nodeRef;
  using NodeFn::nodeDeref;
  using NodeFn::nodeDelete;
  using NodeFn::nodeAcquire;

  static KeyRet key(Node *node) {
    if (ZuLikely(node)) return node->Node::key();
    return ZuNullRef<Key, Cmp>();
  }
  static ValRet val(Node *node) {
    if (ZuLikely(node)) return node->Node::val();
    return ZuNullRef<Val, ValCmp>();
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
    return add(ZuFwdTuple(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }
  template <bool _ = !ZuInspect<NodeRef, Node *>::Same>
  ZuIfT<_> addNode(const NodeRef &node_) { addNode(node_.ptr()); }
  template <bool _ = !ZuInspect<NodeRef, Node *>::Same>
  ZuIfT<_> addNode(NodeRef &&node_) {
    Node *node = ZuMv(node_).release();
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
    if constexpr (Unique) ZmAssert(!newNode->NodeExt::dup());
    ZmAssert(!newNode->NodeExt::left());
    ZmAssert(!newNode->NodeExt::right());
    ZmAssert(!newNode->NodeExt::parent());
    ZmAssert(!newNode->NodeExt::black());

    Node *node;

    if (!(node = m_root)) {
      newNode->NodeExt::setBlack();
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

	  newNode->NodeExt::dup(child = node->NodeExt::dup());
	  if (child) child->NodeExt::parent(newNode);
	  node->NodeExt::dup(newNode);
	  newNode->NodeExt::parent(node);
	  ++m_count;
	  return;
	}
      }

      if (c >= 0) {
	if (!node->NodeExt::left()) {
	  node->NodeExt::left(newNode);
	  newNode->NodeExt::parent(node);
	  if (minimum) m_minimum = newNode;
	  break;
	}

	node = node->NodeExt::left();
	maximum = false;
      } else {
	if (!node->NodeExt::right()) {
	  node->NodeExt::right(newNode);
	  newNode->NodeExt::parent(node);
	  if (maximum) m_maximum = newNode;
	  break;
	}

	node = node->NodeExt::right();
	minimum = false;
      }
    }

    rebalance(newNode);
    ++m_count;
  }

  template <typename U, typename V = Key>
  struct IsKey : public ZuBool<ZuInspect<U, V>::Converts> { };
  template <typename U, typename R = void>
  using MatchKey = ZuIfT<IsKey<U>{}, R>;
  template <typename U, typename V = T, bool = ZuInspect<NodeBase, V>::Is>
  struct IsData : public ZuBool<!IsKey<U>{} && ZuInspect<U, V>::Converts> { };
  template <typename U, typename V>
  struct IsData<U, V, true> : public ZuFalse { };
  template <typename U, typename R = void>
  using MatchData = ZuIfT<IsData<U>{}, R>;

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
	  while (!match.equals(node)) if (!(node = node->NodeExt::dup())) break;
	  return node;
	}
      } else if (c > 0) {
	node = node->NodeExt::left();
      } else {
	node = node->NodeExt::right();
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
	node = node->NodeExt::left();
      } else {
	node = node->NodeExt::right();
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
	node = node->NodeExt::right();
      } else if (c > 0) {
	foundNode = node;
	node = node->NodeExt::left();
      } else {
	node = node->NodeExt::right();
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
	node = node->NodeExt::left();
      } else {
	foundNode = node;
	node = node->NodeExt::right();
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
	node = node->NodeExt::left();
      } else if (c > 0) {
	node = node->NodeExt::left();
      } else {
	foundNode = node;
	node = node->NodeExt::right();
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
    return find<Direction>(ZuFwdTuple(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
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
    return findKey<Direction>(ZuFwdTuple(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
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
    return findVal<Direction>(ZuFwdTuple(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

public:
  NodeRef minimum() const { ReadGuard guard(m_lock); return m_minimum; }
  Node *minimumPtr() const { ReadGuard guard(m_lock); return m_minimum; }
  Key minimumKey() const { ReadGuard guard(m_lock); return key(m_minimum); }
  Val minimumVal() const { ReadGuard guard(m_lock); return val(m_minimum); }

  NodeRef maximum() const { ReadGuard guard(m_lock); return m_maximum; }
  Node *maximumPtr() const { ReadGuard guard(m_lock); return m_maximum; }
  Key maximumKey() const { ReadGuard guard(m_lock); return key(m_maximum); }
  Val maximumVal() const { ReadGuard guard(m_lock); return val(m_maximum); }

  template <int Direction = ZmRBTreeEqual, typename P>
  MatchKey<P, NodeMvRef> del(const P &key) {
    ReadGuard guard(m_lock);
    Node *node = find_<Direction>(MatchKeyFn<P>{key});
    if (!node) return nullptr;
    delNode_(node);
    return nodeAcquire(node);
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, NodeMvRef> del(const P &data) {
    ReadGuard guard(m_lock);
    Node *node = find_<Direction>(MatchDataFn<P>{data});
    if (!node) return nullptr;
    delNode_(node);
    return nodeAcquire(node);
  }
  template <int Direction = ZmRBTreeEqual, typename P0, typename P1>
  NodeMvRef del(P0 &&p0, P1 &&p1) {
    return del<Direction>(ZuFwdTuple(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <int Direction = ZmRBTreeEqual, typename P>
  MatchKey<P, Key> delKey(const P &key) {
    ReadGuard guard(m_lock);
    NodeMvRef node = find_<Direction>(MatchKeyFn<P>{key});
    if (!node) return ZuNullRef<Key, Cmp>();
    delNode_(node);
    return ZuMv(*node).Node::key();
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, Key> delKey(const P &data) {
    ReadGuard guard(m_lock);
    NodeMvRef node = find_<Direction>(MatchDataFn<P>{data});
    if (!node) return ZuNullRef<Key, Cmp>();
    delNode_(node);
    return ZuMv(*node).Node::key();
  }
  template <int Direction = ZmRBTreeEqual, typename P0, typename P1>
  Key delKey(P0 &&p0, P1 &&p1) {
    return delKey<Direction>(ZuFwdTuple(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <int Direction = ZmRBTreeEqual, typename P>
  MatchKey<P, Val> delVal(const P &key) {
    ReadGuard guard(m_lock);
    NodeMvRef node = find_<Direction>(MatchKeyFn<P>{key});
    if (!node) return ZuNullRef<Val, ValCmp>();
    delNode_(node);
    return ZuMv(*node).Node::val();
  }
  template <int Direction = ZmRBTreeEqual, typename P>
  MatchData<P, Val> delVal(const P &data) {
    ReadGuard guard(m_lock);
    NodeMvRef node = find_<Direction>(MatchDataFn<P>{data});
    if (!node) return ZuNullRef<Val, ValCmp>();
    delNode_(node);
    return ZuMv(*node).Node::val();
  }
  template <int Direction = ZmRBTreeEqual, typename P0, typename P1>
  Val delVal(P0 &&p0, P1 &&p1) {
    return delVal<Direction>(ZuFwdTuple(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  NodeMvRef delNode(Node *node) {
    if (ZuUnlikely(!node)) return nullptr;
    Guard guard(m_lock);
    delNode_(node);
    return nodeAcquire(node);
  }

private:
  void delNode_(Node *node) {
    if constexpr (!Unique) {
      Node *parent = node->NodeExt::parent();
      Node *dup = node->NodeExt::dup();

      if (parent && parent->NodeExt::dup() == node) {
	parent->NodeExt::dup(dup);
	if (dup) dup->NodeExt::parent(parent);
	--m_count;
	node->NodeExt::clearDup();
	return;
      }
      if (dup) {
	{
	  Node *child;

	  dup->NodeExt::left(child = node->NodeExt::left());
	  if (child) {
	    node->NodeExt::left(nullptr);
	    child->NodeExt::parent(dup);
	  }
	  dup->NodeExt::right(child = node->NodeExt::right());
	  if (child) {
	    node->NodeExt::right(nullptr);
	    child->NodeExt::parent(dup);
	  }
	}
	if (!parent) {
	  m_root = dup;
	  dup->NodeExt::parent(0);
	} else if (node == parent->NodeExt::right()) {
	  parent->NodeExt::right(dup);
	  dup->NodeExt::parent(parent);
	} else {
	  parent->NodeExt::left(dup);
	  dup->NodeExt::parent(parent);
	}
	dup->NodeExt::black(node);
	if (node == m_minimum) m_minimum = dup;
	if (node == m_maximum) m_maximum = dup;
	--m_count;
	node->NodeExt::clearDup();
	return;
      }
    }
    delRebalance(node);
    node->NodeExt::clear();
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

  void clean() { clean([](auto) { }); }
  template <typename L> void clean(L l) {
    Guard guard(m_lock);
    clean_(ZuMv(l));
    m_minimum = m_maximum = m_root = nullptr;
    m_count = 0;
  }
private:
  void clean_() { clean_([](auto) { }); }
  template <typename L> void clean_(L l) {
    Node *node = m_minimum, *next;
    if (!node) return;
    do {
      if (next = node->NodeExt::left()) { node = next; continue; }
      if (next = node->NodeExt::dup()) { node = next; continue; }
      if (next = node->NodeExt::right()) { node = next; continue; }
      if (next = node->NodeExt::parent()) {
	if (node == next->NodeExt::left())
	  next->NodeExt::left(nullptr);
	else if constexpr (!Unique) {
	  if (node == next->NodeExt::dup())
	    next->NodeExt::dup(nullptr);
	  else
	    next->NodeExt::right(nullptr);
	} else
	  next->NodeExt::right(nullptr);
      }
      l(NodeMvRef{nodeAcquire(node)});
      node = next;
    } while (node);
  }

  void rotateRight(Node *node, Node *parent) {
    Node *left = node->NodeExt::left();
    Node *mid = left->NodeExt::right();

  // move left to the right, under node's parent
  // (make left the root if node is the root)

    if (parent) {
      if (parent->NodeExt::left() == node)
	parent->NodeExt::left(left);
      else
	parent->NodeExt::right(left);
    } else
      m_root = left;
    left->NodeExt::parent(parent);

  // node descends to left's right

    left->NodeExt::right(node), node->NodeExt::parent(left);

  // mid switches from left's right to node's left

    node->NodeExt::left(mid); if (mid) mid->NodeExt::parent(node);
  }

  void rotateLeft(Node *node, Node *parent) {
    Node *right = node->NodeExt::right();
    Node *mid = right->NodeExt::left();

  // move right to the left, under node's parent
  // (make right the root if node is the root)

    if (parent) {
      if (parent->NodeExt::right() == node)
	parent->NodeExt::right(right);
      else
	parent->NodeExt::left(right);
    } else
      m_root = right;
    right->NodeExt::parent(parent);

  // node descends to right's left

    right->NodeExt::left(node), node->NodeExt::parent(right);

  // mid switches from right's left to node's right

    node->NodeExt::right(mid); if (mid) mid->NodeExt::parent(node);
  }

  void rebalance(Node *node) {

  // rebalance until we hit a black node (the root is always black)

    for (;;) {
      Node *parent = node->NodeExt::parent();

      if (!parent) { node->NodeExt::setBlack(); return; }// force root to black

      if (parent->NodeExt::black()) return;

      Node *gParent = parent->NodeExt::parent();

      if (parent == gParent->NodeExt::left()) {
	Node *uncle = gParent->NodeExt::right();

	if (uncle && !uncle->NodeExt::black()) {
	  parent->NodeExt::setBlack();
	  uncle->NodeExt::setBlack();
	  (node = gParent)->NodeExt::clrBlack();
	} else {
	  if (node == parent->NodeExt::right()) {
	    rotateLeft(node = parent, gParent);
	    gParent = (parent = node->NodeExt::parent())->NodeExt::parent();
	  }
	  parent->NodeExt::setBlack();
	  gParent->NodeExt::clrBlack();
	  rotateRight(gParent, gParent->NodeExt::parent());
	  m_root->NodeExt::setBlack();			// force root to black
	  return;
	}
      } else {
	Node *uncle = gParent->NodeExt::left();

	if (uncle && !uncle->NodeExt::black()) {
	  parent->NodeExt::setBlack();
	  uncle->NodeExt::setBlack();
	  (node = gParent)->NodeExt::clrBlack();
	} else {
	  if (node == parent->NodeExt::left()) {
	    rotateRight(node = parent, gParent);
	    gParent = (parent = node->NodeExt::parent())->NodeExt::parent();
	  }
	  parent->NodeExt::setBlack();
	  gParent->NodeExt::clrBlack();
	  rotateLeft(gParent, gParent->NodeExt::parent());
	  m_root->NodeExt::setBlack();			// force root to black
	  return;
	}
      }
    }
  }

  void delRebalance(Node *node) {
    Node *successor = node;
    Node *child, *parent;

    if (!successor->NodeExt::left())
      child = successor->NodeExt::right();
    else if (!successor->NodeExt::right())
      child = successor->NodeExt::left();
    else {
      successor = successor->NodeExt::right();
      while (successor->NodeExt::left()) successor = successor->NodeExt::left();
      child = successor->NodeExt::right();
    }

    if (successor != node) {
      node->NodeExt::left()->NodeExt::parent(successor);
      successor->NodeExt::left(node->NodeExt::left());
      if (successor != node->NodeExt::right()) {
	parent = successor->NodeExt::parent();
	if (child) child->NodeExt::parent(parent);
	successor->NodeExt::parent()->NodeExt::left(child);
	successor->NodeExt::right(node->NodeExt::right());
	node->NodeExt::right()->NodeExt::parent(successor);
      } else
	parent = successor;

      Node *childParent = parent;

      parent = node->NodeExt::parent();

      if (!parent)
	m_root = successor;
      else if (node == parent->NodeExt::left())
	parent->NodeExt::left(successor);
      else
	parent->NodeExt::right(successor);
      successor->NodeExt::parent(parent);

      bool black = node->NodeExt::black();

      node->NodeExt::black(successor);
      successor->NodeExt::black(black);

      successor = node;

      parent = childParent;
    } else {
      parent = node->NodeExt::parent();

      if (child) child->NodeExt::parent(parent);

      if (!parent)
	m_root = child;
      else if (node == parent->NodeExt::left())
	parent->NodeExt::left(child);
      else
	parent->NodeExt::right(child);

      if (node == m_minimum) {
	if (!node->NodeExt::right())
	  m_minimum = parent;
	else {
	  Node *minimum = child;

	  do {
	    m_minimum = minimum;
	  } while (minimum = minimum->NodeExt::left());
	}
      }

      if (node == m_maximum) {
	if (!node->NodeExt::left())
	  m_maximum = parent;
	else {
	  Node *maximum = child;

	  do {
	    m_maximum = maximum;
	  } while (maximum = maximum->NodeExt::right());
	}
      }
    }

    if (successor->NodeExt::black()) {
      Node *sibling;

      while (parent && (!child || child->NodeExt::black()))
	if (child == parent->NodeExt::left()) {
	  sibling = parent->NodeExt::right();
	  if (!sibling->NodeExt::black()) {
	    sibling->NodeExt::setBlack();
	    parent->NodeExt::clrBlack();
	    rotateLeft(parent, parent->NodeExt::parent());
	    sibling = parent->NodeExt::right();
	  }
	  if ((!sibling->NodeExt::left() ||
		sibling->NodeExt::left()->NodeExt::black()) &&
	      (!sibling->NodeExt::right() ||
		sibling->NodeExt::right()->NodeExt::black())) {
	    sibling->NodeExt::clrBlack();
	    child = parent;
	    parent = child->NodeExt::parent();
	  } else {
	    if (!sibling->NodeExt::right() ||
		sibling->NodeExt::right()->NodeExt::black()) {
	      if (sibling->NodeExt::left())
		sibling->NodeExt::left()->NodeExt::setBlack();
	      sibling->NodeExt::clrBlack();
	      rotateRight(sibling, parent);
	      sibling = parent->NodeExt::right();
	    }
	    sibling->NodeExt::black(parent);
	    parent->NodeExt::setBlack();
	    if (sibling->NodeExt::right())
	      sibling->NodeExt::right()->NodeExt::setBlack();
	    rotateLeft(parent, parent->NodeExt::parent());
	    break;
	  }
	} else {
	  sibling = parent->NodeExt::left();
	  if (!sibling->NodeExt::black()) {
	    sibling->NodeExt::setBlack();
	    parent->NodeExt::clrBlack();
	    rotateRight(parent, parent->NodeExt::parent());
	    sibling = parent->NodeExt::left();
	  }
	  if ((!sibling->NodeExt::right() ||
		sibling->NodeExt::right()->NodeExt::black()) &&
	      (!sibling->NodeExt::left() ||
		sibling->NodeExt::left()->NodeExt::black())) {
	    sibling->NodeExt::clrBlack();
	    child = parent;
	    parent = child->NodeExt::parent();
	  } else {
	    if (!sibling->NodeExt::left() ||
		sibling->NodeExt::left()->NodeExt::black()) {
	      if (sibling->NodeExt::right())
		sibling->NodeExt::right()->NodeExt::setBlack();
	      sibling->NodeExt::clrBlack();
	      rotateLeft(sibling, parent);
	      sibling = parent->NodeExt::left();
	    }
	    sibling->NodeExt::black(parent);
	    parent->NodeExt::setBlack();
	    if (sibling->NodeExt::left())
	      sibling->NodeExt::left()->NodeExt::setBlack();
	    rotateRight(parent, parent->NodeExt::parent());
	    break;
	  }
	}
      if (child) child->NodeExt::setBlack();
    }
  }

public:
  Node *next(Node *node) {
    Node *next;

    if constexpr (!Unique) {
      if (next = node->NodeExt::dup()) return next;

      if (next = node->NodeExt::parent())
	while (node == next->NodeExt::dup()) {
	  node = next;
	  if (!(next = node->NodeExt::parent())) break;
	}
    }

    if (next = node->NodeExt::right()) {
      node = next;
      while (node = node->NodeExt::left()) next = node;
      return next;
    }

    if (!(next = node->NodeExt::parent())) return nullptr;

    while (node == next->NodeExt::right()) {
      node = next;
      if (!(next = node->NodeExt::parent())) return nullptr;
    }

    return next;
  }

  Node *prev(Node *node) {
    Node *prev;

    if constexpr (!Unique) {
      if (prev = node->NodeExt::dup()) return prev;

      if (prev = node->NodeExt::parent())
	while (node == prev->NodeExt::dup()) {
	  node = prev;
	  if (!(prev = node->NodeExt::parent())) break;
	}
    }

    if (prev = node->NodeExt::left()) {
      node = prev;
      while (node = node->NodeExt::right()) prev = node;
      return prev;
    }

    if (!(prev = node->NodeExt::parent())) return nullptr;

    while (node == prev->NodeExt::left()) {
      node = prev;
      if (!(prev = node->NodeExt::parent())) return nullptr;
    }

    return prev;
  }

private:

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
    iterator.m_node = node->NodeExt::dup();
    return node;
  }
  template <int Direction>
  ZuIfT<(Direction < 0), Node *> iterate(Iterator_<Direction> &iterator) {
    Node *node = iterator.m_node;
    if (!node) return nullptr;
    iterator.m_node = prev(node);
    return node;
  }

  NodeMvRef delIterate(Node *node) {
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
  ZmRBTree<ZuTuple<P0, P1>,
    ZmRBTreeKeyVal<ZuTupleAxor<0>(), ZuTupleAxor<1>(), NTP>>;

#endif /* ZmRBTree_HH */
