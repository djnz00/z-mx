//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// chained hash table (policy-based)
//
// separately chained with linked lists, optionally locked, lock striping
//
// - intentionally disdains range-based for() and structured binding
// - globally configured sizing, lock striping and heap configuration
//   - see ZmHashMgr
//   - supports profile-guided optimization of heap and hash configuration
// - efficient statistics and telemetry (Ztel)

#ifndef ZmHash_HH
#define ZmHash_HH

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <zlib/ZuNull.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuHash.hh>
#include <zlib/ZuInspect.hh>

#include <zlib/ZmNoLock.hh>
#include <zlib/ZmAtomic.hh>
#include <zlib/ZmGuard.hh>
#include <zlib/ZmRef.hh>
#include <zlib/ZmHeap.hh>
#include <zlib/ZmLock.hh>
#include <zlib/ZmLockTraits.hh>
#include <zlib/ZmNode.hh>
#include <zlib/ZmNodeFn.hh>

#include <zlib/ZmHashMgr.hh>

// hash bits function

// some hash functions have flatter high-bits distribution
// (e.g. multiplicative hash functions), others have flatter
// low-bits (e.g. FNV string hash); here we pay for an extra xor
// to get the best of both
inline constexpr const uint32_t ZmHashBits(uint32_t code, unsigned bits) {
  bits = 32U - bits;
  return ((code<<bits) ^ code)>>bits;
}

// hash table lock manager

template <typename Lock> class ZmHash_LockMgr {
  using LockTraits = ZmLockTraits<Lock>;

  enum { CacheLineSize = Zm::CacheLineSize };

  ZuAssert(sizeof(Lock) <= CacheLineSize);

  Lock &lock_(unsigned i) const {
    return *reinterpret_cast<Lock *>(
	reinterpret_cast<uint8_t *>(m_locks) + (i * CacheLineSize));
  }

public:
  unsigned bits() const { return m_bits; }
  unsigned cBits() const { return m_cBits; }

protected:
  ZmHash_LockMgr() { }

  void init(const ZmHashParams &params) {
    bits(params.bits());
    cBits(params.cBits());

    if (m_cBits > m_bits) m_cBits = m_bits;
    unsigned n = 1U<<m_cBits;
    unsigned size = n * CacheLineSize;
    m_locks = Zm::alignedAlloc<CacheLineSize>(size);
    if (!m_locks) throw std::bad_alloc{};
    for (unsigned i = 0; i < n; ++i) new (&lock_(i)) Lock();
  }

  ~ZmHash_LockMgr() {
    if (ZuUnlikely(!m_locks)) return;
    unsigned n = 1U<<m_cBits;
    for (unsigned i = 0; i < n; ++i) lock_(i).~Lock();
    Zm::alignedFree(m_locks);
  }

  void bits(unsigned n)  {  m_bits = n < 2 ? 2 : n > 28 ? 28 : n; }
  void cBits(unsigned n) { m_cBits = n < 0 ? 0 : n > 12 ? 12 : n; }

  Lock &lockCode(uint32_t code) const {
    return lockSlot(ZmHashBits(code, m_bits));
  }
  Lock &lockSlot(unsigned slot) const {
    return lock_(slot>>(m_bits - m_cBits));
  }

  bool lockAllResize(unsigned bits) {
    for (unsigned i = 0; i < (1U<<m_cBits); i++) {
      LockTraits::lock(lock_(i));
      // concurrent resize() occurred, m_bits changed, abandon attempt
      if (m_bits >= bits) {
	for (int j = i; j >= 0; --j) LockTraits::unlock(lock_(j));
	return false;
      }
    }
    return true;
  }
  void lockAll() {
    unsigned n = (1U<<m_cBits);
    for (unsigned i = 0; i < n; i++) LockTraits::lock(lock_(i));
  }
  void unlockAll() {
    unsigned n = (1U<<m_cBits);
    for (int i = n; --i >= 0; ) LockTraits::unlock(lock_(i));
  }

protected:
  unsigned	m_bits = 2;
private:
  unsigned	m_cBits = 2;
  mutable void	*m_locks = nullptr;
};

template <> class ZmHash_LockMgr<ZmNoLock> {
protected:
  ZmHash_LockMgr() { }
  ~ZmHash_LockMgr() { }

  void init(const ZmHashParams &params) { bits(params.bits()); }

public:
  unsigned bits() const { return m_bits; }
  static constexpr unsigned cBits() { return 0; }

protected:
  void bits(unsigned n) { m_bits = n < 2 ? 2 : n > 28 ? 28 : n; }
  void cBits(unsigned) { }

  ZmNoLock &lockCode(uint32_t code) const {
    return const_cast<ZmNoLock &>(ZuNullRef<ZmNoLock>());
  }
  ZmNoLock &lockSlot(unsigned slot) const {
    return const_cast<ZmNoLock &>(ZuNullRef<ZmNoLock>());
  }

  bool lockAllResize(unsigned) { return true; }
  void lockAll() { }
  void unlockAll() { }

protected:
  unsigned	m_bits = 2;
};

// NTP (named template parameters):
//
// ZmHashKV<ZtString, ZtString,		// key, value pair of ZtStrings
//   ZmHashKeyCmp<ZuICmp> >		// case-insensitive comparison

// NTP defaults
struct ZmHash_Defaults {
  static constexpr auto KeyAxor = ZuDefaultAxor();
  static constexpr auto ValAxor = ZuDefaultAxor();
  template <typename T> using CmpT = ZuCmp<T>;
  template <typename T> using ValCmpT = ZuCmp<T>;
  template <typename T> using HashFnT = ZuHash<T>;
  using Lock = ZmNoLock;
  using Node = ZuNull;
  enum { Shadow = 0 };
  static const char *ID() { return "ZmHash"; }
  static const char *HeapID() { return "ZmHash"; }
  enum { Sharded = 0 };
};

// ZmHashKey - key accessor
template <auto KeyAxor_, typename NTP = ZmHash_Defaults>
struct ZmHashKey : public NTP {
  static constexpr auto KeyAxor = KeyAxor_;
};

// ZmHashKeyVal - key and optional value accessors
template <auto KeyAxor_, auto ValAxor_, typename NTP = ZmHash_Defaults>
struct ZmHashKeyVal : public NTP {
  static constexpr auto KeyAxor = KeyAxor_;
  static constexpr auto ValAxor = ValAxor_;
};

// ZmHashCmp - the comparator
template <template <typename> typename Cmp_, typename NTP = ZmHash_Defaults>
struct ZmHashCmp : public NTP {
  template <typename T> using CmpT = Cmp_<T>;
};

// ZmHashValCmp - the optional value comparator
template <template <typename> typename ValCmp_, typename NTP = ZmHash_Defaults>
struct ZmHashValCmp : public NTP {
  template <typename T> using ValCmpT = ValCmp_<T>;
};

// ZmHashFn - the hash function
template <template <typename> typename HashFn_, typename NTP = ZmHash_Defaults>
struct ZmHashFn : public NTP {
  template <typename T> using HashFnT = HashFn_<T>;
};

// ZmHashLock - the lock type used (ZmRWLock will permit concurrent reads)
template <typename Lock_, typename NTP = ZmHash_Defaults>
struct ZmHashLock : public NTP {
  using Lock = Lock_;
};

// ZmHashNode - the base type for nodes
template <typename Node_, typename NTP = ZmHash_Defaults>
struct ZmHashNode : public NTP {
  using Node = Node_;
};

// ZmHashID - the hash ID - also sets ZmHashHeapID if that remains the default
template <auto ID_, typename NTP = ZmHash_Defaults, auto HeapID_ = NTP::HeapID>
struct ZmHashID : public NTP {
  static constexpr auto ID = ID_;
};
template <auto ID_, typename NTP>
struct ZmHashID<ID_, NTP, ZmHash_Defaults::HeapID> : public NTP {
  static constexpr auto ID = ID_;
  static constexpr auto HeapID = ID_;
};

// ZmHashShadow - shadow nodes, do not manage ownership
template <bool Shadow_, typename NTP = ZmHash_Defaults>
struct ZmHashShadow : public NTP {
  enum { Shadow = Shadow_ };
  static constexpr auto HeapID = ZmHeapDisable();
};

// ZmHashHeapID - the heap ID - also sets ZmHashID if that remains the default
template <auto HeapID_, typename NTP = ZmHash_Defaults, auto ID_ = NTP::ID>
struct ZmHashHeapID : public NTP {
  static constexpr auto HeapID = HeapID_;
};
template <auto HeapID_, typename NTP>
struct ZmHashHeapID<HeapID_, NTP, ZmHash_Defaults::ID> : public NTP {
  static constexpr auto ID = HeapID_;
  static constexpr auto HeapID = HeapID_;
};
template <typename NTP, auto ID_>
struct ZmHashHeapID<ZmHeapDisable(), NTP, ID_> : public NTP {
  static constexpr auto HeapID = ZmHeapDisable();
};
template <typename NTP>
struct ZmHashHeapID<ZmHeapDisable(), NTP, ZmHash_Defaults::ID> : public NTP {
  static constexpr auto HeapID = ZmHeapDisable();
};

// ZmHashSharded - sharded heap
template <bool Sharded_, typename NTP = ZmHash_Defaults>
struct ZmHashSharded : public NTP {
  enum { Sharded = Sharded_ };
};

// ZmHash node
template <typename Node>
struct ZmHash_NodeExt {
  Node	*next = nullptr;
};

// compile-time check if cmp is static
template <typename T, typename Cmp, typename = void>
struct ZmHash_IsStaticCmp : public ZuFalse { };
template <typename T, typename Cmp>
struct ZmHash_IsStaticCmp<T, Cmp, decltype(
  Cmp::cmp(ZuDeclVal<const T &>(), ZuDeclVal<const T &>()),
  void())> : public ZuTrue { };
// compile-time check if equals is static
template <typename T, typename Cmp, typename = void>
struct ZmHash_IsStaticEquals : public ZuFalse { };
template <typename T, typename Cmp>
struct ZmHash_IsStaticEquals<T, Cmp, decltype(
  Cmp::equals(ZuDeclVal<const T &>(), ZuDeclVal<const T &>()),
  void())> : public ZuTrue { };

template <typename T_, typename NTP = ZmHash_Defaults>
class ZmHash :
    public ZmAnyHash,
    public ZmHash_LockMgr<typename NTP::Lock>,
    public ZmNodeFn<NTP::Shadow, typename NTP::Node> {
public:
  using T = T_;
  static constexpr auto KeyAxor = NTP::KeyAxor;
  static constexpr auto ValAxor = NTP::ValAxor;
  using KeyRet = decltype(KeyAxor(ZuDeclVal<const T &>()));
  using ValRet = decltype(ValAxor(ZuDeclVal<const T &>()));
  using Key = ZuRDecay<KeyRet>;
  using Val = ZuRDecay<ValRet>;
  using Cmp = typename NTP::template CmpT<Key>;
  using ValCmp = typename NTP::template ValCmpT<Val>;
  using HashFn = typename NTP::template HashFnT<Key>;
  using Lock = typename NTP::Lock;
  using NodeBase = typename NTP::Node;
  static constexpr auto ID = NTP::ID;
  enum { Shadow = NTP::Shadow };
  static constexpr auto HeapID = NTP::HeapID;
  enum { Sharded = NTP::Sharded };

private:
  using LockMgr = ZmHash_LockMgr<Lock>;
  using NodeFn = ZmNodeFn<Shadow, NodeBase>;

  using LockTraits = ZmLockTraits<Lock>;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  using LockMgr::lockCode;
  using LockMgr::lockSlot;
  using LockMgr::lockAllResize;
  using LockMgr::lockAll;
  using LockMgr::unlockAll;

  using LockMgr::m_bits;

public:
  unsigned bits() const { return m_bits; }
  using LockMgr::cBits;

  struct Node;
  using Node_ = ZmNode<
    T, KeyAxor, ValAxor, NodeBase, ZmHash_NodeExt<Node>, HeapID, Sharded>;
  struct Node : public Node_ {
    using Node_::Node_;
    using Node_::operator =;
  };
  using NodeExt = ZmHash_NodeExt<Node>;
  using NodeRef = typename NodeFn::template Ref<Node>;
  using NodeMvRef = typename NodeFn::template MvRef<Node>;
  using NodePtr = Node *;

  using NodeFn::nodeRef;
  using NodeFn::nodeDeref;
  using NodeFn::nodeAcquire;
  using NodeFn::nodeDelete;

  static KeyRet key(const Node *node) {
    if (ZuLikely(node)) return node->Node::key();
    return ZuNullRef<Key, Cmp>();
  }
  Key keyMv(NodeMvRef node) {
    if (ZuLikely(node)) return ZuMv(*node).Node::key();
    return ZuNullRef<Key, Cmp>();
  }
  static ValRet val(const Node *node) {
    if (ZuLikely(node)) return node->Node::val();
    return ZuNullRef<Val, ValCmp>();
  }
  Val valMv(NodeMvRef node) {
    if (ZuLikely(node)) return ZuMv(*node).Node::val();
    return ZuNullRef<Val, ValCmp>();
  }

private:
  template <typename I> struct Iterator__ { // CRTP
    decltype(auto) iterateKey() {
      return key(static_cast<I *>(this)->iterate());
    }
    decltype(auto) iterateVal() {
      return val(static_cast<I *>(this)->iterate());
    }
  };

  template <typename I> class Iterator_;
template <typename> friend class Iterator_;
  template <typename I> class Iterator_ : public Iterator__<I> { // CRTP
    using Hash = ZmHash<T, NTP>;
  friend Hash;

    Iterator_(const Iterator_ &) = delete;
    Iterator_ &operator =(const Iterator_ &) = delete;

  protected:
    Hash			&hash;
    int				slot = -1;
    typename Hash::Node		*node = nullptr;
    typename Hash::Node		*prev = nullptr;

    Iterator_(Iterator_ &&) = default;
    Iterator_ &operator =(Iterator_ &&) = default;

    Iterator_(Hash &hash_) : hash{hash_} { }

  public:
    void reset() { hash.startIterate(static_cast<I &>(*this)); }
    Node *iterate() { return hash.iterate(static_cast<I &>(*this)); }

    unsigned count() const { return hash.count_(); }
  };

  template <typename, typename> class KeyIterator_;
template <typename, typename> friend class KeyIterator_;
  template <typename I, typename IKey_>
  class KeyIterator_ : public Iterator_<I> { // CRTP
    using Hash = ZmHash<T, NTP>;
  friend class ZmHash<T, NTP>;

    KeyIterator_(const KeyIterator_ &) = delete;
    KeyIterator_ &operator =(const KeyIterator_ &) = delete;

  public:
    using IKey = IKey_;

  protected:
    IKey	key;

    using Iterator_<I>::hash;

    KeyIterator_(KeyIterator_ &&) = default;
    KeyIterator_ &operator =(KeyIterator_ &&) = default;

    template <typename IKey__>
    KeyIterator_(Hash &hash_, IKey__ &&key_) :
      Iterator_<I>{hash_}, key{ZuFwd<IKey__>(key_)} { }

  public:
    void reset() { hash.startKeyIterate(static_cast<I &>(*this)); }
    Node *iterate() { return hash.keyIterate(static_cast<I &>(*this)); }
  };

public:
  class Iterator : public Iterator_<Iterator> {
    Iterator(const Iterator &) = delete;
    Iterator &operator =(const Iterator &) = delete;

    using Hash = ZmHash<T, NTP>;
  friend Hash;

    using Base = Iterator_<Iterator>;
    using Base::hash;

  public:
    Iterator(Iterator &&) = default;
    Iterator &operator =(Iterator &&) = default;

    Iterator(Hash &hash_) : Base{hash_} { hash.startIterate(*this); }
    ~Iterator() { hash.endIterate(*this); }

    NodeMvRef del() { return hash.delIterate(*this); }
  };

  class ReadIterator : public Iterator_<ReadIterator> {
    ReadIterator(const ReadIterator &) = delete;
    ReadIterator &operator =(const ReadIterator &) = delete;

    using Hash = ZmHash<T, NTP>;
  friend Hash;

    using Base = Iterator_<ReadIterator>;
    using Base::hash;

  public:
    ReadIterator(ReadIterator &&) = default;
    ReadIterator &operator =(ReadIterator &&) = default;

    ReadIterator(const Hash &hash_) : Base{const_cast<Hash &>(hash_)} {
      const_cast<Hash &>(hash).startIterate(*this);
    }
    ~ReadIterator() { hash.endIterate(*this); }
  };

  template <typename IKey_>
  class KeyIterator : public KeyIterator_<KeyIterator<IKey_>, IKey_> {
    KeyIterator(const KeyIterator &) = delete;
    KeyIterator &operator =(const KeyIterator &) = delete;

    using Hash = ZmHash<T, NTP>;
  friend Hash;

    using Base = KeyIterator_<KeyIterator<IKey_>, IKey_>;
    using typename Base::IKey;
    using Base::key;
    using Base::hash;

  public:
    KeyIterator(KeyIterator &&) = default;
    KeyIterator &operator =(KeyIterator &&) = default;

    template <typename IKey__>
    KeyIterator(Hash &hash_, IKey__ &&key_) :
      Base{hash_, ZuFwd<IKey__>(key_)}
    {
      hash.startKeyIterate(*this);
    }
    ~KeyIterator() { hash.endIterate(*this); }
    NodeMvRef del() { return hash.delIterate(*this); }
  };

  template <typename IKey_>
  class ReadKeyIterator : public KeyIterator_<ReadKeyIterator<IKey_>, IKey_> {
    ReadKeyIterator(const ReadKeyIterator &) = delete;
    ReadKeyIterator &operator =(const ReadKeyIterator &) = delete;

    using Hash = ZmHash<T, NTP>;
  friend Hash;
    using Base = KeyIterator_<ReadKeyIterator<IKey_>, IKey_>;

    using typename Base::IKey;
    using Base::key;
    using Base::hash;

  public:
    ReadKeyIterator(ReadKeyIterator &&) = default;
    ReadKeyIterator &operator =(ReadKeyIterator &&) = default;

    template <typename IKey__>
    ReadKeyIterator(const Hash &hash_, IKey__ &&key_) :
	Base{const_cast<Hash &>(hash_), ZuFwd<IKey__>(key_)} {
      const_cast<Hash &>(hash).startKeyIterate(*this);
    }
    ~ReadKeyIterator() { hash.endIterate(*this); }
  };

private:
  void init(const ZmHashParams &params) {
    double loadFactor = params.loadFactor();
    if (loadFactor < 1.0) loadFactor = 1.0;
    m_loadFactor = (unsigned)(loadFactor * (1<<4));
    m_table = static_cast<NodePtr *>(
      Zm::alignedAlloc<Zm::CacheLineSize>(sizeof(NodePtr)<<m_bits));
    memset(m_table, 0, sizeof(NodePtr)<<m_bits);
    ZmHashMgr::add(this);
  }

public:
  // for maximum usability, accept any/all combinations of ID, cmp, params
  ZmHash() : m_id{ID()} {
    auto params = ZmHashParams{m_id};
    LockMgr::init(params);
    ZmHash::init(params);
  }
  // ID
  template <
    typename ID,
    decltype(ZuIfT<ZuTraits<ID>::IsString>(), int()) = 0>
  ZmHash(const ID &id) : m_id{id} {
    auto params = ZmHashParams{m_id};
    LockMgr::init(params);
    ZmHash::init(params);
  }
  // cmp
  template <
    typename Cmp_,
    decltype(ZuIfT<ZuIsExact<Cmp_, Cmp>{}>(), int()) = 0>
  ZmHash(Cmp_ cmp) : m_id{ID()}, m_cmp{ZuMv(cmp)} {
    auto params = ZmHashParams{m_id};
    LockMgr::init(params);
    ZmHash::init(params);
  }
  // params
  template <
    typename Params,
    decltype(ZuIfT<ZuIsExact<Params, ZmHashParams>{}>(), int()) = 0>
  ZmHash(const Params &params) : m_id{ID()} {
    LockMgr::init(params);
    ZmHash::init(params);
  }
  // ID, cmp
  template <
    typename ID,
    typename Cmp_,
    decltype(ZuIfT<
      ZuTraits<ID>::IsString &&
      bool(ZuIsExact<Cmp_, Cmp>{})>(), int()) = 0>
  ZmHash(const ID &id, Cmp_ cmp) : m_id{id}, m_cmp{ZuMv(cmp)} {
    auto params = ZmHashParams{m_id};
    LockMgr::init(params);
    ZmHash::init(params);
  }
  // cmp, params
  template <
    typename Cmp_,
    typename Params,
    decltype(ZuIfT<
      bool(ZuIsExact<Cmp_, Cmp>{}) &&
      bool(ZuIsExact<Params, ZmHashParams>{})>(), int()) = 0>
  ZmHash(Cmp_ cmp, const Params &params) : m_id{ID()}, m_cmp{ZuMv(cmp)} {
    LockMgr::init(params);
    ZmHash::init(params);
  }
  // ID, params
  template <
    typename ID,
    typename Params,
    decltype(ZuIfT<
      ZuTraits<ID>::IsString &&
      bool(ZuIsExact<Params, ZmHashParams>{})>(), int()) = 0>
  ZmHash(
    const ID &id,
    const Params &params) : m_id{id}
  {
    LockMgr::init(params);
    ZmHash::init(params);
  }
  // ID, cmp, params
  template <
    typename ID,
    typename Cmp_,
    typename Params,
    decltype(ZuIfT<
      ZuTraits<ID>::IsString &&
      bool(ZuIsExact<Cmp_, Cmp>{}) &&
      bool(ZuIsExact<Params, ZmHashParams>{})>(), int()) = 0>
  ZmHash(
    const ID &id,
    Cmp_ cmp,
    const Params &params) : m_id{id}, m_cmp(ZuMv(cmp))
  {
    LockMgr::init(params);
    ZmHash::init(params);
  }

  ZmHash(const ZmHash &) = delete;
  ZmHash &operator =(const ZmHash &) = delete;
  ZmHash(ZmHash &&) = delete;
  ZmHash &operator =(ZmHash &&) = delete;

  ~ZmHash() {
    ZmHashMgr::del(this);
    clean();
    Zm::alignedFree(m_table);
  }

  unsigned loadFactor_() const { return m_loadFactor; }
  double loadFactor() const {
    return static_cast<double>(m_loadFactor) / 16.0;
  }
  unsigned size() const {
    return static_cast<double>(static_cast<uint64_t>(1)<<m_bits) * loadFactor();
  }

  // intentionally unlocked and non-atomic
  unsigned count_() const { return m_count.load_(); }

  template <typename P>
  NodeRef add(P &&data) {
    Node *node = new Node{ZuFwd<P>(data)};
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
    Node *node = this->nodeRelease(ZuMv(node_));
    uint32_t code = HashFn::hash(node->Node::key());
    Guard guard(lockCode(code));
    addNode_(node, code);
  }
  void addNode(Node *node) {
    uint32_t code = HashFn::hash(node->Node::key());
    nodeRef(node);
    Guard guard(lockCode(code));
    addNode_(node, code);
  }

private:
  void addNode_(Node *node, uint32_t code) {
    unsigned count = m_count.load_();

    {
      unsigned bits = m_bits;

      if (count < (1U<<28) && ((count<<4)>>bits) >= m_loadFactor) {
	Lock &lock = lockCode(code);

	LockTraits::unlock(lock);
	resize(bits + 1);
	LockTraits::lock(lock);
      }
    }

    unsigned slot = ZmHashBits(code, m_bits);

    node->NodeExt::next = m_table[slot];
    m_table[slot] = ZuMv(node);
    m_count.store_(count + 1);
  }

private:
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

public:
  template <
    typename Key_ = Key,
    typename Cmp_ = Cmp,
    decltype(ZuIfT<ZmHash_IsStaticCmp<Key_, Cmp_>{}>(), int()) = 0>
  static ZuInline auto cmp(const Key &l, const Key &r) {
    return Cmp::cmp(l, r);
  }
  template <
    typename Key_ = Key,
    typename Cmp_ = Cmp,
    decltype(ZuIfT<!ZmHash_IsStaticCmp<Key_, Cmp_>{}>(), int()) = 0>
  auto ZuInline cmp(const Key &l, const Key &r) const {
    return m_cmp.cmp(l, r);
  }
  template <
    typename Key_ = Key,
    typename Cmp_ = Cmp,
    decltype(ZuIfT<ZmHash_IsStaticEquals<Key_, Cmp_>{}>(), int()) = 0>
  static ZuInline auto equals(const Key &l, const Key &r) {
    return Cmp::equals(l, r);
  }
  template <
    typename Key_ = Key,
    typename Cmp_ = Cmp,
    decltype(ZuIfT<!ZmHash_IsStaticEquals<Key_, Cmp_>{}>(), int()) = 0>
  auto ZuInline equals(const Key &l, const Key &r) const {
    return m_cmp.equals(l, r);
  }

private:
  template <
    typename P,
    typename Key_ = Key,
    typename Cmp_ = Cmp,
    decltype(ZuIfT<ZmHash_IsStaticCmp<Key_, Cmp_>{}>(), int()) = 0>
  static ZuInline auto matchKey(const P &key) {
    return [&key](const Node *node) {
      return Cmp::equals(node->Node::key(), key);
    };
  }
  template <
    typename P,
    typename Key_ = Key,
    typename Cmp_ = Cmp,
    decltype(ZuIfT<!ZmHash_IsStaticCmp<Key_, Cmp_>{}>(), int()) = 0>
  ZuInline auto matchKey(const P &key) const {
    return [this, &key](const Node *node) {
      return m_cmp.equals(node->Node::key(), key);
    };
  }

  template <typename P>
  static ZuInline auto matchData(const P &data) {
    return [&data](const Node *node) {
      return node->Node::data() == data;
    };
  }
  static ZuInline auto matchNode(Node *node_) {
    return [node_](const Node *node) {
      return node == node_;
    };
  }

public:
  template <typename P>
  MatchKey<P, NodeRef> find(const P &key) const {
    uint32_t code = HashFn::hash(key);
    ReadGuard guard(lockCode(code));
    return find_(matchKey(key), code);
  }
  template <typename P>
  MatchData<P, NodeRef> find(const P &data) const {
    uint32_t code = HashFn::hash(KeyAxor(data));
    ReadGuard guard(lockCode(code));
    return find_(matchData(data), code);
  }
  template <typename P0, typename P1>
  NodeRef find(P0 &&p0, P1 &&p1) {
    return find(ZuFwdTuple(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <typename P>
  MatchKey<P, Node *> findPtr(const P &key) const {
    uint32_t code = HashFn::hash(key);
    ReadGuard guard(lockCode(code));
    return find_(matchKey(key), code);
  }
  template <typename P>
  MatchData<P, Node *> findPtr(const P &data) const {
    uint32_t code = HashFn::hash(KeyAxor(data));
    ReadGuard guard(lockCode(code));
    return find_(matchData(data), code);
  }

  template <typename P>
  MatchKey<P, Key> findKey(const P &key) const {
    uint32_t code = HashFn::hash(key);
    ReadGuard guard(lockCode(code));
    return key(find_(matchKey(key), code));
  }
  template <typename P>
  MatchData<P, Key> findKey(const P &data) const {
    uint32_t code = HashFn::hash(KeyAxor(data));
    ReadGuard guard(lockCode(code));
    return key(find_(matchData(data), code));
  }
  template <typename P0, typename P1>
  decltype(auto) findKey(P0 &&p0, P1 &&p1) {
    return findKey(ZuFwdTuple(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <typename P>
  MatchKey<P, Val> findVal(const P &key) const {
    uint32_t code = HashFn::hash(key);
    ReadGuard guard(lockCode(code));
    return val(find_(matchKey(key), code));
  }
  template <typename P>
  MatchData<P, Val> findVal(const P &data) const {
    uint32_t code = HashFn::hash(KeyAxor(data));
    ReadGuard guard(lockCode(code));
    return val(find_(matchData(data), code));
  }
  template <typename P0, typename P1>
  Val findVal(P0 &&p0, P1 &&p1) {
    return findVal(ZuFwdTuple(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

private:
  template <typename Match>
  Node *find_(Match match, uint32_t code) const {
    Node *node;
    unsigned slot = ZmHashBits(code, m_bits);

    for (node = m_table[slot];
	 node && !match(node);
	 node = node->NodeExt::next);

    return node;
  }

public:
  template <typename P>
  NodeRef findAdd(P &&data) {
    uint32_t code = HashFn::hash(KeyAxor(data));
    Guard guard(lockCode(code));
    return findAdd_(ZuFwd<P>(data), code);
  }
  template <typename P0, typename P1>
  NodeRef findAdd(P0 &&p0, P1 &&p1) {
    return findAdd(ZuFwdTuple(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }
  template <typename P>
  Node *findAddPtr(P &&data) {
    uint32_t code = HashFn::hash(KeyAxor(data));
    Guard guard(lockCode(code));
    return findAdd_(ZuFwd<P>(data), code);
  }
  template <typename P0, typename P1>
  Node *findAddPtr(P0 &&p0, P1 &&p1) {
    return findAddPtr(ZuFwdTuple(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }
private:
  template <typename P>
  Node *findAdd_(P &&data, uint32_t code) {
    Node *node;
    unsigned slot = ZmHashBits(code, m_bits);

    for (node = m_table[slot];
	 node && !equals(node->Node::key(), KeyAxor(data));
	 node = node->NodeExt::next);

    if (!node) addNode_(node = new Node{ZuFwd<P>(data)}, code);
    return node;
  }

public:
  template <typename P>
  MatchKey<P, NodeMvRef> del(const P &key) {
    uint32_t code = HashFn::hash(key);
    Guard guard(lockCode(code));
    return delNode_(matchKey(key), code);
  }
  template <typename P>
  MatchData<P, NodeMvRef> del(const P &data) {
    uint32_t code = HashFn::hash(KeyAxor(data));
    Guard guard(lockCode(code));
    return delNode_(matchData(data), code);
  }
  template <typename P0, typename P1>
  NodeMvRef del(P0 &&p0, P1 &&p1) {
    return del(ZuFwdTuple(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }
  NodeMvRef delNode(Node *node) {
    uint32_t code = HashFn::hash(node->Node::key());
    Guard guard(lockCode(code));
    return delNode_(matchNode(node), code);
  }

  template <typename P>
  MatchKey<P, Key> delKey(const P &key) {
    uint32_t code = HashFn::hash(key);
    Guard guard(lockCode(code));
    return keyMv(delNode_(matchKey(key), code));
  }
  template <typename P>
  MatchData<P, Key> delKey(const P &data) {
    uint32_t code = HashFn::hash(KeyAxor(data));
    Guard guard(lockCode(code));
    return keyMv(delNode_(matchData(data), code));
  }
  template <typename P0, typename P1>
  decltype(auto) delKey(P0 &&p0, P1 &&p1) {
    return delKey(ZuFwdTuple(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }
  decltype(auto) delNodeKey(Node *node) {
    uint32_t code = HashFn::hash(node->Node::key());
    return keyMv(delNode_(matchNode(node), code));
  }

  template <typename P>
  MatchKey<P, Val> delVal(const P &key) {
    uint32_t code = HashFn::hash(key);
    Guard guard(lockCode(code));
    return valMv(delNode_(matchKey(key), code));
  }
  template <typename P>
  MatchData<P, Val> delVal(const P &data) {
    uint32_t code = HashFn::hash(KeyAxor(data));
    Guard guard(lockCode(code));
    return valMv(delNode_(matchData(data), code));
  }
  template <typename P0, typename P1>
  decltype(auto) delVal(P0 &&p0, P1 &&p1) {
    return delVal(ZuFwdTuple(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }
  decltype(auto) delNodeVal(Node *node) {
    uint32_t code = HashFn::hash(node->Node::key());
    return valMv(delNode_(matchNode(node), code));
  }

private:
  template <typename Match>
  NodeMvRef delNode_(Match match, uint32_t code) {
    unsigned count = m_count.load_();
    if (!count) return 0;

    Node *node, *prevNode = nullptr;
    unsigned slot = ZmHashBits(code, m_bits);

    for (node = m_table[slot];
	 node && !match(node);
	 prevNode = node, node = node->NodeExt::next);

    if (!node) return 0;

    if (!prevNode)
      m_table[slot] = node->NodeExt::next;
    else
      prevNode->NodeExt::next = node->NodeExt::next;

    m_count.store_(count - 1);

    node->NodeExt::next = nullptr;

    return nodeAcquire(node);
  }

public:
  auto iterator() { return Iterator{*this}; }
  template <typename P>
  auto iterator(P key) {
    return KeyIterator<P>{*this, ZuMv(key)};
  }

  auto readIterator() const { return ReadIterator{*this}; }
  template <typename P>
  auto readIterator(P key) const {
    return ReadKeyIterator<P>{*this, ZuMv(key)};
  }

private:
  template <typename I>
  void startIterate(I &iterator) {
    LockTraits::lock(lockSlot(0));
    iterator.slot = 0;
    iterator.node = nullptr;
    iterator.prev = nullptr;
  }
  template <typename I>
  void startKeyIterate(I &iterator) {
    uint32_t code = HashFn::hash(iterator.key);

    LockTraits::lock(lockCode(code));
    iterator.slot = ZmHashBits(code, m_bits);
    iterator.node = nullptr;
    iterator.prev = nullptr;
  }
  template <typename I>
  Node *iterate(I &iterator) {
    int slot = iterator.slot;

    if (slot < 0) return nullptr;

    Node *node = iterator.node, *prevNode;

    if (!node) {
      prevNode = nullptr;
      node = m_table[slot];
    } else {
      prevNode = node;
      node = node->NodeExt::next;
    }

    if (!node) {
      prevNode = nullptr;
      do {
	LockTraits::unlock(lockSlot(slot));
	if (++slot >= (1U<<m_bits)) {
	  iterator.slot = -1;
	  return nullptr;
	}
	LockTraits::lock(lockSlot(slot));
	iterator.slot = slot;
      } while (!(node = m_table[slot]));
    }

    iterator.prev = prevNode;
    return iterator.node = node;
  }
  template <typename I>
  Node *keyIterate(I &iterator) {
    int slot = iterator.slot;

    if (slot < 0) return nullptr;

    Node *node = iterator.node, *prevNode;

    if (!node) {
      prevNode = nullptr;
      node = m_table[slot];
    } else {
      prevNode = node;
      node = node->NodeExt::next;
    }

    for (;
	 node && !equals(node->Node::key(), iterator.key);
	 prevNode = node, node = node->NodeExt::next);

    if (!node) {
      LockTraits::unlock(lockSlot(slot));
      iterator.slot = -1;
      return nullptr;
    }

    iterator.prev = prevNode;
    return iterator.node = node;
  }
  template <typename I>
  void endIterate(I &iterator) {
    if (iterator.slot < 0) return;

    LockTraits::unlock(lockSlot(iterator.slot));
  }
  template <typename I>
  NodeMvRef delIterate(I &iterator) {
    Node *node = iterator.node, *prevNode = iterator.prev;

    unsigned count = m_count.load_();
    if (!count || !node) return nullptr;

    if (!prevNode)
      m_table[iterator.slot] = node->NodeExt::next;
    else
      prevNode->NodeExt::next = node->NodeExt::next;

    iterator.node = prevNode;

    m_count.store_(count - 1);

    node->NodeExt::next = nullptr;

    return nodeAcquire(node);
  }

public:
  void clean() {
    lockAll();

    for (unsigned i = 0, n = (1U<<m_bits); i < n; i++) {
      Node *node, *prevNode;

      node = m_table[i];

      while (prevNode = node) {
	node = prevNode->NodeExt::next;
	nodeDeref(prevNode);
	nodeDelete(prevNode);
      }

      m_table[i] = nullptr;
    }
    m_count = 0;

    unlockAll();
  }

  template <typename P>
  Lock &lock(P &&key) {
    return lockCode(HashFn::hash(ZuFwd<P>(key)));
  }

  void telemetry(ZmHashTelemetry &data) const {
    data.id = m_id;
    data.addr = reinterpret_cast<uintptr_t>(this);
    data.loadFactor = loadFactor();
    unsigned count = m_count.load_();
    unsigned bits = m_bits;
    data.effLoadFactor = static_cast<double>(count) / (1<<bits);
    data.nodeSize = sizeof(Node);
    data.count = count;
    data.resized = m_resized.load_();
    data.bits = bits;
    data.cBits = cBits();
    data.linear = false;
    data.shadow = Shadow;
  }

private:
  void resize(unsigned bits) {
    if (!lockAllResize(bits)) return;

    m_resized.store_(m_resized.load_() + 1);

    unsigned n = (1U<<m_bits);

    m_bits = bits;

    NodePtr *table = static_cast<NodePtr *>(
      Zm::alignedAlloc<Zm::CacheLineSize>(sizeof(NodePtr)<<bits));
    memset(table, 0, sizeof(NodePtr)<<bits);
    Node *node, *nextNode;

    for (unsigned i = 0; i < n; i++)
      for (node = m_table[i]; node; node = nextNode) {
	nextNode = node->NodeExt::next;
	unsigned j = ZmHashBits(HashFn::hash(node->Node::key()), bits);
	node->NodeExt::next = table[j];
	table[j] = node;
      }
    Zm::alignedFree(m_table);
    m_table = table;

    unlockAll();
  }

  ZmIDString		m_id;
  Cmp			m_cmp;
  unsigned		m_loadFactor = 0;
  ZmAtomic<unsigned>	m_count = 0;
  ZmAtomic<unsigned>	m_resized = 0;
  NodePtr		*m_table;
};

template <typename P0, typename P1, typename NTP = ZmHash_Defaults>
using ZmHashKV =
  ZmHash<ZuTuple<P0, P1>,
    ZmHashKeyVal<ZuTupleAxor<0>(), ZuTupleAxor<1>(), NTP>>;

#endif /* ZmHash_HH */
