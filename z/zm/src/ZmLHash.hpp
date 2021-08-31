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

// hash table (policy-based)
//
// open addressing, linear probing, fast chained lookup, optionally locked
//
// nodes are stored by value - avoids run-time heap usage and improves
// cache coherence (except during initialization/resizing)
//
// use ZmHash for high-contention high-throughput read/write data
// use ZmLHash for unlocked or mostly uncontended reference data

#ifndef ZmLHash_HPP
#define ZmLHash_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZuNull.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuHash.hpp>
#include <zlib/ZuPair.hpp>
#include <zlib/ZuArrayFn.hpp>

#include <zlib/ZmAtomic.hpp>
#include <zlib/ZmLock.hpp>
#include <zlib/ZmNoLock.hpp>
#include <zlib/ZmLockTraits.hpp>
#include <zlib/ZmGuard.hpp>
#include <zlib/ZmObject.hpp>
#include <zlib/ZmRef.hpp>
#include <zlib/ZmAssert.hpp>

#include <zlib/ZmHashMgr.hpp>

// NTP (named template parameters):
//
// ZmLHashKV<ZtString, ZtString,	// keys are ZtStrings
//   ZmLHashCmp<ZtCmp> >		// case-insensitive comparison

// NTP defaults
struct ZmLHash_Defaults {
  using KeyAxor = ZuDefaultAxor;
  using ValAxor = ZuDefaultAxor;
  template <typename T> using CmpT = ZuCmp<T>;
  template <typename T> using ValCmpT = ZuCmp<T>;
  template <typename T> using HashFnT = ZuHash<T>;
  template <typename T> using ValCmpT = ZuCmp<T>;
  using Lock = ZmLock;
  struct ID { static constexpr const char *id() { return "ZmLHash"; } };
  enum { Static = 0 };
};

// ZmLHashKey - key accessor
template <typename KeyAxor_, typename NTP = ZmLHash_Defaults>
struct ZmLHashKey : public NTP {
  using KeyAxor = KeyAxor_;
};

// ZmLHashKeyVal - key and optional value accessors
template <typename KeyAxor_, typename ValAxor_, typename NTP = ZmLHash_Defaults>
struct ZmLHashKeyVal : public NTP {
  using KeyAxor = KeyAxor_;
  using ValAxor = ValAxor_;
};

// ZmLHashCmp - the comparator
template <template <typename> typename Cmp_, typename NTP = ZmLHash_Defaults>
struct ZmLHashCmp : public NTP {
  template <typename T> using CmpT = Cmp_<T>;
};

// ZmLHashValCmp - the optional value comparator
template <template <typename> typename ValCmp_, typename NTP = ZmLHash_Defaults>
struct ZmLHashValCmp : public NTP {
  template <typename T> using ValCmpT = ValCmp_<T>;
};

// ZmLHashFn - the hash function
template <template <typename> typename HashFn_, typename NTP = ZmLHash_Defaults>
struct ZmLHashFn : public NTP {
  template <typename T> using HashFnT = HashFn_<T>;
};

// ZmLHashLock - the lock type used (ZmRWLock will permit concurrent reads)
template <class Lock_, typename NTP = ZmLHash_Defaults>
struct ZmLHashLock : public NTP {
  using Lock = Lock_;
};

// ZmLHashID - the hash ID
template <class ID_, typename NTP = ZmLHash_Defaults>
struct ZmLHashID : public NTP {
  using ID = ID_;
};

// ZmLHashStatic<Bits> - static/non-resizable vs dynamic/resizable allocation
template <unsigned Static_, typename NTP = ZmLHash_Defaults>
struct ZmLHashStatic : public NTP {
  enum { Static = Static_ };
};

template <typename T>
struct ZmLHash_Ops : public ZuArrayFn<T, ZuCmp<T> > {
  static T *alloc(unsigned size) {
    T *ptr = (T *)::malloc(size * sizeof(T));
    if (!ptr) throw std::bad_alloc();
    return ptr;
  }
  static void free(T *ptr) {
    ::free(ptr);
  }
};

template <typename T_, typename KeyAxor, typename ValAxor>
class ZmLHash_Node {
template <typename, typename> friend class ZmLHash;
template <typename, typename, typename, unsigned> friend class ZmLHash_;

public:
  using T = T_;

  ZmLHash_Node() { }
  ~ZmLHash_Node() { if (m_u) data().~T(); }

  ZmLHash_Node(const ZmLHash_Node &n) {
    if (m_u = n.m_u) new (m_data) T{n.data()};
  }
  ZmLHash_Node &operator =(const ZmLHash_Node &n) {
    if (ZuLikely(this != &n)) {
      if (m_u) data().~T();
      if (m_u = n.m_u) new (m_data) T{n.data()};
    }
    return *this;
  }

  ZmLHash_Node(ZmLHash_Node &&n) {
    if (m_u = n.m_u) new (m_data) T{ZuMv(n.data())};
  }
  ZmLHash_Node &operator =(ZmLHash_Node &&n) {
    if (ZuLikely(this != &n)) {
      if (m_u) data().~T();
      if (m_u = n.m_u) new (m_data) T{ZuMv(n.data())};
    }
    return *this;
  }

private:
  template <typename P>
  void init(unsigned head, unsigned tail, unsigned next, P &&v) {
    if (!m_u)
      new (m_data) T{ZuFwd<P>(v)};
    else
      data() = ZuFwd<P>(v);
    m_u = (next<<3U) | (head<<2U) | (tail<<1U) | 1U;
  }
  void null() {
    if (m_u) {
      data().~T();
      m_u = 0;
    }
  }

public:
  bool operator !() const { return !m_u; }
  ZuOpBool

  bool equals(const ZmLHash_Node &n) const {
    if (!n.m_u) return !m_u;
    if (!m_u) return false;
    return data().equals(n.data());
  }
  int cmp(const ZmLHash_Node &n) const {
    if (!n.m_u) return !m_u ? 0 : 1;
    if (!m_u) return -1;
    return data().cmp(n.data());
  }
  friend inline bool operator ==(const ZmLHash_Node &l, const ZmLHash_Node &r) {
    return l.equals(r);
  }
  friend inline int operator <=>(const ZmLHash_Node &l, const ZmLHash_Node &r) {
    return l.cmp(r);
  }

private:
  bool head() const { return m_u & 4U; }
  void setHead() { m_u |= 4U; }
  void clrHead() { m_u &= ~4U; }
  bool tail() const { return m_u & 2U; }
  void setTail() { m_u |= 2U; }
  void clrTail() { m_u &= ~2U; }
  unsigned next() const { return m_u>>3U; }
  void next(unsigned n) { m_u = (n<<3U) | (m_u & 7U); }

  decltype(auto) key() const & { return KeyAxor::get(data()); }
  decltype(auto) key() & { return KeyAxor::get(data()); }
  decltype(auto) key() && { return KeyAxor::get(ZuMv(data())); }

  decltype(auto) val() const & { return ValAxor::get(data()); }
  decltype(auto) val() & { return ValAxor::get(data()); }
  decltype(auto) val() && { return ValAxor::get(ZuMv(data())); }

  const auto &data() const & {
    ZmAssert(m_u);
    const T *ZuMayAlias(ptr) = reinterpret_cast<const T *>(m_data);
    return *ptr;
  }
  auto &data() & {
    ZmAssert(m_u);
    T *ZuMayAlias(ptr) = reinterpret_cast<T *>(m_data);
    return *ptr;
  }
  decltype(auto) data() && {
    ZmAssert(m_u);
    T *ZuMayAlias(ptr) = reinterpret_cast<T *>(m_data);
    return ZuMv(*ptr);
  }

public:
  struct Traits : public ZuBaseTraits<ZmLHash_Node> {
    enum { IsPOD = ZuTraits<T>::IsPOD };
  };
  friend Traits ZuTraitsType(ZmLHash_Node *);

private:
  uint32_t	m_u = 0;
  char		m_data[sizeof(T)];
};

// common base class for both static and dynamic tables
template <typename NTP> class ZmLHash__ : public ZmAnyHash {
  using Lock = typename NTP::Lock;

public:
  unsigned loadFactor_() const { return m_loadFactor; }
  double loadFactor() const { return (double)m_loadFactor / 16.0; }

  unsigned count_() const { return m_count.load_(); }

protected:
  ZmLHash__(const ZmHashParams &params) {
    double loadFactor = params.loadFactor();
    if (loadFactor < 0.5) loadFactor = 0.5;
    else if (loadFactor > 1.0) loadFactor = 1.0;
    m_loadFactor = (unsigned)(loadFactor * 16.0);
  }

  unsigned		m_loadFactor = 0;
  ZmAtomic<unsigned>	m_count = 0;
  Lock			m_lock;
};

// statically allocated hash table base class
template <typename Hash, typename T, typename NTP, unsigned Static>
class ZmLHash_ : public ZmLHash__<NTP> {
  using Base = ZmLHash__<NTP>;
  using KeyAxor = typename NTP::KeyAxor;
  using ValAxor = typename NTP::ValAxor;
  using Key = ZuDecay<decltype(KeyAxor::get(ZuDeclVal<const T &>()))>;
  using Val = ZuDecay<decltype(ValAxor::get(ZuDeclVal<const T &>()))>;
  using Cmp = typename NTP::template CmpT<Key>;
  using ValCmp = typename NTP::template ValCmpT<Val>;
  using HashFn = typename NTP::template HashFnT<Key>;
  using Node = ZmLHash_Node<T, KeyAxor, ValAxor>;
  using Ops = ZmLHash_Ops<Node>;

public:
  static constexpr unsigned bits() { return Static; }

protected:
  ZmLHash_(const ZmHashParams &params) : Base(params) { }

  void init() { Ops::initItems(m_table, 1U<<Static); }
  void final() { Ops::destroyItems(m_table, 1U<<Static); }
  void resize() { }
  static constexpr unsigned resized() { return 0; }

  Node		m_table[1U<<Static];
};

// dynamically allocated hash table base class
template <class Hash, typename T, typename NTP>
class ZmLHash_<Hash, T, NTP, 0> : public ZmLHash__<NTP> {
  using Base = ZmLHash__<NTP>;
  using KeyAxor = typename NTP::KeyAxor;
  using ValAxor = typename NTP::ValAxor;
  using Key = ZuDecay<decltype(KeyAxor::get(ZuDeclVal<const T &>()))>;
  using Val = ZuDecay<decltype(ValAxor::get(ZuDeclVal<const T &>()))>;
  using Cmp = typename NTP::template CmpT<Key>;
  using ValCmp = typename NTP::template ValCmpT<Val>;
  using HashFn = typename NTP::template HashFnT<Key>;
  using Node = ZmLHash_Node<T, KeyAxor, ValAxor>;
  using Ops = ZmLHash_Ops<Node>;

public:
  unsigned bits() const { return m_bits; }

protected:
  ZmLHash_(const ZmHashParams &params) : Base(params),
    m_bits(params.bits()) { }

  void init() {
    unsigned size = 1U<<m_bits;
    m_table = Ops::alloc(size);
    Ops::initItems(m_table, size);
    ZmHashMgr::add(this);
  }

  void final() {
    ZmHashMgr::del(this);
    Ops::destroyItems(m_table, 1U<<m_bits);
    Ops::free(m_table);
  }

  void resize() {
    ++m_resized;
    ++m_bits;
    unsigned size = 1U<<m_bits;
    Node *oldTable = m_table;
    m_table = Ops::alloc(size);
    Ops::initItems(m_table, size);
    for (unsigned i = 0; i < (size>>1U); i++)
      if (!!oldTable[i]) {
	auto code = HashFn::hash(oldTable[i].key());
	static_cast<Hash *>(this)->add__(ZuMv(oldTable[i].data()), code);
      }
    Ops::destroyItems(oldTable, (size>>1U));
    Ops::free(oldTable);
  }

  unsigned resized() const { return m_resized.load_(); }

  ZmAtomic<unsigned>	m_resized = 0;
  unsigned		m_bits;
  Node	 		*m_table = nullptr;
};

template <typename T_, typename NTP = ZmLHash_Defaults>
class ZmLHash : public ZmLHash_<ZmLHash<T_, NTP>, T_, NTP, NTP::Static> {
  ZmLHash(const ZmLHash &) = delete;
  ZmLHash &operator =(const ZmLHash &) = delete; // prevent mis-use

template <typename, typename, typename, unsigned> friend class ZmLHash_;

  using Base = ZmLHash_<ZmLHash<T_, NTP>, T_, NTP, NTP::Static>;

public:
  using T = T_;
  using KeyAxor = typename NTP::KeyAxor;
  using ValAxor = typename NTP::ValAxor;
  using Key = ZuDecay<decltype(KeyAxor::get(ZuDeclVal<const T &>()))>;
  using Val = ZuDecay<decltype(ValAxor::get(ZuDeclVal<const T &>()))>;
  using Cmp = typename NTP::template CmpT<Key>;
  using ValCmp = typename NTP::template ValCmpT<Val>;
  using HashFn = typename NTP::template HashFnT<Key>;
  using Lock = typename NTP::Lock;
  using ID = typename NTP::ID;
  using LockTraits = ZmLockTraits<Lock>;
  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;
  using Node = ZmLHash_Node<T, KeyAxor, ValAxor>;
  using Ops = ZmLHash_Ops<Node>;
  enum { Static = NTP::Static };
 
private:
  // CheckHashFn ensures that legacy hash functions returning int
  // trigger a compile-time assertion failure; hash() must return uint32_t
  class CheckHashFn {
    using Small = char;
    struct Big { char _[2]; };
    static Small test(const uint32_t &_); // named parameter due to VS2010 bug
    static Big	 test(const int &_);
    static Big	 test(...);
  public:
    CheckHashFn(); // keep gcc quiet
    enum _ {
      IsUInt32 = sizeof(test(HashFn::hash(ZuDeclVal<Key>()))) == sizeof(Small)
    };
  };
  ZuAssert(CheckHashFn::IsUInt32);

  using Base::m_count;
  using Base::m_lock;
  using Base::m_table;

public:
  using Base::bits;
  using Base::loadFactor_;
  using Base::loadFactor;
  using Base::resized;

private:
  const T *data(int slot) const {
    if (ZuLikely(slot >= 0)) return &m_table[slot].data();
    return nullptr;
  }
  const Key &key(int slot) const {
    if (ZuLikely(slot >= 0)) return m_table[slot].key();
    return Cmp::null();
  }
  const Val &val(int slot) const {
    if (ZuLikely(slot >= 0)) return m_table[slot].val();
    return ValCmp::null();
  }

protected:
  class Iterator_;
friend Iterator_;
  class Iterator_ {			// hash iterator
    Iterator_(const Iterator_ &) = delete;
    Iterator_ &operator =(const Iterator_ &) = delete;

    using Hash = ZmLHash<T, NTP>;

  friend Hash;

  protected:
    Iterator_(Iterator_ &&) = default;
    Iterator_ &operator =(Iterator_ &&) = default;

    Iterator_(Hash &hash) : m_hash(hash), m_slot(-1), m_next(-1) { }

    virtual void lock(Lock &l) = 0;
    virtual void unlock(Lock &l) = 0;

  public:
    void reset() { m_hash.startIterate(*this); }
    const T *iterate() { return m_hash.iterate(*this); }
    const Key &iterateKey() { return m_hash.iterateKey(*this); }
    const Val &iterateVal() { return m_hash.iterateVal(*this); }

    unsigned count() const { return m_hash.count_(); }

    bool operator !() const { return m_slot < 0; }
    ZuOpBool

  protected:
    Hash	&m_hash;
    int		m_slot;
    int		m_next;
  };

  class KeyIterator_;
friend KeyIterator_;
  class KeyIterator_ : protected Iterator_ {
    KeyIterator_(const KeyIterator_ &) = delete;
    KeyIterator_ &operator =(const KeyIterator_ &) = delete;

    using Hash = ZmLHash<T, NTP>;
  friend Hash;

    using Iterator_::m_hash;

  protected:
    KeyIterator_(KeyIterator_ &&) = default;
    KeyIterator_ &operator =(KeyIterator_ &&) = default;

    template <typename P>
    KeyIterator_(Hash &hash, P &&v) :
	Iterator_{hash}, m_key{ZuFwd<P>(v)}, m_prev{-1} { }

  public:
    void reset() { m_hash.startIterate(*this); }
    const T *iterate() { return m_hash.iterate(*this); }
    const Key &iterateKey() { return m_hash.iterateKey(*this); }
    const Val &iterateVal() { return m_hash.iterateVal(*this); }

  protected:
    Key		m_key;
    int		m_prev;
  };

public:
  class Iterator : public Iterator_ {
    Iterator(const Iterator &) = delete;
    Iterator &operator =(const Iterator &) = delete;

    using Hash = ZmLHash<T, NTP>;
    void lock(Lock &l) { LockTraits::lock(l); }
    void unlock(Lock &l) { LockTraits::unlock(l); }

    using Iterator_::m_hash;

  public:
    Iterator(Iterator &&) = default;
    Iterator &operator =(Iterator &&) = default;

    Iterator(Hash &hash) : Iterator_(hash) { hash.startIterate(*this); }
    ~Iterator() { m_hash.endIterate(*this); }
    void del() { m_hash.delIterate(*this); }
  };

  class ReadIterator : public Iterator_ {
    ReadIterator(const ReadIterator &) = delete;
    ReadIterator &operator =(const ReadIterator &) = delete;

    using Hash = ZmLHash<T, NTP>;
    void lock(Lock &l) { LockTraits::readlock(l); }
    void unlock(Lock &l) { LockTraits::readunlock(l); }

    using Iterator_::m_hash;

  public:
    ReadIterator(ReadIterator &&) = default;
    ReadIterator &operator =(ReadIterator &&) = default;

    ReadIterator(const Hash &hash) : Iterator_(const_cast<Hash &>(hash))
      { const_cast<Hash &>(hash).startIterate(*this); }
    ~ReadIterator() { m_hash.endIterate(*this); }
  };

  class KeyIterator : public KeyIterator_ {
    KeyIterator(const KeyIterator &) = delete;
    KeyIterator &operator =(const KeyIterator &) = delete;

    using Hash = ZmLHash<T, NTP>;
    void lock(Lock &l) { LockTraits::lock(l); }
    void unlock(Lock &l) { LockTraits::unlock(l); }

    using KeyIterator_::m_hash;

  public:
    KeyIterator(KeyIterator &&) = default;
    KeyIterator &operator =(KeyIterator &&) = default;

    template <typename Index_>
    KeyIterator(Hash &hash, Index_ &&index) :
	KeyIterator_(hash, ZuFwd<Index_>(index)) { hash.startIterate(*this); }
    ~KeyIterator() { m_hash.endIterate(*this); }
    void del() { m_hash.delIterate(*this); }
  };

  class ReadKeyIterator : public KeyIterator_ {
    ReadKeyIterator(const ReadKeyIterator &) = delete;
    ReadKeyIterator &operator =(const ReadKeyIterator &) = delete;

    using Hash = ZmLHash<T, NTP>;
    void lock(Lock &l) { LockTraits::readlock(l); }
    void unlock(Lock &l) { LockTraits::readunlock(l); }

    using KeyIterator_::m_hash;

  public:
    ReadKeyIterator(ReadKeyIterator &&) = default;
    ReadKeyIterator &operator =(ReadKeyIterator &&) = default;

    template <typename Index_>
    ReadKeyIterator(Hash &hash, Index_ &&index) :
	KeyIterator_(const_cast<Hash &>(hash), ZuFwd<Index_>(index))
      { const_cast<Hash &>(hash).startIterate(*this); }
    ~ReadKeyIterator() { m_hash.endIterate(*this); }
  };

  template <typename ...Args>
  ZmLHash(ZmHashParams params = ZmHashParams(ID::id())) : Base(params) {
    Base::init();
  }

  ~ZmLHash() { Base::final(); }

  unsigned size() const {
    return static_cast<double>(static_cast<uint64_t>(1)<<bits()) * loadFactor();
  }

  template <typename P>
  int add(P &&data) {
    uint32_t code = HashFn::hash(KeyAxor::get(data));
    Guard guard(m_lock);
    return add_(ZuFwd<P>(data), code);
  }

  template <typename P0, typename P1>
  int add(P0 &&p0, P1 &&p1) {
    return add(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

private:
  int alloc(unsigned slot) {
    unsigned size = 1U<<bits();
    for (unsigned i = 1; i < size; i++) {
      unsigned probe = (slot + i) & (size - 1);
      if (!m_table[probe]) return probe;
    }
    return -1;
  }
  int prev(unsigned slot) {
    unsigned size = 1U<<bits();
    for (unsigned i = 1; i < size; i++) {
      unsigned prev = (slot + size - i) & (size - 1);
      if (m_table[prev] &&
	  !m_table[prev].tail() &&
	  m_table[prev].next() == slot)
	return prev;
    }
    return -1;
  }
  template <typename P>
  int add_(P &&data, uint32_t code) {
    unsigned size = 1U<<bits();

    unsigned count = m_count.load_();
    if (count < (1U<<28) && ((count<<4)>>bits()) >= loadFactor_()) {
      Base::resize();
      size = 1U<<bits();
    }

    if (count >= size) return -1;

    m_count.store_(count + 1);

    return add__(ZuFwd<P>(data), code);
  }
  template <typename P>
  int add__(P &&data, uint32_t code) {
    unsigned size = 1U<<bits();
    unsigned slot = code & (size - 1);

    if (!m_table[slot]) {
      m_table[slot].init(1, 1, 0, ZuFwd<P>(data));
      return slot;
    }

    int move = alloc(slot);
    if (move < 0) return -1;

    if (m_table[slot].head()) {
      (m_table[move] = ZuMv(m_table[slot])).clrHead();
      m_table[slot].init(1, 0, move, ZuFwd<P>(data));
      return slot;
    }

    int prev = this->prev(slot);
    if (prev < 0) return -1;

    m_table[move] = ZuMv(m_table[slot]);
    m_table[prev].next(move);
    m_table[slot].init(1, 1, 0, ZuFwd<P>(data));
    return slot;
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
  MatchKey<P, bool> exists(const P &key) const {
    uint32_t code = HashFn::hash(key);
    ReadGuard guard(const_cast<Lock &>(m_lock));
    return find_(matchKey(key), code) >= 0;
  }
  template <typename P>
  MatchData<P, bool> exists(const P &data) const {
    uint32_t code = HashFn::hash(KeyAxor::get(data));
    ReadGuard guard(const_cast<Lock &>(m_lock));
    return find_(matchData(data), code) >= 0;
  }

  template <typename P>
  MatchKey<P, const T *> find(const P &key) const {
    uint32_t code = HashFn::hash(key);
    ReadGuard guard(const_cast<Lock &>(m_lock));
    return data(find_(matchKey(key), code));
  }
  template <typename P>
  MatchData<P, const T *> find(const P &data) const {
    uint32_t code = HashFn::hash(KeyAxor::get(data));
    ReadGuard guard(const_cast<Lock &>(m_lock));
    return data(find_(matchData(data), code));
  }
  template <typename P0, typename P1>
  const T *find(P0 &&p0, P1 &&p1) {
    return find(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <typename P>
  MatchKey<P, Key> findKey(const P &key) const {
    uint32_t code = HashFn::hash(key);
    ReadGuard guard(const_cast<Lock &>(m_lock));
    return key(find_(matchKey(key), code));
  }
  template <typename P>
  MatchData<P, Key> findKey(const P &data) const {
    uint32_t code = HashFn::hash(KeyAxor::get(data));
    ReadGuard guard(const_cast<Lock &>(m_lock));
    return key(find_(matchData(data), code));
  }
  template <typename P0, typename P1>
  Key findKey(P0 &&p0, P1 &&p1) {
    return findKey(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <typename P>
  MatchKey<P, Val> findVal(const P &key) const {
    uint32_t code = HashFn::hash(key);
    ReadGuard guard(const_cast<Lock &>(m_lock));
    return val(find_(matchKey(key), code));
  }
  template <typename P>
  MatchData<P, Val> findVal(const P &data) const {
    uint32_t code = HashFn::hash(KeyAxor::get(data));
    ReadGuard guard(const_cast<Lock &>(m_lock));
    return val(find_(matchData(data), code));
  }
  template <typename P0, typename P1>
  Val findVal(P0 &&p0, P1 &&p1) {
    return findVal(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

private:
  template <typename Match>
  int find_(Match match, uint32_t code) const {
    unsigned size = 1U<<bits();
    unsigned slot = code & (size - 1);
    if (!m_table[slot] || !m_table[slot].head()) return -1;
    for (;;) {
      if (match(&m_table[slot])) return slot;
      if (m_table[slot].tail()) return -1;
      slot = m_table[slot].next();
    }
  }
  template <typename Match>
  int findPrev_(Match match, uint32_t code) const {
    unsigned size = 1U<<bits();
    unsigned slot = code & (size - 1);
    if (!m_table[slot] || !m_table[slot].head()) return -1;
    int prev = -1;
    for (;;) {
      if (match(&m_table[slot]))
	return prev < 0 ? (-(static_cast<int>(slot)) - 2) : prev;
      if (m_table[slot].tail()) return -1;
      prev = slot, slot = m_table[slot].next();
    }
  }

public:
  template <typename P>
  const T *findAdd(P &&data) {
    uint32_t code = HashFn::hash(KeyAxor::get(data));
    Guard guard(m_lock);
    return this->data(findAdd__(ZuFwd<P>(data), code));
  }
  template <typename P0, typename P1>
  const T *findAdd(P0 &&p0, P1 &&p1) {
    return findAdd(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

private:
  template <typename P>
  int findAdd__(P &&data, uint32_t code) {
    unsigned size = 1U<<bits();
    unsigned slot = code & (size - 1);
    if (!!m_table[slot] && m_table[slot].head())
      for (;;) {
	if (m_table[slot].data() == data) return slot;
	if (m_table[slot].tail()) break;
	slot = m_table[slot].next();
      }
    return add_(ZuFwd<P>(data), code);
  }

public:
  template <typename P>
  MatchKey<P> del(const P &key) {
    uint32_t code = HashFn::hash(key);
    Guard guard(m_lock);
    del_(findPrev_(matchKey(key), code));
  }
  template <typename P>
  MatchData<P> del(const P &data) {
    uint32_t code = HashFn::hash(KeyAxor::get(data));
    Guard guard(m_lock);
    del_(findPrev_(matchData(data), code));
  }
  template <typename P0, typename P1>
  void del(P0 &&p0, P1 &&p1) {
    del(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <typename P>
  MatchKey<P, Key> delKey(const P &key) {
    uint32_t code = HashFn::hash(key);
    Guard guard(m_lock);
    return delKey_(findPrev_(matchKey(key), code));
  }
  template <typename P>
  MatchData<P, Key> delKey(const P &data) {
    uint32_t code = HashFn::hash(KeyAxor::get(data));
    Guard guard(m_lock);
    return delKey_(findPrev_(matchData(data), code));
  }
  template <typename P0, typename P1>
  Key delKey(P0 &&p0, P1 &&p1) {
    return delKey(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

  template <typename P>
  MatchKey<P, Val> delVal(const P &key) {
    uint32_t code = HashFn::hash(key);
    Guard guard(m_lock);
    return delVal_(findPrev_(matchKey(key), code));
  }
  template <typename P>
  MatchData<P, Val> delVal(const P &data) {
    uint32_t code = HashFn::hash(KeyAxor::get(data));
    Guard guard(m_lock);
    return delVal_(findPrev_(matchData(data), code));
  }
  template <typename P0, typename P1>
  Val delVal(P0 &&p0, P1 &&p1) {
    return delVal(ZuFwdPair(ZuFwd<P0>(p0), ZuFwd<P1>(p1)));
  }

private:
  void del__(int prev) {
    int slot;

    if (prev < 0)
      slot = (-prev - 2), prev = -1;
    else
      slot = m_table[prev].next();

    if (!m_table[slot]) return;

    if (unsigned count = m_count.load_())
      m_count.store_(count - 1);

    if (m_table[slot].head()) {
      ZmAssert(prev < 0);
      if (m_table[slot].tail()) {
	m_table[slot].null();
	return;
      }
      unsigned next = m_table[slot].next();
      (m_table[slot] = ZuMv(m_table[next])).setHead();
      m_table[next].null();
      return;
    }

    if (m_table[slot].tail()) {
      ZmAssert(prev >= 0);
      if (prev >= 0) m_table[prev].setTail();
      m_table[slot].null();
      return;
    }

    unsigned next = m_table[slot].next();
    m_table[slot] = ZuMv(m_table[next]);
    m_table[next].null();
  }

  void del_(int prev) {
    if (prev == -1) return;
    del__(prev);
  }
  Key delKey_(int prev) {
    if (prev == -1) return Cmp::null();
    int slot = prev < 0 ? (-prev - 2) : m_table[prev].next();
    Key key{ZuMv(m_table[slot]).key()};
    del__(prev);
    return key;
  }
  Key delVal_(int prev) {
    if (prev == -1) return Cmp::null();
    int slot = prev < 0 ? (-prev - 2) : m_table[prev].next();
    Val val{ZuMv(m_table[slot]).val()};
    del__(prev);
    return val;
  }

public:
  void clean() {
    unsigned size = 1U<<bits();
    Guard guard(m_lock);

    for (unsigned i = 0; i < size; i++) m_table[i].null();
    m_count = 0;
  }

  void telemetry(ZmHashTelemetry &data) const {
    data.id = ID::id();
    data.addr = (uintptr_t)this;
    data.loadFactor = loadFactor();
    unsigned count = m_count.load_();
    unsigned bits = this->bits();
    data.effLoadFactor = ((double)count) / ((double)(1<<bits));
    data.nodeSize = sizeof(Node);
    data.count = count;
    data.resized = resized();
    data.bits = bits;
    data.cBits = 0;
    data.linear = true;
  }

  auto iterator() { return Iterator(*this); }
  template <typename Index_>
  auto iterator(Index_ &&index) {
    return KeyIterator(*this, ZuFwd<Index_>(index));
  }

  auto readIterator() const { return ReadIterator(*this); }
  template <typename Index_>
  auto readIterator(Index_ &&index) const {
    return ReadKeyIterator(*this, ZuFwd<Index_>(index));
  }

private:
  void startIterate(Iterator_ &iterator) {
    iterator.lock(m_lock);
    iterator.m_slot = -1;
    int next = -1;
    int size = 1<<bits();
    while (++next < size)
      if (!!m_table[next]) {
	iterator.m_next = next;
	return;
      }
    iterator.m_next = -1;
  }
  void startIterate(KeyIterator_ &iterator) {
    iterator.lock(m_lock);
    iterator.m_slot = -1;
    int prev =
      findPrev_(matchKey(iterator.m_key), HashFn::hash(iterator.m_key));
    if (prev == -1) {
      iterator.m_next = iterator.m_prev = -1;
      return;
    }
    if (prev < 0)
      iterator.m_next = -prev - 2, iterator.m_prev = -1;
    else
      iterator.m_next = m_table[iterator.m_prev = prev].next();
  }
  void iterate_(Iterator_ &iterator) {
    int next = iterator.m_next;
    if (next < 0) {
      iterator.m_slot = -1;
      return;
    }
    iterator.m_slot = next;
    int size = 1<<bits();
    while (++next < size)
      if (!!m_table[next]) {
	iterator.m_next = next;
	return;
      }
    iterator.m_next = -1;
  }
  void iterate_(KeyIterator_ &iterator) {
    int next = iterator.m_next;
    if (next < 0) {
      iterator.m_slot = -1;
      return;
    }
    if (iterator.m_slot >= 0) iterator.m_prev = iterator.m_slot;
    iterator.m_slot = next;
    while (!m_table[next].tail()) {
      next = m_table[next].next();
      if (Cmp::equals(m_table[next].key(), iterator.m_key)) {
	iterator.m_next = next;
	return;
      }
    }
    iterator.m_next = -1;
  }
  template <typename I>
  const T *iterate(I &iterator) {
    iterate_(iterator);
    return data(iterator.m_slot);
  }
  template <typename I>
  const Key &iterateKey(I &iterator) {
    iterate_(iterator);
    return key(iterator.m_slot);
  }
  template <typename I>
  const Val &iterateVal(I &iterator) {
    iterate_(iterator);
    return val(iterator.m_slot);
  }
  void endIterate(Iterator_ &iterator) {
    iterator.unlock(m_lock);
    iterator.m_slot = iterator.m_next = -1;
  }
  void delIterate(Iterator_ &iterator) {
    int slot = iterator.m_slot;
    if (slot < 0) return;
    bool advanceRegardless =
      !m_table[slot].tail() && static_cast<int>(m_table[slot].next()) < slot;
    del__(m_table[slot].head() ? (-slot - 2) : prev(slot));
    if (!advanceRegardless && !!m_table[slot]) iterator.m_next = slot;
    iterator.m_slot = -1;
  }
  void delIterate(KeyIterator_ &iterator) {
    int slot = iterator.m_slot;
    if (slot < 0) return;
    del__(iterator.m_prev < 0 ? (-slot - 2) : iterator.m_prev);
    iterator.m_slot = -1;
    if (!m_table[slot]) return;
    for (;;) {
      if (Cmp::equals(m_table[slot].key(), iterator.m_key)) {
	iterator.m_next = slot;
	return;
      }
      if (m_table[slot].tail()) break;
      slot = m_table[slot].next();
    }
    iterator.m_next = -1;
  }
};

template <typename P0, typename P1, typename NTP = ZmLHash_Defaults>
using ZmLHashKV =
  ZmLHash<ZuPair<P0, P1>,
    ZmLHashKeyVal<ZuPairAxor<0>, ZuPairAxor<1>, NTP> >;

#endif /* ZmLHash_HPP */
