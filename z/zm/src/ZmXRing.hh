//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// simple fast dynamic-sized ring buffer (supports FIFO and LIFO) for types
// with a sentinel null value (defaults to ZuCmp<T>::null())

#ifndef ZmXRing_HH
#define ZmXRing_HH

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <zlib/ZuTraits.hh>
#include <zlib/ZuNull.hh>
#include <zlib/ZuArrayFn.hh>
#include <zlib/ZuCmp.hh>

#include <zlib/ZmAssert.hh>
#include <zlib/ZmGuard.hh>
#include <zlib/ZmLock.hh>
#include <zlib/ZmNoLock.hh>
#include <zlib/ZmVHeap.hh>

// defaults
#define ZmXRingInitial		8
#define ZmXRingIncrement	8
#define ZmXRingMaxFrag		50.0

class ZmXRingParams {
public:
  ZmXRingParams &&initial(unsigned v) { m_initial = v; return ZuMv(*this); }
  ZmXRingParams &&increment(unsigned v) { m_increment = v; return ZuMv(*this); }
  ZmXRingParams &&maxFrag(double v) { m_maxFrag = v; return ZuMv(*this); }

  unsigned initial() const { return m_initial; }
  unsigned increment() const { return m_increment; }
  double maxFrag() const { return m_maxFrag; }

private:
  unsigned	m_initial = ZmXRingInitial;
  unsigned	m_increment = ZmXRingIncrement;
  double	m_maxFrag = ZmXRingMaxFrag;
};

// uses NTP (named template parameters):
//
// ZmXRing<ZtString,			// ring of ZtStrings
//   ZmXRingCmp<ZuICmp> >		// case-insensitive comparison

// NTP defaults
struct ZmXRing_Defaults {
  static constexpr auto KeyAxor = ZuDefaultAxor();
  template <typename T> using CmpT = ZuCmp<T>;
  template <typename T> using KeyCmpT = ZuCmp<T>;
  template <typename T> using OpsT = ZuArrayFn<T>;
  using Lock = ZmNoLock;
  static const char *HeapID() { return "ZmXRing"; }
  enum { Sharded = 0 };
};

// ZmXRingKey - key accessor
template <auto KeyAxor_, typename NTP = ZmXRing_Defaults>
struct ZmXRingKey : public NTP {
  static constexpr auto KeyAxor = KeyAxor_;
};

// ZmXRingCmp - the comparator
template <template <typename> typename Cmp_, typename NTP = ZmXRing_Defaults>
struct ZmXRingCmp : public NTP {
  template <typename T> using CmpT = Cmp_<T>;
  template <typename T> using OpsT = ZuArrayFn<T, Cmp_<T>>;
};

// ZmXRingKeyCmp - the optional value comparator
template <template <typename> typename KeyCmp_, typename NTP = ZmXRing_Defaults>
struct ZmXRingKeyCmp : public NTP {
  template <typename T> using KeyCmpT = KeyCmp_<T>;
};

// ZmXRingLock - the lock type
template <typename Lock_, class NTP = ZmXRing_Defaults>
struct ZmXRingLock : public NTP {
  using Lock = Lock_;
};

// ZmXRingHeapID - the heap ID
template <auto HeapID_, typename NTP = ZmXRing_Defaults>
struct ZmXRingHeapID : public NTP {
  static constexpr auto HeapID = HeapID_;
};

// ZmXRingSharded - sharded heap
template <bool Sharded_, typename NTP = ZmXRing_Defaults>
struct ZmXRingSharded : public NTP {
  enum { Sharded = Sharded_ };
};

// only provide delPtr and findPtr methods to callers of unlocked ZmXRings
// since they are intrinsically not thread-safe

template <typename T, class NTP> class ZmXRing;

template <typename Ring> struct ZmXRing_Unlocked;
template <typename T, class NTP>
struct ZmXRing_Unlocked<ZmXRing<T, NTP> > {
  using Ring = ZmXRing<T, NTP>;

  template <typename P>
  T *findPtr(P &&v) {
    return static_cast<Ring *>(this)->findPtr_(ZuFwd<P>(v));
  }
  void delPtr(T *ptr) {
    return static_cast<Ring *>(this)->delPtr_(ptr);
  }
};

template <typename Ring, class Lock> struct ZmXRing_Base { };
template <typename Ring>
struct ZmXRing_Base<Ring, ZmNoLock> : public ZmXRing_Unlocked<Ring> { };

// derives from Ops so that a ZmXRing includes an *instance* of Ops

template <typename T_, class NTP = ZmXRing_Defaults>
class ZmXRing :
    private ZmVHeap<NTP::HeapID, NTP::Sharded>,
    public ZmXRing_Base<ZmXRing<T_, NTP>, typename NTP::Lock>,
    public NTP::template OpsT<T_> {
  ZmXRing(const ZmXRing &);
  ZmXRing &operator =(const ZmXRing &);	// prevent mis-use

friend ZmXRing_Unlocked<ZmXRing>;

public:
  using T = T_;
  static constexpr auto KeyAxor = NTP::KeyAxor;
  using Key = ZuRDecay<decltype(KeyAxor(ZuDeclVal<const T &>()))>;
  using Ops = typename NTP::template OpsT<T>;
  using Cmp = typename NTP::template CmpT<T>;
  using KeyCmp = typename NTP::template KeyCmpT<T>;
  using Lock = typename NTP::Lock;
  static constexpr auto HeapID = NTP::HeapID;
  enum { Sharded = NTP::Sharded };

  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  ZmXRing(ZmXRingParams params = {}) :
      m_initial{params.initial()}, m_increment{params.increment()},
      m_defrag{1.0 - (double)params.maxFrag() / 100.0} { }

  ~ZmXRing() {
    clean_();
    vfree(m_data);
  }

  ZmXRing(ZmXRing &&ring) noexcept :
      m_initial{ring.m_initial}, m_increment{ring.m_increment},
      m_defrag{ring.m_defrag} {
    Guard guard(ring.m_lock);
    m_data = ring.m_data;
    m_offset = ring.m_offset;
    m_size = ring.m_size;
    m_length = ring.m_length;
    m_count = ring.m_count;
    ring.m_data = nullptr;
    ring.m_offset = 0;
    ring.m_size = 0;
    ring.m_length = 0;
    ring.m_count = 0;
  }
  ZmXRing &operator =(ZmXRing &&ring) noexcept {
    this->~ZmXRing();
    new (this) ZmXRing{ZuMv(ring)};
  }

  unsigned initial() const { return m_initial; }
  unsigned increment() const { return m_increment; }
  unsigned maxFrag() const {
    return (unsigned)((1.0 - m_defrag) * 100.0);
  }

  unsigned size() const { ReadGuard guard(m_lock); return m_size; }
  unsigned length() const { ReadGuard guard(m_lock); return m_length; }
  // unsigned count() const { ReadGuard guard(m_lock); return m_count; }
  unsigned size_() const { return m_size; }
  unsigned length_() const { return m_length; }
  unsigned count_() const { return m_count; }
  unsigned offset_() const { return m_offset; }

private:
  using ZmVHeap<HeapID, Sharded>::valloc;
  using ZmVHeap<HeapID, Sharded>::vfree;

  void lazy() {
    if (ZuUnlikely(!m_data)) extend(m_initial);
  }

  void clean_() {
    if (!m_data) return;
    unsigned o = m_offset + m_length;
    if (o > m_size) {
      int n = m_size - m_offset;
      Ops::destroyItems(m_data + m_offset, n);
      Ops::destroyItems(m_data, o - m_size);
    } else {
      Ops::destroyItems(m_data + m_offset, m_length);
    }
  }

  void extend(unsigned size) {
    T *data = static_cast<T *>(valloc(size * sizeof(T)));
    ZmAssert(data);
    if (m_data) {
      unsigned o = m_offset + m_length;
      if (o > m_size) {
	int n = m_size - m_offset;
	Ops::moveItems(data, m_data + m_offset, n);
	Ops::moveItems(data + n, m_data, o - m_size);
      } else {
	Ops::moveItems(data, m_data + m_offset, m_length);
      }
      vfree(m_data);
    }
    m_data = data, m_size = size, m_offset = 0;
  }

public:
  void init(ZmXRingParams params = ZmXRingParams()) {
    Guard guard(m_lock);
    if ((m_initial = params.initial()) > m_size) extend(params.initial());
    m_increment = params.increment();
    m_defrag = 1.0 - (double)params.maxFrag() / 100.0;
  }

  void clean() {
    Guard guard(m_lock);
    clean_();
    m_offset = m_length = m_count = 0;
  }

private:
  void push() {
    if (m_count >= m_size) extend(m_size + m_increment);
  }
  unsigned offset(unsigned i) {
    if ((i += m_offset) >= m_size) i -= m_size;
    return i;
  }

public:
  template <typename P>
  void push(P &&v) {
    Guard guard(m_lock);

    lazy();
    push();
    Ops::initItem(m_data + offset(m_length++), ZuFwd<P>(v));
    m_count++;
  }

  // idempotent push
  template <typename P>
  void findPush(P &&v) {
    Guard guard(m_lock);

    for (int i = m_length; --i >= 0; ) {
      unsigned o = offset(i);
      if (Cmp::equals(m_data[o], v)) return;
    }
    lazy();
    push();
    Ops::initItem(m_data + offset(m_length++), ZuFwd<P>(v));
    m_count++;
  }

  T pop() {
    Guard guard(m_lock);

    if (m_count <= 0) return Cmp::null();
    --m_count;
    unsigned o = offset(--m_length);
    T v = ZuMv(m_data[o]);
    Ops::destroyItem(m_data + o);
    {
      int i = m_length;
      while (--i >= 0) {
	o = offset(i);
	if (!Cmp::null(m_data[o])) break;
	Ops::destroyItem(m_data + o);
      }
      m_length = ++i;
    }
    return v;
  }

  template <typename P>
  void unshift(P &&v) {
    Guard guard(m_lock);

    lazy();
    push();
    unsigned o = offset(m_size - 1);
    m_offset = o, m_length++;
    Ops::initItem(m_data + o, ZuFwd<P>(v));
    m_count++;
  }

  // idempotent unshift
  template <typename P>
  void findUnshift(P &&v) {
    Guard guard(m_lock);

    for (unsigned i = 0; i < m_length; i++) {
      unsigned o = offset(i);
      if (Cmp::equals(m_data[o], v)) return;
    }
    lazy();
    push();
    unsigned o = offset(m_size - 1);
    m_offset = o, m_length++;
    Ops::initItem(m_data + o, ZuFwd<P>(v));
    m_count++;
  }

  T shift() {
    Guard guard(m_lock);

    if (m_count <= 0) return Cmp::null();
    --m_count;
    int i = m_length, j = 0, o = m_offset;
    T v = ZuMv(m_data[o]);
    do {
      Ops::destroyItem(m_data + o);
    } while (--i > 0 && Cmp::null(m_data[o = offset(++j)]));
    m_offset = o, m_length = i;
    return v;
  }

  T head() {
    Guard guard(m_lock);

    if (m_length <= 0) return Cmp::null();
    return m_data[m_offset];
  }
  T tail() {
    Guard guard(m_lock);

    if (m_length <= 0) return Cmp::null();
    return m_data[offset(m_length - 1)];
  }

  template <typename P>
  T find(const P &v) {
    Guard guard(m_lock);
    for (int i = m_length; --i >= 0; ) {
      unsigned o = offset(i);
      if (KeyCmp::equals(KeyAxor(m_data[o]), v)) return m_data[o];
    }
    return Cmp::null();
  }

private:
  template <typename P>
  T *findPtr_(const P &v) {
    for (int i = m_length; --i >= 0; ) {
      unsigned o = offset(i);
      if (Cmp::equals(m_data[o], v)) return &m_data[o];
    }
    return nullptr;
  }

public:
  template <typename P>
  T del(const P &v) {
    Guard guard(m_lock);
    T *ptr = findPtr_(v);
    if (!ptr) return Cmp::null();
    T data = ZuMv(*ptr);
    delPtr_(ptr);
    return data;
  }

private:
  void delPtr_(T *ptr) {
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4244)
#endif
    int i = ptr - m_data;
#ifdef _MSC_VER
#pragma warning(pop)
#endif

    m_count--;

    int o = offset(m_length - 1);
    if (i == o) {
      Ops::destroyItem(ptr);
      i = --m_length;
      while (--i >= 0) {
	if (!Cmp::null(m_data[o = offset(i)])) break;
	Ops::destroyItem(m_data + o);
      }
      m_length = ++i;
    } else if (i == (int)m_offset) {
      Ops::destroyItem(ptr);
      ++m_offset, --m_length, i = -1;
      while (++i < (int)m_length) {
	if (!Cmp::null(m_data[o = offset(i)])) break;
	Ops::destroyItem(m_data + o);
      }
      if (--i > 0) m_offset = offset(i), m_length -= i;
    } else {
      *ptr = Cmp::null();
      if ((double)m_count < (double)m_length * m_defrag) {
	i = m_length - 1;
	while (--i >= 0) {
	  if (Cmp::null(m_data[offset(i)])) {
	    int j = i;
	    while (--j >= 0 && Cmp::null(m_data[offset(j)])); ++i, ++j;
	    for (int n = j; n < i; n++)
	      Ops::destroyItem(m_data + offset(n));
	    for (int n = m_length - i; --n >= 0; i++, j++) {
	      Ops::initItem(m_data + offset(j), ZuMv(m_data[offset(i)]));
	      Ops::destroyItem(m_data + offset(i));
	    }
	    m_length -= (i - j);
	    i = j;
	  }
	}
      }
    }
  }

  class Iterator_;
friend Iterator_;
  class Iterator_ : private Guard {
    Iterator_(const Iterator_ &);
    Iterator_ &operator =(const Iterator_ &);	// prevent mis-use

  protected:
    using Ring = ZmXRing<T, NTP>;

    Iterator_(Ring &ring, unsigned i) :
	Guard(ring.m_lock), m_ring(ring), m_i(i) { }

    Ring	&m_ring;
    int		m_i;
  };
public:
  class Iterator;
friend Iterator;
  class Iterator : private Iterator_ {
    using Iterator_::m_ring;
    using Iterator_::m_i;
  public:
    Iterator(typename Iterator_::Ring &ring) : Iterator_(ring, 0) { }
    T *iteratePtr() {
      unsigned o;
      do {
	if (m_i >= m_ring.m_length) return nullptr;
	o = m_ring.offset(m_i++);
      } while (Cmp::null(m_ring.m_data[o]));
      return m_ring.m_data + o;
    }
    const T &iterate() {
      unsigned o;
      do {
	if (m_i >= (int)m_ring.m_length) return ZuNullRef<T, Cmp>();
	o = m_ring.offset(m_i++);
      } while (Cmp::null(m_ring.m_data[o]));
      return m_ring.m_data[o];
    }
  };
  auto iterator() { return Iterator{*this}; }
  class RevIterator;
friend RevIterator;
  class RevIterator : private Iterator_ {
    using Iterator_::m_ring;
    using Iterator_::m_i;
  public:
    RevIterator(typename Iterator_::Ring &ring) :
	Iterator_(ring, ring.m_length) { }
    T *iteratePtr() {
      unsigned o;
      do {
	if (m_i <= 0) return nullptr;
	o = m_ring.offset(--m_i);
      } while (Cmp::null(m_ring.m_data[o]));
      return m_ring.m_data + o;
    }
    const T &iterate() {
      unsigned o;
      do {
	if (m_i <= 0) return ZuNullRef<T, Cmp>();
	o = m_ring.offset(--m_i);
      } while (Cmp::null(m_ring.m_data[o]));
      return m_ring.m_data[o];
    }
  };
  auto revIterator() { return RevIterator{*this}; }

private:
  Lock		m_lock;
    T		  *m_data = nullptr;
    unsigned	  m_offset = 0;
    unsigned	  m_size = 0;
    unsigned	  m_length = 0;
    unsigned	  m_count = 0;
    unsigned	  m_initial;
    unsigned	  m_increment;
    double	  m_defrag;
};

#endif /* ZmXRing_HH */
