//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// simple fast stack (LIFO array) for types with
// a distinguished null value (defaults to ZuCmp<T>::null())

#ifndef ZmStack_HH
#define ZmStack_HH

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <zlib/ZuTraits.hh>
#include <zlib/ZuNull.hh>
#include <zlib/ZuArrayFn.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuInspect.hh>

#include <zlib/ZmAssert.hh>
#include <zlib/ZmGuard.hh>
#include <zlib/ZmNoLock.hh>
#include <zlib/ZmVHeap.hh>

// defaults
#define ZmStackInitial		4
#define ZmStackIncrement	8
#define ZmStackMaxFrag		50.0

struct ZmStackParams {
  ZmStackParams &&initial(unsigned v) { m_initial = v; return ZuMv(*this); }
  ZmStackParams &&increment(unsigned v) { m_increment = v; return ZuMv(*this); }
  ZmStackParams &&maxFrag(double v) { m_maxFrag = v; return ZuMv(*this); }

  unsigned initial() const { return m_initial; }
  unsigned increment() const { return m_increment; }
  double maxFrag() const { return m_maxFrag; }

private:
  unsigned	m_initial = ZmStackInitial;
  unsigned	m_increment = ZmStackIncrement;
  double	m_maxFrag = ZmStackMaxFrag;
};

// uses NTP (named template parameters):
//
// ZmStack<ZtString>			// stack of ZtStrings
//    ZmStackCmp<ZuICmp> >		// case-insensitive comparison

// NTP defaults
struct ZmStack_Defaults {
  static constexpr auto KeyAxor = ZuDefaultAxor();
  template <typename T> using CmpT = ZuCmp<T>;
  template <typename T> using KeyCmpT = ZuCmp<T>;
  template <typename T> using OpsT = ZuArrayFn<T>;
  using Lock = ZmNoLock;
  static const char *HeapID() { return "ZmStack"; }
  enum { Sharded = 0 };
};

// ZmStackKey - key accessor
template <auto KeyAxor_, typename NTP = ZmStack_Defaults>
struct ZmStackKey : public NTP {
  static constexpr auto KeyAxor = KeyAxor_;
};

// ZmStackCmp - the comparator
template <template <typename> typename Cmp_, typename NTP = ZmStack_Defaults>
struct ZmStackCmp : public NTP {
  template <typename T> using CmpT = Cmp_<T>;
  template <typename T> using OpsT = ZuArrayFn<T, Cmp_<T>>;
};

// ZmStackKeyCmp - the optional value comparator
template <template <typename> typename KeyCmp_, typename NTP = ZmStack_Defaults>
struct ZmStackKeyCmp : public NTP {
  template <typename T> using KeyCmpT = KeyCmp_<T>;
};

// ZmStackLock - the lock type
template <class Lock_, class NTP = ZmStack_Defaults>
struct ZmStackLock : public NTP {
  using Lock = Lock_;
};

// ZmStackHeapID - the heap ID
template <auto HeapID_, typename NTP = ZmStack_Defaults>
struct ZmStackHeapID : public NTP {
  static constexpr auto HeapID = HeapID_;
};

// ZmStackSharded - sharded heap
template <bool Sharded_, typename NTP = ZmStack_Defaults>
struct ZmStackSharded : public NTP {
  enum { Sharded = Sharded_ };
};

// only provide delPtr and findPtr methods to callers of unlocked ZmStacks
// since they are intrinsically not thread-safe

template <typename T, class NTP> class ZmStack;

template <class Stack> struct ZmStack_Unlocked;
template <typename T, class NTP>
struct ZmStack_Unlocked<ZmStack<T, NTP> > {
  using Stack = ZmStack<T, NTP>;

  template <typename P>
  T *findPtr(P &&v) {
    return static_cast<Stack *>(this)->findPtr_(ZuFwd<P>(v));
  }
  void delPtr(T *ptr) {
    return static_cast<Stack *>(this)->delPtr_(ptr);
  }
};

template <class Stack, class Lock> struct ZmStack_Base { };
template <class Stack>
struct ZmStack_Base<Stack, ZmNoLock> : public ZmStack_Unlocked<Stack> { };

// derives from Ops so that a ZmStack includes an *instance* of Ops

template <typename T_, class NTP = ZmStack_Defaults> class ZmStack :
    private ZmVHeap<NTP::HeapID, NTP::Sharded>,
    public ZmStack_Base<ZmStack<T_, NTP>, typename NTP::Lock>,
    public NTP::template OpsT<T_> {
  ZmStack(const ZmStack &);
  ZmStack &operator =(const ZmStack &);	// prevent mis-use

friend ZmStack_Unlocked<ZmStack>;

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

  ZmStack(ZmStackParams params = {}) :
      m_initial(params.initial()),
      m_increment(params.increment()),
      m_defrag(1.0 - (double)params.maxFrag() / 100.0) { }

  ~ZmStack() {
    clean_();
    vfree(m_data);
  }

  ZmStack(ZmStack &&stack) noexcept :
      m_initial{stack.m_initial}, m_increment{stack.m_increment},
      m_defrag{stack.m_defrag} {
    Guard guard(stack.m_lock);
    m_data = stack.m_data;
    m_size = stack.m_size;
    m_length = stack.m_length;
    m_count = stack.m_count;
    stack.m_data = nullptr;
    stack.m_size = 0;
    stack.m_length = 0;
    stack.m_count = 0;
  }
  ZmStack &operator =(ZmStack &&stack) noexcept {
    this->~ZmStack();
    new (this) ZmStack{ZuMv(stack)};
  }

  unsigned initial() const { return m_initial; }
  unsigned increment() const { return m_increment; }
  unsigned maxFrag() const {
    return (unsigned)((1.0 - m_defrag) * 100.0);
  }

  unsigned size() const { ReadGuard guard(m_lock); return m_size; }
  unsigned length() const { ReadGuard guard(m_lock); return m_length; }
  unsigned count() const { ReadGuard guard(m_lock); return m_count; }
  unsigned size_() const { return m_size; }
  unsigned length_() const { return m_length; }
  unsigned count_() const { return m_count; }

private:
  using ZmVHeap<HeapID, Sharded>::valloc;
  using ZmVHeap<HeapID, Sharded>::vfree;

  void lazy() {
    if (ZuUnlikely(!m_data)) extend(m_initial);
  }

  void clean_() {
    if (!m_data) return;
    Ops::destroyItems(m_data, m_length);
  }

  void extend(unsigned size) {
    T *data = static_cast<T *>(valloc((m_size = size) * sizeof(T)));
    ZmAssert(data);
    if (m_data) {
      Ops::moveItems(data, m_data, m_length);
      vfree(m_data);
    }
    m_data = data;
  }

public:
  void init(ZmStackParams params = ZmStackParams()) {
    Guard guard(m_lock);
    if ((m_initial = params.initial()) > m_size) extend(params.initial());
    m_increment = params.increment();
    m_defrag = 1.0 - (double)params.maxFrag() / 100.0;
  }

  void clean() {
    Guard guard(m_lock);
    clean_();
    m_length = m_count = 0;
  }

  template <typename P>
  void push(P &&v) {
    Guard guard(m_lock);
    lazy();
    if (m_length >= m_size) {
      T *data = static_cast<T *>(valloc((m_size += m_increment) * sizeof(T)));
      ZmAssert(data);
      Ops::moveItems(data, m_data, m_length);
      vfree(m_data);
      m_data = data;
    }
    Ops::initItem(m_data + m_length++, ZuFwd<P>(v));
    m_count++;
  }

  T pop() {
    Guard guard(m_lock);
    if (m_length <= 0) return ZuNullRef<T, Cmp>();
    --m_count;
    T v = ZuMv(m_data[--m_length]);
    {
      int i = m_length;
      while (--i >= 0 && Cmp::null(m_data[i])); ++i;
      Ops::destroyItems(m_data + i, m_length + 1 - i);
      m_length = i;
    }
    return v;
  }

  T head() {
    Guard guard(m_lock);
    for (unsigned i = 0; i < m_length; i++)
      if (!Cmp::null(m_data[i])) return m_data[i];
    return ZuNullRef<T, Cmp>();
  }
  T tail() {
    Guard guard(m_lock);
    if (m_length <= 0) return ZuNullRef<T, Cmp>();
    return m_data[m_length - 1];
  }

  template <typename P>
  T find(const P &v) {
    T *ptr = findPtr_(v);
    if (ZuLikely(ptr)) return *ptr;
    return ZuNullRef<T, Cmp>();
  }

private:
  template <typename P>
  T *findPtr_(const P &v) {
    Guard guard(m_lock);
    return findPtr__(v);
  }

  template <typename P>
  T *findPtr__(const P &v) {
    for (int i = m_length; --i >= 0; )
      if (KeyCmp::equals(KeyAxor(m_data[i]), v)) return &m_data[i];
    return nullptr;
  }

public:
  template <typename P>
  T del(const P &v) {
    Guard guard(m_lock);
    T *ptr = findPtr__(v);
    if (!ptr) return ZuNullRef<T, Cmp>();
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
    if (m_defrag >= 1.0) {
      Ops::destroyItems(m_data + i, 1);
      if (i < (int)--m_length)
	Ops::moveItems(m_data + i, m_data + i + 1, m_length - i);
    } else if (i == (int)m_length - 1) {
      while (--i >= 0 && Cmp::null(m_data[i])); ++i;
      Ops::destroyItems(m_data + i, m_length - i);
      m_length = i;
    } else {
      *ptr = Cmp::null();
      if ((double)m_count < (double)m_length * m_defrag) {
	i = m_length - 1;
	while (--i >= 0) {
	  if (Cmp::null(m_data[i])) {
	    int j;
	    for (j = i; --j >= 0 && Cmp::null(m_data[j]); ); ++i, ++j;
	    Ops::destroyItems(m_data + j, i - j);
	    Ops::moveItems(m_data + j, m_data + i, m_length - i);
	    m_length -= (i - j);
	    i = j;
	  }
	}
      }
    }
  }

public:
  class Iterator;
friend Iterator;
  class Iterator : private Guard {
    Iterator(const Iterator &);
    Iterator &operator =(const Iterator &);	// prevent mis-use

    using Stack = ZmStack<T, NTP>;

  public:
    Iterator(Stack &stack) :
      Guard(stack.m_lock), m_stack(stack), m_i(stack.m_length) { }
    T *iteratePtr() {
      do {
	if (m_i <= 0) return nullptr;
      } while (Cmp::null(m_stack.m_data[--m_i]));
      return &m_stack.m_data[m_i];
    }
    const T &iterate() {
      do {
	if (m_i <= 0) return ZuNullRef<T, Cmp>();
      } while (Cmp::null(m_stack.m_data[--m_i]));
      return m_stack.m_data[m_i];
    }

  private:
    Stack	&m_stack;
    int		m_i;
  };
  auto iterator() { return Iterator(*this); }

private:
  Lock		m_lock;
  T		*m_data = nullptr;
  unsigned	m_size = 0;
  unsigned	m_length = 0;
  unsigned	m_count = 0;
  unsigned	m_initial;
  unsigned	m_increment;
  double	m_defrag;
};

#endif /* ZmStack_HH */
