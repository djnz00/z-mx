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

// simple fast dynamic ring buffer (supports FIFO and LIFO) for types with
// a sentinel null value (defaults to ZuCmp<T>::null())

#ifndef ZmDRing_HPP
#define ZmDRing_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuNull.hpp>
#include <zlib/ZuArrayFn.hpp>
#include <zlib/ZuCmp.hpp>

#include <zlib/ZmAssert.hpp>
#include <zlib/ZmGuard.hpp>
#include <zlib/ZmLock.hpp>
#include <zlib/ZmNoLock.hpp>

// defaults
#define ZmDRingInitial		8
#define ZmDRingIncrement	8
#define ZmDRingMaxFrag		50.0

class ZmDRingParams {
public:
  ZmDRingParams() :
    m_initial(ZmDRingInitial),
    m_increment(ZmDRingIncrement),
    m_maxFrag(ZmDRingMaxFrag) { }

  ZmDRingParams &initial(unsigned v) { m_initial = v; return *this; }
  ZmDRingParams &increment(unsigned v) { m_increment = v; return *this; }
  ZmDRingParams &maxFrag(double v) { m_maxFrag = v; return *this; }

  unsigned initial() const { return m_initial; }
  unsigned increment() const { return m_increment; }
  double maxFrag() const { return m_maxFrag; }

private:
  unsigned	m_initial;
  unsigned	m_increment;
  double	m_maxFrag;
};

// uses NTP (named template parameters):
//
// ZmDRing<ZtString,			// ring of ZtStrings
//   ZmDRingCmp<ZtICmp> >		// case-insensitive comparison

struct ZmDRing_Defaults {
  using KeyAxor = ZuDefaultAxor;
  template <typename T> using CmpT = ZuCmp<T>;
  template <typename T> using KeyCmpT = ZuCmp<T>;
  template <typename T> using OpsT = ZuArrayFn<T>;
  using Lock = ZmNoLock;
};

// ZmDRingKey - key accessor
template <typename KeyAxor_, typename NTP = ZmDRing_Defaults>
struct ZmDRingKey : public NTP {
  using KeyAxor = KeyAxor_;
};

// ZmDRingCmp - the comparator
template <template <typename> typename Cmp_, typename NTP = ZmDRing_Defaults>
struct ZmDRingCmp : public NTP {
  template <typename T> using CmpT = Cmp_<T>;
  template <typename T> using OpsT = ZuArrayFn<T, Cmp_<T>>;
};

// ZmDRingKeyCmp - the optional value comparator
template <template <typename> typename KeyCmp_, typename NTP = ZmDRing_Defaults>
struct ZmDRingKeyCmp : public NTP {
  template <typename T> using KeyCmpT = KeyCmp_<T>;
};

// ZmDRingLock - the lock type
template <class Lock_, class NTP = ZmDRing_Defaults>
struct ZmDRingLock : public NTP {
  using Lock = Lock_;
};

// only provide delPtr and findPtr methods to callers of unlocked ZmDRings
// since they are intrinsically not thread-safe

template <typename T, class NTP> class ZmDRing;

template <class Ring> struct ZmDRing_Unlocked;
template <typename T, class NTP>
struct ZmDRing_Unlocked<ZmDRing<T, NTP> > {
  using Ring = ZmDRing<T, NTP>;

  template <typename P>
  T *findPtr(P &&v) {
    return static_cast<Ring *>(this)->findPtr_(ZuFwd<P>(v));
  }
  void delPtr(T *ptr) {
    return static_cast<Ring *>(this)->delPtr_(ptr);
  }
};

template <class Ring, class Lock> struct ZmDRing_Base { };
template <class Ring>
struct ZmDRing_Base<Ring, ZmNoLock> : public ZmDRing_Unlocked<Ring> { };

// derives from Ops so that a ZmDRing includes an *instance* of Ops

template <typename T_, class NTP = ZmDRing_Defaults>
class ZmDRing :
    public ZmDRing_Base<ZmDRing<T_, NTP>, typename NTP::Lock>,
    public NTP::template OpsT<T_> {
  ZmDRing(const ZmDRing &);
  ZmDRing &operator =(const ZmDRing &);	// prevent mis-use

friend ZmDRing_Unlocked<ZmDRing>;

public:
  using T = T_;
  using KeyAxor = typename NTP::KeyAxor;
  using Key = ZuDecay<decltype(KeyAxor::get(ZuDeclVal<const T &>()))>;
  using Ops = typename NTP::template OpsT<T>;
  using Cmp = typename NTP::template CmpT<T>;
  using KeyCmp = typename NTP::template KeyCmpT<Key>;
  using Lock = typename NTP::Lock;

  using Guard = ZmGuard<Lock>;
  using ReadGuard = ZmReadGuard<Lock>;

  ZmDRing(ZmDRingParams params = ZmDRingParams()) :
      m_data(0), m_offset(0), m_size(0), m_length(0), m_count(0),
      m_initial(params.initial()), m_increment(params.increment()),
      m_defrag(1.0 - (double)params.maxFrag() / 100.0) { }

  ~ZmDRing() {
    clean_();
    free(m_data);
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
  unsigned offset_() const { return m_offset; }

private:
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
    T *data = static_cast<T *>(malloc(size * sizeof(T)));
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
      free(m_data);
    }
    m_data = data, m_size = size, m_offset = 0;
  }

public:
  void init(ZmDRingParams params = ZmDRingParams()) {
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
      if (KeyCmp::equals(KeyAxor::get(m_data[o]), v)) return m_data[o];
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
    using Ring = ZmDRing<T, NTP>;

    Iterator_(Ring &ring, unsigned i) :
	Guard(ring.m_lock), m_ring(ring), m_i(i) { }

    Ring	&m_ring;
    int		m_i;
  };
public:
  class Iterator;
friend Iterator;
  class Iterator : private Iterator_ {
  public:
    Iterator(typename Iterator_::Ring &ring) : Iterator_(ring, 0) { }
    T *iteratePtr() {
      unsigned o;
      do {
	if (this->m_i >= this->m_ring.m_length) return nullptr;
	o = this->m_ring.offset(this->m_i++);
      } while (Cmp::null(this->m_ring.m_data[o]));
      return this->m_ring.m_data + o;
    }
    const T &iterate() {
      unsigned o;
      do {
	if (this->m_i >= (int)this->m_ring.m_length) return Cmp::null();
	o = this->m_ring.offset(this->m_i++);
      } while (Cmp::null(this->m_ring.m_data[o]));
      return this->m_ring.m_data[o];
    }
  };
  auto iterator() { return Iterator{*this}; }
  class RevIterator;
friend RevIterator;
  class RevIterator : private Iterator_ {
  public:
    RevIterator(typename Iterator_::Ring &ring) :
	Iterator_(ring, ring.m_length) { }
    T *iteratePtr() {
      unsigned o;
      do {
	if (this->m_i <= 0) return nullptr;
	o = this->m_ring.offset(--this->m_i);
      } while (Cmp::null(this->m_ring.m_data[o]));
      return this->m_ring.m_data + o;
    }
    const T &iterate() {
      unsigned o;
      do {
	if (this->m_i <= 0) return Cmp::null();
	o = this->m_ring.offset(--this->m_i);
      } while (Cmp::null(this->m_ring.m_data[o]));
      return this->m_ring.m_data[o];
    }
  };
  auto revIterator() { return RevIterator{*this}; }

private:
  Lock		m_lock;
  T		*m_data;
  unsigned	m_offset;
  unsigned	m_size;
  unsigned	m_length;
  unsigned	m_count;
  unsigned	m_initial;
  unsigned	m_increment;
  double	m_defrag;
};

#endif /* ZmDRing_HPP */
