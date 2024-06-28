//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// fixed-size sliding window with dynamically allocated underlying buffer

#ifndef ZtWindow_HH
#define ZtWindow_HH

#ifndef ZtLib_HH
#include <zlib/ZtLib.hh>
#endif

#include <zlib/ZuIterator.hh>

#include <zlib/ZtArray.hh>

namespace ZtWindow_ {

template <typename Window_, typename Elem_>
class Iterator : public ZuIterator<Iterator<Window_, Elem_>, Window_, Elem_> {
  using Base = ZuIterator<Iterator<Window_, Elem_>, Window_, Elem_>;
public:
  using Window = Window_;
  using Elem = Elem_;
  using Base::Base;
  using Base::operator =;
  using Base::container;
  using Base::i;

  Elem operator *() const;
};

template <typename Window_>
class Elem {
public:
  using Window = Window_;
  using T = typename Window::T;

  Elem() = delete;
  Elem(Window &window, unsigned i) : m_window{window}, m_i{i} { }
  Elem(const Elem &) = default;
  Elem &operator =(const Elem &) = default;
  Elem(Elem &&) = default;
  Elem &operator =(Elem &&) = default;

  const T &get() const;

  operator const T &() const { return get(); }

  void set(T v);

  Elem &operator =(T v);

  bool equals(const Elem &r) const { return get() == r.get(); }
  int cmp(const Elem &r) const { return ZuCmp<T>::cmp(get(), r.get()); }
  friend inline bool
  operator ==(const Elem &l, const Elem &r) { return l.equals(r); }
  friend inline int
  operator <=>(const Elem &l, const Elem &r) { return l.cmp(r); }

  bool operator !() const { return !get(); }

  // traits
  using Traits = ZuWrapTraits<Elem, T>;
  friend Traits ZuTraitsType(Elem *);

  // underlying type
  friend T ZuUnderType(Elem *);

private:
  Window	&m_window;
  unsigned	m_i;
};

template <typename T_>
struct Window {
public:
  using T = T_;
  using Elem = ZtWindow_::Elem<Window>;

  Window() = default;
  Window(const Window &) = default;
  Window &operator =(const Window &) = default;
  Window(Window &&) = default;
  Window &operator =(Window &&) = default;

  Window(unsigned max) : m_max{max} { }

  void clear() {
    m_offset = 0;
    m_data = {};
  }

  const Elem operator [](unsigned i) const {
    return {const_cast<Window &>(*this), i};
  }
  Elem operator [](unsigned i) { return {*this, i}; }

  void set(unsigned i, T v) {
    if (i < m_offset) return;
    if (i >= m_offset + m_max) {
      unsigned newOffset = (i + 1) - m_max;
      if (newOffset >= m_offset + m_max)
	m_data = {};
      else
	while (m_offset < newOffset) {
	  unsigned j = m_offset % m_max;
	  if (j < m_data.length()) m_data[j] = {};
	  ++m_offset;
	}
    }
    unsigned j = i % m_max;
    unsigned o = m_data.length();
    if (j >= o) {
      unsigned n = j + 1;
      n = ZmGrow(o * sizeof(T), n * sizeof(T)) / sizeof(T);
      if (n > m_max) n = m_max;
      m_data.length(n);
    }
    m_data[j] = ZuMv(v);
  }

  void clr(unsigned i) { if ((i = index(i)) >= 0) m_data[i] = {}; }

  const T *ptr(unsigned i) const {
    if ((i = index(i)) < 0 || !m_data[i]) return nullptr;
    return &m_data[i];
  }

  using iterator = Iterator<Window, Elem>;
  using const_iterator = Iterator<const Window, const Elem>;
  const_iterator begin() const { return const_iterator{*this, m_offset}; }
  const_iterator end() const {
    return const_iterator{*this, m_offset + m_max};
  }
  const_iterator cbegin() const { return const_iterator{*this, m_offset}; }
  const_iterator cend() const {
    return const_iterator{*this, m_offset + m_max};
  }
  iterator begin() { return iterator{*this, 0}; }
  iterator end() { return iterator{*this, m_offset + m_max}; }

private:
  int index(unsigned i) const {
    if (i < m_offset || i >= m_offset + m_max) return -1;
    i %= m_max;
    if (i >= m_data.length()) return -1;
    return i;
  }

  ZtArray<T>	m_data;
  unsigned	m_offset = 0;
  unsigned	m_max = 100;
};

template <typename Window, typename Elem>
inline Elem Iterator<Window, Elem>::operator *() const {
  return container[i];
}

template <typename Window>
inline const typename Elem<Window>::T &Elem<Window>::get() const {
  if (auto ptr = m_window.ptr(m_i)) return *ptr;
  return ZuNullRef<T>();
}

template <typename Window>
inline void Elem<Window>::set(T v) {
  m_window.set(m_i, ZuMv(v));
}

} // ZtWindow_

template <typename T> using ZtWindow = ZtWindow_::Window<T>;

#endif /* ZtWindow_HH */
