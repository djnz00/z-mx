//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// heap-allocated move-only array

#ifndef ZuMvArray_HH
#define ZuMvArray_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuSpan.hh>
#include <zlib/ZuArrayFn.hh>
#include <zlib/ZuPrint.hh>

template <typename T> struct ZuMvArray_ { };
template <> struct ZuMvArray_<char> {
  friend ZuPrintString ZuPrintType(ZuMvArray_ *);
};

template <typename T_, typename Cmp_ = ZuCmp<T_>>
class ZuMvArray : public ZuMvArray_<ZuStrip<T_>>, public ZuArrayFn<T_, Cmp_> {
  ZuMvArray(const ZuMvArray &) = delete;
  ZuMvArray &operator =(const ZuMvArray &) = delete;

public:
  using T = T_;
  using Cmp = Cmp_;

  ZuMvArray() = default;

  ZuMvArray(unsigned n) :
      m_length{n},
      m_data{!n ?
	static_cast<T *>(nullptr) :
	static_cast<T *>(::malloc(n * sizeof(T)))} {
    if (ZuUnlikely(n && !m_data)) throw std::bad_alloc();
    this->initItems(m_data, n);
  }
  ZuMvArray(T *data, unsigned n) :
      m_length{n},
      m_data{!n ?
	static_cast<T *>(nullptr) :
	static_cast<T *>(::malloc(n * sizeof(T)))} {
    if (ZuUnlikely(n && !m_data)) throw std::bad_alloc();
    this->moveItems(m_data, data, n);
  }
  ~ZuMvArray() {
    if (!m_data) return;
    this->destroyItems(m_data, m_length);
    ::free(m_data);
  }

  ZuMvArray(ZuMvArray &&a) noexcept {
    m_length = a.m_length;
    m_data = a.m_data;
    a.m_length = 0;
    a.m_data = nullptr;
  }
  ZuMvArray &operator =(ZuMvArray &&a) noexcept {
    m_length = a.m_length;
    m_data = a.m_data;
    a.m_length = 0;
    a.m_data = nullptr;
    return *this;
  }

// array/ptr operators
  T &operator [](unsigned i) { return m_data[i]; }
  const T &operator [](unsigned i) const { return m_data[i]; }

// accessors
  unsigned length() const { return m_length; } 

  T *data() { return m_data; }
  const T *data() const { return m_data; }

// release / free
  T *release() && {
    auto ptr = m_data;
    m_length = 0;
    m_data = nullptr;
    return ptr;
  }
  void free(const T *ptr) { ::free(ptr); }

// reset to null
  void null() { ::free(m_data); m_length = 0;  m_data = nullptr;}

// set length
  void length(unsigned newLength) {
    T *oldData = m_data;
    unsigned oldLength = m_length;
    m_length = newLength;
    m_data = static_cast<T *>(::malloc(newLength * sizeof(T)));
    unsigned mvLength = oldLength < newLength ? oldLength : newLength;
    unsigned initLength = newLength - mvLength;
    if (mvLength) this->moveItems(m_data, oldData, mvLength);
    if (initLength) this->initItems(m_data + mvLength, initLength);
    ::free(oldData);
  }

// comparison
  bool operator !() const { return !m_length; }
  ZuOpBool

// iteration
  template <bool Mutable = false, typename L>
  ZuIfT<!Mutable> all(L l) const {
    for (unsigned i = 0, n = m_length; i < n; i++) l(m_data[i]);
  }
  template <bool Mutable, typename L>
  ZuIfT<Mutable> all(L l) {
    for (unsigned i = 0, n = m_length; i < n; i++) l(m_data[i]);
  }

// STL cruft
  using iterator = T *;
  using const_iterator = const T *;
  const T *begin() const { return m_data; }
  const T *end() const { return m_data + m_length; }
  const T *cbegin() const { return m_data; } // sigh
  const T *cend() const { return m_data + m_length; }
  T *begin() { return m_data; }
  T *end() { return m_data + m_length; }

protected:
  bool same(const ZuMvArray &a) const { return this == &a; }
  template <typename A> constexpr bool same(const A &) const { return false; }

  auto buf() { return ZuSpan{data(), length()}; }
  auto cbuf() const { return ZuSpan{data(), length()}; }

  template <typename A>
  bool equals(const A &a) const {
    return same(a) || cbuf().equals(a);
  }
  template <typename A>
  int cmp(const A &a) const {
    if (same(a)) return 0;
    return cbuf().cmp(a);
  }
  template <typename L, typename R>
  friend inline ZuIfT<ZuInspect<ZuMvArray, L>::Is, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline ZuIfT<ZuInspect<ZuMvArray, L>::Is, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

// hash
  uint32_t hash() const {
    return ZuHash<ZuMvArray>::hash(*this);
  }

// traits
  struct Traits : public ZuBaseTraits<ZuMvArray> {
    using Elem = T;
    enum {
      IsArray = 1, IsPrimitive = 0, IsPOD = 0,
      IsString =
	bool{ZuIsExact<char, ZuDecay<T>>{}} ||
	bool{ZuIsExact<wchar_t, ZuDecay<T>>{}},
      IsWString = bool{ZuIsExact<wchar_t, ZuDecay<T>>{}}
    };
    static T *data(ZuMvArray &a) { return a.data(); }
    static const T *data(const ZuMvArray &a) { return a.data(); }
    static unsigned length(const ZuMvArray &a) { return a.length(); }
  };
  friend Traits ZuTraitsType(ZuMvArray *);

private:
  uint32_t	m_length = 0;
  T		*m_data = nullptr;
};

#endif /* ZuMvArray_HH */
