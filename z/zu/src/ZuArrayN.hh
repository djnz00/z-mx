//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// fixed-size arrays for use in structs and passing by value
// * cached length (and constexpr size)
// * explicitly contiguous
// * direct read/write access to the buffer
// * char and wchar_t arrays as strings
//   - smooth interoperation with other string types
// * structured binding

#ifndef ZuArrayN_HH
#define ZuArrayN_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <initializer_list>

#include <zlib/ZuAssert.hh>
#include <zlib/ZuTraits.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuArray.hh>
#include <zlib/ZuArrayFn.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuUTF.hh>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4800 4996 4348)
#endif

template <typename U, typename> struct ZuArrayN_CanAppend_;
template <typename U> struct ZuArrayN_CanAppend_<U, void> { using T = void; };
template <typename U> struct ZuArrayN_CanAppend_<U, U &> { using T = void; };
template <typename U, typename T, typename = void>
struct ZuArrayN_CanAppend : public ZuFalse { };
template <typename U, typename T>
struct ZuArrayN_CanAppend<U, T,
  typename ZuArrayN_CanAppend_<U,
    decltype(ZuDeclVal<U>().append(ZuDeclVal<const T *>(), 1))>::T> :
  public ZuTrue { };

template <typename> struct ZuArrayN___ { };
template <> struct ZuArrayN___<char> {
  friend ZuPrintString ZuPrintType(ZuArrayN___ *);
};

template <typename T>
struct ZuArrayN__ : public ZuArrayN___<ZuStrip<T>> {
protected:
  using Nop_ = enum { Nop };
  ZuArrayN__(Nop_ _) { }

  ZuArrayN__() : m_length{0} { }

  ZuArrayN__(unsigned length) : m_length{length} { }

  uint32_t	m_length;
};

template <typename T_> struct ZuArrayN_Char2 { using T = ZuNull; };
template <> struct ZuArrayN_Char2<char> { using T = wchar_t; };
template <> struct ZuArrayN_Char2<wchar_t> { using T = char; };

template <typename T_, unsigned N_, typename Cmp_, typename ArrayN>
class ZuArrayN_ : public ZuArrayN__<T_>, public ZuArrayFn<T_, Cmp_> {
  ZuInspectFriend;

  ZuArrayN_(const ZuArrayN_ &);
  ZuArrayN_ &operator =(const ZuArrayN_ &);
  bool operator *() const;

  using Base = ZuArrayN__<T_>;
  using Base::m_length;

public:
  using T = T_;
  enum { N = N_ };
  using Char2 = typename ZuArrayN_Char2<T>::T;
  using Cmp = Cmp_;

  enum Nop_ { Nop };

protected:
  // from some string with same char (including string literals)
  template <typename U, typename V = T> struct IsString : public ZuBool<
      (ZuTraits<U>::IsArray || ZuTraits<U>::IsString) &&
      bool{ZuEquivChar<typename ZuTraits<U>::Elem, V>{}}> { };
  template <typename U, typename R = void>
  using MatchString = ZuIfT<IsString<U>{}, R>;

  // from char2 string (requires conversion)
  template <typename U, typename V = Char2>
  struct IsChar2String : public ZuBool<
      !ZuInspect<ZuNull, V>::Same &&
      (ZuTraits<U>::IsArray || ZuTraits<U>::IsString) &&
      bool{ZuEquivChar<typename ZuTraits<U>::Elem, V>{}}> { };
  template <typename U, typename R = void>
  using MatchChar2String = ZuIfT<IsChar2String<U>{}, R>;

  // from any array type with convertible element type (not a string)
  template <typename U, typename V = T> struct IsArray : public ZuBool<
      !IsString<U>{} &&
      !IsChar2String<U>{} &&
      !ZuInspect<U, V>::Same &&
      ZuTraits<U>::IsArray &&
      ZuInspect<typename ZuTraits<U>::Elem, V>::Converts> { };
  template <typename U, typename R = void>
  using MatchArray = ZuIfT<IsArray<U>{}, R>;

  // from individual char2 (requires conversion, char->wchar_t only)
  template <typename U, typename V = Char2> struct IsChar2 :
    public ZuBool<!ZuInspect<ZuNull, V>::Same &&
      ZuInspect<U, V>::Same &&
      !ZuInspect<U, wchar_t>::Same> { };
  template <typename U, typename R = void>
  using MatchChar2 = ZuIfT<IsChar2<U>{}, R>;

  // from printable type (if this is a char array)
  template <typename U, typename V = T> struct IsPDelegate :
    public ZuBool<bool{ZuEquivChar<char, V>{}} && ZuPrint<U>::Delegate> { };
  template <typename U, typename R = void>
  using MatchPDelegate = ZuIfT<IsPDelegate<U>{}, R>;
  template <typename U, typename V = T> struct IsPBuffer :
    public ZuBool<bool{ZuEquivChar<char, V>{}} && ZuPrint<U>::Buffer> { };
  template <typename U, typename R = void>
  using MatchPBuffer = ZuIfT<IsPBuffer<U>{}, R>;

  // from real primitive types other than chars (if this is a char string)
  template <typename U, typename V = T> struct IsReal : public ZuBool<
      bool{ZuEquivChar<char, V>{}} && !bool{ZuEquivChar<U, V>{}} &&
      ZuTraits<U>::IsReal && ZuTraits<U>::IsPrimitive &&
      !ZuTraits<U>::IsArray> { };
  template <typename U, typename R = void>
  using MatchReal = ZuIfT<IsReal<U>{}, R>;

  // from individual element
  template <typename U, typename V = T> struct IsElem : public ZuBool<
      ZuInspect<U, V>::Same ||
      (!IsString<U>{} &&
       !IsArray<U>{} &&
       !IsChar2String<U>{} &&
       !IsChar2<U>{} &&
       !IsPDelegate<U>{} &&
       !IsPBuffer<U>{} &&
       !IsReal<U>{} &&
       ZuInspect<U, V>::Converts)> { };
  template <typename U, typename R = void>
  using MatchElem = ZuIfT<IsElem<U>{}, R>;

  // limit member operator <<() overload resolution to supported types
  template <typename U>
  struct IsStreamable : public ZuBool<
      bool{IsString<U>{}} ||
      bool{IsArray<U>{}} ||
      bool{IsChar2String<U>{}} ||
      bool{IsChar2<U>{}} ||
      bool{IsPDelegate<U>{}} ||
      bool{IsPBuffer<U>{}} ||
      bool{IsReal<U>{}} ||
      bool{IsElem<U>{}}> { };
  template <typename U, typename R = void>
  using MatchStreamable = ZuIfT<IsStreamable<U>{}, R>;

  ~ZuArrayN_() { dtor(); }

  ZuArrayN_() { }

  ZuArrayN_(Nop_ _) : Base(Base::Nop) { }

  ZuArrayN_(unsigned length, bool initItems) : Base(length) {
    if (m_length > N) m_length = N;
    if (initItems) this->initItems(data(), m_length);
  }

  void dtor() {
    this->destroyItems(data(), m_length);
  }

  template <typename A>
  ZuConvertible<A, T> init(const A *a, unsigned length) {
    if (ZuUnlikely(length > N)) length = N;
    if (ZuUnlikely(!a))
      length = 0;
    else if (ZuLikely(length))
      this->copyItems(data(), a, length);
    m_length = length;
  }
  template <typename A>
  ZuConvertible<A, T> init_mv(A *a, unsigned length) {
    if (ZuUnlikely(length > N)) length = N;
    if (ZuUnlikely(!a))
      length = 0;
    else if (ZuLikely(length))
      this->moveItems(data(), a, length);
    m_length = length;
  }

  template <typename S> MatchString<S> init(S &&s_) {
    ZuArray<const T> s(s_);
    init(s.data(), s.length());
  }

  template <typename A> MatchArray<A> init(A &&a) {
    ZuBind<A>::mvcp(ZuFwd<A>(a),
      [this](auto &&a_) {
	using Array = ZuDecay<decltype(a_)>;
	using Elem = typename ZuTraits<Array>::Elem;
	ZuArray<Elem> a(a_);
	this->init_mv(a.data(), a.length());
      },
      [this](const auto &a_) {
	using Array = ZuDecay<decltype(a_)>;
	using Elem = typename ZuTraits<Array>::Elem;
	ZuArray<const Elem> a(a_);
	this->init(a.data(), a.length());
      });
  }

  template <typename S> MatchChar2String<S> init(S &&s) {
    data()[m_length = ZuUTF<T, Char2>::cvt({data(), N}, s)] = 0;
  }
  template <typename C> MatchChar2<C> init(C c) {
    data()[m_length = ZuUTF<T, Char2>::cvt({data(), N}, {&c, 1})] = 0;
  }

  template <typename P> MatchPDelegate<P> init(const P &p) {
    ZuPrint<P>::print(*static_cast<ArrayN *>(this), p);
  }
  template <typename P> MatchPBuffer<P> init(const P &p) {
    unsigned length = ZuPrint<P>::length(p);
    if (length > N)
      m_length = 0;
    else
      m_length = ZuPrint<P>::print(reinterpret_cast<char *>(data()), length, p);
  }

  template <typename V> MatchReal<V> init(V v) {
    init(ZuBoxed(v));
  }

  template <typename E>
  MatchElem<E> init(E &&e) {
    this->initItem(data(), ZuFwd<E>(e));
    m_length = 1;
  }

  template <typename A>
  ZuConvertible<A, T> append_(const A *a, unsigned length) {
    if (m_length + length > N)
      length = N - m_length;
    if (a && length) this->copyItems(data() + m_length, a, length);
    m_length += length;
  }
  template <typename A>
  ZuConvertible<A, T> append_mv_(A *a, unsigned length) {
    if (m_length + length > N)
      length = N - m_length;
    if (a && length) this->moveItems(data() + m_length, a, length);
    m_length += length;
  }

  template <typename S> MatchString<S> append_(S &&s_) {
    ZuArray<const T> s(s_);
    this->append_(s.data(), s.length());
  }

  template <typename A> MatchArray<A> append_(A &&a) {
    ZuBind<A>::mvcp(ZuFwd<A>(a),
      [this](auto &&a_) {
	using Array = ZuDecay<decltype(a_)>;
	using Elem = typename ZuTraits<Array>::Elem;
	ZuArray<Elem> a(a_);
	this->append_mv_(a.data(), a.length());
      },
      [this](const auto &a_) {
	using Array = ZuDecay<decltype(a_)>;
	using Elem = typename ZuTraits<Array>::Elem;
	ZuArray<const Elem> a(a_);
	this->append_(a.data(), a.length());
      });
  }

  template <typename S> MatchChar2String<S> append_(S &&s) {
    if (m_length < N)
      data()[m_length = ZuUTF<T, Char2>::cvt(
	  {data() + m_length, N - m_length}, s)] = 0;
  }

  template <typename C> MatchChar2<C> append_(C c) {
    if (m_length < N)
      data()[m_length += ZuUTF<T, Char2>::cvt(
	  {data() + m_length, N - m_length}, {&c, 1})] = 0;
  }

  template <typename P> MatchPDelegate<P> append_(const P &p) {
    ZuPrint<P>::print(*static_cast<ArrayN *>(this), p);
  }
  template <typename P> MatchPBuffer<P> append_(const P &p) {
    unsigned length = ZuPrint<P>::length(p);
    if (m_length + length > N) return;
    m_length += ZuPrint<P>::print(
	reinterpret_cast<char *>(data()) + m_length, length, p);
  }

  template <typename V> MatchReal<V> append_(V v) {
    append_(ZuBoxed(v));
  }

  template <typename E> MatchElem<E> append_(E &&e) {
    if (m_length >= N) return;
    this->initItem(data() + m_length, ZuFwd<E>(e));
    ++m_length;
  }

public:
// array/ptr operators

  const T &operator [](unsigned i) const { return data()[i]; }
  T &operator [](unsigned i) { return data()[i]; }

// accessors

  constexpr static unsigned size() { return N; }
  unsigned length() const { return m_length; }

// iteration
  template <bool Mutable = false, typename L>
  ZuIfT<!Mutable> all(L l) const {
    auto data_ = data();
    for (unsigned i = 0, n = m_length; i < n; i++) l(data_[i]);
  }
  template <bool Mutable, typename L>
  ZuIfT<Mutable> all(L l) {
    auto data_ = data();
    for (unsigned i = 0, n = m_length; i < n; i++) l(data_[i]);
  }

// STL cruft
  using iterator = T *;
  using const_iterator = const T *;
  const T *begin() const { return data(); }
  const T *end() const { return data() + m_length; }
  const T *cbegin() const { return data(); } // sigh
  const T *cend() const { return data() + m_length; }
  T *begin() { return data(); }
  T *end() { return data() + m_length; }

// reset to null

  void clear() { null(); }
  void null() { m_length = 0; }

// set length

  template <bool InitItems = !ZuTraits<T>::IsPrimitive>
  void length(unsigned length) {
    if (length > N) length = N;
    if constexpr (InitItems) {
      if (length > m_length) {
	this->initItems(data() + m_length, length - m_length);
      } else if (length < m_length) {
	this->destroyItems(data() + length, m_length - length);
      }
    }
    m_length = length;
  }

// push/pop/shift/unshift

  void *push() {
    if (m_length >= N) return nullptr;
    return &(data()[m_length++]);
  }
  template <typename I> T *push(I &&i) {
    auto ptr = push();
    if (ZuLikely(ptr)) this->initItem(ptr, ZuFwd<I>(i));
    return static_cast<T *>(ptr);
  }
  T pop() {
    if (!m_length) return ZuNullRef<T, Cmp>();
    T t = ZuMv(data()[--m_length]);
    this->destroyItem(data() + m_length);
    return t;
  }
  T shift() {
    if (!m_length) return ZuNullRef<T, Cmp>();
    T t = ZuMv(data()[0]);
    this->destroyItem(data());
    this->moveItems(data(), data() + 1, --m_length);
    return t;
  }
  template <typename I> T *unshift(I &&i) {
    if (m_length >= N) return 0;
    this->moveItems(data() + 1, data(), m_length++);
    T *ptr = data();
    this->initItem((void *)ptr, ZuFwd<I>(i));
    return ptr;
  }

  void splice(int offset, int length) {
    splice_(offset, length, (void *)0);
  }
  template <typename U>
  void splice(int offset, int length, U &removed) {
    splice_(offset, length, &removed);
  }

private:
  template <typename U> struct IsVoid :
    public ZuBool<ZuInspect<void, U>::Same> { };
  template <typename U, typename R = void>
  using MatchVoid = ZuIfT<IsVoid<U>{}, R>;

  template <typename U, typename V = T> struct IsAppend :
    public ZuBool<ZuArrayN_CanAppend<U, V>{}> { };
  template <typename U, typename R = void>
  using MatchAppend = ZuIfT<IsAppend<U>{}, R>;

  template <typename U, typename V = T> struct IsSplice :
    public ZuBool<!IsVoid<U>{} && !IsAppend<U>{}> { };
  template <typename U, typename R = void>
  using MatchSplice = ZuIfT<IsSplice<U>{}, R>;

  template <typename U>
  void splice_(int offset, int length, U *removed) {
    if (ZuUnlikely(!length)) return;
    if (offset < 0) { if ((offset += m_length) < 0) offset = 0; }
    if (offset >= (int)N) return;
    if (length < 0) { if ((length += (m_length - offset)) <= 0) return; }
    if (offset + length > (int)N)
      if (!(length = N - offset)) return;
    if (offset > (int)m_length) {
      this->initItems(data() + m_length, offset - m_length);
      m_length = offset;
      return;
    }
    if (offset + length > (int)m_length)
      if (!(length = m_length - offset)) return;
    if (removed) splice__(data() + offset, length, removed);
    this->destroyItems(data() + offset, length);
    this->moveItems(
	data() + offset,
	data() + offset + length,
	m_length - (offset + length));
    m_length -= length;
  }
  template <typename U>
  MatchVoid<U> splice__(const T *, unsigned, U *) { }
  template <typename U>
  MatchAppend<U>
      splice__(const T *data, unsigned length, U *removed) {
    removed->append_(data, length);
  }
  template <typename U>
  MatchSplice<U>
      splice__(const T *data, unsigned length, U *removed) {
    for (unsigned i = 0; i < length; i++) *removed << data[i];
  }

public:
// buffer access

  auto buf() { return ZuArray{data(), N}; }
  auto cbuf() const { return ZuArray{data(), m_length}; }

// comparison

  bool operator !() const { return !m_length; }
  ZuOpBool

protected:
  bool same(const ZuArrayN_ &a) const { return this == &a; }
  template <typename A> bool same(const A &a) const { return false; }

public:
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
  friend inline ZuIfT<ZuInspect<ZuArrayN_, L>::Is, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline ZuIfT<ZuInspect<ZuArrayN_, L>::Is, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

// hash

  uint32_t hash() const {
    return ZuHash<ArrayN>::hash(*static_cast<const ArrayN *>(this));
  }

private:
  struct Align : public Base { T m_data[1]; };

public:
  T *data() {
    return reinterpret_cast<Align *>(this)->m_data;
  }
  const T *data() const {
    return reinterpret_cast<const Align *>(this)->m_data;
  }
};

namespace Zu_ {
template <typename T_, unsigned N_, class Cmp_ = ZuCmp<T_> >
class ArrayN : public ZuArrayN_<T_, N_, Cmp_, ArrayN<T_, N_, Cmp_> > {
  ZuAssert(N_ > 0);
  ZuAssert(N_ < (1U<<16) - 1U);

public:
  using T = T_;
  enum { N = N_ };
  using Cmp = Cmp_;
  using Base = ZuArrayN_<T, N, Cmp, ArrayN>;
  enum Move_ { Move };

private:
  // an unsigned|int|size_t parameter to the constructor is a buffer length
  template <typename U> struct IsCtorLength : public ZuBool<
      ZuInspect<U, unsigned>::Same ||
      ZuInspect<U, int>::Same ||
      ZuInspect<U, size_t>::Same> { };
  template <typename U, typename R = void>
  using MatchCtorLength = ZuIfT<IsCtorLength<U>{}, R>;

  // constructor arg
  template <typename U> struct IsCtorArg : public ZuBool<
      !IsCtorLength<U>{} &&
      !ZuInspect<Base, U>::Base> { };
  template <typename U, typename R = void>
  using MatchCtorArg = ZuIfT<IsCtorArg<U>{}, R>;

public:
  ArrayN() { }

  ArrayN(const ArrayN &a) : Base(Base::Nop) {
    this->init(a.data(), a.length());
  }
  ArrayN &operator =(const ArrayN &a) {
    if (this != &a) {
      this->dtor();
      this->init(a.data(), a.length());
    }
    return *this;
  }

  ArrayN(ArrayN &&a) : Base(Base::Nop) {
    this->init_mv(a.data(), a.length());
  }
  ArrayN &operator =(ArrayN &&a) {
    if (this != &a) {
      this->dtor();
      this->init_mv(a.data(), a.length());
    }
    return *this;
  }

  ArrayN(std::initializer_list<T> a) : Base(Base::Nop) {
    this->init(a.begin(), a.size());
  }

  // miscellaneous types handled by base class
  template <typename A>
  ArrayN(A &&a, MatchCtorArg<A> *_ = nullptr) : Base(Base::Nop) {
    this->init(ZuFwd<A>(a));
  }
  template <typename A>
  ArrayN &operator =(A &&a) {
    this->dtor();
    this->init(ZuFwd<A>(a));
    return *this;
  }
  template <typename U>
  ArrayN &operator +=(U &&v) { return *this << ZuFwd<U>(v); }
  template <typename U>
  Base::template MatchStreamable<U, ArrayN &>
  operator <<(U &&v) {
    this->append_(ZuFwd<U>(v));
    return *this;
  }

  // length
  template <typename L>
  ArrayN(L l, bool initItems = !ZuTraits<T>::IsPrimitive,
      MatchCtorLength<L> *_ = nullptr) : Base(l, initItems) { }

  // arrays as ptr, length
  template <typename A>
  ArrayN(const A *a, unsigned length,
    ZuConvertible<A, T> *_ = nullptr) : Base(Base::Nop) {
    this->init(a, length);
  }
  ArrayN(Move_ _, T *a, unsigned length) : Base(Base::Nop) {
    this->init_mv(a, length);
  }

  // append()
  template <typename A>
  ZuConvertible<A, T> append(const A *a, unsigned length) {
    this->append_(a, length);
  }
  template <typename A>
  ZuConvertible<A, T> append_mv(A *a, unsigned length) {
    this->append_mv_(a, length);
  }

  // traits
  struct Traits : public ZuBaseTraits<ArrayN> {
    using Elem = T;
    enum {
      IsArray = 1, IsPrimitive = 0,
      IsPOD = ZuTraits<T>::IsPOD,
      IsString =
	bool{ZuIsExact<char, ZuDecay<T>>{}} ||
	bool{ZuIsExact<wchar_t, ZuDecay<T>>{}},
      IsWString = bool{ZuIsExact<wchar_t, ZuDecay<T>>{}}
    };
    template <typename U = ArrayN>
    static typename ZuMutable<U, T *>::T data(U &a) { return a.data(); }
    static Elem *data(ArrayN &a) { return a.data(); }
    static const Elem *data(const ArrayN &a) { return a.data(); }
    static unsigned length(const ArrayN &a) { return a.length(); }
  };
  friend Traits ZuTraitsType(ArrayN *);

private:
  char	m_data[N * sizeof(T)];
};

} // namespace Zu_

template <typename T, unsigned N, typename Cmp = ZuCmp<T>>
using ZuArrayN = Zu_::ArrayN<T, N, Cmp>;

// STL structured binding cruft
#include <type_traits>
namespace std {
  template <class> struct tuple_size;
  template <typename T, unsigned N, typename Cmp>
  struct tuple_size<ZuArrayN<T, N, Cmp>> :
  public integral_constant<size_t, N> { };

  template <size_t, typename> struct tuple_element;
  template <size_t I, typename T, unsigned N, typename Cmp>
  struct tuple_element<I, ZuArrayN<T, N, Cmp>> { using type = T; };
}
namespace Zu_ {
  using size_t = std::size_t;
  namespace {
    template <size_t I, typename T>
    using tuple_element_t = typename std::tuple_element<I, T>::type;
  }
  template <size_t I, typename T, unsigned N, typename Cmp>
  constexpr tuple_element_t<I, ArrayN<T, N, Cmp>> &
  get(ArrayN<T, N, Cmp> &a) noexcept { return a[I]; }
  template <size_t I, typename T, unsigned N, typename Cmp>
  constexpr const tuple_element_t<I, ArrayN<T, N, Cmp>> &
  get(const ArrayN<T, N, Cmp> &a) noexcept { return a[I]; }
  template <size_t I, typename T, unsigned N, typename Cmp>
  constexpr tuple_element_t<I, ArrayN<T, N, Cmp>> &&
  get(ArrayN<T, N, Cmp> &&a) noexcept {
    return static_cast<tuple_element_t<I, ArrayN<T, N, Cmp>> &&>(a[I]);
  }
  template <size_t I, typename T, unsigned N, typename Cmp>
  constexpr const tuple_element_t<I, ArrayN<T, N, Cmp>> &&
  get(const ArrayN<T, N, Cmp> &&a) noexcept {
    return static_cast<const tuple_element_t<I, ArrayN<T, N, Cmp>> &&>(a[I]);
  }
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZuArrayN_HH */
