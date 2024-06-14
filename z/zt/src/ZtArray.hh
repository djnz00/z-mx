//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// heap-allocated dynamic array class
// * explicitly contiguous
// * lightweight, lean, fast
// * uses ZmVHeap when not shadowing memory managed elsewhere
// * provides direct read/write access to the buffer
// * zero-copy and deep-copy
// * ZtArray<T> where T is a byte is heavily overloaded as a string

// Note: use ZuArrayN<> for fixed-size arrays without heap overhead

#ifndef ZtArray_HH
#define ZtArray_HH

#ifndef ZtLib_HH
#include <zlib/ZtLib.hh>
#endif

#include <initializer_list>

#include <stdlib.h>
#include <string.h>

#include <zlib/ZuInt.hh>
#include <zlib/ZuNull.hh>
#include <zlib/ZuTraits.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuHash.hh>
#include <zlib/ZuArrayFn.hh>
#include <zlib/ZuUTF.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuBox.hh>
#include <zlib/ZuEquiv.hh>

#include <zlib/ZmAssert.hh>
#include <zlib/ZmVHeap.hh>

#include <zlib/ZtPlatform.hh>
#include <zlib/ZtIconv.hh>

// uses NTP (named template parameters):
//
// ZtArray<ZtString,			// array of ZtStrings
//   ZtArrayCmp<ZuICmp> >		// case-insensitive comparison

// NTP defaults
struct ZtArray_Defaults {
  template <typename T> using CmpT = ZuCmp<T>;
  static const char *HeapID() { return "ZtArray"; }
};

// ZtArrayCmp - the comparator
template <template <typename> typename Cmp_, typename NTP = ZtArray_Defaults>
struct ZtArrayCmp : public NTP {
  template <typename T> using CmpT = Cmp_<T>;
};

// ZtArrayHeapID - the heap ID
template <auto HeapID_, typename NTP = ZtArray_Defaults>
struct ZtArrayHeapID : public NTP {
  constexpr static auto HeapID = HeapID_;
};

template <typename T, typename NTP> class ZtArray;

template <typename T> struct ZtArray_ { };
template <> struct ZtArray_<char> {
  friend ZuPrintString ZuPrintType(ZtArray_ *);
};

template <typename T_> struct ZtArray_Char2 { using T = ZuNull; };
template <> struct ZtArray_Char2<char> { using T = wchar_t; };
template <> struct ZtArray_Char2<wchar_t> { using T = char; };

template <typename T_, typename NTP = ZtArray_Defaults>
class ZtArray :
    private ZmVHeap<NTP::HeapID>,
    public ZtArray_<ZuStrip<T_>>,
    public ZuArrayFn<T_, typename NTP::template CmpT<T_>> {
  template <typename, typename> friend class ZtArray;

public:
  using T = T_;
  using Cmp = typename NTP::template CmpT<T>;
  constexpr static auto HeapID = NTP::HeapID;

  using Ops = ZuArrayFn<T, Cmp>;

  struct Move { };

private:
  using Char = T;
  using Char2 = typename ZtArray_Char2<T>::T;

  template <typename U, typename V = T>
  struct IsZtArray : public ZuBool<
      bool(ZuEquiv<typename ZuTraits<U>::Elem, V>{}) &&
      ZuInspect<ZtArray_<typename ZuTraits<U>::Elem>, U>::Base> { };
  template <typename U, typename R = void>
  using MatchZtArray = ZuIfT<IsZtArray<U>{}, R>;

  // from string literal with same char
  template <typename U, typename V = Char>
  struct IsStrLiteral : public ZuBool<
      ZuTraits<U>::IsCString &&
      ZuIsExact<U, const V (&)[sizeof(U) / sizeof(V)]>{}> { };
  template <typename U, typename R = void>
  using MatchStrLiteral = ZuIfT<IsStrLiteral<U>{}, R>;

  // from some other string with equivalent char (including string literals)
  template <typename U, typename V = Char>
  struct IsAnyString : public ZuBool<
      !IsZtArray<U>{} &&
      (ZuTraits<U>::IsSpan || ZuTraits<U>::IsString) &&
      bool(ZuEquiv<typename ZuTraits<U>::Elem, V>{})> { };
  template <typename U, typename R = void>
  using MatchAnyString = ZuIfT<IsAnyString<U>{}, R>;

  // from some other string with equivalent char (other than a string literal)
  template <typename U, typename V = Char>
  struct IsString : public ZuBool<!IsStrLiteral<U>{} && IsAnyString<U>{}> { };
  template <typename U, typename R = void>
  using MatchString = ZuIfT<IsString<U>{}, R>;

  // from char2 string (requires conversion)
  template <typename U, typename V = Char2>
  struct IsChar2String : public ZuBool<
      !ZuInspect<ZuNull, V>::Same &&
      (ZuTraits<U>::IsSpan || ZuTraits<U>::IsString) &&
      bool(ZuEquiv<typename ZuTraits<U>::Elem, V>{})> { };
  template <typename U, typename R = void>
  using MatchChar2String = ZuIfT<IsChar2String<U>{}, R>;

  // from another array type with convertible element type (not a string)
  template <typename U, typename V = T>
  struct IsSpan : public ZuBool<
      !IsZtArray<U>{} &&
      !IsAnyString<U>{} &&
      !IsChar2String<U>{} &&
      !ZuInspect<U, V>::Same &&
      ZuTraits<U>::IsSpan &&
      ZuInspect<typename ZuTraits<U>::Elem, V>::Converts> { };
  template <typename U, typename R = void>
  using MatchSpan = ZuIfT<IsSpan<U>{}, R>;

  // from another array type with same element type (not a string)
  template <typename U, typename V = T>
  struct IsSameSpan : public ZuBool<
      !IsZtArray<U>{} &&
      !IsAnyString<U>{} &&
      !IsChar2String<U>{} &&
      !ZuInspect<U, V>::Same &&
      ZuTraits<U>::IsSpan &&
      ZuInspect<typename ZuTraits<U>::Elem, V>::Same> { };
  template <typename U, typename R = void>
  using MatchSameSpan = ZuIfT<IsSameSpan<U>{}, R>;

  // from any STL iterable with convertible element type (not array or string)
  template <typename U, typename = void>
  struct IsIterable_ : public ZuFalse { };
  template <typename U>
  struct IsIterable_<U, decltype(
    ZuDeclVal<const U &>().end() - ZuDeclVal<const U &>().begin(), void())> :
      public ZuTrue { };
  template <typename U, typename V = T>
  struct IsIterable : public ZuBool<
      !IsZtArray<U>{} &&
      !IsAnyString<U>{} &&
      !IsChar2String<U>{} &&
      !ZuInspect<U, V>::Same &&
      !ZuTraits<U>::IsSpan &&
      bool(IsIterable_<ZuDecay<U>>{}) &&
      ZuInspect<typename ZuTraits<U>::Elem, V>::Constructs> { };
  template <typename U, typename R = void>
  using MatchIterable = ZuIfT<IsIterable<U>{}, R>;

  // from individual char2 (requires conversion)
  template <typename U, typename V = Char2>
  struct IsChar2 : public ZuBool<
      !ZuIsExact<ZuNull, V>{} &&
      bool(ZuEquiv<U, V>{}) &&
      !ZuEquiv<U, wchar_t>{}> { };
  template <typename U, typename R = void>
  using MatchChar2 = ZuIfT<IsChar2<U>{}, R>;

  // from printable type (if this is a char array)
  template <typename U, typename V = Char>
  struct IsPrint : public ZuBool<
      bool(ZuEquiv<char, V>{}) &&
      ZuPrint<U>::OK &&
      !ZuPrint<U>::String> { };
  template <typename U, typename R = void>
  using MatchPrint = ZuIfT<IsPrint<U>{}, R>;
  template <typename U, typename V = Char> struct IsPDelegate :
    public ZuBool<bool(ZuEquiv<char, V>{}) && ZuPrint<U>::Delegate> { };
  template <typename U, typename R = void>
  using MatchPDelegate = ZuIfT<IsPDelegate<U>{}, R>;
  template <typename U, typename V = Char> struct IsPBuffer :
    public ZuBool<bool(ZuEquiv<char, V>{}) && ZuPrint<U>::Buffer> { };
  template <typename U, typename R = void>
  using MatchPBuffer = ZuIfT<IsPBuffer<U>{}, R>;

  // from real primitive types other than chars (if this is a char array)
  template <typename U, typename V = T>
  struct IsReal : public ZuBool<
      bool(ZuEquiv<char, V>{}) && !bool(ZuEquiv<U, V>{}) &&
      ZuTraits<U>::IsReal && ZuTraits<U>::IsPrimitive &&
      !ZuTraits<U>::IsArray> { };
  template <typename U, typename R = void>
  using MatchReal = ZuIfT<IsReal<U>{}, R>;

  // from primitive pointer (not an array, string, or otherwise printable)
  template <typename U, typename V = Char>
  struct IsPtr : public ZuBool<
      bool(ZuEquiv<char, V>{}) &&
      ZuTraits<U>::IsPointer && ZuTraits<U>::IsPrimitive &&
      !ZuTraits<U>::IsArray && !ZuTraits<U>::IsString> { };
  template <typename U, typename R = void>
  using MatchPtr = ZuIfT<IsPtr<U>{}, R>;

  // from individual element
  template <typename U, typename V = T>
  struct IsElem : public ZuBool<
      ZuInspect<U, V>::Same ||
      (!IsZtArray<U>{} &&
       !IsString<U>{} &&
       !ZuTraits<U>::IsArray &&	// broader than !IsSpan
       !IsChar2<U>{} &&
       !IsPrint<U>{} &&
       !IsReal<U>{} &&
       !IsPtr<U>{} &&
       ZuInspect<U, V>::Converts)> { };
  template <typename U, typename R = void>
  using MatchElem = ZuIfT<IsElem<U>{}, R>;

  // limit member operator <<() overload resolution to supported types
  template <typename U>
  struct IsStreamable : public ZuBool<
      bool(IsZtArray<U>{}) ||
      bool(IsSpan<U>{}) ||
      bool(IsAnyString<U>{}) ||
      bool(IsChar2String<U>{}) ||
      bool(IsChar2<U>{}) ||
      bool(IsPDelegate<U>{}) ||
      bool(IsPBuffer<U>{}) ||
      bool(IsReal<U>{}) ||
      bool(IsPtr<U>{}) ||
      bool(IsElem<U>{})> { };
  template <typename U, typename R = void>
  using MatchStreamable = ZuIfT<IsStreamable<U>{}, R>;

  // an unsigned|int|size_t parameter to the constructor is a buffer size
  template <typename U, typename V = T>
  struct IsCtorSize : public ZuBool<
      ZuInspect<U, unsigned>::Same ||
      ZuInspect<U, int>::Same ||
      ZuInspect<U, size_t>::Same> { };
  template <typename U, typename R = void>
  using MatchCtorSize = ZuIfT<IsCtorSize<U>{}, R>;

  // construction from individual element
  template <typename U, typename V = T, typename W = Char2>
  struct IsCtorElem : public ZuBool<
      !IsZtArray<U>{} &&
      !IsString<U>{} &&
      !ZuTraits<U>::IsArray &&	// broader than !IsSpan
      !IsChar2<U>{} &&
      !IsPrint<U>{} &&
      !IsReal<U>{} &&
      !IsPtr<U>{} &&
      !IsCtorSize<U>{} &&
      ZuInspect<U, V>::Converts> { };
  template <typename U, typename R = void>
  using MatchCtorElem = ZuIfT<IsCtorElem<U>{}, R>;

public:
  ZtArray() { null_(); }
private:
  enum NoInit_ { NoInit };
  ZtArray(NoInit_ _) { }
public:
  ZtArray(const ZtArray &a) { ctor(a); }
  ZtArray(ZtArray &&a) noexcept {
    if (!a.owned())
      shadow_(a.m_data, a.length());
    else {
      own_(a.m_data, a.length(), a.size(), a.vallocd());
      a.owned(false);
    }
  }
  ZtArray(std::initializer_list<T> a) {
    copy__(a.begin(), a.size());
  }

  template <typename A> ZtArray(A &&a) { ctor(ZuFwd<A>(a)); }

  template <typename A> ZtArray(Move, A &a_) {
    ZuArray<const typename ZuTraits<A>::Elem> a{a_};
    move__(a.data(), a.length());
  }

private:
  using ZmVHeap<HeapID>::valloc;
  using ZmVHeap<HeapID>::vfree;

  template <typename A_> struct Fwd_ZtArray {
    using A = ZuDecay<A_>;

    static void ctor_(ZtArray *this_, const A &a) {
      this_->copy__(a.m_data, a.length());
    }
    static void ctor_(ZtArray *this_, A &&a) {
      if (!a.owned())
	this_->shadow_(reinterpret_cast<T *>(a.m_data), a.length());
      else {
	this_->own_(
	    reinterpret_cast<T *>(a.m_data), a.length(), a.size(), a.vallocd());
	a.owned(false);
      }
    }

    static void assign_(ZtArray *this_, const A &a) {
      uint32_t oldLength = 0;
      T *oldData = this_->free_1(oldLength);
      this_->copy__(a.m_data, a.length());
      this_->free_2(oldData, oldLength);
    }
    static void assign_(ZtArray *this_, A &&a) {
      this_->free_();
      if (!a.owned())
	this_->shadow_(reinterpret_cast<T *>(a.m_data), a.length());
      else {
	this_->own_(
	    reinterpret_cast<T *>(a.m_data), a.length(), a.size(), a.vallocd());
	a.owned(false);
      }
    }

    static ZtArray add_(const ZtArray *this_, const A &a) {
      return this_->add(a.m_data, a.length());
    }
    static ZtArray add_(const ZtArray *this_, A &&a) {
      return this_->add_mv(a.m_data, a.length());
    }

    static void splice_(ZtArray *this_,
	ZtArray *removed, int offset, int length, const A &a) {
      if (this_ == &a) {
	ZtArray a_ = a;
	this_->splice_cp_(removed, offset, length, a_.m_data, a_.length());
      } else
	this_->splice_cp_(removed, offset, length, a.m_data, a.length());
    }
    static void splice_(ZtArray *this_,
	ZtArray *removed, int offset, int length, A &&a) {
      this_->splice_mv_(removed, offset, length, a.m_data, a.length());
    }
  };
  template <typename A_> struct Fwd_Array {
    using A = ZuDecay<A_>;
    using Elem = typename ZuTraits<A>::Elem;

    static void ctor_(ZtArray *this_, const A &a_) {
      ZuArray<const Elem> a(a_);
      this_->copy__(a.data(), a.length());
    }
    static void ctor_(ZtArray *this_, A &&a_) {
      ZuArray<Elem> a(a_);
      this_->move__(a.data(), a.length());
    }

    static void assign_(ZtArray *this_, const A &a_) {
      ZuArray<const Elem> a(a_);
      uint32_t oldLength = 0;
      T *oldData = this_->free_1(oldLength);
      this_->copy__(a.data(), a.length());
      this_->free_2(oldData, oldLength);
    }
    static void assign_(ZtArray *this_, A &&a_) {
      ZuArray<Elem> a(a_);
      uint32_t oldLength = 0;
      T *oldData = this_->free_1(oldLength);
      this_->move__(a.data(), a.length());
      this_->free_2(oldData, oldLength);
    }

    static ZtArray add_(const ZtArray *this_, const A &a_) {
      ZuArray<const Elem> a(a_);
      return this_->add(a.data(), a.length());
    }
    static ZtArray add_(const ZtArray *this_, A &&a_) {
      ZuArray<Elem> a(a_);
      return this_->add_mv(a.data(), a.length());
    }

    static void splice_(ZtArray *this_,
	ZtArray *removed, int offset, int length, const A &a_) {
      ZuArray<const Elem> a(a_);
      this_->splice_cp_(removed, offset, length, a.data(), a.length());
    }
    static void splice_(ZtArray *this_,
	ZtArray *removed, int offset, int length, A &&a_) {
      ZuArray<Elem> a(a_);
      this_->splice_mv_(removed, offset, length, a.data(), a.length());
    }
  };

  template <typename A>
  MatchZtArray<A> ctor(A &&a) { Fwd_ZtArray<A>::ctor_(this, ZuFwd<A>(a)); }
  template <typename A>
  MatchSpan<A> ctor(A &&a) { Fwd_Array<A>::ctor_(this, ZuFwd<A>(a)); }
  template <typename A>
  MatchIterable<A> ctor(const A &a) {
    null_();
    auto i = a.begin();
    unsigned n = a.end() - i;
    this->size(n);
    for (unsigned j = 0; j < n; j++) this->initItem(push(), *i++);
  }

  template <typename S>
  MatchStrLiteral<S> ctor(S &&s_) {
    ZuArray<const T> s(s_); shadow_(s.data(), s.length());
  }
  template <typename S>
  MatchString<S> ctor(S &&s_) {
    ZuArray<const T> s(s_); copy__(s.data(), s.length());
  }

  template <typename S> MatchChar2String<S> ctor(S &&s_) {
    ZuArray<const Char2> s(s_);
    unsigned o = ZuUTF<Char, Char2>::len(s);
    if (!o) { null_(); return; }
    alloc_(o, 0);
    length_(ZuUTF<Char, Char2>::cvt(ZuArray<Char>(m_data, o), s));
  }
  template <typename C> MatchChar2<C> ctor(C c) {
    ZuArray<const Char2> s{&c, 1};
    unsigned o = ZuUTF<Char, Char2>::len(s);
    if (!o) { null_(); return; }
    alloc_(o, 0);
    length_(ZuUTF<Char, Char2>::cvt(ZuArray<Char>(m_data, o), s));
  }

  template <typename P> MatchPDelegate<P> ctor(const P &p) {
    null_();
    ZuPrint<P>::print(*this, p);
  }
  template <typename P> MatchPBuffer<P> ctor(const P &p) {
    unsigned o = ZuPrint<P>::length(p);
    if (!o) { null_(); return; }
    alloc_(o, 0);
    length_(ZuPrint<P>::print(reinterpret_cast<char *>(m_data), o, p));
  }

  template <typename V> MatchCtorSize<V> ctor(V size) {
    if (!size) { null_(); return; }
    alloc_(size, 0);
  }

  template <typename R> MatchCtorElem<R> ctor(R &&r) {
    unsigned z = grow_(0, 1);
    m_data = static_cast<T *>(valloc(z * sizeof(T)));
    if (!m_data) throw std::bad_alloc{};
    size_owned(z, 1);
    length_vallocd(1, 1);
    this->initItem(m_data, ZuFwd<R>(r));
  }

public:
  template <typename A> MatchZtArray<A> copy(const A &a) {
    copy__(a.m_data, a.length());
  }
  template <typename A> MatchSpan<A> copy(A &&a_) {
    ZuArray<const typename ZuTraits<A>::Elem> a{a_};
    copy__(a.data(), a.length());
  }
  template <typename A> MatchIterable<A> copy(const A &a) {
    assign(a);
  }

  template <typename S>
  MatchAnyString<S> copy(S &&s) { ctor(ZuFwd<S>(s)); }
  template <typename S>
  MatchChar2String<S> copy(S &&s) { ctor(ZuFwd<S>(s)); }
  template <typename C>
  MatchChar2<C> copy(C c) { ctor(c); }
  template <typename R>
  MatchElem<R> copy(R &&r) { ctor(ZuFwd<R>(r)); }

public:
  ZtArray &operator =(const ZtArray &a) {
    assign(a);
    return *this;
  }
  ZtArray &operator =(ZtArray &&a) noexcept {
    this->~ZtArray();
    new (this) ZtArray(ZuMv(a));
    return *this;
  }

  template <typename A>
  ZtArray &operator =(A &&a) { assign(ZuFwd<A>(a)); return *this; }

  ZtArray &operator =(std::initializer_list<T> a) {
    uint32_t oldLength = 0;
    T *oldData = free_1(oldLength);
    copy__(a.begin(), a.size());
    free_2(oldData, oldLength);
    return *this;
  }

private:
  template <typename A> MatchZtArray<A> assign(A &&a) {
    Fwd_ZtArray<A>::assign_(this, ZuFwd<A>(a));
  }
  template <typename A> MatchSpan<A> assign(A &&a) {
    Fwd_Array<A>::assign_(this, ZuFwd<A>(a));
  }
  template <typename A>
  MatchIterable<A> assign(const A &a) {
    auto i = a.begin();
    unsigned n = a.end() - i;
    this->length(0);
    this->ensure(n);
    for (unsigned j = 0; j < n; j++)
      this->initItem(push(), *i++);
  }

  template <typename S> MatchStrLiteral<S> assign(S &&s_) {
    ZuArray<const T> s(s_);
    free_();
    shadow_(s.data(), s.length());
  }
  template <typename S> MatchString<S> assign(S &&s_) {
    ZuArray<const T> s(s_);
    uint32_t oldLength = 0;
    T *oldData = free_1(oldLength);
    copy__(s.data(), s.length());
    free_2(oldData, oldLength);
  }

  template <typename S> MatchChar2String<S> assign(S &&s_) {
    ZuArray<const Char2> s(s_);
    unsigned o = ZuUTF<Char, Char2>::len(s);
    if (!o) { null(); return; }
    if (!owned() || size() < o) size(o);
    length_(ZuUTF<Char, Char2>::cvt(ZuArray<Char>(m_data, o), s));
  }
  template <typename C> MatchChar2<C> assign(C c) {
    ZuArray<const Char2> s{&c, 1};
    unsigned o = ZuUTF<Char, Char2>::len(s);
    if (!o) { null(); return; }
    if (!owned() || size() < o) size(o);
    length_(ZuUTF<Char, Char2>::cvt(ZuArray<Char>(m_data, o), s));
  }

  template <typename P>
  MatchPDelegate<P> assign(const P &p) {
    ZuPrint<P>::print(*this, p);
  }
  template <typename P>
  MatchPBuffer<P> assign(const P &p) {
    unsigned o = ZuPrint<P>::length(p);
    if (!o) { null(); return; }
    if (!owned() || size() < o) size(o);
    length_(ZuPrint<P>::print(reinterpret_cast<char *>(m_data), o, p));
  }

  template <typename V> MatchReal<V> assign(V v) {
    assign(ZuBoxed(v));
  }
  template <typename V> MatchPtr<V> assign(V v) {
    assign(ZuBoxPtr(v).hex<false, ZuFmt::Alt<>>());
  }

  template <typename V> MatchElem<V> assign(V &&v) {
    free_();
    ctor(ZuFwd<V>(v));
  }

public:
  template <typename A> ZtArray &operator -=(A &&a) {
    shadow(ZuFwd<A>(a));
    return *this;
  }

private:
  template <typename A> MatchZtArray<A> shadow(const A &a) {
    if (this == &a) return;
    free_();
    shadow_(a.m_data, a.length());
  }
  template <typename A>
  MatchSameSpan<A> shadow(A &&a_) {
    ZuArray<const typename ZuTraits<A>::Elem> a{ZuFwd<A>(a_)};
    free_();
    shadow_(a.data(), a.length());
  }

public:
  template <typename S, decltype(ZuMatchString<S>(), int()) = 0>
  ZtArray(S &&s_, ZtIconv *iconv) {
    ZuArray<const typename ZuTraits<S>::Elem> s{s_};
    convert_(s, iconv);
  }
  ZtArray(const Char *data, unsigned length, ZtIconv *iconv) {
    ZuArray<const Char> s(data, length);
    convert_(s, iconv);
  }
  ZtArray(const Char2 *data, unsigned length, ZtIconv *iconv) {
    ZuArray<const Char2> s(data, length);
    convert_(s, iconv);
  }

  ZtArray(unsigned length, unsigned size,
      bool initItems = !ZuTraits<T>::IsPrimitive) {
    if (!size) { null_(); return; }
    alloc_(size, length);
    if (initItems) this->initItems(m_data, length);
  }
  explicit ZtArray(const T *data, unsigned length) {
    if (!length) { null_(); return; }
    copy__(data, length);
  }
  explicit ZtArray(Move, T *data, unsigned length) {
    if (!length) { null_(); return; }
    move__(data, length);
  }
  explicit ZtArray(
      const T *data, unsigned length, unsigned size, bool vallocd) {
    if (!size) { null_(); return; }
    own_(data, length, size, vallocd);
  }

  ~ZtArray() { free_(); }

// re-initializers

  void init() { free_(); init_(); }
  void init_() { null_(); }

  template <typename A> void init(A &&a) { assign(ZuFwd<A>(a)); }
  template <typename A> void init_(A &&a) { ctor(ZuFwd<A>(a)); }

  void init(
      unsigned length, unsigned size,
      bool initItems = !ZuTraits<T>::IsPrimitive) {
    if (this->size() < size || initItems) {
      free_();
      alloc_(size, length);
    } else
      length_(length);
    if (initItems) this->initItems(m_data, length);
  }
  void init_(
      unsigned length, unsigned size,
      bool initItems = !ZuTraits<T>::IsPrimitive) {
    if (!size) { null_(); return; }
    alloc_(size, length);
    if (initItems) this->initItems(m_data, length);
  }
  void copy(const T *data, unsigned length) {
    uint32_t oldLength = 0;
    T *oldData = free_1(oldLength);
    copy__(data, length);
    free_2(oldData, oldLength);
  }
  void move(T *data, unsigned length) {
    uint32_t oldLength = 0;
    T *oldData = free_1(oldLength);
    copy__(data, length);
    free_2(oldData, oldLength);
  }
  void copy_(const T *data, unsigned length) {
    if (!length) { null_(); return; }
    copy__(data, length);
  }
  void move_(T *data, unsigned length) {
    if (!length) { null_(); return; }
    move__(data, length);
  }
  void init(
      const T *data, unsigned length, unsigned size, bool vallocd) {
    free_();
    init_(data, length, size, vallocd);
  }
  void init_(
      const T *data, unsigned length, unsigned size, bool vallocd) {
    if (!size) { null_(); return; }
    own_(data, length, size, vallocd);
  }

// internal initializers / finalizer

private:
  void null_() {
    m_data = 0;
    size_owned(0, 0);
    length_vallocd(0, 0);
  }

  void own_(const T *data, unsigned length, unsigned size, bool vallocd) {
    ZmAssert(size >= length);
    if (!size) {
      if (data && vallocd) vfree(data);
      null_();
      return;
    }
    m_data = const_cast<T *>(data);
    size_owned(size, 1);
    length_vallocd(length, vallocd);
  }

  void shadow_(const T *data, unsigned length) {
    if (!length) { null_(); return; }
    m_data = const_cast<T *>(data);
    size_owned(length, 0);
    length_vallocd(length, 0);
  }

  void alloc_(unsigned size, unsigned length) {
    if (!size) { null_(); return; }
    m_data = static_cast<T *>(valloc(size * sizeof(T)));
    if (!m_data) throw std::bad_alloc{};
    size_owned(size, 1);
    length_vallocd(length, 1);
  }

  template <typename S> void copy__(const S *data, unsigned length) {
    if (!length) { null_(); return; }
    m_data = static_cast<T *>(valloc(length * sizeof(T)));
    if (!m_data) throw std::bad_alloc{};
    if (length) this->copyItems(m_data, data, length);
    size_owned(length, 1);
    length_vallocd(length, 1);
  }

  template <typename S> void move__(S *data, unsigned length) {
    if (!length) { null_(); return; }
    m_data = static_cast<T *>(valloc(length * sizeof(T)));
    if (!m_data) throw std::bad_alloc{};
    if (length) this->moveItems(m_data, data, length);
    size_owned(length, 1);
    length_vallocd(length, 1);
  }

  template <typename S> void convert_(const S &s, ZtIconv *iconv);

  void free_() {
    if (m_data && owned()) {
      this->destroyItems(m_data, length());
      if (vallocd()) vfree(m_data);
    }
  }
  T *free_1(uint32_t &length_vallocd) {
    if (!m_data || !owned()) return 0;
    length_vallocd = m_length_vallocd;
    return m_data;
  }
  void free_2(T *data, uint32_t length_vallocd) {
    if (data) {
      this->destroyItems(data, length_vallocd & ~(1U<<31U));
      if (length_vallocd>>31U) vfree(data);
    }
  }

public:
// truncation (to minimum size)

  void truncate() {
    size(length());
    unsigned n = length();
    if (!m_data || size() <= n) return;
    T *newData = static_cast<T *>(valloc(n * sizeof(T)));
    if (!newData) throw std::bad_alloc{};
    this->moveItems(newData, m_data, n);
    free_();
    m_data = newData;
    vallocd(1);
    size_owned(length(), 1);
  }

// array / ptr operators
  T &operator [](unsigned i) { return m_data[i]; }
  const T &operator [](unsigned i) const { return m_data[i]; }

// accessors
  T *data() { return m_data; }
  const T *data() const { return m_data; }

  unsigned length() const { return m_length_vallocd & ~(1U<<31U); }
  unsigned size() const { return m_size_owned & ~(1U<<31U); }

  bool vallocd() const { return m_length_vallocd>>31U; }
  bool owned() const { return m_size_owned>>31U; }

// iteration - all() is const by default, all<true>() is mutable
  template <bool Mutable = false, typename L>
  ZuIfT<!Mutable> all(L l) const {
    for (unsigned i = 0, n = length(); i < n; i++) l(m_data[i]);
  }
  template <bool Mutable, typename L>
  ZuIfT<Mutable> all(L l) {
    for (unsigned i = 0, n = length(); i < n; i++) l(m_data[i]);
  }

// STL cruft
  using iterator = T *;
  using const_iterator = const T *;
  const T *begin() const { return m_data; }
  const T *end() const { return &m_data[length()]; }
  const T *cbegin() const { return m_data; } // sigh
  const T *cend() const { return &m_data[length()]; }
  T *begin() { return m_data; }
  T *end() { return &m_data[length()]; }

private:
  void length_(unsigned v) {
    m_length_vallocd =
      (m_length_vallocd & (1U<<31U)) | static_cast<uint32_t>(v);
  }
  void vallocd(bool v) {
    m_length_vallocd =
      (m_length_vallocd & ~(1U<<31U)) | ((static_cast<uint32_t>(v))<<31U);
  }
  void length_vallocd(unsigned l, bool m) {
    m_length_vallocd = l | ((static_cast<uint32_t>(m))<<31U);
  }
  void size_(unsigned v) {
    m_size_owned = (m_size_owned & (1U<<31U)) | static_cast<uint32_t>(v);
  }
  void owned(bool v) {
    m_size_owned =
      (m_size_owned & ~(1U<<31U)) | ((static_cast<uint32_t>(v))<<31U);
  }
  void size_owned(unsigned z, bool o) {
    m_size_owned = z | ((static_cast<uint32_t>(o))<<31U);
  }

public:
// release / free
  T *release() && {
    owned(0);
    return m_data;
  }
  static void free(const T *ptr) { vfree(ptr); }

// reset to null array
  void null() {
    free_();
    null_();
  }

// reset without freeing
  void clear() {
    if (!owned()) { null_(); return; }
    if constexpr (!ZuTraits<T>::IsPrimitive)
      if (unsigned n = this->length())
	this->destroyItems(m_data, n);
    length_(0);
  }

// set length
  void length(unsigned length) {
    if (!owned() || length > size()) size(length);
    if constexpr (!ZuTraits<T>::IsPrimitive) {
      unsigned n = this->length();
      if (length > n) {
	this->initItems(m_data + n, length - n);
      } else if (length < n) {
	this->destroyItems(m_data + length, n - length);
      }
    }
    length_(length);
  }
  void length(unsigned length, bool initItems) {
    if (!owned() || length > size()) size(length);
    if (initItems) {
      unsigned n = this->length();
      if (length > n) {
	this->initItems(m_data + n, length - n);
      } else if (length < n) {
	this->destroyItems(m_data + length, n - length);
      }
    }
    length_(length);
  }

// ensure size
  T *ensure(unsigned z) {
    if (ZuLikely(z <= size())) return m_data;
    return size(z);
  }

// set size
  T *size(unsigned z) {
    if (!z) { null(); return 0; }
    if (owned() && z == size()) return m_data;
    T *newData = static_cast<T *>(valloc(z * sizeof(T)));
    if (!newData) throw std::bad_alloc{};
    unsigned n = z;
    if (n > length()) n = length();
    if (m_data) {
      if (n) this->moveItems(newData, m_data, n);
      free_();
    } else
      n = 0;
    m_data = newData;
    size_owned(z, 1);
    length_vallocd(n, 1);
    return newData;
  }

// set element i, extending array as needed
  void *set(unsigned i) {
    unsigned n = length();
    if (ZuLikely(i < n)) {
      this->destroyItem(m_data + i);
      return m_data + i;
    }
    unsigned z = size();
    if (!owned() || i + 1 > z) {
      z = grow_(z, i + 1);
      T *newData = static_cast<T *>(valloc(z * sizeof(T)));
      if (!newData) throw std::bad_alloc{};
      this->moveItems(newData, m_data, n);
      free_();
      m_data = newData;
      size_owned(z, 1);
      if (i > n) this->initItems(m_data + n, i - n);
      length_vallocd(i + 1, 1);
    } else {
      if (i > n) this->initItems(m_data + n, i - n);
      length_(i + 1);
    }
    return m_data + i;
  }
  template <typename V> void set(unsigned i, V &&v) {
    auto ptr = set(i);
    this->initItem(ptr, ZuFwd<V>(v));
  }

  const T *getPtr(unsigned i) const {
    if (ZuUnlikely(i >= length())) return nullptr;
    return static_cast<const T *>(m_data + i);
  }
  const T &get(unsigned i) const {
    if (ZuUnlikely(i >= length())) return ZuNullRef<T, Cmp>();
    return m_data[i];
  }

// hash()
  uint32_t hash() const { return Ops::hash(m_data, length()); }

// buffer access
  auto buf() { return ZuArray{data(), size()}; }
  auto cbuf() const { return ZuArray{data(), length()}; }

// comparison
  bool operator !() const { return !length(); }
  ZuOpBool

  template <typename A>
  MatchZtArray<A, bool> equals(const A &a) const {
    if (this == &a) return true;
    return equals(a.m_data, a.length());
  }
  template <typename A>
  MatchSpan<A, bool> equals(A &&a_) const {
    ZuArray<const typename ZuTraits<A>::Elem> a{ZuFwd<A>(a_)};
    return equals(a.data(), a.length());
  }
  template <typename S>
  MatchAnyString<S, bool> equals(S &&s_) const {
    ZuArray<const typename ZuTraits<S>::Elem> s{ZuFwd<S>(s_)};
    return equals(reinterpret_cast<const T *>(s.data()), s.length());
  }
  template <typename S>
  MatchChar2String<S, bool> equals(const S &s) const {
    return equals(ZtArray(s));
  }

  bool equals(const T *a, unsigned n) const {
    if (!a) return !m_data;
    if (!m_data) return false;
    if (length() != n) return false;
    return Ops::equals(m_data, a, n);
  }

  template <typename A>
  MatchZtArray<A, int> cmp(const A &a) const {
    if (this == &a) return 0;
    return cmp(a.m_data, a.length());
  }
  template <typename A>
  MatchSpan<A, int> cmp(A &&a_) const {
    ZuArray<const typename ZuTraits<A>::Elem> a{ZuFwd<A>(a_)};
    return cmp(a.data(), a.length());
  }
  template <typename S>
  MatchAnyString<S, int> cmp(S &&s_) const {
    ZuArray<const typename ZuTraits<S>::Elem> s{ZuFwd<S>(s_)};
    return cmp(s.data(), s.length());
  }
  template <typename S>
  MatchChar2String<S, int> cmp(const S &s) const {
    return cmp(ZtArray(s));
  }

  int cmp(const T *a, unsigned n) const {
    if (!a) return !!m_data;
    if (!m_data) return -1;
    unsigned l = length();
    if (int i = Ops::cmp(m_data, a, l < n ? l : n)) return i;
    return l - n;
  }

  template <typename L, typename R>
  friend inline ZuIfT<ZuInspect<ZtArray, L>::Is, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline ZuIfT<ZuInspect<ZtArray, L>::Is, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

// +, += operators

  template <typename A>
  ZtArray operator +(const A &a) const { return add(a); }

private:
  template <typename A>
  MatchZtArray<A, ZtArray> add(A &&a) const {
    return Fwd_ZtArray<A>::add_(this, ZuFwd<A>(a));
  }
  template <typename A>
  MatchSpan<A, ZtArray> add(A &&a) const {
    return Fwd_Array<A>::add_(this, ZuFwd<A>(a));
  }

  template <typename S>
  MatchAnyString<S, ZtArray> add(S &&s_) const {
    ZuArray<const typename ZuTraits<S>::Elem> s{ZuFwd<S>(s_)};
    return add(s.data(), s.length());
  }
  template <typename S>
  MatchChar2String<S, ZtArray> add(S &&s) const {
    return add(ZtArray(ZuFwd<S>(s)));
  }
  template <typename C>
  MatchChar2<C, ZtArray> add(C c) const {
    return add(ZtArray(c));
  }

  template <typename P>
  MatchPDelegate<P, ZtArray> add(P &&p) const {
    return add(ZtArray(ZuFwd<P>(p)));
  }
  template <typename P>
  MatchPBuffer<P, ZtArray> add(P &&p) const {
    return add(ZtArray(ZuFwd<P>(p)));
  }

  template <typename R>
  MatchElem<R, ZtArray> add(R &&r) const {
    unsigned n = length();
    unsigned z = grow_(n, n + 1);
    T *newData = static_cast<T *>(valloc(z * sizeof(T)));
    if (!newData) throw std::bad_alloc{};
    if (n) this->copyItems(newData, m_data, n);
    this->initItem(newData + n, ZuFwd<R>(r));
    return ZtArray(newData, n + 1, z);
  }

  ZtArray add(const T *data, unsigned length) const {
    unsigned n = this->length();
    unsigned z = n + length;
    if (ZuUnlikely(!z)) return ZtArray{};
    T *newData = static_cast<T *>(valloc(z * sizeof(T)));
    if (!newData) throw std::bad_alloc{};
    if (n) this->copyItems(newData, m_data, n);
    if (length) this->copyItems(newData + n, data, length);
    return ZtArray(newData, z, z);
  }
  ZtArray add_mv(T *data, unsigned length) const {
    unsigned n = this->length();
    unsigned z = n + length;
    if (ZuUnlikely(!z)) return ZtArray{};
    T *newData = static_cast<T *>(valloc(z * sizeof(T)));
    if (!newData) throw std::bad_alloc{};
    if (n) this->copyItems(newData, m_data, n);
    if (length) this->moveItems(newData + n, data, length);
    return ZtArray(newData, z, z);
  }

public:
  template <typename U>
  ZtArray &operator +=(U &&v) { return *this << ZuFwd<U>(v); }
  template <typename U>
  MatchStreamable<U, ZtArray &>
  operator <<(U &&v) {
    append_(ZuFwd<U>(v));
    return *this;
  }

private:
  template <typename A>
  MatchZtArray<A> append_(A &&a) {
    Fwd_ZtArray<A>::splice_(this, 0, length(), 0, ZuFwd<A>(a));
  }
  template <typename A>
  MatchSpan<A> append_(A &&a) {
    Fwd_Array<A>::splice_(this, 0, length(), 0, ZuFwd<A>(a));
  }
  template <typename A>
  MatchIterable<A> append_(const A &a) {
    auto i = a.begin();
    unsigned n = a.end() - i;
    this->ensure(length() + n);
    for (unsigned j = 0; j < n; j++)
      this->initItem(push(), *i++);
  }

  template <typename S> MatchAnyString<S> append_(S &&s_) {
    ZuArray<const typename ZuTraits<S>::Elem> s{ZuFwd<S>(s_)};
    splice_cp_(0, length(), 0, s.data(), s.length());
  }

  template <typename S>
  MatchChar2String<S> append_(S &&s) { append_(ZtArray(ZuFwd<S>(s))); }
  template <typename C>
  MatchChar2<C> append_(C c) { append_(ZtArray(c)); }

  template <typename P>
  MatchPDelegate<P> append_(const P &p) { ZuPrint<P>::print(*this, p); }
  template <typename P>
  MatchPBuffer<P> append_(const P &p) {
    unsigned n = length();
    unsigned o = ZuPrint<P>::length(p);
    if (!owned() || size() < n + o) size(n + o);
    length(n + ZuPrint<P>::print(reinterpret_cast<char *>(m_data) + n, o, p));
  }

  template <typename V> MatchReal<V> append_(V v) {
    append_(ZuBoxed(v));
  }
  template <typename V> MatchPtr<V> append_(V v) {
    append_(ZuBoxPtr(v).hex<false, ZuFmt::Alt<>>());
  }

  template <typename V> MatchElem<V> append_(V &&v) {
    this->initItem(push(), ZuFwd<V>(v));
  }

public:
  void append(const T *data, unsigned length) {
    if (data) splice_cp_(0, this->length(), 0, data, length);
  }
  void append_mv(T *data, unsigned length) {
    if (data) splice_mv_(0, this->length(), 0, data, length);
  }

// splice()

public:
  template <typename A>
  void splice(
      ZtArray &removed, int offset, int length, A &&replace) {
    splice_(&removed, offset, length, ZuFwd<A>(replace));
  }

  template <typename A>
  void splice(int offset, int length, A &&replace) {
    splice_(nullptr, offset, length, ZuFwd<A>(replace));
  }

  void splice(ZtArray &removed, int offset, int length) {
    splice_del_(&removed, offset, length);
  }

  void splice(int offset, int length) {
    splice_del_(nullptr, offset, length);
  }

private:
  template <typename A>
  MatchZtArray<A> splice_(
      ZtArray *removed, int offset, int length, A &&a) {
    Fwd_ZtArray<A>::splice_(this, removed, offset, length, ZuFwd<A>(a));
  }
  template <typename A>
  MatchSpan<A> splice_(
      ZtArray *removed, int offset, int length, A &&a) {
    Fwd_Array<A>::splice_(this, removed, offset, length, ZuFwd<A>(a));
  }

  template <typename S>
  MatchAnyString<S> splice_(
      ZtArray *removed, int offset, int length, const S &s_) {
    ZuArray<const typename ZuTraits<S>::Elem> s{s_};
    splice_cp_(removed, offset, length, s.data(), s.length());
  }

  template <typename S>
  MatchChar2String<S> splice_(
      ZtArray *removed, int offset, int length, const S &s) {
    splice_(removed, offset, length, ZtArray(s));
  }
  template <typename C>
  MatchChar2<C> splice_(
      ZtArray *removed, int offset, int length, C c) {
    splice_(removed, offset, length, ZtArray(c));
  }

  template <typename R>
  MatchElem<R> splice_(
      ZtArray *removed, int offset, int length, R &&r_) {
    T r{ZuFwd<R>(r_)};
    splice_mv_(removed, offset, length, &r, 1);
  }

public:
  template <typename R>
  void splice(
      ZtArray &removed, int offset, int length,
      const R *replace, unsigned rlength) {
    splice_cp_(&removed, offset, length, replace, rlength);
  }
  template <typename R>
  void splice_mv(
      ZtArray &removed, int offset, int length,
      R *replace, unsigned rlength) {
    splice_mv_(&removed, offset, length, replace, rlength);
  }

  template <typename R>
  void splice(
      int offset, int length, const R *replace, unsigned rlength) {
    splice_cp_(nullptr, offset, length, replace, rlength);
  }
  template <typename R>
  void splice_mv(
      int offset, int length, R *replace, unsigned rlength) {
    splice_mv_(nullptr, offset, length, replace, rlength);
  }

  void *push() {
    unsigned n = length();
    unsigned z = size();
    if (!owned() || n + 1 > z) {
      z = grow_(z, n + 1);
      T *newData = static_cast<T *>(valloc(z * sizeof(T)));
      if (!newData) throw std::bad_alloc{};
      this->moveItems(newData, m_data, n);
      free_();
      m_data = newData;
      size_owned(z, 1);
      length_vallocd(n + 1, 1);
    } else
      length_(n + 1);
    return static_cast<void *>(m_data + n);
  }
  template <typename V> T *push(V &&v) {
    auto ptr = push();
    if (ZuLikely(ptr)) this->initItem(ptr, ZuFwd<V>(v));
    return static_cast<T *>(ptr);
  }
  T pop() {
    unsigned n = length();
    if (!n) return ZuNullRef<T, Cmp>();
    T v;
    if (ZuUnlikely(!owned())) {
      v = m_data[--n];
    } else {
      v = ZuMv(m_data[--n]);
      this->destroyItem(m_data + n);
    }
    length_(n);
    return v;
  }
  T shift() {
    unsigned n = length();
    if (!n) return ZuNullRef<T, Cmp>();
    T v;
    if (ZuUnlikely(!owned())) {
      v = m_data[0];
      ++m_data;
      --n;
    } else {
      v = ZuMv(m_data[0]);
      this->destroyItem(m_data);
      this->moveItems(m_data, m_data + 1, --n);
    }
    length_(n);
    return v;
  }

  template <typename A> MatchZtArray<A> unshift(A &&a)
    { Fwd_ZtArray<A>::splice_(this, nullptr, 0, 0, ZuFwd<A>(a)); }
  template <typename A> MatchSpan<A> unshift(A &&a)
    { Fwd_Array<A>::splice_(this, nullptr, 0, 0, ZuFwd<A>(a)); }

  void *unshift() {
    unsigned n = length();
    unsigned z = size();
    if (!owned() || n + 1 > z) {
      z = grow_(z, n + 1);
      T *newData = static_cast<T *>(valloc(z * sizeof(T)));
      if (!newData) throw std::bad_alloc{};
      this->moveItems(newData + 1, m_data, n);
      free_();
      m_data = newData;
      size_owned(z, 1);
      length_vallocd(n + 1, 1);
    } else {
      this->moveItems(m_data + 1, m_data, n);
      length_(n + 1);
    }
    return static_cast<void *>(m_data);
  }
  template <typename V> void unshift(V &&v) {
    this->initItem(unshift(), ZuFwd<V>(v));
  }

private:
  void splice_del_(
      ZtArray *removed,
      int offset,
      int length) {
    unsigned n = this->length();
    unsigned z = size();
    if (offset < 0) { if ((offset += n) < 0) offset = 0; }
    if (length < 0) { if ((length += (n - offset)) < 0) length = 0; }

    if (offset > static_cast<int>(n)) {
      if (removed) removed->clear();
      if (!owned() || offset > static_cast<int>(z)) {
	z = grow_(z, offset);
	size(z);
      }
      this->initItems(m_data + n, offset - n);
      length_(offset);
      return;
    }

    if (offset + length > static_cast<int>(n)) length = n - offset;

    int l = n - length;

    if (l > 0 && (!owned() || l > static_cast<int>(z))) {
      z = grow_(z, l);
      if (removed) removed->move(m_data + offset, length);
      T *newData = static_cast<T *>(valloc(z * sizeof(T)));
      if (!newData) throw std::bad_alloc{};
      this->moveItems(newData, m_data, offset);
      if (offset + length < static_cast<int>(n))
	this->moveItems(
	    newData + offset,
	    m_data + offset + length,
	    n - (offset + length));
      free_();
      m_data = newData;
      size_owned(z, 1);
      length_vallocd(l, 1);
      return;
    }

    if (removed) removed->move(m_data + offset, length);
    this->destroyItems(m_data + offset, length);
    if (l > 0) {
      if (offset + length < static_cast<int>(n))
	this->moveItems(
	    m_data + offset,
	    m_data + offset + length,
	    n - (offset + length));
    }
    length_(l);
  }

  template <typename R>
  void splice_cp_(
      ZtArray *removed,
      int offset,
      int length,
      const R *replace,
      unsigned rlength) {
    unsigned n = this->length();
    unsigned z = size();
    if (offset < 0) { if ((offset += n) < 0) offset = 0; }
    if (length < 0) { if ((length += (n - offset)) < 0) length = 0; }

    if (offset > static_cast<int>(n)) {
      if (removed) removed->clear();
      if (!owned() ||
	  offset + static_cast<int>(rlength) > static_cast<int>(z)) {
	z = grow_(z, offset + rlength);
	size(z);
      }
      this->initItems(m_data + n, offset - n);
      this->copyItems(m_data + offset, replace, rlength);
      length_(offset + rlength);
      return;
    }

    if (offset + length > static_cast<int>(n)) length = n - offset;

    int l = n + rlength - length;

    if (l > 0 && (!owned() || l > static_cast<int>(z))) {
      z = grow_(z, l);
      if (removed) removed->move(m_data + offset, length);
      T *newData = static_cast<T *>(valloc(z * sizeof(T)));
      if (!newData) throw std::bad_alloc{};
      this->moveItems(newData, m_data, offset);
      this->copyItems(newData + offset, replace, rlength);
      if (offset + length < static_cast<int>(n))
	this->moveItems(
	    newData + offset + rlength,
	    m_data + offset + length,
	    n - (offset + length));
      free_();
      m_data = newData;
      size_owned(z, 1);
      length_vallocd(l, 1);
      return;
    }

    if (removed) removed->move(m_data + offset, length);
    this->destroyItems(m_data + offset, length);
    if (l > 0) {
      if (static_cast<int>(rlength) != length &&
	  offset + length < static_cast<int>(n))
	this->moveItems(
	    m_data + offset + rlength,
	    m_data + offset + length,
	    n - (offset + length));
      this->copyItems(m_data + offset, replace, rlength);
    }
    length_(l);
  }

  template <typename R>
  void splice_mv_(
      ZtArray *removed,
      int offset,
      int length,
      R *replace,
      unsigned rlength) {
    unsigned n = this->length();
    unsigned z = size();
    if (offset < 0) { if ((offset += n) < 0) offset = 0; }
    if (length < 0) { if ((length += (n - offset)) < 0) length = 0; }

    if (offset > static_cast<int>(n)) {
      if (removed) removed->clear();
      if (!owned() ||
	  offset + static_cast<int>(rlength) > static_cast<int>(z)) {
	z = grow_(z, offset + rlength);
	size(z);
      }
      this->initItems(m_data + n, offset - n);
      this->moveItems(m_data + offset, replace, rlength);
      length_(offset + rlength);
      return;
    }

    if (offset + length > static_cast<int>(n)) length = n - offset;

    int l = n + rlength - length;

    if (l > 0 && (!owned() || l > static_cast<int>(z))) {
      z = grow_(z, l);
      if (removed) removed->move(m_data + offset, length);
      T *newData = static_cast<T *>(valloc(z * sizeof(T)));
      if (!newData) throw std::bad_alloc{};
      this->moveItems(newData, m_data, offset);
      this->moveItems(newData + offset, replace, rlength);
      if (static_cast<int>(rlength) != length &&
	  offset + length < static_cast<int>(n))
	this->moveItems(
	    newData + offset + rlength,
	    m_data + offset + length,
	    n - (offset + length));
      free_();
      m_data = newData;
      size_owned(z, 1);
      length_vallocd(l, 1);
      return;
    }

    if (removed) removed->move(m_data + offset, length);
    this->destroyItems(m_data + offset, length);
    if (l > 0) {
      if (static_cast<int>(rlength) != length &&
	  offset + length < static_cast<int>(n))
	this->moveItems(
	    m_data + offset + rlength,
	    m_data + offset + length,
	    n - (offset + length));
      this->moveItems(m_data + offset, replace, rlength);
    }
    length_(l);
  }

// iterate

public:
  template <typename Fn> void iterate(Fn fn) {
    unsigned n = length();
    for (unsigned i = 0; i < n; i++) fn(m_data[i]);
  }

// grep

  // l(item) -> bool // item is spliced out if true
  template <typename L> void grep(L l) {
    for (unsigned i = 0, n = length(); i < n; i++) {
      if (l(m_data[i])) {
	splice_del_(0, i, 1);
	--i, --n;
      }
    }
  }

// growth algorithm

  void grow(unsigned length) {
    unsigned o = size();
    if (ZuUnlikely(length > o)) size(grow_(o, length));
    o = this->length();
    if (ZuUnlikely(length > o)) this->length(length);
  }
  void grow(unsigned length, bool initItems) {
    unsigned o = size();
    if (ZuUnlikely(length > o)) size(grow_(o, length));
    o = this->length();
    if (ZuUnlikely(length > o)) this->length(length, initItems);
  }
private:
  static unsigned grow_(unsigned o, unsigned n) {
    return ZmGrow(o * sizeof(T), n * sizeof(T)) / sizeof(T);
  }

public:
  // traits

  struct Traits : public ZuBaseTraits<ZtArray> {
    using Elem = T;
    enum {
      IsArray = 1, IsSpan = 1, IsPrimitive = 0,
      IsString =
	bool(ZuIsExact<char, ZuDecay<T>>{}) ||
	bool(ZuIsExact<wchar_t, ZuDecay<T>>{}),
      IsWString = bool(ZuIsExact<wchar_t, ZuDecay<T>>{})
    };
    static T *data(ZtArray &a) { return a.data(); }
    static const T *data(const ZtArray &a) { return a.data(); }
    static unsigned length(const ZtArray &a) { return a.length(); }
  };
  friend Traits ZuTraitsType(ZtArray *);

private:
  uint32_t		m_size_owned;	// allocated size and owned flag
  uint32_t		m_length_vallocd;// initialized length and malloc'd flag
  T			*m_data;	// data buffer
};

template <typename T, typename NTP>
template <typename S>
inline void ZtArray<T, NTP>::convert_(const S &s, ZtIconv *iconv) {
  null_();
  iconv->convert(*this, s);
}

using ZtBytes = ZtArray<uint8_t>;

#endif /* ZtArray_HH */
