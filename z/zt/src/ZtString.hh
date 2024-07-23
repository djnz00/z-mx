//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// use ZuStringN<> for fixed-size strings by value without heap overhead
//
// ZtString is a heap-allocated C string class (null-terminated)
//
// * fast, lightweight
// * explicitly contiguous
// * provides direct read/write access to the buffer
// * no heap allocation for small strings below a built-in size
// * supports both zero-copy and deep-copy
// * very thin layer on ANSI C string functions
// * no C library locale or character set overhead (except when requested)
// * no STL cruft

#ifndef ZtString_HH
#define ZtString_HH

#ifndef ZtLib_HH
#include <zlib/ZtLib.hh>
#endif

#include <string.h>
#include <wchar.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>

#include <zlib/ZuInt.hh>
#include <zlib/ZuNull.hh>
#include <zlib/ZuTraits.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuHash.hh>
#include <zlib/ZuStringFn.hh>
#include <zlib/ZuUTF.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuBox.hh>
#include <zlib/ZuEquiv.hh>

#include <zlib/ZuVStream.hh>
#include <zlib/ZmVHeap.hh>

#include <zlib/ZtPlatform.hh>
#include <zlib/ZtArray.hh>
#include <zlib/ZtIconv.hh>

// built-in buffer size (before falling back to malloc())
// Note: must be a multiple of sizeof(uintptr_t)
#define ZtString_Builtin	(3 * sizeof(uintptr_t))

// buffer size increment for vsnprintf()
#define ZtString_vsnprintf_Growth	256
#define ZtString_vsnprintf_MaxSize	(1U<<20) // 1Mb

template <typename Char> inline const Char *ZtString_Null();
template <> inline const char *ZtString_Null() { return ""; }
template <> inline const wchar_t *ZtString_Null() { return Zu::nullWString(); }

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4800 4348)
#endif

template <typename T_> struct ZtString_Char2;
template <> struct ZtString_Char2<char> { using T = wchar_t; };
template <> struct ZtString_Char2<wchar_t> { using T = char; };

template <typename> struct ZtString__ { };
template <> struct ZtString__<char> {
  friend ZuPrintString ZuPrintType(ZtString__ *);
};

inline constexpr const char *ZtString_ID() { return "ZtString"; }

template <typename Char_, auto HeapID_>
class ZtString_ : private ZmVHeap<HeapID_>, public ZtString__<ZuStrip<Char_>> {
public:
  using Char = Char_;
  using Char2 = typename ZtString_Char2<Char>::T;
  enum { IsWString = ZuInspect<Char, wchar_t>::Same };
  enum { BuiltinSize_ = (ZtString_Builtin + sizeof(Char) - 1) / sizeof(Char) };
  enum { BuiltinUPtr =
    (BuiltinSize_ * sizeof(Char) + sizeof(uintptr_t) - 1) / sizeof(uintptr_t) };
  enum { BuiltinSize = (BuiltinUPtr * sizeof(uintptr_t)) / sizeof(Char) };
  static constexpr auto HeapID = HeapID_;

private:
  // from same type ZtString
  template <typename U, typename V = Char> struct IsZtString :
    public ZuBool<ZuInspect<ZtString__<V>, U>::Base> { };
  template <typename U, typename R = void>
  using MatchZtString = ZuIfT<IsZtString<U>{}, R>;

  // from string literal with same char
  template <typename U, typename V = Char>
  struct IsStrLiteral : public
      ZuIsExact<U, const V (&)[sizeof(U) / sizeof(V)]> { };
  template <typename U, typename R = void>
  using MatchStrLiteral = ZuIfT<IsStrLiteral<U>{}, R>;

  // from some other C string with same char (including string literals)
  template <typename U, typename V = Char> struct IsAnyCString : public ZuBool<
      !IsZtString<U>{} &&
      ZuTraits<U>::IsCString &&
      bool{ZuEquiv<typename ZuTraits<U>::Elem, V>{}}> { };
  template <typename U, typename R = void>
  using MatchAnyCString = ZuIfT<IsAnyCString<U>{}, R>;

  // from some other C string with same char (other than a string literal)
  template <typename U, typename V = Char>
  struct IsCString : public
      ZuBool<!IsStrLiteral<U>{} && IsAnyCString<U>{}> { };
  template <typename U, typename R = void>
  using MatchCString = ZuIfT<IsCString<U>{}, R>;

  // from some other non-C string with same char (non-null-terminated)
  template <typename U, typename V = Char>
  struct IsOtherString : public ZuBool<
      !IsZtString<U>{} &&
      ZuTraits<U>::IsString && !ZuTraits<U>::IsCString &&
      bool{ZuEquiv<typename ZuTraits<U>::Elem, V>{}}> { };
  template <typename U, typename R = void>
  using MatchOtherString = ZuIfT<IsOtherString<U>{}, R>;

  // from char2 string (requires conversion)
  template <typename U, typename V = Char2>
  struct IsChar2String : public ZuBool<
      (ZuTraits<U>::IsArray || ZuTraits<U>::IsString) &&
      bool{ZuEquiv<typename ZuTraits<U>::Elem, V>{}}> { };
  template <typename U, typename R = void>
  using MatchChar2String = ZuIfT<IsChar2String<U>{}, R>;

  // from individual char2 (requires conversion, char->wchar_t only)
  template <typename U, typename V = Char2> struct IsChar2 : public ZuBool<
      bool{ZuEquiv<U, V>{}} &&
      !bool{ZuEquiv<U, wchar_t>{}}> { };
  template <typename U, typename R = void>
  using MatchChar2 = ZuIfT<IsChar2<U>{}, R>;

  // from printable type (if this is a char array)
  template <typename U, typename V = Char> struct IsPrint : public ZuBool<
      bool{ZuEquiv<char, V>{}} &&
      ZuPrint<U>::OK && !ZuPrint<U>::String> { };
  template <typename U, typename R = void>
  using MatchPrint = ZuIfT<IsPrint<U>{}, R>;
  template <typename U, typename V = Char> struct IsPDelegate : public ZuBool<
      bool{ZuEquiv<char, V>{}} && ZuPrint<U>::Delegate> { };
  template <typename U, typename R = void>
  using MatchPDelegate = ZuIfT<IsPDelegate<U>{}, R>;
  template <typename U, typename V = Char> struct IsPBuffer :
    public ZuBool<bool{ZuEquiv<char, V>{}} && ZuPrint<U>::Buffer> { };
  template <typename U, typename R = void>
  using MatchPBuffer = ZuIfT<IsPBuffer<U>{}, R>;

  // from any other real and primitive type (integers, floating point, etc.)
  template <typename U, typename V = Char> struct IsReal : public ZuBool<
      bool{ZuEquiv<char, V>{}} && !bool{ZuEquiv<U, V>{}} &&
      ZuTraits<U>::IsReal && ZuTraits<U>::IsPrimitive &&
      !ZuTraits<U>::IsArray> { };
  template <typename U, typename R = void>
  using MatchReal = ZuIfT<IsReal<U>{}, R>;

  // from primitive pointer (not an array, string, or otherwise printable)
  template <typename U, typename V = Char> struct IsPtr : public ZuBool<
      bool{ZuEquiv<char, V>{}} &&
      ZuTraits<U>::IsPointer && ZuTraits<U>::IsPrimitive &&
      !ZuTraits<U>::IsArray && !ZuTraits<U>::IsString> { };
  template <typename U, typename R = void>
  using MatchPtr = ZuIfT<IsPtr<U>{}, R>;

  // from individual char
  template <typename U, typename V = Char> struct IsChar :
    public ZuBool<bool{ZuEquiv<U, V>{}}> { };
  template <typename U, typename R = void>
  using MatchChar = ZuIfT<IsChar<U>{}, R>;

  // limit member operator <<() overload resolution to supported types
  template <typename U> struct IsStreamable : public ZuBool<
      bool{IsZtString<U>{}} ||
      bool{IsAnyCString<U>{}} ||
      bool{IsOtherString<U>{}} ||
      bool{IsChar<U>{}} ||
      bool{IsChar2String<U>{}} ||
      bool{IsChar2<U>{}} ||
      bool{IsPDelegate<U>{}} ||
      bool{IsPBuffer<U>{}} ||
      bool{IsReal<U>{}} ||
      bool{IsPtr<U>{}}> { };
  template <typename U, typename R = void>
  using MatchStreamable = ZuIfT<IsStreamable<U>{}, R>;

  // an integer parameter to the constructor is a buffer size
  // - except for character element types
  template <typename U, typename V = Char>
  struct IsCtorSize : public ZuBool<
    ZuTraits<U>::IsIntegral &&
    (sizeof(U) > 2 || !ZuIsExact<ZuNormChar<V>, ZuNormChar<U>>{})> { };
  template <typename U, typename R = void>
  using MatchCtorSize = ZuIfT<IsCtorSize<U>{}, R>;
  // disambiguate ZuBox<int>, etc.
  template <typename U, typename R = void>
  using MatchCtorPDelegate =
    ZuIfT<bool(IsPDelegate<U>{}) && !IsCtorSize<U>{}, R>;
  template <typename U, typename R = void>
  using MatchCtorPBuffer =
    ZuIfT<bool(IsPBuffer<U>{}) && !IsCtorSize<U>{}, R>;

  // construction from any other real and primitive type
  template <typename U, typename V = Char> struct IsCtorReal :
    public ZuBool<IsReal<U>{} && !IsCtorSize<U>{}> { };
  template <typename U, typename R = void>
  using MatchCtorReal = ZuIfT<IsCtorReal<U>{}, R>;

public:
// constructors, assignment operators and destructor

  ZtString_() { null_(); }
private:
  enum NoInit_ { NoInit };
  ZtString_(NoInit_ _) { }
public:
  ZtString_(const ZtString_ &s) {
    copy_(s.data_(), s.length());
  }
  ZtString_(ZtString_ &&s) noexcept {
    if (ZuUnlikely(s.null__())) { null_(); return; }
    if (ZuLikely(s.builtin())) { copy_(s.data_(), s.length()); return; }
    if (ZuUnlikely(!s.owned())) { shadow_(s.data_(), s.length()); return; }
    own_(s.data_(), s.length(), s.size(), s.vallocd());
    s.owned(s.builtin());
    s.vallocd(0);
  }

  template <typename S> ZtString_(S &&s) { ctor(ZuFwd<S>(s)); }

private:
  template <typename S> MatchZtString<S> ctor(const S &s)
    { copy_(s.data_(), s.length()); }
  template <typename S> MatchStrLiteral<S> ctor(S &&s_)
    { ZuArray<const Char> s(ZuFwd<S>(s_)); shadow_(s.data(), s.length()); }
  template <typename S> MatchCString<S> ctor(S &&s_)
    { ZuArray<const Char> s(ZuFwd<S>(s_)); copy_(s.data(), s.length()); }
  template <typename S> MatchOtherString<S> ctor(S &&s_)
    { ZuArray<const Char> s(ZuFwd<S>(s_)); copy_(s.data(), s.length()); }
  template <typename C> MatchChar<C> ctor(C c)
    { copy_(&c, 1); }

  template <typename S> MatchChar2String<S> ctor(S &&s_) {
    ZuArray<const Char2> s(s_);
    unsigned o = ZuUTF<Char, Char2>::len(s);
    if (!o) { null_(); return; }
    length_(ZuUTF<Char, Char2>::cvt(ZuArray<Char>{alloc_(o + 1, 0), o}, s));
  }

  template <typename C> MatchChar2<C> ctor(C c) {
    ZuArray<const Char2> s(&c, 1);
    unsigned o = ZuUTF<Char, Char2>::len(s);
    if (!o) { null_(); return; }
    length_(ZuUTF<Char, Char2>::cvt(ZuArray<Char>(alloc_(o + 1, 0), o), s));
  }

  template <typename P> MatchCtorPDelegate<P> ctor(const P &p)
    { null_(); ZuPrint<P>::print(*this, p); }
  template <typename P> MatchCtorPBuffer<P> ctor(const P &p) {
    unsigned o = ZuPrint<P>::length(p);
    if (!o) { null_(); return; }
    length_(ZuPrint<P>::print(alloc_(o + 1, 0), o, p));
  }

  template <typename V> MatchCtorSize<V> ctor(V size) {
    if (!size) { null_(); return; }
    alloc_(size, 0)[0] = 0;
  }

  template <typename R> MatchCtorReal<R> ctor(R r) {
    ctor(ZuBoxed(r));
  }

public:
  void copy(const ZtString_ &s) {
    copy_(s.data_(), s.length());
  }
  template <typename S> MatchAnyCString<S> copy(S &&s_) {
    ZuArray<const Char> s(ZuFwd<S>(s_));
    copy_(s.data(), s.length());
  }
  template <typename S> MatchOtherString<S> copy(S &&s_) {
    ZuArray<const Char> s(ZuFwd<S>(s_));
    copy_(s.data(), s.length());
  }
  template <typename C> MatchChar<C> copy(C c) {
    copy_(&c, 1);
  }

  template <typename S> MatchChar2String<S> copy(S &&s_) {
    ZuArray<const Char2> s(ZuFwd<S>(s_));
    unsigned o = ZuUTF<Char, Char2>::len(s);
    if (ZuUnlikely(!o)) { length_(0); return; }
    unsigned z = size();
    Char *data;
    if (!owned() || o >= z)
      data = size(o + 1);
    else
      data = data_();
    length_(ZuUTF<Char, Char2>::cvt(ZuArray<Char>(data, o), s));
  }
	  
  template <typename C> MatchChar2<C> copy(C c) {
    ZuArray<const Char2> s(&c, 1);
    unsigned o = ZuUTF<Char, Char2>::len(s);
    if (ZuUnlikely(!o)) { length_(0); return; }
    unsigned z = size();
    Char *data;
    if (!owned() || o >= z)
      data = size(o + 1);
    else
      data = data_();
    length_(ZuUTF<Char, Char2>::cvt(ZuArray<Char>(data, o), s));
  }

public:
  ZtString_ &operator =(const ZtString_ &s) {
    if (ZuLikely(this != &s)) {
      Char *oldData = free_1();
      copy_(s.data_(), s.length());
      free_2(oldData);
    }
    return *this;
  }
  ZtString_ &operator =(ZtString_ &&s) noexcept {
    if (ZuLikely(this != &s)) {
      this->~ZtString_();
      new (this) ZtString_(ZuMv(s));
    }
    return *this;
  }

  template <typename S>
  ZtString_ &operator =(S &&s) { assign(ZuFwd<S>(s)); return *this; }

private:
  template <typename S> MatchZtString<S> assign(const S &s) { 
    if (this == &s) return;
    Char *oldData = free_1();
    copy_(s.data_(), s.length());
    free_2(oldData);
  }
  template <typename S> MatchStrLiteral<S> assign(S &&s_) { 
    ZuArray<const Char> s(ZuFwd<S>(s_));
    free_();
    shadow_(s.data(), s.length());
  }
  template <typename S> MatchCString<S> assign(S &&s_) { 
    ZuArray<const Char> s(ZuFwd<S>(s_));
    Char *oldData = free_1();
    copy_(s.data(), s.length());
    free_2(oldData);
  }
  template <typename S> MatchOtherString<S> assign(S &&s_) { 
    ZuArray<const Char> s(ZuFwd<S>(s_));
    Char *oldData = free_1();
    copy_(s.data(), s.length());
    free_2(oldData);
  }
  template <typename C> MatchChar<C> assign(C c) {
    Char *oldData = free_1();
    copy_(&c, 1);
    free_2(oldData);
  }

  template <typename S> MatchChar2String<S> assign(S &&s_) {
    ZuArray<const Char2> s(ZuFwd<S>(s_));
    unsigned o = ZuUTF<Char, Char2>::len(s);
    unsigned z = size();
    if (ZuUnlikely(!o)) { length_(0); return; }
    Char *data;
    if (!owned() || o >= z)
      data = size(o + 1);
    else
      data = data_();
    length_(ZuUTF<Char, Char2>::cvt(ZuArray<Char>(data, o), s));
  }
  template <typename C> MatchChar2<C> assign(C c) {
    ZuArray<const Char2> s(&c, 1);
    unsigned o = ZuUTF<Char, Char2>::len(s);
    unsigned z = size();
    if (ZuUnlikely(!o)) { length_(0); return; }
    Char *data;
    if (!owned() || o >= z)
      data = size(o + 1);
    else
      data = data_();
    length_(ZuUTF<Char, Char2>::cvt(ZuArray<Char>(data, o), s));
  }

  template <typename P> MatchPDelegate<P> assign(const P &p)
    { ZuPrint<P>::print(*this, p); }
  template <typename P> MatchPBuffer<P> assign(const P &p) {
    unsigned o = ZuPrint<P>::length(p);
    if (ZuUnlikely(!o)) { length_(0); return; }
    unsigned z = size();
    Char *data;
    if (!owned() || o >= z)
      data = size(o + 1);
    else
      data = data_();
    length_(ZuPrint<P>::print(data, o, p));
  }

  template <typename V> MatchReal<V> assign(V v) {
    assign(ZuBoxed(v));
  }
  template <typename V> MatchPtr<V> assign(V v) {
    assign(ZuBoxPtr(v).hex<false, ZuFmt::Alt<>>());
  }

public:
  template <typename S> ZtString_ &operator -=(const S &s) {
    shadow(s);
    return *this;
  }

private:
  template <typename S> MatchZtString<S> shadow(const S &s) {
    if (this == &s) return;
    free_();
    shadow_(s.data_(), s.length());
  }
  template <typename S>
  MatchAnyCString<S> shadow(S &&s_) {
    ZuArray<const Char> s(ZuFwd<S>(s_));
    free_();
    shadow_(s.data(), s.length());
  }
  template <typename S>
  MatchOtherString<S> shadow(S &&s_) {
    ZuArray<const Char> s(ZuFwd<S>(s_));
    free_();
    shadow_(s.data(), s.length());
  }

public:
  template <typename S, decltype(ZuMatchString<S>(), int()) = 0>
  ZtString_(S &&s_, ZtIconv *iconv) {
    ZuArray<const typename ZuTraits<S>::Elem> s{ZuFwd<S>(s_)};
    convert_(s, iconv);
  }
  ZtString_(const Char *data, unsigned length, ZtIconv *iconv) {
    ZuArray<const Char> s(data, length);
    convert_(s, iconv);
  }
  ZtString_(const Char2 *data, unsigned length, ZtIconv *iconv) {
    ZuArray<const Char2> s(data, length);
    convert_(s, iconv);
  }

public:
  ZtString_(unsigned length, unsigned size) {
    if (!size) { null_(); return; }
    alloc_(size, length)[length] = 0;
  }
  explicit ZtString_(const Char *data, unsigned length) {
    if (!length) { null_(); return; }
    copy_(data, length);
  }
  explicit ZtString_(
      Char *data, unsigned length, unsigned size, bool vallocd) {
    if (!size) { null_(); return; }
    own_(data, length, size, vallocd);
  }

  ~ZtString_() { free_(); }

// re-initializers
  void init() { free_(); init_(); }
  void init_() { null_(); }

  template <typename S> void init(const S &s) { assign(s); }
  template <typename S> void init_(const S &s) { ctor(s); }

  void init(unsigned length, unsigned size) {
    if (!size) { null_(); return; }
    unsigned z = this->size();
    if (z < size) {
      free_();
      alloc_(size, length);
    } else
      length_(length);
  }
  void init_(unsigned length, unsigned size) {
    if (!size) { null_(); return; }
    alloc_(size, length);
  }
  void init(const Char *data, unsigned length) {
    Char *oldData = free_1();
    init_(data, length);
    free_2(oldData);
  }
  void init_(const Char *data, unsigned length) {
    if (!length) { null_(); return; }
    copy_(data, length);
  }
  void init(
      const Char *data, unsigned length, unsigned size, bool vallocd) {
    free_();
    init_(data, length, size, vallocd);
  }
  void init_(
      const Char *data, unsigned length, unsigned size, bool vallocd) {
    if (!size) { null_(); return; }
    own_(data, length, size, vallocd);
  }

// internal initializers / finalizer
private:
  using ZmVHeap<HeapID>::valloc;
  using ZmVHeap<HeapID>::vfree;

public: // useful if the caller is sure that the length is being reduced
  void length_(unsigned n) {
    null__(0);
    length__(n);
    data_()[n] = 0;
  }

private:
  void null_() {
    m_data[0] = 0;
    size_owned_null(BuiltinSize, 1, 1);
    length_vallocd_builtin(0, 0, 1);
  }

  void own_(const Char *data, unsigned length, unsigned size, bool vallocd) {
    if (!size) {
      if (data && vallocd) vfree((void *)data);
      null_();
      return;
    }
    m_data[0] = reinterpret_cast<uintptr_t>(data);
    size_owned_null(size, 1, 0);
    length_vallocd_builtin(length, vallocd, 0);
  }

  void shadow_(const Char *data, unsigned length) {
    if (!length) { null_(); return; }
    m_data[0] = reinterpret_cast<uintptr_t>(data);
    size_owned_null(length + 1, 0, 0);
    length_vallocd_builtin(length, 0, 0);
  }

  Char *alloc_(unsigned size, unsigned length) {
    if (ZuLikely(size <= BuiltinSize)) {
      size_owned_null(size, 1, 0);
      length_vallocd_builtin(length, 0, 1);
      return reinterpret_cast<Char *>(m_data);
    }
    Char *newData = static_cast<Char *>(valloc(size * sizeof(Char)));
    if (!newData) throw std::bad_alloc{};
    m_data[0] = reinterpret_cast<uintptr_t>(newData);
    size_owned_null(size, 1, 0);
    length_vallocd_builtin(length, 1, 0);
    return newData;;
  }

  void copy_(const Char *copyData, unsigned length) {
    if (!length) { null_(); return; }
    if (length < BuiltinSize) {
      memcpy(reinterpret_cast<Char *>(m_data), copyData, length * sizeof(Char));
      (reinterpret_cast<Char *>(m_data))[length] = 0;
      size_owned_null(BuiltinSize, 1, 0);
      length_vallocd_builtin(length, 0, 1);
      return;
    }
    Char *newData = static_cast<Char *>(valloc((length + 1) * sizeof(Char)));
    if (!newData) throw std::bad_alloc{};
    memcpy(newData, copyData, length * sizeof(Char));
    newData[length] = 0;
    m_data[0] = reinterpret_cast<uintptr_t>(newData);
    size_owned_null(length + 1, 1, 0);
    length_vallocd_builtin(length, 1, 0);
  }

  template <typename S> void convert_(const S &s, ZtIconv *iconv);

  void free_() {
    if (vallocd())
      if (Char *data = reinterpret_cast<Char *>(m_data[0]))
	vfree(data);
  }
  Char *free_1() {
    if (!vallocd()) return nullptr;
    return data_();
  }
  void free_2(Char *data) {
    if (data) vfree(data);
  }

public:
// truncation (to minimum size)
  void truncate() { size(length() + 1); }

// array / ptr operators
  Char &operator [](unsigned i) { return data_()[i]; }
  Char operator [](unsigned i) const { return data_()[i]; }

  operator Char *() { return null__() ? nullptr : data_(); }
  operator const Char *() const { return null__() ? nullptr : data_(); }

// accessors
  using iterator = Char *;
  using const_iterator = const Char *;
  const Char *begin() const {
    if (null__()) return nullptr;
    return data_();
  }
  const Char *end() const {
    if (null__()) return nullptr;
    return data_() + length();
  }
  Char *begin() {
    return const_cast<Char *>(static_cast<const ZtString_ &>(*this).begin());
  }
  Char *end() {
    return const_cast<Char *>(static_cast<const ZtString_ &>(*this).end());
  }

  Char *data() {
    if (null__()) return nullptr;
    return data_();
  }
  Char *data_() {
    return builtin() ?
      reinterpret_cast<Char *>(m_data) :
      reinterpret_cast<Char *>(m_data[0]);
  }
  const Char *data() const {
    if (null__()) return nullptr;
    return data_();
  }
  const Char *data_() const {
    return builtin() ?
      reinterpret_cast<const Char *>(m_data) :
      reinterpret_cast<const Char *>(m_data[0]);
  }
  const Char *ndata() const {
    if (null__()) return ZtString_Null<Char>();
    return data_();
  }

  unsigned length() const {
    return m_length_vallocd_builtin & ~(3U<<30);
  }
  unsigned size() const {
    uint32_t u = m_size_owned_null;
    return u & ~(uint32_t)(((int32_t)u)>>31) & ~(3U<<30);
  }
  bool vallocd() const { return (m_length_vallocd_builtin>>30) & 1U; }
  bool builtin() const { return m_length_vallocd_builtin>>31; }
  bool owned() const { return (m_size_owned_null>>30) & 1U; }

private:
  void length__(unsigned v) {
    m_length_vallocd_builtin = (m_length_vallocd_builtin & (3U<<30)) | v;
  }
  void vallocd(bool v) {
    m_length_vallocd_builtin =
      (m_length_vallocd_builtin & ~(1U<<30)) | ((uint32_t(v))<<30);
  }
  void builtin(bool v) {
    m_length_vallocd_builtin =
      (m_length_vallocd_builtin & ~(1U<<31)) | ((uint32_t(v))<<31);
  }
  void length_vallocd_builtin(unsigned l, bool m, bool b) {
    m_length_vallocd_builtin = l | ((uint32_t(m))<<30) | ((uint32_t(b))<<31);
  }
  unsigned size_() const {
    return m_size_owned_null & ~(3U<<30);
  }
  void size_(unsigned v) {
    m_size_owned_null = (m_size_owned_null & (3U<<30)) | v;
  }
  void owned(bool v) {
    m_size_owned_null = (m_size_owned_null & ~(1U<<30)) | ((uint32_t(v))<<30);
  }
  bool null__() const { return m_size_owned_null>>31; }
  void null__(bool v) {
    m_size_owned_null = (m_size_owned_null & ~(1U<<31)) | ((uint32_t(v))<<31);
  }
  void size_owned_null(unsigned z, bool o, bool n) {
    m_size_owned_null = z | ((uint32_t(o))<<30) | ((uint32_t(n))<<31);
  }

public:
// release / free
  Char *release() && {
    if (null__()) return nullptr;
    if (builtin()) {
      Char *newData = static_cast<Char *>(valloc(BuiltinSize * sizeof(Char)));
      if (!newData) throw std::bad_alloc{};
      memcpy(newData, m_data, (length() + 1) * sizeof(Char));
      return newData;
    } else {
      owned(0);
      vallocd(0);
      return reinterpret_cast<Char *>(m_data[0]);
    }
  }
  static void free(const Char *ptr) { vfree(ptr); }

// reset to null string
  void null() {
    free_();
    null_();
  }

// reset without freeing
  void clear() {
    if (!null__()) {
      if (!owned()) { null_(); return; }
      length_(0);
    }
  }

// set length
  void length(unsigned n) {
    if (!owned() || n >= size_()) size(n + 1);
    length_(n);
  }
  void calcLength() {
    if (null__())
      length__(0);
    else {
      auto data = data_();
      data[size_() - 1] = 0;
      length__(Zu::strlen_(data));
    }
  }

// set size
  Char *ensure(unsigned z) {
    if (ZuLikely(owned() && z <= size_())) return data_();
    return size(z);
  }
  Char *size(unsigned z) {
    if (ZuUnlikely(!z)) { null(); return nullptr; }
    if (owned() && z == size_()) return data_();
    Char *oldData = data_();
    Char *newData;
    if (z <= BuiltinSize)
      newData = reinterpret_cast<Char *>(m_data);
    else {
      newData = static_cast<Char *>(valloc(z * sizeof(Char)));
      if (!newData) throw std::bad_alloc{};
    }
    unsigned n = z - 1U;
    if (n > length()) n = length();
    if (oldData != newData) {
      memcpy(newData, oldData, (n + 1) * sizeof(Char));
      if (vallocd()) vfree(oldData);
    }
    if (z <= BuiltinSize) {
      size_owned_null(z, 1, 0);
      length_vallocd_builtin(n, 0, 1);
      return newData;
    }
    m_data[0] = reinterpret_cast<uintptr_t>(newData);
    size_owned_null(z, 1, 0);
    length_vallocd_builtin(n, 1, 0);
    return newData;
  }

// common prefix
  template <typename S>
  MatchZtString<S, ZuArray<const Char>> prefix(const S &s) {
    if (this == &s) return ZuArray<const Char>{data_(), length() + 1};
    return prefix(s.data_(), s.length());
  }
  template <typename S>
  MatchAnyCString<S, ZuArray<const Char>> prefix(S &&s_) {
    ZuArray<const Char> s(ZuFwd<S>(s_));
    return prefix(s.data(), s.length());
  }
  template <typename S>
  MatchOtherString<S, ZuArray<const Char>> prefix(S &&s_) {
    ZuArray<const Char> s(ZuFwd<S>(s_));
    return prefix(s.data(), s.length());
  }

  ZuArray<const Char> prefix(const Char *pfxData, unsigned length) const {
    if (null__()) return ZuArray<const Char>();
    const Char *p1 = data_();
    if (!pfxData) return ZuArray<const Char>(p1, 1);
    const Char *p2 = pfxData;
    unsigned i, n = this->length();
    n = n > length ? length : n;
    for (i = 0; i < n && p1[i] == p2[i]; ++i);
    return ZuArray<const Char>(data_(), i);
  }

public:
// hash()
  uint32_t hash() const { return ZuHash<ZtString_>::hash(*this); }

// buffer access
  auto buf() { return ZuArray{data(), size() - 1}; }
  auto cbuf() const { return ZuArray{data(), length()}; }

// comparison
  bool operator !() const { return !length(); }

  template <typename S>
  bool equals(const S &s) const { return ZuCmp<ZtString_>::equals(*this, s); }
  template <typename S>
  int cmp(const S &s) const { return ZuCmp<ZtString_>::cmp(*this, s); }
  template <typename L, typename R>
  friend inline
  ZuIfT<ZuInspect<ZtString_, L>::Is && ZuTraits<R>::IsString, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline
  ZuIfT<ZuInspect<ZtString_, L>::Is && ZuTraits<R>::IsString, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

  bool equals(const Char *s, unsigned n) const {
    if (null__()) return !s;
    if (!s) return false;
    return !Zu::strcmp_(data_(), s, n);
  }
  int cmp(const Char *s, unsigned n) const {
    if (null__()) return s ? -1 : 0;
    if (!s) return 1;
    return Zu::strcmp_(data_(), s, n);
  }
  int icmp(const Char *s, unsigned n) const {
    if (null__()) return s ? -1 : 0;
    if (!s) return 1;
    return Zu::stricmp_(data_(), s, n);
  }

// +, += operators
  template <typename S>
  ZtString_ operator +(const S &s) const { return add(s); }

private:
  template <typename S>
  MatchZtString<S, ZtString_>
  add(const S &s) const { return add(s.data_(), s.length()); }
  template <typename S>
  MatchAnyCString<S, ZtString_> add(S &&s_) const {
    ZuArray<const Char> s(ZuFwd<S>(s_));
    return add(s.data(), s.length());
  }
  template <typename S>
  MatchOtherString<S, ZtString_> add(S &&s_) const {
    ZuArray<const Char> s(ZuFwd<S>(s_));
    return add(s.data(), s.length());
  }
  template <typename C>
  MatchChar<C, ZtString_> add(C c) const {
    return add(&c, 1);
  }

  template <typename S>
  MatchChar2String<S, ZtString_>
  add(const S &s) const { return add(ZtString_(s)); }
  template <typename C>
  MatchChar2<C, ZtString_>
  add(C c) const { return add(ZtString_(c)); }

  template <typename P>
  MatchPDelegate<P, ZtString_>
  add(const P &p) const { return add(ZtString_(p)); }
  template <typename P>
  MatchPBuffer<P, ZtString_>
  add(const P &p) const { return add(ZtString_(p)); }

  ZtString_ add(
      const Char *data, unsigned length) const {
    unsigned n = this->length();
    unsigned o = n + length;
    if (ZuUnlikely(!o)) return ZtString_{};
    Char *newData = static_cast<Char *>(valloc((o + 1) * sizeof(Char)));
    if (!newData) throw std::bad_alloc{};
    if (n) memcpy(newData, data_(), n * sizeof(Char));
    if (length) memcpy(newData + n, data, length * sizeof(Char));
    newData[o] = 0;
    return ZtString_{newData, o, o + 1, true};
  }

public:
  template <typename U>
  ZtString_ &operator +=(U &&v) { return *this << ZuFwd<U>(v); }
  template <typename U>
  MatchStreamable<U, ZtString_ &>
  operator <<(U &&v) {
    append_(ZuFwd<U>(v));
    return *this;
  }

private:
  template <typename S>
  MatchZtString<S> append_(const S &s) {
    if (this == &s) {
      ZtString_ s_ = s;
      splice__(0, length(), 0, s_.data_(), s_.length());
    } else
      splice__(0, length(), 0, s.data_(), s.length());
  }
  template <typename S>
  MatchAnyCString<S> append_(S &&s_) {
    ZuArray<const Char> s(ZuFwd<S>(s_));
    splice__(0, length(), 0, s.data(), s.length());
  }
  template <typename S>
  MatchOtherString<S> append_(S &&s_) {
    ZuArray<const Char> s(ZuFwd<S>(s_));
    splice__(0, length(), 0, s.data(), s.length());
  }
  template <typename C>
  MatchChar<C> append_(C c) {
    unsigned n = length();
    unsigned z = size_();
    Char *data;
    if (!owned() || n + 2 >= z)
      data = size(grow_(z, n + 2));
    else
      data = data_();
    data[n++] = c;
    length_(n);
  }

  template <typename S>
  MatchChar2String<S> append_(const S &s) { append_(ZtString_(s)); }
  template <typename C>
  MatchChar2<C> append_(C c) { append_(ZtString_(c)); }

  template <typename P>
  MatchPDelegate<P> append_(const P &p) {
    ZuPrint<P>::print(*this, p);
  }
  template <typename P> MatchPBuffer<P> append_(const P &p) {
    unsigned n = length();
    unsigned z = size_();
    unsigned o = ZuPrint<P>::length(p);
    Char *data;
    if (!owned() || z <= n + o)
      data = size(grow_(z, n + o + 1));
    else
      data = data_();
    length_(n + ZuPrint<P>::print(data + n, o, p));
  }

  template <typename V> MatchReal<V> append_(V v) {
    append_(ZuBoxed(v));
  }
  template <typename V> MatchPtr<V> append_(V v) {
    append_(ZuBoxPtr(v).hex<false, ZuFmt::Alt<>>());
  }

public:
  void append(const Char *data, unsigned length) {
    if (data) splice__(0, this->length(), 0, data, length);
  }

// splice()

  void splice(
      ZtString_ &removed, int offset, int length, const ZtString_ &replace) {
    splice_(&removed, offset, length, replace);
  }

  void splice(int offset, int length, const ZtString_ &replace) {
    splice_(0, offset, length, replace);
  }

  void splice(ZtString_ &removed, int offset, int length) {
    splice__(&removed, offset, length, 0, 0);
  }

  void splice(int offset) {
    splice__(0, offset, INT_MAX, 0, 0);
  }

  void splice(int offset, int length) {
    splice__(0, offset, length, 0, 0);
  }

  template <typename S>
  void splice(
      ZtString_ &removed, int offset, int length, const S &replace) {
    splice_(&removed, offset, length, replace);
  }

  template <typename S>
  void splice(int offset, int length, const S &replace) {
    splice_(0, offset, length, replace);
  }

private:
  template <typename S>
  MatchZtString<S> splice_(
      ZtString_ *removed, int offset, int length, const S &s) {
    if (this == &s) {
      ZtString_ s_ = s;
      splice__(removed, offset, length, s_.data_(), s_.length());
    } else
      splice__(removed, offset, length, s.data_(), s.length());
  }
  template <typename S>
  MatchAnyCString<S> splice_(
      ZtString_ *removed, int offset, int length, S &&s_) {
    ZuArray<const Char> s(ZuFwd<S>(s_));
    splice__(removed, offset, length, s.data(), s.length());
  }
  template <typename S>
  MatchOtherString<S> splice_(
      ZtString_ *removed, int offset, int length, S &&s_) {
    ZuArray<const Char> s(ZuFwd<S>(s_));
    splice__(removed, offset, length, s.data(), s.length());
  }
  template <typename C>
  MatchChar<C> splice_(
      ZtString_ *removed, int offset, int length, C c) {
    splice__(removed, offset, length, &c, 1);
  }
  template <typename S>
  MatchChar2String<S> splice_(
      ZtString_ *removed, int offset, int length, const S &s) {
    splice_(removed, offset, length, ZtString_(s));
  }
  template <typename C>
  MatchChar2<C> splice_(
      ZtString_ *removed, int offset, int length, C c) {
    splice_(removed, offset, length, ZtString_(c));
  }

public:
  void splice(
      ZtString_ &removed, int offset, int length,
      const Char *replace, unsigned rlength) {
    splice__(&removed, offset, length, replace, rlength);
  }

  void splice(
      int offset, int length, const Char *replace, unsigned rlength) {
    splice__(0, offset, length, replace, rlength);
  }

  // simple read-only cases

  ZuArray<const Char> splice(int offset) const {
    unsigned n = length();
    if (offset < 0) {
      if ((offset += n) < 0) offset = 0;
    } else {
      if (offset > static_cast<int>(n)) offset = n;
    }
    return ZuArray<const Char>(data_() + offset, n - offset);
  }

  ZuArray<const Char> splice(int offset, int length) const {
    unsigned n = this->length();
    if (offset < 0) {
      if ((offset += n) < 0) offset = 0;
    } else {
      if (offset > static_cast<int>(n)) offset = n;
    }
    if (length < 0) {
      if ((length += n - offset) < 0) length = 0;
    } else {
      if (offset + length > static_cast<int>(n)) length = n - offset;
    }
    return ZuArray<const Char>(data_() + offset, length);
  }

private:
  void splice__(
      ZtString_ *removed,
      int offset,
      int length,
      const Char *replace,
      unsigned rlength) {
    unsigned n = this->length();
    unsigned z = size_();
    if (offset < 0) { if ((offset += n) < 0) offset = 0; }
    if (length < 0) { if ((length += (n - offset)) < 0) length = 0; }

    if (offset > static_cast<int>(n)) {
      if (removed) removed->clear();
      Char *data;
      if (!owned() || offset + rlength >= static_cast<int>(z)) {
	z = grow_(z, offset + rlength + 1);
	data = size(z);
      } else
	data = data_();
      Zu::strpad(data + n, offset - n);
      if (rlength) memcpy(data + offset, replace, rlength * sizeof(Char));
      length_(offset + rlength);
      return;
    }

    if (length == INT_MAX || offset + length > static_cast<int>(n))
      length = n - offset;

    int l = n + rlength - length;

    if (l > 0 && (!owned() || l >= static_cast<int>(z))) {
      z = grow_(z, l + 1);
      Char *oldData = data_();
      if (removed) removed->init(oldData + offset, length);
      Char *newData;
      if (z <= BuiltinSize)
	newData = reinterpret_cast<Char *>(m_data);
      else {
	newData = static_cast<Char *>(valloc(z * sizeof(Char)));
      if (!newData) throw std::bad_alloc{};
      }
      if (oldData != newData && offset)
	memcpy(newData, oldData, offset * sizeof(Char));
      if (rlength)
	memcpy(newData + offset, replace, rlength * sizeof(Char));
      if (offset + length < static_cast<int>(n) &&
	  (oldData != newData || static_cast<int>(rlength) != length))
	memmove(newData + offset + rlength,
		oldData + offset + length,
		(n - (offset + length)) * sizeof(Char));
      if (oldData != newData && vallocd()) vfree(oldData);
      newData[l] = 0;
      if (z <= BuiltinSize) {
	size_owned_null(z, 1, 0);
	length_vallocd_builtin(l, 0, 1);
	return;
      }
      m_data[0] = reinterpret_cast<uintptr_t>(newData);
      size_owned_null(z, 1, 0);
      length_vallocd_builtin(l, 1, 0);
      return;
    }

    Char *data = data_();
    if (removed) removed->init(data + offset, length);
    if (l > 0) {
      if (static_cast<int>(rlength) != length &&
	  offset + length < static_cast<int>(n))
	memmove(data + offset + rlength,
		data + offset + length,
		(n - (offset + length)) * sizeof(Char));
      if (rlength) memcpy(data + offset, replace, rlength * sizeof(Char));
    }
    length_(l);
  }

// chomp(), trim(), strip()

private:
  // match whitespace
  auto matchS() {
    return [](int c) {
      return c == ' ' || c == '\t' || c == '\n' || c == '\r';
    };
  }
public:
  // remove trailing characters
  template <typename Match>
  void chomp(Match match) {
    if (!owned()) truncate();
    int o = length();
    if (!o) return;
    Char *data = data_();
    while (--o >= 0 && match(data[o]));
    length_(o + 1);
  }
  void chomp() { return chomp(matchS()); }

  // remove leading characters
  template <typename Match>
  void trim(Match match) {
    if (!owned()) truncate();
    unsigned n = length();
    unsigned o;
    Char *data = data_();
    for (o = 0; o < n && match(data[o]); o++);
    if (!o) return;
    if (!(n -= o)) { null(); return; }
    memmove(data, data + o, n * sizeof(Char));
    length_(n);
  }
  void trim() { return trim(matchS()); }

  // remove leading & trailing characters
  template <typename Match>
  void strip(Match match) {
    if (!owned()) truncate();
    int o = length();
    if (!o) return;
    Char *data = data_();
    while (--o >= 0 && match(data[o]));
    if (o < 0) { null(); return; }
    length_(o + 1);
    unsigned n = o + 1;
    for (o = 0; o < static_cast<int>(n) && match(data[o]); o++);
    if (!o) { length_(n); return; }
    if (!(n -= o)) { null(); return; }
    memmove(data, data + o, n * sizeof(Char));
    length_(n);
  }
  void strip() { return strip(matchS()); }
 
// sprintf(), vsprintf()

  ZtString_ &sprintf(const Char *format, ...) {
    va_list args;

    va_start(args, format);
    vsnprintf(format, args);
    va_end(args);
    return *this;
  }
  ZtString_ &vsprintf(const Char *format, va_list args) {
    vsnprintf(format, args);
    return *this;
  }

// growth algorithm

  void grow(unsigned length) {
    unsigned o = owned() ? size_() : 0U;
    if (ZuLikely(length + 1 > o)) size(grow_(o, length + 1));
    o = this->length();
    if (ZuUnlikely(length > o)) length_(length);
  }

private:
  static unsigned grow_(unsigned o, unsigned n) {
    if (n <= BuiltinSize) return BuiltinSize;
    return ZmGrow(o * sizeof(Char), n * sizeof(Char)) / sizeof(Char);
  }

  unsigned vsnprintf_grow(unsigned z) {
    z = grow_(z, z + ZtString_vsnprintf_Growth);
    size(z);
    return z;
  }

public:
  void vsnprintf(const Char *format, va_list args) {
    unsigned n = length();
    unsigned z = size_();

    if (!owned() || n + 2 >= z)
      z = vsnprintf_grow(z);

retry:
    Char *data = data_();

    int r = Zu::vsnprintf(data + n, z - n, format, args);

    if (r < 0 || (n += r) == z || n == z - 1) {
      if (z >= ZtString_vsnprintf_MaxSize) goto truncate;
      z = vsnprintf_grow(z);
      n = length();
      goto retry;
    }

    if (n > z) {
      if (z >= ZtString_vsnprintf_MaxSize) goto truncate;
      size(z = grow_(z, n + 2));
      n = length();
      goto retry;
    }

    length_(n);
    return;

truncate:
    length_(z - 1);
  }

public:
  // traits
  struct Traits : public ZuBaseTraits<ZtString_> {
    using Elem = Char;
    enum {
      IsCString = 1, IsString = 1,
      IsWString = bool{ZuEquiv<wchar_t, Char>{}}
    };
    static Char *data(ZtString_ &s) { return s.data(); }
    static const Char *data(const ZtString_ &s) { return s.data(); }
    static unsigned length(const ZtString_ &s) { return s.length(); }
  };
  friend Traits ZuTraitsType(ZtString_ *);

private:
  uint32_t		m_size_owned_null;
  uint32_t		m_length_vallocd_builtin;
  uintptr_t		m_data[BuiltinUPtr];
};

template <typename Char, auto HeapID>
template <typename S>
inline void ZtString_<Char, HeapID>::convert_(const S &s, ZtIconv *iconv)
{
  null_();
  iconv->convert(*this, s);
}

#ifdef _MSC_VER
ZtExplicit template class ZtAPI ZtString_<char, ZtString_ID>;
ZtExplicit template class ZtAPI ZtString_<wchar_t, ZtString_ID>;
#endif

using ZtString = ZtString_<char, ZtString_ID>;
template <auto HeapID> using ZtVString = ZtString_<char, HeapID>;
using ZtWString = ZtString_<wchar_t, ZtString_ID>;
template <auto HeapID> using ZtVWString = ZtString_<wchar_t, HeapID>;

// RVO shortcuts

#ifdef __GNUC__
ZtString ZtSprintf(const char *, ...) __attribute__((format(printf, 1, 2)));
#endif
inline ZtString ZtSprintf(const char *format, ...)
{
  va_list args;

  va_start(args, format);
  ZtString s;
  s.vsprintf(format, args);
  va_end(args);
  return s;
}
inline ZtWString ZtWSprintf(const wchar_t *format, ...)
{
  va_list args;

  va_start(args, format);
  ZtWString s;
  s.vsprintf(format, args);
  va_end(args);
  return s;
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZtString_HH */
