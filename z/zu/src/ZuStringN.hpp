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

// fixed-size POD string/buffer for use in POD structs and passing by value
//
// * cached length (and constexpr size)
// * always null-terminated
// * explicitly contiguous
// * provides direct read/write access to the buffer
// * optimized for smaller sizes
// * max 64k (by design)

#ifndef ZuStringN_HPP
#define ZuStringN_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <string.h>
#include <wchar.h>
#include <stdio.h>

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuConversion.hpp>
#include <zlib/ZuStringFn.hpp>
#include <zlib/ZuString.hpp>
#include <zlib/ZuPrint.hpp>
#include <zlib/ZuCmp.hpp>
#include <zlib/ZuHash.hpp>
#include <zlib/ZuBox.hpp>
#include <zlib/ZuUTF.hpp>
#include <zlib/ZuEquivChar.hpp>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4800 4996)
#endif

#pragma pack(push, 1)

template <typename T_> struct ZuStringN_Char2;
template <> struct ZuStringN_Char2<char> { using T = wchar_t; };
template <> struct ZuStringN_Char2<wchar_t> { using T = char; };

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstringop-overflow"
#endif

template <typename Char_, unsigned N, typename StringN>
class ZuStringN_ {
  ZuConversionFriend;

  ZuStringN_(const ZuStringN_ &);
  ZuStringN_ &operator =(const ZuStringN_ &);

  ZuAssert(N >= 1);
  enum { M = N - 1 };

public:
  using Char = Char_;
  using Char2 = typename ZuStringN_Char2<Char>::T;

private:
  // from any string with same char (including string literals)
  template <typename U, typename V = Char> struct IsString {
    enum { OK =
      (ZuTraits<U>::IsArray || ZuTraits<U>::IsString) &&
      ZuEquivChar<typename ZuTraits<U>::Elem, V>::Same };
  };
  template <typename U, typename R = void>
  using MatchString = ZuIfT<IsString<U>::OK, R>;

  // from individual char
  template <typename U, typename V = Char> struct IsChar {
    enum { OK = ZuEquivChar<U, V>::Same };
  };
  template <typename U, typename R = void>
  using MatchChar = ZuIfT<IsChar<U>::OK, R>;

  // from char2 string (requires conversion)
  template <typename U, typename V = Char2> struct IsChar2String {
    enum { OK = (ZuTraits<U>::IsArray || ZuTraits<U>::IsString) &&
      ZuEquivChar<typename ZuTraits<U>::Elem, V>::Same };
  };
  template <typename U, typename R = void>
  using MatchChar2String = ZuIfT<IsChar2String<U>::OK, R>;

  // from individual char2 (requires conversion, char->wchar_t only)
  template <typename U, typename V = Char2> struct IsChar2 {
    enum { OK = ZuEquivChar<U, V>::Same &&
      !ZuEquivChar<U, wchar_t>::Same };
  };
  template <typename U, typename R = void>
  using MatchChar2 = ZuIfT<IsChar2<U>::OK, R>;

  // from printable type (if this is a char array)
  template <typename U, typename V = Char> struct IsPDelegate {
    enum { OK = ZuEquivChar<char, V>::Same && ZuPrint<U>::Delegate };
  };
  template <typename U, typename R = void>
  using MatchPDelegate = ZuIfT<IsPDelegate<U>::OK, R>;
  template <typename U, typename V = Char> struct IsPBuffer {
    enum { OK = ZuEquivChar<char, V>::Same && ZuPrint<U>::Buffer };
  };
  template <typename U, typename R = void>
  using MatchPBuffer = ZuIfT<IsPBuffer<U>::OK, R>;

  // from real primitive types other than chars (if this is a char string)
  template <typename U, typename V = Char> struct IsReal {
    enum { OK = ZuEquivChar<char, V>::Same && !ZuEquivChar<U, V>::Same &&
      ZuTraits<U>::IsReal && ZuTraits<U>::IsPrimitive &&
      !ZuTraits<U>::IsArray };
  };
  template <typename U, typename R = void>
  using MatchReal = ZuIfT<IsReal<U>::OK, R>;

  // from primitive pointer (not an array, string, or otherwise printable)
  template <typename U, typename V = Char> struct IsPtr {
    enum { OK = ZuEquivChar<char, V>::Same &&
      ZuTraits<U>::IsPointer && ZuTraits<U>::IsPrimitive &&
      !ZuTraits<U>::IsArray && !ZuTraits<U>::IsString };
  };
  template <typename U, typename R = void>
  using MatchPtr = ZuIfT<IsPtr<U>::OK, R>;

protected:
  ZuStringN_() : m_length(0) { data()[0] = 0; }

  using Nop_ = enum { Nop };
  ZuStringN_(Nop_ _) { }

  ZuStringN_(unsigned length) : m_length(length) {
    if (m_length >= N) m_length = M;
    data()[m_length] = 0;
  }

  void init(const Char *s) {
    if (!s) { null(); return; }
    unsigned i;
    for (i = 0; i < M; i++) if (!(data()[i] = *s++)) break;
    if (i == M) data()[i] = 0;
    m_length = i;
  }

  void init(const Char *s, unsigned length) {
    if (ZuUnlikely(length >= N)) length = M;
    if (ZuLikely(s && length)) memcpy(data(), s, length * sizeof(Char));
    memset(data() + (m_length = length), 0, (N - length) * sizeof(Char));
  }

  template <typename S> MatchString<S> init(S &&s_) {
    ZuArray<const Char> s(s_);
    init(s.data(), s.length());
  }

  template <typename C> MatchChar<C> init(C c) {
    auto data = this->data();
    data[0] = c;
    data[m_length = 1] = 0;
  }

  template <typename S> MatchChar2String<S> init(S &&s) {
    data()[m_length = ZuUTF<Char, Char2>::cvt({data(), M}, s)] = 0;
  }
  template <typename C> MatchChar2<C> init(C c) {
    data()[m_length = ZuUTF<Char, Char2>::cvt({data(), M}, {&c, 1})] = 0;
  }

  template <typename P> MatchPDelegate<P> init(const P &p) {
    m_length = 0;
    ZuPrint<P>::print(*static_cast<StringN *>(this), p);
  }
  template <typename P> MatchPBuffer<P> init(const P &p) {
    unsigned length = ZuPrint<P>::length(p);
    if (length >= N)
      data()[m_length = 0] = 0;
    else
      data()[m_length = ZuPrint<P>::print(data(), length, p)] = 0;
  }

  template <typename V> MatchReal<V> init(V v) {
    init(ZuBoxed(v));
  }
  template <typename V> MatchPtr<V> init(V v) {
    init(ZuBoxPtr(v).hex<false, ZuFmt::Alt<>>());
  }

  void append(const Char *s, unsigned length) {
    if (m_length + length >= N) {
      if (m_length >= M) return;
      length = M - m_length;
    }
    if (s && length) memcpy(data() + m_length, s, length * sizeof(Char));
    data()[m_length += length] = 0;
  }

  template <typename S> MatchString<S> append_(S &&s_) {
    ZuArray<const Char> s{s_};
    append(s.data(), s.length());
  }

  template <typename C> MatchChar<C> append_(C c) {
    if (m_length >= M) return;
    auto data = this->data();
    data[m_length++] = c;
    data[m_length] = 0;
  }

  template <typename S>
  MatchChar2String<S> append_(S &&s) {
    if (m_length < M)
      data()[m_length = ZuUTF<Char, Char2>::cvt(
	  {data() + m_length, M - m_length}, s)] = 0;
  }

  template <typename C> MatchChar2<C> append_(C c) {
    if (m_length < M)
      data()[m_length += ZuUTF<Char, Char2>::cvt(
	  {data() + m_length, M - m_length}, {&c, 1})] = 0;
  }

  template <typename P>
  MatchPDelegate<P> append_(const P &p) {
    ZuPrint<P>::print(*static_cast<StringN *>(this), p);
  }
  template <typename P>
  MatchPBuffer<P> append_(const P &p) {
    unsigned length = ZuPrint<P>::length(p);
    if (m_length + length >= N) return;
    data()[m_length += ZuPrint<P>::print(data() + m_length, length, p)] = 0;
  }

  template <typename V> MatchReal<V> append_(V v) {
    append_(ZuBoxed(v));
  }
  template <typename V> MatchPtr<V> append_(V v) {
    append_(ZuBoxPtr(v).hex<false, ZuFmt::Alt<>>());
  }

public:
// array/ptr operators

  Char &operator [](unsigned i) { return data()[i]; }
  const Char &operator [](unsigned i) const { return data()[i]; }

  // operator Char *() must return 0 if the string is empty, oherwise
  // these use cases stop working:
  // if (ZuStringN<2> s = "") { } else { puts("ok"); }
  // if (ZuStringN<2> s = 0) { } else { puts("ok"); }
  operator Char *() {
    return !m_length ? static_cast<Char *>(nullptr) : data();
  }
  operator const Char *() const {
    return !m_length ? static_cast<const Char *>(nullptr) : data();
  }

// accessors

  Char *data() { return reinterpret_cast<Char *>(&this[1]); }
  const Char *data() const { return reinterpret_cast<const Char *>(&this[1]); }
  unsigned length() const { return m_length; }
  constexpr static unsigned size() { return N; }

// chomp(), trim(), strip()

private:
  // match whitespace
  auto matchS() {
    return [](int c) {
      return c == ' ' || c == '\t' || c == '\n' || c == '\t';
    };
  }
public:
  // remove trailing characters
  template <typename Match>
  void chomp(Match match) {
    int o = m_length;
    if (!o) return;
    while (--o >= 0 && match(data()[0]));
    data()[m_length = o + 1] = 0;
  }
  void chomp() { return chomp(matchS()); }

  // remove leading characters
  template <typename Match>
  void trim(Match match) {
    int o;
    for (o = 0; o < (int)m_length && match(data()[0]); o++);
    if (!o) return;
    if (!(m_length -= o)) { null(); return; }
    memmove(data(), data() + o, m_length * sizeof(Char));
    data()[m_length] = 0;
  }
  void trim() { return trim(matchS()); }

  // remove leading & trailing characters
  template <typename Match>
  void strip(Match match) {
    int o = m_length;
    if (!o) return;
    while (--o >= 0 && match(data()[o]));
    if (o < 0) { null(); return; }
    m_length = o + 1;
    for (o = 0; o < (int)m_length && match(data()[0]); o++);
    if (!o) { data()[m_length] = 0; return; }
    if (!(m_length -= o)) { null(); return; }
    memmove(data(), data() + o, m_length * sizeof(Char));
    data()[m_length] = 0;
  }
  void strip() { return strip(matchS()); }

// sprintf

  StringN &sprintf(const Char *format, ...) {
    va_list args;

    va_start(args, format);
    vsnprintf(format, args);
    va_end(args);
    return *static_cast<StringN *>(this);
  }
  StringN &vsprintf(const Char *format, va_list args) {
    vsnprintf(format, args);
    return *static_cast<StringN *>(this);
  }

private:
  void vsnprintf(const Char *format, va_list args) {
    if (N <= m_length + 2) return;
    int n = Zu::vsnprintf(
	data() + m_length, N - m_length, format, args);
    if (n < 0) {
      calcLength();
      return;
    }
    n += m_length;
    if (n == (int)N || n == (int)M) {
      data()[m_length = M] = 0;
      return;
    }
    m_length = n;
  }

public:
// reset to null string

  void clear() { null(); }
  void null() { data()[m_length = 0] = 0; }

// set length

  void length(unsigned length) {
    if (length >= N) length = M;
    data()[m_length = length] = 0;
  }

  void calcLength() {
    data()[M] = 0;
    m_length = Zu::strlen_(data());
  }

// buffer access

  auto buf() { return ZuArray{data(), M}; }
  auto cbuf() const { return ZuArray{data(), m_length}; }

// comparison

  bool operator !() const { return !m_length; }

protected:
  bool same(const StringN &s) const {
    return static_cast<const StringN *>(this) == &s;
  }
  template <typename S> bool same(const S &s) const { return false; }

public:
  template <typename S>
  bool equals(const S &s) const {
    return same(s) ||
      ZuCmp<StringN>::equals(*static_cast<const StringN *>(this), s);
  }
  template <typename S>
  int cmp(const S &s) const {
    if (same(s)) return 0;
    return ZuCmp<StringN>::cmp(*static_cast<const StringN *>(this), s);
  }
  template <typename L, typename R>
  friend inline ZuIfT<ZuConversion<StringN, L>::Is, bool>
  operator ==(const L &l, const R &r) { return l.equals(r); }
  template <typename L, typename R>
  friend inline ZuIfT<ZuConversion<StringN, L>::Is, int>
  operator <=>(const L &l, const R &r) { return l.cmp(r); }

  uint32_t hash() const {
    return ZuHash<StringN>::hash(*static_cast<const StringN *>(this));
  }

  // traits

  struct Traits : public ZuBaseTraits<ZuStringN_> {
    using Elem = Char;
    enum {
      IsPOD = 1, IsCString = 1, IsString = 1, IsStream = 1,
      IsWString = ZuConversion<Char, wchar_t>::Same
    };
    template <typename U = ZuStringN_>
    static typename ZuNotConst<U, Char *>::T data(U &s) { return s.data(); }
    static const Elem *data(const ZuStringN_ &s) { return s.data(); }
    static unsigned length(const ZuStringN_ &s) { return s.length(); }
  };
  friend Traits ZuTraitsType(ZuStringN_ *);

protected:
  uint16_t	m_length;
};

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

template <unsigned N_>
class ZuStringN : public ZuStringN_<char, N_, ZuStringN<N_>> {
  ZuAssert(N_ > 1);
  ZuAssert(N_ < (1U<<16) - 1U);

public:
  using Char = char;
  using Char2 = wchar_t;
  using Base = ZuStringN_<char, N_, ZuStringN<N_>>;
  enum { N = N_ };

private:
  // an unsigned|int|size_t parameter to the constructor is a buffer length
  template <typename U> struct IsCtorLength {
    enum { OK =
      ZuConversion<U, unsigned>::Same ||
      ZuConversion<U, int>::Same ||
      ZuConversion<U, size_t>::Same };
  };
  template <typename U, typename R = void>
  using MatchCtorLength = ZuIfT<IsCtorLength<U>::OK, R>;

  // constructor arg
  template <typename U> struct IsCtorArg {
    enum { OK =
      !IsCtorLength<U>::OK &&
      !ZuConversion<Base, U>::Base };
  };
  template <typename U, typename R = void>
  using MatchCtorArg = ZuIfT<IsCtorArg<U>::OK, R>;

public:
  ZuStringN() { }

  ZuStringN(const ZuStringN &s) : Base{Base::Nop} {
    this->init(s.data(), s.length());
  }
  ZuStringN &operator =(const ZuStringN &s) {
    if (ZuLikely(this != &s)) this->init(s.data(), s.length());
    return *this;
  }

  // update()
  ZuStringN &update(ZuString s) {
    if (s.length()) this->init(s.data(), s.length());
    return *this;
  }

  // C string types
  ZuStringN(const char *s) : Base{Base::Nop} {
    this->init(s);
  }
  ZuStringN(const char *s, unsigned length) : Base{Base::Nop} {
    this->init(s, length);
  }
  ZuStringN &operator =(const char *s) {
    this->init(s);
    return *this;
  }
  ZuStringN &operator +=(const char *s_) {
    ZuString s{s_};
    this->append(s.data(), s.length());
    return *this;
  }
  ZuStringN &operator <<(const char *s_) {
    ZuString s{s_};
    this->append(s.data(), s.length());
    return *this;
  }
  ZuStringN &update(const char *s) {
    if (s) this->init(s);
    return *this;
  }

  // miscellaneous types handled by base class
  template <typename S>
  ZuStringN(S &&s, MatchCtorArg<S> *_ = nullptr) : Base{Base::Nop} {
    this->init(ZuFwd<S>(s));
  }
  template <typename S>
  ZuStringN &operator =(S &&s) {
    this->init(ZuFwd<S>(s));
    return *this;
  }
  template <typename S>
  ZuStringN &operator +=(S &&s) {
    this->append_(ZuFwd<S>(s));
    return *this;
  }
  template <typename S>
  ZuStringN &operator <<(S &&s) {
    this->append_(ZuFwd<S>(s));
    return *this;
  }

  // length
  template <typename L>
  ZuStringN(L l, MatchCtorLength<L> *_ = nullptr) : Base{l} { }

  // traits
  friend typename Base::Traits ZuTraitsType(ZuStringN *);

  // printing
  friend ZuPrintString ZuPrintType(ZuStringN *);

private:
  char		m_data[N];
};

template <unsigned N_>
class ZuWStringN : public ZuStringN_<wchar_t, N_, ZuWStringN<N_>> {
  ZuAssert(N_ > 1);
  ZuAssert(N_ < (1U<<15) - 1U);

public:
  using Char = wchar_t;
  using Char2 = char;
  using Base = ZuStringN_<wchar_t, N_, ZuWStringN<N_>>;
  enum { N = N_ };

private:
  // an unsigned|int|size_t parameter to the constructor is a buffer length
  template <typename U> struct IsCtorLength {
    enum { OK =
      ZuConversion<U, unsigned>::Same ||
      ZuConversion<U, int>::Same ||
      ZuConversion<U, size_t>::Same };
  };
  template <typename U, typename R = void>
  using MatchCtorLength = ZuIfT<IsCtorLength<U>::OK, R>;

  // constructor arg
  template <typename U> struct IsCtorArg {
    enum { OK =
      !IsCtorLength<U>::OK &&
      !ZuConversion<Base, U>::Base };
  };
  template <typename U, typename R = void>
  using MatchCtorArg = ZuIfT<IsCtorArg<U>::OK, R>;

public:
  ZuWStringN() { }

  ZuWStringN(const ZuWStringN &s) : Base{Base::Nop} {
    this->init(s.data(), s.length());
  }
  ZuWStringN &operator =(const ZuWStringN &s) {
    if (ZuLikely(this != &s)) init(s.data(), s.length());
    return *this;
  }

  // update()
  template <typename S> ZuWStringN &update(S &&s_) {
    ZuWString s{s_};
    if (s.length()) this->init(s.data(), s.length());
    return *this;
  }

  // C string types
  ZuWStringN(const wchar_t *s) : Base{Base::Nop} {
    this->init(s);
  }
  ZuWStringN(const wchar_t *s, unsigned length) : Base{Base::Nop} {
    this->init(s, length);
  }
  ZuWStringN &operator =(const wchar_t *s) {
    this->init(s);
    return *this;
  }
  ZuWStringN &operator +=(const wchar_t *s_) { // unoptimized
    ZuWString s{s_};
    this->append(s.data(), s.length());
    return *this;
  }
  ZuWStringN &operator <<(const wchar_t *s_) { // unoptimized
    ZuWString s{s_};
    this->append(s.data(), s.length());
    return *this;
  }
  ZuWStringN &update(const wchar_t *s) {
    if (s) this->init(s);
    return *this;
  }

  // miscellaneous types handled by base class
  template <typename S>
  ZuWStringN(S &&s, MatchCtorArg<S> *_ = nullptr) : Base{Base::Nop} {
    this->init(ZuFwd<S>(s));
  }
  template <typename S>
  ZuWStringN &operator =(S &&s) {
    this->init(ZuFwd<S>(s));
    return *this;
  }
  template <typename S>
  ZuWStringN &operator +=(S &&s) {
    this->append_(ZuFwd<S>(s));
    return *this;
  }
  template <typename S>
  ZuWStringN &operator <<(S &&s) {
    this->append_(ZuFwd<S>(s));
    return *this;
  }

  // length
  template <typename L>
  ZuWStringN(L l, MatchCtorLength<L> *_ = nullptr) : Base{l} { }

  // traits
  friend typename Base::Traits ZuTraitsType(ZuWStringN *);

private:
  wchar_t	m_data[N];
};

#pragma pack(pop)

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZuStringN_HPP */
