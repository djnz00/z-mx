//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// stream to ZuArray<char>, ZuArray<wchar_t>

#ifndef ZuStream_HH
#define ZuStream_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuTraits.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuStringFn.hh>
#include <zlib/ZuString.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuCmp.hh>
#include <zlib/ZuHash.hh>
#include <zlib/ZuBox.hh>
#include <zlib/ZuUTF.hh>
#include <zlib/ZuEquiv.hh>
#include <zlib/ZuArray.hh>

template <typename T_> struct ZuStream_Char2;
template <> struct ZuStream_Char2<char> { using T = wchar_t; };
template <> struct ZuStream_Char2<wchar_t> { using T = char; };

template <typename Char_>
class ZuStream_ : public ZuArray<Char_> {
  ZuAssert((ZuIsExact<char, Char_>{} || ZuIsExact<wchar_t, Char_>{}));

public:
  using Base = ZuArray<Char_>;
  using Base::Base;
  using Base::operator =;

  template <typename ...Args>
  ZuStream_(Args &&...args) : Base{ZuFwd<Args>(args)...} { }

public:
  using Char = Char_;
  using Char2 = typename ZuStream_Char2<Char>::T;

protected:
  // from any string with same char (including string literals)
  template <typename U, typename V = Char> struct IsString :
    public ZuBool<(ZuTraits<U>::IsArray || ZuTraits<U>::IsString) &&
      bool{ZuEquiv<typename ZuTraits<U>::Elem, V>{}}> { };
  template <typename U, typename R = void>
  using MatchString = ZuIfT<IsString<U>{}, R>;

  // from individual char
  template <typename U, typename V = Char> struct IsChar :
    public ZuEquiv<U, V> { };
  template <typename U, typename R = void>
  using MatchChar = ZuIfT<IsChar<U>{}, R>;

  // from char2 string (requires conversion)
  template <typename U, typename V = Char2> struct IsChar2String :
    public ZuBool<(ZuTraits<U>::IsArray || ZuTraits<U>::IsString) &&
      bool{ZuEquiv<typename ZuTraits<U>::Elem, V>{}}> { };
  template <typename U, typename R = void>
  using MatchChar2String = ZuIfT<IsChar2String<U>{}, R>;

  // from individual char2 (requires conversion, char->wchar_t only)
  template <typename U, typename V = Char2> struct IsChar2 :
    public ZuBool<bool{ZuEquiv<U, V>{}} &&
      !bool{ZuEquiv<U, wchar_t>{}}> { };
  template <typename U, typename R = void>
  using MatchChar2 = ZuIfT<IsChar2<U>{}, R>;

  // from printable type (if this is a char array)
  template <typename U, typename V = Char> struct IsPDelegate :
    public ZuBool<bool{ZuEquiv<char, V>{}} && ZuPrint<U>::Delegate> { };
  template <typename U, typename R = void>
  using MatchPDelegate = ZuIfT<IsPDelegate<U>{}, R>;
  template <typename U, typename V = Char> struct IsPBuffer :
    public ZuBool<bool{ZuEquiv<char, V>{}} && ZuPrint<U>::Buffer> { };
  template <typename U, typename R = void>
  using MatchPBuffer = ZuIfT<IsPBuffer<U>{}, R>;

  // from real primitive types other than chars (if this is a char string)
  template <typename U, typename V = Char> struct IsReal :
    public ZuBool<bool{ZuEquiv<char, V>{}} && !bool{ZuEquiv<U, V>{}} &&
      ZuTraits<U>::IsReal && ZuTraits<U>::IsPrimitive &&
      !ZuTraits<U>::IsArray> { };
  template <typename U, typename R = void>
  using MatchReal = ZuIfT<IsReal<U>{}, R>;

  // from primitive pointer (not an array, string, or otherwise printable)
  template <typename U, typename V = Char> struct IsPtr :
    public ZuBool<bool{ZuEquiv<char, V>{}} &&
      ZuTraits<U>::IsPointer && ZuTraits<U>::IsPrimitive &&
      !ZuTraits<U>::IsArray && !ZuTraits<U>::IsString> { };
  template <typename U, typename R = void>
  using MatchPtr = ZuIfT<IsPtr<U>{}, R>;

  // limit member operator <<() overload resolution to supported types
  template <typename U>
  struct IsStreamable : public ZuBool<
      bool{IsString<U>{}} ||
      bool{IsChar<U>{}} ||
      bool{IsChar2String<U>{}} ||
      bool{IsChar2<U>{}} ||
      bool{IsPDelegate<U>{}} ||
      bool{IsPBuffer<U>{}} ||
      bool{IsReal<U>{}} ||
      bool{IsPtr<U>{}}> { };
  template <typename U, typename R = void>
  using MatchStreamable = ZuIfT<IsStreamable<U>{}, R>;

  void append(const Char *s, unsigned length) {
    if (this->length() < length) {
      if (!this->length()) return;
      length = this->length();
    }
    if (s && length) memcpy(this->data(), s, length * sizeof(Char));
    this->offset(length);
  }

  template <typename S> MatchString<S> append_(S &&s_) {
    ZuArray<const Char> s{s_};
    append(s.data(), s.length());
  }

  template <typename C> MatchChar<C> append_(C c) {
    if (!this->length()) return;
    Char *s = this->data();
    *s++ = c;
    this->offset(1);
  }

  template <typename S>
  MatchChar2String<S> append_(S &&s) {
    if (!this->length()) return;
    unsigned n = ZuUTF<Char, Char2>::cvt({this->data(), this->length()}, s);
    this->offset(n);
  }

  template <typename C> MatchChar2<C> append_(C c) {
    if (!this->length()) return;
    unsigned n = ZuUTF<Char, Char2>::cvt(
      {this->data(), this->length()}, {&c, 1});
    this->offset(n);
  }

  template <typename P>
  MatchPDelegate<P> append_(const P &p) {
    ZuPrint<P>::print(*this, p);
  }
  template <typename P>
  MatchPBuffer<P> append_(const P &p) {
    unsigned length = ZuPrint<P>::length(p);
    if (this->length() < length) return;
    unsigned n = ZuPrint<P>::print(this->data(), this->length(), p);
    this->offset(n);
  }

  template <typename V> MatchReal<V> append_(V v) {
    append_(ZuBoxed(v));
  }
  template <typename V> MatchPtr<V> append_(V v) {
    append_(ZuBoxPtr(v).hex<false, ZuFmt::Alt<>>());
  }

public:
  template <typename U>
  MatchStreamable<U, ZuStream_ &>
  operator <<(U &&v) {
    this->append_(ZuFwd<U>(v));
    return *this;
  }
};

using ZuStream = ZuStream_<char>;
using ZuWStream = ZuStream_<wchar_t>;

#endif /* ZuStream_HH */
