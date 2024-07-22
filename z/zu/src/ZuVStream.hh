//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// monomorphic meta-stream type
// * encapsulates any arbitrary stream type into a monomorphic type that
//   can be used in compiled library code interfaces

#ifndef ZuVStream_HH
#define ZuVStream_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuTraits.hh>
#include <zlib/ZuInspect.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuString.hh>

class ZuVStreamBuf {
public:
  template <typename T>
  ZuVStreamBuf(const T &v) :
    m_ptr{&v},
    m_lengthFn{[](const void *v) -> unsigned {
	return ZuPrint<T>::length(*static_cast<const T *>(v));
    }},
    m_printFn{[](const void *v, char *buf, unsigned n) -> unsigned {
	return ZuPrint<T>::print(buf, n, *static_cast<const T *>(v));
    }} { }

  ZuVStreamBuf(const ZuVStreamBuf &) = default;
  ZuVStreamBuf &operator =(const ZuVStreamBuf &) = default;
  ZuVStreamBuf(ZuVStreamBuf &&s) = default;
  ZuVStreamBuf &operator =(ZuVStreamBuf &&s) = default;

  unsigned length() const {
    return m_lengthFn(m_ptr);
  }
  unsigned print(char *buf, unsigned n) const {
    return m_printFn(m_ptr, buf, n);
  }

  struct PrintType : public ZuPrintBuffer {
    static unsigned length(const ZuVStreamBuf &b) {
      return b.length();
    }
    static unsigned print(char *buf, unsigned n, const ZuVStreamBuf &b) {
      return b.print(buf, n);
    }
  };
  friend PrintType ZuPrintType(ZuVStreamBuf *);

private:
  typedef unsigned (*LengthFn)(const void *);
  typedef unsigned (*PrintFn)(const void *, char *, unsigned);

  const void	*m_ptr;
  LengthFn	m_lengthFn;
  PrintFn	m_printFn;
};

class ZuVStream {
public:
  template <typename S>
  ZuVStream(S &s) :
    m_ptr{&s},
    m_strFn{[](void *s, const ZuString &v) {
      *static_cast<S *>(s) << v;
    }},
    m_bufFn{[](void *s, const ZuVStreamBuf &v) {
      *static_cast<S *>(s) << v;
    }} { }

  ZuVStream() = delete;

  ZuVStream(const ZuVStream &) = default;
  ZuVStream &operator =(const ZuVStream &) = default;
  ZuVStream(ZuVStream &&s) = default;
  ZuVStream &operator =(ZuVStream &&s) = default;

private:
  template <typename U, typename R = void>
  using MatchChar = ZuIfT<ZuInspect<U, char>::Same, R>;

  template <typename U, typename R = void>
  using MatchReal = ZuIfT<
    ZuTraits<U>::IsPrimitive &&
    ZuTraits<U>::IsReal &&
    !ZuInspect<U, char>::Same, R>;

  template <typename U, typename R = void>
  using MatchString = ZuIfT<
    ZuTraits<U>::IsString &&
    !ZuTraits<U>::IsWString &&
    !ZuInspect<ZuString, U>::Is, R>;

  template <typename U, typename R = void>
  using MatchPDelegate = ZuIfT<ZuPrint<U>::Delegate, R>;

  template <typename U, typename R = void>
  using MatchPBuffer = ZuIfT<ZuPrint<U>::Buffer, R>;

public:
  template <typename C>
  MatchChar<C, ZuVStream &> operator <<(C c) {
    (*m_strFn)(m_ptr, ZuString{&c, 1});
    return *this;
  }
  template <typename R>
  MatchReal<R, ZuVStream &> operator <<(const R &r) {
    (*m_bufFn)(m_ptr, ZuVStreamBuf{ZuBoxed(r)});
    return *this;
  }
  ZuVStream &operator <<(ZuString s) {
    (*m_strFn)(m_ptr, s);
    return *this;
  }
  template <typename S>
  MatchString<S, ZuVStream &> operator <<(S &&s_) {
    (*m_strFn)(m_ptr, ZuString{ZuFwd<S>(s_)});
    return *this;
  }
  template <typename P>
  MatchPDelegate<P, ZuVStream &> operator <<(const P &p) {
    ZuPrint<P>::print(*this, p);
    return *this;
  }
  template <typename P>
  MatchPBuffer<P, ZuVStream &> operator <<(const P &p) {
    (*m_bufFn)(m_ptr, ZuVStreamBuf{p});
    return *this;
  }

private:
  typedef void (*StrFn)(void *, const ZuString &);
  typedef void (*BufFn)(void *, const ZuVStreamBuf &);

  void		*m_ptr;
  StrFn		m_strFn;
  BufFn		m_bufFn;
};

#endif /* ZuVStream_HH */
