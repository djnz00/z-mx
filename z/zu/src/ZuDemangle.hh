//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// C++ demangling (standard library version)
// - ZmDemangle does it better using binutils liberty

#ifndef ZuDemangle_HH
#define ZuDemangle_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <typeinfo>
#include <cxxabi.h>

#include <zlib/ZuString.hh>

class ZuDemangle_ {
  ZuDemangle_(const ZuDemangle_ &) = delete;
  ZuDemangle_ &operator =(const ZuDemangle_ &) = delete;
  ZuDemangle_(ZuDemangle_ &&) = delete;
  ZuDemangle_ &operator =(ZuDemangle_ &&) = delete;

public:
  ZuDemangle_() { }

  ZuDemangle_(const char *symbol) {
    demangle(symbol);
  }
  ~ZuDemangle_() {
    ::free(m_buf);
  }

  ZuDemangle_ &operator =(const char *symbol) {
    demangle(symbol);
    return *this;
  }
  
  void demangle(const char *symbol) {
    int status;
    m_buf = __cxxabiv1::__cxa_demangle(symbol, m_buf, &m_length, &status);
    if (status || !m_buf)
      m_output = symbol;
    else
      m_output = m_buf;
  }

  template <typename S>
  void print(S &s) const { s << m_output; }

  template <typename S>
  friend S &operator <<(S &s, const ZuDemangle_ &d) { d.print(s); return s; }

private:
  char		*m_buf = nullptr;
  size_t	m_length = 0;
  ZuString	m_output;
};

template <typename T>
struct ZuDemangle {
  template <typename S>
  static void print(S &s) { s << ZuDemangle_{typeid(T).name()}; }
};
template <typename T>
struct ZuDemangle<const T> {
  template <typename S>
  static void print(S &s) { s << "const "; ZuDemangle<T>::print(s); }
};
template <typename T>
struct ZuDemangle<volatile T> {
  template <typename S>
  static void print(S &s) { s << "volatile "; ZuDemangle<T>::print(s); }
};
template <typename T>
struct ZuDemangle<const volatile T> {
  template <typename S>
  static void print(S &s) { s << "const volatile "; ZuDemangle<T>::print(s); }
};
template <typename T>
struct ZuDemangle<T &> {
  template <typename S>
  static void print(S &s) { ZuDemangle<T>::print(s); s << " &"; }
};
template <typename T>
struct ZuDemangle<const T &> {
  template <typename S>
  static void print(S &s) {
    s << "const ";
    ZuDemangle<T>::print(s);
    s << " &";
  }
};
template <typename T>
struct ZuDemangle<T &&> {
  template <typename S>
  static void print(S &s) { ZuDemangle<T>::print(s); s << " &&"; }
};

template <typename S, typename T>
inline S &operator <<(S &s, const ZuDemangle<T> &d) { d.print(s); return s; }

#endif /* ZuDemangle_HH */
