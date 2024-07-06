//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// universal string span

#ifndef ZuQuote_HH
#define ZuQuote_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#include <zlib/ZuString.hh>

namespace ZuQuote {

// C string quoting
struct CString {
  const char *v;
  template <typename S>
  friend S &operator <<(S &s, const CString &print) {
    const char *v = print.v;
    s << '"';
    if (v)
      for (unsigned i = 0; v[i]; i++) {
	char c = v[i];
	if (ZuUnlikely(c == '"')) s << '\\';
	s << c;
      }
    return s << '"';
  }
};

// string quoting
struct String {
  ZuString v;
  template <typename S>
  friend S &operator <<(S &s, const String &print) {
    const auto &v = print.v;
    s << '"';
    for (unsigned i = 0, n = v.length(); i < n; i++) {
      char c = v[i];
      if (ZuUnlikely(c == '"')) s << '\\';
      s << c;
    }
    return s << '"';
  }
};

} // ZuQuote

#endif /* ZuQuote_HH */
