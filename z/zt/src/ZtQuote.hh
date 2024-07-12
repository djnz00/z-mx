//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// string quoting and binary data base64 printing

#ifndef ZtQuote_HH
#define ZtQuote_HH

#ifndef ZtLib_HH
#include <zlib/ZtLib.hh>
#endif

#include <zlib/ZuString.hh>
#include <zlib/ZuBytes.hh>
#include <zlib/ZuBase64.hh>

#include <zlib/ZmAlloc.hh>

namespace ZtQuote {

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

// printing ZuBytes in base64
struct Base64 {
  ZuBytes v;
  template <typename S>
  friend S &operator <<(S &s, const Base64 &print) {
    const auto &v = print.v;
    auto n = ZuBase64::enclen(v.length());
    auto buf_ = ZmAlloc(uint8_t, n);
    ZuArray<uint8_t> buf(&buf_[0], n);
    buf.trunc(ZuBase64::encode(buf, v));
    return s << ZuString{buf};
  }
};

} // ZtQuote

#endif /* ZtQuote_HH */
