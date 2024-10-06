//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Z http library

#ifndef Zhttp_HH
#define Zhttp_HH

#include <zlib/ZhttpLib.hh>

#include <zlib/Ztls.hh>

namespace Zhttp {

// hard-coded Boyer-Moore to find end of header "\r\n\r\n"
inline int eoh(ZuCSpan data) {
  unsigned n = data.length();

  if (ZuUnlikely(n < 4)) return -1;
  n -= 4;

  int j;
  char c;

  for (unsigned o = 0; o <= n; ) {
    j = 3;
    while (j >= 0 && ((j & 1) ? '\n' : '\r') == (c = data[o + j])) j--;
    if (j < 0) return o + 2;
    j -= (c == '\r' ? 2 : c == '\n' ? 3 : -1);
    o += j < 1 ? 1 : j;
  }
  return -1;
}

// hard-coded Boyer-Moore to find end of line "\r\n[^\r\t ]"
inline int eol(ZuCSpan data) {
  unsigned n = data.length();

  if (ZuUnlikely(n < 2)) return -1;
  n -= 2;

  char c;

  for (unsigned o = 0; o <= n; ) {
    if (ZuLikely(o < n)) {
      c = data[o + 2];
      if (c == '\t' || c == ' ') { o += 3; continue; }
      if (c == '\r') { o += 2; continue; }
    }
    if (data[o + 1] != '\n') { ++o; continue; }
    if (data[o] == '\r') return o;
    o += 2;
  }
  return -1;
}

// hard-coded Boyer-Moore to find end of key ": "
inline int eok(ZuCSpan data) {
  unsigned n = data.length();

  if (ZuUnlikely(n < 2)) return -1;
  n -= 2;

  for (unsigned o = 0; o <= n; ) {
    if (data[o + 1] != ' ') { ++o; continue; }
    if (data[o] == ':') return o;
    o += 2;
  }
  return -1;
}

// normalize key case to be consistent (mutates key in place)
inline void normalize(ZuSpan<char> key) {
  unsigned n = key.length();
  bool upper = true;
  int c; // intentionally int, not char

  for (unsigned o = 0; o < n; o++) {
    c = key[o];
    if (c == '-') { upper = true; continue; }
    if (upper) {
      if (c >= 'a' && c <= 'z') key[o] = c + 'A' - 'a';
      upper = false;
    } else {
      if (c >= 'A' && c <= 'Z') key[o] = c + 'a' - 'A';
    }
  }
}

struct Header {
  ZuCSpan	key;
  ZuCSpan	value;
};

template <unsigned Bits>
using Headers =
  ZmLHashKV<ZuCSpan, ZuCSpan, ZmLHashStatic<Bits, ZmLHashLocal<>>>;

template <unsigned Bits>
struct Message {
  Headers<Bits>	headers;
  ZuCSpan	body;

  // parse headers
  int parse(ZuSpan<char> data, unsigned offset) {
    int o = offset;

    data.offset(offset);
    o = eoh(data);
    if (ZuUnlikely(o < 0)) return -1; // incomplete header
    o += 2;
    offset += o;
    body = {&data[o], data.length() - o};
    data.trunc(o - 2); // exclude body and intervening \r\n
    do {
      o = eok(data);
      if (ZuUnlikely(o < 0)) return -1; // unterminated key
      ZuSpan key{&data[0], unsigned(o)};
      data.offset(o + 2); // skip key and delimiter
      o = eol(data);
      if (ZuUnlikely(o < 0)) return -1; // unterminated value
      ZuSpan value{&data[0], unsigned(o)};
      data.offset(o + 2); // skip value and EOL
      normalize(key);
      headers.add(key, value);
    } while (data);
    return offset;
  }
};

template <unsigned Bits>
struct Request : public Message<Bits> {
  ZuCSpan	method;		// e.g. GET
  ZuCSpan	path;		// path
  ZuCSpan	protocol;	// e.g. HTTP/1.1

  // parse request line
  int parse(ZuSpan<char> data) { // returns offset to body, -1 if incomplete
    unsigned n = data.length();

    if (ZuUnlikely(n < 27)) return -1; // shortest possible request length is 27

    int o = 0; // intentionally int

    for (o = 0; data[o] != ' '; )
      if (ZuUnlikely(++o > 7)) return -1; // unterminated method
    if (!o) return -1;
    method = {&data[0], unsigned(o)};
    unsigned b = ++o;
    while (data[o] != ' ')
      if (ZuUnlikely(++o >= n)) return -1; // unterminated path
    if (ZuUnlikely(b == o)) return -1;
    path = {&data[b], o - b};
    b = ++o;
    o = eol({&data[b], n - b});
    if (ZuUnlikely(o < 0)) return -1; // unterminated protocol
    protocol = {&data[b], unsigned(o)};
    return Message<Bits>::parse(data, b + o + 2);
  }
};

template <unsigned Bits>
struct Response : public Message<Bits> {
  ZuCSpan	protocol;	// e.g. HTTP/1.1
  int		code = -1;	// e.g. 200
  ZuCSpan	reason;		// e.g. OK

  // parse response line
  int parse(ZuSpan<char> data) { // returns offset to body, -1 if incomplete
    unsigned n = data.length();

    if (ZuUnlikely(n < 19)) return -1; // shortest possible response is 19

    int o = 0; // intentionally int

    for (o = 0; data[o] != ' '; )
      if (ZuUnlikely(++o > 8)) return -1; // unterminated protocol
    if (!o) return -1;
    protocol = {&data[0], unsigned(o)};
    unsigned b = ++o;
    int c; // intentionally int
    while ((c = data[o]) != ' ') {
      if (c < '0' || c > '9') return -1; // not a number
      c -= '0';
      code = code < 0 ? c : (code * 10) + c;
      if (ZuUnlikely(++o > b + 3)) return -1; // unterminated code
    }
    if (ZuUnlikely(b == o)) return -1;
    b = ++o;
    o = eol({&data[b], n - b});
    if (ZuUnlikely(o < 0)) return -1; // unterminated reason
    reason = {&data[b], unsigned(o)};
    return Message<Bits>::parse(data, b + o + 2);
  }
};

}

#endif /* Zhttp_HH */
