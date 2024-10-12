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

// hard-coded isspace (ASCII/UTF8)
// - intentionally includes non-linear white space
ZuInline constexpr bool isspace__(char c) {
  return ((c >= '\t' && c <= '\r') || c == ' ');
}

// hard-coded linear white space (ASCII/UTF8)
ZuInline constexpr bool islspace__(char c) {
  return c == '\t' || c == ' ';
}

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

// hard-coded Boyer-Moore to find end of line "\r\n[^\t ]"
inline int eol(ZuCSpan data) {
  unsigned n = data.length();

  if (ZuUnlikely(n < 2)) return -1;
  n -= 2;

  char c;

  for (unsigned o = 0; o <= n; ) {
    if (ZuLikely(o < n)) {
      c = data[o + 2];
      if (c == '\t' || c == ' ') { o += 3; continue; }
    }
    if (data[o + 1] != '\n') { ++o; continue; }
    if (data[o] == '\r') return o;
    o += 2;
  }
  return -1;
}

// find end of key ":"
// - uses memchr to leverage any available performance advantage
inline int eok(ZuCSpan data) {
  auto p = static_cast<const char *>(memchr(&data[0], ':', data.length()));
  if (!p) return -1;
  return p - &data[0];
}

// skip leading linear white space to find beginning of header value
inline int bov(ZuCSpan data) {
  for (unsigned o = 0, n = data.length(); o < n; ++o)
    if (!islspace__(data[o])) return o;
  return -1;
}

// remove trailing linear white space to find end of header value
inline int eov(ZuCSpan data) {
  for (int o = data.length(); --o >= 0; )
    if (!islspace__(data[o])) return o + 1;
  return -1;
}

// split and iterate over HTTP value delimited by \s+,\s+
// - strips leading/trailing white space
// - single-pass, no back-tracking
// - optional alternate delimiter character (';' is also frequently used)
template <char Delim = ',', typename L>
inline void split(ZuCSpan data, L &&l) {
  unsigned count = 0;
  int begin, end;
  unsigned o = 0, n = data.length();
  for (;;) {
    // skip leading linear white space
    for (; o < n; ++o) if (!islspace__(data[o])) break;
    begin = o; end = -1;
    // find delimiter or end of string, remembering last non-white-space
    for (; o < n; ++o) {
      auto c = data[o];
      if (c == Delim) break;
      if (end < 0 ) {
	if (islspace__(c)) end = o;
      } else {
	if (!islspace__(c)) end = -1;
      }
    }
    if (end < 0) end = o;
    if (ZuLikely(end > begin || count || o < n))
      l(count++, ZuCSpan{&data[begin], unsigned(end - begin)});
    if (o >= n) break;
    // skip trailing linear white space
    while (++o < n) if (!islspace__(data[o])) break;
  }
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

// Bits is the power of 2 of the linear hash table size used to index
// headers - e.g. 7 for 128, which is a safe limit since most sites
// generate fewer than 32 headers; we keep this static since resizing
// is expensive

template <unsigned Bits>
using IHeaders =
  ZmLHashKV<ZuCSpan, ZuCSpan, ZmLHashStatic<Bits, ZmLHashLocal<>>>;

namespace TransferEncoding {
  ZtEnumValues(TransferEncoding, int8_t, compress, deflate, gzip);
}

// HTTP body encoding
struct BodyEncoding {
  int			contentLength = -1;
  TransferEncoding::T	transferEncoding = -1;
  bool			chunked = false;
  bool			valid = true;
};

template <unsigned Bits>
struct Message {
  IHeaders<Bits>	headers;
  ZuCSpan		body;

  // parse headers
  int parse(ZuSpan<char> data, unsigned offset) {
    data.offset(offset);
    for (;;) {
      if (data.length() < 2) return 0;
      if (data[0] == '\r' && data[1] == '\n') break;
      int o = eok(data);
      if (ZuUnlikely(o < 0)) return eol(data) < 0 ? 0 : -1; // unterminated key
      ZuSpan key{&data[0], unsigned(o)};
      offset += o + 1;
      data.offset(o + 1); // skip key and delimiter
      o = bov(data);
      if (ZuUnlikely(o < 0)) return 0; // unterminated value
      offset += o;
      data.offset(o); // skip white space
      o = eol(data);
      if (ZuUnlikely(o < 0)) return 0; // unterminated value
      ZuSpan value{&data[0], unsigned(o)};
      offset += o + 2;
      data.offset(o + 2); // skip value and EOL
      o = eov(value);
      if (ZuUnlikely(o < 0)) return -1; // should never happen
      value.trunc(o);
      normalize(key);
      headers.add(key, value);
    }
    offset += 2;
    data.offset(2);
    body = data;

    return offset;
  }

  // if a body is expected, bodyEncoding() will parse and validate
  // the Transfer-Encoding and Content-Length headers
  BodyEncoding bodyEncoding() {
    BodyEncoding enc;

    if (auto s = headers.findVal("Transfer-Encoding"))
      split(s, [&enc](unsigned i, ZuCSpan token) {
	// chunked must come last, anything else must be first
	if (enc.chunked)
	  enc.valid = false;
	else if (token == "chunked")
	  enc.chunked = true;
	else if (i)
	  enc.valid = false;
	else
	  enc.transferEncoding = TransferEncoding::lookup(token);
      });
    if (enc.valid && !enc.chunked)
      if (auto s = headers.findVal("Content-Length"))
	enc.contentLength = ZuBox<unsigned>{s};
    return enc;
  }
};

// parse() returns:
// +ve - offset to body
// 0   - incomplete
// -1  - invalid / corrupt
 
template <unsigned Bits>
struct Request : public Message<Bits> {
  ZuCSpan	method;		// e.g. GET
  ZuCSpan	path;		// path
  ZuCSpan	protocol;	// e.g. HTTP/1.1

  // parse request
  int parse(ZuSpan<char> data) {
    unsigned n = data.length();

    if (ZuUnlikely(n < 27)) return 0; // shortest possible request length is 27

    int o = 0; // intentionally int

    for (o = 0; data[o] != ' '; )
      if (ZuUnlikely(++o > 7)) return 0; // unterminated method
    if (!o) return 0; // missing method
    method = {&data[0], unsigned(o)};
    unsigned b = ++o;
    while (data[o] != ' ')
      if (ZuUnlikely(++o >= n)) return 0; // unterminated path
    if (ZuUnlikely(b == o)) return -1; // missing path
    path = {&data[b], o - b};
    b = ++o;
    o = eol({&data[b], n - b});
    if (ZuUnlikely(o < 0)) return 0; // unterminated protocol
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

    if (ZuUnlikely(n < 19)) return 0; // shortest possible response is 19

    int o = 0; // intentionally int

    for (o = 0; data[o] != ' '; )
      if (ZuUnlikely(++o > 8)) return 0; // unterminated protocol
    if (!o) return 0; // missing protocol
    protocol = {&data[0], unsigned(o)};
    unsigned b = ++o;
    int c; // intentionally int
    while ((c = data[o]) != ' ') {
      if (c < '0' || c > '9') return -1; // not a number
      c -= '0';
      code = code < 0 ? c : (code * 10) + c;
      if (ZuUnlikely(++o > b + 3)) return 0; // unterminated code
    }
    if (ZuUnlikely(b == o)) return -1; // missing code
    b = ++o;
    o = eol({&data[b], n - b});
    if (ZuUnlikely(o < 0)) return 0; // unterminated reason
    reason = {&data[b], unsigned(o)};
    return Message<Bits>::parse(data, b + o + 2);
  }
};

// parse chunked length
// - returns {length, offset} where offset is offset to chunk data
// - returns {-1, -1} if invalid/corrupt
ZuInline constexpr uint8_t hex(uint8_t c) {
  c |= 0x20;
  return 
    (c >= 'a' && c <= 'f') ? (c - 'a') + 10 :
    (c >= '0' && c <= '9') ? c - '0' : 0xff;
}
inline ZuTuple<int, int> chunkLength(ZuCSpan data)
{
  unsigned o = 0, n = data.length();
  if (n > 10) n = 10;
  int len = 0;
  while (o < n) {
    auto c = data[o];
    if (c == '\r') {
      if (++o < n && data[o] == '\n' && len > 0) return {len, o + 1};
      return {-1, -1};
    }
    uint8_t i = hex(c);
    if (i == 0xff) return {-1, -1};
    len = (len<<4) | i;
    o++;
  }
  return {-1, -1};
}

namespace Method {
  ZtEnumValues(Method, int8_t,
    GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS, CONNECT, TRACE);
}

// output headers
using OHeaders = ZuSpan<ZuTuple<ZuCSpan, ZuCSpan>>; // key, value

// output request
// - body can be empty if it will be sent separately, in chunks, etc.
template <typename S>
void request(S &s, int method, ZuCSpan path, OHeaders headers, ZuCSpan body)
{
  s << Method::name(method) << ' ' << path << " HTTP/1.1\r\n";
  headers.all([&s](auto &header) {
    s << header.template p<0>() << ": " << header.template p<1>() << "\r\n";
  });
  s << "\r\n" << body;
}

// output response
// - body can be empty if it will be sent separately, in chunks, etc.
template <typename S>
void response(
  S &s, unsigned code, ZuCSpan reason, OHeaders headers, ZuCSpan body)
{
  s << "HTTP/1.1 " << ZuBox<unsigned>{code}.fmt<ZuFmt::Right<3>>() << ' '
    << reason << "\r\n";
  headers.all([&s](auto &header) {
    s << header.template p<0>() << ": " << header.template p<1>() << "\r\n";
  });
  s << "\r\n" << body;
}

} // Zhttp

#endif /* Zhttp_HH */
