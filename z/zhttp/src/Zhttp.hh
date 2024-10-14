//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Z http library
// - HTTP 1.1
// - supports:
//   - chunked body
//   - chunked trailers
// - caller is responsible for decompression (if required)

#ifndef Zhttp_HH
#define Zhttp_HH

#include <zlib/ZhttpLib.hh>

#include <zlib/Ztls.hh>

namespace Zhttp {

// hard-coded linear white space (ASCII/UTF8)
ZuInline constexpr bool islws(char c) {
  return c == '\t' || c == ' ';
}

// hard-coded Boyer-Moore to find end of header "\r\n\r\n"
ZuInline int eoh(ZuCSpan data) {
  unsigned n = data.length();

  if (ZuUnlikely(n < 4)) return -1;
  n -= 4;

  int j;
  char c;

  for (unsigned o = 0; o <= n; ) {
    j = 3;
    while (j >= 0 && ((j & 1) ? '\n' : '\r') == (c = data[o + j])) j--;
    if (j < 0) return o + 4;
    j -= (c == '\r' ? 2 : c == '\n' ? 3 : -1);
    o += j < 1 ? 1 : j;
  }
  return -1;
}

// hard-coded Boyer-Moore to find end of line "\r\n[^\t ]" or "\r\n"
template <bool CanFold = true> // set to false to just match "\r\n"
ZuInline int eol(ZuCSpan data) {
  unsigned n = data.length();

  if (ZuUnlikely(n < 2)) return -1;
  n -= 2;

  char c;

  for (unsigned o = 0; o <= n; ) {
    if (ZuLikely(o < n)) {
      c = data[o + 2];
      if constexpr (CanFold)
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
ZuInline int eok(ZuCSpan data) {
  auto p = static_cast<const char *>(memchr(&data[0], ':', data.length()));
  if (!p) return -1;
  return p - &data[0];
}

// skip leading linear white space to find beginning of header value
ZuInline int bov(ZuCSpan data) {
  for (unsigned o = 0, n = data.length(); o < n; ++o)
    if (!islws(data[o])) return o;
  return -1;
}

// remove trailing linear white space to find end of header value
ZuInline int eov(ZuCSpan data) {
  for (int o = data.length(); --o >= 0; )
    if (!islws(data[o])) return o + 1;
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
    for (; o < n; ++o) if (!islws(data[o])) break;
    begin = o; end = -1;
    // find delimiter or end of string, remembering last non-white-space
    for (; o < n; ++o) {
      auto c = data[o];
      if (c == Delim) break;
      if (end < 0 ) {
	if (islws(c)) end = o;
      } else {
	if (!islws(c)) end = -1;
      }
    }
    if (end < 0) end = o;
    if (ZuLikely(end > begin || count || o < n))
      l(count++, ZuCSpan{&data[begin], unsigned(end - begin)});
    if (o >= n) break;
    // skip trailing linear white space
    while (++o < n) if (!islws(data[o])) break;
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

// handles everything after the start line, or a chunked encoding trailer
template <unsigned Bits>
struct Header {
  IHeaders<Bits>	headers;
  unsigned		offset = 0;
  bool			complete = false;

  // following a previous parse() the buffer's memory address
  // may have moved due to growth reallocation; if necessary
  // rebase all previously parsed headers
  void rebase(ptrdiff_t o) {
    if (!o || !offset) return;	// not moved or nothing parsed yet
    auto i = headers.iterator();
    while (auto node = i.iterate()) {
      node->template p<0>().rebase(o);
      node->template p<1>().rebase(o);
    }
  }

  // parse headers
  int parse(ZuSpan<char> data) {
    if (complete) return offset;
    unsigned o = offset;
    data.offset(o);
    for (;;) {
      if (data.length() < 2) return 0;
      if (data[0] == '\r' && data[1] == '\n') break;
      int n = eok(data);
      if (ZuUnlikely(n < 0)) return eol(data) < 0 ? 0 : -1; // unterminated key
      ZuSpan key{&data[0], unsigned(n)};
      o += n + 1;
      data.offset(n + 1); // skip key and delimiter
      n = bov(data);
      if (ZuUnlikely(n < 0)) return 0; // unterminated value
      o += n;
      data.offset(n); // skip white space
      n = eol(data);
      if (ZuUnlikely(n < 0)) return 0; // unterminated value
      ZuSpan value{&data[0], unsigned(n)};
      o += n + 2;
      data.offset(n + 2); // skip value and EOL
      n = eov(value);
      if (ZuUnlikely(n < 0)) return -1; // should never happen
      value.trunc(n);
      normalize(key);
      headers.add(key, value);
      offset = o;
    }
    o += 2;
    data.offset(2);
    offset = o;
    complete = true;

    return o;
  }
};

// parse() returns:
// +ve - offset to body
// 0   - incomplete
// -1  - invalid / corrupt
 
template <unsigned Bits = 7>
struct Request : public Header<Bits> {
  ZuCSpan	method;		// e.g. GET
  ZuCSpan	path;		// path
  ZuCSpan	protocol;	// e.g. HTTP/1.1

  using Header<Bits>::offset;

  // rebase spans
  void rebase(ptrdiff_t o) {
    if (!o || !offset) return;
    method.rebase(o);
    path.rebase(o);
    protocol.rebase(o);
    Header<Bits>::rebase(o);
  }

  // parse request
  int parse(ZuSpan<char> data) {
    if (!offset) {
      unsigned n = data.length();
      if (ZuUnlikely(n < 27)) return 0; // shortest request length is 27
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
      offset = b + o + 2;
    }
    return Header<Bits>::parse(data);
  }
};

template <unsigned Bits = 7>
struct Response : public Header<Bits> {
  ZuCSpan	protocol;	// e.g. HTTP/1.1
  int		code = -1;	// e.g. 200
  ZuCSpan	reason;		// e.g. OK

  using Header<Bits>::offset;

  // rebase spans
  void rebase(ptrdiff_t o) {
    if (!o || !offset) return;
    protocol.rebase(o);
    reason.rebase(o);
    Header<Bits>::rebase(o);
  }

  // parse response line
  int parse(ZuSpan<char> data) { // returns offset to body, -1 if incomplete
    if (!offset) {
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
      offset = b + o + 2;
    }
    return Header<Bits>::parse(data);
  }
};

// parse chunk header
// - returns {length, offset} where offset is offset to chunk data
// - returns {0} if unterminated/incomplete
// - returns {-1} if invalid/corrupt
ZuInline constexpr uint8_t hex(uint8_t c) {
  c |= 0x20;
  return 
    (c >= 'a' && c <= 'f') ? (c - 'a') + 10 :
    (c >= '0' && c <= '9') ? c - '0' : 0xff;
}
struct ChunkHdr {
  int		offset = 0;
  int		length = 0;

  inline int parse(ZuCSpan data) {
    unsigned o = 0, n = data.length();
    if (n > 10) n = 10;
    int len = 0;
    while (o < n) {
      auto c = data[o];
      if (c == '\r') {
	if (++o < n && data[o] == '\n') {
	  if (len < 0 || o < 2) return offset = -1; // invalid
	  length = len;
	  return offset = o + 1;
	}
	if (o >= n && n < 10) return 0; // unterminated / incomplete
	return offset = -1;
      }
      uint8_t i = hex(c);
      if (i == 0xff) return offset = -1;
      len = (len<<4) | i;
      o++;
    }
    return offset = -1;
  }

  bool complete() { return offset != 0; }
  bool valid() { return offset >= 0; }
  bool eob() { return !length; }
};

struct Body {
  static constexpr unsigned DefltMax = (1<<20); // 1Mb default

  ZtArray<char>		data; // the payload
  unsigned		max = DefltMax;
  int			contentLength = -1;
  ZuArray<char, 12>	chunkBuf;
  ChunkHdr		chunkHeader;
  ZtArray<char>		chunkTrailer = ZtArray<char>(4);
  unsigned		chunkTotal = 0;
  TransferEncoding::T	transferEncoding = -1;
  bool			chunked = false;
  bool			valid = true;
  bool			complete = false;

  // access and validate the Transfer-Encoding and Content-Length headers
  template <typename Header>
  bool init(const Header &header, unsigned max = DefltMax) {
    this->max = max;
    if (auto s = header.headers.findVal("Transfer-Encoding"))
      split(s, [this](unsigned i, ZuCSpan token) {
	// chunked must come last, anything else must be first
	if (chunked)
	  valid = false;
	else if (token == "chunked")
	  chunked = true;
	else if (i)
	  valid = false;
	else
	  transferEncoding = TransferEncoding::lookup(token);
      });
    if (valid && !chunked)
      if (auto s = header.headers.findVal("Content-Length")) {
	contentLength = ZuBox<unsigned>{s};
	if (contentLength > max)
	  valid = false;
	else
	  data.size(contentLength);
      }
    // if (chunked) chunkTrailer << "\r\n";
    return valid;
  }

  // once complete, chunkTrailer can be parsed by Header<Bits>
  int process(ZuCSpan rcvd) {
    if (ZuUnlikely(complete)) return valid ? 0 : -1;
    if (!chunked) {
      if (ZuUnlikely(contentLength < 0)) {
	valid = false, complete = true;
	return -1;
      }
      unsigned remaining = contentLength - data.length();
      if (rcvd.length() > remaining) rcvd.trunc(remaining);
      data << rcvd;
      if (rcvd.length() == remaining) complete = true;
      return rcvd.length();
    }
    unsigned processed = 0;
    while (rcvd) {
      // reading chunk header?
      if (!chunkHeader.complete()) {
	unsigned remaining = chunkBuf.size() - chunkBuf.length();
	unsigned n = rcvd.length();
	if (n > remaining) n = remaining;
	chunkBuf << ZuCSpan{&rcvd[0], n};
	if (chunkTotal) { // not the first chunk, chunkBuf starts with "\r\n"
	  if (chunkBuf.length() < 5) return n; // incomplete
	  if (chunkBuf[0] != '\r' || chunkBuf[1] != '\n') {
	    valid = false, complete = true;
	    return -1;
	  }
	  chunkBuf.shift(2);
	}
	chunkHeader.parse(chunkBuf);
	if (!chunkHeader.complete()) return n;
	// chunk header complete - update state
	if (!chunkHeader.valid()) {
	  valid = false, complete = true;
	  return -1;
	}
	chunkTotal += chunkHeader.length;
	n -= (chunkBuf.length() - chunkHeader.offset);
	chunkBuf.length(chunkHeader.offset);
	processed += n;
	rcvd.offset(n);
	if (!rcvd) break;
      }
      // last chunk?
      if (chunkHeader.eob()) {
	// optimize for fast path - no trailer payload
	if (ZuLikely(!chunkTrailer &&
	    rcvd.length() >= 2 &&
	    rcvd[0] == '\r' && rcvd[1] == '\n')) {
	  chunkTrailer << "\r\n\r\n";
	  processed += 2;
	  complete = true;
	  break;
	}
	if (ZuLikely(chunkTrailer.length() == 1 &&
	    chunkTrailer[0] == '\r' && rcvd[0] == '\n')) {
	  chunkTrailer << "\n\r\n";
	  ++processed;
	  complete = true;
	  break;
	}
	// slow path - append received data to chunk trailer
	unsigned o = chunkTrailer.length();
	chunkTrailer << rcvd;
	int n = eoh(chunkTrailer);
	if (ZuUnlikely(n < 0)) { // unterminated trailer
	  processed += rcvd.length();
	  break;
	}
	// truncate chunk trailer
	chunkTrailer.length(n);
	chunkTrailer.truncate();
	// complete
	n -= o;
	processed += n;
	rcvd.offset(n);
	complete = true;
	break;
      }
      // append to body
      if (data.length() < chunkTotal) {
	unsigned n = chunkTotal - data.length();
	if (n > rcvd.length()) n = rcvd.length();
	data << ZuCSpan{&rcvd[0], n};
	processed += n;
	rcvd.offset(n);
	if (data.length() >= chunkTotal) { // onto the next chunk
	  chunkHeader = {};
	  chunkBuf = {};
	}
      }
    }
    return processed;
  }
};

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
