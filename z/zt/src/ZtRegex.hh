//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Perl compatible regular expressions (pcre)

#ifndef ZtRegex_HH
#define ZtRegex_HH

#ifndef ZtLib_HH
#include <zlib/ZtLib.hh>
#endif

#include <pcre.h>

#include <zlib/ZuAssert.hh>
#include <zlib/ZuTraits.hh>
#include <zlib/ZuString.hh>
#include <zlib/ZuPrint.hh>

#include <zlib/ZmCleanup.hh>
#include <zlib/ZmSingleton.hh>

#include <zlib/ZtString.hh>

struct ZtAPI ZtRegexError {
  const char	*message = 0;
  int		code;
  int		offset;

  static const char *strerror(int);

  template <typename S> void print(S &s) const {
    if (message) {
      s << "ZtRegex Error \"" << message << "\" (" << code << ")"
	" at offset " << offset;
    } else {
      s << "ZtRegex pcre_exec() Error: " << strerror(code);
    }
  }
  friend ZuPrintFn ZuPrintType(ZtRegexError *);
};

class ZtAPI ZtRegex {
  ZtRegex(const ZtRegex &) = delete;
  ZtRegex &operator =(const ZtRegex &) = delete;

  ZtRegex();

public:
  using Capture = ZuString;
  using Captures = ZtArray<Capture>;

  // pcre_compile() options
  ZtRegex(const char *pattern, int options = PCRE_UTF8);

  // ZtRegex is move-only (by design)
  ZtRegex(ZtRegex &&r) :
      m_regex(r.m_regex), m_extra(r.m_extra), m_captureCount(r.m_captureCount) {
    r.m_regex = 0;
    r.m_extra = 0;
    r.m_captureCount = 0;
  }
  ZtRegex &operator =(ZtRegex &&r) {
    if (this == &r) return *this;
    m_regex = r.m_regex;
    m_extra = r.m_extra;
    m_captureCount = r.m_captureCount;
    r.m_regex = 0;
    r.m_extra = 0;
    r.m_captureCount = 0;
    return *this;
  }

  ~ZtRegex();

  friend ZuUnsigned<ZmCleanup::Platform> ZmCleanupLevel(ZtRegex *);

  void study();

  unsigned captureCount() const { return m_captureCount; }

  // options below are pcre_exec() options

  // captures[0] is $`
  // captures[1] is $&
  // captures[2] is $1
  // captures[n - 1] is $' (where n = number of captured substrings + 3)
  // n is always >= 3 for a successful match, or 0 for no match
  // return value is number of captures excluding $` and $', i.e. (n - 2)
  //   0 implies no match
  //   1 implies $`, $&, $' captured (n == 3)
  //   2 implies $`, $&, $1, $' captured (n == 4)
  // etc.
  /*
     $& is the entire matched string.
     $` is everything before the matched string.
     $' is everything after the matched string.
  */

  unsigned m(ZuString s, unsigned offset = 0, int options = 0) const {
    ZtArray<unsigned> ovector;
    return exec(s, offset, options, ovector);
  }
  unsigned m(ZuString s,
      Captures &captures, unsigned offset = 0, int options = 0) const {
    ZtArray<unsigned> ovector;
    unsigned i = exec(s, offset, options, ovector);
    if (i) capture(s, ovector, captures);
    return i;
  }

  template <typename S>
  ZuIfT<
    ZuInspect<ZtString, S>::Is ||
    ZuInspect<ZtArray<char>, S>::Is, unsigned>
  s(S &s, ZuString r, unsigned offset = 0, int options = 0) const {
    ZtArray<unsigned> ovector;
    unsigned i = exec(s, offset, options, ovector);
    if (i) s.splice(ovector[0], ovector[1] - ovector[0], r.data(), r.length());
    return i;
  }

  template <typename S>
  ZuIfT<
    ZuInspect<ZtString, S>::Is ||
    ZuInspect<ZtArray<char>, S>::Is, unsigned>
  sg(S &s, ZuString r, unsigned offset = 0, int options = 0) const {
    ZtArray<unsigned> ovector;
    unsigned n = 0;
    unsigned slength = s.length(), rlength = r.length();

    while (offset < slength && exec(s, offset, options, ovector)) {
      s.splice(ovector[0], ovector[1] - ovector[0], r.data(), rlength);
      offset = ovector[0] + rlength;
      if (ovector[1] == ovector[0]) offset++;
      options |= PCRE_NO_UTF8_CHECK;
      ++n;
    }

    return n;
  }

  unsigned split(ZuString s, Captures &a, int options = 0) const;

  int index(const char *name) const; // pcre_get_stringnumber()

private:
  unsigned exec(ZuString s,
      unsigned offset, int options, ZtArray<unsigned> &ovector) const;
  void capture(ZuString s,
      const ZtArray<unsigned> &ovector, Captures &captures) const;

  pcre		*m_regex;
  pcre_extra	*m_extra;
  unsigned	m_captureCount;
};

// we quote the pattern using the pre-processor to avoid having to double
// backslash the RE, then strip the leading/trailing double-quotes

#define ZtREGEX(pattern_, ...) ZmStatic([]{ \
  static char pattern[] = #pattern_; \
  ZuAssert(sizeof(pattern) >= 2); \
  pattern[sizeof(pattern) - 2] = 0; \
  return new ZtRegex(&pattern[1] __VA_OPT__(,) __VA_ARGS__); \
})

#endif /* ZtRegex_HH */
