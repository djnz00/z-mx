//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Perl compatible regular expressions (pcre)

#define ZtRegex_CPP

#include <zlib/ZtRegex.hh>

ZtRegex::ZtRegex(const char *pattern, int options) : m_extra(0)
{
  ZtRegexError error;

  m_regex = pcre_compile2(
    pattern, options, &error.code, &error.message, &error.offset, 0);

  if (!m_regex) throw error;

  if (!pcre_fullinfo(m_regex, 0, PCRE_INFO_CAPTURECOUNT, &m_captureCount))
    m_captureCount++;
  else
    m_captureCount = 1;
}

ZtRegex::~ZtRegex()
{
  if (m_extra) (pcre_free_study)(m_extra);
  if (m_regex) (pcre_free)(m_regex);
}

void ZtRegex::study()
{
  ZtRegexError error;

  error.offset = -1;
  error.code = -1;

  m_extra = pcre_study(m_regex, PCRE_STUDY_JIT_COMPILE, &error.message);

  if (error.message) {
    if (m_extra) (pcre_free)(m_extra);
    m_extra = 0;
    throw error;
  }
}

unsigned ZtRegex::split(ZuString s, Captures &a, int options) const
{
  unsigned offset = 0, last = 0;
  ZtArray<unsigned> ovector;
  unsigned slength = s.length();

  while (offset < slength && exec(s, offset, options, ovector)) {
    if (offset || ovector[1] > ovector[0])
      new (a.push()) Capture(s.data() + last, ovector[0] - last);
    last = offset = ovector[1];
    if (ovector[1] == ovector[0]) offset++;
    options |= PCRE_NO_UTF8_CHECK;
  }
  if (last < slength)
    new (a.push()) Capture(s.data() + last, slength - last);

  return a.length();
}

int ZtRegex::index(const char *name) const
{
  int i = pcre_get_stringnumber(m_regex, name);
  if (i < 0) return -1;
  return i + 1;
}

unsigned ZtRegex::exec(
    ZuString s, unsigned offset,
    int options, ZtArray<unsigned> &ovector) const
{
  unsigned slength = s.length();

  if (slength <= offset) return 0;

  ovector.length(m_captureCount * 3, false);

  int c = pcre_exec(
      m_regex, m_extra, s.data(), slength,
      offset, options,
      reinterpret_cast<int *>(ovector.data()), ovector.length());

  if (c >= 0) return c;

  if (c == PCRE_ERROR_NOMATCH) return 0;

  throw ZtRegexError{nullptr, c, -1};
}

void ZtRegex::capture(
    ZuString s, const ZtArray<unsigned> &ovector, Captures &captures) const
{
  unsigned offset, length;
  unsigned slength = s.length();

  captures.length(0, false);
  captures.size(m_captureCount + 2);
  new (captures.push()) Capture(s.data(), ovector[0]); // $`
  unsigned n = m_captureCount;
  for (unsigned i = 0; i < n; i++) {
    offset = ovector[i<<1];
    if (offset < 0) {
      new (captures.push()) Capture();
    } else {
      length = ovector[(i<<1) + 1] - offset;
      new (captures.push()) Capture(s.data() + offset, length);
    }
  }
  new (captures.push())
    Capture(s.data() + ovector[1], slength - ovector[1]); // $'
}
static const char *exec_errors[] = {
  "NOMATCH",
  "NULL",
  "BADOPTION",
  "BADMAGIC",
  "UNKNOWN_OPCODE",
  "NOMEMORY",
  "NOSUBSTRING",
  "MATCHLIMIT",
  "CALLOUT",
  "BADUTF",
  "BADUTF_OFFSET",
  "PARTIAL",
  "BADPARTIAL",
  "INTERNAL",
  "BADCOUNT",
  "DFA_UITEM",
  "DFA_UCOND",
  "DFA_UMLIMIT",
  "DFA_WSSIZE",
  "DFA_RECURSE",
  "RECURSIONLIMIT",
  "NULLWSLIMIT",
  "BADNEWLINE",
  "BADOFFSET",
  "SHORTUTF",
  "RECURSELOOP",
  "JIT_STACKLIMIT",
  "BADMODE",
  "BADENDIANNESS",
  "DFA_BADRESTART",
  "JIT_BADOPTION",
  "BADLENGTH",
  "UNSET"
};

const char *ZtRegexError::strerror(int i)
{
  enum { N = sizeof(exec_errors) / sizeof(exec_errors[0]) };
  i = -i - 1;
  if (i < 0 || i >= N) return "UNKNOWN";
  return exec_errors[i];
}
