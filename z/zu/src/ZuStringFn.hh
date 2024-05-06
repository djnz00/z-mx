//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// generic string operations

#ifndef ZuStringFn_HH
#define ZuStringFn_HH

#ifndef ZuLib_HH
#include <zlib/ZuLib.hh>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <string.h>
#include <wchar.h>
#include <stdarg.h>
#include <stdio.h>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4996)
#endif

namespace Zu {
// length

  ZuInline unsigned strlen_(const char *s) { return strlen(s); }
  ZuInline unsigned strlen_(const wchar_t *w) { return wcslen(w); }

// comparison

  ZuInline int strcmp_(const char *s1, const char *s2)
    { return strcmp(s1, s2); }
  ZuInline int strcmp_(const wchar_t *w1, const wchar_t *w2)
    { return wcscmp(w1, w2); }
  ZuInline int strcmp_(const char *s1, const char *s2, unsigned n)
    { return strncmp(s1, s2, n); }
  ZuInline int strcmp_(const wchar_t *w1, const wchar_t *w2, unsigned n)
    { return wcsncmp(w1, w2, n); }

#ifdef _WIN32
  ZuInline int stricmp_(const char *s1, const char *s2)
    { return stricmp(s1, s2); }
  ZuInline int stricmp_(const wchar_t *w1, const wchar_t *w2)
    { return wcsicmp(w1, w2); }
  ZuInline int stricmp_(const char *s1, const char *s2, unsigned n)
    { return strnicmp(s1, s2, n); }
  ZuInline int stricmp_(const wchar_t *w1, const wchar_t *w2, unsigned n)
    { return wcsnicmp(w1, w2, n); }
#else
  ZuInline int stricmp_(const char *s1, const char *s2)
    { return strcasecmp(s1, s2); }
#if !defined(MIPS_NO_WCHAR)
  ZuInline int stricmp_(const wchar_t *w1, const wchar_t *w2)
    { return wcscasecmp(w1, w2); }
#endif
  ZuInline int stricmp_(const char *s1, const char *s2, unsigned n)
    { return strncasecmp(s1, s2, n); }
#if !defined(MIPS_NO_WCHAR)
  ZuInline int stricmp_(const wchar_t *w1, const wchar_t *w2, unsigned n)
    { return wcsncasecmp(w1, w2, n); }
#endif
#endif

// padding

  ZuInline void strpad(char *s, unsigned n) { memset(s, ' ', n); }
  ZuInline void strpad(wchar_t *w, unsigned n) { wmemset(w, L' ', n); }

// vsnprintf

  ZuExtern int vsnprintf(
      char *s, unsigned n, const char *format, va_list ap_);
  ZuExtern int vsnprintf(
      wchar_t *w, unsigned n, const wchar_t *format, va_list ap_);

// null wchar_t string

  ZuInline const wchar_t *nullWString() {
#ifdef _MSC_VER
    return L"";
#else
    static wchar_t s[1] = { 0 };

    return s;
#endif
  }
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZuStringFn_HH */
