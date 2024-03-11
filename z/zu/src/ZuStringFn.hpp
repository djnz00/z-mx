//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// generic string operations

#ifndef ZuStringFn_HPP
#define ZuStringFn_HPP

#ifndef ZuLib_HPP
#include <zlib/ZuLib.hpp>
#endif

#ifdef _MSC_VER
#pragma once
#endif

#include <string.h>
#include <wchar.h>
#include <stdarg.h>
#include <stdio.h>

#include <zlib/ZuAssert.hpp>
#include <zlib/ZuTraits.hpp>
#include <zlib/ZuInspect.hpp>

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

#endif /* ZuStringFn_HPP */
