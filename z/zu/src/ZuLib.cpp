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

// Zero Copy Universal Library

#include <zlib/ZuLib.hpp>

#include "../../version.h"

ZuExtern const char ZuLib[] = "@(#) Zero Copy Universal Library v" Z_VERNAME;

#ifdef _MSC_VER
#pragma warning(disable:4996)
#endif

#ifndef _WIN32
#include <pthread.h>
#endif

ZuThreadContext::ZuThreadContext()
{
#ifndef _WIN32
  void *addr;
  size_t size;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_getattr_np(pthread_self(), &attr);
  pthread_attr_getstack(&attr, &addr, &size);
  m_stackAddr = addr;
  m_stackSize = size;
#else /* !_WIN32 */
  auto tib = reinterpret_cast<const NT_TIB *>(__readfsdword(0x18));
  m_stackAddr = tib->StackLimit;
  m_stackSize =
    reinterpret_cast<const uint8_t *>(tib->StackBase) -
    reinterpret_cast<const uint8_t *>(tib->StackLimit);
#endif /* !_WIN32 */
}

ZuThreadContext *ZuSelf()
{
  thread_local ZuThreadContext self_;
  return &self_;
}

#include <stdarg.h>
#include <stdio.h>

#include <zlib/ZuStringFn.hpp>

#ifndef va_copy
#ifdef __va_copy
#define va_copy(DST, SRC) __va_copy((DST), (SRC))
#else
#ifdef HAVE_VA_LIST_IS_ARRAY
#define va_copy(DST, SRC) (*(DST) = *(SRC))
#else
#define va_copy(DST, SRC) ((DST) = (SRC))
#endif
#endif
#endif

int Zu::vsnprintf(char *s, unsigned n, const char *format, va_list ap_)
{
  va_list ap;
  va_copy(ap, ap_);
  n = ::vsnprintf(s, n, format, ap);
  va_end(ap);
  return n;
}

#if !defined(MIPS_NO_WCHAR)
int Zu::vsnprintf(wchar_t *w, unsigned n, const wchar_t *format, va_list ap_)
{
  va_list ap;
  va_copy(ap, ap_);
#ifndef _WIN32
  n = vswprintf(w, n, format, ap);
#else
  n = _vsnwprintf(w, n, format, ap);
#endif
  va_end(ap);
  return n;
}
#endif
