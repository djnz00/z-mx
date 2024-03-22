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

// low-level last-ditch error logging and signal trapping

#include <zlib/ZmTrap.hpp>

#include <stdio.h>
#include <signal.h>

#include <zlib/ZuStringN.hpp>
#include <zlib/ZuBox.hpp>

#include <zlib/ZmBackTrace.hpp>
#include <zlib/ZmTime.hpp>

static ZmTrap::Fn ZmTrap_sigintFn;
static ZmTrap::Fn ZmTrap_sighupFn;

// no need for MT safety
void ZmTrap::sigintFn(Fn fn) { ZmTrap_sigintFn = fn; }
ZmTrap::Fn ZmTrap::sigintFn() { return ZmTrap_sigintFn; }

void ZmTrap::sighupFn(Fn fn) { ZmTrap_sighupFn = fn; }
ZmTrap::Fn ZmTrap::sighupFn() { return ZmTrap_sighupFn; }

extern "C" {
#ifndef _WIN32
  extern void ZmTrap_sigabrt(int);
  extern void ZmTrap_sigint(int);
  extern void ZmTrap_sighup(int);
  extern void ZmTrap_sigsegv(int, siginfo_t *, void *);
#else
  extern void __cdecl ZmTrap_sigabrt(int);
  extern BOOL WINAPI ZmTrap_handler(DWORD event);
  extern LONG NTAPI ZmTrap_exHandler(EXCEPTION_POINTERS *exInfo);
#endif
};

// trap signals, etc.
void ZmTrap::trap()
{
#ifndef _WIN32
  {
    struct sigaction s;

    memset(&s, 0, sizeof(struct sigaction));
    s.sa_handler = ZmTrap_sigabrt;
    sigemptyset(&s.sa_mask);
    sigaction(SIGABRT, &s, 0);
    memset(&s, 0, sizeof(struct sigaction));
    s.sa_handler = ZmTrap_sigint;
    sigemptyset(&s.sa_mask);
    sigaction(SIGINT, &s, 0);
    sigaction(SIGTERM, &s, 0);
    s.sa_handler = ZmTrap_sighup;
    sigaction(SIGHUP, &s, 0);
#ifdef ZDEBUG
    memset(&s, 0, sizeof(struct sigaction));
    s.sa_flags = SA_SIGINFO;
    s.sa_sigaction = ZmTrap_sigsegv;
    sigemptyset(&s.sa_mask);
    sigaction(SIGSEGV, &s, 0);
#endif
  }
#else
  _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
  signal(SIGABRT, &ZmTrap_sigabrt);
  SetConsoleCtrlHandler(&ZmTrap_handler, TRUE);
#ifdef ZDEBUG
  AddVectoredExceptionHandler(1, &ZmTrap_exHandler);
#endif
#endif
}

// last-resort logging of a fatal error
void ZmTrap::log(ZuString s)
{
#ifndef _WIN32
  ::write(2, s.data(), s.length());
#else
  ZmTrap::winErrLog(EVENTLOG_ERROR_TYPE, s);
#endif
}

// Windows error logging
// - used for last-ditch error logging under Windows
// - MessageBoxA(0, s, "Title", MB_ICONEXCLAMATION)
//   is inappropriate for server-side mission-critical software
// - GetStdHandle(STD_ERROR_HANDLE) causes unwanted console popups
//   in non-console apps
#ifdef _WIN32
enum { ProgramSize = 64 }; // program name length
using Program = ZuStringN<ProgramSize>;
static Program ZmTrap_program;
static ZmPLock ZmTrap_lock;

void ZmTrap::winProgram(ZuString s)
{
  ZmGuard<ZmPLock> guard(ZmTrap_lock);
  ZmTrap_program = s;
}

void ZmTrap::winErrLog(int type, ZuString s)
{
  // NTFS max path length - MAX_PATH is 260 and deprecated
  enum { BufSize = 32768 };
  using WBuf = ZuWStringN<BufSize>;
  using Buf = ZuStringN<BufSize>;

  static Name name;
  static WBuf wbuf;
  static Buf buf;
  static HANDLE handle = INVALID_HANDLE_VALUE;

  ZmGuard<ZmPLock> guard(ZmTrap_lock);

  if (ZuUnlikely(handle == INVALID_HANDLE_VALUE))
    handle = RegisterEventSource(0, L"EventSystem");

  if (ZuUnlikely(!ZmTrap_program)) {
    wbuf.null();
    GetModuleFileName(0, wbuf.data(), BufSize);
    wbuf.calcLength();
    int i = wbuf.length();
    while (i > 0) { if (wbuf[--i] == L'\\') break; }
    if (i) ++i;
    ZuString wname = wbuf;
    wname.offset(i);
    if (wname.length() > 64) wname.offset(wname.length() - 64);
    auto &name = ZmTrap_program;
    name.length(ZuUTF<char, wchar_t>::cvt(
	  ZuArray<char>{name.data(), name.size() - 1}, wname));
  }

  buf.null();
  buf << ZmTrap_program << " - " << s;

  wbuf.null();
  wbuf.length(ZuUTF<wchar_t, char>::cvt(
	ZuArray<wchar_t>{wbuf.data(), wbuf.size() - 1}, buf));
  wbuf[wbuf.length() - (wbuf.length() == wbuf.size())] = 0;

  const wchar_t *w = wbuf.data();
  ReportEvent(handle, type, 0, 512, 0, 1, 0, &w, 0);
}
#endif

// SIGABRT handling, primarily for assertion failures
void ZmTrap_sigabrt(int)
{
  static ZmAtomic<uint32_t> recursed = 0;
  if (recursed.cmpXch(1, 0)) return;
#ifndef _WIN32
  {
    struct sigaction s;

    memset(&s, 0, sizeof(struct sigaction));
    s.sa_flags = 0;
    s.sa_sigaction =
      reinterpret_cast<void (*)(int, siginfo_t *, void *)>(SIG_DFL);
    sigemptyset(&s.sa_mask);
    sigaction(SIGSEGV, &s, 0);
  }
#else
  signal(SIGABRT, SIG_DFL);
#endif
  ZmBackTrace bt;
  bt.capture(1);
  static ZuStringN<ZmBackTrace_BUFSIZ> buf;
  buf << "SIGABRT\n" << bt;
  ZmTrap::log(buf);
}

// CTRL-C and disconnect/logout handling
#ifndef _WIN32
void ZmTrap_sigint(int)
{
  if (auto fn = ZmTrap_sigintFn) fn();
}

void ZmTrap_sighup(int)
{
  if (auto fn = ZmTrap_sighupFn) fn();
}
#else
BOOL WINAPI ZmTrap_handler(DWORD event)
{
  if (auto fn = ZmTrap_sigintFn) fn();
  return TRUE;
}
#endif

// SEGV handling
#ifndef _WIN32
void ZmTrap_sigsegv(int s, siginfo_t *si, void *c)
{
  static ZmAtomic<uint32_t> recursed = 0;
  if (recursed.cmpXch(1, 0)) return;
  {
    struct sigaction s;

    memset(&s, 0, sizeof(struct sigaction));
    s.sa_flags = 0;
    s.sa_sigaction =
      reinterpret_cast<void (*)(int, siginfo_t *, void *)>(SIG_DFL);
    sigemptyset(&s.sa_mask);
    sigaction(SIGSEGV, &s, 0);
  }
  ZmBackTrace bt;
  bt.capture(1);
  static ZuStringN<ZmBackTrace_BUFSIZ> buf;
  buf << "SIGSEGV @0x" << ZuBoxPtr(si->si_addr).hex() << '\n' << bt;
  ZmTrap::log(buf);
}
#else
LONG NTAPI ZmTrap_exHandler(EXCEPTION_POINTERS *exInfo)
{
  static ZmAtomic<uint32_t> recursed = 0;
  if (recursed.cmpXch(1, 0) ||
      exInfo->ExceptionRecord->ExceptionCode != STATUS_ACCESS_VIOLATION)
    return EXCEPTION_CONTINUE_SEARCH;
  ZmBackTrace bt;
  bt.capture(exInfo, 0);
  static ZuStringN<ZmBackTrace_BUFSIZ> buf;
  buf << "SIGSEGV\n" << bt;
  ZmTrap::log(buf);
  return EXCEPTION_CONTINUE_SEARCH;
}
#endif
