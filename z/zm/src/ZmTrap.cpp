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

// SIGINT/SIGTERM/SIGSEGV trapping (and the Windows equivalent)

#include <zlib/ZmTrap.hpp>

#include <stdio.h>
#include <signal.h>

#include <zlib/ZuStringN.hpp>
#include <zlib/ZuBox.hpp>

#include <zlib/ZmAssert.hpp>
#include <zlib/ZmBackTrace.hpp>
#include <zlib/ZmSingleton.hpp>
#include <zlib/ZmTime.hpp>

ZmTrap *ZmTrap::instance()
{
  return ZmSingleton<ZmTrap>::instance();
}

extern "C" {
#ifndef _WIN32
  extern void ZmTrap_sigint(int);
  extern void ZmTrap_sighup(int);
  extern void ZmTrap_sigsegv(int, siginfo_t *, void *);
#else
  extern BOOL WINAPI ZmTrap_handler(DWORD event);
  extern LONG NTAPI ZmTrap_exHandler(EXCEPTION_POINTERS *exInfo);
#endif
};

void ZmTrap::trap_()
{
#ifndef _WIN32
  {
    struct sigaction s;

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
  SetConsoleCtrlHandler(&ZmTrap_handler, TRUE);
#ifdef ZDEBUG
  AddVectoredExceptionHandler(1, &ZmTrap_exHandler);
#endif
#endif
}

#ifndef _WIN32
void ZmTrap_sigint(int)
{
  ZmFn<> sigintFn = ZmTrap::sigintFn();

  if (sigintFn) sigintFn();
}

void ZmTrap_sighup(int)
{
  ZmFn<> sighupFn = ZmTrap::sighupFn();

  if (sighupFn) sighupFn();
}
#else
BOOL WINAPI ZmTrap_handler(DWORD event)
{
  ZmFn<> sigintFn = ZmTrap::sigintFn();

  if (sigintFn) sigintFn();
  return TRUE;
}
#endif

void ZmTrap_sleep()
{
  Zm::sleep(1);
}

#ifdef ZDEBUG
#ifndef _WIN32
void ZmTrap_sigsegv(int s, siginfo_t *si, void *c)
{
  static ZmAtomic<uint32_t> recursed = 0;
  if (recursed.cmpXch(1, 0)) return;
  {
    struct sigaction s;

    memset(&s, 0, sizeof(struct sigaction));
    s.sa_flags = 0;
    s.sa_sigaction = (void (*)(int, siginfo_t *, void *))SIG_DFL;
    sigemptyset(&s.sa_mask);
    sigaction(SIGSEGV, &s, 0);
  }
  ZmBackTrace bt;
  bt.capture(1);
  write(2, "SIGSEGV @", 10);
  {
    ZuStringN<32> buf;
    buf << "0x" << ZuBoxPtr(si->si_addr).hex() << '\n';
    write(2, buf.data(), buf.length());
  }
  {
    ZuStringN<ZmBackTrace_BUFSIZ> buf;
    buf << bt;
    write(2, buf.data(), buf.length());
  }
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
  HANDLE h = GetStdHandle(STD_ERROR_HANDLE);
  DWORD r;
  WriteConsoleA(h, "SIGSEGV\n", 8, &r, 0);
  {
    ZuStringN<32736> buf;
    buf << bt;
    WriteConsoleA(h, buf.data(), buf.length(), &r, 0);
  }
  WriteConsoleA(h, "\n", 1, &r, 0);
  return EXCEPTION_CONTINUE_SEARCH;
}
#endif
#endif /* ZDEBUG */
