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

// Meyers / Phoenix TLS Multiton

#include <zlib/ZuLib.hpp>

#include <zlib/ZmSpecific.hpp>

// statically-initialized spinlock to guard initial singleton registration
// and cleanup at exit; little if any contention is anticipated; access
// intended to be exceptional, intermittent, almost exclusively during
// application startup and shutdown
static uint32_t ZmSpecific_lock_ = 0;
ZmAPI void ZmSpecific_lock()
{
  ZmAtomic<uint32_t> *ZuMayAlias(lock) =
    reinterpret_cast<ZmAtomic<uint32_t> *>(&ZmSpecific_lock_);
  while (ZuUnlikely(lock->cmpXch(1, 0))) Zm::yield();
}
ZmAPI void ZmSpecific_unlock()
{
  ZmAtomic<uint32_t> *ZuMayAlias(lock) =
    reinterpret_cast<ZmAtomic<uint32_t> *>(&ZmSpecific_lock_);
  *lock = 0;
}

#ifdef _WIN32

// Win32 voodoo to force TLS support in linked image
extern "C" { extern DWORD _tls_used; }
struct Win32_Voodoo {
  volatile DWORD value;
  Win32_Voodoo() : value(_tls_used) { };
};

// use TLS API from within DLL to be safe on 2K and XP
using Allocator = ZmSpecific_Allocator<>;

// per-instance cleanup context
using O = ZmSpecific_Object;

struct Cleanup {
  static Allocator &head_() { static Allocator a; return a; }
  static Allocator &tail_() { static Allocator a; return a; }
  static O *head() { return static_cast<O *>(head_().get()); }
  static void head(O *o) { head_().set(o); }
  static O *tail() { return static_cast<O *>(tail_().get()); }
  static void tail(O *o) { tail_().set(o); }

  static Win32_Voodoo	ref;
};

Win32_Voodoo Cleanup::ref;	// TLS reference

void ZmSpecific_cleanup()
{
  for (;;) {
    ZmSpecific_lock();
    O *o = Cleanup::head(); // LIFO
    if (!o) { ZmSpecific_unlock(); return; }
    o->dtor(); // unlocks
  }
}

ZmAPI void ZmSpecific_cleanup_add(O *o)
{
  o->modPrev = nullptr;
  if (!(o->modNext = Cleanup::head()))
    Cleanup::tail(o);
  else
    o->modNext->modPrev = o;
  Cleanup::head(o);
}

ZmAPI void ZmSpecific_cleanup_del(O *o)
{
  if (!o->modPrev)
    Cleanup::head(o->modNext);
  else
    o->modPrev->modNext = o->modNext;
  if (!o->modNext)
    Cleanup::tail(o->modPrev);
  else
    o->modNext->modPrev = o->modPrev;
}

extern "C" {
  typedef void (NTAPI *ZmSpecific_cleanup_ptr)(HINSTANCE, DWORD, void *);
  ZmExtern void NTAPI ZmSpecific_cleanup_(HINSTANCE, DWORD, void *);
}

void NTAPI ZmSpecific_cleanup_(HINSTANCE, DWORD d, void *)
{
  if (d == DLL_THREAD_DETACH) ZmSpecific_cleanup();
}

// more Win32 voodoo
#ifdef _MSC_VER
#pragma section(".CRT$XLC", long, read)
extern "C" {
  __declspec(allocate(".CRT$XLC")) ZmSpecific_cleanup_ptr
    __ZmSpecific_cleanup__ = ZmSpecific_cleanup_;
}
#else
extern "C" {
  PIMAGE_TLS_CALLBACK __ZmSpecific_cleanup__
    __attribute__((section(".CRT$XLC"))) =
      (PIMAGE_TLS_CALLBACK)ZmSpecific_cleanup_;
}
#endif

#endif
