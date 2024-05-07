//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Gtk application wrapper

#ifndef ZGtkApp_HH
#define ZGtkApp_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZGtkLib_HH
#include <zlib/ZGtkLib.hh>
#endif

#include <glib.h>

#include <gtk/gtk.h>

#include <zlib/ZuString.hh>

#include <zlib/ZmScheduler.hh>
#include <zlib/ZmLock.hh>

#include <zlib/ZtString.hh>

namespace ZGtk {

class ZGtkAPI App {
public:
  // e.g. "gimp20", "/usr/share" - initialize locale, libintl (gettext)
  void i18n(ZtString domain, ZtString dataDir);

  void attach(ZmScheduler *sched, unsigned tid); // calls ZmTrap::trap()
  ZuInline void detach() { detach(ZmFn<>{}); }
  void detach(ZmFn<>);

  ZuInline ZmScheduler *sched() const { return m_sched; }
  ZuInline unsigned tid() const { return m_tid; }

  template <typename ...Args>
  ZuInline void run(Args &&... args)
    { m_sched->run(m_tid, ZuFwd<Args>(args)...); }
  template <typename ...Args>
  ZuInline void invoke(Args &&... args)
    { m_sched->invoke(m_tid, ZuFwd<Args>(args)...); }

private:
  void attach_();	// runs on Gtk thread
  void detach_(ZmFn<>);	// ''

  void wake();
  void wake_();		// ''
  static void run_();	// ''

private:
  GSource		*m_source = nullptr;
  ZmScheduler		*m_sched = nullptr;
  unsigned		m_tid = 0;
  ZtString		m_domain;	// libintl domain
  ZtString		m_dataDir;	// libintl data directory
};

} // ZGtk

#endif /* ZGtkApp_HH */
