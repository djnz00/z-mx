//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Gtk application wrapper

#ifndef ZGtkApp_HH
#define ZGtkApp_HH

#ifndef ZGtkLib_HH
#include <zlib/ZGtkLib.hh>
#endif

#include <zlib/ZuString.hh>

#include <zlib/ZmScheduler.hh>
#include <zlib/ZmLock.hh>

#include <zlib/ZtString.hh>

namespace ZGtk {

class ZGtkAPI App {
public:
  // e.g. "gimp20", "/usr/share" - initialize locale, libintl (gettext)
  void i18n(ZtString domain, ZtString dataDir);

  void attach(ZmScheduler *sched, unsigned sid); // calls ZmTrap::trap()
  ZuInline void detach() { detach(ZmFn<>{}); }
  void detach(ZmFn<>);

  ZuInline ZmScheduler *sched() const { return m_sched; }
  ZuInline unsigned sid() const { return m_sid; }

  template <typename ...Args>
  ZuInline void run(Args &&... args)
    { m_sched->run(m_sid, ZuFwd<Args>(args)...); }
  template <typename ...Args>
  ZuInline void invoke(Args &&... args)
    { m_sched->invoke(m_sid, ZuFwd<Args>(args)...); }

private:
  void attach_();	// runs on Gtk thread
  void detach_(ZmFn<>);	// ''

  void wake();
  void wake_();		// ''
  static void run_();	// ''

private:
  GSource		*m_source = nullptr;
  ZmScheduler		*m_sched = nullptr;
  unsigned		m_sid = 0;
  ZtString		m_domain;	// libintl domain
  ZtString		m_dataDir;	// libintl data directory
};

} // ZGtk

#endif /* ZGtkApp_HH */
