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

// singleton logger

// ZeLog::init("program", "daemon");	// for LOG_DAEMON
// ZeLog::sink(ZeLog::sysSink());	// sysSink() is syslog / event log
// ZeLOG(Debug, "debug message");	// ZeLOG() is macro
// ZeLOG(Error, ZeLastError);		// errno / GetLastError()
// ZeLOG(Error,
//	 ZtSprintf("fopen(%s) failed: %s", file, ZeLastError.message().data()));
// try { ... } catch (ZeError &e) { ZeLOG(Error, e); }

// if no sink is registered at initialization, the default sink is stderr
// on Unix and the Application event log on Windows

#ifndef ZeLog_HPP
#define ZeLog_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZeLib_HPP
#include <zlib/ZeLib.hpp>
#endif

#include <stdio.h>

#include <zlib/ZuCmp.hpp>
#include <zlib/ZuString.hpp>

#include <zlib/ZmBackTrace.hpp>
#include <zlib/ZmCleanup.hpp>
#include <zlib/ZmList.hpp>
#include <zlib/ZmRWLock.hpp>
#include <zlib/ZmSemaphore.hpp>
#include <zlib/ZmThread.hpp>
#include <zlib/ZmTime.hpp>
#include <zlib/ZmNoLock.hpp>

#include <zlib/ZtString.hpp>

#include <zlib/ZePlatform.hpp>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4251 4231 4660)
#endif

namespace ZeSinkType {
  ZtEnumValues("ZeSinkType", File, Debug, System, Lambda);
};
struct ZeSink : public ZmPolymorph {
  ZeSink(int type_) : type(type_) { }

  virtual void log(ZeEvent *) = 0;
  virtual void age() = 0;

  int	type;	// ZeSinkType
};

class ZeAPI ZeFileSink : public ZeSink {
  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;

public:
  ZeFileSink() :
      ZeSink{ZeSinkType::File} { init(); }
  ZeFileSink(ZuString name, unsigned age = 8, int tzOffset = 0) :
      ZeSink{ZeSinkType::File},
      m_filename(name), m_age(age), m_tzOffset(tzOffset) { init(); }

  ~ZeFileSink();

  void log(ZeEvent *e);
  void age();

private:
  void init();
  void age_();

  ZtString	m_filename;
  unsigned	m_age = 8;
  int		m_tzOffset = 0;	// timezone offset

  Lock		m_lock;
    FILE *	  m_file = nullptr;
};

class ZeAPI ZeDebugSink : public ZeSink {
  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;

public:
  ZeDebugSink() : ZeSink{ZeSinkType::Debug},
    m_started(ZmTime::Now) { init(); }
  ZeDebugSink(ZuString name) : ZeSink{ZeSinkType::Debug},
    m_filename(name), m_started(ZmTime::Now) { init(); }

  ~ZeDebugSink();

  void log(ZeEvent *e);
  void age() { } // unused

private:
  void init();

  ZtString	m_filename;
  FILE *	m_file = nullptr;
  ZmTime	m_started;
};

struct ZeAPI ZeSysSink : public ZeSink {
  ZeSysSink() : ZeSink{ZeSinkType::System} { }

  void log(ZeEvent *e);
  void age() { } // unused
};

template <typename L>
struct ZeLambdaSink : public ZeSink, public L {
  ZeLambdaSink(L l) : ZeSink{ZeSinkType::Lambda}, L{ZuMv(l)} { }

  void log(ZeEvent *e) { L::operator()(e); }
  void age() { } // unused
};

class ZeLog;

template <> struct ZmCleanup<ZeLog> {
  enum { Level = ZmCleanupLevel::Library };
  static void final(ZeLog *);
};

class ZeAPI ZeLog {
  ZeLog(const ZeLog &);
  ZeLog &operator =(const ZeLog &);		// prevent mis-use

friend ZmSingletonCtor<ZeLog>;
friend ZmCleanup<ZeLog>;

  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;

  using EventQueue = ZeEvent_Queue;

  ZeLog();

public:
  virtual ~ZeLog() { }

  template <typename ...Args>
  static ZmRef<ZeSink> fileSink(Args &&... args) {
    return new ZeFileSink(ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  static ZmRef<ZeSink> debugSink(Args &&... args) {
    return new ZeDebugSink(ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  static ZmRef<ZeSink> sysSink(Args &&... args) {
    return new ZeSysSink(ZuFwd<Args>(args)...);
  }
  template <typename L>
  static ZmRef<ZeSink> lambdaSink(L l) {
    return new ZeLambdaSink<L>(ZuMv(l));
  }

  template <typename ...Args>
  static void init(Args &&... args) {
    instance()->init_(ZuFwd<Args>(args)...);
  }

  static ZuString program() { return instance()->program_(); }

  static int level() { return instance()->level_(); }
  static void level(int l) { instance()->level_(l); }

  template <typename ...Args>
  static void sink(Args &&... args) {
    instance()->sink_(ZuFwd<Args>(args)...);
  }

  static void start() { instance()->start_(); }
  static void stop() { instance()->stop_(); }
  static void forked() { instance()->forked_(); }

  static void log(ZmRef<ZeEvent> e) { instance()->log_(ZuMv(e)); }
  static void age() { instance()->age_(); }

private:
  static ZeLog *instance();

  void init_(ZuString program) {
    m_program = program;
    Ze::sysloginit(m_program, 0);
  }
  void init_(ZuString program, ZuString facility) {
    m_program = program;
    Ze::sysloginit(m_program, facility);
  }

  ZuString program_();

  int level_() const { return m_level; }
  void level_(int l) { m_level = l; }

  void sink_(ZmRef<ZeSink> sink);

  void start_();
  void start__();
  void stop_();
  void forked_();

  void work_();
  void log_(ZmRef<ZeEvent> e);
  void log__(ZeEvent *e);

  void age_();

private:
  ZtString	m_program;
  int		m_level;
  ZmSemaphore	m_work;
  ZmSemaphore	m_started;
  Lock		m_lock;
    ZmRef<ZeSink> m_sink;
    EventQueue	  m_queue;
    ZmThread	  m_thread;
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

template <typename Msg>
auto Ze_BackTrace_fn(ZmBackTrace bt, Msg &&msg) {
  return [bt = ZuMv(bt), fn = ZeMessageFn(ZuFwd<Msg>(msg))](
      const ZeEvent &e, ZmStream &s) {
    fn(e, s);
    s << '\n' << bt;
  };
}

#define Ze_ERROR_(sev, e) ZeLog::log(ZeEVENT_(sev, (e)))
#define Ze_LOG_(sev, msg) ZeLog::log(ZeEVENT_(sev, msg))
#define Ze_BackTrace_(sev, msg) \
  do { \
    ZmBackTrace bt__(0); \
    ZeLog::log(ZeEVENT_(sev, Ze_BackTrace_fn(bt__, msg))); \
  } while (0)

#ifndef ZDEBUG

// filter out DEBUG messages in production builds
#define ZeERROR_(sev, e) \
  ((sev > Ze::Debug) ? (void)Ze_ERROR_(sev, e) : (void)0)
#define ZeLOG_(sev, msg) \
  ((sev > Ze::Debug) ? (void)Ze_LOG_(sev, msg) : (void)0)
#define ZeBackTrace_(sev, msg) \
  do { if (sev > Ze::Debug) Ze_BackTrace_(sev, msg); } while (0)

#else /* !ZEBUG */

#define ZeERROR_(sev, e) Ze_ERROR_(sev, e)
#define ZeLOG_(sev, msg) Ze_LOG_(sev, msg)
#define ZeBackTrace_(sev, msg) Ze_BackTrace_(sev, msg)

#endif /* !ZEBUG */

#define ZeERROR(sev, e) ZeERROR_(Ze:: sev, e)
#define ZeLOG(sev, msg) ZeLOG_(Ze:: sev, msg)
#define ZeBackTrace(sev, msg) ZeBackTrace_(Ze:: sev, msg)

#endif /* ZeLog_HPP */
