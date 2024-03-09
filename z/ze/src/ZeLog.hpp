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
//      ZtSprintf("fopen(%s) failed: %s", file, ZeLastError.message().data()));
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
#include <zlib/ZmSemaphore.hpp>
#include <zlib/ZmThread.hpp>
#include <zlib/ZmTime.hpp>
#include <zlib/ZmRing.hpp>
#include <zlib/ZmRingFn.hpp>

#include <zlib/ZtString.hpp>

#include <zlib/ZePlatform.hpp>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4251 4231 4660)
#endif

class ZeLog;

namespace ZeSinkType {
  ZtEnumValues(ZeSinkType, File, Debug, System, Lambda);
}
struct ZeSink : public ZmPolymorph {
  int	type;	// ZeSinkType

  ZeSink(int type_) : type(type_) { }

  virtual void pre(ZeLogBuf &, const ZeEvent &) = 0;
  virtual void post(ZeLogBuf &, const ZeEvent &) = 0;
  virtual void age() = 0;
};

struct ZeSinkOptions {
  ZeSinkOptions &path(ZuString path) { m_path = path; return *this; }
  ZeSinkOptions &age(unsigned age) { m_age = age; return *this; }
  ZeSinkOptions &tzOffset(unsigned tzOffset)
    { m_tzOffset = tzOffset; return *this; }

  const auto &path() const { return m_path; }
  auto age() const { return m_age; }
  auto tzOffset() const { return m_tzOffset; }

private:
  ZuString	m_path;
  unsigned	m_age = 8;
  int		m_tzOffset = 0;
};

class ZeAPI ZeFileSink : public ZeSink {
  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;

public:
  ZeFileSink() :
      ZeSink{ZeSinkType::File} { init(); }
  ZeFileSink(const ZeSinkOptions &options) :
      ZeSink{ZeSinkType::File}, m_path{options.path()},
      m_age{options.age()}, m_dateFmt{options.tzOffset()} { init(); }

  ~ZeFileSink();

  void pre(ZeLogBuf &, const ZeEvent &);
  void post(ZeLogBuf &, const ZeEvent &);
  void age();

private:
  void init();
  void age_();

  ZtString		m_path;
  unsigned		m_age = 8;
  ZtDateFmt::CSV	m_dateFmt;

  Lock			m_lock;
    FILE *		  m_file = nullptr;
};

class ZeAPI ZeDebugSink : public ZeSink {
  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;

public:
  ZeDebugSink() : ZeSink{ZeSinkType::Debug},
    m_started{ZmTime::Now} { init(); }
  ZeDebugSink(const ZeSinkOptions &options) :
      ZeSink{ZeSinkType::Debug},
      m_path{options.path()}, m_started{ZmTime::Now} { init(); }

  ~ZeDebugSink();

  void pre(ZeLogBuf &, const ZeEvent &);
  void post(ZeLogBuf &, const ZeEvent &);
  void age() { } // unused

private:
  void init();

  ZtString	m_path;
  FILE *	m_file = nullptr;
  ZmTime	m_started;
};

struct ZeAPI ZeSysSink : public ZeSink {
  ZeSysSink() : ZeSink{ZeSinkType::System} { }

  void pre(ZeLogBuf &, const ZeEvent &);
  void post(ZeLogBuf &, const ZeEvent &);
  void age() { } // unused
};

struct ZeAPI ZeLambdaSink_ : public ZeSink {
  ZeLambdaSink_(int tzOffset = 0) :
      ZeSink{ZeSinkType::Lambda}, m_dateFmt{tzOffset} { }

  void pre(ZeLogBuf &buf, const ZeEvent &e);

private:
  ZtDateFmt::CSV	m_dateFmt;
};
template <typename Fn>
struct ZeLambdaSink : public ZeLambdaSink_ {
  Fn	fn;

  ZeLambdaSink(Fn fn_, int tzOffset = 0) :
      ZeLambdaSink_{tzOffset}, fn{ZuMv(fn_)} { }

  void post(ZeLogBuf &buf, const ZeEvent &e) { fn(buf, e); }
  void age() { } // unused
};

class ZeAPI ZeLog {
  ZeLog(const ZeLog &);
  ZeLog &operator =(const ZeLog &);		// prevent mis-use

friend ZmSingletonCtor<ZeLog>;

  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;

  using Ring = ZmRing<ZmRingMW<true>>;
  using Fn = ZmRingFn<ZeLog *>;

  ZeLog();

public:
  friend ZuUnsigned<ZmCleanup::Library> ZmCleanupLevel(ZeLog *);

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

  static void init() {
    instance()->init_();
  }
  static void init(const char *program) {
    instance()->init_(program);
  }
  static void init(const char *program, const char *facility) {
    instance()->init_(program, facility);
  }

  static void bufSize(unsigned n) {
    instance()->bufSize_(n);
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

  template <typename L>
  static void log(ZeLambdaEvent<L> e) {
    instance()->log_(ZuMv(e));
  }
  template <typename L>
  void log_(ZeLambdaEvent<L> e) {
    if (static_cast<int>(e.severity) < m_level) return;
    auto fn_ = [e = ZuMv(e)](ZeLog *this_) mutable {
      auto sink = this_->sink_();
      auto &buf = this_->m_buf;
      buf.null();
      sink->pre(buf, e);
      e.l(buf);
      sink->post(buf, e);
    };
    Fn fn{fn_};
    log__(fn);
  }
  static void age() { instance()->age_(); }

private:
  static ZeLog *instance();

  void init_();
  void init__();
  void init_(const char *program);
  void init_(const char *program, const char *facility);
  void init__(const char *program, const char *facility);

  void bufSize_(unsigned n) { m_bufSize = n; }

  ZuString program_() const { return m_program; }
  ZuString facility_() const { return m_facility; }

  int level_() const { return m_level; }
  void level_(int l) { m_level = l; }

  ZmRef<ZeSink> sink_();
  void sink_(ZmRef<ZeSink> sink);

  void start_();
  void start__();
  void stop_();
  void forked_();

  void work_();

  void log__(Fn &fn);

  void age_();

private:
  ZtString		m_program;
  ZtString		m_facility;
  int			m_level;
  unsigned		m_bufSize = (1<<20);	// 1Mbyte

  ZmThread		m_thread;
  Ring			m_ring;

  Lock			m_lock;
    ZmRef<ZeSink>	  m_sink;

  // thread-specific
  ZeLogBuf		m_buf;
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

template <typename Event>
inline void ZeBackTrace__(Event event_) {
  ZmBackTrace bt{1};
  ZeLog::log(ZeMkLambdaEvent__(
      event_.severity, event_.fileName, event_.lineNumber, event_.function,
      [bt = ZuMv(bt), msg = ZuMv(event_).msg](auto &s) mutable {
	msg(s);
	s << '\n' << ZuMv(bt);
      }));
}

#ifndef ZDEBUG

// filter out DEBUG messages in production builds
#define ZeLOG_(sev, msg) \
  ((sev > Ze::Debug) ? ZeLog::log(ZeMkLambdaEvent_(sev, msg)) : void())
#define ZeBackTrace_(sev, msg) \
  ((sev > Ze::Debug) ? ZeBackTrace__(ZeMkLambdaEvent_(sev, msg)) : void())

#else /* !ZEBUG */

#define ZeLOG_(sev, msg) ZeLOG__(ZeMkLambdaEvent_(sev, msg))
#define ZeBackTrace_(sev, msg) ZeBackTrace__(ZeMkLambdaEvent_(sev, msg))

#endif /* !ZEBUG */

#define ZeLOG(sev, msg) ZeLOG_(Ze:: sev, msg)
#define ZeBackTrace(sev, msg) ZeBackTrace_(Ze:: sev, msg)

#endif /* ZeLog_HPP */
