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

using ZeLogBuf = ZuStringN<ZeLog_BUFSIZ>;

namespace ZeSinkType {
  ZtEnumValues("ZeSinkType", File, Debug, System, Lambda);
}
struct ZeSink : public ZmPolymorph {
  int	type;	// ZeSinkType

  ZeSink(int type_) : type(type_) { }

  virtual void pre(ZeLogBuf &, const ZeEvent &) = 0;
  virtual void post(ZeLogBuf &, const ZeEvent &) = 0;
  virtual void age() = 0;
};

class ZeAPI ZeFileSink : public ZeSink {
  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;

public:
  ZeFileSink() :
      ZeSink{ZeSinkType::File} { init(); }
  ZeFileSink(ZuString path, unsigned age = 8, int tzOffset = 0) :
      ZeSink{ZeSinkType::File},
      m_path{path}, m_age{age}, m_dateFmt{tzOffset} { init(); }

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
  ZeDebugSink(ZuString path) : ZeSink{ZeSinkType::Debug},
    m_path{path}, m_started{ZmTime::Now} { init(); }

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
  using Fn = ZmRingFn<ZmRingFnArgs<ZuTypeList<ZeLog *>>>;

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

  static void init(const char *program) {
    instance()->init_(program);
  }
  static void init(const char *program, const char *facility) {
    instance()->init_(program, facility);
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
  static void log(ZeEvent e, L l) { instance()->log_(ZuMv(e), ZuMv(l)); }
  template <typename L>
  void log_(ZeEvent e, L l) {
    if (static_cast<int>(e.severity) < m_level) return;
    auto fn_ = [e = ZuMv(e), l = ZuMv(l)](ZeLog *this_) mutable {
      auto sink = this_->sink_();
      auto &buf = this_->m_buf;
      buf.null();
      sink->pre(buf, e);
      ZuMv(l)(buf);
      sink->post(buf, e);
    };
    Fn fn{fn_};
    log__(fn);
  }
  static void age() { instance()->age_(); }

private:
  static ZeLog *instance();

  void init_();
  void init_(const char *program);
  void init_(const char *program, const char *facility);

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

namespace ZeLog_ {

template <typename U_> struct IsLiteral {
  using U = ZuDecay<U_>;
  enum { OK = ZuTraits<U>::IsArray &&
    ZuTraits<U>::IsPrimitive && ZuTraits<U>::IsCString &&
    ZuConversion<typename ZuTraits<U>::Elem, const char>::Same };
};
template <typename U, typename R = void>
using MatchLiteral = ZuIfT<IsLiteral<U>::OK, R>;

template <typename U_> struct IsPrint {
  using U = ZuDecay<U_>;
  enum { OK = !IsLiteral<U_>::OK &&
    (ZuTraits<U>::IsString || ZuPrint<U>::OK) };
};
template <typename U, typename R = void>
using MatchPrint = ZuIfT<IsPrint<U>::OK, R>;

template <typename U_> struct IsOther {
  using U = ZuDecay<U_>;
  enum { OK = !IsLiteral<U_>::OK && !IsPrint<U_>::OK };
};
template <typename U, typename R = void>
using MatchOther = ZuIfT<IsOther<U>::OK, R>;

template <typename Msg>
inline auto fn(Msg &&msg, ZeLog_::MatchOther<Msg> *_ = nullptr) {
  return ZuFwd<Msg>(msg);
}
template <typename Msg>
inline auto fn(Msg &&msg, ZeLog_::MatchLiteral<Msg> *_ = nullptr) {
  return [msg = static_cast<const char *>(msg)](ZeLogBuf &buf) mutable {
    buf << msg;
  };
}
template <typename Msg>
inline auto fn(Msg &&msg, ZeLog_::MatchPrint<Msg> *_ = nullptr) {
  return [msg = ZuFwd<Msg>(msg)](ZeLogBuf &buf) mutable {
    buf << ZuMv(msg);
  };
}

} // ZeLog_

template <typename Msg>
inline void ZeLOG__(ZeEvent e, Msg &&msg) {
  ZeLog::log(ZuMv(e), ZeLog_::fn(ZuFwd<Msg>(msg)));
}

template <typename Msg>
inline void ZeBackTrace__(ZeEvent e, Msg &&msg) {
  ZmBackTrace bt{1};
  ZeLog::log(ZuMv(e),
      [bt = ZuMv(bt), fn = ZeLog_::fn(ZuFwd<Msg>(msg))](ZeLogBuf &buf) mutable {
    ZuMv(fn)(buf);
    buf << '\n' << ZuMv(bt);
  });
}

#ifndef ZDEBUG

// filter out DEBUG messages in production builds
#define ZeLOG_(sev, msg) \
  ((sev > Ze::Debug) ? ZeLOG__(ZeEVENT_(sev), msg) : void())
#define ZeBackTrace_(sev, msg) \
  do { if (sev > Ze::Debug) ZeBackTrace__(ZeEVENT_(sev), msg); } while (0)

#else /* !ZEBUG */

#define ZeLOG_(sev, msg) ZeLOG__(ZeEVENT_(sev), msg)
#define ZeBackTrace_(sev, msg) ZeBackTrace__(ZeEVENT_(sev), msg)

#endif /* !ZEBUG */

#define ZeLOG(sev, msg) ZeLOG_(Ze:: sev, msg)
#define ZeBackTrace(sev, msg) ZeBackTrace_(Ze:: sev, msg)

#endif /* ZeLog_HPP */
