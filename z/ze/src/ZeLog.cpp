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

#include <zlib/ZuBox.hpp>

#include <zlib/ZmPlatform.hpp>
#include <zlib/ZmSingleton.hpp>

#include <zlib/ZtArray.hpp>
#include <zlib/ZtDate.hpp>
#include <zlib/ZtString.hpp>
#include <zlib/ZtRegex.hpp>

#include <zlib/ZeLog.hpp>

ZeLog::ZeLog() : m_level{1} {
  init_();
}

ZeLog *ZeLog::instance()
{
  return ZmSingleton<ZeLog>::instance();
}

#ifndef _WIN32

struct ZePlatform_Syslogger {
  using Lock = ZmLock;
  using Guard = ZmGuard<Lock>;

public:
  ZePlatform_Syslogger() { openlog("", 0, LOG_USER); }
  ~ZePlatform_Syslogger() { closelog(); }

  void init(const char *program, int facility = LOG_USER) {
    Guard guard(m_lock);

    closelog();
    openlog(program, 0, m_facility = facility);
  }

  int facility() { return m_facility; }

  friend ZuConstant<ZmCleanup::Platform> ZmCleanupLevel(ZePlatform_Syslogger *);

private:
  ZmLock	m_lock;
    int		  m_facility;
};

static ZePlatform_Syslogger *syslogger() {
  return ZmSingleton<ZePlatform_Syslogger>::instance();
}

static int sysloglevel(int i) {
  static const int levels[] = {
    LOG_DEBUG,		// Debug
    LOG_INFO,		// Info
    LOG_WARNING,	// Warning
    LOG_ERR,		// Error
    LOG_CRIT		// Fatal
  };

  return (i < 0 || i > 4) ? LOG_ERR : levels[i];
}

#else /* !_WIN32 */

#define Ze_NTFS_MAX_PATH	32768	// MAX_PATH is 260 and deprecated

static int eventlogtype(int i) {
  static const int types[] = {
    EVENTLOG_SUCCESS,		// Debug
    EVENTLOG_INFORMATION_TYPE,	// Info
    EVENTLOG_WARNING_TYPE,	// Warning
    EVENTLOG_ERROR_TYPE,	// Error
    EVENTLOG_ERROR_TYPE		// Fatal
  };
  enum { N = sizeof(types) / sizeof(types[0]) };

  return (i < 0 || i >= N) ? EVENTLOG_WARNING_TYPE : types[i];
}

struct ZePlatform_EventLogger {
  HANDLE			handle = INVALID_HANDLE_VALUE;
  ZtString			program;
  ZuWStringN<ZeLog_BUFSIZ / 2>	buf;

  ZePlatform_EventLogger() {
    handle = RegisterEventSource(0, L"EventSystem");

    ZtWString path_;

    path_.size(Ze_NTFS_MAX_PATH);
    GetModuleFileName(0, path_.data(), Ze_NTFS_MAX_PATH);
    path_.calcLength();

    ZtString path(path_);

    program = "Application";
    try {
      ZtRegex::Captures c;
      if (ZtREGEX("[^\\]*$").m(path, c, 0)) program = c[1];
    } catch (...) { }
  }
  ~ZePlatform_EventLogger() {
    DeregisterEventSource(handle);
  }

  void report(const ZeEvent &e, const ZeLog::Buf &buf) {
    wbuf.null();
    wbuf.length(ZuUTF<wchar_t, char>::cvt(
	  ZuArray<wchar_t>(wbuf.data(), wbuf.size() - 1), buf));
    const wchar_t *w = buf.data();

    ReportEvent(
      handle, eventlogtype(e.severity), 0, 512, 0, 1, 0, &w, 0);
  }

  friend ZuConstant<ZmCleanup::Platform>
    ZmCleanupLevel(ZePlatform_EventLogger *);
};

static ZePlatform_EventLogger *eventLogger()
{
  return ZmSingleton<ZePlatform_EventLogger>::instance();
}

#endif /* !_WIN32 */

#ifdef linux
extern "C" {
  extern char *program_invocation_short_name;
}
#endif

void ZeLog::init_()
{
#ifdef linux
  init_(program_invocation_short_name, "user");
#else
  init_("ZeLog", "user");
#endif
}

void ZeLog::init_(const char *program)
{
  init_(program, "user");
}

void ZeLog::init_(const char *program, const char *facility)
{
  m_program = program;
  m_facility = facility;
#ifndef _WIN32
  static const char * const names[] = {
    "daemon",
    "local0", "local1", "local2", "local3",
    "local4", "local5", "local6", "local7", 0
  };
  static const int values[] = {
    LOG_DAEMON,
    LOG_LOCAL0, LOG_LOCAL1, LOG_LOCAL2, LOG_LOCAL3,
    LOG_LOCAL4, LOG_LOCAL5, LOG_LOCAL6, LOG_LOCAL7
  };
  const char *name;

  if (facility)
    for (unsigned i = 0; name = names[i]; i++) {
      if (!strcmp(facility, name)) {
	syslogger()->init(program, values[i]);
	return;
      }
    }
  syslogger()->init(program, LOG_USER);
#else
  eventLogger()->program = program;
#endif
}

void ZeLog::sink_(ZmRef<ZeSink> sink)
{
  Guard guard(m_lock);
  m_sink = sink;
}

void ZeLog::start_()
{
  Guard guard(m_lock);
  if (m_thread) return;
  start__();
}

void ZeLog::start__()
{
  m_ring.eof(false);
  m_thread = ZmThread{[this]() { work_(); },
      ZmThreadParams().name("log").priority(ZmThreadPriority::Low)};
}

void ZeLog::forked_()
{
  Guard guard(m_lock);
  start__();
}

void ZeLog::stop_()
{
  ZmThread thread;
  {
    Guard guard(m_lock);
    thread = m_thread;
    m_thread = {};
  }
  if (thread) return;
  m_ring.eof(true);
  thread.join();
}

void ZeLog::work_()
{
  for (;;) {
    if (void *ptr = m_ring.shift()) {
      m_ring.shift2(Fn::invoke(ptr, this));
    } else {
      if (m_ring.readStatus() == Zu::EndOfFile) break;
    }
  }
}

ZmRef<ZeSink> ZeLog::sink_()
{
  Guard guard(m_lock);
  if (ZuUnlikely(!m_sink)) {
#ifdef _WIN32
    m_sink = sysSink(); // on Windows, default to the event log
#else
    m_sink = fileSink(); // on Unix, default to stderr
#endif
  }
  return m_sink;
}

void ZeLog::log__(Fn &fn)
{
  unsigned size = fn.pushSize();
  void *ptr;
  if (ZuLikely(ptr = m_ring.push(size))) {
    fn.push(ptr);
    m_ring.push2(ptr, size);
  }
}

void ZeLog::age_()
{
  ZmRef<ZeSink> sink;
  {
    Guard guard(m_lock);
    sink = m_sink;
  }
  if (sink) sink->age();
}

void ZeSysSink::pre(ZeLogBuf &buf, const ZeEvent &e)
{
#ifndef _WIN32
  if (e.severity == Ze::Debug || e.severity == Ze::Fatal)
    buf << '\"' << Ze::filename(e.filename) << "\":" <<
      ZuBoxed(e.lineNumber) << ' ';
  buf << Ze::function(e.function);
#else
  ZePlatform_EventLogger *logger = eventLogger();
  buf <<
    logger->program << ' ' <<
    ZuBoxed(e.tid) << " - ";
  if (e.severity == Ze::Debug || e.severity == Ze::Fatal)
    buf <<
      '\"' << Ze::filename(e.filename) << "\":" <<
      ZuBoxed(e.lineNumber) << ' ';
  buf << Ze::function(e.function) << ' ' << *e;
#endif
}

void ZeSysSink::post(ZeLogBuf &buf, const ZeEvent &e)
{
  buf << '\n';

  {
    unsigned len = buf.length();

    if (buf[len - 1] != '\n') buf[len - 1] = '\n';
  }

#ifndef _WIN32
  ::syslog(syslogger()->facility() | sysloglevel(e.severity),
      "%.*s", buf.length(), buf.data());
#else
  eventLogger()->report(e, buf);
#endif
}

// suppress security warnings about fopen()
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4996)
#endif

void ZeFileSink::init()
{
  if (!m_path) m_path << ZeLog::program() << ".log";

  if (m_path != "&2") {
    age_();
    m_file = fopen(m_path, "w");
  }

  if (!m_file)
    m_file = stderr;
  else
    setvbuf(m_file, 0, _IOLBF, ZeLog_BUFSIZ);
}

ZeFileSink::~ZeFileSink()
{
  if (m_file) fclose(m_file);
}

void ZeFileSink::pre(ZeLogBuf &buf, const ZeEvent &e)
{
  ZtDate d{e.time};

  buf << d.print(m_dateFmt) << ' ' <<
    ZuBoxed(e.tid) << ' ' <<
    Ze::severity(e.severity) << ' ';
  if (e.severity == Ze::Debug || e.severity == Ze::Fatal)
    buf << '\"' << Ze::filename(e.filename) << "\":" <<
      ZuBoxed(e.lineNumber) << ' ';
  buf << Ze::function(e.function) << "() ";
}

void ZeFileSink::post(ZeLogBuf &buf, const ZeEvent &e)
{
  buf << '\n';

  unsigned len = buf.length();

  if (buf[len - 1] != '\n') buf[len - 1] = '\n';

  fwrite(buf.data(), 1, len, m_file);
  if (e.severity > Ze::Debug) fflush(m_file);
}

void ZeFileSink::age()
{
  Guard guard(m_lock);

  fclose(m_file);
  age_();
  m_file = fopen(m_path, "w");
}

void ZeFileSink::age_()
{
  unsigned size = m_path.length() + ZuBoxed(m_age).length() + 4;

  ZtString prevName_(size), nextName_(size), sideName_(size);
  ZtString *prevName = &prevName_;
  ZtString *nextName = &nextName_;
  ZtString *sideName = &sideName_;

  *prevName << m_path;
  bool last = false;
  unsigned i;
  for (i = 0; i < m_age && !last; i++) {
    nextName->length(0);
    *nextName << m_path << '.' << ZuBoxed(i + 1);
    sideName->length(0);
    *sideName << *nextName << '_';
    last = (::rename(*nextName, *sideName) < 0);
    ::rename(*prevName, *nextName);
    ZtString *oldName = prevName;
    prevName = sideName;
    sideName = oldName;
  }
  if (i == m_age) ::remove(*prevName);
}

void ZeDebugSink::init()
{
  m_path << ZeLog::program() << ".log." << ZuBoxed(Zm::getPID());

  if (m_path != "&2") m_file = fopen(m_path, "w");

  if (!m_file)
    m_file = stderr;
  else
    setvbuf(m_file, 0, _IOLBF, ZeLog_BUFSIZ);
}

ZeDebugSink::~ZeDebugSink()
{
  if (m_file) fclose(m_file);
}

void ZeDebugSink::pre(ZeLogBuf &buf, const ZeEvent &e)
{
  ZmTime d = e.time - m_started;

  buf <<
    '+' << ZuBoxed(d.dtime()).fmt<ZuFmt::FP<9>>() << ' ' <<
    ZuBoxed(e.tid) << ' ' <<
    Ze::severity(e.severity) << ' ';
  if (e.severity == Ze::Debug || e.severity == Ze::Fatal)
    buf <<
      '\"' << Ze::filename(e.filename) << "\":" <<
      ZuBoxed(e.lineNumber) << ' ';
  buf <<
    Ze::function(e.function) << "() ";
}

void ZeDebugSink::post(ZeLogBuf &buf, const ZeEvent &e)
{
  buf << '\n';

  unsigned len = buf.length();

  if (buf[len - 1] != '\n') buf[len - 1] = '\n';

  fwrite(buf.data(), 1, len, m_file);
  fflush(m_file);
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif
