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

void ZmCleanup<ZeLog>::final(ZeLog *log)
{
  try { log->stop_(); } catch (...) { }
}

ZeLog::ZeLog() : m_level(1) { }

ZeLog *ZeLog::instance()
{
  return ZmSingleton<ZeLog>::instance();
}

#ifdef linux
extern "C" {
  extern char *program_invocation_short_name;
}
#endif

ZuString ZeLog::program_() {
#ifdef linux
  if (ZuUnlikely(!m_program))
    m_program = program_invocation_short_name;
#endif
  return m_program;
}

void ZeLog::sink_(ZmRef<ZeSink> sink)
{
  Guard guard(m_lock);
  m_sink = sink;
}

void ZeLog::start_()
{
  Guard guard(m_lock);
  if (!!m_thread) return;
  start__();
}

void ZeLog::start__()
{
  m_thread = ZmThread(0,
      ZmFn<>::Member<&ZeLog::work_>::fn(this),
      ZmThreadParams().name("log").priority(ZmThreadPriority::Low));
  m_started.wait();
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
    m_thread = 0;
  }
  if (!thread) return;
  m_work.post();
  thread.join();
}

void ZeLog::work_()
{
  m_started.post();
  for (;;) {
    m_work.wait();
    ZmRef<ZeEvent> event;
    {
      Guard guard(m_lock);
      event = m_queue.shiftNode();
    }
    if (!event) return;
    log__(event);
  }
}

void ZeLog::log_(ZmRef<ZeEvent> e)
{
  if (!e) return;
  if ((int)e->severity() < m_level) return;
  {
    Guard guard(m_lock);
    m_queue.pushNode(ZuMv(e));
  }
  m_work.post();
}

void ZeLog::log__(ZeEvent *e)
{
  ZmRef<ZeSink> sink;
  {
    Guard guard(m_lock);
    sink = m_sink;
    if (ZuUnlikely(!sink)) {
#ifdef _WIN32
      sink = m_sink = sysSink();	// on Windows, default to the event log
#else
      sink = m_sink = fileSink();	// on Unix, default to stderr
#endif
    }
  }
  sink->log(e);
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

struct ZeLog_Buf;
template <> struct ZmCleanup<ZeLog_Buf> {
  enum { Level = ZmCleanupLevel::Platform };
};
struct ZeLog_Buf : public ZmObject {
  ZeLog_Buf() { dateFmt.pad(' '); }

  ZuStringN<ZeLog_BUFSIZ>	s;
  ZtDateFmt::CSV		dateFmt;
};
static ZeLog_Buf *logBuf()
{
  ZeLog_Buf *buf = ZmSpecific<ZeLog_Buf>::instance();
  buf->s.null();
  return buf;
}
static ZeLog_Buf *logBuf(int tzOffset)
{
  ZeLog_Buf *buf = ZmSpecific<ZeLog_Buf>::instance();
  buf->s.null();
  buf->dateFmt.offset(tzOffset);
  return buf;
}

void ZeFileSink::init()
{
  if (!m_filename) m_filename << ZeLog::program() << ".log";

  if (m_filename != "&2") {
    age_();
    m_file = fopen(m_filename, "w");
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

// suppress security warning about fopen()
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4996)
#endif

void ZeFileSink::log(ZeEvent *e)
{
  Guard guard(m_lock);

  ZeLog_Buf *buf = logBuf(m_tzOffset);

  ZtDate d{e->time()};

  buf->s <<
    d.print(buf->dateFmt) << ' ' <<
    ZuBoxed(e->tid()) << ' ' <<
    Ze::severity(e->severity()) << ' ';
  if (e->severity() == Ze::Debug || e->severity() == Ze::Fatal)
    buf->s <<
      '\"' << Ze::filename(e->filename()) << "\":" <<
      ZuBoxed(e->lineNumber()) << ' ';
  buf->s <<
    Ze::function(e->function()) << "() " <<
    e->message() << '\n';

  unsigned len = buf->s.length();

  if (buf->s[len - 1] != '\n') buf->s[len - 1] = '\n';

  fwrite(buf->s.data(), 1, len, m_file);
  if (e->severity() > Ze::Debug) fflush(m_file);
}

void ZeFileSink::age()
{
  Guard guard(m_lock);

  fclose(m_file);
  age_();
  m_file = fopen(m_filename, "w");
}

void ZeFileSink::age_()
{
  unsigned size = m_filename.length() + ZuBoxed(m_age).length() + 4;

  ZtString prevName_(size), nextName_(size), sideName_(size);
  ZtString *prevName = &prevName_;
  ZtString *nextName = &nextName_;
  ZtString *sideName = &sideName_;

  *prevName << m_filename;
  bool last = false;
  unsigned i;
  for (i = 0; i < m_age && !last; i++) {
    nextName->length(0);
    *nextName << m_filename << '.' << ZuBoxed(i + 1);
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
  m_filename << ZeLog::program() << ".log." << ZuBoxed(Zm::getPID());

  if (m_filename != "&2") m_file = fopen(m_filename, "w");

  if (!m_file)
    m_file = stderr;
  else
    setvbuf(m_file, 0, _IOLBF, ZeLog_BUFSIZ);
}

ZeDebugSink::~ZeDebugSink()
{
  if (m_file) fclose(m_file);
}

void ZeDebugSink::log(ZeEvent *e)
{
  ZeLog_Buf *buf = logBuf();

  ZmTime d = e->time() - m_started;

  buf->s <<
    '+' << ZuBoxed(d.dtime()).fmt<ZuFmt::FP<9>>() << ' ' <<
    ZuBoxed(e->tid()) << ' ' <<
    Ze::severity(e->severity()) << ' ';
  if (e->severity() == Ze::Debug || e->severity() == Ze::Fatal)
    buf->s <<
      '\"' << Ze::filename(e->filename()) << "\":" <<
      ZuBoxed(e->lineNumber()) << ' ';
  buf->s <<
    Ze::function(e->function()) << "() " <<
    e->message() << '\n';

  unsigned len = buf->s.length();

  if (buf->s[len - 1] != '\n') buf->s[len - 1] = '\n';

  fwrite(buf->s.data(), 1, len, m_file);
  fflush(m_file);
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif

void ZeSysSink::log(ZeEvent *e)
{
  Ze::syslog(e);
}
