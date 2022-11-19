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

// platform-specific

#include <zlib/ZuUTF.hpp>

#include <zlib/ZmSingleton.hpp>
#include <zlib/ZmLock.hpp>
#include <zlib/ZmGuard.hpp>

#include <zlib/ZtPlatform.hpp>
#include <zlib/ZtString.hpp>
#include <zlib/ZtRegex.hpp>

#include <zlib/ZePlatform.hpp>

ZuString Ze::severity(unsigned i)
{
  static const char * const name[] = {
    "DEBUG", "INFO", "WARNING", "ERROR", "FATAL"
  };
  constexpr static unsigned namelen[] = { 5, 4, 7, 5, 5 };

  return i > 4 ? ZuString("UNKNOWN", 7) : ZuString(name[i], namelen[i]);
}

ZuString Ze::filename(ZuString s)
{
  ZtRegex::Captures c;
#ifndef _WIN32
  const auto &r = ZtREGEX("([^/]*)$");
#else
  const auto &r = ZtREGEX("([^:/\\]*)$");
#endif
  if (r.m(s, c) < 2) return s;
  return c[2];
}

ZuString Ze::function(ZuString s)
{
  ZtRegex::Captures c;
  if (ZtREGEX("([a-zA-Z_][a-zA-Z_0-9:]*)\(").m(s, c) < 2) return s;
  return c[2];
}

#ifndef _WIN32

struct ZePlatform_Syslog;
template <> struct ZmCleanup<ZePlatform_Syslog> {
  enum { Level = ZmCleanupLevel::Platform };
};
struct ZePlatform_Syslog {
public:
  ZePlatform_Syslog() { openlog("", 0, LOG_USER); }
  ~ZePlatform_Syslog() { closelog(); }

  void init(const char *program, int facility = LOG_USER);

  int facility() { return m_facility; }

private:
  ZmLock	m_lock;
  int		m_facility;
};

void ZePlatform_Syslog::init(const char *program, int facility)
{
  ZmGuard<ZmLock> guard(m_lock);

  closelog();
  openlog(program, 0, m_facility = facility);
}

static ZePlatform_Syslog *syslogger() {
  return ZmSingleton<ZePlatform_Syslog>::instance();
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

void Ze::sysloginit(const char *program, const char *facility)
{
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
    for (int i = 0; name = names[i]; i++) {
      if (!strcmp(facility, name)) {
	syslogger()->init(program, values[i]);
	return;
      }
    }
  syslogger()->init(program, LOG_USER);
}

struct ZePlatform_SyslogBuf;
template <> struct ZmCleanup<ZePlatform_SyslogBuf> {
  enum { Level = ZmCleanupLevel::Platform };
};
struct ZePlatform_SyslogBuf : public ZmObject {
  ZuStringN<ZeLog_BUFSIZ>	s;
};
static ZePlatform_SyslogBuf *syslogBuf()
{
  ZePlatform_SyslogBuf *buf = ZmSpecific<ZePlatform_SyslogBuf>::instance();
  buf->s.null();
  return buf;
}

void Ze::syslog(ZeEvent *e)
{
  ZePlatform_SyslogBuf *buf = syslogBuf();

  // buf->s << ZuBoxed(e->tid()) << " - ";
  if (e->severity() == Ze::Debug || e->severity() == Ze::Fatal)
    buf->s <<
      '\"' << Ze::filename(e->filename()) << "\":" <<
      ZuBoxed(e->lineNumber()) << ' ';
  buf->s <<
    Ze::function(e->function()) << ' ' <<
    e->message();

  ::syslog(syslogger()->facility() | sysloglevel(e->severity()),
      "%.*s", buf->s.length(), buf->s.data());
}

#else

static int eventlogtype(int i) {
  static const int types[] = {
    EVENTLOG_SUCCESS,		// Debug
    EVENTLOG_INFORMATION_TYPE,	// Info
    EVENTLOG_WARNING_TYPE,	// Warning
    EVENTLOG_ERROR_TYPE,	// Error
    EVENTLOG_ERROR_TYPE		// Fatal
  };

  return (i < 0 || i > 4) ? EVENTLOG_WARNING_TYPE : types[i];
}

#define Ze_NTFS_MAX_PATH	32768	// MAX_PATH is 260 and deprecated

struct ZePlatform_EventLogger;
template <> struct ZmCleanup<ZePlatform_EventLogger> {
  enum { Level = ZmCleanupLevel::Platform };
};
struct ZePlatform_EventLogger {
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

  HANDLE	handle;
  ZtString	program;
};
static ZePlatform_EventLogger *eventLogger()
{
  return ZmSingleton<ZePlatform_EventLogger>::instance();
}

void Ze::sysloginit(const char *program, const char *facility)
{
  eventLogger()->program = program;
}

struct ZePlatform_SyslogBuf;
template <> struct ZmCleanup<ZePlatform_SyslogBuf> {
  enum { Level = ZmCleanupLevel::Platform };
};
struct ZePlatform_SyslogBuf : public ZmObject {
  ZuWStringN<ZeLog_BUFSIZ / 2>	w;
  ZuStringN<ZeLog_BUFSIZ>	s;
};
static ZePlatform_SyslogBuf *syslogBuf()
{
  ZePlatform_SyslogBuf *buf = ZmSpecific<ZePlatform_SyslogBuf>::instance();
  buf->w.null();
  buf->s.null();
  return buf;
}

void Ze::syslog(ZeEvent *e)
{
  ZePlatform_EventLogger *logger = eventLogger();
  ZePlatform_SyslogBuf *buf = syslogBuf();

  buf->s <<
    logger->program << ' ' <<
    ZuBoxed(e->tid()) << " - ";
  if (e->severity() == Ze::Debug || e->severity() == Ze::Fatal)
    buf->s <<
      '\"' << Ze::filename(e->filename()) << "\":" <<
      ZuBoxed(e->lineNumber()) << ' ';
  buf->s <<
    Ze::function(e->function()) << ' ' <<
    e->message();

  buf->w.length(ZuUTF<wchar_t, char>::cvt(
	ZuArray<wchar_t>(buf->w.data(), buf->w.size() - 1), buf->s));

  const wchar_t *w = buf->w.data();

  ReportEvent(
    logger->handle, eventlogtype(e->severity()), 0, 512, 0, 1, 0, &w, 0);
}

constexpr static struct {
  Ze::ErrNo	code;
  const char		*msg;
} ZePlatform_WSAErrors_[] = {
{ WSAEINTR,		  "Interrupted system call" },
{ WSAEBADF,		  "Bad file number" },
{ WSAEACCES,		  "Permission denied" },
{ WSAEFAULT,		  "Bad address" },
{ WSAEINVAL,		  "Invalid argument" },
{ WSAEMFILE,		  "Too many open sockets" },
{ WSAEWOULDBLOCK,	  "Operation would block" },
{ WSAEINPROGRESS,	  "Operation now in progress" },
{ WSAEALREADY,		  "Operation already in progress" },
{ WSAENOTSOCK,		  "Socket operation on non-socket" },
{ WSAEDESTADDRREQ,	  "Destination address required" },
{ WSAEMSGSIZE,		  "Message too long" },
{ WSAEPROTOTYPE,	  "Protocol wrong type for socket" },
{ WSAENOPROTOOPT,	  "Bad protocol option" },
{ WSAEPROTONOSUPPORT,	  "Protocol not supported" },
{ WSAESOCKTNOSUPPORT,	  "Socket type not supported" },
{ WSAEOPNOTSUPP,	  "Operation not supported on socket" },
{ WSAEPFNOSUPPORT,	  "Protocol family not supported" },
{ WSAEAFNOSUPPORT,	  "Address family not supported" },
{ WSAEADDRINUSE,	  "Address already in use" },
{ WSAEADDRNOTAVAIL,	  "Can't assign requested address" },
{ WSAENETDOWN,		  "Network is down" },
{ WSAENETUNREACH,	  "Network is unreachable" },
{ WSAENETRESET,		  "Net connection reset" },
{ WSAECONNABORTED,	  "Software caused connection abort" },
{ WSAECONNRESET,	  "Connection reset by peer" },
{ WSAENOBUFS,		  "No buffer space available" },
{ WSAEISCONN,		  "Socket is already connected" },
{ WSAENOTCONN,		  "Socket is not connected" },
{ WSAESHUTDOWN,		  "Can't send after socket shutdown" },
{ WSAETOOMANYREFS,	  "Too many references, can't splice" },
{ WSAETIMEDOUT,		  "Connection timed out" },
{ WSAECONNREFUSED,	  "Connection refused" },
{ WSAELOOP,		  "Too many levels of symbolic links" },
{ WSAENAMETOOLONG,	  "File name too long" },
{ WSAEHOSTDOWN,		  "Host is down" },
{ WSAEHOSTUNREACH,	  "No route to host" },
{ WSAENOTEMPTY,		  "Directory not empty" },
{ WSAEPROCLIM,		  "Too many processes" },
{ WSAEUSERS,		  "Too many users" },
{ WSAEDQUOT,		  "Disc quota exceeded" },
{ WSAESTALE,		  "Stale NFS file handle" },
{ WSAEREMOTE,		  "Too many levels of remote in path" },
{ WSASYSNOTREADY,	  "Network system is unavailable" },
{ WSAVERNOTSUPPORTED,	  "Winsock version out of range" },
{ WSANOTINITIALISED,	  "WSAStartup not yet called" },
{ WSAEDISCON,		  "Graceful shutdown in progress" },
{ WSAHOST_NOT_FOUND,	  "Host not found" },
{ WSANO_DATA,		  "No host data of that type was found" },
{ WSAENOMORE,		  "No more results" },
{ WSAECANCELLED,	  "Call cancelled" },
{ WSAEINVALIDPROCTABLE,	  "Invalid procedure call table" },
{ WSAEINVALIDPROVIDER,	  "Invalid requested service provider" },
{ WSAEPROVIDERFAILEDINIT, "Could not load or initialize service provider" },
{ WSASYSCALLFAILURE,	  "Critical system call failure" },
{ WSASERVICE_NOT_FOUND,	  "No such service known" },
{ WSATYPE_NOT_FOUND,	  "Class not found" },
{ WSA_E_NO_MORE,	  "No more results" },
{ WSA_E_CANCELLED,	  "Call cancelled" },
{ WSAEREFUSED,		  "Database query refused" },
{ WSATRY_AGAIN,		  "Transient error - retry" },
{ WSANO_RECOVERY,	  "Unrecoverable database query error" },
{ 0,			  NULL }
};
class ZePlatform_WSAErrors {
public:
  using Hash =
    ZmLHashKV<DWORD, const char *,
      ZmLHashStatic<6,
	ZmLHashLock<ZmNoLock> > >;

  ZePlatform_WSAErrors() {
    m_hash = new Hash();
    const char *msg;
    for (unsigned i = 0; msg = ZePlatform_WSAErrors_[i].msg; i++)
      m_hash->add(ZePlatform_WSAErrors_[i].code, msg);
  }
  ZuInline const char *lookup(DWORD i) {
    return m_hash->findVal(i);
  }

private:
  ZmRef<Hash>	m_hash;
};

struct ZePlatform_FMBuf;
template <> struct ZmCleanup<ZePlatform_FMBuf> {
  enum { Level = ZmCleanupLevel::Platform };
};
struct ZePlatform_FMBuf : public ZmObject {
  ZuWStringN<ZeLog_BUFSIZ / 2>	w;
  ZuStringN<ZeLog_BUFSIZ>	s;
};
static ZePlatform_FMBuf *fmBuf()
{
  ZePlatform_FMBuf *buf = ZmSpecific<ZePlatform_FMBuf>::instance();
  buf->w.null();
  buf->s.null();
  return buf;
}
const char *Ze::strerror(ErrNo e)
{
  const char *msg;

  if (msg = ZmSingleton<ZePlatform_WSAErrors>::instance()->lookup(e))
    return msg;

  if (e == ERROR_DUP_NAME)
    return "Duplicate network name or too many network end-points";

  ZePlatform_FMBuf *buf = fmBuf();

  DWORD n = FormatMessage(
	FORMAT_MESSAGE_IGNORE_INSERTS |
	FORMAT_MESSAGE_FROM_SYSTEM,
	0, e, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
	buf->w.data(), buf->w.size(), 0);

  if (!n || !buf->w[0]) return "";

  buf->w.length(n);

  buf->s.length(ZuUTF<char, wchar_t>::cvt(
	ZuArray<char>(buf->s.data(), buf->s.size() - 1), buf->w));

  // FormatMessage() often returns verbose junk; clean it up
  {
    char *ptr = buf->s.data();
    char *end = ptr + buf->s.length();
    int c;
    while (ptr < end) {
      if (ZuUnlikely(
	    (c = *ptr++) == ' ' || c == '\t' || c == '\r' || c == '\n')) {
	char *ws = --ptr;
	while (ZuLikely(ws < end) && ZuUnlikely(
	      (c = *++ws) == ' ' || c == '\t' || c == '\r' || c == '\n'));
	*ptr++ = ' ';
	if (ZuUnlikely(ws > ptr)) {
	  memmove(ptr, ws, end - ws);
	  end -= (ws - ptr);
	}
      }
    }
    while (ZuUnlikely(
	(c = *--end) == ' ' || c == '\t' || c == '\r' || c == '\n' ||
	c == '.'));
    buf->s.length(++end - buf->s.data());
  }

  return buf->s.data();
}

#endif /* _WIN32 */
