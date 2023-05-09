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

// platform primitives

#ifndef ZePlatform_HPP
#define ZePlatform_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZeLib_HPP
#include <zlib/ZeLib.hpp>
#endif

#ifndef _WIN32
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <netdb.h>
#include <syslog.h>
#else
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#include <zlib/ZuTraits.hpp>
#include <zlib/ZuFnName.hpp>
#include <zlib/ZuStringN.hpp>
#include <zlib/ZuPrint.hpp>

#include <zlib/ZmObject.hpp>
#include <zlib/ZmRef.hpp>
#include <zlib/ZmSingleton.hpp>
#include <zlib/ZmHeap.hpp>
#include <zlib/ZmStream.hpp>

#include <zlib/ZtDate.hpp>
#include <zlib/ZtString.hpp>
#include <zlib/ZtEnum.hpp>

// Ze::ErrNo is a regular native OS error code
//
// In addition to the native OS error code, need to handle the EAI_ error
// codes returned by getaddrinfo() (Windows - GetAddrInfo())
//
//   Windows	- EAI_ codes are #defined identically to equiv. system codes
//   Unix	- EAI_ codes are negative, all errno codes are positive
//
// Both types of platform also use a signed integer type for system errors,
// so both sets of codes can be stored in the same type; However,
// strerror() on Unix will not work with EAI_ codes - on Unix we
// need to check for < 0 explicitly and call gai_strerror()

#define ZeLog_BUFSIZ (32<<10)	// caps individual log message size to 32k

namespace Ze {
  ZtEnumValues("Ze", Debug, Info, Warning, Error, Fatal);
}

namespace Ze {

#ifndef _WIN32
using ErrNo = int;
inline constexpr ErrNo OK() { return 0; }

inline ErrNo errNo() { return errno; }
inline ErrNo sockErrNo() { return errno; }
inline const char *strerror(ErrNo e) {
  return e >= 0 ? ::strerror(e) : gai_strerror(e);
}
#else
using ErrNo = DWORD;					// <= sizeof(int)
inline constexpr ErrNo OK() { return ERROR_SUCCESS; }	// == 0

inline ErrNo errNo() { return GetLastError(); }
inline ErrNo sockErrNo() { return WSAGetLastError(); }
ZeExtern const char *strerror(ErrNo e);
#endif

}

class ZeError {
public:
  using ErrNo = Ze::ErrNo;

  ZeError() : m_errNo(Ze::OK()) { }

  ZeError(const ZeError &) = default;
  ZeError &operator =(const ZeError &) = default;
  ZeError(ZeError &&) = default;
  ZeError &operator =(ZeError &&) = default;

  ZeError(ErrNo e) : m_errNo(e) { }
  ZeError &operator =(ErrNo e) {
    m_errNo = e;
    return *this;
  }

  ErrNo errNo() const { return m_errNo; }
  const char *message() const { return Ze::strerror(m_errNo); }

  bool operator !() const { return m_errNo == Ze::OK(); }
  ZuOpBool

  template <typename S> void print(S &s) const { s << message(); }
  friend ZuPrintFn ZuPrintType(ZeError *);

  struct Traits : public ZuBaseTraits<ZeError> { enum { IsPOD = 1 }; };
  friend Traits ZuTraitsType(ZeError *);

private:
  ErrNo		m_errNo;
};

inline ZeError Ze_OK() { return ZeError{}; }
#define ZeOK Ze_OK()

inline ZeError Ze_LastError() { return ZeError{Ze::errNo()}; }
#define ZeLastError Ze_LastError()

inline ZeError Ze_LastSockError() { return ZeError{Ze::sockErrNo()}; }
#define ZeLastSockError Ze_LastSockError()

struct ZeEvent {
  using ThreadID = Zm::ThreadID;

  ZmTime	time;
  ThreadID	tid;
  int		severity;
  const char	*filename;
  int		lineNumber;
  const char	*function;

  ZeEvent(
      int severity_,
      const char *filename_, int lineNumber_,
      const char *function_) :
    time{ZmTime::Now}, tid{Zm::getTID()},
    severity{severity_},
    filename{filename_}, lineNumber{lineNumber_},
    function{function_} { }
};

namespace Ze {

ZeExtern ZuString severity(unsigned i);
ZeExtern ZuString filename(ZuString s);
ZeExtern ZuString function(ZuString s);

}

#define ZeEVENT_(sev) ZeEvent{sev, __FILE__, __LINE__, ZuFnName}
#define ZeEVENT(sev) ZeEVENT_(Ze:: sev)

#endif /* ZePlatform_HPP */
