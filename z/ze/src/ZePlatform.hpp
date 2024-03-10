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

// error handling - platform primitives

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

// normalized severity levels
namespace Ze {
  ZtEnumValues(Ze, Debug, Info, Warning, Error, Fatal);
}

// normalized OS error number
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

// OS error number wrapper
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

// encapsulates time, thread ID, severity, file name, line number, function
struct ZeEvent_ {
  using ThreadID = Zm::ThreadID;

  ZmTime	time;
  ThreadID	tid;
  int		severity;
  const char	*fileName;
  int		lineNumber;
  const char	*function;

  ZeEvent_(
      int severity_,
      const char *fileName_, int lineNumber_,
      const char *function_) :
    time{ZmTime::Now}, tid{Zm::getTID()},
    severity{severity_},
    fileName{fileName_}, lineNumber{lineNumber_},
    function{function_} { }
};

// log buffer
using ZeLogBuf = ZuStringN<ZeLog_BUFSIZ>;

// event with lambda message - [...](ZeLogBuf &buf) { buf << ... }
template <typename L>
struct ZeLambdaEvent : public ZeEvent_ {
  L	l;

  template <typename L_>
  ZeLambdaEvent(
      int severity_,
      const char *fileName_, int lineNumber_,
      const char *function_, L_ &&l_) :
    ZeEvent_(severity_, fileName_, lineNumber_, function_),
    l{ZuFwd<L_>(l_)} { }
};
template <typename L>
ZeLambdaEvent(int, const char *, int, const char *, L) ->
  ZeLambdaEvent<ZuDecay<L>>;

#include <zlib/ZmDemangle.hpp>
// convert string/printable to lambda
namespace ZeMsg_ {
template <typename U> struct IsLiteral_ : public ZuBool<
    ZuTraits<U>::IsArray &&
    ZuTraits<U>::IsPrimitive && ZuTraits<U>::IsCString &&
    ZuConversion<typename ZuTraits<U>::Elem, const char>::Same> { };
template <typename U> struct IsLiteral : public IsLiteral_<ZuDecay<U>> { };
template <typename U, typename R = void>
using MatchLiteral = ZuIfT<IsLiteral<U>{}, R>;

template <typename U> struct IsPrint_ : public ZuBool<
    !IsLiteral_<U>{} && (ZuTraits<U>::IsString || ZuPrint<U>::OK)> { };
template <typename U> struct IsPrint : public IsPrint_<ZuDecay<U>> { };
template <typename U, typename R = void>
using MatchPrint = ZuIfT<IsPrint<U>{}, R>;

template <typename U> struct IsOther_ :
  public ZuBool<!IsLiteral_<U>{} && !IsPrint_<U>{}> { };
template <typename U> struct IsOther : public IsOther_<ZuDecay<U>> { };
template <typename U, typename R = void>
using MatchOther = ZuIfT<IsOther<U>{}, R>;

template <typename Msg>
inline decltype(auto) fn(Msg &&msg, MatchOther<Msg> *_ = nullptr) {
  return ZuFwd<Msg>(msg);
}
template <typename Msg>
inline auto fn(Msg &&msg, MatchLiteral<Msg> *_ = nullptr) {
  return [msg = static_cast<const char *>(msg)](ZeLogBuf &s) mutable {
    s << msg;
  };
}
template <typename Msg>
inline auto fn(Msg &&msg, MatchPrint<Msg> *_ = nullptr) {
  return [msg = ZuFwd<Msg>(msg)](ZeLogBuf &s) mutable {
    s << ZuMv(msg);
  };
}
} // ZeMsg_

// make a lambda event
template <typename Msg>
auto ZeMkLambdaEvent__(
    int severity_,
    const char *fileName_, int lineNumber_,
    const char *function_, Msg &&msg) {
  return ZeLambdaEvent(
      severity_, fileName_, lineNumber_, function_,
      ZeMsg_::fn(ZuFwd<Msg>(msg)));
}
#define ZeMkLambdaEvent_(sev, msg) \
  ZeMkLambdaEvent__(sev, __FILE__, __LINE__, ZuFnName, msg)
#define ZeMkLambdaEvent(sev, msg) ZeMkEvent_(Ze:: sev, msg)

// non-polymorphic message
using ZeMsgFn = ZmFn<ZeLogBuf &>;

// non-polymorphic event
using ZeEvent = ZeLambdaEvent<ZeMsgFn>;

// make a non-polymorphic ZeEvent
template <typename Msg>
ZeEvent ZeMkEvent__(
    int severity_,
    const char *fileName_, int lineNumber_,
    const char *function_, Msg &&msg) {
  return ZeEvent(
      severity_, fileName_, lineNumber_, function_,
      ZeMsg_::fn(ZuFwd<Msg>(msg)));
}
#define ZeMkEvent_(sev, msg) \
  ZeMkEvent__(sev, __FILE__, __LINE__, ZuFnName, msg)
#define ZeMkEvent(sev, msg) ZeMkEvent_(Ze:: sev, msg)

namespace Ze {

ZeExtern ZuString severity(unsigned i);
ZeExtern ZuString fileName(ZuString s);
ZeExtern ZuString function(ZuString s);

}

#endif /* ZePlatform_HPP */
