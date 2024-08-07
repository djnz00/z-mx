//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// error handling - platform primitives

#ifndef ZePlatform_HH
#define ZePlatform_HH

#ifndef ZeLib_HH
#include <zlib/ZeLib.hh>
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

#include <zlib/ZuTraits.hh>
#include <zlib/ZuFnName.hh>
#include <zlib/ZuStringN.hh>
#include <zlib/ZuPrint.hh>
#include <zlib/ZuDateTime.hh>

#include <zlib/ZmObject.hh>
#include <zlib/ZmRef.hh>
#include <zlib/ZmSingleton.hh>
#include <zlib/ZmHeap.hh>
#include <zlib/ZuVStream.hh>
#include <zlib/ZmTime.hh>
#include <zlib/ZmAlloc.hh>

#include <zlib/ZtString.hh>
#include <zlib/ZtEnum.hh>

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
  ZtEnumValues(Ze, int8_t, Debug, Info, Warning, Error, Fatal);
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

// event time, thread ID, severity, file name, line number, function
struct ZeEventInfo {
  using ThreadID = Zm::ThreadID;

  ZuTime	time;
  ThreadID	tid = 0;
  int		severity = -1;	// Ze:: Debug, Info, Warning, Error, Fatal
  const char	*file = nullptr;
  int		line = -1;
  const char	*function = nullptr;

  ZeEventInfo() = default;

  ZeEventInfo(
      int severity_,
      const char *file_, int line_,
      const char *function_) :
    time{Zm::now()}, tid{Zm::getTID()},
    severity{severity_},
    file{file_}, line{line_},
    function{function_} { }

  ZeEventInfo(const ZeEventInfo &) = default;
  ZeEventInfo &operator =(const ZeEventInfo &) = default;

  ZeEventInfo(ZeEventInfo &&) = default;
  ZeEventInfo &operator =(ZeEventInfo &&) = default;

  ~ZeEventInfo() = default;

  bool operator !() const { return !*time; }
  ZuOpBool
};

// log buffer
// - many output streams (cout, cerr, etc.) interleave stream operator calls
//   during concurrent fan-in - the log buffer serves as both a consistent
//   interface type and to reduce the risk of interleaved output
using ZeLogBuf = ZuStringN<ZeLog_BUFSIZ>;

// message as function delegate
using ZeMsgFn = ZmFn<void(ZeLogBuf &, const ZeEventInfo &)>;

// event base class
struct ZeAnyEvent : public ZeEventInfo {
  ZeAnyEvent(
      int severity_,
      const char *file_, int line_,
      const char *function_) :
    ZeEventInfo(severity_, file_, line_, function_) { }

  virtual ZeMsgFn fn() const = 0;

  ZeAnyEvent() = default;

  ZeAnyEvent(const ZeAnyEvent &) = default;
  ZeAnyEvent &operator =(const ZeAnyEvent &) = default;

  ZeAnyEvent(ZeAnyEvent &&) = default;
  ZeAnyEvent &operator =(ZeAnyEvent &&) = default;

protected:
  ~ZeAnyEvent() = default;

public:
  template <typename S> void print(S &s) const {
    auto buf = ZmAlloc(ZeLogBuf, 1);
    new (&buf[0]) ZeLogBuf{};
    fn()(buf[0], *this);
    s << buf[0];
  }
  friend ZuPrintFn ZuPrintType(ZeAnyEvent *);
};

// event enriched with lambda message - [...](auto &s) { s << ... }
template <typename L>
struct ZeEvent final : public ZeAnyEvent {
  mutable L	l;

  template <typename L_>
  ZeEvent(
      int severity_,
      const char *file_, int line_,
      const char *function_, L_ &&l_) :
    ZeAnyEvent(severity_, file_, line_, function_),
    l{ZuFwd<L_>(l_)} { }

  template <typename S, typename L_ = L>
  friend decltype(
      ZuDeclVal<L_ &>()(ZuDeclVal<S &>()),
      ZuDeclVal<S &>())
  operator <<(S &s, const ZeEvent &e) { e.l(s); return s; }
  template <typename S, typename L_ = L>
  friend decltype(
      ZuDeclVal<L_ &>()(ZuDeclVal<S &>(), ZuDeclVal<const ZeEventInfo &>()),
      ZuDeclVal<S &>())
  operator <<(S &s, const ZeEvent &e) { e.l(s, e); return s; }

  template <typename L_ = L>
  decltype(ZuDeclVal<L_ &>()(
	ZuDeclVal<ZeLogBuf &>()),
      ZeMsgFn())
  fn_() const {
    return {[l_ = ZuMv(l)](auto &s, const auto &) mutable { l_(s); }};
  }
  template <typename L_ = L>
  decltype(ZuDeclVal<L_ &>()(
	ZuDeclVal<ZeLogBuf &>(),
	ZuDeclVal<const ZeEventInfo &>()),
      ZeMsgFn())
  fn_() const {
    return {ZuMv(l)};
  }
  ZeMsgFn fn() const { return fn_(); }

  ZeEvent() = delete;

  ZeEvent(const ZeEvent &) = delete;
  ZeEvent &operator =(const ZeEvent &) = delete;

  ZeEvent(ZeEvent &&) = default;
  ZeEvent &operator =(ZeEvent &&) = default;

  ~ZeEvent() = default;
};

// monomorphic event
template <>
struct ZeEvent<ZeMsgFn> final : public ZeAnyEvent {
  using L = ZeMsgFn;

  mutable L	l;

  template <
    typename L_,
    decltype(ZuDeclVal<L_ &>()(ZuDeclVal<ZeLogBuf &>()), int()) = 0>
  ZeEvent(
      int severity_,
      const char *file_, int line_,
      const char *function_, L_ l_) :
    ZeAnyEvent(severity_, file_, line_, function_),
    l{[l_ = ZuMv(l_)](auto &s, const auto &) mutable { l_(s); }} { }
  template <
    typename L_,
    decltype(ZuDeclVal<L_ &>()(
	ZuDeclVal<ZeLogBuf &>(),
	ZuDeclVal<const ZeEventInfo &>()), int()) = 0>
  ZeEvent(
      int severity_,
      const char *file_, int line_,
      const char *function_, L_ l_) :
    ZeAnyEvent(severity_, file_, line_, function_),
    l{ZuMv(l_)} { }

  ZeEvent() = default;

  ZeEvent(const ZeEvent &) = delete;
  ZeEvent &operator =(const ZeEvent &) = delete;

  ZeEvent(ZeEvent &&) = default;
  ZeEvent &operator =(ZeEvent &&) = default;

  ~ZeEvent() = default;

  ZeMsgFn fn() const { return {ZuMv(l)}; }

  ZeEvent(const ZeAnyEvent &e) : ZeAnyEvent{e}, l{e.fn()} { };
  ZeEvent &operator =(const ZeAnyEvent &e) {
    this->~ZeEvent();
    new (this) ZeEvent{e};
    return *this;
  }

  template <
    typename L_,
    decltype(ZuDeclVal<L_ &>()(ZuDeclVal<ZeLogBuf &>()), int()) = 0>
  ZeEvent(ZeEvent<L_> &&e) :
      ZeAnyEvent{static_cast<ZeAnyEvent &&>(e)},
      l{[l_ = ZuMv(e.l)](auto &s, auto) mutable { l_(s); }} { }
  template <
    typename L_,
    decltype(ZuDeclVal<L_ &>()(
	ZuDeclVal<ZeLogBuf &>(),
	ZuDeclVal<const ZeEventInfo &>()), int()) = 0>
  ZeEvent(ZeEvent<L_> &&e) :
      ZeAnyEvent{static_cast<ZeAnyEvent &&>(e)},
      l{ZuMv(e.l)} { }

  template <typename L_>
  ZeEvent &operator =(ZeEvent<L> &&e) {
    this->~ZeEvent();
    new (this) ZeEvent{ZuMv(e)};
    return *this;
  }
};
using ZeVEvent = ZeEvent<ZeMsgFn>;

template <typename L>
ZeEvent(int, const char *, int, const char *, L) -> ZeEvent<ZuDecay<L>>;

// convert string/printable to lambda
namespace ZeMsg_ {
template <typename U> struct IsLiteral_ : public ZuBool<
    ZuIsExact<U, const char (&)[sizeof(U)]>{}> { };
template <typename U> struct IsLiteral : public IsLiteral_<U> { };
template <typename U, typename R = void>
using MatchLiteral = ZuIfT<IsLiteral<U>{}, R>;

template <typename U> struct IsPrint_ : public ZuBool<
    !IsLiteral_<U>{} && (ZuTraits<U>::IsString || ZuPrint<U>::OK)> { };
template <typename U> struct IsPrint : public IsPrint_<U> { };
template <typename U, typename R = void>
using MatchPrint = ZuIfT<IsPrint<U>{}, R>;

template <typename U> struct IsOther_ :
  public ZuBool<!IsLiteral_<U>{} && !IsPrint_<U>{}> { };
template <typename U> struct IsOther : public IsOther_<ZuDecay<U>> { };
template <typename U, typename R = void>
using MatchOther = ZuIfT<IsOther<U>{}, R>;

template <typename Msg, decltype(MatchOther<Msg>(), int()) = 0>
inline decltype(auto) fn(Msg &&msg) {
  return ZuFwd<Msg>(msg);
}
template <typename Msg, decltype(MatchLiteral<Msg>(), int()) = 0>
inline auto fn(Msg &&msg) {
  return [msg = static_cast<const char *>(msg)](auto &s) mutable { s << msg; };
}
template <typename Msg, decltype(MatchPrint<Msg>(), int()) = 0>
inline auto fn(Msg &&msg) {
  return [msg = ZuFwd<Msg>(msg)](auto &s) mutable { s << ZuMv(msg); };
}
} // ZeMsg_

// make an event
template <typename Msg>
auto ZeMkEvent(
    int severity_,
    const char *file_, int line_,
    const char *function_, Msg &&msg) {
  return ZeEvent(
      severity_, file_, line_, function_,
      ZeMsg_::fn(ZuFwd<Msg>(msg)));
}
#define ZeEVENT_(sev, msg) \
  ZeMkEvent(sev, __FILE__, __LINE__, ZuFnName, msg)
#define ZeEVENT(sev, msg) ZeEVENT_(Ze:: sev, msg)

// make a non-polymorphic ZeEvent
template <typename Msg>
ZeVEvent ZeMkVEvent(
    int severity_,
    const char *file_, int line_,
    const char *function_, Msg &&msg) {
  return ZeVEvent(
      severity_, file_, line_, function_,
      ZeMsg_::fn(ZuFwd<Msg>(msg)));
}
#define ZeVEVENT_(sev, msg) \
  ZeMkVEvent(sev, __FILE__, __LINE__, ZuFnName, msg)
#define ZeVEVENT(sev, msg) ZeVEVENT_(Ze:: sev, msg)

namespace Ze {

ZeExtern ZuString severity(unsigned i);
ZeExtern ZuString file(ZuString s);
ZeExtern ZuString function(ZuString s);

}

#endif /* ZePlatform_HH */
