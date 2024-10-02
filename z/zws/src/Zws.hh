//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Z websockets library

#ifndef Zws_HH
#define Zws_HH

#include <zlib/ZwsLib.hh>

#include <zlib/Ztls.hh>

namespace Zws {

template <typename App, typename Link> class Client;

template <
  typename App_,
  typename Impl_,
  typename IOBufAlloc_ = Ztls::IOBufAlloc<>>
class CliLink :
  public Ztls::CliLink<App_, CliLink<App_, Impl_, IOBufAlloc_>>,
  public ZiRx<CliLink<App_, Impl_, IOBufAlloc_>, IOBufAlloc_> {
public:
  using App = App_;
  using Impl = Impl_;
  using Base = Ztls::CliLink<App, Impl>;
  using IOBufAlloc = IOBufAlloc_;
  using Rx = ZiRx<CliLink, IOBufAlloc>;

friend Base;
template <typename, typename> friend class Client;

public:
  auto impl() const { return static_cast<const Impl *>(this); }
  auto impl() { return static_cast<Impl *>(this); }

  struct State {
    enum {
      Down = 0,
      Handshake,
      Up
    };
  };

  CliLink(App *app, ZtString server, uint16_t port) :
      Base{app, ZuMv(server), port} { }

  void connect() {
    this->Base::connect();
  }

  void connected(const char *alpn, int /* tlsver */) {
    if (!alpn || strcmp(alpn, "http/1.1")) {
      this->disconnect();
      return;
    }

    scheduleTimeout();
    m_state = State::Handshake;

    // FIXME send HTTP 1/1 upgrade header
  }

  void connectFailed(bool transient) { impl()->connectFailed(transient); }

  void disconnected() {
    m_state = State::Down;

    cancelTimeout();
  }

  // handle all the following opcodes send/receive:
  //
  // Text Frame (Opcode: 0x1): Used to send textual data, typically encoded
  // as UTF-8.
  // 
  // Binary Frame (Opcode: 0x2): Used to send binary data, such as files or
  // media streams.
  // 
  // Continuation Frame (Opcode: 0x0): Used when a message is fragmented
  // across multiple frames. This frame continues a previous text or binary
  // frame until the message is fully received.
  // 
  // Connection Close Frame (Opcode: 0x8): Used to initiate or confirm the
  // closing of the WebSocket connection. It can optionally contain a status
  // code and reason for closure. After sending or receiving a close frame,
  // no further data can be sent, and the connection will be closed.
  // 
  // Ping Frame (Opcode: 0x9): Used to check if the recipient is still
  // responsive. It can include an optional payload that the recipient must
  // return in the pong response.
  // 
  // Pong Frame (Opcode: 0xA): Sent in response to a ping frame. It can also
  // be sent unsolicited as a heartbeat.

  // FIXME
  // - provide a bufAlloc() that returns an IOBuf with a skipped header
  //   that is large enough to prepend a websockets frame header without
  //   copying/moving data
 
  // FIXME really there are three choices - text, binary, continuation
  void send(ZmRef<ZiIOBuf> buf, bool text = false, bool final = true) {
    // FIXME - prepend websockets frame header then call Base::send
  }

  // FIXME - need to have configurable ping sending
  // FIXME - need to have configurable pong sending
  // FIXME - need to have configurable pong timeout
  // FIXME - need additional state for graceful shutdown - sending of
  //         close frame that is also acked by close frame

  int process(const uint8_t *data, unsigned length) {
    auto state = m_state.load_();
    if (ZuUnlikely(state == State::Down))
      return -1; // disconnect

    if (ZuUnlikely(state == State::Handshake)) {
      cancelTimeout();

      // FIXME 
      // - parse/validate upgrade ack
      // - on failure:
      //   impl()->connectFailed(bool transient);
      //   m_state = State::Down;
      //   return -1;
 
      m_state = State::Up;

      impl()->connected();

      return length; // FIXME - actual number bytes consumed
    }

    // FIXME - handle all opcodes including continuations, ping/pong, etc.

    auto i = impl()->process(data, length);

    if (ZuUnlikely(i < 0)) m_state = State::Down;
    return i;
  }

private:
  void scheduleTimeout() {
    if (this->app()->timeout())
      this->app()->mx()->add([link = ZmMkRef(impl())]() {
	link->disconnect();
      }, Zm::now(this->app()->timeout()), &m_timer);
  }
  void cancelTimeout() {
    this->app()->mx()->del(&m_timer);
  }

private:
  ZmScheduler::Timer	m_timer;
  ZmAtomic<int>		m_state = State::Down;
};

// FIXME - actually three timeouts / intervals:
// 1] http upgrade timeout
// 2] ping send interval (optional)
//    (pong timeout should be ping send interval, if configured)
// 3] unsolicited pong send timer (which should be reset if pong
//    is sent early in response to a ping)

template <typename App_, typename Link_>
class Client : public Ztls::Client<App_> {
public:
  using App = App_;
  using Link = Link_;
  using TLS = Ztls::Client<App>;
friend TLS;

  const App *app() const { return static_cast<const App *>(this); }
  App *app() { return static_cast<App *>(this); }

  void init(ZiMultiplex *mx, const ZvCf *cf) {
    static const char *alpn[] = { "http/1.1", 0 };

    TLS::init(mx, cf->get("thread", true), cf->get("caPath", true), alpn);

    m_reconnFreq = cf->getInt("reconnFreq", 0, 3600, 0);
    m_timeout = cf->getInt("timeout", 0, 3600, 0);
  }

  void final() {
    TLS::final();
  }

  unsigned reconnFreq() const { return m_reconnFreq; }
  unsigned timeout() const { return m_timeout; }

private:
  unsigned		m_reconnFreq = 0;
  unsigned		m_timeout = 0;
};

}

#endif /* Zws_HH */
