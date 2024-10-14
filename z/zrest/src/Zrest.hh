//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Z REST library

#ifndef Zrest_HH
#define Zrest_HH

#include <zlib/ZrestLib.hh>

#include <zlib/Ztls.hh>
#include <zlib/Zhttp.hh>

namespace Zrest {

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

  using Base::app;

public:
  auto impl() const { return static_cast<const Impl *>(this); }
  auto impl() { return static_cast<Impl *>(this); }

  struct State {
    enum {
      Down = 0,
      Up,
      Closing
    };
  };

  CliLink(App *app, ZtString server, uint16_t port) :
      Base{app, ZuMv(server), port} {
  }

  bool connect() {
    this->Base::connect();
    return true;
  }

  void connected(const char *alpn, int /* tlsver */) {
    if (!alpn || strcmp(alpn, "http/1.1")) {
      this->disconnect();
      return;
    }

    scheduleTimeout();
  }

  void connectFailed(bool transient) { impl()->connectFailed(transient); }

  void disconnected() {
    m_state = State::Down;
    cancelTimeout();
  }

  void send(ZmRef<ZiIOBuf> buf) {
  }

  int process(const uint8_t *data, unsigned length) {
    auto state = m_state.load_();
    if (ZuUnlikely(state == State::Down))
      return -1; // disconnect
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

    TLS::init(mx, cf->get("thread", true), alpn, cf->get("caPath", false));

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

} // Zrest

#endif /* Zrest_HH */
