//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

#include <zlib/ZuLib.hh>

#include <iostream>

#include <zlib/ZtRegex.hh>
#include <zlib/ZtArray.hh>

#include <zlib/Ztls.hh>

const char *Request =
  "GET / HTTP/1.1\r\n"
  "Host: ";
const char *Request2 = "\r\n"
  "User-Agent: ZtlsClient/1.0\r\n"
  "Accept: */*\r\n"
  "\r\n";

struct App : public Ztls::Client<App> {
  struct Link : public Ztls::CliLink<App, Link> {
    Link(App *app) : Ztls::CliLink<App, Link>(app) { }

    void connected(const char *alpn) {
      ZtString hostname = this->server();
      std::cerr << (ZuStringN<100>()
	  << "TLS handshake completed (hostname: " << hostname
	  << " ALPN: " << alpn << ")\n")
	<< std::flush;
      ZtString request;
      request << Request << hostname << Request2;
      send_((const uint8_t *)request.data(), request.length()); // in TLS thread
    }
    void disconnected() {
      std::cerr << "disconnected\n" << std::flush;
      app()->done();
    }

    void connectFailed(bool transient) {
      if (transient)
	std::cerr << "failed to connect (transient)\n" << std::flush;
      else
	std::cerr << "failed to connect\n" << std::flush;
      app()->done();
    }

    int process(const uint8_t *data, unsigned len) {
      auto s = ZuString{(const char *)data, len};
      std::cout << s << std::flush;
      if (ZtREGEX("</html>").m(s)) {
	disconnect_();
	return -1;
      }
      return len;
    }
  };

  void done() { sem.post(); }

  ZmSemaphore sem;
};

void usage()
{
  std::cerr <<
    "usage: ZtlsClient CA SERVER PORT\n\n"
    "Note: use /etc/ssl/certs as CA for public servers\n"
    << std::flush;
  ::exit(1);
}

int main(int argc, char **argv)
{
  if (argc != 4) usage();

  ZuString server = argv[2];
  unsigned port = ZuBox<unsigned>(argv[3]);

  if (!port) usage();

  ZeLog::init("ZtlsClient");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::fileSink(ZeSinkOptions{}.path("&2")));
  ZeLog::start();

  static const char *alpn[] = { "http/1.1", 0 };

  App app;

  ZiMultiplex mx(
      ZiMxParams()
	.scheduler([](auto &s) {
	  s.nThreads(4)
	  .thread(1, [](auto &t) { t.isolated(1); })
	  .thread(2, [](auto &t) { t.isolated(1); })
	  .thread(3, [](auto &t) { t.isolated(1); }); })
	.rxThread(1).txThread(2));

  if (!mx.start()) {
    std::cerr << "ZiMultiplex start failed\n" << std::flush;
    return 1;
  }

  if (!app.init(&mx, "3", argv[1], alpn)) {
    std::cerr << "TLS client initialization failed\n" << std::flush;
    return 1;
  }

  {
    ZmRef<App::Link> link = new App::Link(&app);

    link->connect(server, port);

    app.sem.wait();
  }

  mx.stop();

  ZeLog::stop();

  return 0;
}
