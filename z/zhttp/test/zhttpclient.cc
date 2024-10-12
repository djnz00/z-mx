//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <iostream>

#include <zlib/ZuLib.hh>

#include <zlib/ZtRegex.hh>
#include <zlib/ZtArray.hh>

#include <zlib/Ztls.hh>

#include <zlib/Zhttp.hh>

struct App : public Ztls::Client<App> {
  struct Link : public Ztls::CliLink<App, Link> {
    using Base = Ztls::CliLink<App, Link>;

    Link(App *app) : Base{app} { }

    void connected(const char *alpn, int tlsver) {
      ZtString hostname = this->server();
      std::cerr << (ZtString{}
	  << "TLS handshake completed (hostname: " << hostname
	  << " TLS: " << tlsver << " ALPN: " << alpn << ")\n")
	<< std::flush;
      ZtString request;
      Zhttp::request(request, Method::GET, "/",
	{ { "Host", "foo.com" },
	  { "User-Agent", "zhttptest/1.0" },
	  { "Accept", "*/*" } }, {});
      // connected() is called in TLS thread
      send_(
	reinterpret_cast<const uint8_t *>(request.data()),
	request.length());
    }
    void disconnected() {
      std::cerr << "disconnected\n" << std::flush;
      close();
      app()->done();
    }

    void connectFailed(bool transient) {
      if (transient)
	std::cerr << "failed to connect (transient)\n" << std::flush;
      else
	std::cerr << "failed to connect\n" << std::flush;
      close();
      app()->done();
    }

    int process(const uint8_t *data, unsigned len) {
      if (!file) {
	if (!response.headers.count()) {
	  header << ZuSpan<char>{const_cast<uint8_t *>(data), len};
	  auto r = response.parse(header);
	  if (r < 0) {
	    std::cerr << "invalid HTTP response\n" << std::flush;
	    return -1;
	  }
	  if (!r) return len;
	  body << ZuCSpan{&header[r], header.length() - r};
	  header.length(r);
	  encoding = response.bodyEncoding();
	  if (!encoding.valid) {
	    std::cerr
	      << "invalid HTTP Transfer-Encoding / Content-Length\n"
	      << std::flush;
	    return -1;
	  }
	} else {
	  body << ZuSpan<char>{const_cast<uint8_t *>(data), len};
	}
	if (encoding.contentLength >= 0) {
	  if (body.length() >= encoding.contentLength) goto done;
	  return len;
	} 
	// chunks
done:
	file = fopen("index.hdr", "w");
	ZmAssert(file);
	fwrite(header.data(), 1, header.length(), file);
	fclose(file);
	file = fopen("index.html", "w");
	ZmAssert(file);
	fwrite(body.data(), 1, body.length(), file);
      } else {
	fwrite(data, 1, len, file);
	if (length <= len) return -1;
	length -= len;
      }
      return len;
    }

    void close() {
      if (file) { fclose(file); file = nullptr; }
    }

    unsigned		length = 0;
    ZtString		header, body;
    ZHttp::Response	response;
    ZHttp::BodyEncoding	encoding;
    FILE		*file = nullptr;
  };

  void done() { sem.post(); }

  ZmSemaphore sem;
};

void usage()
{
  std::cerr << "Usage: ZtlsClient SERVER PORT [CA]\n" << std::flush;
  ::exit(1);
}

int main(int argc, char **argv)
{
  if (argc < 3 || argc > 4) usage();

  ZuCSpan server = argv[1];
  unsigned port = ZuBox<unsigned>(argv[2]);

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

  if (!app.init(&mx, "3", alpn, argc == 4 ? argv[3] : nullptr)) {
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
