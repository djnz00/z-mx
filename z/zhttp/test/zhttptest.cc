//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <iostream>

#include <zlib/Zhttp.hh>

inline void out(const char *s) { std::cout << s << '\n'; }

#define CHECK(x) ((x) ? out("OK  " #x) : out("NOK " #x))

char response_[] =
"HTTP/1.1 200 OK\r\n"
"Server: nginx\r\n"
"Date: Sun, 06 Oct 2024 06:12:39 GMT\r\n"
"Content-Type: text/html; charset=UTF-8\r\n"
"Content-Length: 211\r\n"
"Connection: keep-alive\r\n"
"X-hacker: If you're reading this, you should visit wpvip.com/careers and apply to join the fun, mention this header.\r\n"
"X-Powered-By: WordPress VIP <https://wpvip.com>\r\n"
"Host-Header: a9130478a60e5f9135f765b23f26593b\r\n"
"X-Frame-Options: SAMEORIGIN\r\n"
"Referrer-Policy: no-referrer-when-downgrade\r\n"
"X-Content-Type-Options: nosniff\r\n"
"X-XSS-Protection: 1; mode=block\r\n"
"Content-Security-Policy: frame-ancestors nypost.com decider.com pagesix.com *.nypost.com *.decider.com *.pagesix.com; form-action 'self' *.nypdev.com nypost.com decider.com pagesix.com *.nypost.com *.decider.com *.pagesix.com\r\n"
"Link: <https://nypost.com/wp-json/>; rel=\"https://api.w.org/\"\r\n"
"Link: <https://wp.me/b3Qpq>; rel=shortlink\r\n"
"Strict-Transport-Security: max-age=31536000\r\n"
"x-rq: nrt1 123 242 443\r\n"
"accept-ranges: bytes\r\n"
"x-cache: HIT\r\n"
"cache-control: private, no-store\r\n\r\n"

"<!doctype html>\n"
"<html lang=\"en-US\">\n"
"<head prefix=\"og: https://ogp.me/ns# fb: https://ogp.me/ns/fb#\">\n"
"<title>New York Post – Breaking News, Top Headlines, Photos & Videos</title>\n"
"</head>\n"
"<body>\n"
"</body>\n"
"</html>\n";

char request_[] =
"GET / HTTP/1.1\r\n"
"Host: foo.com\r\n"
"User-Agent: zhttptest/1.0\r\n"
"Accept: */*\r\n"
"\r\n";

int main()
{
  using namespace Zhttp;

  CHECK(eoh("\r\n\r") == -1);
  CHECK(eoh("\r\n\r\n") == 2);
  CHECK(eoh("\r\r\n\r\n") == 3);
  CHECK(eoh("\n\r\n\r\n") == 3);
  CHECK(eoh("\r\r\r\n\r\n") == 4);
  CHECK(eoh("\n\n\r\n\r\n") == 4);
  CHECK(eoh("\r\r\r\r\n\r\n") == 5);
  CHECK(eoh("\n\n\n\r\n\r\n") == 5);
  CHECK(eoh("\r\nx\r\r\n\r\n") == 6);
  CHECK(eoh("\r\nx\n\r\n\r\n") == 6);
  CHECK(eoh("\n\rx\r\r\n\r") == -1);
  CHECK(eol("\n") == -1);
  CHECK(eol("\r") == -1);
  CHECK(eol("\r\nx") == 0);
  CHECK(eol("\r\r\nx") == 1);
  CHECK(eol("\n\r\nx") == 1);
  CHECK(eol("\r\n ") == -1);
  CHECK(eol("\r\r\n ") == -1);
  CHECK(eol("\n\r\n ") == -1);
  CHECK(eol("\r\n \r\nx") == 3);
  CHECK(eol("\r\r\n\t\r\nx") == 4);
  CHECK(eol("\n\r\n\r\r\nx") == 1);
  CHECK(eol("\r\r") == -1);
  CHECK(eol("\n\r") == -1);
  CHECK(eok(":") == 0);
  CHECK(eok(": ") == 0);
  CHECK(eok("x: ") == 1);
  CHECK(eok("x:: ") == 1);
  CHECK(eok("x ::") == 2);

  {
    ZtString s;
    request(s, Method::GET, "/",
	{ { "Host", "foo.com" },
	  { "User-Agent", "zhttptest/1.0" },
	  { "Accept", "*/*" } }, {});
    CHECK(s == request_);
  }
  {
    int i = -1;
    split("", [&i](unsigned j, ZuCSpan) { i = j; }); CHECK(i == -1);
    split(" ", [&i](unsigned j, ZuCSpan) { i = j; }); CHECK(i == -1);
    split(",", [&i](unsigned j, ZuCSpan s) {
      CHECK(s == "");
      i = j;
    });
    CHECK(i == 1); i = -1;
    auto check = [&i](unsigned j, ZuCSpan s) {
      CHECK(s == "foo");
      i = j;
    };
    split("foo", check); CHECK(!i); i = -1;
    split(" foo", check); CHECK(!i); i = -1;
    split("foo ", check); CHECK(!i); i = -1;
    split(" foo ", check); CHECK(!i); i = -1;
    auto check2 = [&i](unsigned j, ZuCSpan s) {
      CHECK(s == (!j ? "foo" : "bar"));
      i = j;
    };
    split("foo,bar", check2); CHECK(i == 1); i = -1;
    split("foo ,bar", check2); CHECK(i == 1); i = -1;
    split("foo, bar", check2); CHECK(i == 1); i = -1;
    split("foo , bar", check2); CHECK(i == 1); i = -1;
    split("foo  ,  bar", check2); CHECK(i == 1); i = -1;
    split(" foo  ,  bar ", check2); CHECK(i == 1); i = -1;
  }

  {
    Request<5> r;
    CHECK(r.parse(request_) > 0);
    CHECK(r.protocol == "HTTP/1.1");
    CHECK(r.path == "/");
    CHECK(r.method == "GET");
    CHECK(r.headers.findVal("Host") == "foo.com");
    CHECK(r.body.length() == 0);
  }
  {
    Response<5> r;
    CHECK(r.parse(response_) > 0);
    CHECK(r.protocol == "HTTP/1.1");
    CHECK(r.code == 200);
    CHECK(r.reason == "OK");
    CHECK(r.headers.findVal("Referrer-Policy") == "no-referrer-when-downgrade");
    auto body = r.bodyEncoding();
    CHECK(body.valid);
    CHECK(!body.chunked);
    CHECK(body.transferEncoding < 0);
    CHECK(body.contentLength == 211);
    CHECK(r.body.length() == 211);
  }
  { auto [ length, offset ] = chunkLength("Aa0\r\n");
    CHECK(length == 0xaa0 && offset == 5); }
  { auto [ length, offset ] = chunkLength("Aa0 \r\n");
    CHECK(length == -1 && offset == -1); }
  { auto [ length, offset ] = chunkLength("\r\n");
    CHECK(length == -1 && offset == -1); }
  { auto [ length, offset ] = chunkLength("aaaaaaaa\r\n");
    CHECK(length == -1 && offset == -1); }
  { auto [ length, offset ] = chunkLength("aaaaaaaaa\r\n");
    CHECK(length == -1 && offset == -1); }
}
