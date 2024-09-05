//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <iostream>

#include <zlib/ZuLib.hh>

#include <zlib/ZmTrap.hh>

#include <zlib/ZvCf.hh>
#include <zlib/ZvMxParams.hh>

#include <zlib/ZdbMemStore.hh>

#include <zlib/ZdfCompress.hh>
#include <zlib/ZdfSeries.hh>
#include <zlib/ZdfStore.hh>

void print(const char *s) {
  std::cout << s << '\n' << std::flush;
}
void print(const char *s, int64_t i) {
  std::cout << s << ' ' << i << '\n' << std::flush;
}
void ok(const char *s) { print(s); }
void ok(const char *s, int64_t i) { print(s, i); }
void fail(const char *s) { print(s); }
void fail(const char *s, int64_t i) { print(s, i); }
#define CHECK(x) ((x) ? ok("OK  " #x) : fail("NOK " #x))
#define CHECK2(x, y) ((x == y) ? ok("OK  " #x, x) : fail("NOK " #x, x))

// database
ZmRef<Zdb> db;

// dataframe store
ZuPtr<Zdf::Store> store;

// multiplexer
ZuPtr<ZiMultiplex> mx;

ZmSemaphore done;

void sigint()
{
  std::cerr << "SIGINT\n" << std::flush;
  done.post();
}

ZmRef<ZvCf> inlineCf(ZuCSpan s)
{
  ZmRef<ZvCf> cf = new ZvCf{};
  cf->fromString(s);
  return cf;
}

void gtfo()
{
  if (mx) mx->stop();
  ZeLog::stop();
  Zm::exit(1);
}

using Series = Zdf::Series<Zdf::Decoder>;

struct Test {
  ZmRef<Series>	series;

  void run() {
    store->openSeries<Zdf::Decoder, true>(0, "test",
      [this](ZmRef<Series> series_) {
	series = ZuMv(series_);
	run_opened();
      });
  }
  void run_opened() {
    if (!series) {
      ZeLOG(Fatal, "open failed");
      gtfo();
      return;
    }
    series->write([this](auto w) { run_write(ZuMv(w)); }, 1);
  }
  void run_write(Series::WrRef w) {
    if (!w) {
      ZeLOG(Fatal, "write1 failed");
      gtfo();
      return;
    }
    CHECK(w->write(42));
    CHECK(w->write(42));
    w = {};
    series->write([this](auto w) mutable { run_write2(ZuMv(w)); }, 2);
  }
  void run_write2(Series::WrRef w) {
    if (!w) {
      ZeLOG(Fatal, "write2 failed");
      gtfo();
      return;
    }
    CHECK(w->write(4301));
    CHECK(w->write(4302));
    w = {};
    series->write([this](auto w) mutable { run_write3(ZuMv(w)); }, 3);
  }
  void run_write3(Series::WrRef w) {
    if (!w) {
      ZeLOG(Fatal, "write3 failed");
      gtfo();
      return;
    }
    CHECK(w->write(43030));
    CHECK(w->write(43040));
    w = {};
    series->write([this](auto w) mutable { run_write4(ZuMv(w)); }, 4);
  }
  void run_write4(Series::WrRef w) {
    if (!w) {
      ZeLOG(Fatal, "write4 failed");
      gtfo();
      return;
    }
    CHECK(w->write(430500));
    CHECK(w->write(430600));
    for (unsigned i = 0; i < 300; i++) {
      CHECK(w->write(430700));
    }
    CHECK(w->series()->blkCount() == 4);
    w = {};
    run_read();
  }
  void run_read() {
    auto r = series->seek();
    if (!r->read([this, i = 0](Series::Reader *r, ZuFixed v) mutable {
      switch (i++) {
	case 0:
	  CHECK(v.mantissa == 42 && !v.ndp);
	  break;
	case 1:
	  CHECK(v.mantissa == 42 && !v.ndp);
	  break;
	case 2:
	  CHECK(v.mantissa == 4301 && v.ndp == 2);
	  break;
	case 3:
	  CHECK(v.mantissa == 4302 && v.ndp == 2);
	  break;
	case 4:
	  CHECK(v.mantissa == 43030 && v.ndp == 3);
	  break;
	case 5:
	  CHECK(v.mantissa == 43040 && v.ndp == 3);
	  break;
	case 6:
	  CHECK(v.mantissa == 430500 && v.ndp == 4);
	  break;
	case 7:
	  CHECK(v.mantissa == 430600 && v.ndp == 4);
	  break;
	default:
	  CHECK(v.mantissa == 430700 && v.ndp == 4);
	  break;
      }
      if (i < 308) return true;
      series->run([this]() { run_read2(); });
      return false;
    })) {
      ZeLOG(Fatal, "read failed");
      gtfo();
    }
  }
  void run_read2() {
    auto r = series->find(ZuFixed{425, 1});
    if (!r->read([this](Series::Reader *r, ZuFixed v) {
      CHECK(v.mantissa == 4301 && v.ndp == 2);
      series->run([this]() { run_read3(); });
      return false;
    })) {
      ZeLOG(Fatal, "read2 failed");
      gtfo();
    }
  }
  void run_read3() {
    auto r = series->find(ZuFixed{43020, 3});
    if (!r->read([this](Series::Reader *r, ZuFixed v) {
      CHECK(v.mantissa == 4302 && v.ndp == 2);
      r->purge();
      series->run([this]() { run_read4(); });
      return false;
    })) {
      ZeLOG(Fatal, "read3 failed");
      gtfo();
    }
  }
  void run_read4() {
    auto r = series->find(ZuFixed{44, 0});
    if (!r->read([this](Series::Reader *r, ZuFixed v) {
      CHECK(!*v);
      series->run([this]() { run_read5(); });
      return false;
    })) {
      ZeLOG(Fatal, "read4 failed");
      gtfo();
    }
  }
  void run_read5() {
    CHECK(series->blkCount() == 3);
    done.post();
  }
};
#if 0
    {
      auto r = series->find<DeltaDecoder<>>(ZuFixed{44, 0});
      ZuFixed v;
      CHECK(!r);
      CHECK(!r.read(v));
    }
    {
      auto r = series->seek<DeltaDecoder<>>();
      ZuFixed v;
      CHECK(r.read(v)); CHECK(v.mantissa == 4301 && v.ndp == 2);
    }
    {
      auto r = series->seek<DeltaDecoder<>>(208);
      ZuFixed v;
      for (unsigned i = 0; i < 50; i++) {
	CHECK(r.read(v));
	CHECK(v.mantissa == 430700 && v.ndp == 4);
      }
      CHECK(r.offset() == 258);
      for (unsigned i = 0; i < 50; i++) {
	CHECK(r.read(v));
	CHECK(v.mantissa == 430700 && v.ndp == 4);
      }
      CHECK(!r.read(v));
    }
#endif

Test test;

int main()
{
  ZmRef<ZvCf> cf;

  try {
    cf = inlineCf(
      "zdb {\n"
      "  thread zdb\n"
      "  store { thread zdb_mem }\n"
      "  hostID 0\n"
      "  hosts {\n"
      "    0 { standalone 1 }\n"
      "  }\n"
      "  tables { }\n"
      "  debug 1\n"
      "}\n"
      "mx {\n"
      "  nThreads 4\n"
      "  threads {\n"
      "    1 { name rx isolated true }\n"
      "    2 { name tx isolated true }\n"
      "    3 { name zdb isolated true }\n"
      "    4 { name zdb_mem isolated true }\n"
      "  }\n"
      "  rxThread rx\n"
      "  txThread tx\n"
      "}\n"
    );

  } catch (const ZvError &e) {
    std::cerr << e << '\n' << std::flush;
    Zm::exit(1);
  } catch (const ZeError &e) {
    std::cerr << e << '\n' << std::flush;
    Zm::exit(1);
  } catch (...) {
    Zm::exit(1);
  }

  ZeLog::init("ZdfStoreTest");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::fileSink(ZeSinkOptions{}.path("&2"))); // log to stderr
  ZeLog::start();

  ZmTrap::sigintFn(sigint);
  ZmTrap::trap();

  try {
    ZeError e;

    mx = new ZiMultiplex{ZvMxParams{"mx", cf->getCf<true>("mx")}};

    if (!mx->start()) throw ZeEVENT(Fatal, "multiplexer start failed");

    db = new Zdb();

    ZdbCf dbCf{cf->getCf<true>("zdb")};

    Zdf::Store::dbCf(cf, dbCf);

    db->init(ZuMv(dbCf), mx, ZdbHandler{
      .upFn = [](Zdb *, ZdbHost *host) {
	ZeLOG(Info, ([id = host ? host->id() : ZuID{"unset"}](auto &s) {
	  s << "ACTIVE (was " << id << ')';
	}));
	done.post();
      },
      .downFn = [](Zdb *, bool) {
	ZeLOG(Info, "INACTIVE");
      }
    }, new ZdbMem::Store());

    store = new Zdf::Store{};
    store->init(db);

    db->start();
    done.wait(); // ensure active

    store->run(0, []() {
      store->open([](bool ok) {
	std::cout << "open(): " << (ok ? "OK" : "NOT OK") << '\n';
	if (ok)
	  test.run();
	else
	  done.post();
      });
    });

    done.wait();

    db->stop(); // closes all tables

    db->final();

    mx->stop();

    ZeLOG(Debug, (ZtString{} << '\n' << ZmHashMgr::csv()));

    db = {};
    store = {};

  } catch (const ZvError &e) {
    ZeLOG(Fatal, ZtString{e});
    gtfo();
  } catch (const ZeError &e) {
    ZeLOG(Fatal, ZtString{e});
    gtfo();
  } catch (const ZeAnyEvent &e) {
    ZeLogEvent(ZeVEvent{e});
    gtfo();
  } catch (...) {
    ZeLOG(Fatal, "unknown exception");
    gtfo();
  }

  mx = {};

  ZeLog::stop();

  return 0;
}

