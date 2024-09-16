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

struct Frame {
  uint64_t	v1;
  double	v2;
};
ZtFieldTbl(Frame,
  (((v1),	(Ctor<0>, Series, Index, Delta)),	(UInt64)),
  (((v2),	(Series, NDP<9>)),			(Float)));

void usage() {
  std::cerr << "Usage: zdffptest\n" << std::flush;
  ::exit(1);
}

using DF = Zdf::DataFrame<Frame, false>;
using DFWriter = DF::Writer;

struct Test {
  ZmRef<DF>	df;

  void run() {
    store->openDF<Frame, false, true>(
      0, "frame", {this, ZmFnPtr<&Test::run_opened>{}});
  }
  void run_opened(ZmRef<DF> df_) {
    if (!df_) {
      ZeLOG(Fatal, "data frame open failed");
      done.post();
      return;
    }
    df = ZuMv(df_);
    df->write({this, ZmFnPtr<&Test::run_write>{}}, []{
      ZeLOG(Fatal, "data frame write failed");
      done.post();
    });
  }
  void run_write(ZmRef<DFWriter> w) {
    Frame frame;
    for (uint64_t i = 0; i < 300; i++) { // 1000
      frame.v1 = i;
      frame.v2 = (double(i) * 42) / 1000000000;
      w->write(frame);
    }
    w->stop();
    df->run([this]() { run_read1(); });
  }
  void run_read1() {
    using Field = ZtField(Frame, v1);
    using Ctrl = Zdf::FieldRdrCtrl<Field>;
    df->find<Field>(
      ZuFixed{20, 0}, {this, ZmFnPtr<&Test::run_read2<Ctrl>>{}}, []{
	ZeLOG(Fatal, "data frame read1 failed");
	done.post();
      });
  }
  template <typename Ctrl>
  bool run_read2(Ctrl rc, ZuFixed) {
    using Field = ZtField(Frame, v2);
    using V2Ctrl = Zdf::FieldRdrCtrl<Field>;
    df->seek<Field>(
      rc.stop() - 1, {this, ZmFnPtr<&Test::run_read3<V2Ctrl>>{}}, []{
	ZeLOG(Fatal, "data frame read2 failed");
	done.post();
      });
    return true;
  }
  template <typename Ctrl>
  bool run_read3(Ctrl rc, double v) {
    std::cout << "v=" << v << '\n';
    std::cout << v << '\n';
    CHECK(v == 0.00000084);
    rc.fn({this, ZmFnPtr<&Test::run_read4<Ctrl>>{}});
    rc.findFwd(0.0000084);
    return true;
  }
  template <typename Ctrl>
  bool run_read4(Ctrl rc, double v) {
    std::cout << v << '\n';
    CHECK(v == 0.0000084);
    using Field = ZtField(Frame, v1);
    using V1Ctrl = Zdf::FieldRdrCtrl<Field>;
    df->seek<Field>(
      rc.stop() - 1, {this, ZmFnPtr<&Test::run_read5<V1Ctrl>>{}}, []{
	ZeLOG(Fatal, "data frame read4 failed");
	done.post();
      });
    return true;
  }
  template <typename Ctrl>
  bool run_read5(Ctrl rc, ZuFixed) {
    rc.fn({this, ZmFnPtr<&Test::run_read6<Ctrl>>{}});
    rc.findRev(ZuFixed{100, 0});
    return true;
  }
  template <typename Ctrl>
  bool run_read6(Ctrl rc, ZuFixed) {
    using Field = ZtField(Frame, v2);
    using V2Ctrl = Zdf::FieldRdrCtrl<Field>;
    df->seek<Field>(
      rc.stop() - 1, {this, ZmFnPtr<&Test::run_read7<V2Ctrl>>{}}, []{
	ZeLOG(Fatal, "data frame read6 failed");
	done.post();
      });
    return true;
  }
  template <typename Ctrl>
  bool run_read7(Ctrl rc, double v) {
    std::cout << v << '\n';
    CHECK(v == 0.0000042);
    rc.stop();
    done.post();
    return true;
  }
};

#if 0
    reader.seekRev(index.offset());
    AnyReader cleaner;
    {
      auto offset = reader.offset();
      offset = offset < 100 ? 0 : offset - 100;
      df.seek(cleaner, 1, offset);
    }
    Zdf::StatsTree<> w;
    while (reader.read(v)) {
      w.add(v);
      if (cleaner.read(v)) w.del(v);
      std::cout << "min=" << ZuBoxed(w.minimum()) <<
	" max=" << ZuBoxed(w.maximum()) <<
	" mean=" << ZuBoxed(w.mean()) <<
	" stddev=" << ZuBoxed(w.std()) <<
	" median=" << ZuBoxed(w.median()) <<
	" 95%=" << ZuBoxed(w.rank(0.95)) << '\n';
    }
    // for (auto k = w.begin(); k != w.end(); ++k) std::cout << *k << '\n';
    // for (auto k: w) std::cout << k.first << '\n';
    // std::cout << "stddev=" << w.std() << '\n';
#endif

Test test;

int main(int argc, char **argv)
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

  ZeLog::init("zdftest");
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

    // ZeLOG(Debug, (ZtString{} << '\n' << ZmHashMgr::csv()));
    // ZeLOG(Debug, (ZtString{} << '\n' << ZmHeapMgr::csv()));

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
