//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <iostream>

#include <zlib/ZuLib.hh>

#include <zlib/ZmTrap.hh>

#include <zlib/ZvCf.hh>
#include <zlib/ZvMxParams.hh>

#include <zlib/Zdb.hh>

#include <zlib/ZdfCompress.hh>
#include <zlib/ZdfSeries.hh>
#include <zlib/ZdfStore.hh>
#include <zlib/ZdfStats.hh>

void print(const char *msg) {
  std::cout << msg << '\n';
}
void print(const char *msg, double i) {
  std::cout << msg << ' ' << ZuBoxed(i) << '\n';
}
void ok(const char *msg) { print(msg); }
void ok(const char *msg, double i) { print(msg, i); }
void fail(const char *msg) { print(msg); }
void fail(const char *msg, double i) { print(msg, i); }
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

void usage()
{
  static const char *help =
    "Usage: zdffpftest [OPTION]...\n\n"
    "Options:\n"
    "      --help\t\tthis help\n"
    "  -m, --module=MODULE\tspecify data store module (default: $ZDB_MODULE)\n"
    "  -c, --connect=CONNECT\t"
      "specify data store connection (default: $ZDB_CONNECT)\n"
    "  -d, --debug\t\tenable Zdb debug logging\n"
    "  -t, --hash-tel\toutput hash table telemetry CSV at exit\n"
    "  -T, --heap-tel\toutput heap telemetry CSV at exit\n"
    ;

  std::cerr << help << std::flush;
  Zm::exit(1);
}

using DF = Zdf::DataFrame<Frame, false>;
using DFWriter = DF::Writer;

struct Test {
  ZmRef<DF>		df;
  ZmQueue<double>	queue{ZmQueueParams{}.initial(100)};
  Zdf::StatsTree<>	stats;

  static double v2(double i) {
    return (double(i) * 42) * .000000001;
  }

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
    auto count = df->series<ZtField(Frame, v1)>()->count();
    if (count) {
      df->run([this]() { run_read1(); });
    } else
      df->write({this, ZmFnPtr<&Test::run_write>{}}, []{
	ZeLOG(Fatal, "data frame write failed");
	done.post();
      });
  }
  void run_write(ZmRef<DFWriter> w) {
    Frame frame;
    for (int64_t i = 0; i < 100000; i++) { // 10000; i++) {
      frame.v1 = i;
      frame.v2 = v2(i);
      w->write(frame);
    }
    df->run([this]() { run_read1(); });
  }
  void run_read1() {
    using Field = ZtField(Frame, v1);
    using Ctrl = Zdf::FieldRdrCtrl<Field>;
    df->find<Field>(
      ZuFixed{20, 0}, {this, ZmFnPtr<&Test::run_read2<Ctrl>>{}}, []{
	ZeLOG(Fatal, "data frame read2 failed");
	done.post();
      });
  }
  template <typename Ctrl>
  bool run_read2(Ctrl &rc, ZuFixed) {
    using Field = ZtField(Frame, v2);
    using V2Ctrl = Zdf::FieldRdrCtrl<Field>;
    df->seek<Field>(
      rc.stop() - 1, {this, ZmFnPtr<&Test::run_read3<V2Ctrl>>{}}, []{
	ZeLOG(Fatal, "data frame read3 failed");
	done.post();
      });
    return false;
  }
  template <typename Ctrl>
  bool run_read3(Ctrl &rc, double v) {
    CHECK(ZuBoxed(v).feq(0.00000084));
    rc.fn({this, ZmFnPtr<&Test::run_read4<Ctrl>>{}});
    rc.findFwd(0.0000084);
    return false;
  }
  template <typename Ctrl>
  bool run_read4(Ctrl &rc, double v) {
    CHECK(ZuBoxed(v).feq(0.0000084));
    using Field = ZtField(Frame, v1);
    using V1Ctrl = Zdf::FieldRdrCtrl<Field>;
    df->seek<Field>(
      rc.stop() - 1, {this, ZmFnPtr<&Test::run_read5<V1Ctrl>>{}}, []{
	ZeLOG(Fatal, "data frame read5 failed");
	done.post();
      });
    return false;
  }
  template <typename Ctrl>
  bool run_read5(Ctrl &rc, ZuFixed) {
    rc.fn({this, ZmFnPtr<&Test::run_read6<Ctrl>>{}});
    rc.findRev(ZuFixed{100, 0});
    return false;
  }
  template <typename Ctrl>
  bool run_read6(Ctrl &rc, ZuFixed) {
    using Field = ZtField(Frame, v2);
    using V2Ctrl = Zdf::FieldRdrCtrl<Field>;
    df->seek<Field>(
      rc.stop() - 1, {this, ZmFnPtr<&Test::run_read7<V2Ctrl>>{}}, []{
	ZeLOG(Fatal, "data frame read7 failed");
	done.post();
      });
    return false;
  }
  template <typename Ctrl>
  bool run_read7(Ctrl &rc, double v) {
    CHECK(ZuBoxed(v).feq(0.0000042));
    rc.fn({this, ZmFnPtr<&Test::run_read8<Ctrl>>{}});
    rc.seekRev(0);
    return false;
  }
  template <typename Ctrl>
  bool run_read8(Ctrl &rc, double v) {
    queue.push(v);
    stats.add(v);

    if (queue.count_() < 100) return true;

    v = queue.shift();
    stats.del(v);
    std::cout << "min=" << ZuBoxed(stats.minimum()) <<
      " max=" << ZuBoxed(stats.maximum()) <<
      " mean=" << ZuBoxed(stats.mean()) <<
      " stdev=" << ZuBoxed(stats.std()) <<
      " median=" << ZuBoxed(stats.median()) <<
      " 95%=" << ZuBoxed(stats.rank(0.95)) << '\n';

    if (rc.reader.offset() < 110) return true;
    df->run([this]() { run_read9(); });
    return false;
  }
  void run_read9() {
    using Field = ZtField(Frame, v2);
    using Ctrl = Zdf::FieldRdrCtrl<Field>;
    df->seek<Field>(
      Zdf::maxOffset(),
      {this, ZmFnPtr<&Test::run_read10<Ctrl>>{}}, []{
	ZeLOG(Fatal, "data frame read10 failed");
      });
  }
  template <typename Ctrl>
  bool run_read10(Ctrl &rc, double v) {
    auto j = rc.reader.offset() - 1;
    if (ZuCmp<double>::null(v)) {
      df->run([this]() { run_live_write(); });
    } else {
      CHECK(ZuBoxed(v).feq(v2(j)));
    }
    return true;
  }
  void run_live_write() {
    df->write({this, ZmFnPtr<&Test::run_live_write2>{}}, []{
      ZeLOG(Fatal, "data frame live_write2 failed");
      done.post();
    });
  }
  void run_live_write2(ZmRef<DFWriter> w) {
    auto end = df->count();
    Frame frame;
    for (uint64_t i = 0; i < 10; i++) {
      auto j = i + end;
      frame.v1 = j;
      frame.v2 = v2(j);
      w->write(frame);
    }
    df->stopWriting([]{ done.post(); });
    df->stopReading();
  }
};

#if 0
#endif

Test test;

int main(int argc, char **argv)
{
  ZmRef<ZvCf> cf;

  try {
    ZmRef<ZvCf> options = inlineCf(
      "module m m { param zdb.store.module }\n"
      "connect c c { param zdb.store.connection }\n"
      "debug d d { flag zdb.debug }\n"
      "hash-tel t t { flag hashTel }\n"
      "heap-tel T T { flag heapTel }\n"
      "help { flag help }\n");

      // "  module ../src/.libs/libZdbPQ.so\n"
      // "  connection \"dbname=test host=/tmp\"\n"

    cf = inlineCf(
      "zdb {\n"
      "  thread zdb\n"
      "  hostID 0\n"
      "  hosts {\n"
      "    0 { standalone 1 }\n"
      "  }\n"
      "  store {\n"
      "    module ${ZDB_MODULE}\n"
      "    connection ${ZDB_CONNECT}\n"
      "    thread zdb_pq\n"
      "    replicated true\n"
      "  }\n"
      "  tables { }\n"
      "}\n"
      "mx {\n"
      "  nThreads 4\n"
      "  threads {\n"
      "    1 { name rx isolated true }\n"
      "    2 { name tx isolated true }\n"
      "    3 { name zdb isolated true }\n"
      "    4 { name zdb_pq isolated true }\n"
      "  }\n"
      "  rxThread rx\n"
      "  txThread tx\n"
      "}\n");

    // command line overrides environment
    if (cf->fromArgs(options, ZvCf::args(argc, argv)) != 1) usage();

    if (cf->getBool("help")) usage();

    if (!cf->get("zdb.store.module")) {
      std::cerr << "set ZDB_MODULE or use --module=MODULE\n" << std::flush;
      Zm::exit(1);
    }
    if (!cf->get("zdb.store.connection")) {
      std::cerr << "set ZDB_CONNECT or use --connect=CONNECT\n" << std::flush;
      Zm::exit(1);
    }
  } catch (const ZvError &e) {
    std::cerr << e << '\n' << std::flush;
    Zm::exit(1);
  } catch (const ZeError &e) {
    std::cerr << e << '\n' << std::flush;
    Zm::exit(1);
  } catch (...) {
    Zm::exit(1);
  }

  ZeLog::init("zdffptest");
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
    });

    store = new Zdf::Store{};
    store->init(db);

    db->start();
    done.wait(); // ensure active

    store->run(0, []() {
      store->open([](bool ok) {
	ZeLOG(Info, ([ok](auto &s) { s << (ok ? "OK" : "NOT OK"); }));
	if (ok)
	  test.run();
	else
	  done.post();
      });
    });

    done.wait();

    if (cf->getBool("hashTel"))
      ZeLOG(Debug, (ZtString{} << '\n' << ZmHashMgr::csv()));

    if (cf->getBool("heapTel"))
      ZeLOG(Debug, (ZtString{} << '\n' << ZmHeapMgr::csv()));

    db->stop(); // closes all tables

    mx->stop();

    // ZeLOG(Debug, (ZtString{} << '\n' << ZmHashMgr::csv()));
    // ZeLOG(Debug, (ZtString{} << '\n' << ZmHeapMgr::csv()));

    db->final();
    db = {};

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
