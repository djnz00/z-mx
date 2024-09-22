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

void usage() {
  std::cerr << "Usage: zdffptest\n" << std::flush;
  ::exit(1);
}

using DF = Zdf::DataFrame<Frame, false>;
using DFWriter = DF::Writer;

struct Test {
  ZmRef<DF>		df;
  ZmQueue<double>	queue{ZmQueueParams{}.initial(100)};
  Zdf::StatsTree<>	stats;

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
      frame.v2 = (double(i) * 42) * .000000001;
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
    ZeLOG(Debug, ([v](auto &s) { s << "v=" << ZuBoxed(v); }));
    CHECK(ZuBoxed(v).feq(0.00000084));
    rc.fn({this, ZmFnPtr<&Test::run_read4<Ctrl>>{}});
    rc.findFwd(0.0000084);
    return false;
  }
  template <typename Ctrl>
  bool run_read4(Ctrl &rc, double v) {
    ZeLOG(Debug, ([v](auto &s) { s << "v=" << ZuBoxed(v); }));
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
    ZeLOG(Debug, ([v](auto &s) { s << "v=" << ZuBoxed(v); }));
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

    using Field = ZtField(Frame, v2);
    df->seek<Field>(
      Zdf::maxOffset(), {this, ZmFnPtr<&Test::run_read9<Ctrl>>{}}, []{
	ZeLOG(Fatal, "data frame read9 failed");
	done.post();
      });
    df->write({this, ZmFnPtr<&Test::run_live_write>{}}, []{
      ZeLOG(Fatal, "data frame live_write failed");
      done.post();
    });
    return false;
  }
  template <typename Ctrl>
  bool run_read9(Ctrl &, double v) {
    CHECK(ZuCmp<double>::null(v) || v == 42);
    return true;
  }
  void run_live_write(ZmRef<DFWriter> w) {
    Frame frame;
    for (uint64_t i = 0; i < 10; i++) {
      frame.v1 = i;
      frame.v2 = 42;
      w->write(frame);
    }
    df->stopWriting();
    df->stopReading();
    done.post();
  }
};

#if 0
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
	ZeLOG(Info, ([ok](auto &s) { s << (ok ? "OK" : "NOT OK"); }));
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
