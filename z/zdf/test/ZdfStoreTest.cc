//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuLib.hh>

#include <zlib/ZmTrap.hh>

#include <zlib/ZeLog.hh>

#include <zlib/ZvCf.hh>
#include <zlib/ZvMxParams.hh>

#include <zlib/ZdbMemStore.hh>

#include <zlib/ZdfStore.hh>

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

ZmRef<ZvCf> inlineCf(ZuString s)
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

