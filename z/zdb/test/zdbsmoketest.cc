//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuLib.hh>

#include <zlib/ZmTrap.hh>

#include <zlib/ZeLog.hh>

#include <zlib/ZvCf.hh>

#include <zlib/Zdb.hh>

#include "ZdbMockStore.hh"
#include "zdbtest.hh"

using namespace zdbtest;

// mock data store
ZmRef<zdbtest::Store> store;

// database
ZmRef<Zdb> db;

// table
ZmRef<ZdbTable<Order>> orders;

// app scheduler, Zdb multiplexer
ZmScheduler *appMx = nullptr;
ZiMultiplex *dbMx = nullptr;

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

void usage()
{
  static const char *help =
    "usage: zdbsmoketest ...\n";

  std::cerr << help << std::flush;
  Zm::exit(1);
}

void gtfo()
{
  if (dbMx) dbMx->stop();
  if (appMx) appMx->stop();
  ZeLog::stop();
  Zm::exit(1);
}

int main(int argc, char **argv)
{
  static ZvOpt opts[] = { { 0 } };

  ZmRef<ZvCf> cf;
  ZuString hashOut;

  try {
    cf = inlineCf(
      "thread 3\n"
      "hostID 0\n"
      "hosts {\n"
      "  0 { priority 100 ip 127.0.0.1 port 9943 }\n"
      "}\n"
      "tables {\n"
      "  order { }\n"
      "}\n"
      "debug 1\n"
      "dbMx {\n"
      "  nThreads 4\n"
      "  threads {\n"
      "    1 { isolated true }\n"
      "    2 { isolated true }\n"
      "    3 { isolated true }\n"
      "  }\n"
      "  rxThread 1\n"
      "  txThread 2\n"
      "}\n"
    );

    if (cf->fromArgs(opts, argc, argv) != 1) usage();

  } catch (const ZvError &e) {
    std::cerr << e << '\n' << std::flush;
    usage();
  } catch (const ZeError &e) {
    std::cerr << e << '\n' << std::flush;
    usage();
  } catch (...) {
    usage();
  }

  ZeLog::init("zdbsmoketest");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::fileSink(ZeSinkOptions{}.path("&2"))); // log to stderr
  ZeLog::start();

  ZmTrap::sigintFn(sigint);
  ZmTrap::trap();

  try {
    ZeError e;

    appMx = new ZmScheduler(ZmSchedParams().nThreads(1));
    dbMx = new ZiMultiplex(ZvMxParams{"dbMx", cf->getCf<true>("dbMx")});

    appMx->start();
    if (!dbMx->start()) throw ZeEVENT(Fatal, "multiplexer start failed");

    store = new zdbtest::Store();
    db = new Zdb();

    db->init(ZdbCf(cf), dbMx, ZdbHandler{
	.upFn = [](Zdb *, ZdbHost *host) {
	  ZeLOG(Info, ([id = host ? host->id() : ZuID{"unset"}](auto &s) {
	    s << "ACTIVE (was " << id << ')';
	  }));
	},
	.downFn = [](Zdb *) { ZeLOG(Info, "INACTIVE"); }
    }, store);

    orders = db->initTable<Order>("order"); // might throw

    db->start();

    uint64_t id;

    // orders->writeCache(false);

    orders->run([&id]{
      orders->insert([&id](ZdbObject<Order> *o) {
	new (o->ptr())
	  Order{"IBM", 0, "FIX0", "order0", 0, Side::Buy, {100}, {100}};
	// o->data().flags = ZuType<8, ZuFieldList<Order>>::deflt();
	// LATER - think about simplifying this, potentially using
	// ZuField() and renaming the existing ZuField() macros which
	// are only used internally
	o->data().flags = ZuFields_::ZtFieldTypeName(Order, flags)::deflt();
	o->commit();
	id = o->data().orderID;
	ZeLOG(Info, ([id](auto &s) { s << "orderID=" << id; }));
      });
      orders->insert([](ZdbObject<Order> *o) {
	new (o->ptr())
	  Order{"IBM", 1, "FIX0", "order1", 2, Side::Buy, {100}, {100}};
	o->commit();
      });
      orders->insert([](ZdbObject<Order> *o) {
	new (o->ptr())
	  Order{"IBM", 2, "FIX0", "order2", 4, Side::Buy, {100}, {100}};
	o->commit();
	done.post();
      });
    });

    done.wait();

    orders->run([&id]{
      static ZmSemaphore done_;
      orders->find<0>(ZuFwdTuple("IBM", id),
	[&id](ZmRef<ZdbObject<Order>> o) {
	  if (!o)
	    ZeLOG(Info, ([id](auto &s) {
	      s << "find(IBM, " << id << "): (null)";
	    }));
	  else
	    ZeLOG(Info, ([id, o = ZuMv(o)](auto &s) {
	      s << "find(IBM, " << id << "): " << o->data();
	    }));
	  done_.post();
	});
      done_.wait();
      orders->glob<2>(ZuFwdTuple("FIX0"), 0, 1, [](auto max) {
	using Key = ZuFieldKeyT<Order, 2>;
	if (max.template is<Key>())
	  ZeLOG(Info, ([max = ZuMv(max)](auto &s) {
	    s << "maximum(FIX0): " << max.template p<Key>();
	  }));
	else
	  ZeLOG(Info, ([max = ZuMv(max)](auto &s) {
	    s << "maximum(FIX0): EOR";
	  }));
	done_.post();
      });
      done_.wait();
      done.post();
    });

    done.wait();

    db->stop(); // closes all tables

    store->preserve();

    orders = {};
    db->final();

    db->init(ZdbCf(cf), dbMx, ZdbHandler{
	.upFn = [](Zdb *, ZdbHost *host) {
	  ZeLOG(Info, ([id = host ? host->id() : ZuID{"unset"}](auto &s) {
	    s << "ACTIVE (was " << id << ')';
	  }));
	},
	.downFn = [](Zdb *) { ZeLOG(Info, "INACTIVE"); }
    }, store);

    orders = db->initTable<Order>("order"); // might throw

    db->start();

    ZeLOG(Info, ([count = orders->count()](auto &s) {
      s << "orders count=" << count;
    }));

    orders->run([&id]{
      static ZmSemaphore done_;
      orders->find<0>(ZuFwdTuple("IBM", id),
	[&id](ZmRef<ZdbObject<Order>> o) {
	  if (!o)
	    ZeLOG(Info, ([id](auto &s) {
	      s << "find(IBM, " << id << "): (null)";
	    }));
	  else
	    ZeLOG(Info, ([id, o = ZuMv(o)](auto &s) {
	      s << "find(IBM, " << id << "): " << o->data();
	    }));
	  done_.post();
	});
      done_.wait();
      orders->glob<2>(ZuFwdTuple("FIX0"), 0, 1, [](auto max) {
	using Key = ZuFieldKeyT<Order, 2>;
	if (max.template is<Key>())
	  ZeLOG(Info, ([max = ZuMv(max)](auto &s) {
	    s << "maximum(FIX0): " << max.template p<Key>();
	  }));
	else
	  ZeLOG(Info, ([max = ZuMv(max)](auto &s) {
	    s << "maximum(FIX0): EOR";
	  }));
	done_.post();
      });
      done_.wait();
      done.post();
    });

    done.wait();

    db->stop();

    appMx->stop();
    dbMx->stop();

    ZeLOG(Debug, (ZtString{} << '\n' << ZmHashMgr::csv()));

    orders = {};
    db->final(); // calls Store::final()
    db = {};
    store = {};

  } catch (const ZvError &e) {
    ZeLOG(Fatal, ZtString{e});
    gtfo();
  } catch (const ZeError &e) {
    ZeLOG(Fatal, ZtString{e});
    gtfo();
  } catch (const ZeAnyEvent &e) {
    ZeLogEvent(ZeMEvent{e});
    gtfo();
  } catch (...) {
    ZeLOG(Fatal, "unknown exception");
    gtfo();
  }

  if (appMx) delete appMx;
  if (dbMx) delete dbMx;

  ZeLog::stop();

  return 0;
}
