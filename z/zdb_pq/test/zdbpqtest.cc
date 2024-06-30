//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuLib.hh>

#include <zlib/ZmTrap.hh>

#include <zlib/ZeLog.hh>

#include <zlib/ZvCf.hh>

#include <zlib/Zdb.hh>

#include "zdbtest.hh"

using namespace zdbtest;

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
    "usage: zdbpqtest ...\n";

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
      "thread zdb\n"
      "hostID 0\n"
      "hosts {\n"
      "  0 { priority 100 ip 127.0.0.1 port 9943 }\n"
      "}\n"
      "store {\n"
      "  module ../src/.libs/libZdbPQ.so\n"
      "  connection \"dbname=test host=/tmp\"\n"
      "  thread zdb_pq\n"
      "  replicated true\n"
      "}\n"
      "tables {\n"
      "  order { }\n"
      "}\n"
      "debug 1\n"
      "dbMx {\n"
      "  nThreads 4\n"
      "  threads {\n"
      "    1 { name rx isolated true }\n"
      "    2 { name tx isolated true }\n"
      "    3 { name zdb isolated true }\n"
      "    4 { name zdb_pq isolated true }\n"
      "  }\n"
      "  rxThread rx\n"
      "  txThread tx\n"
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

  ZeLog::init("zdbpqtest");
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

    db = new Zdb();

    db->init(ZdbCf(cf), dbMx, ZdbHandler{
      .upFn = [](Zdb *, ZdbHost *host) {
	ZeLOG(Info, ([id = host ? host->id() : ZuID{"unset"}](auto &s) {
	  s << "ACTIVE (was " << id << ')';
	}));
      },
      .downFn = [](Zdb *) { ZeLOG(Info, "INACTIVE"); }
    });

    orders = db->initTable<Order>("order"); // might throw

    if (!db->start()) throw ZeEVENT(Fatal, "Zdb start failed");

    ZuNBox<uint64_t> seqNo;

    orders->run([&seqNo]{
      orders->glob<2>(ZuFwdTuple("FIX0"), 0, 1, [&seqNo](auto max) {
	using Key = ZuFieldKeyT<Order, 2>;
	if (max.template is<Key>()) {
	  seqNo = max.template p<Key>().template p<1>();
	  ZeLOG(Info, ([max = ZuMv(max)](auto &s) {
	    s << "maximum(FIX0): " << max.template p<Key>();
	  }));
	} else
	  ZeLOG(Info, ([max = ZuMv(max)](auto &s) {
	    s << "maximum(FIX0): EOR";
	  }));
	done.post();
      });
    });

    done.wait();

    ZuNBox<uint64_t> id;

    if (*seqNo) {
      orders->run([seqNo, &id]{
	orders->find<2>(ZuFwdTuple("FIX0", seqNo),
	  [seqNo, &id](ZmRef<ZdbObject<Order>> o) {
	    if (!o) {
	      id = {};
	      ZeLOG(Info, ([seqNo](auto &s) {
		s << "find(FIX0, " << seqNo << "): (null)";
	      }));
	    } else {
	      id = o->data().orderID;
	      ZeLOG(Info, ([seqNo, o = ZuMv(o)](auto &s) {
		s << "find(FIX0, " << seqNo << "): " << o->data();
	      }));
	    }
	    done.post();
	  });
      });

      done.wait();

      ++seqNo;
    } else
      seqNo = 0;

    if (*id)
      ++id;
    else
      id = 0;

    orders->run([&id, &seqNo]{
      orders->insert([&id, &seqNo](ZdbObject<Order> *o) {
	ZuStringN<32> clOrdID;
	clOrdID << "order" << id;
	new (o->ptr())
	  Order{"IBM", id, "FIX0", clOrdID, seqNo, Side::Buy, {100}, {100}};
	o->data().flags.set(42);
	ZeLOG(Debug, ([o = ZmRef{o}](auto &s) { s << o->data(); }));
	o->commit();
	id = o->data().orderID;
	seqNo = o->data().seqNo;
	ZeLOG(Info, ([id, seqNo](auto &s) {
	  s << "orderID=" << id << " seqNo=" << seqNo;
	}));
	done.post();
      });
    });

    done.wait();

    orders->run([&id]{
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
	  done.post();
	});
      done.wait();
      orders->glob<2>(ZuFwdTuple("FIX0"), 0, 1, [](auto max) {
	using Key = ZuFieldKeyT<Order, 2>;
	if (max.template is<Key>()) {
	  ZeLOG(Info, ([max = ZuMv(max)](auto &s) {
	    s << "maximum(FIX0): " << max.template p<Key>();
	  }));
	} else
	  ZeLOG(Info, ([max = ZuMv(max)](auto &s) {
	    s << "maximum(FIX0): EOR";
	  }));
	done.post();
      });
    });

    done.wait();

    if (id > 0) {
      orders->run([id = id - 1]{
	orders->findUpd<0, ZuSeq<1>>(ZuFwdTuple("IBM", id),
	  [id](ZmRef<ZdbObject<Order>> o) {
	    if (!o) {
	      ZeLOG(Info, ([id](auto &s) {
		s << "findUpd(IBM, " << id << "): (null)";
	      }));
	      return;
	    }
	    ZeLOG(Info, ([id, data = o->data()](auto &s) {
	      s << "findUpd(IBM, " << id << "): " << data;
	    }));
	    ZuStringN<32> clOrdID;
	    clOrdID << "order" << id << "_1";
	    o->data().prices[0] = o->data().prices[0] + 42;
	    o->data().clOrdID = clOrdID;
	    o->commit();
	  });
	done.post();
      });

      done.wait();
    }

    if (id > 3) {
      orders->run([id = id - 3]{
	orders->findDel<0>(ZuFwdTuple("IBM", id),
	  [id](ZmRef<ZdbObject<Order>> o) {
	    if (!o) {
	      ZeLOG(Info, ([id](auto &s) {
		s << "findDel(IBM, " << id << "): (null)";
	      }));
	      return;
	    }
	    ZeLOG(Info, ([id, o](auto &s) {
	      s << "findDel(IBM, " << id << "): " << o->data();
	    }));
	    o->commit();
	    done.post();
	  });
      });

      done.wait();
    }

    db->stop(); // closes all tables

    appMx->stop();
    dbMx->stop();

    ZeLOG(Debug, (ZtString{} << '\n' << ZmHashMgr::csv()));

    orders = {};
    db->final(); // calls Store::final()
    db = {};

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
