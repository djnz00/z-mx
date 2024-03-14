//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuLib.hpp>

#include <limits.h>

#include <zlib/ZmTrap.hpp>

#include <zlib/ZtEnum.hpp>

#include <zlib/ZeLog.hpp>

#include <zlib/ZvCf.hpp>

#include <zlib/Zdb.hpp>

#include "zdbtest_fbs.h"

#ifdef _MSC_VER
#pragma warning(disable:4996)
#endif

using namespace zdbtest;

namespace Side {
  ZfbEnumValues(Side, Buy, Sell);
};

struct Order {
  int			side;
  ZuStringN<32>		symbol;
  int			price;
  int			quantity;
};

ZfbFields(Order, fbs::Order,
    (((side)), (Enum, Side::Map), (Ctor(0))),
    (((symbol)), (String), (Ctor(1))),
    (((price)), (Int), (Ctor(2))),
    (((quantity)), (Int), (Ctor(3))));

ZmRef<Zdb> orders;

#if 0
TestSeq(ZdbRN rn, TestLoop l0, l1, ...) // sequence of op loops
  // rn += range.size(), ...
  RN run(RN rn) { rn = l0.run(rn), rn = l1.run(rn), ... }

TestPlan(unsigned n, TestSeq seq)
  RN run(RN rn) { for (unsigned i = 0, i < n, i++) rn = seq.run(rn); }

#endif

ZmSemaphore done;

ZmScheduler *appMx = 0;
ZiMultiplex *dbMx = 0;
unsigned del = 0;
bool append = false;
unsigned skip = 0;
unsigned stride = 1;
unsigned chain = 0;
unsigned nThreads = 1;
ZdbRN initRN;
unsigned nOps = 0;
ZmAtomic<unsigned> opCount = 0;

void sigint()
{
  std::cerr << "SIGINT\n" << std::flush;
  done.post();
}

ZmRef<ZvCf> inlineCf(ZuString s)
{
  ZmRef<ZvCf> cf = new ZvCf();
  cf->fromString(s);
  return cf;
}

void initOrder(Order *order) {
  order->side = Side::Buy;
  order->symbol = "IBM";
  order->price = 100;
  order->quantity = 100;
}

void updateOrder(Order *order) {
  ++order->price;
}

ZdbRN TestStep::run(ZdbRN rn) const
{
  using ObjRef = ZmRef<ZdbObject<Order>>;
  ObjRef object;
  for (unsigned i = 0; i < repeat; i++) {
    if (push) {
      if (!ZmBlock<bool>{}([rn, &object](auto wake) mutable {
	orders->invoke([rn, &object, wake = ZuMv(wake)]() mutable {
	  orders->push(rn,
	      [&object, wake = ZuMv(wake)](ObjRef object_) mutable {
	    if (!object_) { wake(false); return; }
	    object = ZuMv(object_);
	    initOrder(object->ptr());
	    object->put();
	    wake(true);
	  });
	});
      })) return rn;
      ++rn;
    }
    for (unsigned j = 0; j < append; j++) {
      if (!ZmBlock<bool>{}([rn, &object](auto wake) mutable {
	orders->invoke([rn, &object, wake = ZuMv(wake)]() mutable {
	  orders->update(object, rn,
	      [wake = ZuMv(wake)](ObjRef object) mutable {
	    if (!object) { wake(false); return; }
	    updateOrder(object->ptr());
	    object->append();
	    wake(true);
	  });
	});
      })) return rn;
      ++rn;
    }
    if (del) {
      if (!ZmBlock<bool>{}([rn, &object](auto wake) mutable {
	orders->invoke([rn, &object, wake = ZuMv(wake)]() mutable {
	  orders->update(object, rn,
	      [wake = ZuMv(wake)](ObjRef object) mutable {
	    if (!object) { wake(false); return; }
	    object->del();
	    wake(true);
	  });
	});
      })) return rn;
      ++rn;
    }
  }
  return rn;
}

void active(ZdbEnv *, ZdbHost *) {
  puts("ACTIVE");
  initRN = orders->nextRN();
  for (unsigned i = 0; i < nThreads; i++) {
    // FIXME
    appMx->add(ZmFn<>::Ptr<&push>::fn());
  }
}

void inactive(ZdbEnv *) {
  puts("INACTIVE");
}

void usage()
{
  static const char *help =
    "usage: ZdbTest nThreads nOps [OPTION]...\n\n"
    "Options:\n"
    "  -D, --del=N\t\t\t- delete first N sequences\n"
    "  -k, --skip=N\t\t\t- skip N RNs before first\n"
    "  -s, --stride=N\t\t- increment RNs by N with each operation\n"
    "  -h, --hostID=N\t\t- host ID\n"
    "  -H, --hashOut=FILE\t\t- hash table CSV output file\n"
    "  -d\t\t\t\t- enable debug logging\n"
    "  --nAccepts=N\t\t\t- number of concurrent accepts to listen for\n"
    "  --heartbeatFreq=N\t\t- heartbeat frequency in seconds\n"
    "  --heartbeatTimeout=N\t\t- heartbeat timeout in seconds\n"
    "  --reconnectFreq=N\t\t- reconnect frequency in seconds\n"
    "  --electionTimeout=N\t\t- election timeout in seconds\n"
    "  --hosts.1.priority=N\t\t- host 0 priority\n"
    "  --hosts.1.IP=N\t\t- host 0 IP\n"
    "  --hosts.1.port=N\t\t- host 0 port\n"
    "  --hosts.1.up=CMD\t\t- host 0 up command\n"
    "  --hosts.1.down=CMD\t\t- host 0 down command\n"
    "  --hosts.2.priority=N\t\t- host 1 priority\n"
    "  --hosts.2.IP=N\t\t- host 1 IP\n"
    "  --hosts.2.port=N\t\t- host 1 port\n"
    "  --hosts.2.up=CMD\t\t- host 1 up command\n"
    "  --hosts.2.down=CMD\t\t- host 1 down command\n"
    "  --hosts.3.priority=N\t\t- host 2 priority\n"
    "  --hosts.3.IP=N\t\t- host 2 IP\n"
    "  --hosts.3.port=N\t\t- host 2 port\n"
    "  --hosts.3.up=CMD\t\t- host 2 up command\n"
    "  --hosts.3.down=CMD\t\t- host 2 down command\n";

  std::cerr << help << std::flush;
  Zm::exit(1);
}

int main(int argc, char **argv)
{
  static ZvOpt opts[] = {
    { "del", "D", ZvOptValue },
    { "skip", "k", ZvOptValue, "0" },
    { "stride", "s", ZvOptValue, "1" },
    { "hostID", "h", ZvOptValue, "0" },
    { "hashOut", "H", ZvOptValue },
    { "debug", "d", ZvOptFlag },
    { "hosts.1.priority", 0, ZvOptValue, "100" },
    { "hosts.1.IP", 0, ZvOptValue, "127.0.0.1" },
    { "hosts.1.port", 0, ZvOptValue, "9943" },
    { "hosts.1.up", 0, ZvOptValue },
    { "hosts.1.down", 0, ZvOptValue },
    { "hosts.2.priority", 0, ZvOptValue },
    { "hosts.2.IP", 0, ZvOptValue },
    { "hosts.2.port", 0, ZvOptValue },
    { "hosts.2.up", 0, ZvOptValue },
    { "hosts.2.down", 0, ZvOptValue },
    { "hosts.3.priority", 0, ZvOptValue },
    { "hosts.3.IP", 0, ZvOptValue },
    { "hosts.3.port", 0, ZvOptValue },
    { "hosts.3.up", 0, ZvOptValue },
    { "hosts.3.down", 0, ZvOptValue },
    { "nAccepts", 0, ZvOptValue },
    { "heartbeatFreq", 0, ZvOptValue },
    { "heartbeatTimeout", 0, ZvOptValue },
    { "reconnectFreq", 0, ZvOptValue },
    { "electionTimeout", 0, ZvOptValue },
    { 0 }
  };

  ZmRef<ZvCf> cf;
  ZuString hashOut;

  try {
    cf = inlineCf(
      "hostID 1\n"
      "hosts {\n"
      "  1 { priority 100 IP 127.0.0.1 port 9943 }\n"
      "  2 { priority 75 IP 127.0.0.1 port 9944 }\n"
      "  3 { priority 50 IP 127.0.0.1 port 9945 }\n"
      "}\n"
      "dbs {\n"
      "  orders { }\n"
      "}\n"
    );

    if (cf->fromArgs(opts, argc, argv) != 3) usage();

    del = cf->getInt("del", 1, INT_MAX, 0);
    skip = cf->getInt("skip", 0, INT_MAX, 0);
    stride = cf->getInt("stride", 1, INT_MAX, 1);
    nThreads = cf->getInt<true>("1", 1, 1<<10);
    nOps = cf->getInt<true>("2", 0, 1<<20);
    hashOut = cf->get("hashOut");

  } catch (const ZvError &e) {
    std::cerr << e << '\n' << std::flush;
    usage();
  } catch (const ZeError &e) {
    std::cerr << e << '\n' << std::flush;
    usage();
  } catch (...) {
    usage();
  }

  ZeLog::init("ZdbTest");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::debugSink());
  ZeLog::start();

  ZmTrap::sigintFn(ZmFn<>::Ptr<&sigint>::fn());
  ZmTrap::trap();

  try {
    ZeError e;

    {
      appMx = new ZmScheduler(ZmSchedParams().nThreads(nThreads));
      dbMx = new ZiMultiplex(
	  ZiMxParams()
	    .scheduler([](auto &s) {
	      s.nThreads(4)
	      .thread(1, [](auto &t) { t.isolated(1); })
	      .thread(2, [](auto &t) { t.isolated(1); })
	      .thread(3, [](auto &t) { t.isolated(1); }); })
	    .rxThread(1).txThread(2)
#ifdef ZiMultiplex_DEBUG
	    .debug(cf->getBool("debug"))
#endif
	    );
    }

    appMx->start();
    if (!dbMx->start()) throw ZtString("multiplexer start failed");

    ZmRef<ZdbEnv> env = new ZdbEnv();

    env->init(ZdbEnvConfig(cf), dbMx, EnvHandler{
      .upFn = &active, .downFn = &inactive});

    orders = env->initDB<Order>("orders",
	[](AnyObject *object) { },
	[](ZdbRN rn) { });

    if (!env->open()) throw ZtString{} << "Zdb open failed";
    env->start();

    done.wait();

    env->checkpoint();

    env->stop();
    env->close();

    appMx->stop();
    dbMx->stop();

    env->final();

  } catch (const ZvError &e) {
    std::cerr << e << '\n' << std::flush;
    ZeLog::stop();
    Zm::exit(1);
  } catch (const ZeError &e) {
    std::cerr << e << '\n' << std::flush;
    ZeLog::stop();
    Zm::exit(1);
  } catch (const ZtString &s) {
    std::cerr << s << '\n' << std::flush;
    ZeLog::stop();
    Zm::exit(1);
  } catch (...) {
    std::cerr << "Unknown Exception\n" << std::flush;
    ZeLog::stop();
    Zm::exit(1);
  }

  if (appMx) delete appMx;
  if (dbMx) delete dbMx;

  ZeLog::stop();

  if (hashOut) {
    FILE *f = fopen(hashOut, "w");
    if (!f)
      perror(hashOut);
    else {
      ZtString csv;
      csv << ZmHashMgr::csv();
      fwrite(csv.data(), 1, csv.length(), f);
    }
    fclose(f);
  }

  return 0;
}
