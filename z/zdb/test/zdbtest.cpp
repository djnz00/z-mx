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

namespace Side {
  ZfbEnumValues(Side, Buy, Sell);
};

struct Order {
  Side			side;
  ZuStringN<32>		symbol[32];
  int			price;
  int			quantity;
};

ZfbFields(Order, fbs::Order,
    (((side)), (Enum, Side::Map), (Ctor(0))),
    (((symbol)), (String), (Ctor(1))),
    (((price)), (Int), (Ctor(2))),
    (((quantity)), (Int), (Ctor(3))));

using OrderDB = Zdb<Order>;

ZmRef<OrderDB> orders;

struct TestStep {
  unsigned	repeat;
  bool		push;
  unsigned	append;
  bool		del;

  RN size() const { return repeat * (push + append + del); }
  RN run(RN rn) const;
};

ZtFields(TestStep,
    (((repeat)), (Int, 1), (Ctor(0))),
    (((push)), (Bool), (Ctor(1))),
    (((append)), (Int, 0), (Ctor(2))),
    (((del)), (Bool), (Ctor(3))));

struct TestSeq {
  ZtArray<TestStep>	steps;

  TestSeq(const ZvCf *cf) {
    cf->all(
    cf->ctor<TestStep>(steps.push());
  }

  RN size() const {
    RN n = 0;
    steps.all([&n](const TestStep &step) { n += step.size(); });
    return n;
  }
  RN run(RN rn) const {
    steps.all([&rn](const TestStep &step) { rn = step.run(rn); });
    return rn;
  }
};

struct TestPlan {
  ZtArray<TestSeq>	sequences;

  TestPlan(const ZvCf *cf) {
    // FIXME
  }

  RN size() const {
    RN n = 0;
    sequences.all([&n](const TestSeq &seq) { n += seq.size(); });
    return n;
  }
  RN run(RN rn) const {
    sequences.all([&rn](const TestSeq &seq) { rn = seq.run(rn); });
    return rn;
  }
};

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
  cf->fromString(s, false);
  return cf;
}

void updateOrder(Order *order) {
  order->m_side = Buy;
  strncpy(order->m_symbol, "IBM", 32);
  order->m_price = 100;
  order->m_quantity = 100;
}

void push() {
  unsigned op = opCount++;
  if (op >= nOps) return;
  ZdbRN rn = initRN + skip + op * (stride + chain);
  using ObjRef = ZmRef<ZdbObject<Order>>;
  ObjRef object;
  if (append) {
    while (ZmBlock<bool>{}(
	  [prevRN = initRN - nOps + op, &object](auto wake) mutable {
      orders->invoke([prevRN, &object, wake = ZuMv(wake)]() mutable {
	orders->update(prevRN, [&object, wake = ZuMv(wake)](ObjRef object_) {
	  if (!object_) { wake(false); return; }
	  object = ZuMv(object_);
	  updateOrder(object->ptr());
	  object->append();
	  wake(true);
	});
      });
    }));
    if (!object) return;
  } else {
    if (!ZmBlock<bool>{}([rn, &object])(auto wake) mutable {
      orders->invoke([rn, &object, wake = ZuMv(wake)]() mutable {
	orders->push(rn, [&object, wake = ZuMv(wake)](ObjRef object_) mutable {
	  if (!object_) { wake(false); return; }
	  object = ZuMv(object_);
	  updateOrder(object->ptr());
	  object->put();
	  wake(true);
	});
      });
    }) return;
  }
  if (chain) {
    ++rn;
    for (unsigned i = 0; i < chain; i++) {
      object = orders->update(object, rn + i);
      order = object->ptr();
      ++order->m_price;
      orders->putUpdate(object, false);
    }
  }
  appMx->add(ZmFn<>::Ptr<&push>::fn());
}

// FIXME
void active(ZdbEnv *, ZdbHost *) {
  puts("ACTIVE");
  if (del) {
    for (unsigned i = 0; i < del; i++)
      if (ZmRef<ZdbObject<Order> > object = orders->get_(i))
	orders->del(object);
  }
  initRN = orders->nextRN();
  if (append) {
    for (unsigned i = 0; i < nOps; i++) {
      ZmRef<ZdbObject<Order> > object = orders->push(initRN++);
      if (!object) break;
      Order *order = object->ptr();
      order->m_side = Buy;
      strcpy(order->m_symbol, "IBM");
      order->m_price = 100;
      order->m_quantity = 100;
      orders->put(object);
    }
  }
  for (unsigned i = 0; i < nThreads; i++) appMx->add(ZmFn<>::Ptr<&push>::fn());
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
    "  -a, --append\t\t\t- append\n"
    "  -c, --chain=N\t\t\t- append chains of N records\n"
    "  -f, --dbs:orders:path=PATH\t\t- file path\n"
    "  -p, --dbs:orders:preAlloc=N\t- number of records to pre-allocate\n"
    "  -h, --hostID=N\t\t- host ID\n"
    "  -H, --hashOut=FILE\t\t- hash table CSV output file\n"
    "  -d\t\t\t\t- enable debug logging\n"
    "  --nAccepts=N\t\t\t- number of concurrent accepts to listen for\n"
    "  --heartbeatFreq=N\t\t- heartbeat frequency in seconds\n"
    "  --heartbeatTimeout=N\t\t- heartbeat timeout in seconds\n"
    "  --reconnectFreq=N\t\t- reconnect frequency in seconds\n"
    "  --electionTimeout=N\t\t- election timeout in seconds\n"
    "  --hosts:1:priority=N\t\t- host 0 priority\n"
    "  --hosts:1:IP=N\t\t- host 0 IP\n"
    "  --hosts:1:port=N\t\t- host 0 port\n"
    "  --hosts:1:up=CMD\t\t- host 0 up command\n"
    "  --hosts:1:down=CMD\t\t- host 0 down command\n"
    "  --hosts:2:priority=N\t\t- host 1 priority\n"
    "  --hosts:2:IP=N\t\t- host 1 IP\n"
    "  --hosts:2:port=N\t\t- host 1 port\n"
    "  --hosts:2:up=CMD\t\t- host 1 up command\n"
    "  --hosts:2:down=CMD\t\t- host 1 down command\n"
    "  --hosts:3:priority=N\t\t- host 2 priority\n"
    "  --hosts:3:IP=N\t\t- host 2 IP\n"
    "  --hosts:3:port=N\t\t- host 2 port\n"
    "  --hosts:3:up=CMD\t\t- host 2 up command\n"
    "  --hosts:3:down=CMD\t\t- host 2 down command\n";

  std::cerr << help << std::flush;
  Zm::exit(1);
}

int main(int argc, char **argv)
{
  static ZvOpt opts[] = {
    { "del", "D", ZvOptScalar },
    { "skip", "k", ZvOptScalar, "0" },
    { "stride", "s", ZvOptScalar, "1" },
    { "append", "a", ZvOptFlag },
    { "chain", "c", ZvOptScalar, "0" },
    { "dbs:orders:path", "f", ZvOptScalar, "orders" },
    { "dbs:orders:preAlloc", "p", ZvOptScalar, "1" },
    { "hostID", "h", ZvOptScalar, "0" },
    { "hashOut", "H", ZvOptScalar },
    { "debug", "d", ZvOptFlag },
    { "hosts:1:priority", 0, ZvOptScalar, "100" },
    { "hosts:1:IP", 0, ZvOptScalar, "127.0.0.1" },
    { "hosts:1:port", 0, ZvOptScalar, "9943" },
    { "hosts:1:up", 0, ZvOptScalar },
    { "hosts:1:down", 0, ZvOptScalar },
    { "hosts:2:priority", 0, ZvOptScalar },
    { "hosts:2:IP", 0, ZvOptScalar },
    { "hosts:2:port", 0, ZvOptScalar },
    { "hosts:2:up", 0, ZvOptScalar },
    { "hosts:2:down", 0, ZvOptScalar },
    { "hosts:3:priority", 0, ZvOptScalar },
    { "hosts:3:IP", 0, ZvOptScalar },
    { "hosts:3:port", 0, ZvOptScalar },
    { "hosts:3:up", 0, ZvOptScalar },
    { "hosts:3:down", 0, ZvOptScalar },
    { "nAccepts", 0, ZvOptScalar },
    { "heartbeatFreq", 0, ZvOptScalar },
    { "heartbeatTimeout", 0, ZvOptScalar },
    { "reconnectFreq", 0, ZvOptScalar },
    { "electionTimeout", 0, ZvOptScalar },
    { 0 }
  };

  ZmRef<ZvCf> cf;
  ZuString hashOut;

  try {
    cf = inlineCf(
      "fileThread 3\n"
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

    del = cf->getInt("del", 0, 1, INT_MAX);
    skip = cf->getInt("skip", 0, 0, INT_MAX);
    stride = cf->getInt("stride", 1, 1, INT_MAX);
    append = cf->getInt("append", 0, 0, 1);
    chain = cf->getInt("chain", 0, 0, INT_MAX);
    nThreads = cf->getInt("1", true, 1, 1<<10);
    nOps = cf->getInt("2", true, 0, 1<<20);
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
	    .debug(cf->getInt("debug", 0, 0, 1))
#endif
	    );
    }

    appMx->start();
    if (!dbMx->start()) throw ZtString("multiplexer start failed");

    ZmRef<ZdbEnv> env = new ZdbEnv();

    env->init(ZdbEnvConfig(cf), dbMx, EnvHandler{
      .upFn = &active, .downFn = &inactive});

    orders = env->initDB<Order>("orders");

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
