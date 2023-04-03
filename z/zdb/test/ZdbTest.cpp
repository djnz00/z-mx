//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuLib.hpp>

#include <limits.h>

#include <zlib/ZmTrap.hpp>
#include <zlib/ZmRandom.hpp>

#include <zlib/ZeLog.hpp>

#include <zlib/ZvCf.hpp>

#include <zlib/Zdb.hpp>

#ifdef _MSC_VER
#pragma warning(disable:4996)
#endif

enum Side { Buy, Sell };

struct Order {
  Side		side;
  char		symbol[32];
  int		price;
  int		quantity;
  // char	pad[1500];
  template <typename S> void print(S &s) const {
    s << "Side: " << (side == Buy ? "Buy" : "Sell") <<
      " Symbol: " << symbol <<
      " Price: " << price <<
      " Qty: " << quantity;
  }
  friend ZuPrintFn ZuPrintType(Order *);
};

using OrderDB = Zdb<Order>;

static void dump(const char *prefix, int op, ZdbAnyPOD *pod)
{
  ZuStringN<200> s;
  s << prefix << ZdbOp::name(op)
    << " RN=" << ZuBoxed(pod->rn())
    << ", prevRN=" << ZuBoxed(pod->prevRN())
    << ", data={" << *(Order *)pod->ptr() << "}}";
  std::cout << s << '\n' << std::flush;
}

ZmRef<OrderDB> orders;
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

void push() {
  unsigned op = opCount++;
  if (op >= nOps) return;
  ZdbRN rn = initRN + skip + op * (stride + chain);
  ZmRef<ZdbPOD<Order> > pod;
  if (append) {
    do {
      ZdbRN prevRN = initRN - nOps + op;
      if (pod = orders->get(prevRN))
	if (!(pod = orders->update(pod, rn))) return;
    } while (!pod);
  } else
    if (!(pod = orders->push(rn))) return;
  Order *order = pod->ptr();
  order->m_side = Buy;
  strncpy(order->m_symbol, "IBM", 32);
  order->m_price = 100;
  order->m_quantity = 100;
  if (!append)
    orders->put(ZdbPOD<Order>::pod(order));
  else
    orders->putUpdate(pod, false);
  if (chain) {
    ++rn;
    for (unsigned i = 0; i < chain; i++) {
      pod = orders->update(pod, rn + i);
      order = pod->ptr();
      ++order->m_price;
      orders->putUpdate(pod, false);
    }
  }
  appMx->add(ZmFn<>::Ptr<&push>::fn());
}

void active() {
  puts("ACTIVE");
  if (del) {
    for (unsigned i = 0; i < del; i++)
      if (ZmRef<ZdbPOD<Order> > pod = orders->get_(i))
	orders->del(pod);
  }
  initRN = orders->nextRN();
  if (append) {
    for (unsigned i = 0; i < nOps; i++) {
      ZmRef<ZdbPOD<Order> > pod = orders->push(initRN++);
      if (!pod) break;
      Order *order = pod->ptr();
      order->m_side = Buy;
      strcpy(order->m_symbol, "IBM");
      order->m_price = 100;
      order->m_quantity = 100;
      orders->put(pod);
    }
  }
  for (unsigned i = 0; i < nThreads; i++) appMx->add(ZmFn<>::Ptr<&push>::fn());
}

void inactive() { puts("INACTIVE"); }

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
    "  -f, --dbs:0:path=PATH\t\t- file path\n"
    "  -p, --dbs:0:preAlloc=N\t- number of records to pre-allocate\n"
    "  -h, --hostID=N\t\t- host ID\n"
    "  -H, --hashOut=FILE\t\t- hash table CSV output file\n"
    "  -d\t\t\t\t- enable debug logging\n"
    "  --nAccepts=N\t\t\t- number of concurrent accepts to listen for\n"
    "  --heartbeatFreq=N\t\t- heartbeat frequency in seconds\n"
    "  --heartbeatTimeout=N\t\t- heartbeat timeout in seconds\n"
    "  --reconnectFreq=N\t\t- reconnect frequency in seconds\n"
    "  --electionTimeout=N\t\t- election timeout in seconds\n"
    "  --orders:cache:bits=N\t\t- bits for cache\n"
    "  --orders:cache:loadFactor=N\t- load factor for cache\n"
    "  --orders:fileHash:bits=N\t- bits for file hash table\n"
    "  --orders:fileHash:loadFactor=N - load factor for file hash table\n"
    "  --orders:fileHash:cBits=N\t- concurrency bits for file hash table\n"
    "  --orders:indexHash:bits=N\t- bits for index hash table\n"
    "  --orders:indexHash:loadFactor=N - load factor for index hash table\n"
    "  --orders:indexHash:cBits=N\t- concurrency bits for index hash table\n"
    "  --orders:lockHash:bits=N\t- bits for lock hash table\n"
    "  --orders:lockHash:loadFactor=N - load factor for lock hash table\n"
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
    "  --hosts:3:down=CMD\t\t- host 2 down command\n"
    "  --cxnHash:bits=N\t\t- bits for cxn hash table\n"
    "  --cxnHash:loadFactor=N\t- load factor for cxn hash table\n"
    "  --cxnHash:cBits=N\t\t- concurrency bits for cxn hash table\n";

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
    { "orders:path", "f", ZvOptScalar, "orders" },
    { "orders:preAlloc", "p", ZvOptScalar, "1" },
    { "orders:cache:bits", 0, ZvOptScalar, "8" },
    { "orders:cache:loadFactor", 0, ZvOptScalar, "1.0" },
    { "orders:fileHash:bits", 0, ZvOptScalar, "8" },
    { "orders:fileHash:loadFactor", 0, ZvOptScalar, "1.0" },
    { "orders:fileHash:cBits", 0, ZvOptScalar, "5" },
    { "orders:indexHash:bits", 0, ZvOptScalar, "8" },
    { "orders:indexHash:loadFactor", 0, ZvOptScalar, "1.0" },
    { "orders:indexHash:cBits", 0, ZvOptScalar, "5" },
    { "orders:lockHash:bits", 0, ZvOptScalar, "8" },
    { "orders:lockHash:loadFactor", 0, ZvOptScalar, "1.0" },
    { "cxnHash:bits", 0, ZvOptScalar, "5" },
    { "cxnHash:loadFactor", 0, ZvOptScalar, "1.0" },
    { "cxnHash:cBits", 0, ZvOptScalar, "5" },
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
      "hosts { 1 { priority 100 IP 127.0.0.1 port 9943 } }\n"
      "hosts { 2 { priority 75 IP 127.0.0.1 port 9944 } }\n"
      "hosts { 3 { priority 50 IP 127.0.0.1 port 9945 } }\n"
      "dbs orders\n"
    );

    if (cf->fromArgs(opts, argc, argv) != 3) usage();

    del = cf->getInt("del", 1, INT_MAX, false, 0);
    skip = cf->getInt("skip", 0, INT_MAX, false, 0);
    stride = cf->getInt("stride", 1, INT_MAX, false, 1);
    append = cf->getInt("append", 0, 1, false, 0);
    chain = cf->getInt("chain", 0, INT_MAX, false, 0);
    nThreads = cf->getInt("1", 1, 1<<10, true);
    nOps = cf->getInt("2", 0, 1<<20, true);
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
	    .debug(cf->getInt("debug", 0, 1, false, 0))
#endif
	    );
    }

    appMx->start();
    if (!dbMx->start()) throw ZtString("multiplexer start failed");

    ZmRef<ZdbEnv> env = new ZdbEnv();

    env->init(ZdbEnvConfig(cf),
      dbMx, ZmFn<>::Ptr<&active>::fn(), ZmFn<>::Ptr<&inactive>::fn());

    orders = new OrderDB(env, "orders", 0, ZdbCacheMode::Normal, ZdbHandler{
	  [](ZdbAny *db, ZmRef<ZdbAnyPOD> &pod) {
	    pod = new ZdbPOD<Order>(db);
	    // new (pod->ptr()) Order();
	  },
	  [](ZdbAnyPOD *pod, int op, bool recovered) {
	    dump(recovered ? "recovered " : "replicated ", op, pod);
	  },
	  [](ZdbAnyPOD *pod, int op) {
	    dump("DC ", op, pod);
	  }
	});

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
