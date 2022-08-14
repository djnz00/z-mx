//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZuLib.hpp>

#include <stdio.h>
#include <signal.h>
#include <stdlib.h>

#include <zlib/ZmSemaphore.hpp>
#include <zlib/ZmSingleton.hpp>
#include <zlib/ZmTrap.hpp>

#include <zlib/ZeLog.hpp>

#include <zlib/ZiMultiplex.hpp>

#include <zlib/ZvCf.hpp>
#include <zlib/ZvMxParams.hpp>

#include <zlib/Zdb.hpp>
#include <zlib/ZdbRep.hpp>

// application context and I/O multiplexer

struct App {
  App() : m_mx(0), m_env(0), m_rep(0), m_insert(0), m_update(0),
	  m_stats(false), m_activated(false) { }
  ~App() { }

  void init(
      ZiMultiplex *mx, ZdbEnv *env, ZdbRep *rep, int i, int u, bool stats) {
    m_mx = mx;
    m_env = env;
    m_rep = rep;
    m_insert = i;
    m_update = u;
    m_stats = stats;
  }

  void active();
  void passive();

  void wait() { m_done.wait(); }
  void timedWait(ZmTime t) { m_done.timedWait(t); }
  void done() { m_done.post(); }

  static App *instance() { return ZmSingleton<App>::instance(); }

  ZiMultiplex	*m_mx;
  ZdbEnv	*m_env;
  ZdbRep	*m_rep;
  int		m_insert;	// # orders to insert
  int		m_update;	// # orders to update
  bool		m_stats;	// whether or not to print BDB stats
  bool		m_activated;	// set once active() has been run
  ZmSemaphore	m_done;		// inform main thread that all work is done
};

enum Side { Buy, Sell };

struct Order {
  uint32_t	m_id;		// order ID - the primary key
  Side		m_side;		// side - the first field of the record value
  char		m_prodID[32];	// product ID
  int		m_price;	// price
  int		m_quantity;	// quantity
};

// the order table - note how the key and value are defined using offsetof

using OrderTable = ZdbTable<uint32_t, offsetof(Order, m_side), Order>;

// dump an individual order

static void dump(Order *o)
{
  printf("ID: %d Side: %s Product ID: %s Price: %d Quantity: %d\n",
	 (int)o->m_id, o->m_side == Buy ? "Buy" : "Sell", o->m_prodID,
	 o->m_price, o->m_quantity);
}

// dump the order table

static void dump(OrderTable *orders)
{
  OrderTable::Iterator i(orders);
  Order o_;
  Order *o;

  while (o = i.iterate(&o_)) dump(o);
}

void usage()
{
  fputs("usage: ZdbRepTest cf insertOrders updateOrders sleep [options]\n"
	"  cf\t\t- configuration file\n"
	"  insertOrders\t- number of orders to insert\n"
	"  updateOrders\t- number of orders to update\n"
	"  sleep\t\t- seconds to sleep at end\n"
	"Options:\n"
	"  --stats\t- dump Zdb statistics\n", stderr);
  Zm::exit(1);
}

struct Abort { };

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4800)
#endif

int main(int argc, char **argv)
{
  ZmRef<ZvCf> cf = new ZvCf();

  static ZvOpt opts[] = {
    { "stats", 0, ZvOptFlag, "0" },
    { 0 }
  };

  int n, u, s;
  bool stats;

  try {
    if (cf->fromArgs(opts, argc, argv) != 5) usage();

    cf->fromFile(cf->get("1"), false);

    n = cf->getInt("2", 0, 1<<20, true),
    u = cf->getInt("3", 0, 1<<20, true),
    s = cf->getInt("4", 0, 1<<20, true);

    stats = cf->getInt("stats", 0, 1, false, 0);
  } catch (const ZvError &e) {
    fputs(e.message().data(), stderr);
    fputc('\n', stderr);
    usage();
  } catch (const ZeError &e) {
    fputs(e.message().data(), stderr);
    fputc('\n', stderr);
    usage();
  } catch (...) {
    usage();
  }

  ZeLog::init("ZdbRepTest");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::debugSink());
  ZeLog::start();

  App *app = App::instance();

  ZmTrap::sigintFn(ZmFn<>::Member<&App::done>::fn(app));
  ZmTrap::trap();

  ZeError e;

  try {
    ZiMultiplex mx(ZvMxParams(cf->subset("mx")));

    if (mx.start(&e) != Zi::OK) throw e;

    try {
      ZdbEnv env(ZdbEnvParams(cf->subset("db", true)));
      ZdbRep rep;

      rep.init(ZdbRepParams(cf->subset("db", true)), &mx, &env,
	       ZmFn<>::Member<&App::active>::fn(app), 
	       ZmFn<>::Member<&App::passive>::fn(app));

      env.open();

      try {
	app->init(&mx, &env, &rep, n, u, stats);

	app->wait();

	puts("waiting..."); fflush(stdout);

	app->timedWait(ZmTimeNow(s));

	mx.stop();

	env.close();
      } catch (...) {
	mx.stop(false);
	env.close();
	throw;
      }
    } catch (...) {
      mx.stop(false);
      throw;
    }
  } catch (const ZdbError &e) {
    fprintf(stderr, "BDB Error in %s - %s\n",
	    e.fn().data(), e.message().data());
    ZeLog::stop();
    Zm::exit(1);
  } catch (const ZvError &e) {
    fputs(e.message().data(), stderr);
    fputc('\n', stderr);
    ZeLog::stop();
    Zm::exit(1);
  } catch (const ZeError &e) {
    fputs(e.message().data(), stderr);
    fputc('\n', stderr);
    ZeLog::stop();
    Zm::exit(1);
  } catch (...) {
    fputs("Unknown Exception\n", stderr);
    ZeLog::stop();
    Zm::exit(1);
  }

  ZeLog::stop();
  return 0;
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif

void App::active()
{
  // don't execute this function twice
  if (m_activated) return;

  printf("active insert: %d update: %d\n", m_insert, m_update);

  m_activated = true;

  try {
    ZmRef<OrderTable> orders = new OrderTable(m_env, "orders", 0);

    // open (create) the orders table
    orders->open();

    // print out all orders
    dump(orders);

    // used to hold the order with the highest ID
    Order m = { 0 };

    ZmTime t(ZmTime::Now);

    for (int i = 0; i < m_insert; i++) {
      ZdbTxn txn(m_env);
      Order o = { 0, Buy, "IBM", 100, 1042 };

      orders->maximum(&m);
      o.m_id = m.m_id + 1;
      // printf("put %d\n", (int)m.m_id + 1); fflush(stdout);
      orders->put(&o);
      txn.commit();
    }

    t = ZmTimeNow() - t;

    printf("insert time: %d %8.4f\n", m_insert, t.dtime()); fflush(stdout);

    t = ZmTimeNow();

    for (int i = 1; i <= m_update; i++) {
      ZdbTxn txn(m_env);
      Order o = { i, Buy, "IBM", 100, 1042 };

      // printf("put %d\n", i); fflush(stdout);
      orders->put(&o);
      txn.commit();
    }

    t = ZmTimeNow() - t;

    printf("update time: %d %8.4f\n", m_update, t.dtime()); fflush(stdout);

    // add an order, but abort the transaction
    try {
      ZdbTxn txn(m_env);
      Order o = { 0, Buy, "IBM", 100, 1042 };
      Order m = { 0 };

      orders->maximum(&m);
      o.m_id = ++m.m_id;
      // printf("put %d\n", (int)m.m_id); fflush(stdout);
      orders->put(&o);
      throw Abort();
    } catch (...) { }

    puts("closing database..."); fflush(stdout);

    // dump statistics
    if (m_stats) {
      DB *db = orders->db();
      DB_ENV *dbEnv = m_env->dbEnv();

      db->stat_print(db, 0);
      dbEnv->stat_print(dbEnv, DB_STAT_ALL);
    }

    // close the database
    orders->close();

    puts("checkpointing..."); fflush(stdout);

    m_env->checkpoint();

    puts("all done"); fflush(stdout);

  } catch (const ZdbError &e) {
    fprintf(stderr, "BDB Error in %s - %s\n",
	    e.fn().data(), e.message().data());
  } catch (const ZvError &e) {
    fputs(e.message().data(), stderr);
    fputc('\n', stderr);
  } catch (const ZeError &e) {
    fputs(e.message().data(), stderr);
    fputc('\n', stderr);
  } catch (...) {
    fputs("Unknown Exception\n", stderr);
  }

  // wake up main thread
  done();
}

void App::passive()
{
  puts("passive");
}
