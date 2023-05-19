//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZmTrap.hpp>

#include <zlib/ZeLog.hpp>

#include <zlib/ZvCf.hpp>

#include <zlib/Zdb.hpp>

enum Side { Buy, Sell };

struct Abort { };

struct Order {
  Side		m_side;
  char		m_prodID[32];
  int		m_price;
  int		m_quantity;
  char		m_pad[1500];
};

using OrderQueue = ZdbQueue<Order>;

static void dump(db_recno_t id, Order *o)
{
  printf("ID: %d Side: %s Product ID: %s Price: %d Quantity: %d\n",
	 (int)id, o->m_side == Buy ? "Buy" : "Sell", o->m_prodID,
	 o->m_price, o->m_quantity);
}

static void dump(OrderQueue *orders)
{
  OrderQueue::Iterator i(orders);
  db_recno_t id;
  Order o;

  while (id = i.iterate(&o)) dump(id, &o);
}

void usage()
{
  fputs("usage: ZdbQueueTest cf pushOrders updateOrders pushbackOrders\n"
	"  cf\t\t\t- configuration file\n"
	"  pushOrders\t\t- number of orders to insert\n"
	"  updateOrders\t\t- number of orders to update\n"
	"  pushbackOrders\t- number of orders to push back\n", stderr);
  Zm::exit(1);
}

void sigint()
{
  fputs("SIGINT\n", stderr);
  Zm::exit(1);
}

int main(int argc, char **argv)
{
  ZmRef<ZvCf> cf = new ZvCf();

  static ZvOpt opts[] = { { 0 } };

  int n, u, p;

  try {
    if (cf->fromArgs(opts, argc, argv) != 5) usage();

    cf->fromFile(cf->get("1"), false);

    n = cf->getInt("2", true, 0, 1<<20);
    u = cf->getInt("3", true, 0, 1<<20);
    p = cf->getInt("4", true, 0, 1<<20);

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

  ZeLog::init("ZdbBQueueTest");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::debugSink());
  ZeLog::start();

  ZmTrap::sigintFn(ZmFn<>::Ptr<&sigint>::fn());
  ZmTrap::trap();

  try {
    ZdbEnv env(ZdbEnvParams(cf->subset("db", true)));

    env.open();

    ZmRef<OrderQueue> orders = new OrderQueue(&env, "orders", cf);

    orders->open();

    dump(orders);

    ZmTime t(ZmTime::Now);

    for (int i = 0; i < n; i++) {
      Order o = { Buy, "IBM", 100, 1042 };

      orders->push(&o);
    }

    t = ZmTimeNow() - t;

    printf("push time: %d %.6f %.6f\n", n, t.dtime(), t.dtime() / n);

    t = ZmTimeNow();

    for (int i = 1; i <= u; i++) {
      Order o = { Buy, "IBM", 100, 1042 };

      orders->put(i, &o);
    }

    t = ZmTimeNow() - t;

    printf("put time: %d %.6f %.6f\n", u, t.dtime(), t.dtime() / n);

    t = ZmTimeNow();

    for (int i = 0; i < n; i++) {
      Order o;

      orders->shift(&o);
    }

    t = ZmTimeNow() - t;

    printf("get time: %d %.6f\n", n, t.dtime());

    for (int i = 0; i < n; i++) {
      Order o = { Buy, "IBM", 100, 1042 };

      orders->push(&o);
    }

    t = ZmTimeNow();

    if (p) {
      Order *o = new Order[p];
      int *id = new int[p];
      for (int i = 0; i < n; i += p) {
	for (int j = 0; j < p; j++)
	  id[j] = orders->shift(&o[j]);
	for (int j = p; (j -= 2) >= 0; ) {
	  o[j].m_price += 10;
	  orders->put(id[j] , &o[j]);
	}
      }
      delete [] o;
      delete [] id;
    }

    t = ZmTimeNow() - t;

    printf("pushback time: %d %.6f %.6f", n, t.dtime(), t.dtime() / n);

    { DB *db = orders->db(); db->stat_print(db, 0); }
    { DB_ENV *dbEnv = env.dbEnv(); dbEnv->stat_print(dbEnv, DB_STAT_ALL); }

    orders->close();
    orders = 0;

    ZmRef<ZdbQueue<ZuNull> > nullq = new ZdbQueue<ZuNull>(&env, 0);

    nullq->open();

    nullq->put(11);
    nullq->put(12);
    nullq->put(42);
    nullq->put(1);
    nullq->put(30);
    for (int i = 0; i < 5; i++) printf("%d\n", (int)nullq->shift());
    nullq->close();

    env.checkpoint();

    env.close();
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
