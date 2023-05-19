//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

#include <zlib/ZmTrap.hpp>

#include <zlib/ZeLog.hpp>

#include <zlib/ZvCf.hpp>

#include <zlib/Zdb.hpp>

enum Side { Buy, Sell };

struct Abort { };

struct Order {
  uint32_t	m_id;
  Side		m_side;
  char		m_prodID[32];
  int		m_price;
  int		m_quantity;
};

using OrderTable = ZdbTable<uint32_t, offsetof(Order, m_side), Order>;

static void dump(Order *o)
{
  printf("ID: %d Side: %s Product ID: %s Price: %d Quantity: %d\n",
	 (int)o->m_id, o->m_side == Buy ? "Buy" : "Sell", o->m_prodID,
	 o->m_price, o->m_quantity);
}

static void dump(OrderTable *orders)
{
  OrderTable::Iterator i(orders);
  Order o_;
  Order *o;

  while (o = i.iterate(&o_)) dump(o);
}

void usage()
{
  fputs("usage: ZdbBTreeTest cf insertOrders updateOrders\n"
	"  cf\t\t- configuration file\n"
	"  insertOrders\t- number of orders to insert\n"
	"  updateOrders\t- number of orders to update\n", stderr);
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

  int n, u;

  try {
    if (cf->fromArgs(opts, argc, argv) != 4) usage();

    cf->fromFile(cf->get("1"), false);

    n = cf->getInt("2", true, 0, 1<<20);
    u = cf->getInt("3", true, 0, 1<<20);
  } catch (const ZvError &e) {
    fputs(e.message().data(), stderr);
    fputc('\n', stderr);
    usage();
  } catch (...) {
    usage();
  }

  ZeLog::init("ZdbBTreeTest");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::debugSink());
  ZeLog::start();

  ZmTrap::sigintFn(ZmFn<>::Ptr<&sigint>::fn());
  ZmTrap::trap();

  try {
    ZdbEnv env(ZdbEnvParams(cf->subset("db", true)));

    env.open();

    ZmRef<OrderTable> orders = new OrderTable(&env, "orders", 0);

    orders->open();

    dump(orders);

    Order m = { 0 };

    ZmTime t(ZmTime::Now);

    for (int i = 0; i < n; i++) {
      ZdbTxn txn(&env);
      Order o = { 0, Buy, "IBM", 100, 1042 };

      orders->maximum(&m);
      o.m_id = m.m_id + 1;
      orders->put(&o);
      txn.commit();
    }

    t = ZmTimeNow() - t;

    printf("insert time: %d %.6f\n", n, t.dtime());

    t = ZmTimeNow();

    for (int i = 1; i <= u; i++) {
      ZdbTxn txn(&env);
      Order o = { i, Buy, "IBM", 100, 1042 };

      orders->put(&o);
      txn.commit();
    }

    t = ZmTimeNow() - t;

    printf("update time: %d %.6f\n", u, t.dtime());

    try {
      ZdbTxn txn(&env);
      Order o = { 0, Buy, "IBM", 200, 1042 };
      Order m = { 0 };

      orders->maximum(&m);
      o.m_id = m.m_id + 1;
      orders->put(&o);
      throw Abort();
    } catch (...) { }

    { DB *db = orders->db(); db->stat_print(db, 0); }
    { DB_ENV *dbEnv = env.dbEnv(); dbEnv->stat_print(dbEnv, DB_STAT_ALL); }

    orders->close();
    orders = 0;

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
