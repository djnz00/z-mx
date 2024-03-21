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

  friend ZtFieldPrint ZuPrintType(Order *);
};

ZfbFields(Order, fbs::Order,
    (((side)), (Enum, Side::Map), (Ctor<0>)),
    (((symbol)), (String), (Ctor<1>)),
    (((price)), (Int), (Ctor<2>)),
    (((quantity)), (Int), (Ctor<3>)));

// mock row
struct Row {
  ZdbRN		rn;
  ZdbUN		un;
  ZdbSN		sn;
  ZdbVN		vn;
  Order		order;

  static auto RNAxor(const Row &r) { return r.rn; }
  static auto UNAxor(const Row &r) { return r.un; }
};
inline constexpr const char *Row_HeapID() { return "Row"; }
using RowsUN =
  ZmRBTree<Row,
    ZmRBTreeNode<Row,
      ZmRBTreeShadow<true,
	ZmRBTreeKey<Row::UNAxor,
	  ZmRBTreeUnique<true>>>>>;
using Rows =
  ZmRBTree<RowsUN::Node,
    ZmRBTreeNode<RowsUN::Node,
      ZmRBTreeKey<Row::RNAxor,
	ZmRBTreeUnique<true,
	  ZmRBTreeHeapID<Row_HeapID>>>>>;

// mock table
namespace MockTable {
using namespace Zdb_;
using namespace Zdb_::Table_;
struct Table : public Interface {
  DB			*db;
  ZuID			id;
  Rows			rows;
  RowsUN		rowsUN;
  ZtField::Importer	importer = ZtField::importer<Order>();
  ZtField::Exporter	exporter = ZtField::exporter<Order>();

  Table(DB *db_, ZuID id_) : db{db_}, id{id_} { }
protected:
  ~Table() = default;
public:

  void close() { }

  void get(RN rn, GetFn getFn) {
    auto row = rows.find(rn);
    if (row) {
      ZtField::Import import_{importer, &row->order};
      GetData data{
	.un = row->un,
	.sn = row->sn,
	.vn = row->vn,
	.import_ = import_
      };
      getFn(db, rn, GetResult{ZuMv(data)});
    } else {
      getFn(db, rn, GetResult{});
    }
  }

  void recover(UN un, RecoverFn recoverFn) {
    auto row = rowsUN.find(un);
    if (row) {
      ZtField::Import import_{importer, &row->order};
      RecoverData data{
	.rn = row->rn,
	.sn = row->sn,
	.vn = row->vn,
	.import_ = import_
      };
      recoverFn(db, un, RecoverResult{ZuMv(data)});
    } else {
      recoverFn(db, un, RecoverResult{});
    }
  }

  void push(RN rn, UN un, SN sn,
      const void *object, ExportFn exportFn, CommitFn commitFn) {
    if (rowsUN.find(un)) {
      commitFn(db, un, CommitResult{});
      return;
    }
    if (rows.find(rn)) {
      commitFn(db, un, CommitResult{
	ZeMEVENT(Error, ([rn](auto &s, const auto &) {
	  s << rn << " conflicting RN";
	}))
      });
      return;
    }
    auto row = new Rows::Node{rn, un, sn, VN{0}};
    exportFn(object, ZtField::Export{exporter, &row->order});
    rows.addNode(row);
    rowsUN.addNode(row);
    commitFn(db, un, CommitResult{});
  }

  void update(RN rn, UN un, SN sn, VN vn,
      const void *object, ExportFn exportFn, CommitFn commitFn) {
    if (rowsUN.find(un)) {
      commitFn(db, un, CommitResult{});
      return;
    }
    auto row = rows.find(rn);
    if (row) {
      rowsUN.delNode(row);
      exportFn(object, ZtField::Export{exporter, &row->order});
      row->un = un;
      row->sn = sn;
      row->vn = vn;
      rowsUN.addNode(row);
      commitFn(db, un, CommitResult{});
    } else {
      commitFn(db, un, CommitResult{
	ZeMEVENT(Error, ([rn](auto &s, const auto &) {
	  s << rn << " missing RN";
	}))
      });
    }
  }
  void del(RN rn, UN un, SN sn, VN vn, CommitFn commitFn) {
    if (rowsUN.find(un)) {
      commitFn(db, un, CommitResult{});
      return;
    }
    auto row = rows.find(rn);
    if (row) {
      rowsUN.delNode(row);
      rows.delNode(row);
      commitFn(db, un, CommitResult{});
    } else {
      commitFn(db, un, CommitResult{
	ZeMEVENT(Error, ([rn](auto &s, const auto &) {
	  s << rn << " missing RN";
	}))
      });
    }
  }
};
} // MockTable
using Table = MockTable::Table;
inline ZuID Table_IDAxor(const Table &table) { return table.id; }
inline constexpr const char *Tables_HeapID() { return "Tables"; }
using Tables =
  ZmHash<Table,
    ZmHashNode<Table,
      ZmHashFinal<true,
	ZmHashKey<Table_IDAxor,
	  ZmHashLock<ZmPLock,
	    ZmHashHeapID<Tables_HeapID>>>>>>;
// mock data store
namespace MockStore {
using namespace Zdb_;
using namespace Zdb_::Store_;
using Zdb_::Store_::ScanFn; // disambiguate
struct Store : public Interface, public ZmPolymorph {
  ZmRef<Tables>	tables;

  InitResult init(ZvCf *, LogFn) {
    tables = new Tables{};
    return {InitData{ .replicated = false }};
  }
  void final() { tables->clean(); tables = nullptr; }

  void open(
      DB *db,
      ZuID id,
      ZtMFieldArray fields,
      OpenFn openFn,
      ScanFn) {
    Tables::Node *table = tables->find(id);
    if (table) {
      openFn(db, OpenResult{ZeMEVENT(Error, ([id](auto &s, const auto &) {
	s << "table " << id << " already open";
      }))});
      return;
    }
    table = new Tables::Node{db, id};
    tables->addNode(table);
    openFn(db, OpenResult{OpenData{
      .table = table,
      .rn = ZdbNullRN(),
      .un = ZdbNullUN(),
      .sn = ZdbNullSN()
    }});
  }
};
} // MockStore
using Store = MockStore::Store;
using StoreInterface = MockStore::Interface;

// environment and mock data store
ZmRef<Store> store;
ZmRef<ZdbEnv> env;

// database
ZmRef<Zdb> orders;

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
    "usage: ZdbTest ...\n";

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
      "dbs {\n"
      "  orders { }\n"
      "}\n"
      "debug 1\n"
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

  ZmTrap::sigintFn(ZmFn<>::Ptr<&sigint>::fn());
  ZmTrap::trap();

  try {
    ZeError e;

    {
      appMx = new ZmScheduler(ZmSchedParams().nThreads(1));
      dbMx = new ZiMultiplex(
	  ZiMxParams()
	    .scheduler([](auto &s) {
	      s.nThreads(4)
	      .thread(1, [](auto &t) { t.isolated(true); })
	      .thread(2, [](auto &t) { t.isolated(true); })
	      .thread(3, [](auto &t) { t.isolated(true); }); })
	    .rxThread(1).txThread(2)
#ifdef ZiMultiplex_DEBUG
	    .debug(cf->getBool("debug"))
#endif
	    );
    }

    appMx->start();
    if (!dbMx->start()) throw ZeEVENT(Fatal, "multiplexer start failed");

    store = new Store();
    env = new ZdbEnv();

    env->init(ZdbEnvCf(cf), dbMx, ZdbEnvHandler{
	.upFn = [](ZdbEnv *, ZdbHost *host) {
	  ZeLOG(Info, ([id = host ? host->id() : ZuID{"unset"}](auto &s) {
	    s << "ACTIVE (was " << id << ')';
	  }));
	},
	.downFn = [](ZdbEnv *) { ZeLOG(Info, "INACTIVE"); }
    }, store);

    orders = env->initDB<Order>("orders"); // might throw

    env->start();

    ZdbRN rn;

    orders->run([&rn]{
      orders->push<Order>([&rn](ZdbObject<Order> *o) {
	new (o->ptr()) Order{Side::Buy, "IBM", 100, 100};
	o->put();
	rn = o->rn();
	ZeLOG(Info, ([rn](auto &s) { s << "RN: " << rn; }));
      });
    });

    orders->run([&rn]{
      orders->get<Order>(rn, [](ZmRef<ZdbObject<Order>> o) {
	if (!o)
	  ZeLOG(Info, "get(): (null)");
	else
	  ZeLOG(Info, ([o = ZuMv(o)](auto &s) {
	    s << "get(): " << o->data();
	  }));
      });
      done.post();
    });

    done.wait();

    env->stop();

    appMx->stop();
    dbMx->stop();

    ZeLOG(Debug, (ZtString{} << '\n' << ZmHashMgr::csv()));

    orders = {};
    env->final(); // calls Store::final()
    env = {};
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
