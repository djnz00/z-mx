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

ZmXRing<ZmFn<>, ZmXRingLock<ZmPLock>> work, callbacks;

bool deferWork = false, deferCallbacks = false;

void performWork() { while (auto fn = work.shift()) fn(); }
void performCallbacks() { while (auto fn = callbacks.shift()) fn(); }

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
    auto work_ = [this, rn, getFn = ZuMv(getFn)]() mutable {
      auto row = rows.find(rn);
      if (row) {
	ZtField::Import import_{importer, &row->order};
	GetData data{
	  .un = row->un,
	  .sn = row->sn,
	  .vn = row->vn,
	  .import_ = import_
	};
	auto callback = [
	  this, rn, getFn = ZuMv(getFn), result = GetResult{ZuMv(data)}
	]() mutable {
	  getFn(db, rn, ZuMv(result));
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
      } else {
	auto callback = [this, rn, getFn = ZuMv(getFn)]() mutable {
	  getFn(db, rn, GetResult{});
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
      }
    };
    deferWork ? work.push(ZuMv(work_)) : work_();
  }

  void recover(UN un, RecoverFn recoverFn) {
    auto work_ = [this, un, recoverFn = ZuMv(recoverFn)]() mutable {
      auto row = rowsUN.find(un);
      if (row) {
	ZtField::Import import_{importer, &row->order};
	RecoverData data{
	  .rn = row->rn,
	  .sn = row->sn,
	  .vn = row->vn,
	  .import_ = import_
	};
	auto callback = [
	  this, un, recoverFn = ZuMv(recoverFn),
	  result = RecoverResult{ZuMv(data)}
	]() mutable {
	  recoverFn(db, un, ZuMv(result));
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
      } else {
	auto callback = [this, un, recoverFn = ZuMv(recoverFn)]() mutable {
	  recoverFn(db, un, RecoverResult{});
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
      }
    };
    deferWork ? work.push(ZuMv(work_)) : work_();
  }

  void push(RN rn, UN un, SN sn,
      const void *object, ExportFn exportFn, CommitFn commitFn) {
    auto work_ = [
      this, rn, un, sn, object,
      exportFn=ZuMv(exportFn),
      commitFn=ZuMv(commitFn)
    ]() mutable {
      if (rowsUN.find(un)) {
	auto callback = [this, un, commitFn = ZuMv(commitFn)] {
	  commitFn(db, un, CommitResult{});
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
	return;
      }
      if (rows.find(rn)) {
	auto callback = [this, rn, un, commitFn = ZuMv(commitFn)] {
	  commitFn(db, un, CommitResult{
	    ZeMEVENT(Error, ([rn](auto &s, const auto &) {
	      s << rn << " conflicting RN";
	    }))
	  });
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
	return;
      }
      auto row = new Rows::Node{rn, un, sn, VN{0}};
      exportFn(object, ZtField::Export{exporter, &row->order});
      rows.addNode(row);
      rowsUN.addNode(row);
      auto callback = [this, un, commitFn = ZuMv(commitFn)] {
	commitFn(db, un, CommitResult{});
      };
      deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
    };
    deferWork ? work.push(ZuMv(work_)) : work_();
  }

  void update(RN rn, UN un, SN sn, VN vn,
      const void *object, ExportFn exportFn, CommitFn commitFn) {
    auto work_ = [
      this, rn, un, sn, vn, object,
      exportFn=ZuMv(exportFn),
      commitFn=ZuMv(commitFn)
    ]() mutable {
      if (rowsUN.find(un)) {
	auto callback = [this, un, commitFn = ZuMv(commitFn)] {
	  commitFn(db, un, CommitResult{});
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
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
	auto callback = [this, un, commitFn = ZuMv(commitFn)] {
	  commitFn(db, un, CommitResult{});
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
      } else {
	auto callback = [this, rn, un, commitFn = ZuMv(commitFn)] {
	  commitFn(db, un, CommitResult{
	    ZeMEVENT(Error, ([rn](auto &s, const auto &) {
	      s << rn << " missing RN";
	    }))
	  });
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
      }
    };
    deferWork ? work.push(ZuMv(work_)) : work_();
  }
  void del(RN rn, UN un, SN sn, VN vn, CommitFn commitFn) {
    auto work_ = [this, rn, un, commitFn=ZuMv(commitFn)]() mutable {
      if (rowsUN.find(un)) {
	auto callback = [this, un, commitFn = ZuMv(commitFn)] {
	  commitFn(db, un, CommitResult{});
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
	return;
      }
      auto row = rows.find(rn);
      if (row) {
	rowsUN.delNode(row);
	rows.delNode(row);
	auto callback = [this, un, commitFn = ZuMv(commitFn)] {
	  commitFn(db, un, CommitResult{});
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
      } else {
	auto callback = [this, rn, un, commitFn = ZuMv(commitFn)] {
	  commitFn(db, un, CommitResult{
	    ZeMEVENT(Error, ([rn](auto &s, const auto &) {
	      s << rn << " missing RN";
	    }))
	  });
	};
	deferCallbacks ? callbacks.push(ZuMv(callback)) : callback();
      }
    };
    deferWork ? work.push(ZuMv(work_)) : work_();
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

// environments and mock data stores
ZmRef<Store> store[2];
ZmRef<ZdbEnv> env[2];

// databases
ZmRef<Zdb> orders[2];

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
      "hosts {\n"
      "  0 { priority 100 ip 127.0.0.1 port 9943 }\n"
      "  1 { priority  80 ip 127.0.0.1 port 9944 }\n"
      "}\n"
      "dbs {\n"
      "  orders { thread 4 writeThread 5 }\n"
      "}\n"
      "debug 1\n"
      "dbMx {\n"
      "  nThreads 6\n"
      "  threads {\n"
      "    1 { isolated true }\n"
      "    2 { isolated true }\n"
      "    3 { isolated true }\n"
      "    4 { isolated true }\n"
      "    5 { isolated true }\n"
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

  ZeLog::init("zdbreptest");
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

    ZmAtomic<unsigned> ok = 0;

    for (unsigned i = 0; i < 2; i++) {
      store[i] = new Store();
      env[i] = new ZdbEnv();

      ZdbEnvCf envCf{cf};

      envCf.hostID = (ZuStringN<16>{} << i);

      env[i]->init(ZuMv(envCf), dbMx, ZdbEnvHandler{
	.upFn = [](ZdbEnv *env_, ZdbHost *host) {
	  ZeLOG(Info, ([id = host ? host->id() : ZuID{"unset"}](auto &s) {
	    s << "ACTIVE (was " << id << ')';
	  }));
	  if (env_ == env[1]) done.post();
	},
	.downFn = [](ZdbEnv *) { ZeLOG(Info, "INACTIVE"); }
      }, store[i]);

      orders[i] = env[i]->initDB<Order>("orders"); // might throw

      env[i]->start([&ok](bool ok_) { if (ok_) ++ok; done.post(); });
    }

    for (unsigned i = 0; i < 2; i++) done.wait();

    if (ok >= 2) {
      ZdbRN rn;

      orders[0]->writeCache(false);

      deferWork = true;
      deferCallbacks = true;

      orders[0]->run([&rn]{
	orders[0]->push<Order>([&rn](ZdbObject<Order> *o) {
	  new (o->ptr()) Order{Side::Buy, "IBM", 100, 100};
	  o->put();
	  rn = o->rn();
	  ZeLOG(Info, ([rn](auto &s) { s << "RN: " << rn; }));
	});
      });

      orders[0]->run([&rn]{
	orders[0]->get<Order>(rn, [](ZmRef<ZdbObject<Order>> o) {
	  if (!o)
	    ZeLOG(Info, "get(): (null)");
	  else
	    ZeLOG(Info, ([o = ZuMv(o)](auto &s) {
	      s << "get(): " << o->data();
	    }));
	});
	done.post();
      });

      performWork(); deferWork = false;

      performCallbacks(); deferCallbacks = false;

      done.wait();

      ZeLOG(Debug, "ENV 0 STOPPING");

      env[0]->stop();

      ZeLOG(Debug, "ENV 0 STOPPED");

      done.wait(); // wait for env[1] to become active

      orders[1]->run([&rn]{
	orders[1]->get<Order>(rn, [](ZmRef<ZdbObject<Order>> o) {
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
    }

    for (unsigned i = 0; i < 2; i++)
      env[i]->stop();

    appMx->stop();
    dbMx->stop();

    ZeLOG(Debug, (ZtString{} << '\n' << ZmHashMgr::csv()));

    for (unsigned i = 0; i < 2; i++) {
      orders[i] = {};
      env[i]->final(); // calls Store::final()
      env[i] = {};
      store[i] = {};
    }

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
