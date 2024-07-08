//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// server-side user DB bootstrap tool

#include <iostream>

#include <zlib/ZumServer.hh>

void usage()
{
  std::cerr << "usage: zuserdb USER ROLE PASSLEN [PERMS...]\n";
  exit(1);
}

int main(int argc, char **argv)
{
  ZmRef<ZvCf> cf;

  ZtString user, role;
  int passLen;
  ZtArray<ZtString> perms;

  try {
    ZmRef<ZvCf> options = inlineCf(
      "module m m { param store.module }\n"
      "connect c c { param store.connection }\n"
      "help { flag help }\n");

    cf = inlineCf(
      "thread zdb\n"
      "hostID 0\n"
      "hosts { 0 { standalone 1 } }\n"
      "store {\n"
      "  module ${ZDB_MODULE}\n"
      "  connection ${ZDB_CONNECT}\n"
      "  thread zdb_store\n"
      "  replicated true\n"
      "}\n"
      "tables {\n"
      "  zum.user { }\n"
      "  zum.role { }\n"
      "  zum.key { }\n"
      "  zum.perm { }\n"
      "}\n"
      "debug 1\n"
      "mx {\n"
      "  nThreads 4\n"
      "  threads {\n"
      "    1 { name rx isolated true }\n"
      "    2 { name tx isolated true }\n"
      "    3 { name zdb isolated true }\n"
      "    4 { name zdb_store isolated true }\n"
      "    5 { name app }\n"
      "  }\n"
      "  rxThread rx\n"
      "  txThread tx\n"
      "}\n"
    );

    // command line overrides environment
    if (cf->fromArgs(options, ZvCf::args(argc, argv)) != 1) usage();

    if (cf->getBool("help")) usage();

    if (!cf->get("store.module")) {
      std::cerr << "set ZDB_MODULE or use --module=MODULE\n" << std::flush;
      Zm::exit(1);
    }
    if (!cf->get("store.connection")) {
      std::cerr << "set ZDB_CONNECT or use --connect=CONNECT\n" << std::flush;
      Zm::exit(1);
    }

    unsigned argc_ = cf->getInt("#", 0, INT_MAX);
    if (argc_ < 4) usage();
    perms.size(argc_ - 4);

    user = cf->get("1");
    role = cf->get("2");
    passlen = cf->getInt("3", 6, 60);

    for (unsigned i = 4; i < argc_; i++)
      perms.push(cf->get(ZtString{} << argc_));

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

  ZiMultiplex *mx;
  ZmRef<Zdb> db = new Zdb();

  try {
    mx = new ZiMultiplex(ZvMxParams{"mx", cf->getCf<true>("mx")});

    db->init(ZdbCf(cf), dbMx, ZdbHandler{
      .upFn = [](Zdb *, ZdbHost *host) {
	ZeLOG(Info, ([id = host ? host->id() : ZuID{"unset"}](auto &s) {
	  s << "ACTIVE (was " << id << ')';
	}));
      },
      .downFn = [](Zdb *) { ZeLOG(Info, "INACTIVE"); }
    });

    mx->start();
    if (!db->start()) throw ZeEVENT(Fatal, "Zdb start failed");

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

  Ztls::Random rng;

  rng.init();

  Zum::Server::Mgr mgr(&rng, passlen, 6, 30);

  mgr.init(db);

  ZmBlock<>{}([&mgr](auto wake) {
    mgr.open([wake = ZuMv(wake)](bool ok) {
      if (!ok) {
	ZeLOG(Fatal, "userDB open failed");
	gtfo();
      }
      wake();
    });
  });

  ZmBlock<>{}([&mgr](auto wake) {
    mgr.bootstrap([wake = ZuMv(wake)](Zum::Server::BootstrapResult result) {
      if (result.is<bool>()) {
	if (result.p<bool>()) {
	  ZeLOG(Info, "userDB already initialized");
	} else {
	  ZeLOG(Fatal, "userDB bootstrap failed");
	  gtfo();
	}
      } else if (result.is<BootstrapData>()) {
	const auto &data = result.p<BootstrapData>();
	std::cout
	  << "passwd: " << data.passwd << '\n'
	  << "secret: " << data.secret << '\n' << std::flush;
      }
      wake();
    });
  });

  perms.all([&mgr](const auto &perm) {
    ZiIOBuilder fbb;
    fbb.Finish(Zum::fbs::CreateRequest(
      fbb, 0, Zum::fbs::ReqData::PermAdd,
      Zum::fbs::CreatePermName(fbb, Zfb::Save::str(fbb, perm)).Union()));
    ZmBlock<>{}([&mgr, &perm, buf = fbb.buf()](auto wake) {
      mgr.permAdd(fbb.buf(), [&perm, wake = ZuMv(wake)](ZmRef<ZiIOBuf> buf) {
	auto reqAck = Zfb::GetRoot<Zum::fbs::ReqAck>(buf->data());
	if (reqAck.data_type() != Zum::fbs::ReqAckData::PermAdd) {
	  ZeLOG(Error, "invalid request acknowledgment");
	} else {
	  auto permID = static_cast<const Zum::fbs::PermID *>(reqAck->data());
	  std::cout << permID->id() << ' ' << perm << '\n' << std::flush;
	}
	wake();
      });
    });
  });

  db->stop();
  mx->stop();

  mgr.final();

  db->final();
  db = {};

  delete mx;

  ZeLog::stop();

  return 0;
}
