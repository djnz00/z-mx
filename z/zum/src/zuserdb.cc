//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// server-side user DB bootstrap tool

#include <iostream>

#include <zlib/ZuBase32.hh>
#include <zlib/ZuBase64.hh>

#include <zlib/ZeLog.hh>

#include <zlib/ZvCf.hh>

#include <zlib/ZtlsTOTP.hh>

#include <zlib/Zdb.hh>

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
  int passlen;
  ZtArray<ZtString> perms;

  try {
    ZmRef<ZvCf> options = new ZvCf{};

    options->fromString(
      "module m m { param zdb.store.module }\n"
      "connect c c { param zdb.store.connection }\n"
      "log l l { param log }\n"
      "debug d d { flag zdb.debug }\n"
      "help { flag help }\n");

    cf = new ZvCf{};

    cf->fromString(
      "log \"&2\"\n"	// default - stderr
      "mx {\n"
      "  nThreads 5\n"
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
      "userdb {\n"
      "  thread app\n"
      "}\n"
      "zdb {\n"
      "  thread zdb\n"
      "  hostID 0\n"
      "  hosts { 0 { standalone 1 } }\n"
      "  store {\n"
      "    module ${ZDB_MODULE}\n"
      "    connection ${ZDB_CONNECT}\n"
      "    thread zdb_store\n"
      "    replicated true\n"
      "  }\n"
      "  tables {\n"
      "    \"zum.user\" { }\n"
      "    \"zum.role\" { }\n"
      "    \"zum.key\" { }\n"
      "    \"zum.perm\" { }\n"
      "  }\n"
      "}\n"
    );

    // command line overrides environment
    unsigned argc_ = cf->fromArgs(options, ZvCf::args(argc, argv));

    if (argc_ < 4) usage();

    if (cf->getBool("help")) usage();

    if (!cf->get("zdb.store.module")) {
      std::cerr << "set ZDB_MODULE or use --module=MODULE\n" << std::flush;
      Zm::exit(1);
    }
    if (!cf->get("zdb.store.connection")) {
      std::cerr << "set ZDB_CONNECT or use --connect=CONNECT\n" << std::flush;
      Zm::exit(1);
    }

    user = cf->get("1");
    role = cf->get("2");
    passlen = cf->getInt("3", 6, 60);
    perms.size(argc_ - 4);
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
  ZeLog::sink(ZeLog::fileSink(ZeSinkOptions{}.path(cf->get<true>("log"))));
  ZeLog::start();

  ZiMultiplex *mx = nullptr;
  ZmRef<Zdb> db = new Zdb();

  auto gtfo = [&mx]() {
    if (mx) mx->stop();
    ZeLog::stop();
    Zm::exit(1);
  };

  Ztls::Random rng;

  rng.init();

  Zum::Server::Mgr mgr(&rng, passlen, 6, 30);

  try {
    mx = new ZiMultiplex(ZvMxParams{"mx", cf->getCf<true>("mx")});

    db->init(ZdbCf(cf->getCf<true>("zdb")), mx, ZdbHandler{
      .upFn = [](Zdb *, ZdbHost *) { },
      .downFn = [](Zdb *) { }
    });

    mgr.init(cf->getCf<true>("userdb"), mx, db);

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

  ZmBlock<>{}([&mgr, &gtfo](auto wake) {
    mgr.open([wake = ZuMv(wake), &gtfo](bool ok) mutable {
      if (!ok) {
	ZeLOG(Fatal, "userDB open failed");
	gtfo();
      }
      wake();
    });
  });

  ZtString passwd, secret;

  ZmBlock<>{}([
    user = ZuMv(user), role = ZuMv(role), &mgr, &gtfo, &passwd, &secret
  ](auto wake) {
    mgr.bootstrap(ZuMv(user), ZuMv(role), [
      &gtfo, &passwd, &secret, wake = ZuMv(wake)
    ](Zum::Server::Mgr::BootstrapResult result) mutable {
      using Data = Zum::Server::Mgr::BootstrapData;
      if (result.is<bool>()) {
	if (result.p<bool>()) {
	  std::cout << "userDB already initialized\n" << std::flush;
	} else {
	  std::cerr << "userDB bootstrap failed\n" << std::flush;
	  gtfo();
	}
      } else if (result.is<Data>()) {
	auto &data = result.p<Data>();
	passwd = ZuMv(data.passwd);
	secret = ZuMv(data.secret);
	std::cout
	  << "passwd: " << passwd
	  << " secret: " << secret << '\n' << std::flush;
      }
      wake();
    });
  });

  if (perms.length()) {
    unsigned totp;
    {
      ZtArray<uint8_t> secret_(ZuBase32::declen(secret.length()));
      secret_.length(secret_.size());
      secret_.length(ZuBase32::decode(secret_, secret));
      totp = Ztls::TOTP::calc(secret);
    }
    Zfb::IOBuilder fbb;
    fbb.Finish(Zum::fbs::CreateLoginReq(
	fbb, Zum::fbs::LoginReqData::Login,
	Zum::fbs::CreateLogin(fbb,
	  Zfb::Save::str(fbb, user),
	  Zfb::Save::str(fbb, passwd),
	  totp).Union()));
    ZmBlock<>{}([&mgr, &perms, buf = fbb.buf()](auto wake) mutable {
      mgr.loginReq(buf->cbuf_(), [
	&mgr, &perms, wake = ZuMv(wake)
      ](ZmRef<Zum::Server::Session> session) mutable {
	if (!session) {
	  ZeLOG(Fatal, "login failed");
	  wake();
	  return;
	}
	perms.all([&mgr, session = ZuMv(session)](const auto &perm) mutable {
	  Zfb::IOBuilder fbb;
	  fbb.Finish(Zum::fbs::CreateRequest(
	    fbb, 0, Zum::fbs::ReqData::PermAdd,
	    Zum::fbs::CreatePermName(fbb,
	      Zfb::Save::str(fbb, perm)).Union()));
	  ZmBlock<>{}([
	    &mgr, session = ZuMv(session), &perm, buf = fbb.buf()
	  ](auto wake) mutable {
	    mgr.request(session, buf->cbuf_(), [
	      &perm, wake = ZuMv(wake)
	    ](ZmRef<ZiIOBuf<>> buf) mutable {
	      auto reqAck = Zfb::GetRoot<Zum::fbs::ReqAck>(buf->data());
	      if (reqAck->data_type() != Zum::fbs::ReqAckData::PermAdd) {
		ZeLOG(Fatal, "invalid request acknowledgment");
	      } else {
		auto permID =
		  static_cast<const Zum::fbs::PermID *>(reqAck->data());
		std::cout
		  << permID->id() << ' ' << perm << '\n' << std::flush;
	      }
	      wake();
	    });
	  });
	});
	wake();
      });
    });
  }

  db->stop();
  mx->stop();

  mgr.final();

  db->final();
  db = {};

  delete mx;

  ZeLog::stop();

  return 0;
}
