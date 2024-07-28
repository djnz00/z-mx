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
#include <zlib/ZvMxParams.hh>

#include <zlib/ZtlsTOTP.hh>

#include <zlib/Zdb.hh>

#include <zlib/ZumServer.hh>

void usage()
{
  std::cerr <<
    "Usage: zuserdb USER PASSLEN [OPTION]... [PERM]...\n"
    "  Bootstrap user database with admin super-user USER,\n"
    "  generating a random initial password of PASSLEN characters,\n"
    "  optionally adding permissions PERM...\n\n"
    "Options:\n"
    "  -m, --module=MODULE\tZdb data store module e.g. libZdbPQ.so\n"
    "  -c, --connect=CONNECT\tZdb data store connection string\n"
    "\t\t\te.g. \"dbname=test host=/tmp\"\n"
    "  -l, --log=FILE\tlog to FILE\n"
    "  -d, --debug\t\tenable Zdb debugging\n"
    "      --help\t\tthis help\n"
    << std::flush;
  exit(1);
}

int main(int argc, char **argv)
{
  ZmRef<ZvCf> cf;

  ZtString user;
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

    unsigned argc_ = cf->fromArgs(options, ZvCf::args(argc, argv));

    if (cf->getBool("help")) usage();

    if (argc_ < 3) usage();

    if (!cf->exists("zdb.store.module")) {
      std::cerr << "set ZDB_MODULE or use --module=MODULE\n" << std::flush;
      Zm::exit(1);
    }
    if (!cf->exists("zdb.store.connection")) {
      std::cerr << "set ZDB_CONNECT or use --connect=CONNECT\n" << std::flush;
      Zm::exit(1);
    }

    user = cf->get("1");
    cf->set("userDB.passLen", ZtString{} << cf->getInt("2", 6, 60));
    perms.size(argc_ - 3);
    for (unsigned i = 3; i < argc_; i++)
      perms.push(cf->get(ZtString{} << i));

  } catch (const ZvError &e) {
    std::cerr << e << '\n' << std::flush;
    usage();
  } catch (const ZeError &e) {
    std::cerr << e << '\n' << std::flush;
    usage();
  } catch (...) {
    usage();
  }

  ZeLog::init("zuserdb");
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

  Zum::Server::UserDB userDB(&rng);

  try {
    mx = new ZiMultiplex{ZvMxParams{"mx", cf->getCf<true>("mx")}};

    db->init(ZdbCf(cf->getCf<true>("zdb")), mx, ZdbHandler{
      .upFn = [](Zdb *, ZdbHost *) { },
      .downFn = [](Zdb *, bool) { }
    });

    userDB.init(cf->getCf<true>("userdb"), db);

    mx->start();
    if (!db->start()) throw ZeEVENT(Fatal, "Zdb start failed");

  } catch (const ZvError &e) {
    ZeLOG(Fatal, ZtString{e});
    gtfo();
  } catch (const ZeError &e) {
    ZeLOG(Fatal, ZtString{e});
    gtfo();
  } catch (const ZeAnyEvent &e) {
    ZeLogEvent(ZeVEvent{e});
    gtfo();
  } catch (...) {
    ZeLOG(Fatal, "unknown exception");
    gtfo();
  }

  ZmBlock<>{}([&userDB, &gtfo, &perms](auto wake) {
    userDB.open(ZuMv(perms), [
      wake = ZuMv(wake), &gtfo, &perms
    ](bool ok, ZtArray<unsigned> permIDs) mutable {
      if (!ok) {
	ZeLOG(Fatal, "userDB open failed");
	gtfo();
      } else {
	for (unsigned i = 0, n = perms.length(); i < n; i++)
	  std::cout << permIDs[i] << ' ' << perms[i] << '\n';
      }
      wake();
    });
  });

  ZtString passwd, secret;

  ZmBlock<>{}([
    user = ZuMv(user), &userDB, &gtfo, &passwd, &secret
  ](auto wake) {
    userDB.bootstrap(ZuMv(user), [
      &gtfo, &passwd, &secret, wake = ZuMv(wake)
    ](Zum::Server::BootstrapResult result) mutable {
      using Data = Zum::Server::BootstrapData;
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
	  << "\nsecret: " << secret << '\n' << std::flush;
      }
      wake();
    });
  });

#if 0
  if (perms.length()) {
    unsigned totp;
    {
      ZtArray<uint8_t> secret_(ZuBase32::declen(secret.length()));
      secret_.length(secret_.size());
      secret_.length(ZuBase32::decode(secret_, secret));
      totp = Ztls::TOTP::calc(secret_);
    }
    Zfb::IOBuilder fbb;
    fbb.Finish(Zum::fbs::CreateLoginReq(
	fbb, Zum::fbs::LoginReqData::Login,
	Zum::fbs::CreateLogin(fbb,
	  Zfb::Save::str(fbb, user),
	  Zfb::Save::str(fbb, passwd),
	  totp).Union()));
    ZmBlock<>{}([&userDB, &perms, buf = fbb.buf()](auto wake) mutable {
      userDB.loginReq(ZuMv(buf), [
	&userDB, &perms, wake = ZuMv(wake)
      ](ZmRef<Zum::Server::Session> session) mutable {
	if (!session) {
	  ZeLOG(Fatal, "login failed");
	  wake();
	  return;
	}

	// recycling lambda - iterates over perms, adding them
	ZuLambda{[
	  &userDB, &perms, wake = ZuMv(wake), session = ZuMv(session), i = 0U
	](auto &&self, ZmRef<Zum::IOBuf> buf) mutable {
	  if (buf) {
	    auto reqAck = Zfb::GetRoot<Zum::fbs::ReqAck>(buf->data());
	    if (reqAck->data_type() != Zum::fbs::ReqAckData::PermAdd) {
	      ZeLOG(Fatal, "invalid request acknowledgment");
	      wake();
	      return;
	    }
	    auto permID = static_cast<const Zum::fbs::PermID *>(reqAck->data());
	    std::cout << permID->id() << ' ' << perms[i] << '\n' << std::flush;
	    if (++i >= perms.length()) {
	      wake();
	      return;
	    }
	  }
	  Zfb::IOBuilder fbb;
	  fbb.Finish(Zum::fbs::CreateRequest(
	    fbb, 0, Zum::fbs::ReqData::PermAdd,
	    Zum::fbs::CreatePermName(fbb,
	      Zfb::Save::str(fbb, perms[i])).Union()));
	  const auto &session_ = session;
	  userDB.request(session_, fbb.buf(), [
	    self = ZuMv(self)
	  ](ZmRef<Zum::IOBuf> buf) mutable { ZuMv(self)(ZuMv(buf)); });
	}}(nullptr);
      });
    });
  }
#endif

  db->stop();
  mx->stop();

  userDB.final();

  db->final();
  db = {};

  delete mx;

  ZeLog::stop();

  return 0;
}
