//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuPrint.hh>
#include <zlib/ZuPolymorph.hh>

#include <zlib/ZmTrap.hh>
#include <zlib/ZmTime.hh>

#include <zlib/ZcmdServer.hh>

class CmdTest;

struct Link : public ZcmdSrvLink<CmdTest, Link, ZiIOBufAlloc<>> {
  using Base = ZcmdSrvLink<CmdTest, Link, ZiIOBufAlloc<>>;
  Link(CmdTest *app) : Base{app} { }
};

class CmdTest : public ZmPolymorph, public ZcmdServer<CmdTest, Link> {
public:
  void init(const ZvCf *cf, ZiMultiplex *mx, Zdb *db) {
    m_uptime = Zm::now();

    ZcmdServer::init(cf, mx, db);

    addCmd("ackme", "", ZcmdFn{
      [](ZcmdContext *ctx) {
	auto link = static_cast<Link *>(ctx->dest.p<void *>());
	if (auto cxn = link->cxn())
	  std::cout << cxn->info().remoteIP << ':'
	    << ZuBoxed(cxn->info().remotePort) << ' ';
	const auto &user = link->session()->user->data();
	std::cout << "user: "
	  << user.id << ' ' << user.name << '\n'
	  << "cmd: " << ctx->args->get("0") << '\n';
	ctx->out << "this is an ack\n";
      }}, "test ack", "");
    addCmd("nakme", "", ZcmdFn{
      [](ZcmdContext *ctx) {
	ctx->out << "this is a nak\n";
      }}, "test nak", "");
    addCmd("quit", "", ZcmdFn{
      [](ZcmdContext *ctx) {
	auto this_ = static_cast<CmdTest *>(ctx->host);
	this_->post();
	ctx->out << "quitting...\n";
      }}, "quit", "");
  }

  void wait() { m_done.wait(); }
  void post() { m_done.post(); }

  void telemetry(Ztel::App &data) {
    using namespace Ztel;
    data.id = "cmdtest";
    data.version = "1.0";
    data.uptime = m_uptime;
    data.role = AppRole::Dev;
    data.rag = RAG::Green;
  }

private:
  ZuDateTime	m_uptime;
  ZmSemaphore	m_done;
};

void usage()
{
  std::cerr << "usage: cmdtest CERTPATH KEYPATH IP PORT [OPTION]...\n"
    "  CERTPATH\tTLS/SSL certificate path\n"
    "  KEYPATH\tTLS/SSL private key path\n"
    "  IP\t\tlistener IP address\n"
    "  PORT\tlistener port\n\n"
    "Options:\n"
    "  -m, --module=MODULE\tZdb data store module e.g. libZdbPQ.so\n"
    "  -c, --connect=CONNECT\tZdb data store connection string\n"
    "\t\t\te.g. \"dbname=test host=/tmp\"\n"
    "  -C, --ca-path=CAPATH\t\tset CA path (default: /etc/ssl/certs)\n"
    "      --pass-len=N\t\tset default password length (default: 12)\n"
    "      --totp-range=N\tset TOTP accepted range (default: 2)\n"
    "      --key-interval=N\tset key refresh interval (default: 30)\n"
    "      --max-age=N\t\tset user DB file backups (default: 8)\n"
    "  -l, --log=FILE\tlog to FILE\n"
    "  -d, --debug\t\tenable Zdb debugging\n"
    "      --help\t\tthis help\n"
    << std::flush;
  ::exit(1);
}

ZmRef<CmdTest> server;

void sigint() { if (server) server->post(); }

int main(int argc, char **argv)
{
  ZmRef<ZvCf> cf;
  ZiMultiplex *mx = nullptr;
  ZmRef<Zdb> db = new Zdb();
  ZmRef<CmdTest> server = new CmdTest{};

  try {
    ZmRef<ZvCf> options = new ZvCf{};

    options->fromString(
      "module m m { param zdb.store.module }\n"
      "connect c c { param zdb.store.connection }\n"
      "ca-path C C { param caPath }\n"
      "pass-len { param userDB.passLen }\n"
      "totp-range { param userDB.totpRange }\n"
      "key-interval { param userDB.keyInterval }\n"
      "max-age { param userDB.maxAge }\n"
      "log l l { param log }\n"
      "debug d d { flag zdb.debug }\n"
      "help { flag help }\n");

    ZmRef<ZvCf> cf = new ZvCf{};

    cf->fromString(
      "log \"&2\"\n"	// default - stderr
      "caPath /etc/ssl/certs\n"
      "thread app\n"
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
      "userDB {\n"
      "  thread zdb\n"
      "  passLen 12\n"
      "  totpRange 2\n"
      "  keyInterval 30\n"
      "  maxAge 8\n"
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
      "}\n");

    if (cf->fromArgs(options, ZvCf::args(argc, argv)) != 5) usage();

    if (cf->getBool("help")) usage();

    cf->set("certPath", cf->get("1"));
    cf->set("keyPath", cf->get("2"));
    cf->set("localIP", cf->get("3"));
    cf->set("localPort", cf->get("4"));

    mx = new ZiMultiplex{ZvMxParams{"mx", cf->getCf<true>("mx")}};

    db->init(ZdbCf(cf->getCf<true>("zdb")), mx, ZdbHandler{
      .upFn = [](Zdb *, ZdbHost *) { },
      .downFn = [](Zdb *) { }
    });

    server->init(cf, mx, db);

  } catch (const ZvCfError::Usage &e) {
    usage();
  } catch (const ZvError &e) {
    std::cerr << e << '\n' << std::flush;
    Zm::exit(1);
  } catch (const ZtString &e) {
    std::cerr << e << '\n' << std::flush;
    Zm::exit(1);
  } catch (...) {
    std::cerr << "unknown exception\n" << std::flush;
    Zm::exit(1);
  }

  ZeLog::init("cmdtest");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::fileSink(ZeSinkOptions{}.path(cf->get<true>("log"))));
  ZeLog::start();

  ZmTrap::sigintFn(sigint);
  ZmTrap::trap();

  mx->start();

  if (!db->start()) {
    ZeLOG(Fatal, "Zdb start failed");
    mx->stop();
    ZeLog::stop();
    Zm::exit(1);
  }

  server->start();

  server->wait();

  server->stop();
  db->stop();
  mx->stop();

  server->final();
  server = {};

  db->final();
  db = {};

  delete mx;

  ZeLog::stop();

  return 0;
}
