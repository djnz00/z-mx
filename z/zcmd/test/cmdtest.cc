//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZuPrint.hh>
#include <zlib/ZuPolymorph.hh>

#include <zlib/ZmTrap.hh>
#include <zlib/ZmTime.hh>

#include <zlib/ZvCmdServer.hh>

class CmdTest;

struct Link : public ZvCmdSrvLink<CmdTest, Link> {
  using Base = ZvCmdSrvLink<CmdTest, Link>;
  Link(CmdTest *app) : Base{app} { }
};

class CmdTest : public ZmPolymorph, public ZvCmdServer<CmdTest, Link> {
public:
  void init(ZiMultiplex *mx, const ZvCf *cf) {
    m_uptime = Zm::now();
    ZvCmdServer::init(mx, cf);
    addCmd("ackme", "", ZvCmdFn{
      [](ZvCmdContext *ctx) {
	if (auto cxn = ctx->link<Link>()->cxn())
	  std::cout << cxn->info().remoteIP << ':'
	    << ZuBoxed(cxn->info().remotePort) << ' ';
	std::cout << "user: "
	  << ctx->user<User>()->id << ' ' << ctx->user<User>()->name << '\n'
	  << "cmd: " << ctx->args->get("0") << '\n';
	ctx->out << "this is an ack\n";
      }}, "test ack", "");
    addCmd("nakme", "", ZvCmdFn{
      [](ZvCmdContext *ctx) {
	ctx->out << "this is a nak\n";
      }}, "test nak", "");
    addCmd("quit", "", ZvCmdFn{
      [](ZvCmdContext *ctx) {
	ctx->app<CmdTest>()->post();
	ctx->out << "quitting...\n";
      }}, "quit", "");
  }

  void wait() { m_done.wait(); }
  void post() { m_done.post(); }

  void telemetry(ZvTelemetry::App &data) {
    using namespace ZvTelemetry;
    data.id = "CmdTest";
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
  std::cerr << "usage: CmdTest "
    "CERTPATH KEYPATH USERDB IP PORT\n"
    "    CERTPATH\tTLS/SSL certificate path\n"
    "    KEYPATH\tTLS/SSL private key path\n"
    "    USERDB\tuser DB path\n"
    "    IP\t\tlistener IP address\n"
    "    PORT\tlistener port\n\n"
    "Options:\n"
    "    --caPath=CAPATH\t\tset CA path (default: /etc/ssl/certs)\n"
    "    --userDB.passLen=N\t\tset default password length (default: 12)\n"
    "    --userDB.totpRange=N\tset TOTP accepted range (default: 2)\n"
    "    --userDB.keyInterval=N\tset key refresh interval (default: 30)\n"
    "    --userDB.maxAge=N\t\tset user DB file backups (default: 8)\n"
    << std::flush;
  ::exit(1);
}

ZmRef<CmdTest> server;

void sigint() { if (server) server->post(); }

int main(int argc, char **argv)
{
  static ZvOpt opts[] = {
    { "caPath", "C", ZvOptValue },
    { "userDB.passLen", nullptr, ZvOptValue },
    { "userDB.totpRange", nullptr, ZvOptValue },
    { "userDB.keyInterval", nullptr, ZvOptValue },
    { "userDB.maxAge", nullptr, ZvOptValue },
    { nullptr }
  };

  ZeLog::init("CmdTest");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::lambdaSink([](ZeLogBuf &buf, const ZeEventInfo &) {
    buf << '\n';
    std::cerr << buf << std::flush;
  }));
  ZeLog::start();

  ZiMultiplex *mx = new ZiMultiplex(
      ZiMxParams()
	.scheduler([](auto &s) {
	  s.nThreads(4)
	  .thread(1, [](auto &t) { t.isolated(1); })
	  .thread(2, [](auto &t) { t.isolated(1); })
	  .thread(3, [](auto &t) { t.isolated(1); }); })
	.rxThread(1).txThread(2));

  ZmRef<CmdTest> server = new CmdTest{};

  ZmTrap::sigintFn(sigint);
  ZmTrap::trap();

  mx->start();

  try {
    ZmRef<ZvCf> cf = new ZvCf{};
    cf->fromString(
	"thread 3\n"
	"caPath /etc/ssl/certs\n"
	"userDB {\n"
	"  passLen 12\n"
	"  totpRange 2\n"
	"  keyInterval 30\n"
	"  maxAge 8\n"
	"}\n");
    if (cf->fromArgs(opts, argc, argv) != 6) usage();
    cf->set("certPath", cf->get("1"));
    cf->set("keyPath", cf->get("2"));
    cf->set("userDB:path", cf->get("3"));
    cf->set("localIP", cf->get("4"));
    cf->set("localPort", cf->get("5"));
    server->init(mx, cf);
  } catch (const ZvCfError::Usage &e) {
    usage();
  } catch (const ZvError &e) {
    std::cerr << e << '\n' << std::flush;
    ::exit(1);
  } catch (const ZtString &e) {
    std::cerr << e << '\n' << std::flush;
    ::exit(1);
  } catch (...) {
    std::cerr << "unknown exception\n" << std::flush;
    ::exit(1);
  }

  server->start();

  server->wait();

  server->stop();
  mx->stop();

  delete mx;

  ZeLog::stop();

  ZmTrap::sigintFn(nullptr);

  return 0;
}
