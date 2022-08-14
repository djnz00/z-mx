//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

#include <zlib/ZuPrint.hpp>
#include <zlib/ZuPolymorph.hpp>

#include <zlib/ZmTrap.hpp>

#include <zlib/ZvCmdServer.hpp>

class CmdTest;

struct Link : public ZvCmdSrvLink<CmdTest, Link> {
  using Base = ZvCmdSrvLink<CmdTest, Link>;
  Link(CmdTest *app) : Base{app} { }
};

class CmdTest :
    public ZmPolymorph, public ZvCmdServer<CmdTest, Link> {
public:
  void init(ZiMultiplex *mx, const ZvCf *cf) {
    m_uptime.now();
    ZvCmdServer::init(mx, cf);
    addCmd("ackme", "", ZvCmdFn{
      [](ZvCmdContext *ctx) {
	if (auto cxn = ctx->link<Link>()->tcp())
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
  ZtDate	m_uptime;
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
    "    --userDB:passLen=N\t\tset default password length (default: 12)\n"
    "    --userDB:totpRange=N\tset TOTP accepted range (default: 6)\n"
    "    --userDB:keyInterval=N\tset key refresh interval (default: 30)\n"
    "    --userDB:maxAge=N\t\tset user DB file backups (default: 8)\n"
    << std::flush;
  ::exit(1);
}

int main(int argc, char **argv)
{
  static ZvOpt opts[] = {
    { "caPath", "C", ZvOptScalar },
    { "userDB:passLen", nullptr, ZvOptScalar },
    { "userDB:totpRange", nullptr, ZvOptScalar },
    { "userDB:keyInterval", nullptr, ZvOptScalar },
    { "userDB:maxAge", nullptr, ZvOptScalar },
    { nullptr }
  };

  ZeLog::init("CmdTest");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::lambdaSink(
	[](ZeEvent *e) { std::cerr << e->message() << '\n' << std::flush; }));
  ZeLog::start();

  ZiMultiplex *mx = new ZiMultiplex(
      ZiMxParams()
	.scheduler([](auto &s) {
	  s.nThreads(4)
	  .thread(1, [](auto &t) { t.isolated(1); })
	  .thread(2, [](auto &t) { t.isolated(1); })
	  .thread(3, [](auto &t) { t.isolated(1); }); })
	.rxThread(1).txThread(2));

  ZmRef<CmdTest> server = new CmdTest();

  ZmTrap::sigintFn(ZmFn<>{server, [](CmdTest *server) { server->post(); }});
  ZmTrap::trap();

  mx->start();

  try {
    ZmRef<ZvCf> cf = new ZvCf();
    cf->fromString(
	"thread 3\n"
	"caPath /etc/ssl/certs\n"
	"userDB {\n"
	"  passLen 12\n"
	"  totpRange 6\n"
	"  keyInterval 30\n"
	"  maxAge 8\n"
	"}\n", false);
    if (cf->fromArgs(opts, argc, argv) != 6) usage();
    cf->set("certPath", cf->get("1"));
    cf->set("keyPath", cf->get("2"));
    cf->set("userDB:path", cf->get("3"));
    cf->set("localIP", cf->get("4"));
    cf->set("localPort", cf->get("5"));
    server->init(mx, cf);
  } catch (const ZvCf::Usage &e) {
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

  ZmTrap::sigintFn(ZmFn<>{});

  return 0;
}
