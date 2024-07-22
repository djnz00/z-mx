//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Mx Telemetry

#include <mxbase/MxTelemetry.hh>

using namespace MxTelemetry;

void Client::init(MxMultiplex *mx, const ZvCf *cf)
{
  m_mx = mx;

  if (ZuString ip = cf->get("interface")) m_interface = ip;
  m_ip = cf->get("ip", "127.0.0.1");
  m_port = cf->getInt("port", 1, (1<<16) - 1, 19300);
}

void Client::final()
{
}

void Client::start()
{
  ZiCxnOptions options;
  options.udp(true);
  if (m_ip.multicast()) {
    options.multicast(true);
    options.mreq(ZiMReq(m_ip, m_interface));
  }
  m_mx->udp(
      ZiConnectFn(this, [](Client *client, const ZiCxnInfo &ci) -> uintptr_t {
	  return (uintptr_t)(new Cxn(client, ci));
	}),
      ZiFailFn(this, [](Client *client, bool transient) {
	  client->error(ZeMkLambdaEvent(Error,
		([ip = client->m_ip, port = client->m_port](
		    const ZeEvent &, ZuVStream &s) {
		  s << "MxTelemetry::Client{" <<
		    ip << ':' << ZuBoxed(port) << "} UDP receive failed";
		})));
	}),
      ZiIP(), m_port, ZiIP(), 0, options);
}

void Client::stop()
{
  ZmRef<Cxn> old;
  {
    Guard connGuard(m_connLock);
    old = m_cxn;
    m_cxn = nullptr;
  }

  if (old) old->disconnect();
}

void Client::connected(Cxn *cxn, ZiIOContext &io)
{
  ZmRef<Cxn> old;
  {
    Guard connGuard(m_connLock);
    old = m_cxn;
    m_cxn = cxn;
  }

  if (ZuUnlikely(old)) { old->disconnect(); old = nullptr; } // paranoia

  cxn->recv(io);
}

void Client::disconnected(Cxn *cxn)
{
  Guard connGuard(m_connLock);
  if (m_cxn == cxn) m_cxn = nullptr;
}


void Server::init(MxMultiplex *mx, const ZvCf *cf)
{
  m_mx = mx;

  if (ZuString ip = cf->get("interface")) m_interface = ip;
  m_ip = cf->get("ip", "127.0.0.1");
  m_port = cf->getInt("port", 1, (1<<16) - 1, 19300);
  m_ttl = cf->getInt("ttl", 0, INT_MAX, 1);
  m_loopBack = cf->getBool("loopBack");
  m_freq = cf->getInt("freq", 0, 60000000, 1000000); // microsecs

  m_addr = ZiSockAddr(m_ip, m_port);
}

void Server::final()
{
}

void Server::start()
{
  ZiCxnOptions options;
  options.udp(true);
  if (m_ip.multicast()) {
    options.multicast(true);
    options.mif(m_interface);
    options.ttl(m_ttl);
    options.loopBack(m_loopBack);
  }
  m_mx->udp(
      ZiConnectFn(this, [](Server *server, const ZiCxnInfo &ci) -> uintptr_t {
	  return (uintptr_t)(new Cxn(server, ci));
	}),
      ZiFailFn(this, [](Server *server, bool transient) {
	  server->error(ZeMkLambdaEvent(Error,
		([ip = server->m_ip, port = server->m_port](
		    const ZeEvent &, ZuVStream &s) {
		  s << "MxTelemetry::Server{" <<
		    ip << ':' << ZuBoxed(port) << "} UDP send failed";
		})));
	}),
      ZiIP(), 0, ZiIP(), 0, options);
}

void Server::stop()
{
  m_mx->del(&m_timer);

  ZmRef<Cxn> old;
  {
    Guard connGuard(m_connLock);
    old = m_cxn;
    m_cxn = nullptr;
  }

  if (old) old->disconnect();
}

void Server::scheduleRun()
{
  m_mx->run(m_mx->txThread(),
      ZmFn<>{this, [](Server *server) { server->run_(); }},
      Zm::now((double)m_freq / 1000000), &m_timer);
}

void Server::run_()
{
  ZmRef<Cxn> cxn;
  {
    Guard guard(m_connLock);
    cxn = m_cxn;
  }
  if (!cxn)
    m_mx->del(&m_timer);
  else {
    run(cxn);
    scheduleRun();
  }
}

void Server::connected(Cxn *cxn)
{
  ZmRef<Cxn> old;
  {
    Guard connGuard(m_connLock);
    old = m_cxn;
    m_cxn = cxn;
  }

  if (ZuUnlikely(old)) { old->disconnect(); old = nullptr; } // paranoia

  scheduleRun();
}

void Server::disconnected(Cxn *cxn)
{
  Guard connGuard(m_connLock);
  if (m_cxn == cxn) m_cxn = nullptr;
}
