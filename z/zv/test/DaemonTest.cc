//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZmPlatform.hh>
#include <zlib/ZmTime.hh>
#include <zlib/ZmSemaphore.hh>
#include <zlib/ZmTrap.hh>

#include <zlib/ZeLog.hh>

#include <zlib/ZvDaemon.hh>
#include <zlib/ZvCf.hh>

namespace {
  void usage() {
    puts("usage: DaemonTest [username [password]] [-d|--daemonize]");
    Zm::exit(1);
  }

  void notify(const char *text) {
    ZeLOG(Info, ZtSprintf("PID %d: %s", (int)Zm::getPID(), text));
  }

  ZmSemaphore done;

  void sigint() {
    ZeLOG(Info, "SIGINT");
    done.post();
  }
} // namespace

#ifdef _MSC_VER
#pragma warning(disable:4800)
#endif

int main(int argc, char **argv)
{
  static ZvOpt opts[] = {
    { "daemonize", "d", ZvOptFlag },
    { "help", 0, ZvOptFlag },
    { 0 }
  };

  ZmRef<ZvCf> cf = new ZvCf();
  ZtString username, password;
  bool daemonize;

  ZeLog::init("DaemonTest");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::debugSink());

  ZmTrap::sigintFn(sigint);
  ZmTrap::trap();

  try {
    cf->fromArgs(opts, argc, argv);
    username = cf->get("1");
    password = cf->get("2");
    daemonize = cf->getBool("daemonize");
  } catch (const ZvError &e) {
    ZeLog::start();
    ZeLOG(Error, e.message());
    ZeLog::stop();
    Zm::exit(1);
  }

  int r = ZvDaemon::init(username, password, 0, daemonize, "DaemonTest.pid");

  ZeLog::start();

  switch (r) {
    case ZvDaemon::OK:
      notify("OK");
      break;
    case ZvDaemon::Running:
      notify("already running");
      ZeLog::stop();
      Zm::exit(1);
      break;
    case ZvDaemon::Error:
      notify("error");
      ZeLog::stop();
      Zm::exit(1);
      break;
  }

  done.wait();

  ZeLog::stop();
  return 0;
}
