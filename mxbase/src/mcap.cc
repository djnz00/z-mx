//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// multicast capture tool

#include <stdio.h>
#include <signal.h>

#include <zlib/ZuPOD.hh>

#include <zlib/ZmLock.hh>
#include <zlib/ZmGuard.hh>
#include <zlib/ZmAtomic.hh>
#include <zlib/ZmSemaphore.hh>
#include <zlib/ZmTimeInterval.hh>
#include <zlib/ZuTime.hh>
#include <zlib/ZmTrap.hh>

#include <zlib/ZeLog.hh>

#include <zlib/ZiMultiplex.hh>
#include <zlib/ZiFile.hh>

#include <zlib/ZvCf.hh>
#include <zlib/ZvMxParams.hh>
#include <zlib/ZvHeapCSV.hh>

#include <mxbase/MxCSV.hh>
#include <mxbase/MxMCapHdr.hh>

struct Group {
  uint16_t		id;
  ZiIP			ip;
  ZuBox0(uint16_t)	port;
};

typedef ZvCSVColumn<ZvCSVColType::Int, uint16_t> GroupCol;
typedef ZvCSVColumn<ZvCSVColType::Int, ZuBox0(uint16_t)> PortCol;
typedef MxIPCol IPCol;

class GroupCSV : public ZvCSV {
public:
  typedef ZuPOD<Group> POD;

  GroupCSV() : m_pod(new POD()) {
    new (m_pod->ptr()) Group();
    add(new GroupCol("group", offsetof(Group, id)));
    add(new IPCol("ip", offsetof(Group, ip)));
    add(new PortCol("port", offsetof(Group, port)));
  }

  void alloc(ZuRef<ZuAnyPOD> &pod) { pod = m_pod; }

  template <typename File>
  void read(const File &file, ZvCSVReadFn fn) {
    ZvCSV::readFile(file,
	ZvCSVAllocFn::Member<&GroupCSV::alloc>::fn(this), fn);
  }

private:
  ZuRef<POD>	m_pod;
};

class App;

class Source : public ZmPolymorph {
public:
  Source(App *app, const Group &group) : m_app(app), m_group(group) { }
  ~Source() { }

  void connect();
  ZiConnection *connected(const ZiCxnInfo &ci);
  void connectFailed(bool transient);

  App *app() const { return m_app; }
  const Group &group() const { return m_group; }

private:
  App		*m_app;
  Group		m_group;
};

class Connection : public ZiConnection {
public:
  Connection(Source *source, const ZiCxnInfo &ci);
  ~Connection() { }

  App *app() { return m_app; }
  const Group &group() const { return m_group; }

  void connected(ZiIOContext &io);
  void disconnected();

  void recv(ZiIOContext &io);

private:
  App		*m_app;
  Group		m_group;
};

class Mx : public ZmObject, public ZiMultiplex {
public:
  Mx(const ZvCf *cf) : ZiMultiplex{ZvMxParams{cf}} { }
  ~Mx() { }
};

class App : public ZmPolymorph {
public:
  App(const ZvCf *cf);

  int start();
  void stop();

  void wait() { m_sem.wait(); }
  void post() { m_sem.post(); }

  void connect(ZuAnyPOD *group_);
  void write(const MxMCapHdr *hdr, const char *buf);

  ZuInline const ZtString &path() const { return m_path; }
  ZuInline const ZtString &groups() const { return m_groups; }
  ZuInline bool raw() const { return m_raw; }
  ZuInline ZiIP interface_() const { return m_interface; }
  ZuInline unsigned reconnectFreq() const { return m_reconnectFreq; }

  ZuInline Mx *mx() { return m_mx; }
  ZuInline Mx *mx2() { return m_mx2; }

private:
  ZmSemaphore	m_sem;

  ZtString	m_path;		// path of capture file
  ZtString	m_groups;	// CSV file containing multicast groups
  bool		m_raw;		// true if TSE (raw) format
  ZiIP		m_interface;	// interface to capture from
  unsigned	m_reconnectFreq;// reconnect frequency

  ZmLock	m_fileLock;	// lock on capture file
    ZiFile	m_file;		// capture file
    ZiVec	m_vecs[2];

  ZmRef<Mx>	m_mx;		// primary (receiver) multiplexer
  ZmRef<Mx>	m_mx2;		// secondary (file writer) multiplexer
};

template <typename Heap> class Msg_ : public Heap, public ZmPolymorph {
public:
  // UDP over Ethernet maximum payload is 1472 (without Jumbo frames)
  enum { Size = 1472 };

  Msg_(Connection *cxn) : m_cxn(cxn) { }
  ~Msg_() { }

  void recv(ZiIOContext &io);
  void rcvd(ZiIOContext &io);
  void write();

private:
  Connection		*m_cxn;
  MxMCapHdr		m_hdr;
  ZiSockAddr		m_addr;
  char			m_buf[Size];
};
struct Msg_HeapID {
  static constexpr const char *id() { return "Msg"; }
};
typedef ZmHeap<Msg_HeapID, Msg_<ZuNull>> Msg_Heap;
typedef Msg_<Msg_Heap> Msg;

void App::connect(ZuAnyPOD *group_) {
  const Group &group = group_->as<Group>();
  ZmRef<Source> source = new Source(this, group);
  source->connect();
}

void Source::connect()
{
  ZiCxnOptions options;
  options.udp(true);
  options.multicast(true);
  options.mreq(ZiMReq(group().ip, m_app->interface_()));
  m_app->mx()->udp(
      ZiConnectFn::Member<&Source::connected>::fn(ZmMkRef(this)),
      ZiFailFn::Member<&Source::connectFailed>::fn(ZmMkRef(this)),
#ifndef _WIN32
      group().ip,
#else
      ZiIP(),
#endif
      group().port, ZiIP(), 0, options);
}

ZiConnection *Source::connected(const ZiCxnInfo &ci)
{
  return new Connection(this, ci);
}

void Source::connectFailed(bool transient)
{
  if (transient) m_app->mx()->add(
      ZmFn<>::Member<&Source::connect>::fn(ZmMkRef(this)),
      Zm::now((int)m_app->reconnectFreq()));
}

Connection::Connection(Source *source, const ZiCxnInfo &ci) :
    ZiConnection(source->app()->mx(), ci),
    m_app(source->app()),
    m_group(source->group())
{
}

void Connection::connected(ZiIOContext &io)
{
  recv(io);
}

void Connection::disconnected()
{
  if (m_app) m_app->post();
}

void Connection::recv(ZiIOContext &io)
{
  ZmRef<Msg> msg = new Msg(this);
  msg->recv(io);
}

template <typename Heap>
void Msg_<Heap>::recv(ZiIOContext &io)
{
  io.init(ZiIOFn::Member<&Msg_::rcvd>::fn(ZmMkRef(this)),
      m_buf, Size, 0, m_addr);
}

template <typename Heap>
void Msg_<Heap>::rcvd(ZiIOContext &io)
{
  ZuTime now{ZuTime::Now};
  m_hdr.len = io.offset + io.length;
  m_hdr.group = m_cxn->group().id;
  m_hdr.sec = now.sec();
  m_hdr.nsec = now.nsec();

  m_cxn->app()->mx2()->add(ZmFn<>::Member<&Msg_::write>::fn(ZmMkRef(this)));

  m_cxn->recv(io);
}

template <typename Heap>
void Msg_<Heap>::write()
{
  m_cxn->app()->write(&m_hdr, m_buf);
}

void App::write(const MxMCapHdr *hdr, const char *buf)
{
  ZeError e;
  {
    ZmGuard<ZmLock> guard(m_fileLock);
    if (!m_raw) {
      if (m_file.write((void *)hdr, sizeof(MxMCapHdr), &e) != Zi::OK ||
	  m_file.write((void *)buf, hdr->len, &e) != Zi::OK) goto error;
    } else {
      if (m_file.write((void *)buf, hdr->len, &e) != Zi::OK) goto error;
    }
  }
  return;

error:
  ZeLOG(Error, ([](auto &s) { s << '"' << m_path << "\": " << e; }));
}

App::App(const ZvCf *cf) :
  m_path(cf->get("path", true)),
  m_groups(cf->get("groups", true)),
  m_raw(cf->getBool("raw")),
  m_interface(cf->get("interface", "0.0.0.0")),
  m_reconnectFreq(cf->getInt("reconnect", 0, 3600, 0))
{
  m_mx = new Mx(cf->getCf("mx"));
  m_mx2 = new Mx(cf->getCf("mx2"));
}

int App::start()
{
  try {
    ZeError e;
    if (m_file.open(
	  m_path, ZiFile::Create | ZiFile::Append, 0666, &e) != Zi::OK) {
      ZeLOG(Fatal, ([](auto &s) { s << '"' << m_path << "\": " << e; }));
      goto error;
    }
    if (!m_mx->start() || !m_mx2->start()) {
      ZeLOG(Fatal, "multiplexer start failed");
      goto error;
    }
    GroupCSV csv;
    csv.read(m_groups, ZvCSVReadFn::Member<&App::connect>::fn(this));
  } catch (const ZvError &e) {
    ZeLOG(Fatal, ([](auto &s) { s << e; }));
    goto error;
  } catch (const ZeError &e) {
    ZeLOG(Fatal, ([](auto &s) { s << e; }));
    goto error;
  } catch (...) {
    ZeLOG(Fatal, "Unknown Exception");
    goto error;
  }
  return Zi::OK;

error:
  m_mx->stop();
  m_mx2->stop();
  m_file.close();
  return Zi::IOError;
}

void App::stop()
{
  m_mx->stop();
  m_mx2->stop();
  m_file.close();
}

void usage()
{
  std::cerr <<
    "Usage: mcap [OPTION]... CONFIG\n"
    "  capture IP multicast data as specified in the CONFIG file\n\n"
    "Options:\n"
    << std::flush;
  Zm::exit(1);
}

#if 0
static void printHeapStats()
{
  std::cout <<
    "\nHeap Statistics\n"
      "===============\n" <<
    ZmHeapMgr::csv() << std::flush;
}
#endif

ZmRef<App> app;

void sigint() { if (app) app->post(); }

int main(int argc, const char *argv[])
{
  const char *cfPath = 0;

  for (int i = 1; i < argc; i++) {
    if (argv[i][0] != '-') {
      if (cfPath) usage();
      cfPath = argv[i];
      continue;
    }
    switch (argv[i][1]) {
      default:
	usage();
	break;
    }
  }
  if (!cfPath) usage();

  ZeLog::init("mcap");
  ZeLog::level(0);
  ZeLog::sink(ZeLog::fileSink(ZeSinkOptions{}.path("&2")));
  ZeLog::start();

  {
    try {

      ZmRef<ZvCf> cf = new ZvCf();

      cf->fromFile(cfPath, false);

      if (ZtString heapCSV = cf->get("heap"))
	ZvHeapCSV::init(heapCSV);

      app = new App(cf);

    } catch (const ZvError &e) {
      ZeLOG(Fatal, ([](auto &s) { s << e; }));
      goto error;
    } catch (const ZeError &e) {
      ZeLOG(Fatal, ([](auto &s) { s << e; }));
      goto error;
    } catch (...) {
      ZeLOG(Fatal, "Unknown Exception");
      goto error;
    }

    ZmTrap::sigintFn(sigint);
    ZmTrap::trap();

    if (app->start() != Zi::OK) goto error;

    app->wait();

    ZmTrap::sigintFn(nullptr);

    app->stop();
  }

  ZeLog::stop();

  // printHeapStats();

  return 0;

error:
  ZeLog::stop();
  Zm::exit(1);
}
