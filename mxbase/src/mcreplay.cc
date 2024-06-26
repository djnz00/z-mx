//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// multicast replay tool

#include <stdio.h>
#include <signal.h>

#include <zlib/ZuPOD.hh>

#include <zlib/ZmAtomic.hh>
#include <zlib/ZmSemaphore.hh>
#include <zlib/ZuTime.hh>
#include <zlib/ZmTrap.hh>
#include <zlib/ZmHash.hh>

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

class Dest : public ZmPolymorph {
public:
  Dest(App *app, const Group &group) : m_app(app), m_group(group) { }
  ~Dest() { }

  void connect();
  ZiConnection *connected(const ZiCxnInfo &ci);
  void connectFailed(bool transient);

  App *app() const { return m_app; }
  const Group &group() const { return m_group; }

private:
  App	*m_app;
  Group	m_group;
};

class Connection : public ZiConnection {
public:
  struct GroupIDAccessor {
    static uint16_t get(const Connection *c) { return c->groupID(); }
  };

  Connection(Dest *dest, const ZiCxnInfo &ci);
  ~Connection() { }

  App *app() const { return m_app; }
  uint16_t groupID() const { return m_groupID; }
  const ZiSockAddr &dest() const { return m_dest; }

  void connected(ZiIOContext &);
  void disconnected();

private:
  App		*m_app;
  uint16_t	m_groupID;
  ZiSockAddr	m_dest;
};

class Mx : public ZmObject, public ZiMultiplex {
public:
  Mx(const ZvCf *cf) : ZiMultiplex{ZvMxParams{cf}} { }
  ~Mx() { }
};

class App : public ZmPolymorph {
  using Cxns =
    ZmHash<ZmRef<Connection>,
      ZmHashKey<Connection::GroupIDAccessor,
	ZmHashObject<ZuNull> > >;

public:
  App(const ZvCf *cf);
  ~App();

  int start();
  void stop();

  void wait() { m_sem.wait(); }
  void post() { m_sem.post(); }

  void connect(ZuAnyPOD *pod);
  void read();

  ZuInline const ZtString &replay() const { return m_replay; }
  ZuInline const ZtString &groups() const { return m_groups; }
  ZuInline ZuBox<double> speed() const { return m_speed; }
  ZuInline ZuBox<double> interval() const { return m_interval; }
  ZuInline ZiIP interface_() const { return m_interface; }
  ZuInline int ttl() const { return m_ttl; }
  ZuInline bool loopBack() const { return m_loopBack; }

  Mx *mx() { return m_mx; }

  void connected_(Connection *cxn) { m_cxns->add(cxn); }
  void disconnected_(Connection *cxn) { m_cxns->del(cxn->groupID()); }
  int nCxns() { return m_cxns->count_(); }

private:
  ZmSemaphore	m_sem;

  ZtString	m_replay;	// path of capture file to replay
  ZtString	m_groups;	// CSV file containing multicast groups
  ZuBox<double>	m_speed;	// replay speed multiplier
  ZuBox<double>	m_interval;	// delay interval between ticks
  ZiIP		m_interface;	// interface to send to
  int		m_ttl;		// broadcast TTL
  bool		m_loopBack;	// broadcast loopback

  ZmLock	m_fileLock;	// lock on capture file
    ZiFile	m_file;		// capture file

  ZmRef<Mx>	m_mx;		// multiplexer

  ZmRef<Cxns>	m_cxns;
  ZuTime	m_prev;
};

template <typename Heap> class Msg_ : public Heap, public ZmPolymorph {
public:
  // UDP over Ethernet maximum payload is 1472 (without Jumbo frames)
  enum { Size = 1472 };

  Msg_(App *app) : m_app(app) { }
  ~Msg_() { }

  int read(ZiFile *);
  uint32_t group() { return m_hdr.group; }

  void send(Connection *);
  void send_(ZiIOContext &);
  void sent_(ZiIOContext &);

  ZuTime stamp() const {
    return ZuTime((time_t)m_hdr.sec, (int32_t)m_hdr.nsec);
  }

private:
  App			*m_app;
  Connection		*m_cxn;
  MxMCapHdr		m_hdr;
  char			m_buf[Size];
};
struct Msg_HeapID {
  static constexpr const char *id() { return "Msg"; }
};
typedef ZmHeap<Msg_HeapID, sizeof(Msg_<ZuNull>)> Msg_Heap;
typedef Msg_<Msg_Heap> Msg;

void App::connect(ZuAnyPOD *pod)
{
  const Group &group = pod->as<Group>();
  ZmRef<Dest> dest = new Dest(this, group);
  dest->connect();
}

void Dest::connect()
{
  ZiCxnOptions options;
  options.udp(true);
  options.multicast(true);
  options.mif(m_app->interface_());
  options.ttl(m_app->ttl());
  options.loopBack(m_app->loopBack());
  m_app->mx()->udp(
      ZiConnectFn::Member<&Dest::connected>::fn(ZmMkRef(this)),
      ZiFailFn::Member<&Dest::connectFailed>::fn(ZmMkRef(this)),
      ZiIP(), m_group.port, ZiIP(), 0, options);
}

ZiConnection *Dest::connected(const ZiCxnInfo &ci)
{
  return new Connection(this, ci);
}

void Dest::connectFailed(bool transient)
{
  if (transient)
    m_app->mx()->add(
	ZmFn<>::Member<&Dest::connect>::fn(this),
	Zm::now(1));
  else
    m_app->post();
}

Connection::Connection(Dest *dest, const ZiCxnInfo &ci) :
    ZiConnection(dest->app()->mx(), ci),
    m_app(dest->app()),
    m_groupID(dest->group().id),
    m_dest(dest->group().ip, dest->group().port)
{
}

void Connection::connected(ZiIOContext &io)
{
  io.complete();
  m_app->connected_(this);
}

void Connection::disconnected()
{
  if (m_app) m_app->disconnected_(this);
}

void App::read()
{
  ZmRef<Msg> msg = new Msg(this);

  {
    ZmGuard<ZmLock> guard(m_fileLock);
    if (msg->read(&m_file) != Zi::OK) { post(); return; }
  }

  if (auto node = m_cxns->findPtr(msg->group()))
    msg->send(node->key());

  ZuBox<double> delay;

  {
    ZuTime next = msg->stamp();

    if (next) {
      delay = !m_prev ? 0.0 : (next - m_prev).as_ldouble() / m_speed;
      m_prev = next;
    } else
      delay = 0;
  }

  delay += m_interval;

  if (delay.feq(0))
    mx()->add(ZmFn<>::Member<&App::read>::fn(this));
  else
    mx()->add(ZmFn<>::Member<&App::read>::fn(this), Zm::now(delay));
}

template <typename Heap>
int Msg_<Heap>::read(ZiFile *file)
{
  ZeError e;
  int n;

  n = file->read(&m_hdr, sizeof(MxMCapHdr), &e);
  if (n == Zi::IOError) goto error;
  if (n == Zi::EndOfFile || (unsigned)n < sizeof(MxMCapHdr)) goto eof;

  if (m_hdr.len > Size) goto lenerror;
  n = file->read(m_buf, m_hdr.len, &e);
  if (n == Zi::IOError) goto error;
  if (n == Zi::EndOfFile || (unsigned)n < m_hdr.len) goto eof;

  return Zi::OK;

eof:
  ZeLOG(Info, ([](auto &s) { s << '"' << m_app->replay() << "\": EOF"; }));
  return Zi::EndOfFile;

error:
  ZeLOG(Error, ([](auto &s) { s << '"' << m_app->replay() << "\": read() - " <<
      Zi::ioResult(n) << " - " << e; }));
  return Zi::IOError;

lenerror:
  {
    uint64_t offset = file->offset();
    offset -= sizeof(MxMCapHdr);
    ZeLOG(Error, ([](auto &s) { s << '"' << m_app->replay() << "\": "
	"message length >" << ZuBoxed(Size) <<
	" at offset " << ZuBoxed(offset); }));
  }
  return Zi::IOError;
}

template <typename Heap>
void Msg_<Heap>::send(Connection *cxn)
{
  (m_cxn = cxn)->send(ZiIOFn::Member<&Msg_<Heap>::send_>::fn(ZmMkRef(this)));
}
template <typename Heap>
void Msg_<Heap>::send_(ZiIOContext &io)
{
  io.init(ZiIOFn::Member<&Msg_<Heap>::sent_>::fn(ZmMkRef(this)),
      m_buf, m_hdr.len, 0, m_cxn->dest());
  m_cxn = nullptr;
}
template <typename Heap>
void Msg_<Heap>::sent_(ZiIOContext &io)
{
  if (ZuUnlikely((io.offset += io.length) < io.size)) return;
  io.complete();
}

App::App(const ZvCf *cf) :
  m_replay(cf->get("replay", true)),
  m_groups(cf->get("groups", true)),
  m_speed(cf->getDbl("speed", 0, ZuBox<double>::inf(), 1)),
  m_interval(cf->getDbl("interval", 0, 1, 0)),
  m_interface(cf->get("interface", "0.0.0.0")),
  m_ttl(cf->getInt("ttl", 0, INT_MAX, 1)),
  m_loopBack(cf->getBool("loopBack"))
{
  m_mx = new Mx(cf->getCf("mx"));
  m_cxns = new Cxns();
}

App::~App()
{
  m_cxns->clean();
}

int App::start()
{
  try {
    ZeError e;
    if (m_file.open(m_replay, ZiFile::ReadOnly, 0666, &e) != Zi::OK) {
      ZeLOG(Fatal, ([](auto &s) { s << '"' << m_replay << "\": " << e; }));
      goto error;
    }
    if (!m_mx->start()) {
      ZeLOG(Fatal, "multiplexer start failed");
      goto error;
    }
    GroupCSV csv;
    csv.read(m_groups, ZvCSVReadFn::Member<&App::connect>::fn(this));
    m_mx->add(ZmFn<>::Member<&App::read>::fn(this));
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
  m_file.close();
  return Zi::IOError;
}

void App::stop()
{
  m_mx->stop();
  m_file.close();
  m_cxns->clean();
}

void usage()
{
  std::cerr <<
    "usage: mcreplay [OPTION]... CONFIG\n"
    "  replay IP multicast data as specified in the CONFIG file\n\n"
    "Options:\n"
    << std::flush;
  Zm::exit(1);
}

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

  ZeLog::init("mcreplay");
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
  return 0;

error:
  ZeLog::stop();
  Zm::exit(1);
}
