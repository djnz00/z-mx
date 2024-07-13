//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#ifndef ZtelServer_HH
#define ZtelServer_HH

#ifndef ZcmdLib_HH
#include <zlib/ZcmdLib.hh>
#endif

#include <zlib/ZuLambdaTraits.hh>

#include <zlib/ZmFn.hh>

#include <zlib/ZtRegex.hh>

#include <zlib/ZvEngine.hh>

#include <zlib/Ztls.hh>

#include <zlib/Zfb.hh>

#include <zlib/Zcmd.hh>
#include <zlib/Ztel.hh>

#include <zlib/zv_telreq_fbs.h>
#include <zlib/zv_telack_fbs.h>

namespace Ztel {

using namespace Zcmd;

using QueueFn = ZvEngineMgr::QueueFn;

using BuildDBFn = ZmFn<void(IOBuilder &, Zfb::Offset<fbs::DB>)>;
using BuildDBHostFn = ZmFn<void(IOBuilder &, Zfb::Offset<fbs::DBHost>)>;
using BuildDBTableFn = ZmFn<void(IOBuilder &, Zfb::Offset<fbs::DBTable>)>;
// DBFn(buildDB, buildHost, buildTable, update)
using DBFn = ZmFn<void(BuildDBFn, BuildDBHostFn, BuildDBTableFn, bool)>;

class AlertFile {
  using BufRef = ZmRef<IOBuf>;
  using BufPtr = IOBuf *;

public:
  AlertFile() { }
  ~AlertFile() { close(); }

private:
  // do not call ZeLOG since that may well recurse back here, print to stderr
  template <typename Message>
  void error(bool index, const Message &message) {
    struct Fmt : public ZuDateTimeFmt::CSV { Fmt() { tzOffset(timezone); } };
    auto &dateFmt = ZmTLS<Fmt>();
    std::cerr << ZuDateTime{Zm::now()}.fmt(dateFmt) <<
      " FATAL " << m_path << (index ? ".idx" : "") <<
      ": " << message << '\n' << std::flush;
  }

  void open(ZuString prefix, unsigned date, unsigned flags) {
    m_date = date;
    m_seqNo = 0;
    if (!prefix) return;
    m_path = prefix;
    m_path << '_' << m_date;
    ZeError e;
    int i = m_file.open(m_path, flags, 0666, &e);
    if (i != Zi::OK) {
      if (e.errNo() == ZiENOENT && !(flags & ZiFile::Create)) return;
      error(false, e); return;
    }
    i = m_index.open(m_path + ".idx", flags, 0666, &e);
    if (i != Zi::OK) { m_file.close(); error(true, e); return; }
    m_offset = m_file.size();
    m_seqNo = m_index.size() / sizeof(size_t);
  }

public:
  void close() {
    m_file.close();
    m_index.close();
    m_path.null();
    m_date = 0;
    m_offset = 0;
    m_seqNo = 0;
  }

  unsigned date() const { return m_date; }
  size_t offset() const { return m_offset; }
  unsigned seqNo() const { return m_seqNo; }

  bool isOpen() const { return m_file; }

  // returns seqNo
  unsigned alloc(ZuString prefix, unsigned date) {
    if (date != m_date) {
      close();
      open(prefix, date, ZiFile::Create);
    }
    return m_seqNo;
  }
  void write(const BufPtr buf) {
    if (m_file) {
      ZeError e;
      if (m_file.pwrite(m_offset, buf->data(), buf->length, &e) != Zi::OK)
	error(false, e);
      else if (m_index.pwrite(m_seqNo * sizeof(size_t),
	    &m_offset, sizeof(size_t), &e) != Zi::OK)
	error(true, e);
    }
    m_seqNo++;
    m_offset += buf->length;
  }

  BufRef read(ZuString prefix, unsigned date, unsigned seqNo, void *bufOwner) {
    if (date != m_date) {
      close();
      open(prefix, date, ZiFile::ReadOnly);
    }
    if (!m_file) return BufRef{};
    if (seqNo >= m_seqNo) return BufRef{};
    size_t offset, next;
    ZeError e;
    if (m_index.pread(seqNo * sizeof(size_t),
	  &offset, sizeof(size_t), &e) != Zi::OK) {
      error(true, e);
      return BufRef{};
    }
    if (offset >= m_offset) {
      error(true, "corrupt");
      return BufRef{};
    }
    if (seqNo == m_seqNo - 1)
      next = m_offset;
    else {
      if (m_index.pread((seqNo + 1) * sizeof(size_t),
	    &next, sizeof(size_t), &e) != Zi::OK) {
	error(true, e);
	return BufRef{};
      }
    }
    if (next < offset || next > m_offset) {
      error(true, "corrupt");
      return BufRef{};
    }
    ZmRef<IOBuf> buf =
      new IOBuf{bufOwner, static_cast<unsigned>(next - offset)};
    if (m_file.pread(offset, buf->data(), buf->length, &e) != Zi::OK) {
      error(false, e);
      return BufRef{};
    }
    return buf;
  }

private:
  unsigned		m_date = 0; // YYYYMMDD
  size_t		m_offset = 0;
  unsigned		m_seqNo = 0;
  ZtString		m_path;
  ZiFile		m_file;
  ZiFile		m_index;
};

using AlertRing = ZmXRing<ZmRef<IOBuf>>;

template <typename App_, typename Link_>
class Server : public ZmEngine<Server<App_, Link_>>, ZvEngineMgr {
friend ZmEngine<Server>;

public:
  using App = App_;
  using Link = Link_;

  using ZmEngine<Server>::start;
  using ZmEngine<Server>::stop;

  const App *app() const { return static_cast<const App *>(this); }
  App *app() { return static_cast<App *>(this); }

  static App *app(const Link *link) {
    return static_cast<const App *>(link->app());
  }

  Server() {
    for (unsigned i = 0; i < ReqType::N; i++)
      m_watchLists[i].server = this;
  }

  bool init(ZiMultiplex *mx, const ZvCf *cf) {
    return ZmEngine<Server>::lock(
	ZmEngineState::Stopped, [this, mx, cf]() -> bool {
      m_mx = mx;

      if (!cf) {
	m_thread = mx->txThread();
	m_minInterval = 10;
	m_alertPrefix = "alerts";
	m_alertMaxReplay = 10;
	return true;
      }

      if (auto thread = cf->get("telemetry:thread", false))
	m_thread = mx->sid(thread);
      else
	m_thread = mx->txThread();

      m_minInterval = cf->getInt("telemetry:minInterval", 1, 1000000, 10);
      m_alertPrefix = cf->get("telemetry:alertPrefix", "alerts");
      m_alertMaxReplay = cf->getInt("telemetry:alertMaxReplay", 1, 1000, 10);

      return true;
    });
  }
  bool final() {
    return ZmEngine<Server>::lock(ZmEngineState::Stopped, [this]() {
      stop_();

      for (unsigned i = 0; i < ReqType::N; i++)
	m_watchLists[i].clean();

      m_alertRing.clean();
      m_alertFile.close();

      m_queues.clean();
      m_engines.clean();
      m_dbFn = DBFn{};
      return true;
    });
  }

  template <typename ...Args>
  void run(Args &&...args) const {
    m_mx->run(m_thread, ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  void invoke(Args &&...args) const {
    m_mx->invoke(m_thread, ZuFwd<Args>(args)...);
  }
  bool invoked() const { return m_mx->invoked(m_thread); }

private:
  template <typename L>
  bool spawn(L l) {
    if (!m_mx || !m_mx->running()) return false;
    run(ZuMv(l));
    return true;
  }

  void wake() {
    if (ZuUnlikely(!m_mx || !m_mx->running())) return;
    run([this]() { this->stopped(); });
  }

public:
  struct Request {
    Server	*server;
    ZmRef<Link>	link;
    int		type;
    unsigned	interval;
    ZmIDString	filter;
    bool	subscribe;
  };

  void process(Link *link, const fbs::Request *in) {
    invoke([req = Request{
      this, link, int(in->type()), in->interval(),
      Zfb::Load::str(in->filter()), in->subscribe()
    }]() mutable { req.server->process_(req); });
  }
  void process_(Request &req) {
    if (req.interval && req.interval < m_minInterval)
      req.interval = m_minInterval;
    switch (req.type) {
      case ReqType::Heap:	heapQuery(req); break;
      case ReqType::HashTbl:	hashQuery(req); break;
      case ReqType::Thread:	threadQuery(req); break;
      case ReqType::Mx:		mxQuery(req); break;
      case ReqType::Queue:	queueQuery(req); break;
      case ReqType::Engine:	engineQuery(req); break;
      case ReqType::DB:		dbQuery(req); break;
      case ReqType::App:	appQuery(req); break;
      case ReqType::Alert:	alertQuery(req); break;
      default: break;
    }
  }

  void disconnected(Link *link) {
    invoke([this, link = ZmMkRef(link)]() { disconnected_(link); });
  }

  // EngineMgr functions

  void updEngine(ZvEngine *engine) {
    invoke([this, engine = ZmMkRef(engine)]() { engineScan(engine); });
  }
  void updLink(ZvAnyLink *link) {
    invoke([this, link = ZmMkRef(link)]() { linkScan(link); });
  }

  void addEngine(ZvEngine *engine) {
    invoke([this, engine = ZmMkRef(engine)]() mutable {
      if (!m_engines.find(engine->id())) m_engines.add(ZuMv(engine));
    });
  }
  void delEngine(ZvEngine *engine) {
    invoke([this, id = engine->id()]() {
      m_engines.del(id);
    });
  }
  void addQueue(unsigned type, ZuID id, QueueFn queueFn) {
    invoke([this, type, id, queueFn = ZuMv(queueFn)]() mutable {
      auto key = ZuFwdTuple(type, id);
      if (!m_queues.find(key)) m_queues.add(key, ZuMv(queueFn));
    });
  }
  void delQueue(unsigned type, ZuID id) {
    invoke([this, type, id]() {
      auto key = ZuFwdTuple(type, id);
      m_queues.del(key);
    });
  }

  // DB registration
 
  void addDB(DBFn fn) {
    invoke([this, fn = ZuMv(fn)]() mutable {
      m_dbFn = ZuMv(fn);
    });
  }
  void delDB() {
    invoke([this]() { m_dbFn = DBFn{}; });
  }

  // app RAG updates

  void appUpdated() {
    invoke([this]() { m_appUpdated = true; });
  }

  // alerts

  template <typename L>
  void alert(ZeEvent<L> e) {
    invoke([this, e = ZuMv(e)]() mutable {
      alert_(ZuMv(e));
    });
  }

private:
  void start_() {
    for (int i = 0; i < ReqType::N; i++) {
      auto &list = m_watchLists[i];
      switch (i) {
	case int(fbs::ReqType::Heap):
	  reschedule(list, [](Server *server) { server->heapScan(); });
	  break;
	case int(fbs::ReqType::HashTbl):
	  reschedule(list, [](Server *server) { server->hashScan(); });
	  break;
	case int(fbs::ReqType::Thread):
	  reschedule(list, [](Server *server) { server->threadScan(); });
	  break;
	case int(fbs::ReqType::Mx):
	  reschedule(list, [](Server *server) { server->mxScan(); });
	  break;
	case int(fbs::ReqType::Queue):
	  reschedule(list, [](Server *server) { server->queueScan(); });
	  break;
	case int(fbs::ReqType::Engine):
	  reschedule(list, [](Server *server) { server->engineScan(); });
	  break;
	case int(fbs::ReqType::DB):
	  reschedule(list, [](Server *server) { server->dbScan(); });
	  break;
	case int(fbs::ReqType::App):
	  reschedule(list, [](Server *server) { server->appScan(); });
	  break;
	case int(fbs::ReqType::Alert):
	  reschedule(list, [](Server *server) { server->alertScan(); });
	  break;
      }
    }
  }

  void stop_() {
    for (unsigned i = 0; i < ReqType::N; i++) {
      auto &list = m_watchLists[i];
      m_mx->del(&list.timer);
    }
  }

  template <typename L>
  void alert_(ZeEvent<L> alert) {
    m_alertBuf.length(0);
    m_alertBuf << alert;
    ZuDateTime date{alert.time};
    unsigned yyyymmdd = date.yyyymmdd();
    unsigned seqNo = m_alertFile.alloc(m_alertPrefix, yyyymmdd);
    using namespace Zfb::Save;
    auto date_ = dateTime(date);
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData::Alert,
	  fbs::CreateAlert(m_fbb,
	    &date_, seqNo, alert.tid,
	    static_cast<fbs::Severity>(alert.severity),
	    str(m_fbb, m_alertBuf)).Union()));
    ZmRef<IOBuf> buf = m_fbb.buf();
    m_alertFile.write(buf);
    m_alertRing.push(ZuMv(buf));
  }

  using Queues =
    ZmRBTreeKV<ZuTuple<unsigned, ZuID>, QueueFn,
      ZmRBTreeUnique<true>>;

  static ZuID EngineIDAxor(const ZvEngine *engine) { return engine->id(); }
  using Engines =
    ZmRBTree<ZmRef<ZvEngine>,
      ZmRBTreeKey<EngineIDAxor,
	ZmRBTreeUnique<true>>>;

  struct Watch_ {
    Link	*link = nullptr;
    ZmIDString	filter;
  };
  using WatchList_ = ZmList<Watch_, ZmListNode<Watch_>>;
  using Watch = typename WatchList_::Node;
  struct WatchList {
    WatchList_		list;
    unsigned		interval = 0;	// in millisecs
    ZmScheduler::Timer	timer;
    Server		*server = nullptr;

    template <typename P>
    void push(P &&v) { list.pushNode(ZuFwd<P>(v)); }
    void clean() { list.clean(); }
    auto count() const { return list.count_(); }
    auto iterator() { return list.iterator(); }
    auto readIterator() const { return list.readIterator(); }
  };

  template <auto Fn>
  void subscribe(WatchList &list, Watch *watch, unsigned interval) {
    bool reschedule = false;
    if (!list.interval || interval < list.interval)
      if (list.interval != interval) {
	reschedule = true;
	list.interval = interval;
      }
    list.push(watch);
    if (reschedule) this->reschedule_<Fn>(list);
  }
  template <auto Fn>
  void reschedule(WatchList &list) {
    if (!list.interval) return;
    this->reschedule_<Fn>(list);
  }
  template <auto Fn>
  void reschedule_(WatchList &list) {
    run([list = &list]() {
      ZuInvoke<Fn>(list->server);
      list->server->template reschedule_<Fn>(*list);
    },
    Zm::now(ZuTime{ZuTime::Nano{int128_t(list.interval) * 1000000}}),
    ZmScheduler::Advance, &list.timer);
  }

  void unsubscribe(WatchList &list, Link *link, ZuString filter) {
    {
      auto i = list.iterator();
      while (auto watch = i.iterate())
	if (watch->link == link && (!filter || watch->filter == filter))
	  i.del();
    }
    if (!list.count() && list.interval) {
      list.interval = 0;
      m_mx->del(&list.timer);
    }
  }

  void disconnected_(Link *link) {
    for (unsigned i = 0; i < ReqType::N; i++)
      unsubscribe(m_watchLists[i], link, ZuString{});
  }

  bool match(ZuString filter, ZuString id) {
    if (!filter || filter[0] == '*') return true; // null or "*"
    unsigned n = filter.length() - 1;
    if (filter[n] == '*') { // "prefix*"
      if (id.length() < n) return false;
      return !memcmp(&filter[0], &id[0], n);
    }
    return filter == id;
  }
  bool matchThread(ZuString filter, ZuString name, unsigned tid) {
    if (!filter || filter[0] == '*') return true; // null or "*"
    unsigned n = filter.length() - 1;
    if (filter[n] == '*') { // "prefix*"
      if (name.length() < n) return false;
      return !memcmp(&filter[0], &name[0], n);
    }
    if (filter == name) return true;
    return ZuBox<unsigned>(filter) == tid;
  }
  bool matchQueue(ZuString filter, unsigned type, ZuString id) {
    if (!filter || filter[0] == '*') return true; // null or "*"
    unsigned n = filter.length() - 1;
    for (unsigned i = 0; i <= n; i++)
      if (filter[i] == ':') { // "type:id"
	if (!i || (i == 1 && filter[0] == '*')) { // ":id" or "*:id"
	  if (i == n || (i == n - 1 && filter[n] == '*'))
	    return true; // ":" or ":*" or "*:" or "*:*"
	  return ZuString{&filter[i + 1], n - i} == id;
	}
	int ftype = QueueType::lookup(ZuString{&filter[0], i});
	if (ftype != (int)type) return false;
	if (i == n || (i == n - 1 && filter[n] == '*'))
	  return true; // "type:" or "type:*"
	if (filter[n] == '*') { // "type:prefix*"
	  n -= (i + 1);
	  if (id.length() < n) return false;
	  return !memcmp(&filter[i + 1], &id[0], n);
	}
	n -= i;
	return ZuString{&filter[i + 1], n} == id;
      }
    if (filter[n] == '*') { // id*
      if (id.length() < n) return false;
      return !memcmp(&filter[0], &id[0], n);
    }
    return filter == id; // id
  }

  // heap processing

  void heapQuery(const Request &req) {
    auto &list = m_watchLists[ReqType::Heap];
    if (req.interval && !req.subscribe) {
      unsubscribe(list, req.link, req.filter);
      return;
    }
    auto watch = new Watch{req.link, req.filter};
    if (req.interval)
      this->subscribe<[](Server *server) { server->heapScan(); }>(
	  list, watch, req.interval);
    ZmHeapMgr::all(ZmFn<void(ZmHeapCache *)>{
      watch, [](Watch *watch, ZmHeapCache *heap) {
	watch->link->app()->heapQuery_(watch, heap);
      }});
    if (!req.interval) delete watch;
  }
  void heapQuery_(Watch *watch, const ZmHeapCache *heap) {
    Heap data;
    heap->telemetry(data);
    if (!match(watch->filter, data.id)) return;
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData::Heap, ZfbField::save(m_fbb, data).Union()));
    watch->link->sendTelemetry(m_fbb);
  }

  void heapScan() {
    if (!m_watchLists[ReqType::Heap].list.count_()) return;
    ZmHeapMgr::all(ZmFn<void(ZmHeapCache *)>{
      this, [](Server *server, ZmHeapCache *heap) {
	server->heapScan(heap);
      }});
  }
  void heapScan(const ZmHeapCache *heap) {
    Heap data;
    heap->telemetry(data);
    auto i = m_watchLists[ReqType::Heap].list.readIterator();
    while (auto watch = i.iterate()) {
      if (!match(watch->filter, data.id)) continue;
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData::Heap, ZfbField::saveUpd(m_fbb, data).Union()));
      watch->link->sendTelemetry(m_fbb);
    }
  }

  // hash table processing

  void hashQuery(const Request &req) {
    auto &list = m_watchLists[ReqType::HashTbl];
    if (req.interval && !req.subscribe) {
      unsubscribe(list, req.link, req.filter);
      return;
    }
    auto watch = new Watch{req.link, req.filter};
    if (req.interval)
      this->subscribe<[](Server *server) { server->hashScan(); }>(
	  list, watch, req.interval);
    ZmHashMgr::all(ZmFn<void(ZmAnyHash *)>{
      watch, [](Watch *watch, ZmAnyHash *tbl) {
	watch->link->app()->hashQuery_(watch, tbl);
      }});
    if (!req.interval) delete watch;
  }
  void hashQuery_(Watch *watch, const ZmAnyHash *tbl) {
    HashTbl data;
    tbl->telemetry(data);
    if (!match(watch->filter, data.id)) return;
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData::HashTbl, ZfbField::save(m_fbb, data).Union()));
    watch->link->sendTelemetry(m_fbb);
  }

  void hashScan() {
    if (!m_watchLists[ReqType::HashTbl].list.count_()) return;
    ZmHashMgr::all(ZmFn<void(ZmAnyHash *)>{
      this, [](Server *server, ZmAnyHash *tbl) {
	server->hashScan(tbl);
      }});
  }
  void hashScan(const ZmAnyHash *tbl) {
    HashTbl data;
    tbl->telemetry(data);
    auto i = m_watchLists[ReqType::HashTbl].list.readIterator();
    while (auto watch = i.iterate()) {
      if (!match(watch->filter, data.id)) continue;
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData::HashTbl, ZfbField::saveUpd(m_fbb, data).Union()));
      watch->link->sendTelemetry(m_fbb);
    }
  }

  // thread processing

  void threadQuery(const Request &req) {
    auto &list = m_watchLists[ReqType::Thread];
    if (req.interval && !req.subscribe) {
      unsubscribe(list, req.link, req.filter);
      return;
    }
    auto watch = new Watch{req.link, req.filter};
    if (req.interval)
      this->subscribe<[](Server *server) { server->threadScan(); }>(
	  list, watch, req.interval);
    ZmSpecific<ZmThreadContext>::all([watch](ZmThreadContext *tc) {
      watch->link->app()->threadQuery_(watch, tc);
    });
    if (!req.interval) delete watch;
  }
  void threadQuery_(Watch *watch, const ZmThreadContext *tc) {
    Thread data;
    tc->telemetry(data);
    if (!matchThread(watch->filter, data.name, data.tid)) return;
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData::Thread, ZfbField::save(m_fbb, data).Union()));
    watch->link->sendTelemetry(m_fbb);
  }

  void threadScan() {
    if (!m_watchLists[ReqType::Thread].list.count_()) return;
    ZmSpecific<ZmThreadContext>::all([this](ZmThreadContext *tc) {
      threadScan(tc);
    });
  }
  void threadScan(const ZmThreadContext *tc) {
    Thread data;
    tc->telemetry(data);
    auto i = m_watchLists[ReqType::Thread].list.readIterator();
    while (auto watch = i.iterate()) {
      if (!matchThread(watch->filter, data.name, data.tid)) continue;
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData::Thread, ZfbField::saveUpd(m_fbb, data).Union()));
      watch->link->sendTelemetry(m_fbb);
    }
  }

  // mx processing

  void mxQuery(const Request &req) {
    auto &list = m_watchLists[ReqType::Mx];
    if (req.interval && !req.subscribe) {
      unsubscribe(list, req.link, req.filter);
      return;
    }
    auto watch = new Watch{req.link, req.filter};
    if (req.interval)
      this->subscribe<[](Server *server) { server->mxScan(); }>(
	  list, watch, req.interval);
    ZiMxMgr::all([watch](ZiMultiplex *mx) {
      watch->link->app()->mxQuery_(watch, mx);
    });
    if (!req.interval) delete watch;
  }
  void mxQuery_(Watch *watch, ZiMultiplex *mx) {
    Mx data;
    mx->telemetry(data);
    if (!match(watch->filter, data.id)) return;
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData::Mx, ZfbField::save(m_fbb, data).Union()));
    watch->link->sendTelemetry(m_fbb);
    {
      using namespace Zfb::Save;
      uint64_t inCount, inBytes, outCount, outBytes;
      for (unsigned tid = 1, n = mx->params().nThreads(); tid <= n; tid++) {
	ZmIDString queueID;
	queueID << mx->params().id() << '.'
	  << mx->params().thread(tid).name();
	{
	  const auto &ring = mx->ring(tid);
	  ring.stats(inCount, inBytes, outCount, outBytes);
	  m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
		fbs::TelData::Queue,
		fbs::CreateQueue(m_fbb,
		  str(m_fbb, queueID), 0, ring.count_(),
		  inCount, inBytes, outCount, outBytes,
		  ring.params().size, ring.full(),
		  fbs::QueueType::Thread).Union()));
	  watch->link->sendTelemetry(m_fbb);
	}
	if (queueID.length() < ZmIDStrSize - 1)
	  queueID << '_';
	else
	  queueID[ZmIDStrSize - 2] = '_';
	{
	  const auto &overRing = mx->overRing(tid);
	  overRing.stats(inCount, outCount);
	  m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
		fbs::TelData::Queue,
		fbs::CreateQueue(m_fbb,
		  str(m_fbb, queueID), 0, overRing.count_(),
		  inCount, inCount * sizeof(ZmFn<>),
		  outCount, outCount * sizeof(ZmFn<>),
		  overRing.size_(), false,
		  fbs::QueueType::Thread).Union()));
	  watch->link->sendTelemetry(m_fbb);
	}
      }
    }
    mx->allCxns([this, watch](ZiConnection *cxn) {
      Socket data;
      cxn->telemetry(data);
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData::Socket, ZfbField::save(m_fbb, data).Union()));
      watch->link->sendTelemetry(m_fbb);
    });
  }

  void mxScan() {
    if (!m_watchLists[ReqType::Mx].list.count_()) return;
    ZiMxMgr::all([this](ZiMultiplex *mx) { mxScan(mx); });
  }
  void mxScan(ZiMultiplex *mx) {
    Mx data;
    mx->telemetry(data);
    auto i = m_watchLists[ReqType::Mx].list.readIterator();
    while (auto watch = i.iterate()) {
      if (!match(watch->filter, data.id)) continue;
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData::Mx, ZfbField::saveUpd(m_fbb, data).Union()));
      watch->link->sendTelemetry(m_fbb);
      {
	uint64_t inCount, inBytes, outCount, outBytes;
	for (unsigned tid = 1, n = mx->params().nThreads(); tid <= n; tid++) {
	  ZmIDString queueID;
	  queueID << mx->params().id() << '.'
	    << mx->params().thread(tid).name();
	  {
	    const auto &ring = mx->ring(tid);
	    ring.stats(inCount, inBytes, outCount, outBytes);
	    auto id_ = Zfb::Save::str(m_fbb, queueID);
	    fbs::QueueBuilder b(m_fbb);
	    b.add_id(id_);
	    b.add_count(ring.count_());
	    b.add_inCount(inCount);
	    b.add_inBytes(inBytes);
	    b.add_outCount(outCount);
	    b.add_outBytes(outBytes);
	    b.add_full(ring.full());
	    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
		  fbs::TelData::Queue, b.Finish().Union()));
	    watch->link->sendTelemetry(m_fbb);
	  }
	  if (queueID.length() < ZmIDStrSize - 1)
	    queueID << '_';
	  else
	    queueID[ZmIDStrSize - 2] = '_';
	  {
	    const auto &overRing = mx->overRing(tid);
	    overRing.stats(inCount, outCount);
	    auto id_ = Zfb::Save::str(m_fbb, queueID);
	    fbs::QueueBuilder b(m_fbb);
	    b.add_id(id_);
	    b.add_count(overRing.count_());
	    b.add_inCount(inCount);
	    b.add_inBytes(inCount * sizeof(ZmFn<>));
	    b.add_outCount(outCount);
	    b.add_outBytes(outCount * sizeof(ZmFn<>));
	    b.add_full(0);
	    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
		  fbs::TelData::Queue, b.Finish().Union()));
	    watch->link->sendTelemetry(m_fbb);
	  }
	}
      }
      mx->allCxns([this, watch](ZiConnection *cxn) {
	Socket data;
	cxn->telemetry(data);
	m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	      fbs::TelData::Socket, ZfbField::saveUpd(m_fbb, data).Union()));
	watch->link->sendTelemetry(m_fbb);
      });
    }
  }

  // queue processing
  // LATER - old queue code - used by caller of addQueue()
#if 0
    ZvQueue *queue;
    queue->stats(data.inCount, data.inBytes, data.outCount, data.outBytes);

    // I/O queues (link, etc.)
	  cxn->transmit(queue(

	    // IPC queues (ZiRing)
	    ring->params().name(), (uint64_t)0, (uint64_t)count,
	    inCount, inBytes, outCount, outBytes,
	    (uint32_t)ring->full(), (uint32_t)ring->params().size(),
	    (uint8_t)QueueType::IPC));
#endif

  void queueQuery(const Request &req) {
    auto &list = m_watchLists[ReqType::Queue];
    if (req.interval && !req.subscribe) {
      unsubscribe(list, req.link, req.filter);
      return;
    }
    auto watch = new Watch{req.link, req.filter};
    if (req.interval)
      this->subscribe<[](Server *server) { server->queueScan(); }>(
	  list, watch, req.interval);
    auto i = m_queues.readIterator();
    while (auto node = i.iterate()) queueQuery_(watch, node->val());
    if (!req.interval) delete watch;
  }
  void queueQuery_(Watch *watch, const QueueFn &fn) {
    Queue data;
    fn(data);
    if (!matchQueue(watch->filter, data.type, data.id)) return;
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData::Queue, ZfbField::save(m_fbb, data).Union()));
    watch->link->sendTelemetry(m_fbb);
  }
  void queueScan() {
    if (!m_watchLists[ReqType::Queue].list.count_()) return;
    auto i = m_queues.readIterator();
    while (auto node = i.iterate()) queueScan(node->val());
  }
  void queueScan(const QueueFn &fn) {
    Queue data;
    fn(data);
    auto i = m_watchLists[ReqType::Queue].list.readIterator();
    while (auto watch = i.iterate()) {
      if (!matchQueue(watch->filter, data.type, data.id)) continue;
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData::Queue, ZfbField::saveUpd(m_fbb, data).Union()));
      watch->link->sendTelemetry(m_fbb);
    }
  }

  // engine processing

  void engineQuery(const Request &req) {
    auto &list = m_watchLists[ReqType::Engine];
    if (req.interval && !req.subscribe) {
      unsubscribe(list, req.link, req.filter);
      return;
    }
    auto watch = new Watch{req.link, req.filter};
    if (req.interval)
      this->subscribe<[](Server *server) { server->engineScan(); }>(
	  list, watch, req.interval);
    {
      auto i = m_engines.readIterator();
      while (auto node = i.iterate()) engineQuery_(watch, node->val());
    }
    if (!req.interval) delete watch;
  }
  void engineQuery_(Watch *watch, ZvEngine *engine) {
    Engine data;
    engine->telemetry(data);
    if (!match(watch->filter, data.id)) return;
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData::Engine, ZfbField::save(m_fbb, data).Union()));
    watch->link->sendTelemetry(m_fbb);
    engine->allLinks<ZvAnyLink>([this, watch](ZvAnyLink *link) {
      Ztel::Link data;
      link->telemetry(data);
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData::Link, ZfbField::save(m_fbb, data).Union()));
      watch->link->sendTelemetry(m_fbb);
      return true;
    });
  }
  void engineScan() {
    if (!m_watchLists[ReqType::Engine].list.count_()) return;
    auto i = m_engines.readIterator();
    while (auto node = i.iterate()) engineScan(node->val());
  }
  void engineScan(ZvEngine *engine) {
    Engine data;
    engine->telemetry(data);
    auto i = m_watchLists[ReqType::Engine].list.readIterator();
    while (auto watch = i.iterate()) {
      if (!match(watch->filter, data.id)) continue;
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData::Engine, ZfbField::saveUpd(m_fbb, data).Union()));
      watch->link->sendTelemetry(m_fbb);
      engine->allLinks<ZvAnyLink>([this, watch](ZvAnyLink *link) {
	linkScan(link, watch);
	return true;
      });
    }
  }
  void linkScan(const ZvAnyLink *link) {
    auto i = m_watchLists[ReqType::Engine].list.readIterator();
    while (auto watch = i.iterate()) linkScan(link, watch);
  }
  void linkScan(const ZvAnyLink *link, Watch *watch) {
    Ztel::Link data;
    link->telemetry(data);
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData::Link, ZfbField::saveUpd(m_fbb, data).Union()));
    watch->link->sendTelemetry(m_fbb);
  }

  // DB processing

  void dbQuery(const Request &req) {
    auto &list = m_watchLists[ReqType::DB];
    if (req.interval && !req.subscribe) {
      unsubscribe(list, req.link, req.filter);
      return;
    }
    auto watch = new Watch{req.link, req.filter};
    if (req.interval)
      this->subscribe<[](Server *server) { server->dbScan(); }>(
	  list, watch, req.interval);
    dbQuery_(watch);
    if (!req.interval) delete watch;
  }
  void dbQuery_(Watch *watch) {
    if (!m_dbFn) return;
    // these callbacks can execute async
    m_dbFn(
      ZmFn<void(IOBuilder &, Zfb::Offset<fbs::DB>)>{ZmMkRef(watch->link),
	[](Link *link, IOBuilder &fbb, Zfb::Offset<fbs::DB> offset) {
	  fbb.Finish(fbs::CreateTelemetry(fbb,
		fbs::TelData::DB, offset.Union()));
	  link->sendTelemetry(fbb);
	}},
      ZmFn<void(IOBuilder &, Zfb::Offset<fbs::DBHost>)>{ZmMkRef(watch->link),
	[](Link *link, IOBuilder &fbb, Zfb::Offset<fbs::DBHost> offset) {
	  fbb.Finish(fbs::CreateTelemetry(fbb,
		fbs::TelData::DBHost, offset.Union()));
	  link->sendTelemetry(fbb);
	}},
      ZmFn<void(IOBuilder &, Zfb::Offset<fbs::DBTable>)>{ZmMkRef(watch->link),
	[](Link *link, IOBuilder &fbb, Zfb::Offset<fbs::DBTable> offset) {
	  fbb.Finish(fbs::CreateTelemetry(fbb,
		fbs::TelData::DBTable, offset.Union()));
	  link->sendTelemetry(fbb);
	}},
      false);
  }
  void dbScan() {
    if (!m_watchLists[ReqType::DB].list.count_()) return;
    if (!m_dbFn) return;
    auto i = m_watchLists[ReqType::DB].list.readIterator();
    while (auto watch = i.iterate()) {
      // these callbacks can execute async
      m_dbFn(
	ZmFn<void(IOBuilder &, Zfb::Offset<fbs::DB>)>{ZmMkRef(watch->link),
	  [](Link *link, IOBuilder &fbb, Zfb::Offset<fbs::DB> offset) {
	    fbb.Finish(fbs::CreateTelemetry(fbb,
		  fbs::TelData::DB, offset.Union()));
	    link->sendTelemetry(fbb);
	  }},
	ZmFn<void(IOBuilder &, Zfb::Offset<fbs::DBHost>)>{ZmMkRef(watch->link),
	  [](Link *link, IOBuilder &fbb, Zfb::Offset<fbs::DBHost> offset) {
	    fbb.Finish(fbs::CreateTelemetry(fbb,
		  fbs::TelData::DBHost, offset.Union()));
	    link->sendTelemetry(fbb);
	  }},
	ZmFn<void(IOBuilder &, Zfb::Offset<fbs::DBTable>)>{ZmMkRef(watch->link),
	  [](Link *link, IOBuilder &fbb, Zfb::Offset<fbs::DBTable> offset) {
	    fbb.Finish(fbs::CreateTelemetry(fbb,
		  fbs::TelData::DBTable, offset.Union()));
	    link->sendTelemetry(fbb);
	  }},
	true);
    }
  }

  // app processing

  void appQuery(const Request &req) {
    auto &list = m_watchLists[ReqType::App];
    if (req.interval && !req.subscribe) {
      unsubscribe(list, req.link, req.filter);
      return;
    }
    auto watch = new Watch{req.link, req.filter};
    if (req.interval)
      this->subscribe<[](Server *server) { server->appScan(); }>(
	  list, watch, req.interval);
    appQuery_(watch);
    if (!req.interval) delete watch;
  }
  void appQuery_(Watch *watch) {
    Ztel::App data;
    app()->telemetry(data);
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData::App,
	  ZfbField::save(m_fbb, data).Union()));
    watch->link->sendTelemetry(m_fbb);
  }
  void appScan() {
    if (!m_appUpdated) return;
    m_appUpdated = false;
    if (!m_watchLists[ReqType::App].list.count_()) return;
    Ztel::App data;
    app()->telemetry(data);
    auto i = m_watchLists[ReqType::App].list.readIterator();
    while (auto watch = i.iterate()) {
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData::App, ZfbField::saveUpd(m_fbb, data).Union()));
      watch->link->sendTelemetry(m_fbb);
    }
  }

  // alert processing

  void alertQuery(const Request &req) {
    auto &list = m_watchLists[ReqType::Alert];
    if (req.interval && !req.subscribe) {
      unsubscribe(list, req.link, req.filter);
      return;
    }
    auto watch = new Watch{req.link, req.filter};
    if (req.interval)
      this->subscribe<[](Server *server) { server->alertScan(); }>(
	  list, watch, req.interval);
    alertQuery_(watch);
    if (!req.interval) delete watch;
  }
  void alertQuery_(Watch *watch) {
    // parse filter - yyyymmdd:seqNo
    ZtRegex::Captures c;
    ZuBox<unsigned> date = 0, seqNo = 0;
    if (ZtREGEX("^(\d{8}):(\d+)$").m(watch->filter, c) == 3) {
      date = c[2];
      seqNo = c[3];
    }
    // ensure date is within range
    unsigned now = ZuDateTime{Zm::now()}.yyyymmdd();
    ZmRef<IOBuf> buf;
    if (!date)
      date = now;
    else if (date < now - m_alertMaxReplay)
      date = now - m_alertMaxReplay;
    // obtain date and seqNo of in-memory alert ring (today:UINT_MAX if empty)
    unsigned headDate = now;
    unsigned headSeqNo = UINT_MAX;
    {
      if (buf = m_alertRing.head()) {
	auto alert = fbs::GetTelemetry(buf->data())->data_as_Alert();
	if (ZuLikely(alert)) {
	  headDate = Zfb::Load::dateTime(alert->time()).yyyymmdd();
	  headSeqNo = alert->seqNo();
	}
      }
    }
    // replay from file(s) up to alerts available in-memory (if any)
    {
      AlertFile replay;
      while (date < headDate) {
	while (buf = replay.read(m_alertPrefix, date, seqNo++, this))
	  watch->link->send(ZuMv(buf));
	seqNo = 0;
	++date;
      }
      while (buf = replay.read(m_alertPrefix, date, seqNo++, this)) {
	if (seqNo >= headSeqNo) break;
	watch->link->send(ZuMv(buf));
      }
    }
    // replay from memory remaining alerts requested, up to latest
    {
      auto i = m_alertRing.iterator();
      while (buf = i.iterate()) {
	auto alert = fbs::GetTelemetry(buf->data())->data_as_Alert();
	if (ZuLikely(alert)) {
	  unsigned alertDate = Zfb::Load::dateTime(alert->time()).yyyymmdd();
	  unsigned alertSeqNo = alert->seqNo();
	  if (alertDate > date || (alertDate == date && alertSeqNo >= seqNo))
	    watch->link->send(ZuMv(buf));
	}
      }
    }
  }
  void alertScan() {
    // dequeue all alerts in-memory, send to all watchers
    while (ZmRef<IOBuf> buf = m_alertRing.shift()) {
      auto i = m_watchLists[ReqType::Alert].list.readIterator();
      while (auto watch = i.iterate()) watch->link->send(buf);
    }
  }

  ZiMultiplex		*m_mx = nullptr;
  unsigned		m_thread;
  unsigned		m_minInterval;	// min. refresh interval in millisecs
  ZtString		m_alertPrefix;	// prefix for alert files
  unsigned		m_alertMaxReplay;// max. replay in days

  // telemetry thread exclusive
  IOBuilder		m_fbb;
  Queues		m_queues;
  Engines		m_engines;
  DBFn			m_dbFn;
  WatchList		m_watchLists[ReqType::N];
  AlertRing		m_alertRing;		// in-memory ring of alerts
  AlertFile		m_alertFile;		// current file being written
  mutable ZtString	m_alertBuf;		// alert message buffer
  bool			m_appUpdated = false;
};

} // Ztel

#endif /* ZtelServer_HH */
