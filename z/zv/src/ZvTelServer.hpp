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

#ifndef ZvTelServer_HPP
#define ZvTelServer_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

#include <zlib/ZuGrow.hpp>
#include <zlib/ZuLambdaFn.hpp>

#include <zlib/ZvTelemetry.hpp>
#include <zlib/ZvEngine.hpp>
#include <zlib/ZvCmdNet.hpp>

#include <zlib/telreq_fbs.h>
#include <zlib/telack_fbs.h>

namespace ZvTelemetry {

using QueueFn = ZvEngineMgr::QueueFn;

using DBEnvFn =
  ZmFn<ZmFn<const DBEnv &>, ZmFn<const DBHost &>, ZmFn<const DB &>>;

using IOBuf = ZiIOBuf<ZuGrow(0, 1)>;

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
    struct Fmt : public ZtDate::CSVFmt { Fmt() { offset(timezone); } };
    thread_local Fmt dateFmt;
    std::cerr << ZtDateNow().csv(dateFmt) <<
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
    ZmRef<IOBuf> buf = new IOBuf(bufOwner, next - offset);
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

using AlertRing = ZmDRing<ZmRef<IOBuf> >;

template <typename App_, typename Link_>
class Server : public ZvEngineMgr {
public:
  using App = App_;
  using Link = Link_;

private:
  using MxTbl =
    ZmRBTree<ZmRef<ZvMultiplex>,
      ZmRBTreeKey<ZvMultiplex::IDAccessor,
	ZmRBTreeUnique<true,
	  ZmRBTreeObject<ZuNull,
	    ZmRBTreeLock<ZmRWLock> > > > >;

public:
  const App *app() const { return static_cast<const App *>(this); }
  App *app() { return static_cast<App *>(this); }

  static App *app(const Link *link) {
    return static_cast<const App *>(link->app());
  }

  Server() {
    for (unsigned i = 0; i < ReqType::N; i++)
      m_watchLists[i].server = this;
  }

  void init(ZiMultiplex *mx, const ZvCf *cf) {
    m_mx = mx;

    if (!cf) {
      m_thread = mx->txThread();
      m_minInterval = 10;
      m_alertPrefix = "alerts";
      m_alertMaxReplay = 10;
      return;
    }

    if (auto thread = cf->get("telemetry:thread", false))
      m_thread = mx->tid(thread);
    else
      m_thread = mx->txThread();

    if (ZmRef<ZvCf> mxCf = cf->subset("mx")) {
      ZvCf::Iterator i(mxCf);
      ZuString key;
      while (ZmRef<ZvCf> mxCf_ = i.subset(key))
	m_mxTbl.add(new ZvMultiplex(key, mxCf_));
    }

    m_minInterval =
      cf->getInt("telemetry:minInterval", 1, 1000000, false, 10);
    m_alertPrefix = cf->get("telemetry:alertPrefix", false, "alerts");
    m_alertMaxReplay =
      cf->getInt("telemetry:alertMaxReplay", 1, 1000, false, 10);
  }
  void final() {
    stop_();

    for (unsigned i = 0; i < ReqType::N; i++)
      m_watchLists[i].clean();

    m_alertRing.clean();
    m_alertFile.close();

    m_mxTbl.clean();
    m_queues.clean();
    m_engines.clean();
    m_dbEnvFn = DBEnvFn{};
  }

  void start() {
    m_mx->invoke(
	m_thread, this, [](Server *server) { server->start_(); });
  }
  void stop() {
    m_mx->invoke(
	m_thread, this, [](Server *server) { server->stop_(); });
  }

  ZvMultiplex *mxLookup(const ZuID &id) const {
    return m_mxTbl.findVal(id);
  }
  template <typename L>
  void allMx(L l) const {
    auto i = m_mxTbl.readIterator();
    while (auto node = i.iterate()) l(node->val());
  }

  struct Request {
    Server	*server;
    ZmRef<Link>	link;
    int		type;
    unsigned	interval;
    ZmIDString	filter;
    bool	subscribe;
  };

  void process(Link *link, const fbs::Request *in) {
    auto request = new Request{
      this, link, in->type(), in->interval(),
      Zfb::Load::str(in->filter()), in->subscribe()
    };
    m_mx->invoke(m_thread, request, [](Request *req) {
      req->server->process_(req);
    });
  }
  void process_(Request *req) {
    if (req->interval && req->interval < m_minInterval)
      req->interval = m_minInterval;
    switch (req->type) {
      case ReqType::Heap:	heapQuery(req); break;
      case ReqType::HashTbl:	hashQuery(req); break;
      case ReqType::Thread:	threadQuery(req); break;
      case ReqType::Mx:		mxQuery(req); break;
      case ReqType::Queue:	queueQuery(req); break;
      case ReqType::Engine:	engineQuery(req); break;
      case ReqType::DBEnv:	dbEnvQuery(req); break;
      case ReqType::App:	appQuery(req); break;
      case ReqType::Alert:	alertQuery(req); break;
      default: break;
    }
    delete req;
  }

  void disconnected(Link *link) {
    m_mx->invoke(m_thread, this,
	[link = ZmMkRef(link)](Server *server) {
	  server->disconnected_(link);
	});
  }

  // EngineMgr functions

  void updEngine(ZvEngine *engine) {
    m_mx->invoke(m_thread, this,
	[engine = ZmMkRef(engine)](Server *server) {
	  server->engineScan(engine);
	});
  }
  void updLink(ZvAnyLink *link) {
    m_mx->invoke(m_thread, this,
	[link = ZmMkRef(link)](Server *server) {
	  server->linkScan(link);
	});
  }

  void addEngine(ZvEngine *engine) {
    m_mx->invoke(m_thread, this,
	[engine = ZmMkRef(engine)](Server *server) mutable {
	  if (!server->m_engines.find(engine->id()))
	    server->m_engines.add(ZuMv(engine));
	});
  }
  void delEngine(ZvEngine *engine) {
    m_mx->invoke(m_thread, this, [id = engine->id()](Server *server) {
      delete server->m_engines.del(id);
    });
  }
  void addQueue(unsigned type, ZuID id, QueueFn queueFn) {
    m_mx->invoke(m_thread, this,
	[type, id, queueFn = ZuMv(queueFn)](Server *server) mutable {
	  auto key = ZuFwdPair(type, id);
	  if (!server->m_queues.find(key))
	    server->m_queues.add(key, ZuMv(queueFn));
	});
  }
  void delQueue(unsigned type, ZuID id) {
    m_mx->invoke(m_thread, this, [type, id](Server *server) {
      auto key = ZuFwdPair(type, id);
      delete server->m_queues.del(key);
    });
  }

  // ZdbEnv registration
 
  void addDBEnv(DBEnvFn fn) {
    m_mx->invoke(m_thread, this,
	[fn = ZuMv(fn)](Server *server) mutable {
	  server->m_dbEnvFn = ZuMv(fn);
	});
  }
  void delDBEnv() {
    m_mx->invoke(m_thread, this, [](Server *server) {
      server->m_dbEnvFn = DBEnvFn{};
    });
  }

  // app RAG updates

  void appUpdated() {
    m_mx->invoke(m_thread, this, [](Server *server) {
      server->m_appUpdated = true;
    });
  }

  // alerts

  void alert(ZmRef<ZeEvent> e) {
    m_mx->invoke(m_thread, this,
	[e = ZuMv(e)](Server *server) mutable {
	  server->alert_(ZuMv(e));
	});
  }

private:
  void start_() {
    for (unsigned i = 0; i < ReqType::N; i++) {
      auto &list = m_watchLists[i];
      switch (i) {
	case fbs::ReqType_Heap:
	  reschedule(list, [](Server *server) { server->heapScan(); });
	  break;
	case fbs::ReqType_HashTbl:
	  reschedule(list, [](Server *server) { server->hashScan(); });
	  break;
	case fbs::ReqType_Thread:
	  reschedule(list, [](Server *server) { server->threadScan(); });
	  break;
	case fbs::ReqType_Mx:
	  reschedule(list, [](Server *server) { server->mxScan(); });
	  break;
	case fbs::ReqType_Queue:
	  reschedule(list, [](Server *server) { server->queueScan(); });
	  break;
	case fbs::ReqType_Engine:
	  reschedule(list, [](Server *server) { server->engineScan(); });
	  break;
	case fbs::ReqType_DBEnv:
	  reschedule(list, [](Server *server) { server->dbEnvScan(); });
	  break;
	case fbs::ReqType_App:
	  reschedule(list, [](Server *server) { server->appScan(); });
	  break;
	case fbs::ReqType_Alert:
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

  void alert_(ZmRef<ZeEvent> alert) {
    m_alertBuf.length(0);
    m_alertBuf << alert->message();
    ZtDate date{alert->time()};
    unsigned yyyymmdd = date.yyyymmdd();
    unsigned seqNo = m_alertFile.alloc(m_alertPrefix, yyyymmdd);
    using namespace Zfb::Save;
    auto date_ = dateTime(date);
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData_Alert,
	  fbs::CreateAlert(m_fbb,
	    &date_, seqNo, alert->tid(),
	    static_cast<fbs::Severity>(alert->severity()),
	    str(m_fbb, m_alertBuf)).Union()));
    ZmRef<IOBuf> buf = m_fbb.buf();
    m_alertFile.write(buf);
    m_alertRing.push(ZuMv(buf));
  }

  using Queues =
    ZmRBTreeKV<ZuPair<unsigned, ZuID>, QueueFn,
      ZmRBTreeUnique<true,
	ZmRBTreeObject<ZuNull,
	  ZmRBTreeLock<ZmNoLock> > > >;

  struct EngineIDAccessor {
    static ZuID get(const ZvEngine *engine) { return engine->id(); }
  };
  using Engines =
    ZmRBTree<ZmRef<ZvEngine>,
      ZmRBTreeKey<EngineIDAccessor,
	ZmRBTreeUnique<true,
	  ZmRBTreeObject<ZuNull,
	    ZmRBTreeLock<ZmNoLock> > > > >;

  struct Watch_ {
    Link	*link = nullptr;
    ZmIDString	filter;
  };
  using WatchList_ =
    ZmList<Watch_,
      ZmListObject<ZuNull,
	ZmListNodeDerive<true,
	  ZmListLock<ZmNoLock> > > >;
  using Watch = typename WatchList_::Node;
  struct WatchList {
    WatchList_		list;
    unsigned		interval = 0;	// in millisecs
    ZmScheduler::Timer	timer;
    Server		*server = nullptr;

    template <typename P>
    void push(P &&v) { list.pushNode(ZuFwd<P>(v)); }
    void clean() { list.clean(); }
    auto count() { return list.count(); }
    auto iterator() { return list.iterator(); }
    auto readIterator() { return list.readIterator(); }
  };

  template <typename L>
  void subscribe(WatchList &list, Watch *watch, unsigned interval, L) {
    bool reschedule = false;
    if (!list.interval || interval < list.interval)
      if (list.interval != interval) {
	reschedule = true;
	list.interval = interval;
      }
    list.push(watch);
    if (reschedule) this->reschedule<L>(list);
  }
  template <typename L>
  void reschedule(WatchList &list, L) {
    if (!list.interval) return;
    this->reschedule<L>(list);
  }
  template <typename L>
  void reschedule(WatchList &list) {
    m_mx->run(m_thread,
	ZmFn<>{&list, [](WatchList *list) {
	  ZuLambdaFn<L>::invoke(list->server);
	  list->server->template reschedule<L>(*list);
	}},
	ZmTimeNow() + ZmTime{ZmTime::Nano, ((int64_t)list.interval) * 1000000},
	ZmScheduler::Advance, &list.timer);
  }

  void unsubscribe(WatchList &list, Link *link, ZuString filter) {
    {
      auto i = list.iterator();
      while (auto watch = i.iterateNode())
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

  void heapQuery(Request *req) {
    auto &list = m_watchLists[ReqType::Heap];
    if (req->interval && !req->subscribe) {
      unsubscribe(list, req->link, req->filter);
      return;
    }
    auto watch = new Watch{req->link, req->filter};
    if (req->interval)
      this->subscribe(list, watch, req->interval,
	  [](Server *server) { server->heapScan(); });
    ZmHeapMgr::all(ZmFn<ZmHeapCache *>{
      watch, [](Watch *watch, ZmHeapCache *heap) {
	watch->link->app()->heapQuery_(watch, heap);
      }});
    if (!req->interval) delete watch;
  }
  void heapQuery_(Watch *watch, const ZmHeapCache *heap) {
    Heap data;
    heap->telemetry(data);
    if (!match(watch->filter, data.id)) return;
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData_Heap, ZvFB::save(m_fbb, data).Union()));
    ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
    watch->link->send(m_fbb.buf());
  }

  void heapScan() {
    if (!m_watchLists[ReqType::Heap].list.count()) return;
    ZmHeapMgr::all(ZmFn<ZmHeapCache *>{
      this, [](Server *server, ZmHeapCache *heap) {
	server->heapScan(heap);
      }});
  }
  void heapScan(const ZmHeapCache *heap) {
    Heap data;
    heap->telemetry(data);
    auto i = m_watchLists[ReqType::Heap].list.readIterator();
    while (auto watch = i.iterateNode()) {
      if (!match(watch->filter, data.id)) continue;
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData_Heap, ZvFB::saveUpdate(m_fbb, data).Union()));
      ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
      watch->link->send(m_fbb.buf());
    }
  }

  // hash table processing

  void hashQuery(Request *req) {
    auto &list = m_watchLists[ReqType::HashTbl];
    if (req->interval && !req->subscribe) {
      unsubscribe(list, req->link, req->filter);
      return;
    }
    auto watch = new Watch{req->link, req->filter};
    if (req->interval)
      this->subscribe(list, watch, req->interval,
	  [](Server *server) { server->hashScan(); });
    ZmHashMgr::all(ZmFn<ZmAnyHash *>{
      watch, [](Watch *watch, ZmAnyHash *tbl) {
	watch->link->app()->hashQuery_(watch, tbl);
      }});
    if (!req->interval) delete watch;
  }
  void hashQuery_(Watch *watch, const ZmAnyHash *tbl) {
    HashTbl data;
    tbl->telemetry(data);
    if (!match(watch->filter, data.id)) return;
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData_HashTbl, ZvFB::save(m_fbb, data).Union()));
    ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
    watch->link->send(m_fbb.buf());
  }

  void hashScan() {
    if (!m_watchLists[ReqType::HashTbl].list.count()) return;
    ZmHashMgr::all(ZmFn<ZmAnyHash *>{
      this, [](Server *server, ZmAnyHash *tbl) {
	server->hashScan(tbl);
      }});
  }
  void hashScan(const ZmAnyHash *tbl) {
    HashTbl data;
    tbl->telemetry(data);
    auto i = m_watchLists[ReqType::HashTbl].list.readIterator();
    while (auto watch = i.iterateNode()) {
      if (!match(watch->filter, data.id)) continue;
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData_HashTbl, ZvFB::saveUpdate(m_fbb, data).Union()));
      ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
      watch->link->send(m_fbb.buf());
    }
  }

  // thread processing

  void threadQuery(Request *req) {
    auto &list = m_watchLists[ReqType::Thread];
    if (req->interval && !req->subscribe) {
      unsubscribe(list, req->link, req->filter);
      return;
    }
    auto watch = new Watch{req->link, req->filter};
    if (req->interval)
      this->subscribe(list, watch, req->interval,
	  [](Server *server) { server->threadScan(); });
    ZmSpecific<ZmThreadContext>::all(ZmFn<ZmThreadContext *>{
      watch, [](Watch *watch, ZmThreadContext *tc) {
	watch->link->app()->threadQuery_(watch, tc);
      }});
    if (!req->interval) delete watch;
  }
  void threadQuery_(Watch *watch, const ZmThreadContext *tc) {
    Thread data;
    tc->telemetry(data);
    if (!matchThread(watch->filter, data.name, data.tid)) return;
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData_Thread, ZvFB::save(m_fbb, data).Union()));
    ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
    watch->link->send(m_fbb.buf());
  }

  void threadScan() {
    if (!m_watchLists[ReqType::Thread].list.count()) return;
    ZmSpecific<ZmThreadContext>::all(ZmFn<ZmThreadContext *>{
      this, [](Server *server, ZmThreadContext *tc) {
	server->threadScan(tc);
      }});
  }
  void threadScan(const ZmThreadContext *tc) {
    Thread data;
    tc->telemetry(data);
    auto i = m_watchLists[ReqType::Thread].list.readIterator();
    while (auto watch = i.iterateNode()) {
      if (!matchThread(watch->filter, data.name, data.tid)) continue;
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData_Thread, ZvFB::saveUpdate(m_fbb, data).Union()));
      ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
      watch->link->send(m_fbb.buf());
    }
  }

  // mx processing

  void mxQuery(Request *req) {
    auto &list = m_watchLists[ReqType::Mx];
    if (req->interval && !req->subscribe) {
      unsubscribe(list, req->link, req->filter);
      return;
    }
    auto watch = new Watch{req->link, req->filter};
    if (req->interval)
      this->subscribe(list, watch, req->interval,
	  [](Server *server) { server->mxScan(); });
    allMx(ZmFn<ZvMultiplex *>{watch, [](Watch *watch, ZvMultiplex *mx) {
	watch->link->app()->mxQuery_(watch, mx);
      }});
    if (!req->interval) delete watch;
  }
  void mxQuery_(Watch *watch, ZvMultiplex *mx) {
    Mx data;
    mx->telemetry(data);
    if (!match(watch->filter, data.id)) return;
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData_Mx, ZvFB::save(m_fbb, data).Union()));
    ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
    watch->link->send(m_fbb.buf());
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
		fbs::TelData_Queue,
		fbs::CreateQueue(m_fbb,
		  str(m_fbb, queueID), 0, ring.count(),
		  inCount, inBytes, outCount, outBytes,
		  ring.params().size(), ring.full(),
		  fbs::QueueType_Thread).Union()));
	  ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
	  watch->link->send(m_fbb.buf());
	}
	if (queueID.length() < ZmIDStrSize - 1)
	  queueID << '_';
	else
	  queueID[ZmIDStrSize - 2] = '_';
	{
	  const auto &overRing = mx->overRing(tid);
	  overRing.stats(inCount, outCount);
	  m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
		fbs::TelData_Queue,
		fbs::CreateQueue(m_fbb,
		  str(m_fbb, queueID), 0, overRing.count(),
		  inCount, inCount * sizeof(ZmFn<>),
		  outCount, outCount * sizeof(ZmFn<>),
		  overRing.size_(), false,
		  fbs::QueueType_Thread).Union()));
	  ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
	  watch->link->send(m_fbb.buf());
	}
      }
    }
    mx->allCxns([this, watch](ZiConnection *cxn) {
      Socket data;
      cxn->telemetry(data);
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData_Socket, ZvFB::save(m_fbb, data).Union()));
      ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
      watch->link->send(m_fbb.buf());
    });
  }

  void mxScan() {
    if (!m_watchLists[ReqType::Mx].list.count()) return;
    allMx(ZmFn<ZvMultiplex *>{this, [](Server *server, ZvMultiplex *mx) {
      server->mxScan(mx);
    }});
  }
  void mxScan(ZvMultiplex *mx) {
    Mx data;
    mx->telemetry(data);
    auto i = m_watchLists[ReqType::Mx].list.readIterator();
    while (auto watch = i.iterateNode()) {
      if (!match(watch->filter, data.id)) continue;
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData_Mx, ZvFB::saveUpdate(m_fbb, data).Union()));
      ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
      watch->link->send(m_fbb.buf());
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
	    b.add_count(ring.count());
	    b.add_inCount(inCount);
	    b.add_inBytes(inBytes);
	    b.add_outCount(outCount);
	    b.add_outBytes(outBytes);
	    b.add_full(ring.full());
	    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
		  fbs::TelData_Queue, b.Finish().Union()));
	    ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
	    watch->link->send(m_fbb.buf());
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
	    b.add_count(overRing.count());
	    b.add_inCount(inCount);
	    b.add_inBytes(inCount * sizeof(ZmFn<>));
	    b.add_outCount(outCount);
	    b.add_outBytes(outCount * sizeof(ZmFn<>));
	    b.add_full(0);
	    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
		  fbs::TelData_Queue, b.Finish().Union()));
	    ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
	    watch->link->send(m_fbb.buf());
	  }
	}
      }
      mx->allCxns([this, watch](ZiConnection *cxn) {
	Socket data;
	cxn->telemetry(data);
	m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	      fbs::TelData_Socket, ZvFB::saveUpdate(m_fbb, data).Union()));
	ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
	watch->link->send(m_fbb.buf());
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

  void queueQuery(Request *req) {
    auto &list = m_watchLists[ReqType::Queue];
    if (req->interval && !req->subscribe) {
      unsubscribe(list, req->link, req->filter);
      return;
    }
    auto watch = new Watch{req->link, req->filter};
    if (req->interval)
      this->subscribe(list, watch, req->interval,
	  [](Server *server) { server->queueScan(); });
    auto i = m_queues.readIterator();
    while (auto node = i.iterate()) queueQuery_(watch, node->val());
    if (!req->interval) delete watch;
  }
  void queueQuery_(Watch *watch, const QueueFn &fn) {
    Queue data;
    fn(data);
    if (!matchQueue(watch->filter, data.type, data.id)) return;
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData_Queue, ZvFB::save(m_fbb, data).Union()));
    ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
    watch->link->send(m_fbb.buf());
  }
  void queueScan() {
    if (!m_watchLists[ReqType::Queue].list.count()) return;
    auto i = m_queues.readIterator();
    while (auto node = i.iterate()) queueScan(node->val());
  }
  void queueScan(const QueueFn &fn) {
    Queue data;
    fn(data);
    auto i = m_watchLists[ReqType::Queue].list.readIterator();
    while (auto watch = i.iterateNode()) {
      if (!matchQueue(watch->filter, data.type, data.id)) continue;
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData_Queue, ZvFB::saveUpdate(m_fbb, data).Union()));
      ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
      watch->link->send(m_fbb.buf());
    }
  }

  // engine processing

  void engineQuery(Request *req) {
    auto &list = m_watchLists[ReqType::Engine];
    if (req->interval && !req->subscribe) {
      unsubscribe(list, req->link, req->filter);
      return;
    }
    auto watch = new Watch{req->link, req->filter};
    if (req->interval)
      this->subscribe(list, watch, req->interval,
	  [](Server *server) { server->engineScan(); });
    {
      auto i = m_engines.readIterator();
      while (auto node = i.iterate()) engineQuery_(watch, node->val());
    }
    if (!req->interval) delete watch;
  }
  void engineQuery_(Watch *watch, ZvEngine *engine) {
    Engine data;
    engine->telemetry(data);
    if (!match(watch->filter, data.id)) return;
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData_Engine, ZvFB::save(m_fbb, data).Union()));
    ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
    watch->link->send(m_fbb.buf());
    engine->allLinks<ZvAnyLink>([this, watch](ZvAnyLink *link) -> uintptr_t {
      ZvTelemetry::Link data;
      link->telemetry(data);
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData_Link, ZvFB::save(m_fbb, data).Union()));
      ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
      watch->link->send(m_fbb.buf());
      return 0;
    });
  }
  void engineScan() {
    if (!m_watchLists[ReqType::Engine].list.count()) return;
    auto i = m_engines.readIterator();
    while (auto node = i.iterate()) engineScan(node->val());
  }
  void engineScan(ZvEngine *engine) {
    Engine data;
    engine->telemetry(data);
    auto i = m_watchLists[ReqType::Engine].list.readIterator();
    while (auto watch = i.iterateNode()) {
      if (!match(watch->filter, data.id)) continue;
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData_Engine, ZvFB::saveUpdate(m_fbb, data).Union()));
      ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
      watch->link->send(m_fbb.buf());
      engine->allLinks<ZvAnyLink>([this, watch](ZvAnyLink *link) -> uintptr_t {
	linkScan(link, watch);
	return 0;
      });
    }
  }
  void linkScan(const ZvAnyLink *link) {
    auto i = m_watchLists[ReqType::Engine].list.readIterator();
    while (auto watch = i.iterateNode()) linkScan(link, watch);
  }
  void linkScan(const ZvAnyLink *link, Watch *watch) {
    ZvTelemetry::Link data;
    link->telemetry(data);
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData_Link, ZvFB::saveUpdate(m_fbb, data).Union()));
    ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
    watch->link->send(m_fbb.buf());
  }

  // DB processing

  void dbEnvQuery(Request *req) {
    auto &list = m_watchLists[ReqType::DBEnv];
    if (req->interval && !req->subscribe) {
      unsubscribe(list, req->link, req->filter);
      return;
    }
    auto watch = new Watch{req->link, req->filter};
    if (req->interval)
      this->subscribe(list, watch, req->interval,
	  [](Server *server) { server->dbEnvScan(); });
    dbEnvQuery_(watch);
    if (!req->interval) delete watch;
  }
  void dbEnvQuery_(Watch *watch) {
    if (!m_dbEnvFn) return;
    m_dbEnvFn([this, watch](const DBEnv &data) {
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData_DBEnv, ZvFB::save(m_fbb, data).Union()));
      ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
      watch->link->send(m_fbb.buf());
    }, [this, watch](const DBHost &data) {
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData_DBHost, ZvFB::save(m_fbb, data).Union()));
      ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
      watch->link->send(m_fbb.buf());
    }, [this, watch](const DB &data) {
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData_DB, ZvFB::save(m_fbb, data).Union()));
      ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
      watch->link->send(m_fbb.buf());
    });
  }
  void dbEnvScan() {
    if (!m_watchLists[ReqType::DBEnv].list.count()) return;
    if (!m_dbEnvFn) return;
    auto i = m_watchLists[ReqType::DBEnv].list.readIterator();
    while (auto watch = i.iterateNode()) {
      m_dbEnvFn([this, watch](const DBEnv &data) {
	m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	      fbs::TelData_DBEnv, ZvFB::saveUpdate(m_fbb, data).Union()));
	ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
	watch->link->send(m_fbb.buf());
      }, [this, watch](const DBHost &data) {
	m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	      fbs::TelData_DBHost, ZvFB::saveUpdate(m_fbb, data).Union()));
	ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
	watch->link->send(m_fbb.buf());
      }, [this, watch](const DB &data) {
	m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	      fbs::TelData_DB, ZvFB::saveUpdate(m_fbb, data).Union()));
	ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
	watch->link->send(m_fbb.buf());
      });
    }
  }

  // app processing

  void appQuery(Request *req) {
    auto &list = m_watchLists[ReqType::App];
    if (req->interval && !req->subscribe) {
      unsubscribe(list, req->link, req->filter);
      return;
    }
    auto watch = new Watch{req->link, req->filter};
    if (req->interval)
      this->subscribe(list, watch, req->interval,
	  [](Server *server) { server->appScan(); });
    appQuery_(watch);
    if (!req->interval) delete watch;
  }
  void appQuery_(Watch *watch) {
    ZvTelemetry::App data;
    app()->telemetry(data);
    m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	  fbs::TelData_App,
	  ZvFB::save(m_fbb, data).Union()));
    ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
    watch->link->send(m_fbb.buf());
  }
  void appScan() {
    if (!m_appUpdated) return;
    m_appUpdated = false;
    if (!m_watchLists[ReqType::App].list.count()) return;
    ZvTelemetry::App data;
    app()->telemetry(data);
    auto i = m_watchLists[ReqType::App].list.readIterator();
    while (auto watch = i.iterateNode()) {
      m_fbb.Finish(fbs::CreateTelemetry(m_fbb,
	    fbs::TelData_App, ZvFB::saveUpdate(m_fbb, data).Union()));
      ZvCmd::saveHdr(m_fbb, ZvCmd::Type::telemetry());
      watch->link->send(m_fbb.buf());
    }
  }

  // alert processing

  void alertQuery(Request *req) {
    auto &list = m_watchLists[ReqType::Alert];
    if (req->interval && !req->subscribe) {
      unsubscribe(list, req->link, req->filter);
      return;
    }
    auto watch = new Watch{req->link, req->filter};
    if (req->interval)
      this->subscribe(list, watch, req->interval,
	  [](Server *server) { server->alertScan(); });
    alertQuery_(watch);
    if (!req->interval) delete watch;
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
    unsigned now = ZtDateNow().yyyymmdd();
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
      while (auto watch = i.iterateNode()) watch->link->send(buf);
    }
  }

  ZiMultiplex		*m_mx = nullptr;
  unsigned		m_thread;
  unsigned		m_minInterval;	// min. refresh interval in millisecs
  ZtString		m_alertPrefix;	// prefix for alert files
  unsigned		m_alertMaxReplay;// max. replay in days

  // telemetry thread exclusive
  Zfb::IOBuilder<IOBuf>	m_fbb;
  MxTbl			m_mxTbl;
  Queues		m_queues;
  Engines		m_engines;
  DBEnvFn		m_dbEnvFn;
  WatchList		m_watchLists[ReqType::N];
  AlertRing		m_alertRing;		// in-memory ring of alerts
  AlertFile		m_alertFile;		// current file being written
  mutable ZtString	m_alertBuf;		// alert message buffer
  bool			m_appUpdated = false;
};

} // ZvTelemetry

#endif /* ZvTelServer_HPP */
