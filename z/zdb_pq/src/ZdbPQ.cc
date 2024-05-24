//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#ifndef _WIN32
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <linux/unistd.h>

#ifndef EPOLLRDHUP
#define EPOLLRDHUP 0
#endif
#endif

#include <zlib/ZdbPQ.hh>

#include <zlib/ZtCase.hh>
#include <zlib/ZtJoin.hh>

#include <zlib/ZeLog.hh>

namespace ZdbPQ {

#pragma pack(push, 1)
struct UInt32 { ZuBigEndian<uint32_t> v; };
#pragma pack(pop)

OIDs::OIDs()
{
  static const char *names[Value::N - 1] = {
    "text",
    "bytea",
    "bool",
    "int8",
    "uint8",
    "int1",
    "uint8",
    "float8",
    "zdecimal",
    "zdecimal",
    "ztime",
    "ztime",
    "int16",
    "uint16",
    "inet",
    "text"
  };

  m_names = names;
}

InitResult Store::init(ZvCf *cf, ZiMultiplex *mx, unsigned sid)
{
  m_cf = cf;
  m_mx = mx;
  m_zdbSID = sid;

  bool replicated;

  try {
    const ZtString &tid = cf->get<true>("thread");
    auto sid = m_mx->sid(tid);
    if (!sid ||
	sid > m_mx->params().nThreads() ||
	sid == m_mx->rxThread() ||
	sid == m_mx->txThread())
      return {ZeMEVENT(Fatal, ([tid = ZtString{tid}](auto &s, const auto &) {
	s << "Store::init() failed: invalid thread configuration \""
	  << tid << '"';
      }))};
    m_pqSID = sid;
    replicated = cf->getBool("replicated", false);
  } catch (const ZvError &e_) {
    ZtString e;
    e << e_;
    return {ZeMEVENT(Fatal, ([e = ZuMv(e)](auto &s, const auto &) {
      s << "Store::init() failed: invalid configuration: " << e;
    }))};
  }

  if (!m_storeTbls) m_storeTbls = new StoreTbls{};

  return {InitData{.replicated = replicated}};
}

void Store::final()
{
  m_storeTbls->clean();
  m_storeTbls = nullptr;
}

void Store::start(StartFn fn)
{
  ZeLOG(Debug, ([](auto &s) { }));

  m_mx->wakeFn(m_pqSID, ZmFn{this, [](Store *store) { store->wake(); }});
  m_mx->push(m_pqSID, [this, fn = ZuMv(fn)]() mutable {
    m_startState.reset();
    m_startFn = ZuMv(fn);
    m_stopFn = StopFn{};
    if (!start_()) {
      start_failed(ZeMEVENT(Fatal, "PostgreSQL start() failed"));
      return;
    }
    getOIDs();
    run_();
  });
}

static ZtString connError(PGconn *conn)
{
  ZtString error = PQerrorMessage(conn);
  error.chomp();
  return error;
}

bool Store::start_()
{
  ZeLOG(Debug, ([](auto &s) { }));

  const auto &connection = m_cf->get<true>("connection");

  m_conn = PQconnectdb(connection);

  if (!m_conn || PQstatus(m_conn) != CONNECTION_OK) {
    ZeLOG(Fatal, ([e = connError(m_conn)](auto &s) {
      s << "PQconnectdb() failed: " << e;
    }));
    return false;
  }

  m_connFD = PQsocket(m_conn);

  if (PQsetnonblocking(m_conn, 1) != 0) {
    ZeLOG(Fatal, ([e = connError(m_conn)](auto &s) {
      s << "PQsetnonblocking() failed: " << e;
    }));
    return false;
  }

  if (PQenterPipelineMode(m_conn) != 1) {
    ZeLOG(Fatal, ([e = connError(m_conn)](auto &s) {
      s << "PQenterPipelineMode() failed: " << e;
    }));
    return false;
  }

#ifndef _WIN32

  // set up I/O multiplexer (epoll)
  if ((m_epollFD = epoll_create(2)) < 0) {
    ZeLOG(Fatal, ([e = ZeLastError](auto &s) {
      s << "epoll_create() failed: " << e;
    }));
    return false;
  }
  if (pipe(&m_wakeFD) < 0) {
    ZeLOG(Fatal, ([e = errno](auto &s) {
      s << "pipe() failed: " << e;
    }));
    return false;
  }
  if (fcntl(m_wakeFD, F_SETFL, O_NONBLOCK) < 0) {
    ZeLOG(Fatal, ([e = errno](auto &s) {
      s << "fcntl(F_SETFL, O_NONBLOCK) failed: " << e;
    }));
    return false;
  }
  {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(struct epoll_event));
    ev.events = EPOLLIN;
    ev.data.u64 = 0;
    if (epoll_ctl(m_epollFD, EPOLL_CTL_ADD, m_wakeFD, &ev) < 0) {
      ZeLOG(Fatal, ([e = errno](auto &s) {
	s << "epoll_ctl(EPOLL_CTL_ADD) failed: " << e;
      }));
      return false;
    }
  }

  ZeLOG(Debug, ([this](auto &s) {
    s << "epoll_ctl(EPOLL_CTL_ADD) connFD=" << m_connFD;
  }));

  {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(struct epoll_event));
    ev.events = EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR | EPOLLET;
    ev.data.u64 = 1;
    epoll_ctl(m_epollFD, EPOLL_CTL_ADD, m_connFD, &ev);
  }

#else

  m_wakeSem = CreateSemaphore(nullptr, 0, 0x7fffffff, nullptr);
  if (m_wakeSem == NULL || m_wakeSem == INVALID_HANDLE_VALUE) {
    ZeLOG(Fatal, ([e = ZeLastError](auto &s) {
      s << "CreateEvent() failed: " << e;
    }));
    return false;
  }

  m_connEvent = WSACreateEvent();
  if (m_connEvent == NULL || m_connEvent == INVALID_HANDLE_VALUE) {
    ZeLOG(Fatal, ([e = ZeLastError](auto &s) {
      s << "CreateEvent() failed: " << e;
    }));
    return false;
  }
  if (WSAEventSelect(m_connFD, m_connEvent,
      FD_READ | FD_WRITE | FD_OOB | FD_CLOSE)) {
    ZeLOG(Fatal, ([e = WSAGetLastError()](auto &s) {
      s << "WSAEventSelect() failed: " << e;
    }));
    return false;
  }

#endif

  return true;
}

void Store::stop(StopFn fn)
{
  ZeLOG(Debug, ([](auto &s) { }));

  m_stopFn = ZuMv(fn);	// inhibits further application requests

  pqRun([this]() mutable { enqueue(Work::Stop{}); });
}

void Store::stop_()	// called after dequeuing Stop
{
  ZeLOG(Debug, ([](auto &s) { }));

  if (!m_sent.count_()) { stop_1(); return; }
}

void Store::stop_1()
{
  ZeLOG(Debug, ([](auto &s) { s << "pushing stop_2()"; }));

  m_mx->wakeFn(m_pqSID, ZmFn{});
  m_mx->push(m_pqSID, [this]() mutable {
    stop_2();
    StopFn stopFn = ZuMv(m_stopFn);
    m_stopFn = StopFn{};
    zdbRun([stopFn = ZuMv(stopFn)]() { stopFn(StopResult{}); });
  });
  wake_();
}

void Store::stop_2()
{
  ZeLOG(Debug, ([](auto &s) { }));

#ifndef _WIN32

  // close I/O multiplexer
  if (m_epollFD >= 0) {
    if (m_wakeFD >= 0)
      epoll_ctl(m_epollFD, EPOLL_CTL_DEL, m_wakeFD, 0);
    if (m_connFD >= 0)
      epoll_ctl(m_epollFD, EPOLL_CTL_DEL, m_connFD, 0);
    ::close(m_epollFD);
    m_epollFD = -1;
  }
  if (m_wakeFD >= 0) { ::close(m_wakeFD); m_wakeFD = -1; }
  if (m_wakeFD2 >= 0) { ::close(m_wakeFD2); m_wakeFD2 = -1; }

#else /* !_WIN32 */

  // close wakeup event
  if (m_wakeSem != INVALID_HANDLE_VALUE) {
    CloseHandle(m_wakeSem);
    m_wakeSem = INVALID_HANDLE_VALUE;
  }
  // close connection event
  if (m_connEvent != INVALID_HANDLE_VALUE) {
    CloseHandle(m_connEvent);
    m_connEvent = INVALID_HANDLE_VALUE;
  }

#endif /* !_WIN32 */

  // close PG connection
  if (m_conn) {
    PQfinish(m_conn);
    m_conn = nullptr;
    m_connFD = -1;
  }
}

void Store::wake()
{
  ZeLOG(Debug, ([](auto &s) { s << "pushing run_()"; }));

  m_mx->push(m_pqSID, [this]{ run_(); });
  wake_();
}

void Store::wake_()
{
  ZeLOG(Debug, ([](auto &s) { }));

#ifndef _WIN32
  char c = 0;
  while (::write(m_wakeFD2, &c, 1) < 0) {
    ZeError e{errno};
    if (e.errNo() != EINTR && e.errNo() != EAGAIN) {
      ZeLOG(Fatal, ([e](auto &s) { s << "write() failed: " << e; }));
      break;
    }
  }
#else /* !_WIN32 */
  if (!ReleaseSemaphore(m_wakeSem, 1, 0)) {
    ZeLOG(Fatal, ([e = ZeLastError](auto &s) {
      s << "ReleaseSemaphore() failed: " << e;
    }));
  }
#endif /* !_WIN32 */
}

void Store::run_()
{
  ZeLOG(Debug, ([](auto &s) { }));

  // "prime the pump" to ensure that write-readiness is
  // correctly signalled via epoll or WFMO
  send();

  for (;;) {

#ifndef _WIN32

    epoll_event ev[8];

    ZeLOG(Debug, ([](auto &s) { s << "epoll_wait()..."; }));

    int r = epoll_wait(m_epollFD, ev, 8, -1); // max events is 8

    ZeLOG(Debug, ([r](auto &s) { s << "epoll_wait(): " << r; }));

    if (r < 0) {
      ZeLOG(Fatal, ([e = errno](auto &s) {
	s << "epoll_wait() failed: " << e;
      }));
      return;
    }
    for (unsigned i = 0; i < unsigned(r); i++) {
      uint32_t events = ev[i].events;
      auto v = ev[i].data.u64; // ID

      ZeLOG(Debug, ([events, v](auto &s) {
	s << "epoll_wait() events=" << events << " v=" << v
	  << " EPOLLIN=" << ZuBoxed(EPOLLIN).hex()
	  << " EPOLLOUT=" << ZuBoxed(EPOLLOUT).hex();
      }));

      if (ZuLikely(!v)) {
	char c;
	int r = ::read(m_wakeFD, &c, 1);
	if (r >= 1) return;
	if (r < 0) {
	  ZeError e{errno};
	  if (e.errNo() != EINTR && e.errNo() != EAGAIN) return;
	}
	continue;
      }
      if (events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR))
	recv();
      if (events & EPOLLOUT)
	send();
    }

#else

    HANDLE handles[2] = { m_wakeSem, m_connEvent };
    DWORD event = WaitForMultipleObjectsEx(2, handles, false, INFINITE, false);
    if (event == WAIT_FAILED) {
      ZeLOG(Fatal, ([e = ZeLastError](auto &s) {
	s << "WaitForMultipleObjectsEx() failed: " << e;
      }));
      return;
    }
    if (event == WAIT_OBJECT_0) {
      // FIXME WFMO should have decremented the semaphore, but test this,
      // we may need to:
      // switch (WaitForSingleObject(m_wakeSem, 0)) {
      //   case WAIT_OBJECT_0: return;
      //   case WAIT_TIMEOUT:  break;
      // }
      return;
    }
    if (event == WAIT_OBJECT_0 + 1) {
      WSANETWORKEVENTS events;
      auto i = WSAEnumNetworkEvents(m_connFD, m_connEvent, &events);
      if (i != 0) {
	ZeLOG(Fatal, ([e = WSAGetLastError()](auto &s) {
	  s << "WSAEnumNetworkEvents() failed: " << e;
	}));
	return;
      }
      if (events.lNetworkEvents & (FD_READ|FD_OOB|FD_CLOSE))
	recv();
      if ((events.lNetworkEvents & (FD_WRITE|FD_CLOSE)) == FD_WRITE)
	send();
    }

#endif

  }
}

void Store::recv()
{
  ZeLOG(Debug, ([](auto &s) { }));

  bool stop = false;

  bool consumed;
  do {
    consumed = false;
    if (!PQconsumeInput(m_conn)) {
      ZeLOG(Fatal, ([e = connError(m_conn)](auto &s) {
	s << "PQconsumeInput() failed: " << e;
      }));
      return;
    }
    if (!PQisBusy(m_conn)) {
      PGresult *res = PQgetResult(m_conn);
      while (res) {
	consumed = true;
	if (auto pending = m_sent.headNode())
	  switch (PQresultStatus(res)) { // ExecStatusType
	    case PGRES_COMMAND_OK: // query succeeded - no tuples
	      break;
	    case PGRES_TUPLES_OK: // query succeeded - 0..N tuples
	      rcvd(pending, res);
	      break;
	    case PGRES_SINGLE_TUPLE: // query succeeded - 1 of N tuples
	      rcvd(pending, res);
	      break;
	    case PGRES_NONFATAL_ERROR: // notice / warning
	      failed(pending, ZeMEVENT(Error,
		  ([e = connError(m_conn)](auto &s, const auto &) {
		    s << "PQgetResult() query: " << e;
		  })));
	      break;
	    case PGRES_FATAL_ERROR: // query failed
	      failed(pending, ZeMEVENT(Fatal,
		  ([e = connError(m_conn)](auto &s, const auto &) {
		    s << "PQgetResult() query: " << e;
		  })));
	      break;
	    default: // ignore everything else
	      break;
	  }
	PQclear(res);
	if (PQisBusy(m_conn)) break; // nothing more to read (for now)
	res = PQgetResult(m_conn);
      }
      if (!res) { // PQgetResult() returned nullptr
	if (auto pending = m_sent.headNode()) {
	  rcvd(pending, nullptr);
	  m_sent.shift();
	  stop = stopping() && !m_queue.count_() && !m_sent.count_();
	}
      }
    }
  } while (consumed);

  if (stop) stop_1();
}

void Store::rcvd(Work::Queue::Node *work, PGresult *res)
{
  ZeLOG(Debug, ([res, n = (res ? int(PQntuples(res)) : 0)](auto &s) {
    s << "res=" << ZuBoxPtr(res).hex() << " n=" << n;
  }));

  using namespace Work;

  switch (work->data().type()) {
    case Task::Index<Start>{}:
      start_rcvd(res);
      break;
    case Task::Index<TblTask>{}: {
      auto &tblTask = work->data().p<TblTask>();
      switch (tblTask.query.type()) {
	case Query::Index<Open>{}:
	  tblTask.tbl->open_rcvd(res);
	  break;
	case Query::Index<Find>{}:
	  tblTask.tbl->find_rcvd(tblTask.query.p<Find>(), res);
	  break;
	case Query::Index<Recover>{}:
	  tblTask.tbl->recover_rcvd(tblTask.query.p<Recover>(), res);
	  break;
	case Query::Index<Write>{}:
	  tblTask.tbl->write_rcvd(tblTask.query.p<Write>(), res);
	  break;
      }
    } break;
  }
}

void Store::failed(Work::Queue::Node *work, ZeMEvent e)
{
  ZeLOG(Debug, ([](auto &s) { }));

  using namespace Work;

  switch (work->data().type()) {
    case Task::Index<Start>{}:
      start_failed(ZuMv(e));
      break;
    case Task::Index<TblTask>{}: {
      auto &tblTask = work->data().p<TblTask>();
      switch (tblTask.query.type()) {
	case Query::Index<Open>{}:
	  tblTask.tbl->open_failed(ZuMv(e));
	  break;
	case Query::Index<Find>{}:
	  tblTask.tbl->find_failed(tblTask.query.p<Find>(), ZuMv(e));
	  break;
	case Query::Index<Recover>{}:
	  tblTask.tbl->recover_failed(tblTask.query.p<Recover>(), ZuMv(e));
	  break;
	case Query::Index<Write>{}:
	  tblTask.tbl->write_failed(tblTask.query.p<Write>(), ZuMv(e));
	  break;
      }
    } break;
  }
}

// send() is called after every enqueue to prevent starvation; sequence is:
// wake(), enqueue(), dequeue(), send() (possible pushback), epoll_wait / WFMO

// to match results to requests, each result is matched to the head request
// on the sent request list, which is removed when the last tuple has
// been received

void Store::send()
{
  ZeLOG(Debug, ([](auto &s) { }));

  int sendState = SendState::Unsent;

  using namespace Work;

  // the queue includes queries and non-query tasks such as Start, Stop
  while (auto work = m_queue.shift()) {
    switch (work->data().type()) {
      case Task::Index<Start>{}:
	sendState = start_send();
	break;
      case Task::Index<Stop>{}:
	stop_();
	break;
      case Task::Index<TblTask>{}: {
	auto &tblTask = work->data().p<TblTask>();
	switch (tblTask.query.type()) {
	  case Query::Index<Open>{}:
	    sendState = tblTask.tbl->open_send();
	    break;
	  case Query::Index<Find>{}:
	    sendState = tblTask.tbl->find_send(tblTask.query.p<Find>());
	    break;
	  case Query::Index<Recover>{}:
	    sendState = tblTask.tbl->recover_send(tblTask.query.p<Recover>());
	    break;
	  case Query::Index<Write>{}:
	    sendState = tblTask.tbl->write_send(tblTask.query.p<Write>());
	    break;
	}
      } break;
    }
    if (sendState != SendState::Unsent) {
      if (sendState != SendState::Again)
	m_sent.pushNode(ZuMv(work).release());
      else
	m_queue.unshiftNode(ZuMv(work).release());
      break;
    }
  }

  // server-side flush or sync as required by the last sent query
  switch (sendState) {
    case SendState::Flush:
      if (PQsendFlushRequest(m_conn) != 1) {
	ZeLOG(Fatal, ([e = connError(m_conn)](auto &s) {
	  s << "PQsendFlushRequest() failed: " << e;
	}));
	return;
      }
      break;
    case SendState::Sync:
      if (PQpipelineSync(m_conn) != 1) {
	ZeLOG(Fatal, ([e = connError(m_conn)](auto &s) {
	  s << "PQsendFlushRequest() failed: " << e;
	}));
	return;
      }
      break;
  }

  // client-side flush unless already performed by PQpipelineSync()
  if (sendState != SendState::Sync) {
    // ... PQflush() regardless, to ensure client-side send buffer drainage
    // and correct signalling of write-readiness via epoll or WFMO
    if (PQflush(m_conn) < 0) {
      ZeLOG(Fatal, ([e = connError(m_conn)](auto &s) {
	s << "PQflush() failed: " << e;
      }));
      return;
    }
  }
}

void Store::start_enqueue()
{
  using namespace Work;

  enqueue(Task{Start{}});
}

int Store::start_send()
{
  switch (m_startState.phase()) {
    case StartState::GetOIDs:	return getOIDs_send();
    case StartState::MkSchema:	return mkSchema_send();
    case StartState::MkTblMRD:	return mkTblMRD_send();
  }
  return SendState::Unsent;
}

void Store::start_rcvd(PGresult *res)
{
  switch (m_startState.phase()) {
    case StartState::GetOIDs:	getOIDs_rcvd(res); break;
    case StartState::MkSchema:	mkSchema_rcvd(res); break;
    case StartState::MkTblMRD:	mkTblMRD_rcvd(res); break;
  }
}

void Store::start_failed(ZeMEvent e)
{
  ZeLOG(Debug, ([](auto &s) { }));

  stop_2();

  m_startState.phase(StartState::Started);
  m_startState.setFailed();

  auto startFn = ZuMv(m_startFn);

  m_startFn = StartFn{};

  zdbRun([e = ZuMv(e), startFn = ZuMv(startFn)]() mutable {
    startFn(StartResult{ZuMv(e)});
  });
}

void Store::started()
{
  ZeLOG(Debug, ([](auto &s) { }));

  m_startState.phase(StartState::Started);

  auto startFn = ZuMv(m_startFn);

  m_startFn = StartFn{};

  zdbRun([startFn = ZuMv(startFn)]() {
    startFn(StartResult{});
  });
}

void Store::getOIDs()
{
  m_startState.phase(StartState::GetOIDs);
  m_oids.init(Value::Index<String>{}, 25);	// TEXTOID
  start_enqueue();
}
int Store::getOIDs_send()
{
  ZeLOG(Debug, ([v = m_startState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  unsigned type = m_startState.type() + 1;
  // skip re-querying previously resolved OIDs
skip:
  auto name = m_oids.name(type);
  {
    auto oid = m_oids.oid(name);
    if (!ZuCmp<unsigned>::null(oid)) {
      if (type != Value::Index<String>{}) m_oids.init(type, oid);
      m_startState.incType();
      if (++type >= Value::N) {
	// all OIDs resolved
	mkSchema();
	return SendState::Unsent;
      }
      goto skip;
    }
  }

  Tuple params = { Value{String(name)} };
  return sendQuery<SendState::Flush, false>(
    "SELECT oid FROM pg_type WHERE typname = $1::text", params);
}
void Store::getOIDs_rcvd(PGresult *res)
{
  unsigned type = m_startState.type() + 1;

  ZeLOG(Debug, ([type](auto &s) { s << "type=" << type; }));

  if (!res) {
    if (m_startState.failed()) {
      // OID resolution failed
      auto e = ZeMEVENT(Fatal,
	([type = ZtString{m_oids.name(type)}](auto &s, const auto &) {
	  s << "failed to resolve OID for \"" << type << '"';
	}));
      start_failed(ZuMv(e));
    } else if (type >= Value::N) {
      // all OIDs resolved
      mkSchema();
    } else {
      // resolve next OID
      m_startState.incType();
      start_enqueue();
    }
    return;
  }

  if (PQntuples(res) != 1 ||
      PQnfields(res) != 1 ||
      PQgetlength(res, 0, 0) != 4) {
    // invalid query result
    m_startState.setFailed();
    return;
  }

  auto oid = uint32_t(reinterpret_cast<UInt32 *>(PQgetvalue(res, 0, 0))->v);

  ZeLOG(Debug, ([type, name = ZtString{m_oids.name(type)}, oid](auto &s) {
    s << "type=" << type << " name=" << name << " oid=" << oid;
  }));

  m_oids.init(type, oid);
}

void Store::mkSchema()
{
  m_startState.phase(StartState::MkSchema);
  start_enqueue();
}
int Store::mkSchema_send()
{
  ZeLOG(Debug, ([v = m_startState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  return sendQuery<SendState::Sync, false>(
    "CREATE SCHEMA IF NOT EXISTS \"zdb\"", Tuple{});
}
void Store::mkSchema_rcvd(PGresult *res)
{
  ZeLOG(Debug, ([v = m_startState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) mkTblMRD();
}

void Store::mkTblMRD()
{
  m_startState.phase(StartState::MkTblMRD);
  start_enqueue();
}
int Store::mkTblMRD_send()
{
  ZeLOG(Debug, ([v = m_startState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  // the MRD schema is unlikely to evolve, so use IF NOT EXISTS
  return sendQuery<SendState::Sync, false>(
    "CREATE TABLE IF NOT EXISTS \"zdb.mrd\" ("
      "\"tbl\" text PRIMARY KEY NOT NULL, "
      "\"_un\" uint8 NOT NULL, "
      "\"_sn\" uint16 NOT NULL)",
    Tuple{});
}
void Store::mkTblMRD_rcvd(PGresult *res)
{
  ZeLOG(Debug, ([v = m_startState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) started();
}

void Store::open(
  ZuID id,
  ZtMFields fields,
  ZtMKeyFields keyFields,
  const reflection::Schema *schema,
  MaxFn maxFn,
  OpenFn openFn)
{
  ZeLOG(Debug, ([](auto &s) { }));

  pqRun([
    this, id, fields = ZuMv(fields), keyFields = ZuMv(keyFields),
    schema, maxFn = ZuMv(maxFn), openFn = ZuMv(openFn)
  ]() mutable {
    if (stopping()) {
      zdbRun([id, openFn = ZuMv(openFn)]() mutable {
	openFn(OpenResult{ZeMEVENT(Error, ([id](auto &s, const auto &) {
	  s << "open(" << id << ") failed - DB shutdown in progress";
	}))});
      });
      return;
    }
    auto storeTbl =
      new StoreTbls::Node{this, id, ZuMv(fields), ZuMv(keyFields), schema};
    m_storeTbls->addNode(storeTbl);
    storeTbl->open(ZuMv(maxFn), ZuMv(openFn));
  });
}

void Store::enqueue(Work::Task task)
{
  ZeLOG(Debug, ([](auto &s) { }));

  m_queue.push(ZuMv(task));
  wake();
}

// resolve Value union discriminator from field metadata
static XField xField(
  const Zfb::Vector<Zfb::Offset<reflection::Field>> *fbFields_,
  const ZtMField *field,
  const ZtString &id)
{
  // resolve flatbuffers reflection data for field
  const reflection::Field *fbField = fbFields_->LookupByKey(id);
  if (!fbField) return {{}, nullptr, 0};
  unsigned type = 0;
  auto ftype = field->type;
  switch (fbField->type()->base_type()) {
    case reflection::String:
      if (ftype->code == ZtFieldTypeCode::CString ||
	  ftype->code == ZtFieldTypeCode::String)
	type = Value::Index<String>{};
      break;
    case reflection::Bool:
      if (ftype->code == ZtFieldTypeCode::Bool)
	type = Value::Index<Bool>{};
      break;
    case reflection::Byte:
    case reflection::Short:
    case reflection::Int:
    case reflection::Long:
      if (ftype->code == ZtFieldTypeCode::Int) {
	type = Value::Index<Int64>{};
      } else if (ftype->code == ZtFieldTypeCode::Enum) {
	type = Value::Index<Enum>{};
      }
      break;
    case reflection::UByte:
    case reflection::UShort:
    case reflection::UInt:
    case reflection::ULong:
      if (ftype->code == ZtFieldTypeCode::UInt) {
	type = Value::Index<UInt64>{};
      } else if (ftype->code == ZtFieldTypeCode::Flags) {
	type = Value::Index<Flags>{};
      }
      break;
    case reflection::Float:
    case reflection::Double:
      if (ftype->code == ZtFieldTypeCode::Float)
	type = Value::Index<Float>{};
      break;
    case reflection::Obj: {
      switch (ftype->code) {
	case ZtFieldTypeCode::Fixed:
	  type = Value::Index<Fixed>{};
	  break;
	case ZtFieldTypeCode::Decimal:
	  type = Value::Index<Decimal>{};
	  break;
	case ZtFieldTypeCode::Time:
	  type = Value::Index<Time>{};
	  break;
	case ZtFieldTypeCode::DateTime:
	  type = Value::Index<DateTime>{};
	  break;
	case ZtFieldTypeCode::UDT: {
	  auto ftindex = std::type_index{*(ftype->info.udt()->info)};
	  if (ftindex == std::type_index{typeid(int128_t)}) {
	    type = Value::Index<Int128>{};
	    break;
	  }
	  if (ftindex == std::type_index{typeid(uint128_t)}) {
	    type = Value::Index<UInt128>{};
	    break;
	  }
	  if (ftindex == std::type_index{typeid(ZiIP)}) {
	    type = Value::Index<IP>{};
	    break;
	  }
	  if (ftindex == std::type_index{typeid(ZuID)}) {
	    type = Value::Index<ID>{};
	    break;
	  }
	}
      }
    } break;
    default:
      break;
  }
  return {id, fbField, type};
}

StoreTbl::StoreTbl(
  Store *store, ZuID id, ZtMFields fields, ZtMKeyFields keyFields,
  const reflection::Schema *schema) :
  m_store{store}, m_id{id},
  m_fields{ZuMv(fields)}, m_keyFields{ZuMv(keyFields)},
  m_fieldMap{ZmHashParams(m_fields.length())}
{
  ZtCase::camelSnake(m_id, [this](const ZtString &id_) { m_id_ = id_; });
  const reflection::Object *rootTbl = schema->root_table();
  const Zfb::Vector<Zfb::Offset<reflection::Field>> *fbFields_ =
    rootTbl->fields();
  unsigned n = m_fields.length();
  m_xFields.size(n);
  for (unsigned i = 0; i < n; i++)
    ZtCase::camelSnake(m_fields[i]->id,
      [this, fbFields_, i](const ZtString &id_) {
	m_xFields.push(xField(fbFields_, m_fields[i], id_));
	m_fieldMap.add(id_, i);
      });
  n = m_keyFields.length();
  m_xKeyFields.size(n);
  for (unsigned i = 0; i < n; i++) {
    unsigned m = m_keyFields[i].length();
    new (m_xKeyFields.push()) XFields{m};
    for (unsigned j = 0; j < m; j++)
      ZtCase::camelSnake(m_keyFields[i][j]->id,
	[this, fbFields_, i, j](const ZtString &id_) {
	  m_xKeyFields[i].push(xField(fbFields_, m_keyFields[i][j], id_));
	});
  }
  m_maxBuf = new AnyBuf{};
}

StoreTbl::~StoreTbl()
{
}

void StoreTbl::open(MaxFn maxFn, OpenFn openFn)
{
  ZeLOG(Debug, ([](auto &s) { }));

  m_openState.reset();
  m_maxFn = ZuMv(maxFn);
  m_openFn = ZuMv(openFn);
  mkTable();
}

template <int State, bool MultiRow>
int Store::sendQuery(const ZtString &query, const Tuple &params)
{
  auto n = params.length();
  auto paramTypes = ZmAlloc(Oid, n);
  auto paramValues = ZmAlloc(const char *, n);
  auto paramLengths = ZmAlloc(int, n);
  auto paramFormats = ZmAlloc(int, n);
  for (unsigned i = 0; i < n; i++) {
    int type = params[i].type();
    paramTypes[i] = m_oids.oid(type);
    ZuSwitch::dispatch<Value::N>(type,
      [&params, &paramValues, &paramLengths, i](auto I) {
	paramValues[i] = params[i].data<I>();
	paramLengths[i] = params[i].length<I>();
      });
    paramFormats[i] = 1;
  }
  ZeLOG(Debug, ([query = ZtString{query}, n](auto &s) {
    s << '"' << query << "\", n=" << n;
  }));

  int r = PQsendQueryParams(
    m_conn, query.data(),
    n, paramTypes, paramValues, paramLengths, paramFormats, 1);
  if (r != 1) return SendState::Again;
  if constexpr (MultiRow)
    if (PQsetSingleRowMode(m_conn) != 1)
      ZeLOG(Error, ([e = connError(m_conn)](auto &s) {
	s << "PQsetSingleRowMode() failed: " << e;
      }));
  return State;
}

int Store::sendPrepare(
  const ZtString &id, const ZtString &query, ZuArray<Oid> oids)
{
  ZeLOG(Debug, ([id = ZtString{id}, query = ZtString{query}](auto &s) {
    s << '"' << id << "\", \"" << query << '"';
  }));

  int r = PQsendPrepare(
    m_conn, id.data(), query.data(), oids.length(), oids.data());
  if (r != 1) return SendState::Again;
  return SendState::Sync;
}

template <int State, bool MultiRow>
int Store::sendPrepared(const ZtString &id, const Tuple &params)
{
  auto n = params.length();
  auto paramValues = ZmAlloc(const char *, n);
  auto paramLengths = ZmAlloc(int, n);
  auto paramFormats = ZmAlloc(int, n);
  for (unsigned i = 0; i < n; i++) {
    ZuSwitch::dispatch<Value::N>(params[i].type(),
      [&params, &paramValues, &paramLengths, i](auto I) {
	paramValues[i] = params[i].data<I>();
	paramLengths[i] = params[i].length<I>();
      });
    paramFormats[i] = 1;
  }

  ZeLOG(Debug, ([id = ZtString{id}, n](auto &s) {
    s << '"' << id << "\", n=" << n;
  }));

  int r = PQsendQueryPrepared(
    m_conn, id.data(),
    n, paramValues, paramLengths, paramFormats, 1);
  if (r != 1) return SendState::Again;
  if constexpr (MultiRow)
    if (PQsetSingleRowMode(m_conn) != 1)
      ZeLOG(Error, ([e = connError(m_conn)](auto &s) {
	s << "PQsetSingleRowMode() failed: " << e;
      }));
  return State;
}

void StoreTbl::open_enqueue()
{
  using namespace Work;
  m_store->enqueue(Task{TblTask{this, {Open{}}}});
}

int StoreTbl::open_send()
{
  switch (m_openState.phase()) {
    case OpenState::MkTable:	return mkTable_send();
    case OpenState::MkIndices:	return mkIndices_send();
    case OpenState::PrepFind:	return prepFind_send();
    case OpenState::PrepInsert:	return prepInsert_send();
    case OpenState::PrepUpdate:	return prepUpdate_send();
    case OpenState::PrepDelete:	return prepDelete_send();
    case OpenState::PrepMRD:	return prepMRD_send();
    case OpenState::Count:	return count_send();
    case OpenState::MaxUN:	return maxUN_send();
    case OpenState::EnsureMRD:	return ensureMRD_send();
    case OpenState::MRD:	return mrd_send();
    case OpenState::Maxima:	return maxima_send();
  }
  return SendState::Unsent;
}

void StoreTbl::open_rcvd(PGresult *res)
{
  switch (m_openState.phase()) {
    case OpenState::MkTable:	mkTable_rcvd(res); break;
    case OpenState::MkIndices:	mkIndices_rcvd(res); break;
    case OpenState::PrepFind:	prepFind_rcvd(res); break;
    case OpenState::PrepInsert:	prepInsert_rcvd(res); break;
    case OpenState::PrepUpdate:	prepUpdate_rcvd(res); break;
    case OpenState::PrepDelete:	prepDelete_rcvd(res); break;
    case OpenState::PrepMRD:	prepMRD_rcvd(res); break;
    case OpenState::Count:	count_rcvd(res); break;
    case OpenState::MaxUN:	maxUN_rcvd(res); break;
    case OpenState::EnsureMRD:	ensureMRD_rcvd(res); break;
    case OpenState::MRD:	mrd_rcvd(res); break;
    case OpenState::Maxima:	maxima_rcvd(res); break;
  }
}

void StoreTbl::open_failed(Event e)
{
  ZeLOG(Debug, ([](auto &s) { }));

  m_openState.phase(OpenState::Opened);
  m_openState.setFailed();

  auto openFn = ZuMv(m_openFn);

  m_maxFn = MaxFn{};
  m_openFn = OpenFn{};

  m_store->zdbRun([e = ZuMv(e), openFn = ZuMv(openFn)]() mutable {
    openFn(OpenResult{ZuMv(e)});
  });
}

void StoreTbl::opened()
{
  ZeLOG(Debug, ([](auto &s) { }));

  m_openState.phase(OpenState::Opened);

  auto openFn = ZuMv(m_openFn);

  m_maxFn = MaxFn{};
  m_openFn = OpenFn{};

  m_store->zdbRun([this, openFn = ZuMv(openFn)]() {
    openFn(OpenResult{OpenData{
      .storeTbl = this,
      .count = m_count,
      .un = m_maxUN,
      .sn = m_maxSN
    }});
  });
}

void StoreTbl::mkTable()
{
  m_openState.phase(OpenState::MkTable);
  open_enqueue();
}
int StoreTbl::mkTable_send()
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!m_openState.create()) {
    Tuple params = { Value{String(m_id_)} };
    return m_store->sendQuery<SendState::Flush, true>(
      "SELECT a.attname AS name, a.atttypid AS oid "
      "FROM pg_catalog.pg_attribute a "
      "JOIN pg_catalog.pg_class c ON a.attrelid = c.oid "
      "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
      "WHERE c.relname = $1::text "
	"AND n.nspname = 'public' "
	"AND a.attnum > 0 "
	"AND NOT a.attisdropped", params);
  } else {
    ZtString query;
    query << "CREATE TABLE \"" << m_id_ << "\" ("
      "\"_un\" uint8 PRIMARY KEY NOT NULL, "
      "\"_sn\" uint16 NOT NULL, "
      "\"_vn\" int8 NOT NULL";
    unsigned n = m_xFields.length();
    for (unsigned i = 0; i < n; i++) {
      query << ", \"" << m_xFields[i].id_ << "\" "
	<< m_store->oids().name(m_xFields[i].type) << " NOT NULL";
    }
    query << ")";
    return m_store->sendQuery<SendState::Sync, false>(query, Tuple{});
  }
}
void StoreTbl::mkTable_rcvd(PGresult *res)
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (m_openState.create()) {
    if (!res) mkIndices();
    return;
  }

  if (!res) {
    if (!m_openState.failed() && m_openState.field() >= m_xFields.length()) {
      // table exists, all fields ok, proceed to indices
      mkIndices();
    } else if (!m_openState.failed() && !m_openState.field()) {
      // table does not exist, create it
      m_openState.setCreate();
      open_enqueue();
    } else {
      // table exists but not all fields matched
      auto e = ZeMEVENT(Fatal, ([id = this->m_id_](auto &s, const auto &) {
	s << "inconsistent schema for table " << id;
      }));
      open_failed(ZuMv(e));
    }
    return;
  }

  if (m_openState.failed()) return;

  unsigned n = PQntuples(res);
  if (n && (PQnfields(res) != 2)) {
    m_openState.setFailed();
    return;
  }
  for (unsigned i = 0; i < n; i++) {
    const char *id_ = PQgetvalue(res, i, 0);
    ZuString id{id_};
    if (PQgetlength(res, i, 1) != 4) {
      m_openState.setFailed();
      return;
    }
    unsigned oid = reinterpret_cast<UInt32 *>(PQgetvalue(res, i, 1))->v;
    unsigned field = ZuCmp<unsigned>::null();
    unsigned type = ZuCmp<unsigned>::null();
    if (id == "_un") {
      type = Value::Index<UInt64>{};
    } else if (id == "_sn") {
      type = Value::Index<UInt128>{};
    } else if (id == "_vn") {
      type = Value::Index<Int64>{};
    } else {
      field = m_fieldMap.findVal(id);
      if (!ZuCmp<unsigned>::null(field)) {
	m_openState.incField();
	type = m_xFields[field].type;
      }
    }
    bool match = false;
    if (!ZuCmp<unsigned>::null(type))
      match = m_store->oids().match(oid, type);
    ZeLOG(Debug, ([
      id = ZtString{id}, oid, field, match, state = m_openState.v
    ](auto &s) {
      int field_ = ZuCmp<unsigned>::null(field) ? -1 : int(field);
      s << "id=" << id << " oid=" << oid
	<< " field=" << field_ << " match=" << (match ? 'T' : 'F')
	<< " openState=" << ZuBoxed(state).hex();
    }));
    if (!m_openState.failed() && !match) {
      m_openState.setFailed();
      return;
    }
  }
}

void StoreTbl::mkIndices()
{
  m_openState.phase(OpenState::MkIndices);
  open_enqueue();
}
int StoreTbl::mkIndices_send()
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  auto keyID = m_openState.keyID();
  ZtString name(m_id_.length() + 16);
  name << m_id_ << '_' << keyID;
  if (!m_openState.create()) {
    Tuple params = { Value{String(name)} };
    return m_store->sendQuery<SendState::Flush, true>(
      "SELECT a.attname AS name, a.atttypid AS oid "
      "FROM pg_class t "
      "JOIN pg_index i ON t.oid = i.indrelid "
      "JOIN pg_class d ON d.oid = i.indexrelid "
      "JOIN pg_namespace n ON n.oid = t.relnamespace "
      "JOIN pg_attribute a ON a.attrelid = t.oid "
      "WHERE d.relname = $1::text "
	"AND n.nspname = 'public' "
	"AND a.attnum = ANY(i.indkey) "
	"AND NOT a.attisdropped "
      "ORDER BY array_position(i.indkey, a.attnum)", params);
  } else {
    ZtString query;
    // LATER we could consider using hash indices for non-series
    query << "CREATE INDEX \"" << name << "\" ON \"" << m_id_ << "\" (";
    const auto &keyFields = m_keyFields[keyID];
    const auto &xKeyFields = m_xKeyFields[keyID];
    unsigned n = xKeyFields.length();
    for (unsigned i = 0; i < n; i++) {
      if (i) query << ", ";
      query << '"' << xKeyFields[i].id_ << '"';
      if (keyFields[i]->props & ZtMFieldProp::Series) query << " DESC";
    }
    query << ")";
    return m_store->sendQuery<SendState::Sync, false>(query, Tuple{});
  }
}
void StoreTbl::mkIndices_rcvd(PGresult *res)
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  auto nextKey = [this]() {
    m_openState.incKey();
    if (m_openState.keyID() >= m_keyFields.length())
      prepFind();
    else
      open_enqueue();
  };

  if (m_openState.create()) {
    if (!res) nextKey();
    return;
  }

  if (!res) {
    auto keyID = m_openState.keyID();
    unsigned nFields = m_xKeyFields[keyID].length();
    if (!m_openState.failed() && m_openState.field() >= nFields) {
      // index exists, all fields ok, proceed to next index
      nextKey();
    } else if (!m_openState.failed() && !m_openState.field()) {
      // index does not exist, create it
      m_openState.setCreate();
      open_enqueue();
    } else {
      // index exists but not all fields matched
      open_failed(ZeMEVENT(Fatal, ([id = m_id_](auto &s, const auto &) {
	s << "inconsistent schema for table " << id;
      })));
    }
    return;
  }

  if (m_openState.failed()) return;

  unsigned n = PQntuples(res);
  if (n && (PQnfields(res) != 2)) {
    m_openState.setFailed();
    return;
  }
  for (unsigned i = 0; i < n; i++) {
    const char *id_ = PQgetvalue(res, i, 0);
    ZuString id{id_};
    if (PQgetlength(res, i, 1) != sizeof(UInt32)) {
      m_openState.setFailed();
      return;
    }
    unsigned oid = reinterpret_cast<UInt32 *>(PQgetvalue(res, i, 1))->v;
    auto keyID = m_openState.keyID();
    unsigned field = m_openState.field();
    const auto &xKeyFields = m_xKeyFields[keyID];
    ZuString matchID = xKeyFields[field].id_;
    unsigned type = xKeyFields[field].type;
    bool match = m_store->oids().match(oid, type) && id == matchID;
    ZeLOG(Debug, ([
      id = ZtString{id}, oid, field, match, state = m_openState.v
    ](auto &s) {
      int field_ = ZuCmp<unsigned>::null(field) ? -1 : int(field);
      s << "id=" << id << " oid=" << oid
	<< " field=" << field_ << " match=" << (match ? 'T' : 'F')
	<< " openState=" << ZuBoxed(state).hex();
    }));
    if (!m_openState.failed() && !match) {
      m_openState.setFailed();
      return;
    }
    m_openState.incField();
  }
}

void StoreTbl::prepFind()
{
  m_openState.phase(OpenState::PrepFind);
  open_enqueue();
}
int StoreTbl::prepFind_send()
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  unsigned keyID = m_openState.keyID();
  ZtString id(m_id_.length() + 24);
  id << m_id_;
  if (!keyID)
    id << "_recover";
  else
    id << "_find_" << (keyID - 1);
  ZtString query;
  query << "SELECT \"_un\", \"_sn\", \"_vn\"";
  unsigned n = m_xFields.length();
  for (unsigned i = 0; i < n; i++) {
    query << ", \"" << m_xFields[i].id_ << '"';
  }
  query << " FROM \"" << m_id_ << "\" WHERE ";
  ZtArray<Oid> oids;
  if (!keyID) {
    query << "\"_un\"=$1::uint8";
    oids.push(m_store->oids().oid(Value::Index<UInt64>{}));
  } else {
    const auto &xKeyFields = m_xKeyFields[keyID - 1];
    n = xKeyFields.length();
    oids.size(n);
    for (unsigned i = 0; i < n; i++) {
      auto type = xKeyFields[i].type;
      if (i) query << " AND ";
      query << '"' << xKeyFields[i].id_
	<< "\"=$" << (i + 1) << "::" << m_store->oids().name(type);
      oids.push(m_store->oids().oid(type));
    }
  }
  return m_store->sendPrepare(id, query, oids);
}
void StoreTbl::prepFind_rcvd(PGresult *res)
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) {
    m_openState.incKey();
    if (m_openState.keyID() > m_keyFields.length()) // not >=
      prepInsert();
    else
      open_enqueue();
  }
}

void StoreTbl::prepInsert()
{
  m_openState.phase(OpenState::PrepInsert);
  open_enqueue();
}
int StoreTbl::prepInsert_send()
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  ZtString id(m_id_.length() + 8);
  id << m_id_ << "_insert";
  ZtString query;
  unsigned n = m_xFields.length();
  ZtArray<Oid> oids(n + 3);
  query << "INSERT INTO \"" << m_id_ << "\" (\"_un\", \"_sn\", \"_vn\"";
  for (unsigned i = 0; i < n; i++) {
    query << ", \"" << m_xFields[i].id_ << '"';
  }
  query << ") VALUES ($1::uint8, $2::uint16, $3::uint8";
  oids.push(m_store->oids().oid(Value::Index<UInt64>{}));
  oids.push(m_store->oids().oid(Value::Index<UInt128>{}));
  oids.push(m_store->oids().oid(Value::Index<Int64>{}));
  for (unsigned i = 0; i < n; i++) {
    auto type = m_xFields[i].type;
    query << ", $" << (i + 4) << "::" << m_store->oids().name(type);
    oids.push(m_store->oids().oid(type));
  }
  query << ')';
  return m_store->sendPrepare(id, query, oids);
}
void StoreTbl::prepInsert_rcvd(PGresult *res)
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) prepUpdate();
}

void StoreTbl::prepUpdate()
{
  m_openState.phase(OpenState::PrepUpdate);
  open_enqueue();
}
int StoreTbl::prepUpdate_send()
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  ZtString id(m_id_.length() + 8);
  id << m_id_ << "_update";
  ZtString query;
  unsigned n = m_xFields.length();
  const auto &keyFields = m_keyFields[0];
  ZtArray<Oid> oids(n + 3 + keyFields.length());
  query << "UPDATE \"" << m_id_
    << "\" SET \"_un\"=$1::uint8, \"_sn\"=$2::uint16, \"_vn\"=$3::int8";
  oids.push(m_store->oids().oid(Value::Index<UInt64>{}));
  oids.push(m_store->oids().oid(Value::Index<UInt128>{}));
  oids.push(m_store->oids().oid(Value::Index<UInt64>{}));
  unsigned j = 4;
  for (unsigned i = 0; i < n; i++) {
    if (!(m_fields[i]->props & ZtMFieldProp::Update)) continue;
    auto type = m_xFields[i].type;
    query << ", \"" << m_xFields[i].id_
      << "\"=$" << j << "::" << m_store->oids().name(type);
    oids.push(m_store->oids().oid(type));
    j++;
  }
  query << " WHERE ";
  const auto &xKeyFields = m_xKeyFields[0];
  n = xKeyFields.length();
  for (unsigned i = 0; i < n; i++) {
    auto type = xKeyFields[i].type;
    if (i) query << " AND ";
    query << '"' << xKeyFields[i].id_
      << "\"=$" << j << "::" << m_store->oids().name(type);
    oids.push(m_store->oids().oid(type));
    j++;
  }
  return m_store->sendPrepare(id, query, oids);
}
void StoreTbl::prepUpdate_rcvd(PGresult *res)
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) prepDelete();
}

void StoreTbl::prepDelete()
{
  m_openState.phase(OpenState::PrepDelete);
  open_enqueue();
}
int StoreTbl::prepDelete_send()
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  ZtString id(m_id_.length() + 8);
  id << m_id_ << "_delete";
  ZtString query;
  const auto &xKeyFields = m_xKeyFields[0];
  unsigned n = xKeyFields.length();
  ZtArray<Oid> oids(n);
  query << "DELETE FROM \"" << m_id_ << "\" WHERE ";
  for (unsigned i = 0; i < n; i++) {
    auto type = xKeyFields[i].type;
    if (i) query << " AND ";
    query << '"' << xKeyFields[i].id_
      << "\"=$" << (i + 1) << "::" << m_store->oids().name(type);
    oids.push(m_store->oids().oid(type));
  }
  return m_store->sendPrepare(id, query, oids);
}
void StoreTbl::prepDelete_rcvd(PGresult *res)
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) prepMRD();
}

void StoreTbl::prepMRD()
{
  m_openState.phase(OpenState::PrepMRD);
  open_enqueue();
}
int StoreTbl::prepMRD_send()
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  ZtString id(m_id_.length() + 8);
  id << m_id_ << "_mrd";
  ZtString query;
  const auto &xKeyFields = m_xKeyFields[0];
  unsigned n = xKeyFields.length();
  ZtArray<Oid> oids(n);
  query <<
    "UPDATE \"zdb.mrd\" SET \"_un\"=$1::uint8, \"_sn\"=$2::uint16 "
      "WHERE \"tbl\"='" << m_id_ << '\'';
  return m_store->sendPrepare(id, query, oids);
}
void StoreTbl::prepMRD_rcvd(PGresult *res)
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) count();
}

void StoreTbl::count()
{
  m_openState.phase(OpenState::Count);
  open_enqueue();
}
int StoreTbl::count_send()
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  ZtString query;
  query << "SELECT CAST(COUNT(*) AS uint8) FROM \"" << m_id_ << '"';
  return m_store->sendQuery<SendState::Sync, false>(query, Tuple{});
}
void StoreTbl::count_rcvd(PGresult *res)
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) { maxUN(); return; }

  if (PQntuples(res) != 1 ||
      PQnfields(res) != 1 ||
      PQgetlength(res, 0, 0) != sizeof(UInt64)) {
    // invalid query result
    open_failed(ZeMEVENT(Fatal, ([id = m_id_](auto &s, const auto &) {
      s << "inconsistent count() result for table " << id;
    })));
    return;
  }

  m_count = uint64_t(reinterpret_cast<UInt64 *>(PQgetvalue(res, 0, 0))->v);
}

void StoreTbl::maxUN()
{
  m_openState.phase(OpenState::MaxUN);
  open_enqueue();
}
int StoreTbl::maxUN_send()
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  ZtString query;
  query << "SELECT \"_un\", \"_sn\" FROM \"" << m_id_
    << "\" WHERE \"_un\"=(SELECT MAX(\"_un\") FROM \"" << m_id_ << "\")";
  return m_store->sendQuery<SendState::Sync, false>(query, Tuple{});
}
void StoreTbl::maxUN_rcvd(PGresult *res)
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) { ensureMRD(); return; }

  unsigned n = PQntuples(res);
  if (n && (PQnfields(res) != 2)) goto inconsistent;
  for (unsigned i = 0; i < n; i++) {
    if (PQgetlength(res, i, 0) != sizeof(UInt64) ||
	PQgetlength(res, i, 1) != sizeof(UInt128)) goto inconsistent;
    auto un = uint64_t(
      reinterpret_cast<const UInt64 *>(PQgetvalue(res, i, 0))->v);
    auto sn = uint128_t(
      reinterpret_cast<const UInt128 *>(PQgetvalue(res, i, 1))->v);
    ZeLOG(Debug, ([un, sn](auto &s) {
      s << "un=" << un << " sn=" << ZuBoxed(sn);
    }));
    if (m_maxUN == ZdbNullUN() || un > m_maxUN) m_maxUN = un;
    if (m_maxSN == ZdbNullSN() || sn > m_maxSN) m_maxSN = sn;
  }
  return;

inconsistent:
  open_failed(ZeMEVENT(Fatal, ([id = m_id_](auto &s, const auto &) {
    s << "inconsistent MAX(_un) result for table " << id;
  })));
}

void StoreTbl::ensureMRD()
{
  m_openState.phase(OpenState::EnsureMRD);
  open_enqueue();
}
int StoreTbl::ensureMRD_send()
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  ZtString query;
  query <<
    "INSERT INTO \"zdb.mrd\" (\"tbl\", \"_un\", \"_sn\") "
      "VALUES ('" << m_id_ << "', 0, 0) "
      "ON CONFLICT (\"tbl\") DO NOTHING";
  return m_store->sendQuery<SendState::Sync, false>(query, Tuple{});
}
void StoreTbl::ensureMRD_rcvd(PGresult *res)
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) mrd();
}

void StoreTbl::mrd()
{
  m_openState.phase(OpenState::MRD);
  open_enqueue();
}
int StoreTbl::mrd_send()
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  Tuple params = { Value{String(m_id_)} };
  return m_store->sendQuery<SendState::Sync, false>(
    "SELECT \"_un\", \"_sn\" FROM \"zdb.mrd\" WHERE \"tbl\"=$1::text", params);
}
void StoreTbl::mrd_rcvd(PGresult *res)
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) { maxima(); return; }

  unsigned n = PQntuples(res);
  if (n && (PQnfields(res) != 2)) goto inconsistent;
  for (unsigned i = 0; i < n; i++) {
    if (PQgetlength(res, i, 0) != sizeof(UInt64) ||
	PQgetlength(res, i, 1) != sizeof(UInt128)) goto inconsistent;
    auto un = uint64_t(
      reinterpret_cast<const UInt64 *>(PQgetvalue(res, i, 0))->v);
    auto sn = uint128_t(
      reinterpret_cast<const UInt128 *>(PQgetvalue(res, i, 1))->v);
    ZeLOG(Debug, ([un, sn](auto &s) {
      s << "un=" << un << " sn=" << ZuBoxed(sn);
    }));
    if (un > m_maxUN) m_maxUN = un;
    if (sn > m_maxSN) m_maxSN = sn;
  }
  return;

inconsistent:
  open_failed(ZeMEVENT(Fatal, ([id = m_id_](auto &s, const auto &) {
    s << "inconsistent SELECT FROM zdb.mrd result for table " << id;
  })));
}

void StoreTbl::maxima()
{
  m_openState.phase(OpenState::Maxima);
  open_enqueue();
}
int StoreTbl::maxima_send()
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  unsigned keyID = m_openState.keyID();
skip:
  const auto &keyFields = m_keyFields[keyID];
  const auto &xKeyFields = m_xKeyFields[keyID];
  unsigned n = keyFields.length();

  // check if series key
  bool isSeries = false;
  for (unsigned i = 0; i < n; i++)
    if (keyFields[i]->props & ZtMFieldProp::Series) {
      isSeries = true;
      break;
    }

  // skip querying non-series keys
  if (!isSeries) {
    m_openState.incKey();
    keyID = m_openState.keyID();
    if (keyID >= m_keyFields.length()) {
      // all maxima queried
      opened();
      return SendState::Unsent;
    }
    goto skip;
  }

  ZtString query;
  query << "SELECT DISTINCT ON (";
  bool first = true;
  for (unsigned i = 0; i < n; i++) {
    if (!(keyFields[i]->props & ZtMFieldProp::Series)) {
      if (!first) { query << ", "; first = false; }
      query << '"' << xKeyFields[i].id_ << '"';
    }
  }
  query << ") ";
  for (unsigned i = 0; i < n; i++) {
    if (i) query << ", ";
    query << '"' << xKeyFields[i].id_ << '"';
  }
  query << " FROM \"" << m_id_ << "\" ORDER BY ";
  for (unsigned i = 0; i < n; i++) {
    if (i) query << ", ";
    query << '"' << xKeyFields[i].id_ << '"';
    if (keyFields[i]->props & ZtMFieldProp::Series)
      query << " DESC";
  }
  return m_store->sendQuery<SendState::Flush, true>(query, Tuple{});
}
void StoreTbl::maxima_rcvd(PGresult *res)
{
  ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) {
    m_openState.incKey();
    if (m_openState.keyID() >= m_keyFields.length())
      opened();
    else
      open_enqueue();
    return;
  }

  unsigned nr = PQntuples(res);
  if (!nr) return;

  unsigned keyID = m_openState.keyID();
  const auto &keyFields = m_keyFields[keyID];
  const auto &xKeyFields = m_xKeyFields[keyID];
  unsigned nc = keyFields.length();

  // tuple is POD, no need to run destructors when going out of scope
  auto tuple = ZmAlloc(Value, nc);

  if (PQnfields(res) != nc) goto inconsistent;
  for (unsigned i = 0; i < nr; i++) {
    for (unsigned j = 0; j < nc; j++)
      if (!ZuSwitch::dispatch<Value::N>(xKeyFields[j].type,
	  [&tuple, res, i, j](auto Type) {
	    return tuple[j].load<Type>(
	      PQgetvalue(res, i, j), PQgetlength(res, i, j));
	  }))
	goto inconsistent;
    auto buf =
      maxima_save(ZuArray<const Value>(&tuple[0], nc), keyID).constRef();
    // res can go out of scope now - everything is saved in buf
    ZeLOG(Debug, ([](auto &s) { s << "calling maxFn"; }));
    MaxData maxData{
      .keyID = keyID,
      .buf = ZuMv(buf)
    };
    m_store->zdbRun([maxFn = m_maxFn, maxData = ZuMv(maxData)]() mutable {
      maxFn(ZuMv(maxData));
    });
  }
  return;

inconsistent:
  open_failed(ZeMEVENT(Fatal, ([id = m_id_](auto &s, const auto &) {
    s << "inconsistent maxima() result for table " << id;
  })));
}
ZmRef<AnyBuf> StoreTbl::maxima_save(
  ZuArray<const Value> tuple, unsigned keyID)
{
  ZmAssert(m_maxBuf->refCount() == 1);
  IOBuilder fbb;
  fbb.buf(m_maxBuf);
  fbb.Finish(saveTuple(fbb, m_xKeyFields[keyID], tuple));
  return fbb.buf();
}

void StoreTbl::close(CloseFn fn)
{
  m_store->pqRun([this, fn = ZuMv(fn)]() mutable {
    m_openState.phase(OpenState::Closed);
    fn();
  });
}

void StoreTbl::warmup() { /* LATER */ }

void StoreTbl::find(unsigned keyID, ZmRef<const AnyBuf> buf, RowFn rowFn)
{
  ZmAssert(keyID < m_keyFields.length());

  using namespace Work;

  m_store->pqRun(
    [this, keyID, buf = ZuMv(buf), rowFn = ZuMv(rowFn)]() mutable {
      if (m_store->stopping()) {
	store()->zdbRun([id = m_id, rowFn = ZuMv(rowFn)]() mutable {
	  rowFn(RowResult{ZeMEVENT(Error, ([id](auto &s, const auto &) {
	    s << "find(" << id << ") failed - DB shutdown in progress";
	  }))});
	});
	return;
      }
      m_store->enqueue(
	TblTask{this, Query{Find{keyID, ZuMv(buf), ZuMv(rowFn)}}});
    });
}
int StoreTbl::find_send(Work::Find &find)
{
  const auto &keyFields = m_keyFields[find.keyID];
  auto nParams = keyFields.length();
  auto idSize = m_id_.length() + 24;
  // reduce heap allocations with ZmAlloc()
  auto params_ = ZmAlloc(Value, nParams);
  auto id_ = ZmAlloc(char, idSize);
  Tuple params(&params_[0], 0, nParams, false);
  ZtString id(&id_[0], 0, idSize, false);
  params = loadTuple(
    params,
    m_keyFields[find.keyID],
    m_xKeyFields[find.keyID],
    Zfb::GetAnyRoot(find.buf->data()));
  id << m_id_ << "_find_" << find.keyID;
  return m_store->sendPrepared<SendState::Flush, true>(id, params);
}
void StoreTbl::find_rcvd(Work::Find &find, PGresult *res)
{
  if (!find.rowFn) return; // find failed

  find_rcvd_<false>(find.rowFn, find.found, res);
}
template <bool Recovery>
void StoreTbl::find_rcvd_(RowFn &rowFn, bool &found, PGresult *res)
{
  if (!res) {
    if (!found)
      m_store->zdbRun([rowFn = ZuMv(rowFn)]() mutable {
	rowFn(RowResult{});
      });
    return;
  }

  unsigned nr = PQntuples(res);
  if (!nr) return;

  unsigned nc = m_xFields.length() + 3;

  // tuple is POD, no need to run destructors when going out of scope
  auto tuple = ZmAlloc(Value, nc);

  if (PQnfields(res) != nc) goto inconsistent;
  for (unsigned i = 0; i < nr; i++) {
    for (unsigned j = 0; j < nc; j++) {
      unsigned type;
      switch (int(j)) {
	case 0: type = Value::Index<UInt64>{}; break;	// UN
	case 1: type = Value::Index<UInt128>{}; break;	// SN
	case 2: type = Value::Index<Int64>{}; break;	// VN
	default: type = m_xFields[j - 3].type; break;
      }
      if (!ZuSwitch::dispatch<Value::N>(type,
	  [&tuple, res, i, j](auto Type) {
	    return tuple[j].load<Type>(
	      PQgetvalue(res, i, j), PQgetlength(res, i, j));
	  }))
	goto inconsistent;
    }
    auto buf =
      find_save<Recovery>(ZuArray<const Value>(&tuple[0], nc)).constRef();
    if (found) {
      ZeLOG(Error, ([id = m_id_](auto &s) {
	s << "multiple records found with same key in table " << id;
      }));
      return;
    }
    // res can go out of scope now - everything is saved in buf
    RowResult result{RowData{.buf = ZuMv(buf)}};
    m_store->zdbRun([rowFn, result = ZuMv(result)]() mutable {
      rowFn(ZuMv(result));
    });
    found = true;
  }
  return;

inconsistent:
  if constexpr (!Recovery)
    find_failed_(ZuMv(rowFn),
      ZeMEVENT(Error, ([id = m_id_](auto &s, const auto &) {
	s << "inconsistent find() result for table " << id;
      })));
  else
    find_failed_(ZuMv(rowFn),
      ZeMEVENT(Error, ([id = m_id_](auto &s, const auto &) {
	s << "inconsistent recover() result for table " << id;
      })));
}
template <bool Recovery>
ZmRef<AnyBuf> StoreTbl::find_save(ZuArray<const Value> tuple)
{
  IOBuilder fbb;
  auto data = Zfb::Save::nest(fbb, [this, tuple](Zfb::Builder &fbb) mutable {
    tuple.offset(3); // skip un, sn, vn
    return saveTuple(fbb, m_xFields, tuple);
  });
  {
    auto id = Zfb::Save::id(this->id());
    UN un = tuple[0].p<UInt64>().v;
    SN sn = tuple[1].p<UInt128>().v;
    VN vn = tuple[2].p<Int64>().v;
    auto sn_ = Zfb::Save::uint128(sn);
    auto msg = fbs::CreateMsg(
      fbb, Recovery ? fbs::Body::Recovery : fbs::Body::Replication,
      fbs::CreateRecord(fbb, &id, un, &sn_, vn, data).Union()
    );
    fbb.Finish(msg);
  }
  return saveHdr(fbb);
}
void StoreTbl::find_failed(Work::Find &find, ZeMEvent e)
{
  find_failed_(ZuMv(find.rowFn), ZuMv(e));
}
void StoreTbl::find_failed_(RowFn rowFn, ZeMEvent e)
{
  RowResult result{ZuMv(e)};
  m_store->zdbRun([
    rowFn = ZuMv(rowFn),
    result = ZuMv(result)
  ]() mutable {
    rowFn(ZuMv(result));
  });
}

void StoreTbl::recover(UN un, RowFn rowFn)
{
  using namespace Work;

  m_store->pqRun([this, un, rowFn = ZuMv(rowFn)]() mutable {
    if (m_store->stopping()) {
      store()->zdbRun([id = m_id, rowFn = ZuMv(rowFn)]() mutable {
	rowFn(RowResult{ZeMEVENT(Error, ([id](auto &s, const auto &) {
	  s << "recover(" << id << ") failed - DB shutdown in progress";
	}))});
      });
      return;
    }
    m_store->enqueue(TblTask{this, Query{Recover{un, ZuMv(rowFn)}}});
  });
}
int StoreTbl::recover_send(Work::Recover &recover)
{
  Tuple params = { Value{UInt64{recover.un}} };
  ZtString id(m_id_.length() + 8);
  id << m_id_ << "_recover";
  return m_store->sendPrepared<SendState::Flush, true>(id, params);
}
void StoreTbl::recover_rcvd(Work::Recover &recover, PGresult *res)
{
  if (!recover.rowFn) return; // recover failed

  find_rcvd_<true>(recover.rowFn, recover.found, res);
}
void StoreTbl::recover_failed(Work::Recover &recover, ZeMEvent e)
{
  find_failed_(ZuMv(recover.rowFn), ZuMv(e));
}

void StoreTbl::write(ZmRef<const AnyBuf> buf, CommitFn commitFn)
{
  ZeLOG(Debug, ([buf = buf.ptr()](auto &s) {
    s << "buf=" << ZuBoxPtr(buf).hex();
  }));

  using namespace Work;

  m_store->pqRun([this, buf = ZuMv(buf), commitFn = ZuMv(commitFn)]() mutable {
    if (m_store->stopping()) {
      store()->zdbRun([
	id = m_id,
	buf = ZuMv(buf),
	commitFn = ZuMv(commitFn)
      ]() mutable {
	commitFn(ZuMv(buf), CommitResult{
	  ZeMEVENT(Error, ([id](auto &s, const auto &) {
	    s << "write(" << id << ") failed - DB shutdown in progress";
	  }))});
      });
      return;
    }
    m_store->enqueue(TblTask{this, Query{Write{ZuMv(buf), ZuMv(commitFn)}}});
  });
}
int StoreTbl::write_send(Work::Write &write)
{
  ZeLOG(Debug, ([buf = write.buf.ptr()](auto &s) {
    s << "buf=" << ZuBoxPtr(buf).hex();
  }));

  auto record = record_(msg_(write.buf->hdr()));
  auto un =record->un(); 
  auto sn = Zfb::Load::uint128(record->sn());

  if (!write.mrd) {
    if (m_maxUN != ZdbNullUN() && un <= m_maxUN)
      return SendState::Unsent;
    m_maxUN = un;
    m_maxSN = sn;
  }

  ZeLOG(Debug, ([un, sn, vn = record->vn()](auto &s) {
    s << "UN=" << un << " SN=" << ZuBoxed(sn) << " VN=" << vn;
  }));

  auto data = Zfb::Load::bytes(record->data());
  auto fbo = Zfb::GetAnyRoot(data.data());
  auto nParams = 3 + m_fields.length();
  auto idSize = m_id_.length() + 8;
  // reduce heap allocations with ZmAlloc()
  auto params_ = ZmAlloc(Value, nParams);
  auto id_ = ZmAlloc(char, idSize);
  Tuple params(&params_[0], 0, nParams, false);
  ZtString id(&id_[0], 0, idSize, false);
  if (!record->vn()) {
    id << m_id_ << "_insert";
    new (params.push()) Value{UInt64{un}};
    new (params.push()) Value{UInt128{sn}};
    new (params.push()) Value{UInt64{record->vn()}};
    params = loadTuple(params, m_fields, m_xFields, fbo);
  } else if (record->vn() > 0) {
    id << m_id_ << "_update";
    new (params.push()) Value{UInt64{un}};
    new (params.push()) Value{UInt128{sn}};
    new (params.push()) Value{UInt64{record->vn()}};
    params = loadUpdTuple(params, m_fields, m_xFields, fbo);
    params = loadTuple(params, m_keyFields[0], m_xKeyFields[0], fbo);
  } else if (!write.mrd) {
    id << m_id_ << "_delete";
    params = loadTuple(params, m_keyFields[0], m_xKeyFields[0], fbo);
  } else {
    id << m_id_ << "_mrd";
    new (params.push()) Value{UInt64{un}};
    new (params.push()) Value{UInt128{sn}};
  }
  return m_store->sendPrepared<SendState::Sync, false>(id, params);
}
void StoreTbl::write_rcvd(Work::Write &write, PGresult *res)
{
  ZeLOG(Debug, ([buf = write.buf.ptr(), res](auto &s) {
    s << "buf=" << ZuBoxPtr(buf).hex() << " res=" << ZuBoxPtr(res).hex();
  }));

  if (res) return;

  if (!write.buf) return; // write failed

  auto record = record_(msg_(write.buf->hdr()));
  if (record->vn() < 0 && !write.mrd) {
    ZeLOG(Debug, ([vn = record->vn(), mrd = write.mrd](auto &s) {
      s << "VN=" << vn << " mrd=" << mrd;
    }));
    using namespace Work;
    m_store->enqueue(TblTask{this, Query{Write{
      ZuMv(write.buf), ZuMv(write.commitFn), true}}});
  } else {
    m_store->zdbRun([
      buf = ZuMv(write.buf),
      commitFn = ZuMv(write.commitFn)
    ]() mutable {
      commitFn(ZuMv(buf), CommitResult{});
    });
  }
}
void StoreTbl::write_failed(Work::Write &write, ZeMEvent e)
{
  ZeLOG(Debug, ([e = ZuMv(e)](auto &s) { s << e; }));

  CommitResult result{ZuMv(e)};
  m_store->zdbRun([
    buf = ZuMv(write.buf),
    commitFn = ZuMv(write.commitFn),
    result = ZuMv(result)
  ]() mutable {
    commitFn(ZuMv(buf), ZuMv(result));
  });
}

} // ZdbPQ

Zdb_::Store *ZdbStore()
{
  return new ZdbPQ::Store{};
}
