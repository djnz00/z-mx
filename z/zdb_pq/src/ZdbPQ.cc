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

#include <zlib/ZmDemangle.hh>

#include <zlib/ZtCase.hh>

#include <zlib/ZeLog.hh>

namespace ZdbPQ {

OIDs::OIDs()
{
  static const char *names[Value::N - 1] = {
    "text",	// String
    "bytea",	// Bytes
    "bool",	// Bool
    "int1",	// Int8
    "uint1",	// UInt8
    "int2",	// Int16
    "uint2",	// UInt16
    "int4",	// Int32
    "uint4",	// UInt32
    "int8",	// Int64
    "uint8",	// UInt64
    "float8",	// Float
    "zdecimal",	// Fixed
    "zdecimal",	// Decimal
    "ztime",	// Time
    "ztime",	// DateTime
    "int16",	// Int128
    "uint16",	// UInt128
    "zbitmap",	// Bitmap
    "inet",	// IP
    "text",	// ID

    "_text",	// StringVec
    "_bytea",	// BytesVec
    "_int1",	// Int8Vec
    "_uint1",	// UInt8Vec
    "_int2",	// Int16Vec
    "_uint2",	// UInt16Vec
    "_int4",	// Int32Vec
    "_uint4",	// UInt32Vec
    "_int8",	// Int64Vec
    "_uint8",	// UInt64Vec
    "_int16",	// Int128Vec
    "_uint16",	// UInt128Vec
    "_float8",	// FloatVec
    "_zdecimal",// FixedVec
    "_zdecimal",// DecimalVec
    "_ztime",	// TimeVec
    "_ztime"	// DateTimeVec
  };

  m_names = names;
}

InitResult Store::init(ZvCf *cf, ZiMultiplex *mx, FailFn failFn)
{
  m_cf = cf;
  m_mx = mx;
  m_failFn = ZuMv(failFn);

  bool replicated;

  try {
    const ZtString &tid = cf->get<true>("thread");
    auto sid = m_mx->sid(tid);
    if (!sid ||
	sid > m_mx->params().nThreads() ||
	sid == m_mx->rxThread() ||
	sid == m_mx->txThread())
      return {ZeVEVENT(Fatal, ([tid = ZtString{tid}](auto &s, const auto &) {
	s << "Store::init() failed: invalid thread configuration \""
	  << tid << '"';
      }))};
    m_sid = sid;
    replicated = cf->getBool("replicated", false);
  } catch (const ZvError &e_) {
    ZtString e;
    e << e_;
    return {ZeVEVENT(Fatal, ([e = ZuMv(e)](auto &s, const auto &) {
      s << "Store::init() failed: invalid configuration: " << e;
    }))};
  }

  if (!m_storeTbls) m_storeTbls = new StoreTbls{};

  return {InitData{.replicated = replicated}};
}

void Store::final()
{
  m_failFn = FailFn{};
  m_storeTbls->clean();
  m_storeTbls = nullptr;
}

void Store::start(StartFn fn)
{
  // ZeLOG(Debug, ([](auto &s) { }));

  m_mx->push(m_sid, [this, fn = ZuMv(fn)]() mutable {
    m_stopping = false;
    m_startState.reset();
    m_startFn = ZuMv(fn);
    m_stopFn = StopFn{};
    if (!start_()) {
      start_failed(false, ZeVEVENT(Fatal, "PostgreSQL start() failed"));
      return;
    }
    getOIDs();
    m_mx->wakeFn(m_sid, ZmFn<>{this, [](Store *store) { store->wake(); }});
    run_();
  });
}

static ZtString connError(PGconn *conn)
{
  ZtString error = PQerrorMessage(conn);
  error.chomp();
  return error;
}

static void notice_(void *this_, const PGresult *res) {
  static_cast<Store *>(this_)->notice(res);
}

void Store::notice(const PGresult *res) {
  ZtString msg = PQresultErrorMessage(res);

  msg.chomp();
  ZtREGEX("^NOTICE:\s+").s(msg, "");

  if (PQstatus(m_conn) != CONNECTION_OK) {
    auto e = connError(m_conn);
    auto event = ZeVEVENT(Fatal, ([msg = ZuMv(msg), e](auto &s, const auto &) {
      s << msg << " (" << e << ')';
    }));
    m_failFn(ZuMv(event));
  }

  ZeLOG(Info, ([msg = ZuMv(msg)](auto &s) { s << msg; }));
}

bool Store::start_()
{
  // ZeLOG(Debug, ([](auto &s) { }));

  const auto &connection = m_cf->get<true>("connection");

  m_conn = PQconnectdb(connection);

  if (!m_conn || PQstatus(m_conn) != CONNECTION_OK) {
    ZeLOG(Fatal, ([e = connError(m_conn)](auto &s) {
      s << "PQconnectdb() failed: " << e;
    }));
    return false;
  }

  PQsetNoticeReceiver(m_conn, notice_, this);

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

  m_syncSRM = false;

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

  /* ZeLOG(Debug, ([this](auto &s) {
    s << "epoll_ctl(EPOLL_CTL_ADD) connFD=" << m_connFD;
  })); */

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
  // ZeLOG(Debug, ([](auto &s) { }));

  m_stopFn = ZuMv(fn);
  m_stopping = true; // inhibits further application requests

  run([this]() mutable { enqueue(Work::Stop{}); });
}

void Store::stop_()	// called after dequeuing Stop
{
  // ZeLOG(Debug, ([](auto &s) { }));

  if (!m_sent.count_()) { stop_1(); return; }
}

void Store::stop_1()
{
  // ZeLOG(Debug, ([](auto &s) { s << "pushing stop_2()"; }));

  m_mx->wakeFn(m_sid, ZmFn<>{});
  m_mx->push(m_sid, [this]() mutable {
    stop_2();
    StopFn stopFn = ZuMv(m_stopFn);
    m_stopFn = StopFn{};
    stopFn(StopResult{});
  });
  wake_();
}

void Store::stop_2()
{
  // ZeLOG(Debug, ([](auto &s) { }));

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
  // ZeLOG(Debug, ([](auto &s) { s << "pushing run_()"; }));

  m_mx->push(m_sid, [this]{ run_(); });
  wake_();
}

void Store::wake_()
{
  // ZeLOG(Debug, ([](auto &s) { }));

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

static bool isSync(Work::Queue::Node *work)
{
  if (!work) return false;
  const auto &task = work->data();
  return task.is<Work::TblQuery>() ? task.p<Work::TblQuery>().sync : false;
}

static bool isSRM(Work::Queue::Node *work)
{
  if (!work) return false;
  const auto &task = work->data();
  return task.is<Work::TblQuery>() ? task.p<Work::TblQuery>().srm : false;
}

void Store::run_()
{
  // ZeLOG(Debug, ([](auto &s) { }));

  // "prime the pump" to ensure that read- and write-readiness is
  // correctly signalled via epoll / WFMO
  send();
  recv();

  for (;;) {

#ifndef _WIN32

    epoll_event ev[8];

    // ZeLOG(Debug, ([](auto &s) { s << "epoll_wait()..."; }));

again:
    int r = epoll_wait(m_epollFD, ev, 8, -1); // max events is 8

    // ZeLOG(Debug, ([r](auto &s) { s << "epoll_wait(): " << r; }));

    if (r < 0) {
      auto e = errno;
      if (e == EINTR || e == EAGAIN) goto again;
      ZeLOG(Fatal, ([e](auto &s) {
	s << "epoll_wait() failed: " << e;
      }));
      return;
    }
    for (unsigned i = 0; i < unsigned(r); i++) {
      uint32_t events = ev[i].events;
      auto v = ev[i].data.u64; // ID

      /* ZeLOG(Debug, ([events, v](auto &s) {
	s << "epoll_wait() events=" << events << " v=" << v
	  << " EPOLLIN=" << ZuBoxed(EPOLLIN).hex()
	  << " EPOLLOUT=" << ZuBoxed(EPOLLOUT).hex();
      })); */

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
      if (events & EPOLLOUT)
	send();
      if (events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR))
	recv();
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
      if ((events.lNetworkEvents & (FD_WRITE|FD_CLOSE)) == FD_WRITE)
	send();
      if (events.lNetworkEvents & (FD_READ|FD_OOB|FD_CLOSE))
	recv();
    }

#endif

  }
}

// simulate connection failure, for testing purposes only
void Store::disconnect()
{
#ifndef _WIN32
  if (m_connFD >= 0) { ::close(m_connFD); m_connFD = -1; }
#else
  if (m_connFD != (HANDLE)-1) {
    CloseHandle(m_connFD);
    m_connFD = -1;
  }
#endif
}

void Store::recv()
{
  // ZeLOG(Debug, ([](auto &s) { }));

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
	    case PGRES_PIPELINE_SYNC: // pipeline sync
	      if (m_syncSRM) { m_syncSRM = false; setSRM(); }
	      break;
	    case PGRES_NONFATAL_ERROR: // notice / warning
	      failed(pending, ZeVEVENT(Error,
		  ([e = connError(m_conn)](auto &s, const auto &) {
		    s << "PQgetResult() query: " << e;
		  })));
	      break;
	    case PGRES_FATAL_ERROR: // query failed
	      failed(pending, ZeVEVENT(Fatal,
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
      if (!res) { // PQgetResult() returned nullptr, i.e. query completed
	if (auto pending = m_sent.headNode()) {
	  rcvd(pending, nullptr);
	  bool syncing = isSync(pending);
	  m_sent.shift();
	  stop = stopping() && !m_queue.count_() && !m_sent.count_();
	  if (!stop && isSRM(m_sent.headNode())) {
	    if (syncing)
	      m_syncSRM = true;
	    else
	      setSRM();
	  }
	}
      }
    }
  } while (consumed);

  if (stop) stop_1();
}

void Store::rcvd(Work::Queue::Node *work, PGresult *res)
{
  /* ZeLOG(Debug, ([res, n = (res ? int(PQntuples(res)) : 0)](auto &s) {
    s << "res=" << ZuBoxPtr(res).hex() << " n=" << n;
  })); */

  using namespace Work;

  switch (work->data().type()) {
    case Task::Index<Start>{}:
      start_rcvd(res);
      break;
    case Task::Index<TblQuery>{}: {
      auto &tblQuery = work->data().p<TblQuery>();
      switch (tblQuery.query.type()) {
	case Query::Index<Open>{}:
	  tblQuery.tbl->open_rcvd(res);
	  break;
	case Query::Index<Count>{}:
	  tblQuery.tbl->count_rcvd(tblQuery.query.p<Count>(), res);
	  break;
	case Query::Index<Select>{}:
	  tblQuery.tbl->select_rcvd(tblQuery.query.p<Select>(), res);
	  break;
	case Query::Index<Find>{}:
	  tblQuery.tbl->find_rcvd(tblQuery.query.p<Find>(), res);
	  break;
	case Query::Index<Recover>{}:
	  tblQuery.tbl->recover_rcvd(tblQuery.query.p<Recover>(), res);
	  break;
	case Query::Index<Write>{}:
	  tblQuery.tbl->write_rcvd(tblQuery.query.p<Write>(), res);
	  break;
      }
    } break;
  }
}

void Store::failed(Work::Queue::Node *work, ZeVEvent e)
{
  // ZeLOG(Debug, ([](auto &s) { }));

  using namespace Work;

  switch (work->data().type()) {
    case Task::Index<Start>{}:
      start_failed(true, ZuMv(e));
      break;
    case Task::Index<TblQuery>{}: {
      auto &tblQuery = work->data().p<TblQuery>();
      switch (tblQuery.query.type()) {
	case Query::Index<Open>{}:
	  tblQuery.tbl->open_failed(ZuMv(e));
	  break;
	case Query::Index<Count>{}:
	  tblQuery.tbl->count_failed(tblQuery.query.p<Count>(), ZuMv(e));
	  break;
	case Query::Index<Select>{}:
	  tblQuery.tbl->select_failed(tblQuery.query.p<Select>(), ZuMv(e));
	  break;
	case Query::Index<Find>{}:
	  tblQuery.tbl->find_failed(tblQuery.query.p<Find>(), ZuMv(e));
	  break;
	case Query::Index<Recover>{}:
	  tblQuery.tbl->recover_failed(tblQuery.query.p<Recover>(), ZuMv(e));
	  break;
	case Query::Index<Write>{}:
	  tblQuery.tbl->write_failed(tblQuery.query.p<Write>(), ZuMv(e));
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
  // ZeLOG(Debug, ([](auto &s) { }));

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
      case Task::Index<TblQuery>{}: {
	auto &tblQuery = work->data().p<TblQuery>();
	switch (tblQuery.query.type()) {
	  case Query::Index<Open>{}:
	    sendState = tblQuery.tbl->open_send();
	    break;
	  case Query::Index<Count>{}:
	    sendState = tblQuery.tbl->count_send(tblQuery.query.p<Count>());
	    break;
	  case Query::Index<Select>{}:
	    sendState = tblQuery.tbl->select_send(tblQuery.query.p<Select>());
	    break;
	  case Query::Index<Find>{}:
	    sendState = tblQuery.tbl->find_send(tblQuery.query.p<Find>());
	    break;
	  case Query::Index<Recover>{}:
	    sendState = tblQuery.tbl->recover_send(tblQuery.query.p<Recover>());
	    break;
	  case Query::Index<Write>{}:
	    sendState = tblQuery.tbl->write_send(tblQuery.query.p<Write>());
	    break;
	}
      } break;
    }
    if (sendState != SendState::Unsent) {
      if (sendState != SendState::Again) {
	if (!m_sent.count_() && isSRM(work)) setSRM();
	m_sent.pushNode(ZuMv(work).release());
      } else
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

void Store::start_failed(bool running, ZeVEvent e)
{
  // ZeLOG(Debug, ([](auto &s) { }));

  m_startState.phase(StartState::Started);
  m_startState.setFailed();

  if (running)
    stop_1();
  else
    stop_2();

  auto startFn = ZuMv(m_startFn);

  m_startFn = StartFn{};

  startFn(StartResult{ZuMv(e)});
}

void Store::started()
{
  // ZeLOG(Debug, ([](auto &s) { }));

  m_startState.phase(StartState::Started);

  auto startFn = ZuMv(m_startFn);

  m_startFn = StartFn{};

  startFn(StartResult{});
}

void Store::getOIDs()
{
  m_startState.phase(StartState::GetOIDs);
  m_oids.init(Value::Index<String>{}, 25);	// TEXTOID
  start_enqueue();
}
int Store::getOIDs_send()
{
  // ZeLOG(Debug, ([v = m_startState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  unsigned type = m_startState.type() + 1;
  // skip re-querying previously resolved OIDs
skip:
  auto name = m_oids.name(type);
  if (!name) {
    auto e = ZeVEVENT(Fatal,
      ([type](auto &s, const auto &) {
	s << "OID name for type index " << type
	  << " is null - check static names[] array in OIDs constructor";
      }));
    start_failed(true, ZuMv(e));
    return SendState::Unsent;
  }
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
  return sendQuery<SendState::Flush>(
    "SELECT oid FROM pg_type WHERE typname = $1::text", params);
}
void Store::getOIDs_rcvd(PGresult *res)
{
  unsigned type = m_startState.type() + 1;

  /* ZeLOG(Debug, ([type](auto &s) {
    s << "type=" << type << " Value::N=" << Value::N;
  })); */

  if (!res) {
    if (m_startState.failed()) {
      // OID resolution failed
      auto e = ZeVEVENT(Fatal,
	([type = ZtString{m_oids.name(type)}](auto &s, const auto &) {
	  s << "failed to resolve OID for \"" << type << '"';
	}));
      start_failed(true, ZuMv(e));
    } else if (type + 1 >= Value::N) {
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

  /* ZeLOG(Debug, ([type, name = ZtString{m_oids.name(type)}, oid](auto &s) {
    s << "type=" << type << " name=" << name << " oid=" << oid;
  })); */

  m_oids.init(type, oid);
}

void Store::mkSchema()
{
  m_startState.phase(StartState::MkSchema);
  start_enqueue();
}
int Store::mkSchema_send()
{
  // ZeLOG(Debug, ([v = m_startState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  return sendQuery<SendState::Sync>(
    "CREATE SCHEMA IF NOT EXISTS \"zdb\"", Tuple{});
}
void Store::mkSchema_rcvd(PGresult *res)
{
  // ZeLOG(Debug, ([v = m_startState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) mkTblMRD();
}

void Store::mkTblMRD()
{
  m_startState.phase(StartState::MkTblMRD);
  start_enqueue();
}
int Store::mkTblMRD_send()
{
  // ZeLOG(Debug, ([v = m_startState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  // the MRD schema is unlikely to evolve, so use IF NOT EXISTS
  return sendQuery<SendState::Sync>(
    "CREATE TABLE IF NOT EXISTS \"zdb.mrd\" ("
      "\"tbl\" text NOT NULL, "
      "\"shard\" uint1 NOT NULL, "
      "\"un\" uint8 NOT NULL, "
      "\"sn\" uint16 NOT NULL, "
      "PRIMARY KEY (\"tbl\", \"shard\"))",
    Tuple{});
}
void Store::mkTblMRD_rcvd(PGresult *res)
{
  // ZeLOG(Debug, ([v = m_startState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) started();
}

void Store::open(
  ZtString id,
  unsigned nShards,
  ZtVFieldArray fields,
  ZtVKeyFieldArray keyFields,
  const reflection::Schema *schema,
  IOBufAllocFn bufAllocFn,
  OpenFn openFn)
{
  // ZeLOG(Debug, ([](auto &s) { }));

  run([
    this, id = ZuMv(id), nShards,
    fields = ZuMv(fields), keyFields = ZuMv(keyFields),
    schema, bufAllocFn = ZuMv(bufAllocFn), openFn = ZuMv(openFn)
  ]() mutable {
    if (stopping()) {
      openFn(OpenResult{ZeVEVENT(Error,
	  ([id = ZuMv(id)](auto &s, const auto &) {
	    s << "open(" << id << ") failed - DB shutdown in progress";
	  }))});
      return;
    }
    auto storeTbl = new StoreTbls::Node{
      this, ZuMv(id), nShards,
      ZuMv(fields), ZuMv(keyFields), schema, ZuMv(bufAllocFn)};
    m_storeTbls->addNode(storeTbl);
    storeTbl->open(ZuMv(openFn));
  });
}

void Store::enqueue(Work::Task task)
{
  // ZeLOG(Debug, ([](auto &s) { }));

  m_queue.push(ZuMv(task));
  wake();
}

// resolve Value union discriminator from flatbuffers reflection data
static XField xField(
  const Zfb::Vector<Zfb::Offset<reflection::Field>> *fbFields_,
  const ZtVField *field,
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
      if (ftype->code == ZtFieldTypeCode::Int8)
	type = Value::Index<Int8>{};
      break;
    case reflection::UByte:
      if (ftype->code == ZtFieldTypeCode::UInt8)
	type = Value::Index<UInt8>{};
      break;
    case reflection::Short:
      if (ftype->code == ZtFieldTypeCode::Int16)
	type = Value::Index<Int16>{};
      break;
    case reflection::UShort:
      if (ftype->code == ZtFieldTypeCode::UInt16)
	type = Value::Index<UInt16>{};
      break;
    case reflection::Int:
      if (ftype->code == ZtFieldTypeCode::Int32)
	type = Value::Index<Int32>{};
      break;
    case reflection::UInt:
      if (ftype->code == ZtFieldTypeCode::UInt32)
	type = Value::Index<UInt32>{};
      break;
    case reflection::Long:
      if (ftype->code == ZtFieldTypeCode::Int64)
	type = Value::Index<Int64>{};
      break;
    case reflection::ULong:
      if (ftype->code == ZtFieldTypeCode::UInt64)
	type = Value::Index<UInt64>{};
      break;
    case reflection::Double:
      if (ftype->code == ZtFieldTypeCode::Float)
	type = Value::Index<Float>{};
      break;
    case reflection::Obj: {
      switch (ftype->code) {
	case ZtFieldTypeCode::Int128:
	  type = Value::Index<Int128>{};
	  break;
	case ZtFieldTypeCode::UInt128:
	  type = Value::Index<UInt128>{};
	  break;
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
	  ZuID typeID = ftype->info.udt()->id;
	  if (typeID == ZuID("Bitmap")) {
	    type = Value::Index<Bitmap>{};
	    break;
	  }
	  if (typeID == ZuID("IP")) {
	    type = Value::Index<IP>{};
	    break;
	  }
	  if (typeID == ZuID("ID")) {
	    type = Value::Index<ID>{};
	    break;
	  }
	}
      }
    } break;
    case reflection::Vector:
      switch (fbField->type()->element()) {
	default: break;
	case reflection::String:
	  if (ftype->code == ZtFieldTypeCode::StringVec)
	    type = Value::Index<StringVec>{};
	  break;
	case reflection::Byte:
	  if (ftype->code == ZtFieldTypeCode::Int8Vec)
	    type = Value::Index<Int8Vec>{};
	  break;
	case reflection::UByte:
	  if (ftype->code == ZtFieldTypeCode::Bytes)
	    type = Value::Index<Bytes>{};
	  else if (ftype->code == ZtFieldTypeCode::UInt8Vec)
	    type = Value::Index<UInt8Vec>{};
	  break;
	case reflection::Short:
	  if (ftype->code == ZtFieldTypeCode::Int16Vec)
	    type = Value::Index<Int16Vec>{};
	  break;
	case reflection::UShort:
	  if (ftype->code == ZtFieldTypeCode::UInt16Vec)
	    type = Value::Index<UInt16Vec>{};
	  break;
	case reflection::Int:
	  if (ftype->code == ZtFieldTypeCode::Int32Vec)
	    type = Value::Index<Int32Vec>{};
	  break;
	case reflection::UInt:
	  if (ftype->code == ZtFieldTypeCode::UInt32Vec)
	    type = Value::Index<UInt32Vec>{};
	  break;
	case reflection::Long:
	  if (ftype->code == ZtFieldTypeCode::Int64Vec)
	    type = Value::Index<Int64Vec>{};
	  break;
	case reflection::ULong:
	  if (ftype->code == ZtFieldTypeCode::UInt64Vec)
	    type = Value::Index<UInt64Vec>{};
	  break;
	case reflection::Double:
	  if (ftype->code == ZtFieldTypeCode::FloatVec)
	    type = Value::Index<FloatVec>{};
	  break;
	case reflection::Obj:
	  switch (ftype->code) {
	    case ZtFieldTypeCode::BytesVec:
	      type = Value::Index<BytesVec>{};
	      break;
	    case ZtFieldTypeCode::Int128Vec:
	      type = Value::Index<Int128Vec>{};
	      break;
	    case ZtFieldTypeCode::UInt128Vec:
	      type = Value::Index<UInt128Vec>{};
	      break;
	    case ZtFieldTypeCode::FixedVec:
	      type = Value::Index<FixedVec>{};
	      break;
	    case ZtFieldTypeCode::DecimalVec:
	      type = Value::Index<DecimalVec>{};
	      break;
	    case ZtFieldTypeCode::TimeVec:
	      type = Value::Index<TimeVec>{};
	      break;
	    case ZtFieldTypeCode::DateTimeVec:
	      type = Value::Index<DateTimeVec>{};
	      break;
	  }
	  break;
      }
      break;
    default:
      break;
  }
  return {id, fbField, type};
}

StoreTbl::StoreTbl(
  Store *store, ZtString id, unsigned nShards,
  ZtVFieldArray fields, ZtVKeyFieldArray keyFields,
  const reflection::Schema *schema, IOBufAllocFn bufAllocFn
) :
  m_store{store}, m_id{ZuMv(id)},
  m_fields{ZuMv(fields)}, m_keyFields{ZuMv(keyFields)},
  m_fieldMap{ZmHashParams(m_fields.length())},
  m_bufAllocFn{ZuMv(bufAllocFn)}
{
  ZtCase::camelSnake(m_id, [this](const ZtString &id) { m_id_ = id; });
  const reflection::Object *rootTbl = schema->root_table();
  const Zfb::Vector<Zfb::Offset<reflection::Field>> *fbFields_ =
    rootTbl->fields();
  unsigned n = m_fields.length();
  m_xFields.size(n);
  {
    unsigned j = 0;
    for (unsigned i = 0; i < n; i++)
      if (m_fields[i]->props & ZtVFieldProp::Mutable()) j++;
    j += m_keyFields[0].length();
    m_updFields.size(j);
    m_xUpdFields.size(j);
  }
  for (unsigned i = 0; i < n; i++)
    ZtCase::camelSnake(m_fields[i]->id,
      [this, fbFields_, i](const ZtString &id) {
	m_xFields.push(xField(fbFields_, m_fields[i], id));
	if (m_fields[i]->props & ZtVFieldProp::Mutable()) {
	  m_updFields.push(m_fields[i]);
	  m_xUpdFields.push(xField(fbFields_, m_fields[i], id));
	}
	m_fieldMap.add(id, i);
      });
  n = m_keyFields.length();
  m_xKeyFields.size(n);
  m_keyGroup.length(n);
  for (unsigned i = 0; i < n; i++) {
    unsigned m = m_keyFields[i].length();
    new (m_xKeyFields.push()) XFields{m};
    m_keyGroup[i] = 0;
    unsigned k = 0; // number of descending fields in key
    for (unsigned j = 0; j < m; j++) {
      if (m_keyFields[i][j]->group & (uint64_t(1)<<i)) m_keyGroup[i] = j + 1;
      if (m_keyFields[i][j]->descend & (uint64_t(1)<<i)) k++;
      ZtCase::camelSnake(m_keyFields[i][j]->id,
	[this, fbFields_, i, j](const ZtString &id) {
	  m_xKeyFields[i].push(xField(fbFields_, m_keyFields[i][j], id));
	  if (!i) {
	    m_updFields.push(m_keyFields[i][j]);
	    m_xUpdFields.push(xField(fbFields_, m_keyFields[i][j], id));
	  }
	});
    }
    if (k > 0 && k < m)
      ZeLOG(Warning, ([id, i](auto &s) {
	s << id << " key " << i << " has mixed ascending/descending fields";
      }));
  }
  m_maxUN.length(nShards);
  for (unsigned i = 0; i < nShards; i++) m_maxUN[i] = ZdbNullUN();
}

StoreTbl::~StoreTbl()
{
}

void StoreTbl::open(OpenFn openFn)
{
  // ZeLOG(Debug, ([](auto &s) { }));

  m_openState.reset();
  m_openFn = ZuMv(openFn);
  mkTable();
}

template <int State>
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
  /* ZeLOG(Debug, ([query = ZtString{query}, n](auto &s) {
    s << '"' << query << "\", n=" << n;
  })); */

  int r = PQsendQueryParams(
    m_conn, query.data(),
    n, paramTypes, paramValues, paramLengths, paramFormats, 1);
  if (r != 1) return SendState::Again;
  return State;
}

void Store::setSRM()
{
  if (PQsetSingleRowMode(m_conn) != 1)
    ZeLOG(Error, ([e = connError(m_conn)](auto &s) {
      s << "PQsetSingleRowMode() failed: " << e;
    }));
}

int Store::sendPrepare(
  const ZtString &id, const ZtString &query, ZuSpan<Oid> oids)
{
  /* ZeLOG(Debug, ([id = ZtString{id}, query = ZtString{query}](auto &s) {
    s << '"' << id << "\", \"" << query << '"';
  })); */

  int r = PQsendPrepare(
    m_conn, id.data(), query.data(), oids.length(), oids.data());
  if (r != 1) return SendState::Again;
  return SendState::Sync;
}

template <int State>
int Store::sendPrepared(const ZtString &id, const Tuple &params)
{
  auto n = params.length();

  /* ZeLOG(Debug, ([id = ZtString{id}, params = params](auto &s) {
    s << '"' << id << "\" params=[";
    bool first = true;
    params.all([&s, &first](const auto &param) mutable {
      if (first) first = false; else s << ", ";
      s << param;
    });
    s << ']';
  })); */

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

  int r = PQsendQueryPrepared(
    m_conn, id.data(),
    n, paramValues, paramLengths, paramFormats, 1);
  if (r != 1) return SendState::Again;
  return State;
}

void StoreTbl::open_enqueue(bool sync, bool srm)
{
  using namespace Work;
  m_store->enqueue(TblQuery{this, {Open{}}, sync, srm});
}

int StoreTbl::open_send()
{
  switch (m_openState.phase()) {
    case OpenState::MkTable:	return mkTable_send();
    case OpenState::MkIndices:	return mkIndices_send();
    case OpenState::PrepCount:	return prepCount_send();
    case OpenState::PrepSelectKIX:
    case OpenState::PrepSelectKNX:
    case OpenState::PrepSelectKNI:
    case OpenState::PrepSelectRIX:
    case OpenState::PrepSelectRNX:
    case OpenState::PrepSelectRNI: return prepSelect_send();
    case OpenState::PrepFind:	return prepFind_send();
    case OpenState::PrepInsert:	return prepInsert_send();
    case OpenState::PrepUpdate:	return prepUpdate_send();
    case OpenState::PrepDelete:	return prepDelete_send();
    case OpenState::PrepMRD:	return prepMRD_send();
    case OpenState::Count:	return openCount_send();
    case OpenState::MaxUN:	return maxUN_send();
    case OpenState::EnsureMRD:	return ensureMRD_send();
    case OpenState::MRD:	return mrd_send();
  }
  return SendState::Unsent;
}

void StoreTbl::open_rcvd(PGresult *res)
{
  switch (m_openState.phase()) {
    case OpenState::MkTable:	mkTable_rcvd(res); break;
    case OpenState::MkIndices:	mkIndices_rcvd(res); break;
    case OpenState::PrepCount:	prepCount_rcvd(res); break;
    case OpenState::PrepSelectKIX:
    case OpenState::PrepSelectKNX:
    case OpenState::PrepSelectKNI:
    case OpenState::PrepSelectRIX:
    case OpenState::PrepSelectRNX:
    case OpenState::PrepSelectRNI: prepSelect_rcvd(res); break;
    case OpenState::PrepFind:	prepFind_rcvd(res); break;
    case OpenState::PrepInsert:	prepInsert_rcvd(res); break;
    case OpenState::PrepUpdate:	prepUpdate_rcvd(res); break;
    case OpenState::PrepDelete:	prepDelete_rcvd(res); break;
    case OpenState::PrepMRD:	prepMRD_rcvd(res); break;
    case OpenState::Count:	openCount_rcvd(res); break;
    case OpenState::MaxUN:	maxUN_rcvd(res); break;
    case OpenState::EnsureMRD:	ensureMRD_rcvd(res); break;
    case OpenState::MRD:	mrd_rcvd(res); break;
  }
}

void StoreTbl::open_failed(Event e)
{
  // ZeLOG(Debug, ([](auto &s) { }));

  m_openState.phase(OpenState::Opened);
  m_openState.setFailed();

  auto openFn = ZuMv(m_openFn);

  m_openFn = OpenFn{};

  openFn(OpenResult{ZuMv(e)});
}

void StoreTbl::opened()
{
  // ZeLOG(Debug, ([](auto &s) { }));

  m_openState.phase(OpenState::Opened);

  auto openFn = ZuMv(m_openFn);

  m_openFn = OpenFn{};

  openFn(OpenResult{OpenData{
    .storeTbl = this,
    .count = m_count,
    .un = m_maxUN,
    .sn = m_maxSN
  }});
}

void StoreTbl::mkTable()
{
  m_openState.phase(OpenState::MkTable);
  open_enqueue(false, true);
}
int StoreTbl::mkTable_send()
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!m_openState.create()) {
    Tuple params = { Value{String(m_id_)} };
    return m_store->sendQuery<SendState::Flush>(
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
      "\"_shard\" uint1 NOT NULL, "
      "\"_un\" uint8 NOT NULL, "
      "\"_sn\" uint16 NOT NULL, "
      "\"_vn\" int8 NOT NULL";
    unsigned n = m_xFields.length();
    for (unsigned i = 0; i < n; i++) {
      auto name = m_store->oids().name(m_xFields[i].type);
      if (!name) {
	ZeLOG(Fatal, ([type = m_xFields[i].type](auto &s) {
	  s << "missing OID name for type=" << type;
	}));
	return SendState::Unsent;
      }
      query << ", \"" << m_xFields[i].id_ << "\" "
	<< m_store->oids().name(m_xFields[i].type);
      {
	auto type = m_xFields[i].type;
	if (isVar(type) ||
	    type == Value::Index<String>{} ||
	    type == Value::Index<Bytes>{})
	  query << " STORAGE EXTERNAL";
      }
      query << " NOT NULL";
    }
    query << ", PRIMARY KEY (\"_shard\", \"_un\"))";
    return m_store->sendQuery<SendState::Sync>(query, Tuple{});
  }
}
void StoreTbl::mkTable_rcvd(PGresult *res)
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

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
      open_enqueue(true, false);
    } else {
      // table exists but not all fields matched
      auto i = m_openState.field();
      auto e = ZeVEVENT(Fatal, ([
	id = this->m_id_,
	failed = m_openState.failed(),
	i, field = m_fields[i],
	nFields = m_xFields.length()
      ](auto &s, const auto &) {
	auto ftype = field->type;
	s << "inconsistent schema for table " << id
	  << " field[" << i << "]={id=" << field->id
	  << " typeCode=" << ZtFieldTypeCode::name(field->type->code);
	if (ftype->code == ZtFieldTypeCode::UDT) {
	  auto udtInfo = ftype->info.udt();
	  s << " typeID=" << udtInfo->id
	    << " typeName=" << ZmDemangle{udtInfo->info->name()};
	}
	s << "} nFields=" << nFields;
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
    ZuCSpan id{id_};
    if (PQgetlength(res, i, 1) != 4) {
      m_openState.setFailed();
      return;
    }
    unsigned oid = reinterpret_cast<UInt32 *>(PQgetvalue(res, i, 1))->v;
    unsigned field = ZuCmp<unsigned>::null();
    unsigned type = ZuCmp<unsigned>::null();
    if (id == "_shard") {
      type = Value::Index<UInt8>{};
    } else if (id == "_un") {
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

    /* ZeLOG(Debug, ([
      id = ZtString{id}, oid, field, match, state = m_openState.v
    ](auto &s) {
      int field_ = ZuCmp<unsigned>::null(field) ? -1 : int(field);
      s << "id=" << id << " oid=" << oid
	<< " field=" << field_ << " match=" << (match ? 'T' : 'F')
	<< " openState=" << ZuBoxed(state).hex();
    })); */

    if (!m_openState.failed() && !match) {
      m_openState.setFailed();
      return;
    }
  }
}

void StoreTbl::mkIndices()
{
  m_openState.phase(OpenState::MkIndices);
  open_enqueue(false, true);
}
int StoreTbl::mkIndices_send()
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  auto keyID = m_openState.keyID();
  ZtString name(m_id_.length() + 16);
  name << m_id_ << '_' << keyID;
  if (!m_openState.create()) {
    Tuple params = { Value{String(name)} };
    return m_store->sendQuery<SendState::Flush>(
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
    query << "CREATE INDEX \"" << name << "\" ON \"" << m_id_ << "\" (";
    const auto &keyFields = m_keyFields[keyID];
    const auto &xKeyFields = m_xKeyFields[keyID];
    unsigned n = xKeyFields.length();
    // determine if index is a mix of multiple ascending and descending fields
    unsigned j = 0; // count of descending fields in key
    for (unsigned i = 0; i < n; i++)
      if (keyFields[i]->descend & (uint64_t(1)<<keyID)) j++;
    bool mixed = j > 0 && j < n;
    for (unsigned i = 0; i < n; i++) {
      if (i) query << ", ";
      query << '"' << xKeyFields[i].id_ << '"';
      // if mixed, the index itself needs to be descending for this column
      // - Postgres optimizes appending at the tail, but not inserting
      //   at the head; while descending fields are queried in that order
      //   (sequence numbers, integer IDs, etc.), they are rarely if ever
      //   inserted in descending order
      // - meanwhile B-Tree indices query just as efficiently in either
      //   direction as long as the select direction is consistent among the
      //   columns comprising the index, but if the directions are mixed
      //   then the column should be specified as descending:
      // - inserting in the opposite direction to the index costs ~60% more
      //   CPU time (as of Postgres v16, 2024)
      if (mixed && (keyFields[i]->descend & (uint64_t(1)<<keyID)))
	query << " DESC";
    }
    query << ")";
    return m_store->sendQuery<SendState::Sync>(query, Tuple{});
  }
}
void StoreTbl::mkIndices_rcvd(PGresult *res)
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  auto nextKey = [this]() {
    m_openState.incKey();
    if (m_openState.keyID() >= m_keyFields.length())
      prepCount();
    else
      open_enqueue(false, true);
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
      open_enqueue(true, false);
    } else {
      // index exists but not all fields matched
      open_failed(ZeVEVENT(Fatal, ([id = m_id_](auto &s, const auto &) {
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
    ZuCSpan id{id_};
    if (PQgetlength(res, i, 1) != sizeof(UInt32)) {
      m_openState.setFailed();
      return;
    }
    unsigned oid = reinterpret_cast<UInt32 *>(PQgetvalue(res, i, 1))->v;
    auto keyID = m_openState.keyID();
    unsigned field = m_openState.field();
    const auto &xKeyFields = m_xKeyFields[keyID];
    ZuCSpan matchID = xKeyFields[field].id_;
    unsigned type = xKeyFields[field].type;
    bool match = m_store->oids().match(oid, type) && id == matchID;

    /* ZeLOG(Debug, ([
      id = ZtString{id}, oid, field, match, state = m_openState.v
    ](auto &s) {
      int field_ = ZuCmp<unsigned>::null(field) ? -1 : int(field);
      s << "id=" << id << " oid=" << oid
	<< " field=" << field_ << " match=" << (match ? 'T' : 'F')
	<< " openState=" << ZuBoxed(state).hex();
    })); */

    if (!m_openState.failed() && !match) {
      m_openState.setFailed();
      return;
    }
    m_openState.incField();
  }
}

void StoreTbl::prepCount()
{
  m_openState.phase(OpenState::PrepCount);
  open_enqueue(true, false);
}
int StoreTbl::prepCount_send()
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  unsigned keyID = m_openState.keyID();
  // const auto &keyFields = m_keyFields[keyID];
  const auto &xKeyFields = m_xKeyFields[keyID];

  ZtString id(m_id_.length() + 24);
  id << m_id_ << "_count_" << keyID;

  ZtString query;
  query << "SELECT CAST(COUNT(*) AS uint8) FROM \"" << m_id_ << '"';
  ZtArray<Oid> oids;
  unsigned i, k = m_keyGroup[keyID];
  for (i = 0; i < k; i++) {
    auto type = xKeyFields[i].type;
    if (!i)
      query << " WHERE ";
    else
      query << " AND ";
    query << '"' << xKeyFields[i].id_
      << "\"=$" << (i + 1) << "::" << m_store->oids().name(type);
    oids.push(m_store->oids().oid(type));
  }

  return m_store->sendPrepare(id, query, oids);
}
void StoreTbl::prepCount_rcvd(PGresult *res)
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) {
    m_openState.incKey();
    if (m_openState.keyID() >= m_keyFields.length())
      prepSelect();
    else
      open_enqueue(true, false);
  }
}

void StoreTbl::prepSelect()
{
  switch (m_openState.phase()) {
    default:
      m_openState.phase(OpenState::PrepSelectKIX);
      break;
    case OpenState::PrepSelectKIX:
      m_openState.phase(OpenState::PrepSelectKNX);
      break;
    case OpenState::PrepSelectKNX:
      m_openState.phase(OpenState::PrepSelectKNI);
      break;
    case OpenState::PrepSelectKNI:
      m_openState.phase(OpenState::PrepSelectRIX);
      break;
    case OpenState::PrepSelectRIX:
      m_openState.phase(OpenState::PrepSelectRNX);
      break;
    case OpenState::PrepSelectRNX:
      m_openState.phase(OpenState::PrepSelectRNI);
      break;
  }
  open_enqueue(true, false);
}
int StoreTbl::prepSelect_send()
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  unsigned keyID = m_openState.keyID();
  const auto &keyFields = m_keyFields[keyID];
  const auto &xKeyFields = m_xKeyFields[keyID];

  ZtString id(m_id_.length() + 24);
  id << m_id_ << "_select";
  switch (m_openState.phase()) {
    case OpenState::PrepSelectKIX: id << "KIX_"; break;
    case OpenState::PrepSelectKNX: id << "KNX_"; break;
    case OpenState::PrepSelectKNI: id << "KNI_"; break;
    case OpenState::PrepSelectRIX: id << "RIX_"; break;
    case OpenState::PrepSelectRNX: id << "RNX_"; break;
    case OpenState::PrepSelectRNI: id << "RNI_"; break;
  }
  id << keyID;

  unsigned n;

  ZtString query;
  query << "SELECT ";
  if (m_openState.phase() == OpenState::PrepSelectKIX ||
      m_openState.phase() == OpenState::PrepSelectKNX ||
      m_openState.phase() == OpenState::PrepSelectKNI) { // selectKeys
    n = keyFields.length();
    for (unsigned i = 0; i < n; i++) {
      if (i) query << ", ";
      query << '"' << xKeyFields[i].id_ << '"';
    }
  } else { // selectRows
    n = m_fields.length();
    for (unsigned i = 0; i < n; i++) {
      if (i) query << ", ";
      query << '"' << m_xFields[i].id_ << '"';
    }
    n = keyFields.length();
  }
  query << " FROM \"" << m_id_ << '"';
  ZtArray<Oid> oids;
  unsigned i, k = m_keyGroup[keyID];
  for (i = 0; i < k; i++) {
    auto type = xKeyFields[i].type;
    if (!i)
      query << " WHERE ";
    else
      query << " AND ";
    query << '"' << xKeyFields[i].id_
      << "\"=$" << (i + 1) << "::" << m_store->oids().name(type);
    oids.push(m_store->oids().oid(type));
  }
  if (m_openState.phase() == OpenState::PrepSelectKNX ||
      m_openState.phase() == OpenState::PrepSelectKNI ||
      m_openState.phase() == OpenState::PrepSelectRNX ||
      m_openState.phase() == OpenState::PrepSelectRNI) // continuation
    for (i = k; i < n; i++) {
      auto type = xKeyFields[i].type;
      if (!i) // k could be 0
	query << " WHERE ";
      else
	query << " AND ";
      query << '"' << xKeyFields[i].id_ << '"';
      if (keyFields[i]->descend & (uint64_t(1)<<keyID)) {
	if (m_openState.phase() == OpenState::PrepSelectKNI ||
	    m_openState.phase() == OpenState::PrepSelectRNI) // inclusive
	  query << "<=";
	else
	  query << '<';
      } else {
	if (m_openState.phase() == OpenState::PrepSelectKNI ||
	    m_openState.phase() == OpenState::PrepSelectRNI) // inclusive
	  query << ">=";
	else
	  query << '>';
      }
      query << '$' << (i + 1) << "::" << m_store->oids().name(type);
      oids.push(m_store->oids().oid(type));
    }
  query << " ORDER BY ";
  for (i = k; i < n; i++) {
    if (i > k) query << ", ";
    query << '"' << xKeyFields[i].id_ << '"';
    if (keyFields[i]->descend & (uint64_t(1)<<keyID))
      query << " DESC";
  }
  query << " LIMIT $" << (oids.length() + 1) << "::uint8";
  oids.push(m_store->oids().oid(Value::Index<UInt64>{}));

  return m_store->sendPrepare(id, query, oids);
}
void StoreTbl::prepSelect_rcvd(PGresult *res)
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) {
    m_openState.incKey();
    if (m_openState.keyID() >= m_keyFields.length()) {
      if (m_openState.phase() < OpenState::PrepSelectRNI)
	prepSelect();
      else
	prepFind();
    } else
      open_enqueue(true, false);
  }
}

void StoreTbl::prepFind()
{
  m_openState.phase(OpenState::PrepFind);
  open_enqueue(true, false);
}
int StoreTbl::prepFind_send()
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  unsigned keyID = m_openState.keyID();
  ZtString id(m_id_.length() + 24);
  id << m_id_;
  if (!keyID)
    id << "_recover";
  else
    id << "_find_" << (keyID - 1);

  ZtString query;
  query << "SELECT \"_shard\", \"_un\", \"_sn\", \"_vn\"";
  unsigned n = m_xFields.length();
  for (unsigned i = 0; i < n; i++) {
    query << ", \"" << m_xFields[i].id_ << '"';
  }
  query << " FROM \"" << m_id_ << "\" WHERE ";
  ZtArray<Oid> oids;
  if (!keyID) {
    query << "\"_shard\"=$1::uint1 AND \"_un\"=$2::uint8";
    oids.push(m_store->oids().oid(Value::Index<UInt8>{}));
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
  query << " LIMIT 1";
  return m_store->sendPrepare(id, query, oids);
}
void StoreTbl::prepFind_rcvd(PGresult *res)
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) {
    m_openState.incKey();
    if (m_openState.keyID() > m_keyFields.length()) // not >=
      prepInsert();
    else
      open_enqueue(true, false);
  }
}

void StoreTbl::prepInsert()
{
  m_openState.phase(OpenState::PrepInsert);
  open_enqueue(true, false);
}
int StoreTbl::prepInsert_send()
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  ZtString id(m_id_.length() + 8);
  id << m_id_ << "_insert";

  ZtString query;
  unsigned n = m_xFields.length();
  ZtArray<Oid> oids(n + 4);
  query << "INSERT INTO \"" << m_id_
    << "\" (\"_shard\", \"_un\", \"_sn\", \"_vn\"";
  for (unsigned i = 0; i < n; i++) {
    query << ", \"" << m_xFields[i].id_ << '"';
  }
  query << ") VALUES ($1::uint1, $2::uint8, $3::uint16, $4::uint8";
  oids.push(m_store->oids().oid(Value::Index<UInt8>{}));
  oids.push(m_store->oids().oid(Value::Index<UInt64>{}));
  oids.push(m_store->oids().oid(Value::Index<UInt128>{}));
  oids.push(m_store->oids().oid(Value::Index<Int64>{}));
  for (unsigned i = 0; i < n; i++) {
    auto type = m_xFields[i].type;
    query << ", $" << (i + 5) << "::" << m_store->oids().name(type);
    oids.push(m_store->oids().oid(type));
  }
  query << ')';
  return m_store->sendPrepare(id, query, oids);
}
void StoreTbl::prepInsert_rcvd(PGresult *res)
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) prepUpdate();
}

void StoreTbl::prepUpdate()
{
  m_openState.phase(OpenState::PrepUpdate);
  open_enqueue(true, false);
}
int StoreTbl::prepUpdate_send()
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

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
    if (!(m_fields[i]->props & ZtVFieldProp::Mutable())) continue;
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
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) prepDelete();
}

void StoreTbl::prepDelete()
{
  m_openState.phase(OpenState::PrepDelete);
  open_enqueue(true, false);
}
int StoreTbl::prepDelete_send()
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

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
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) prepMRD();
}

void StoreTbl::prepMRD()
{
  m_openState.phase(OpenState::PrepMRD);
  open_enqueue(true, false);
}
int StoreTbl::prepMRD_send()
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  ZtString id(m_id_.length() + 8);
  id << m_id_ << "_mrd";

  ZtString query;
  const auto &xKeyFields = m_xKeyFields[0];
  unsigned n = xKeyFields.length();
  ZtArray<Oid> oids(n);
  query <<
    "UPDATE \"zdb.mrd\" SET \"un\"=$2::uint8, \"sn\"=$3::uint16 "
      "WHERE \"tbl\"='" << m_id_ << "' AND \"shard\"=$1::uint1";
  return m_store->sendPrepare(id, query, oids);
}
void StoreTbl::prepMRD_rcvd(PGresult *res)
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) openCount();
}

void StoreTbl::openCount()
{
  m_openState.phase(OpenState::Count);
  open_enqueue(false, false);
}
int StoreTbl::openCount_send()
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  ZtString query;
  query << "SELECT CAST(COUNT(*) AS uint8) FROM \"" << m_id_ << '"';
  return m_store->sendQuery<SendState::Flush>(query, Tuple{});
}
void StoreTbl::openCount_rcvd(PGresult *res)
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) { maxUN(); return; }

  if (PQntuples(res) != 1 ||
      PQnfields(res) != 1 ||
      PQgetlength(res, 0, 0) != sizeof(UInt64)) {
    // invalid query result
    open_failed(ZeVEVENT(Fatal, ([id = m_id_](auto &s, const auto &) {
      s << "inconsistent count() result for table " << id;
    })));
    return;
  }

  m_count = uint64_t(reinterpret_cast<UInt64 *>(PQgetvalue(res, 0, 0))->v);
}

void StoreTbl::maxUN()
{
  m_openState.phase(OpenState::MaxUN);
  open_enqueue(false, false);
}
int StoreTbl::maxUN_send()
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  Tuple params = { Value{UInt8{m_openState.shard()}} };
  ZtString query;
  query << "SELECT \"_un\", \"_sn\" FROM \"" << m_id_
    << "\" WHERE \"_un\"=(SELECT MAX(\"_un\") FROM \"" << m_id_
    << "\" WHERE \"_shard\"=$1::uint1)";
  return m_store->sendQuery<SendState::Flush>(query, params);
}
void StoreTbl::maxUN_rcvd(PGresult *res)
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) {
    m_openState.incShard();
    if (m_openState.shard() >= m_maxUN.length())
      ensureMRD();
    else
      open_enqueue(false, false);
    return;
  }

  auto shard = m_openState.shard();

  unsigned n = PQntuples(res);
  if (n && (PQnfields(res) != 2)) goto inconsistent;
  for (unsigned i = 0; i < n; i++) {
    if (PQgetlength(res, i, 0) != sizeof(UInt64) ||
	PQgetlength(res, i, 1) != sizeof(UInt128)) goto inconsistent;
    auto un = uint64_t(
      reinterpret_cast<const UInt64 *>(PQgetvalue(res, i, 0))->v);
    auto sn = uint128_t(
      reinterpret_cast<const UInt128 *>(PQgetvalue(res, i, 1))->v);

    /* ZeLOG(Debug, ([un, sn](auto &s) {
      s << "un=" << un << " sn=" << ZuBoxed(sn);
    })); */

    auto &maxUN = m_maxUN[shard];
    if (maxUN == ZdbNullUN() || un > maxUN) maxUN = un;
    if (m_maxSN == ZdbNullSN() || sn > m_maxSN) m_maxSN = sn;
  }
  return;

inconsistent:
  open_failed(ZeVEVENT(Fatal, ([id = m_id_](auto &s, const auto &) {
    s << "inconsistent MAX(_un) result for table " << id;
  })));
}

void StoreTbl::ensureMRD()
{
  m_openState.phase(OpenState::EnsureMRD);
  open_enqueue(true, false);
}
int StoreTbl::ensureMRD_send()
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  Tuple params = { Value{String(m_id_)}, Value{UInt8{m_openState.shard()}} };
  return m_store->sendQuery<SendState::Sync>(
    "INSERT INTO \"zdb.mrd\" (\"tbl\", \"shard\", \"un\", \"sn\") "
      "VALUES ($1::text, $2::uint1, 0, 0) "
      "ON CONFLICT (\"tbl\", \"shard\") DO NOTHING", params);
}
void StoreTbl::ensureMRD_rcvd(PGresult *res)
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) {
    m_openState.incShard();
    if (m_openState.shard() >= m_maxUN.length())
      mrd();
    else
      open_enqueue(true, false);
  }
}

void StoreTbl::mrd()
{
  m_openState.phase(OpenState::MRD);
  open_enqueue(true, false);
}
int StoreTbl::mrd_send()
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  Tuple params = { Value{String(m_id_)}, Value{UInt8{m_openState.shard()}} };
  return m_store->sendQuery<SendState::Sync>(
    "SELECT \"un\", \"sn\" FROM \"zdb.mrd\" "
      "WHERE \"tbl\"=$1::text AND \"shard\"=$2::uint1", params);
}
void StoreTbl::mrd_rcvd(PGresult *res)
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) {
    m_openState.incShard();
    if (m_openState.shard() >= m_maxUN.length())
      opened();
    else
      open_enqueue(true, false);
    return;
  }

  auto shard = m_openState.shard();

  unsigned n = PQntuples(res);
  if (n && (PQnfields(res) != 2)) goto inconsistent;
  for (unsigned i = 0; i < n; i++) {
    if (PQgetlength(res, i, 0) != sizeof(UInt64) ||
	PQgetlength(res, i, 1) != sizeof(UInt128)) goto inconsistent;
    auto un = uint64_t(
      reinterpret_cast<const UInt64 *>(PQgetvalue(res, i, 0))->v);
    auto sn = uint128_t(
      reinterpret_cast<const UInt128 *>(PQgetvalue(res, i, 1))->v);

    /* ZeLOG(Debug, ([un, sn](auto &s) {
      s << "un=" << un << " sn=" << ZuBoxed(sn);
    })); */

    auto &maxUN = m_maxUN[shard];
    if (un > maxUN) maxUN = un;
    if (sn > m_maxSN) m_maxSN = sn;
  }
  return;

inconsistent:
  open_failed(ZeVEVENT(Fatal, ([id = m_id_](auto &s, const auto &) {
    s << "inconsistent SELECT FROM zdb.mrd result for table " << id;
  })));
}

void StoreTbl::close(CloseFn fn)
{
  m_store->run([this, fn = ZuMv(fn)]() mutable {
    m_openState.phase(OpenState::Closed);
    fn();
  });
}

void StoreTbl::warmup() { /* LATER */ }

void StoreTbl::count(unsigned keyID, ZmRef<const IOBuf> buf, CountFn countFn)
{
  ZmAssert(keyID < m_keyFields.length());

  using namespace Work;

  m_store->run([
    this, keyID, buf = ZuMv(buf), countFn = ZuMv(countFn)
  ]() mutable {
    if (m_store->stopping()) {
      countFn(CountResult{ZeVEVENT(Error, ([id = m_id](auto &s, const auto &) {
	s << "count(" << id << ") failed - DB shutdown in progress";
      }))});
      return;
    }
    m_store->enqueue(TblQuery{this,
      Query{Count{
	.keyID = keyID,
	.buf = ZuMv(buf),
	.countFn = ZuMv(countFn)
      }}, false, false});
  });
}

void StoreTbl::select(
  bool selectRow, bool selectNext, bool inclusive,
  unsigned keyID, ZmRef<const IOBuf> buf,
  unsigned limit, TupleFn tupleFn)
{
  ZmAssert(keyID < m_keyFields.length());

  using namespace Work;

  m_store->run([
    this, selectRow, selectNext, inclusive,
    keyID, buf = ZuMv(buf), limit, tupleFn = ZuMv(tupleFn)
  ]() mutable {
    if (m_store->stopping()) {
      tupleFn(TupleResult{ZeVEVENT(Error, ([id = m_id](auto &s, const auto &) {
	s << "select(" << id << ") failed - DB shutdown in progress";
      }))});
      return;
    }
    m_store->enqueue(TblQuery{this,
      Query{Select{
	.keyID = keyID,
	.limit = limit,
	.buf = ZuMv(buf),
	.tupleFn = ZuMv(tupleFn),
	.selectRow = selectRow,
	.selectNext = selectNext,
	.inclusive = inclusive
      }}, false, true});
  });
}

// reduce heap allocations with ZmAlloc()

#define IDAlloc(appendSize) \
  auto idSize = m_id_.length() + appendSize; \
  auto id_ = ZmAlloc(char, idSize); \
  ZtString id(&id_[0], 0, idSize, false);

#define ParamAlloc(nParams) \
  auto params_ = ZmAlloc(Value, nParams); \
  Tuple params(&params_[0], 0, nParams, false)

#define VarAlloc(nParams, xfields, fbo) \
  unsigned nVars = 0; \
  for (unsigned i = 0; i < nParams; i++) \
    if (isVar(xfields[i].type)) nVars++; \
  auto varBufParts_ = ZmAlloc(VarBufPart, nVars); \
  ZtArray<VarBufPart> varBufParts(&varBufParts_[0], 0, nVars, false); \
  unsigned varBufSize_ = 0; \
  if (nVars > 0) { \
    for (unsigned i = 0; i < nParams; i++) { \
      if (!isVar(xfields[i].type)) continue; \
      auto field = xfields[i].field; \
      unsigned size = ZuSwitch::dispatch<Value::N>(xfields[i].type, \
	  [field, fbo](auto Type) { return varBufSize<Type>(field, fbo); }); \
      varBufParts.push(VarBufPart{varBufSize_, size}); \
      varBufSize_ += size; \
    } \
  } \
  auto varBuf_ = ZmAlloc(uint8_t, varBufSize_); \
  ZtArray<uint8_t> varBuf(&varBuf_[0], varBufSize_, varBufSize_, false)

int StoreTbl::count_send(Work::Count &count)
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  const auto &keyFields = m_keyFields[count.keyID];
  const auto &xKeyFields = m_xKeyFields[count.keyID];
  auto nParams = m_keyGroup[count.keyID];
  auto fbo = Zfb::GetAnyRoot(count.buf->data());

  IDAlloc(24);
  ParamAlloc(nParams);
  VarAlloc(nParams, xKeyFields, fbo);

  if (nParams > 0)
    loadTuple(
      params, varBuf, varBufParts,
      m_store->oids(), nParams, keyFields, xKeyFields, fbo);
  id << m_id_ << "_count_" << count.keyID;
  return m_store->sendPrepared<SendState::Flush>(id, params);
}
void StoreTbl::count_rcvd(Work::Count &count, PGresult *res)
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) {
    count.countFn(CountResult{CountData{.count = 0}});
    return;
  }

  if (PQntuples(res) != 1 ||
      PQnfields(res) != 1 ||
      PQgetlength(res, 0, 0) != sizeof(UInt64)) {
    // invalid query result
    count_failed(count, ZeVEVENT(Fatal, ([id = m_id_](auto &s, const auto &) {
      s << "inconsistent count() result for table " << id;
    })));
    return;
  }

  count.countFn(CountResult{CountData{
    .count = uint64_t(reinterpret_cast<UInt64 *>(PQgetvalue(res, 0, 0))->v)
  }});
}
void StoreTbl::count_failed(Work::Count &count, ZeVEvent e)
{
  count.countFn(CountResult{ZuMv(e)});
}

int StoreTbl::select_send(Work::Select &select)
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  const auto &keyFields = m_keyFields[select.keyID];
  const auto &xKeyFields = m_xKeyFields[select.keyID];
  auto nParams =
    select.selectNext ? keyFields.length() : m_keyGroup[select.keyID];
  auto fbo = Zfb::GetAnyRoot(select.buf->data());

  IDAlloc(24);
  nParams += 1; // +1 for limit
  ParamAlloc(nParams);
  nParams -= 1;
  VarAlloc(nParams, xKeyFields, fbo);

  if (nParams > 0)
    loadTuple(
      params, varBuf, varBufParts,
      m_store->oids(), nParams, keyFields, xKeyFields, fbo);
  new (params.push()) Value{UInt64{select.limit}};
  id << m_id_ << "_select"
    << (select.selectRow ? 'R' : 'K')
    << (select.selectNext ? 'N' : 'I')
    << (select.inclusive ? 'I' : 'X')
    << '_' << select.keyID;
  return m_store->sendPrepared<SendState::Flush>(id, params);
}
void StoreTbl::select_rcvd(Work::Select &select, PGresult *res)
{
  // ZeLOG(Debug, ([v = m_openState.v](auto &s) { s << ZuBoxed(v).hex(); }));

  if (!res) {
    select.tupleFn(TupleResult{});
    return;
  }

  unsigned nr = PQntuples(res);
  if (!nr) return;

  unsigned keyID = select.keyID;
  const auto &keyFields = select.selectRow ? m_fields : m_keyFields[keyID];
  const auto &xKeyFields = select.selectRow ? m_xFields : m_xKeyFields[keyID];
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
      select_save(ZuSpan<const Value>(&tuple[0], nc), xKeyFields).constRef();
    // res can go out of scope now - everything is saved in buf
    select.tupleFn(TupleResult{TupleData{
      .keyID = select.selectRow ? ZuFieldKeyID::All : int(keyID),
      .buf = ZuMv(buf),
      .count = ++select.count // do not be tempted to use i (multiple batches)
    }});
  }
  return;

inconsistent:
  select_failed(select, ZeVEVENT(Fatal, ([id = m_id_](auto &s, const auto &) {
    s << "inconsistent select() result for table " << id;
  })));
}
ZmRef<IOBuf> StoreTbl::select_save(
  ZuSpan<const Value> tuple, const XFields &xFields)
{
  Zfb::IOBuilder fbb{m_bufAllocFn()};
  fbb.Finish(saveTuple(fbb, xFields, tuple));
  return fbb.buf();
}
void StoreTbl::select_failed(Work::Select &select, ZeVEvent e)
{
  select.tupleFn(TupleResult{ZuMv(e)});
}

void StoreTbl::find(unsigned keyID, ZmRef<const IOBuf> buf, RowFn rowFn)
{
  ZmAssert(keyID < m_keyFields.length());

  using namespace Work;

  m_store->run(
    [this, keyID, buf = ZuMv(buf), rowFn = ZuMv(rowFn)]() mutable {
      if (m_store->stopping()) {
	rowFn(RowResult{ZeVEVENT(Error, ([id = m_id](auto &s, const auto &) {
	  s << "find(" << id << ") failed - DB shutdown in progress";
	}))});
	return;
      }
      m_store->enqueue(TblQuery{this,
	Query{Find{keyID, ZuMv(buf), ZuMv(rowFn)}}, false, true});
    });
}
int StoreTbl::find_send(Work::Find &find)
{
  const auto &keyFields = m_keyFields[find.keyID];
  const auto &xKeyFields = m_xKeyFields[find.keyID];
  auto nParams = keyFields.length();
  auto fbo = Zfb::GetAnyRoot(find.buf->data());

  IDAlloc(24);
  ParamAlloc(nParams);
  VarAlloc(nParams, xKeyFields, fbo);

  loadTuple(
    params, varBuf, varBufParts,
    m_store->oids(), nParams, keyFields, xKeyFields, fbo);
  id << m_id_ << "_find_" << find.keyID;
  return m_store->sendPrepared<SendState::Flush>(id, params);
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
    if (!found) rowFn(RowResult{});
    return;
  }

  unsigned nr = PQntuples(res);
  if (!nr) return;

  unsigned nc = m_xFields.length() + 4;

  // tuple is POD, no need to run destructors when going out of scope
  auto tuple_ = ZmAlloc(Value, nc);
  auto tuple = ZuSpan<const Value>(&tuple_[0], nc);

  if (PQnfields(res) != nc) goto inconsistent;
  for (unsigned i = 0; i < nr; i++) {
    for (unsigned j = 0; j < nc; j++) {
      unsigned type;
      switch (int(j)) {
	case 0: type = Value::Index<UInt8>{}; break;	// shard
	case 1: type = Value::Index<UInt64>{}; break;	// UN
	case 2: type = Value::Index<UInt128>{}; break;	// SN
	case 3: type = Value::Index<Int64>{}; break;	// VN
	default: type = m_xFields[j - 4].type; break;
      }
      if (!ZuSwitch::dispatch<Value::N>(type,
	  [&tuple_, res, i, j](auto Type) {
	    return tuple_[j].load<Type>(
	      PQgetvalue(res, i, j), PQgetlength(res, i, j));
	  }))
	goto inconsistent;
    }

    /* ZeLOG(Debug, ([tuple = Tuple(tuple)](auto &s) {
      s << "row=[";
      bool first = true;
      tuple.all([&s, &first](const auto &value) mutable {
	if (first) first = false; else s << ", ";
	s << value;
      });
      s << ']';
    })); */

    auto buf = find_save<Recovery>(tuple).constRef();
    if (found) {
      ZeLOG(Error, ([id = m_id_](auto &s) {
	s << "multiple records found with same key in table " << id;
      }));
      return;
    }
    // res can go out of scope now - everything is saved in buf
    RowResult result{RowData{.buf = ZuMv(buf)}};
    rowFn(ZuMv(result));
    found = true;
  }
  return;

inconsistent:
  if constexpr (!Recovery)
    find_failed_(ZuMv(rowFn),
      ZeVEVENT(Error, ([id = m_id_](auto &s, const auto &) {
	s << "inconsistent find() result for table " << id;
      })));
  else
    find_failed_(ZuMv(rowFn),
      ZeVEVENT(Error, ([id = m_id_](auto &s, const auto &) {
	s << "inconsistent recover() result for table " << id;
      })));
}
template <bool Recovery>
ZmRef<IOBuf> StoreTbl::find_save(ZuSpan<const Value> tuple)
{
  Zfb::IOBuilder fbb{m_bufAllocFn()};
  auto data = Zfb::Save::nest(fbb, [this, tuple](Zfb::Builder &fbb) mutable {
    tuple.offset(4); // skip shard, un, sn, vn
    return saveTuple(fbb, m_xFields, tuple);
  });
  {
    auto shard = Shard(tuple[0].p<UInt8>().v);
    UN un = tuple[1].p<UInt64>().v;
    SN sn = tuple[2].p<UInt128>().v;
    VN vn = tuple[3].p<Int64>().v;
    auto sn_ = Zfb::Save::uint128(sn);
    auto msg = fbs::CreateMsg(
      fbb, Recovery ? fbs::Body::Recovery : fbs::Body::Replication,
      fbs::CreateRecord(
	fbb, Zfb::Save::str(fbb, this->id()),
	un, &sn_, vn, shard, data).Union());
    fbb.Finish(msg);
  }
  return saveHdr(fbb);
}
void StoreTbl::find_failed(Work::Find &find, ZeVEvent e)
{
  find_failed_(ZuMv(find.rowFn), ZuMv(e));
}
void StoreTbl::find_failed_(RowFn rowFn, ZeVEvent e)
{
  RowResult result{ZuMv(e)};
  rowFn(ZuMv(result));
}

void StoreTbl::recover(Shard shard, UN un, RowFn rowFn)
{
  using namespace Work;

  m_store->run([this, shard, un, rowFn = ZuMv(rowFn)]() mutable {
    if (m_store->stopping()) {
      rowFn(RowResult{ZeVEVENT(Error, ([id = m_id](auto &s, const auto &) {
	s << "recover(" << id << ") failed - DB shutdown in progress";
      }))});
      return;
    }
    m_store->enqueue(TblQuery{this,
      Query{Recover{shard, un, ZuMv(rowFn)}}, false, true});
  });
}
int StoreTbl::recover_send(Work::Recover &recover)
{
  Tuple params = { Value{UInt8{recover.shard}}, Value{UInt64{recover.un}} };
  ZtString id(m_id_.length() + 8);
  id << m_id_ << "_recover";
  return m_store->sendPrepared<SendState::Flush>(id, params);
}
void StoreTbl::recover_rcvd(Work::Recover &recover, PGresult *res)
{
  if (!recover.rowFn) return; // recover failed

  find_rcvd_<true>(recover.rowFn, recover.found, res);
}
void StoreTbl::recover_failed(Work::Recover &recover, ZeVEvent e)
{
  find_failed_(ZuMv(recover.rowFn), ZuMv(e));
}

void StoreTbl::write(ZmRef<const IOBuf> buf, CommitFn commitFn)
{
  /* ZeLOG(Debug, ([buf = buf.ptr()](auto &s) {
    s << "buf=" << ZuBoxPtr(buf).hex();
  })); */

  using namespace Work;

  m_store->run([this, buf = ZuMv(buf), commitFn = ZuMv(commitFn)]() mutable {
    if (m_store->stopping()) {
      commitFn(ZuMv(buf), CommitResult{
	ZeVEVENT(Error, ([id = m_id](auto &s, const auto &) {
	  s << "write(" << id << ") failed - DB shutdown in progress";
	}))});
      return;
    }
    m_store->enqueue(TblQuery{this,
      Query{Write{ZuMv(buf), ZuMv(commitFn)}}, true, false});
  });
}
int StoreTbl::write_send(Work::Write &write)
{
  /* ZeLOG(Debug, ([buf = write.buf.ptr()](auto &s) {
    s << "buf=" << ZuBoxPtr(buf).hex();
  })); */

  auto record = record_(msg_(write.buf->hdr()));
  auto shard = record->shard();
  auto un =record->un(); 
  auto sn = Zfb::Load::uint128(record->sn());

  if (!write.mrd) {
    auto &maxUN = m_maxUN[shard];
    if (maxUN != ZdbNullUN() && un <= maxUN)
      return SendState::Unsent;
    maxUN = un;
    m_maxSN = sn;
  }

  /* ZeLOG(Debug, ([un, sn, vn = record->vn()](auto &s) {
    s << "UN=" << un << " SN=" << ZuBoxed(sn) << " VN=" << vn;
  })); */

  auto fbo = Zfb::GetAnyRoot(record->data()->data());
  if (!record->vn()) { // insert
    auto nParams = m_fields.length() + 4; // +4 for shard, un, sn, vn
    IDAlloc(8);
    ParamAlloc(nParams);
    nParams -= 4;
    VarAlloc(nParams, m_xFields, fbo);
    id << m_id_ << "_insert";
    new (params.push()) Value{UInt8{shard}};
    new (params.push()) Value{UInt64{un}};
    new (params.push()) Value{UInt128{sn}};
    new (params.push()) Value{UInt64{record->vn()}};
    loadTuple(
      params, varBuf, varBufParts,
      m_store->oids(), nParams, m_fields, m_xFields, fbo);
    return m_store->sendPrepared<SendState::Sync>(id, params);
  } else if (record->vn() > 0) { // update
    auto nParams = m_updFields.length() + 3; // +3 for un, sn, vn
    IDAlloc(8);
    ParamAlloc(nParams);
    nParams -= 3;
    VarAlloc(nParams, m_xUpdFields, fbo);
    id << m_id_ << "_update";
    new (params.push()) Value{UInt64{un}};
    new (params.push()) Value{UInt128{sn}};
    new (params.push()) Value{UInt64{record->vn()}};
    loadTuple(
      params, varBuf, varBufParts,
      m_store->oids(), nParams, m_updFields, m_xUpdFields, fbo);
    return m_store->sendPrepared<SendState::Sync>(id, params);
  } else if (!write.mrd) { // delete
    auto nParams = m_keyFields[0].length();
    IDAlloc(8);
    ParamAlloc(nParams);
    VarAlloc(nParams, m_xKeyFields[0], fbo);
    id << m_id_ << "_delete";
    loadTuple(
      params, varBuf, varBufParts,
      m_store->oids(), nParams, m_keyFields[0], m_xKeyFields[0], fbo);
    return m_store->sendPrepared<SendState::Sync>(id, params);
  } else { // delete - MRD
    IDAlloc(8);
    ParamAlloc(3);
    id << m_id_ << "_mrd";
    new (params.push()) Value{UInt8{shard}};
    new (params.push()) Value{UInt64{un}};
    new (params.push()) Value{UInt128{sn}};
    return m_store->sendPrepared<SendState::Sync>(id, params);
  }
}
void StoreTbl::write_rcvd(Work::Write &write, PGresult *res)
{
  /* ZeLOG(Debug, ([buf = write.buf.ptr(), res](auto &s) {
    s << "buf=" << ZuBoxPtr(buf).hex() << " res=" << ZuBoxPtr(res).hex();
  })); */

  if (res) return;

  if (!write.buf) return; // write failed

  auto record = record_(msg_(write.buf->hdr()));
  if (record->vn() < 0 && !write.mrd) { // delete completed, now update MRD

    /* ZeLOG(Debug, ([vn = record->vn(), mrd = write.mrd](auto &s) {
      s << "VN=" << vn << " mrd=" << mrd;
    })); */

    using namespace Work;
    m_store->enqueue(TblQuery{this,
      Query{Write{ZuMv(write.buf), ZuMv(write.commitFn), true}}, true, false});
  } else {
    write.commitFn(ZuMv(write.buf), CommitResult{});
  }
}
void StoreTbl::write_failed(Work::Write &write, ZeVEvent e)
{
  // ZeLOG(Debug, ([e = ZuMv(e)](auto &s) { s << e; }));

  write.commitFn(ZuMv(write.buf), CommitResult{ZuMv(e)});
}

} // ZdbPQ

Zdb_::Store *ZdbStore()
{
  return new ZdbPQ::Store{};
}
