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

#include <zlib/ZeLog.hh>

namespace ZdbPQ {

#pragma pack(push, 1)
struct UInt32 { ZuBigEndian<uint32_t> i; };
#pragma pack(pop)

void OIDs::init(PGconn *conn)
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
  for (unsigned i = 1; i < Value::N; i++) {
    auto name = names[i - 1];
    auto oid = this->oid(name);
    if (ZuCmp<unsigned>::null(oid)) oid = resolve(conn, name);
    m_oids[i - 1] = oid;
    m_types.add(unsigned(oid), int8_t(i));
    m_lookup.add(name, int8_t(i));
  }
}

unsigned OIDs::resolve(PGconn *conn, ZuString name)
{
  Oid paramTypes[1] = { 25 }; // TEXTOID
  const char *paramValues[1] = { name.data() };
  int paramLengths[1] = { int(name.length()) };
  int paramFormats[1] = { 1 };
  const char *query = "SELECT oid FROM pg_type WHERE typname = $1::text";
  PGresult *res = PQexecParams(conn, query,
    1, paramTypes, paramValues, paramLengths, paramFormats, 1);
  if (PQresultStatus(res) != PGRES_TUPLES_OK) { // failed
    PQclear(res);
    throw ZeMEVENT(Fatal, ([query, name](auto &s, const auto &) {
      s << "Store::init() \"" << query << "\" $1=\""
	<< name << "\" failed\n";
    }));
  }
  if (ZuUnlikely(
      PQnfields(res) != 1 ||
      PQntuples(res) != 1 ||
      PQgetlength(res, 0, 0) != 4))
    throw ZeMEVENT(Fatal, ([query, name](auto &s, const auto &) {
      s << "Store::init() \"" << query << "\" $1=\""
	<< name << "\" returned invalid result\n";
    }));
  return reinterpret_cast<UInt32 *>(PQgetvalue(res, 0, 0))->i;
}

InitResult Store::init(ZvCf *cf, ZiMultiplex *mx, unsigned sid)
{
  m_cf = cf;
  m_mx = mx;
  m_zdbSID = sid;

  try {
    const ZtString &tid = cf->get<true>("thread");
    auto sid = m_mx->sid(tid);
    if (!sid ||
	sid > m_mx->params().nThreads() ||
	sid == m_mx->rxThread() ||
	sid == m_mx->txThread())
      return {ZeMEVENT(Fatal, ([tid = ZtString{tid}](auto &s, const auto &) {
	s << "Store::init() failed: invalid thread configuration \""
	  << tid << "\"";
      }))};
    m_pqSID = sid;
  } catch (const ZvError &e_) {
    ZtString e;
    e << e_;
    return {ZeMEVENT(Fatal, ([e = ZuMv(e)](auto &s, const auto &) {
      s << "Store::init() failed: invalid thread configuration: " << e;
    }))};
  }

  if (!m_storeTbls) m_storeTbls = new StoreTbls{};

  return {InitData{.replicated = true}};
}

void Store::final()
{
  m_storeTbls->clean();
  m_storeTbls = nullptr;
}

void Store::close_fds()
{
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

void Store::start(StartFn fn)
{
  ZeLOG(Debug, ([](auto &s) { }));
  m_mx->wakeFn(m_pqSID, ZmFn{this, [](Store *store) { store->wake(); }});
  m_mx->push(m_pqSID, [this, fn = ZuMv(fn)]() mutable {
    if (!start_()) {
      zdbRun([fn = ZuMv(fn)]() {
	fn(StartResult{ZeMEVENT(Fatal, "PostgreSQL start() failed")});
      });
      return;
    }
    zdbRun([fn = ZuMv(fn)]() { fn(StartResult{}); });
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
    close_fds();
    return false;
  }

  try {
    m_oids.init(m_conn);
  } catch (const ZeMEvent &e) {
    ZeLogEvent(ZuMv(const_cast<ZeMEvent &>(e)));
    close_fds();
    return false;
  }

  m_connFD = PQsocket(m_conn);

  if (PQsetnonblocking(m_conn, 1) != 0) {
    ZeLOG(Fatal, ([e = connError(m_conn)](auto &s) {
      s << "PQsetnonblocking() failed: " << e;
    }));
    close_fds();
    return false;
  }

  if (PQenterPipelineMode(m_conn) != 1) {
    ZeLOG(Fatal, ([e = connError(m_conn)](auto &s) {
      s << "PQenterPipelineMode() failed: " << e;
    }));
    close_fds();
    return false;
  }

#ifndef _WIN32

  // set up I/O multiplexer (epoll)
  if ((m_epollFD = epoll_create(2)) < 0) {
    ZeLOG(Fatal, ([e = ZeLastError](auto &s) {
      s << "epoll_create() failed: " << e;
    }));
    close_fds();
    return false;
  }
  if (pipe(&m_wakeFD) < 0) {
    ZeLOG(Fatal, ([e = errno](auto &s) {
      s << "pipe() failed: " << e;
    }));
    close_fds();
    return false;
  }
  if (fcntl(m_wakeFD, F_SETFL, O_NONBLOCK) < 0) {
    ZeLOG(Fatal, ([e = errno](auto &s) {
      s << "fcntl(F_SETFL, O_NONBLOCK) failed: " << e;
    }));
    close_fds();
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
      close_fds();
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
    close_fds();
    return false;
  }

  m_connEvent = WSACreateEvent();
  if (m_connEvent == NULL || m_connEvent == INVALID_HANDLE_VALUE) {
    ZeLOG(Fatal, ([e = ZeLastError](auto &s) {
      s << "CreateEvent() failed: " << e;
    }));
    close_fds();
    return false;
  }
  if (WSAEventSelect(m_connFD, m_connEvent,
      FD_READ | FD_WRITE | FD_OOB | FD_CLOSE)) {
    ZeLOG(Fatal, ([e = WSAGetLastError()](auto &s) {
      s << "WSAEventSelect() failed: " << e;
    }));
    close_fds();
    return false;
  }

#endif

  return true;
}

void Store::stop(StopFn fn)
{
  ZeLOG(Debug, ([](auto &s) { }));
  enqueue(Work::Stop{ZuMv(fn)});
}

void Store::stop_(StopFn fn)	// called after dequeuing Stop
{
  ZeLOG(Debug, ([](auto &s) { }));
  m_mx->wakeFn(m_pqSID, ZmFn{});
  ZeLOG(Debug, ([](auto &s) { s << "pushing stop_1()"; }));
  m_mx->push(m_pqSID, [this, fn = ZuMv(fn)]() mutable {
    stop_1();
    zdbRun([fn = ZuMv(fn)]() { fn(StopResult{}); });
  });
  wake_();
}

void Store::stop_1()
{
  ZeLOG(Debug, ([](auto &s) { }));
  close_fds();
}

void Store::wake()
{
  ZeLOG(Debug, ([](auto &s) { }));
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
      ZeLOG(Debug, ([events, v](auto &s) { s << "epoll_wait() events=" << events << " v=" << v << " EPOLLIN=" << ZuBoxed(EPOLLIN).hex() << " EPOLLOUT=" << ZuBoxed(EPOLLOUT).hex(); }));
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
	}
      }
    }
  } while (consumed);
}

void Store::rcvd(Work::Queue::Node *work, PGresult *res)
{
  ZeLOG(Debug, ([res, n = (res ? int(PQntuples(res)) : 0)](auto &s) {
    s << "res=" << ZuBoxPtr(res).hex() << " n=" << n;
  }));

  switch (work->data().type()) {
    case Work::Item::Index<Work::TblItem>{}: {
      const auto &tblItem = work->data().p<Work::TblItem>();
      switch (tblItem.query.type()) {
	case Work::Query::Index<Work::Open>{}:
	  tblItem.tbl->open_rcvd(res);
	  break;
      }
    } break;
  }
#if 0
  for (i = 0; i < PQntuples(res); i++) {
    j = 0; /* field number, also returned from PQfnumber(res, "id") */
    char *ptr = PQgetvalue(res, i, j);
    /* decode data */
  }
#endif
}

void Store::failed(Work::Queue::Node *work, ZeMEvent e)
{
  ZeLOG(Debug, ([](auto &s) { }));

  switch (work->data().type()) {
    case Work::Item::Index<Work::TblItem>{}: {
      const auto &tblItem = work->data().p<Work::TblItem>();
      switch (tblItem.query.type()) {
	case Work::Query::Index<Work::Open>{}:
	  tblItem.tbl->open_failed(ZuMv(e));
	  break;
      }
    } break;
  }
}

#if 0
// prepare statement
if (!PQsendPrepare(PGconn *conn,
  const char *stmtID,
  const char *query,
  int nParams,
  const Oid *paramTypes)) {	// Oid is unsigned int
  // failed
}
#endif

// send() is called after every enqueue to prevent starvation:
// wake(), enqueue(), dequeue(), send() (possible pushback), epoll_wait / WFMO

// to match results to requests, each result is matched to the head request
// on the sent request list, which is removed when the last tuple has
// been received

void Store::send()
{
  ZeLOG(Debug, ([](auto &s) { }));

  int sendState = SendState::Unsent;

  // queue items are non-query work such as Stop, or a query to be sent
  while (auto work = m_queue.shift()) {
    switch (work->data().type()) {
      case Work::Item::Index<Work::Stop>{}:
	stop_(ZuMv(ZuMv(work->data()).p<Work::Stop>()).stopped);
	break;
      case Work::Item::Index<Work::TblItem>{}: {
	const auto &tblItem = work->data().p<Work::TblItem>();
	switch (tblItem.query.type()) {
	  case Work::Query::Index<Work::Open>{}:
	    sendState = tblItem.tbl->open_send();
	    break;
	}
	if (sendState != SendState::Unsent)
	  m_sent.pushNode(ZuMv(work).release());
	else
	  m_queue.unshiftNode(ZuMv(work).release());
      } break;
    }
    if (sendState != SendState::Unsent) break;
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

void Store::open(
  ZuID id,
  ZtMFields fields,
  ZtMKeyFields keyFields,
  const reflection::Schema *schema,
  MaxFn maxFn,
  OpenFn openFn)
{
  ZeLOG(Debug, ([](auto &s) { }));
#if 0
  openFn(OpenResult{ZeMEVENT(Error, ([id](auto &s, const auto &) {
    s << "open(" << id << ") failed";
  }))});
#endif
  auto storeTbl =
    new StoreTbls::Node{this, id, ZuMv(fields), ZuMv(keyFields), schema};
  m_storeTbls->addNode(storeTbl);
  storeTbl->open(ZuMv(maxFn), ZuMv(openFn));
}

void Store::enqueue(Work::Item item)
{
  ZeLOG(Debug, ([](auto &s) { }));
  pqRun([this, item = ZuMv(item)]() mutable {
    ZeLOG(Debug, ([](auto &s) { s << "inner"; }));
    m_queue.push(ZuMv(item));
  });
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

// need to figure out prepared statements,
// prepared result processing, etc.

void StoreTbl::open(MaxFn maxFn, OpenFn openFn)
{
  ZeLOG(Debug, ([](auto &s) { }));
  m_maxFn = ZuMv(maxFn);
  m_openFn = ZuMv(openFn);
  mkTable();
}

  // max - one for each maxima
  // find - one for each keyID
  // recover - one
  // insert into X values (...)
  // update  ...
  // delete where ...
  // FIXME - storeTbl->open(...);
  // maxFn and openFn calls are deferred / async
  //
  // call storeTbl->open(ZuMv(openFn));
  //

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
  ZeLOG(Debug, ([query = ZtString{query}, n](auto &s) {
    s << "\"" << query << "\", n=" << n;
  }));
  int r = PQsendQueryParams(m_conn, query.data(),
    n, paramTypes, paramValues, paramLengths, paramFormats, 1);
  if (r != 1) return SendState::Unsent;
  if (PQsetSingleRowMode(m_conn) != 1)
    ZeLOG(Error, ([e = connError(m_conn)](auto &s) {
      s << "PQsetSingleRowMode() failed: " << e;
    }));
  return State;
}

template <int State>
int Store::sendPrepared(const ZtString &query, const Tuple &params)
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
  ZeLOG(Debug, ([query = ZtString{query}, n](auto &s) {
    s << "\"" << query << "\", n=" << n;
  }));
  int r = PQsendQueryPrepared(m_conn, query.data(),
    n, paramValues, paramLengths, paramFormats, 1);
  if (r != 1) return SendState::Unsent;
  if (PQsetSingleRowMode(m_conn) != 1)
    ZeLOG(Error, ([e = connError(m_conn)](auto &s) {
      s << "PQsetSingleRowMode() failed: " << e;
    }));
  return State;
}


// FIXME - consider pipelined OID resolution - this would now be quite
// straightforward

// FIXME - returned PGresult decoding via Value, note that OID returns
// are not supported by Value (32bit unsigned) - uses UInt32 defined
// at top of this file

void StoreTbl::open_()
{
  m_store->enqueue(Work::Item{Work::TblItem{this, {Work::Open{}}}});
}

int StoreTbl::open_send()
{
  switch (m_openState) {
    case OpenState::MkTable:	return mkTable_send();
    case OpenState::MkIndices:	return mkIndices_send();
  }
  return false;
}

void StoreTbl::open_rcvd(PGresult *res)
{
  switch (m_openState) {
    case OpenState::MkTable:	mkTable_rcvd(res); break;
    case OpenState::MkIndices:	mkIndices_rcvd(res); break;
  }
}

void StoreTbl::open_failed(Event e)
{
  ZeLOG(Debug, ([](auto &s) { }));

  m_openState = OpenState::Failed;
  m_openSubState = -1;

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

  m_openState = OpenState::Opened;
  m_openSubState = -1;

  auto openFn = ZuMv(m_openFn);

  m_maxFn = MaxFn{};
  m_openFn = OpenFn{};

  m_store->zdbRun([this, openFn = ZuMv(openFn)]() {
    openFn(OpenResult{OpenData{
      .storeTbl = this,
      .count = 0,
      .un = 0,
      .sn = 0
    }});
  });
}

void StoreTbl::mkTable()
{
  m_openState = OpenState::MkTable;
  m_openSubState = OpenSubState::reset();
  open_();
}
int StoreTbl::mkTable_send()
{
  ZeLOG(Debug, ([v = m_openSubState](auto &s) { s << ZuBoxed(v).hex(); }));
  if (!OpenSubState::create(m_openSubState)) {
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
    query << "CREATE TABLE \"" << m_id_ << "\" (";
    // FIXME - UN, SN
    unsigned n = m_fields.length();
    for (unsigned i = 0; i < n; i++) {
      if (i) query << ", ";
      query << '"' << m_xFields[i].id_ << "\" "
	<< m_store->oids().name(m_xFields[i].type);
    }
    query << ")";
    return m_store->sendQuery<SendState::Sync>(query, Tuple{});
  }
}
void StoreTbl::mkTable_rcvd(PGresult *res)
{
  if (OpenSubState::create(m_openSubState)) {
    if (!res) mkIndices();
    return;
  }

  if (!res) {
    if (!OpenSubState::failed(m_openSubState) &&
	OpenSubState::field(m_openSubState) == m_fields.length()) {
      // table exists, all fields ok, proceed to indices
      mkIndices();
    } else if (!OpenSubState::failed(m_openSubState) &&
	!OpenSubState::field(m_openSubState)) {
      // table does not exist, create it
      m_openSubState = OpenSubState::setCreate(m_openSubState);
      open_();
    } else {
      // table exists but not all fields matched
      auto e = ZeMEVENT(Fatal, ([id = this->m_id_](auto &s, const auto &) {
	s << "inconsistent schema for table " << id;
      }));
      open_failed(ZuMv(e));
    }
    return;
  }
  unsigned n = PQntuples(res);
  for (unsigned i = 0; i < n; i++) {
    const char *id = PQgetvalue(res, i, 0);
    unsigned oid = reinterpret_cast<UInt32 *>(PQgetvalue(res, i, 1))->i;
    // FIXME - UN, SN
    auto field = m_fieldMap.findVal(id);
    bool match = false;
    if (!ZuCmp<unsigned>::null(field)) {
      m_openSubState = OpenSubState::incField(m_openSubState);
      match = m_store->oids().match(oid, m_xFields[field].type);
    }
    if (!OpenSubState::failed(m_openSubState) && !match)
      m_openSubState = OpenSubState::setFailed(m_openSubState);
    ZeLOG(Debug, ([
      id = ZtString{id}, oid, field, match, oss = m_openSubState
    ](auto &s) {
      int field_ = ZuCmp<unsigned>::null(field) ? -1 : int(field);
      s << "id=" << id << " oid=" << oid
	<< " field=" << field_ << " match=" << (match ? 'T' : 'F')
	<< " openSubState=" << ZuBoxed(oss).hex();
    }));
  }
}

void StoreTbl::mkIndices()
{
  if (!m_keyFields.length()) { // FIXME - will never be true due to UN index
    opened();
    return;
  }
  m_openState = OpenState::MkIndices;
  m_openSubState = OpenSubState::reset();
  open_();
}
int StoreTbl::mkIndices_send()
{
  ZeLOG(Debug, ([v = m_openSubState](auto &s) { s << ZuBoxed(v).hex(); }));
  unsigned key = OpenSubState::key(m_openSubState);
  ZtString name(m_id_.length() + 16);
  name << m_id_ << '_' << key;
  if (!OpenSubState::create(m_openSubState)) {
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
    const auto &fields = m_keyFields[key];
    const auto &xFields = m_xKeyFields[key];
    unsigned n = fields.length();
    // LATER we could consider using hash indices for non-series
    query << "CREATE INDEX \"" << name << "\" ON \"" << m_id_ << "\" (";
    for (unsigned i = 0; i < n; i++) {
      if (i) query << ", ";
      query << '"' << xFields[i].id_ << '"';
    }
    query << ")";
    return m_store->sendQuery<SendState::Sync>(query, Tuple{});
  }
}
void StoreTbl::mkIndices_rcvd(PGresult *res)
{
  auto nextKey = [this]() {
    m_openSubState = OpenSubState::incKey(m_openSubState);
    // FIXME - add 1 for UN index
    if (OpenSubState::key(m_openSubState) >= m_keyFields.length())
      // prepFind();
      opened();
    else
      open_();
  };

  if (OpenSubState::create(m_openSubState)) {
    if (!res) nextKey();
    return;
  }

  if (!res) {
    const auto &fields = m_keyFields[OpenSubState::key(m_openSubState)];
    if (!OpenSubState::failed(m_openSubState) &&
	OpenSubState::field(m_openSubState) == fields.length()) {
      // index exists, all fields ok, proceed to next index
      nextKey();
    } else if (!OpenSubState::failed(m_openSubState) &&
	!OpenSubState::field(m_openSubState)) {
      // index does not exist, create it
      m_openSubState = OpenSubState::setCreate(m_openSubState);
      open_();
    } else {
      // index exists but not all fields matched
      auto e = ZeMEVENT(Fatal, ([id = this->m_id_](auto &s, const auto &) {
	s << "inconsistent schema for table " << id;
      }));
      open_failed(ZuMv(e));
    }
    return;
  }

  unsigned n = PQntuples(res);
  for (unsigned i = 0; i < n; i++) {
    const char *id = PQgetvalue(res, i, 0);
    unsigned oid = reinterpret_cast<UInt32 *>(PQgetvalue(res, i, 1))->i;
    // FIXME - handle UN index specially
    auto key = OpenSubState::key(m_openSubState);
    // const auto &fields = m_keyFields[key];
    const auto &xFields = m_xKeyFields[key];
    auto field = OpenSubState::field(m_openSubState);
    m_openSubState = OpenSubState::incField(m_openSubState);
    bool match = m_store->oids().match(oid, xFields[field].type);
    if (!OpenSubState::failed(m_openSubState) && !match)
      m_openSubState = OpenSubState::setFailed(m_openSubState);
    ZeLOG(Debug, ([
      id = ZtString{id}, oid, field, match, oss = m_openSubState
    ](auto &s) {
      int field_ = ZuCmp<unsigned>::null(field) ? -1 : int(field);
      s << "id=" << id << " oid=" << oid
	<< " field=" << field_ << " match=" << (match ? 'T' : 'F')
	<< " openSubState=" << ZuBoxed(oss).hex();
    }));
  }
}

// FIXME - sample maxima query:
/*
SET enable_seqscan = off;

-- Your SELECT query
SELECT link, MAX(seqNo) AS max_seqNo
FROM your_table_name
GROUP BY link;

-- Restore the original setting
RESET enable_seqscan;
*/

void StoreTbl::close(CloseFn fn)
{
  fn();
}

void StoreTbl::warmup() { }

void StoreTbl::maxima(MaxFn maxFn) { }

void StoreTbl::find(unsigned keyID, ZmRef<const AnyBuf> buf, RowFn rowFn) { }

void StoreTbl::recover(UN un, RowFn rowFn) { }

void StoreTbl::write(ZmRef<const AnyBuf> buf, CommitFn commitFn) { }

} // ZdbPQ

Zdb_::Store *ZdbStore()
{
  return new ZdbPQ::Store{};
}
