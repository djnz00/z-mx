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

void OIDs::init(PGconn *conn) {
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

  for (unsigned i = 1; i < Value::N; i++) {
    auto name = names[i - 1];
    auto oid = this->oid(name);
    if (oid < 0) oid = resolve(conn, name);
    m_map.add(name, int8_t(i));
    m_values[i - 1] = oid;
  }
}

unsigned OIDs::resolve(PGconn *conn, ZuString name) {
  Oid paramTypes[1] = { 25 };	// TEXTOID
  const char *paramValues[1] = { name.data() };
  int paramLengths[1] = { int(name.length()) };
  int paramFormats[1] = { 1 };
  const char *query = "SELECT oid FROM pg_type WHERE typname = $1::text";
  PGresult *res = PQexecParams(conn, query,
    1, paramTypes, paramValues, paramLengths, paramFormats, 1);
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    // failed
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
  unsigned oid = reinterpret_cast<UInt32 *>(PQgetvalue(res, 0, 0))->i;
  ZeLOG(Debug, ([name, oid](auto &s) {
    s << "OID::resolve(\"" << name << "\")=" << oid;
  }));
  return oid;
}

InitResult Store::init(ZvCf *cf, ZiMultiplex *mx, unsigned sid) {
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

StartResult Store::start()
{
  ZeLOG(Debug, ([](auto &s) { }));
  m_mx->wakeFn(m_pqSID, ZmFn{this, [](Store *store) { store->wake(); }});
  ZmBlock<>{}([this](auto done) {
    ZeLOG(Debug, ([](auto &s) { s << "pushing start_(), run_()"; }));
    m_mx->push(m_pqSID, [this, done = ZuMv(done)]() mutable {
      ZeLOG(Debug, ([](auto &s) { s << "inner"; }));
      start_();
      done();
      run_();
    });
  });
  if (ZuUnlikely(!m_conn))
    return {ZeMEVENT(Fatal, "PostgreSQL start() failed")};
  return {};
}

void Store::start_()
{
  ZeLOG(Debug, ([](auto &s) { }));

  const auto &connection = m_cf->get<true>("connection");

  m_conn = PQconnectdb(connection);

  if (!m_conn || PQstatus(m_conn) != CONNECTION_OK) {
    ZtString error = PQerrorMessage(m_conn);
    ZeLOG(Fatal, ([error = ZuMv(error)](auto &s) {
      s << "PQconnectdb() failed: " << error;
    }));
    close_fds();
    return;
  }

  try {
    m_oids.init(m_conn);
  } catch (const ZeMEvent &e) {
    ZeLogEvent(ZuMv(const_cast<ZeMEvent &>(e)));
    close_fds();
    return;
  }

  m_connFD = PQsocket(m_conn);

  if (PQsetnonblocking(m_conn, 1) != 0) {
    ZtString e = PQerrorMessage(m_conn);
    ZeLOG(Fatal, ([e = ZuMv(e)](auto &s) {
      s << "PQsetnonblocking() failed: " << e;
    }));
    close_fds();
    return;
  }

  if (PQenterPipelineMode(m_conn) != 1) {
    ZtString e = PQerrorMessage(m_conn);
    ZeLOG(Fatal, ([e = ZuMv(e)](auto &s) {
      s << "PQenterPipelineMode() failed: " << e;
    }));
    close_fds();
    return;
  }

#ifndef _WIN32

  // set up I/O multiplexer (epoll)
  if ((m_epollFD = epoll_create(2)) < 0) {
    ZeLOG(Fatal, ([e = ZeLastError](auto &s) {
      s << "epoll_create() failed: " << e;
    }));
    close_fds();
    return;
  }
  if (pipe(&m_wakeFD) < 0) {
    ZeLOG(Fatal, ([e = errno](auto &s) {
      s << "pipe() failed: " << e;
    }));
    close_fds();
    return;
  }
  if (fcntl(m_wakeFD, F_SETFL, O_NONBLOCK) < 0) {
    ZeLOG(Fatal, ([e = errno](auto &s) {
      s << "fcntl(F_SETFL, O_NONBLOCK) failed: " << e;
    }));
    close_fds();
    return;
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
      return;
    }
  }

  ZeLOG(Debug, ([this](auto &s) { s << "epoll_ctl(EPOLL_CTL_ADD) connFD=" << m_connFD; }));
  {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(struct epoll_event));
    ev.events = EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR | EPOLLET;
    ev.data.u64 = 1;
    epoll_ctl(m_epollFD, EPOLL_CTL_ADD, m_connFD, &ev);
  }

#else

  m_wakeSem = CreateSemaphore(nullptr, 0, 0x7fffffff, nullptr); }
  if (m_wakeSem == NULL || m_wakeSem == INVALID_HANDLE_VALUE) {
    ZeLOG(Fatal, ([e = ZeLastError](auto &s) {
      s << "CreateEvent() failed: " << e;
    }));
    close_fds();
    return;
  }

  m_connEvent = WSACreateEvent();
  if (m_connEvent == NULL || m_connEvent == INVALID_HANDLE_VALUE) {
    ZeLOG(Fatal, ([e = ZeLastError](auto &s) {
      s << "CreateEvent() failed: " << e;
    }));
    close_fds();
    return;
  }
  if (WSAEventSelect(m_connFD, m_connEvent,
      FD_READ | FD_WRITE | FD_OOB | FD_CLOSE)) {
    ZeLOG(Fatal, ([e = WSAGetLastError()](auto &s) {
      s << "WSAEventSelect() failed: " << e;
    }));
    close_fds();
    return;
  }

#endif
}

void Store::stop()
{
  ZeLOG(Debug, ([](auto &s) { }));
  ZmBlock<>{}([this](auto done) {
    enqueue(Work::Foo{});
    enqueue(Work::Foo{});
    enqueue(Work::Foo{});
    enqueue(Work::Stop{ZmFn{[done = ZuMv(done)]() mutable {
      done();
    }}});
  });
}

void Store::stop_(ZmFn<> stopped)	// called after dequeuing Stop
{
  ZeLOG(Debug, ([](auto &s) { }));
  m_mx->wakeFn(m_pqSID, ZmFn{});
  ZeLOG(Debug, ([](auto &s) { s << "pushing stop_1()"; }));
  m_mx->push(m_pqSID, [this, stopped = ZuMv(stopped)]() mutable {
    stop_1();
    stopped();
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
  write();

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
	read();
      if (events & EPOLLOUT)
	write();
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
	read();
      if ((events.lNetworkEvents & (FD_WRITE|FD_CLOSE)) == FD_WRITE)
	write();
    }

#endif

  }
}

void Store::read() {
  ZeLOG(Debug, ([](auto &s) { }));
  bool consumed;
  do {
    consumed = false;
    if (!PQconsumeInput(m_conn)) {
      ZtString e = PQerrorMessage(m_conn);
      ZeLOG(Fatal, ([e = ZuMv(e)](auto &s) {
	s << "PQconsumeInput() failed: " << e;
      }));
      return;
    }
    while (!PQisBusy(m_conn)) {
      PGresult *res = PQgetResult(m_conn);
      if (!res) break;
      consumed = true;
      auto pending = m_pending.headNode();
      ZmAssert(pending);
      do {
	switch (PQresultStatus(res)) { // ExecStatusType
	  case PGRES_COMMAND_OK: // query succeeded - no tuples
	    parse(pending, nullptr);
	    break;
	  case PGRES_TUPLES_OK: // query succeeded - N tuples
	    parse(pending, res);
	    break;
	  case PGRES_SINGLE_TUPLE: // query succeeded - 1 of N tuples
	    parse(pending, res);
	    break;
	  case PGRES_NONFATAL_ERROR: // notice / warning
	    break;
	  case PGRES_FATAL_ERROR: // query failed
	    break;
	  default: // can ignore everything else
	    break;
	}
	PQclear(res);
	res = PQgetResult(m_conn);
      } while (res);
      m_pending.shift();
    }
  } while (consumed);
}

void Store::parse(Work::Queue::Node *work, PGresult *res)
{
  switch (work->data().type()) {
    case Work::Item::Index<Work::TblItem>{}: {
      const auto &tblItem = work->data().p<Work::TblItem>();
      switch (tblItem.query.type()) {
	case Work::Query::Index<Work::GetTable>{}:
	  tblItem.tbl->getTable2(res);
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

// Note: write() is called after every enqueue to ensure no starvation:
// wake() -> enqueue() -> dequeue() -> write() (possible pushback) -> epoll_wait / WFMO

// to match results to requests, each result is matched to the head request
// on the pending request list, which is removed when the last tuple has
// been received

void Store::write() {
  ZeLOG(Debug, ([](auto &s) { }));

  bool flush = false;

  while (auto work = m_queue.shift()) {
    switch (work->data().type()) {
      case Work::Item::Index<Work::Foo>{}:
	ZeLOG(Debug, "Foo!");
	break;
      case Work::Item::Index<Work::Stop>{}:
	stop_(ZuMv(ZuMv(work->data()).p<Work::Stop>()).stopped);
	break;
      case Work::Item::Index<Work::TblItem>{}: {
	flush = true;
	const auto &tblItem = work->data().p<Work::TblItem>();
	switch (tblItem.query.type()) {
	  case Work::Query::Index<Work::GetTable>{}:
	    if (tblItem.tbl->getTable(m_conn))
	      m_pending.pushNode(ZuMv(work).release());
	    else
	      m_queue.unshiftNode(ZuMv(work).release());
	    break;
	}
      } break;
    }
    if (flush) break;
  }

  if (flush) {
    if (PQsendFlushRequest(m_conn) != 1) {
      ZtString e = PQerrorMessage(m_conn);
      ZeLOG(Fatal, ([e = ZuMv(e)](auto &s) {
	s << "PQsendFlushRequest() failed: " << e;
      }));
      return;
    }
  }
  if (PQflush(m_conn) < 0) {
    ZtString e = PQerrorMessage(m_conn);
    ZeLOG(Fatal, ([e = ZuMv(e)](auto &s) {
      s << "PQflush() failed: " << e;
    }));
    return;
  }

#if 0
  // dequeue query, make tuple, send it with query
  const char *paramValues[N];
  int paramLengths[N];
  int paramFormats[N];
  paramValues[0] = ptr; // pointer to binary representation
  paramLengths[0] = sizeof(T);
  paramFormats[0] = 1; // binary
  if (PQsendQueryPrepared(conn,
	id,	// server-side ID (name) for previously prepared statement
	nParams,
	paramValues,
	paramLengths,
	paramFormats,
	1	// binary
      ) == 1) {
    // sent - enqueue on pending requests
  } else {
    // error is ok in non-blocking mode, push it back onto the queue for retry
  }
#endif
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
  openFn(OpenResult{ZeMEVENT(Error, ([id](auto &s) {
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
  m_mx->run(m_pqSID, [this, item = ZuMv(item)]() mutable {
    ZeLOG(Debug, ([](auto &s) { s << "inner"; }));
    m_queue.push(ZuMv(item));
  });
}

// resolve Value union discriminator from field metadata
static FBField fbField(
  const Zfb::Vector<Zfb::Offset<reflection::Field>> *fbFields_,
  const ZtMField *field,
  const ZtString &id)
{
  // resolve flatbuffers reflection data for field
  const reflection::Field *fbField = fbFields_->LookupByKey(id);
  if (!fbField) return {nullptr, 0};
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
  return {fbField, type};
}

StoreTbl::StoreTbl(
  Store *store, ZuID id, ZtMFields fields, ZtMKeyFields keyFields,
  const reflection::Schema *schema) :
  m_store{store}, m_id{id},
  m_fields{ZuMv(fields)}, m_keyFields{ZuMv(keyFields)}
{
  const reflection::Object *rootTbl = schema->root_table();
  const Zfb::Vector<Zfb::Offset<reflection::Field>> *fbFields_ =
    rootTbl->fields();
  unsigned n = m_fields.length();
  m_fbFields.size(n);
  for (unsigned i = 0; i < n; i++)
    ZtCase::camelSnake(
      m_fields[i]->id,
      [this, fbFields_, i](const ZtString &id) {
      m_fbFields.push(fbField(fbFields_, m_fields[i], id));
    });
  n = m_keyFields.length();
  m_fbKeyFields.size(n);
  for (unsigned i = 0; i < n; i++) {
    unsigned m = m_keyFields[i].length();
    new (m_fbKeyFields.push()) FBFields{m};
    for (unsigned j = 0; j < m; j++)
      ZtCase::camelSnake(
	m_keyFields[i][j]->id,
	[this, fbFields_, i, j](const ZtString &id) {
	m_fbKeyFields[i].push(fbField(fbFields_, m_keyFields[i][j], id));
      });
  }
  m_maxBuf = new AnyBuf{};
}

StoreTbl::~StoreTbl()
{
}

// need to figure out prepared statements,
// prepared result processing, etc.

void StoreTbl::open(MaxFn maxFn, OpenFn openFn) {
  ZeLOG(Debug, ([](auto &s) { }));
  m_maxFn = ZuMv(maxFn);
  m_openFn = ZuMv(openFn);
 
  m_store->enqueue(Work::Item{Work::TblItem{this, {Work::GetTable{}}}});
  // opened();
}

  // max - one for each maxima
  // find - one for each keyID
  // recover - one
  // insert into X values (...)
  // update  ...
  // delete where ...
  // FIXME - storeTbl->open(...);
  // maxFn and openFn calls will be deferred / async
  //
  // call storeTbl->open(ZuMv(openFn));
  //

bool StoreTbl::getTable(PGconn *conn)
{
#if 1
  ZeLOG(Debug, ([](auto &s) { }));
  Oid paramTypes[1] = { 25 };	// TEXTOID
  const char *paramValues[1] = { m_id.data() };
  int paramLengths[1] = { int(m_id.length()) };
  int paramFormats[1] = { 1 };
  const char *query =
    "SELECT a.attname AS name, a.atttypid AS oid "
    "FROM pg_catalog.pg_attribute a "
    "JOIN pg_catalog.pg_class c ON a.attrelid = c.oid "
    "JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid "
    "WHERE c.relname = $1::text "
      "AND n.nspname = 'public' "
      "AND a.attnum > 0 "
      "AND NOT a.attisdropped";
  int r = PQsendQueryParams(conn, query,
    1, paramTypes, paramValues, paramLengths, paramFormats, 1);
  ZeLOG(Debug, ([r](auto &s) { s << "PQsendQueryParams(): " << r; }));
  return r == 1;
#else
  m_store->zdbRun([this]() { opened(); });
  return true;
#endif
}

void StoreTbl::getTable2(PGresult *res)
{
  ZeLOG(Debug, ([res](auto &s) { s << ZuBoxPtr(res).hex(); }));
  if (ZuUnlikely(!res)) return;
  for (unsigned i = 0; i < PQntuples(res); i++) {
    const char *id = PQgetvalue(res, i, 0);
    unsigned oid = reinterpret_cast<UInt32 *>(PQgetvalue(res, i, 1))->i;
    ZeLOG(Debug, ([id = ZtString{id}, oid](auto &s) {
      s << "id=" << id << " oid=" << oid;
    }));
  }
  m_store->zdbRun([this]() { opened(); });
}

void StoreTbl::opened()
{
  ZeLOG(Debug, ([](auto &s) { }));
  m_openFn(OpenResult{OpenData{
    .storeTbl = this,
    .count = 0,
    .un = 0,
    .sn = 0
  }});
  m_maxFn = MaxFn{};
  m_openFn = OpenFn{};
}

void StoreTbl::close(CloseFn fn)
{
  // FIXME
  // - no need to deal with close() during open()
  // - Zdb ensures that does not occur
  fn();
}

void StoreTbl::warmup() { }

void StoreTbl::drop() { }

void StoreTbl::maxima(MaxFn maxFn) { }

void StoreTbl::find(unsigned keyID, ZmRef<const AnyBuf> buf, RowFn rowFn) { }

void StoreTbl::recover(UN un, RowFn rowFn) { }

void StoreTbl::write(ZmRef<const AnyBuf> buf, CommitFn commitFn) { }

} // ZdbPQ

Zdb_::Store *ZdbStore()
{
  return new ZdbPQ::Store{};
}
