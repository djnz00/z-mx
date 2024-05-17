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

uint32_t OIDs::resolve(PGconn *conn, const char *name) {
  Oid paramTypes[1] = { 25 };	// TEXTOID
  const char *paramValues[1] = { name };
  int paramLengths[1] = { int(strlen(name)) };
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
  uint32_t oid = reinterpret_cast<UInt32 *>(PQgetvalue(res, 0, 0))->i;
#if 0
  ZeLOG(Debug, ([name, oid](auto &s) {
    s << "OID::resolve(\"" << name << "\")=" << oid;
  }));
#endif
  return oid;
}

namespace PQStore {

InitResult Store::init(ZvCf *cf, ZiMultiplex *mx, unsigned sid) {
  m_cf = cf;
  m_mx = mx;
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
    m_sid = sid;
  } catch (const ZvError &e_) {
    ZtString e;
    e << e_;
    return {ZeMEVENT(Fatal, ([e = ZuMv(e)](auto &s, const auto &) {
      s << "Store::init() failed: invalid thread configuration: " << e;
    }))};
  }

  ZmBlock<>{}([this](auto done) {
    m_mx->run(m_sid, [this, done = ZuMv(done)]() mutable {
      init_();
      done();
    });
  });

  if (m_error) {
    Event e = ZuMv(m_error);
    m_error = Event{};
    return {ZuMv(e)};
  }

  return {InitData{.replicated = true}};
}

void Store::close_fds()
{
#ifndef _WIN32

  // close I/O multiplexer
  if (m_epollFD >= 0) {
    if (m_wakeFD >= 0)
      epoll_ctl(m_epollFD, EPOLL_CTL_DEL, m_wakeFD, 0);
    if (m_socket >= 0)
      epoll_ctl(m_epollFD, EPOLL_CTL_DEL, m_socket, 0);
    ::close(m_epollFD);
    m_epollFD = -1;
  }
  if (m_wakeFD >= 0) { ::close(m_wakeFD); m_wakeFD = -1; }
  if (m_wakeFD2 >= 0) { ::close(m_wakeFD2); m_wakeFD2 = -1; }

#else /* !_WIN32 */

  // close wakeup event
  if (m_wakeEvent != INVALID_HANDLE_VALUE) {
    CloseHandle(m_wakeEvent);
    m_wakeEvent = INVALID_HANDLE_VALUE;
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
    m_socket = -1;
  }
}

void Store::init_()
{
  const auto &connection = m_cf->get<true>("connection");

  m_conn = PQconnectdb(connection);

  if (!m_conn || PQstatus(m_conn) != CONNECTION_OK) {
    ZtString error = PQerrorMessage(m_conn);
    m_error = ZeMEVENT(Fatal, ([error = ZuMv(error)](auto &s, const auto &) {
      s << "PQconnectdb() failed: " << error;
    }));
    close_fds();
    return;
  }

  try {
    m_oids.init(m_conn);
  } catch (const ZeMEvent &e) {
    m_error = ZuMv(const_cast<ZeMEvent &>(e));
    close_fds();
    return;
  }

  m_socket = PQsocket(m_conn);

  if (PQsetnonblocking(m_conn, 1) != 0) {
    ZtString e = PQerrorMessage(m_conn);
    m_error = ZeMEVENT(Fatal, ([e = ZuMv(e)](auto &s, const auto &) {
      s << "PQsetnonblocking() failed: " << e;
    }));
    close_fds();
    return;
  }

  if (PQenterPipelineMode(m_conn) != 1) {
    ZtString e = PQerrorMessage(m_conn);
    m_error = ZeMEVENT(Fatal, ([e = ZuMv(e)](auto &s, const auto &) {
      s << "PQenterPipelineMode() failed: " << e;
    }));
    close_fds();
    return;
  }

#ifndef _WIN32

  // set up I/O multiplexer (epoll)
  if ((m_epollFD = epoll_create(2)) < 0) {
    m_error = ZeMEVENT(Fatal, ([e = ZeLastError](auto &s, const auto &) {
      s << "epoll_create() failed: " << e;
    }));
    close_fds();
    return;
  }
  if (pipe(&m_wakeFD) < 0) {
    m_error = ZeMEVENT(Fatal, ([e = errno](auto &s, const auto &) {
      s << "pipe() failed: " << e;
    }));
    close_fds();
    return;
  }
  if (fcntl(m_wakeFD, F_SETFL, O_NONBLOCK) < 0) {
    m_error = ZeMEVENT(Fatal, ([e = errno](auto &s, const auto &) {
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
      m_error = ZeMEVENT(Fatal, ([e = errno](auto &s, const auto &) {
	s << "epoll_ctl(EPOLL_CTL_ADD) failed: " << e;
      }));
      close_fds();
      return;
    }
  }

  {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(struct epoll_event));
    ev.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
    ev.data.u64 = 1;
    epoll_ctl(m_epollFD, EPOLL_CTL_ADD, m_socket, &ev);
  }

#else

  m_wakeEvent = CreateEvent(nullptr, true, false, L"Local\\ZdbPQ");
  if (m_wakeEvent == NULL || m_wakeEvent == INVALID_HANDLE_VALUE) {
    m_error = ZeMEVENT(Fatal, ([e = ZeLastError](auto &s, const auto &) {
      s << "CreateEvent() failed: " << e;
    }));
    close_fds();
    return;
  }

  m_connEvent = WSACreateEvent();
  if (m_connEvent == NULL || m_connEvent == INVALID_HANDLE_VALUE) {
    m_error = ZeMEVENT(Fatal, ([e = ZeLastError](auto &s, const auto &) {
      s << "CreateEvent() failed: " << e;
    }));
    close_fds();
    return;
  }
  if (WSAEventSelect(m_socket, m_connEvent,
      FD_READ | FD_WRITE | FD_OOB | FD_CLOSE)) {
    m_error = ZeMEVENT(Fatal, ([e = WSAGetLastError()](auto &s, const auto &) {
      s << "WSAEventSelect() failed: " << e;
    }));
    close_fds();
    return;
  }

#endif

  m_mx->wakeFn(m_sid, ZmFn{this, [](Store *store) { store->wake(); }});
  m_mx->push(m_sid, [this]() { run_(); });
}

void Store::final()
{
  ZmBlock<>{}([this](auto done) {
    m_mx->wakeFn(m_sid, ZmFn{});
    m_mx->push(m_sid, [this, done = ZuMv(done)]() mutable {
      final_();
      done();
    });
    wake_();
  });

  if (m_error) {
    Event e = ZuMv(m_error);
    m_error = Event{};
    ZeLogEvent(ZuMv(e));
  }
}

void Store::final_()
{
  close_fds();
}

void Store::wake()
{
  m_mx->push(m_sid, [this]{ run_(); });
  wake_();
}

void Store::wake_()
{
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
  if (!SetEvent(m_wake)) {
    ZeLOG(Fatal, ([e = ZeLastError](auto &s) {
      s << "SetEvent() failed: " << e;
    }));
  }
#endif /* !_WIN32 */
}

void Store::run_()
{
  for (;;) {

#ifdef _WIN32

    HANDLE handles[2] = { m_wakeEvent, m_connEvent };
    DWORD event = WaitForMultipleObjectsEx(2, handles, false, INFINITE, false);
    if (event == WAIT_FAILED) {
      ZeLOG(Fatal, ([e = ZeLastError](auto &s) {
	s << "WaitForMultipleObjectsEx() failed: " << e;
      }));
      return;
    }
    if (event == WAIT_OBJECT_0) return;
    if (event == WAIT_OBJECT_0 + 1) {
      WSANETWORKEVENTS events;
      auto i = WSAEnumNetworkEvents(m_socket, m_connEvent, &events);
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

#else

    epoll_event ev[8];
    int r = epoll_wait(m_epollFD, ev, 8, -1); // max events is 8
    if (r < 0) {
      ZeLOG(Fatal, ([e = errno](auto &s) {
	s << "epoll_wait() failed: " << e;
      }));
      return;
    }
    for (unsigned i = 0; i < unsigned(r); i++) {
      uint32_t events = ev[i].events;
      auto v = ev[i].data.u64; // ID
      if (!v) return;
      if (events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR))
	read();
      if (events & EPOLLOUT)
	write();
    }

#endif

  }
}

void Store::read() {
  bool consumed;
  do {
    consumed = false;
    if (!PQconsumeInput(m_conn)) {
      ZtString e = PQerrorMessage(m_conn);
      ZeLOG(Fatal, ([e = ZuMv(e)](auto &s, const auto &) {
	s << "PQconsumeInput() failed: " << e;
      }));
      return;
    }
    while (!PQisBusy(m_conn)) {
      PGresult *res = PQgetResult(m_conn);
      if (!res) break;
      consumed = true;
      do {
	switch (PQresultStatus(res)) { // ExecStatusType
	  case PGRES_COMMAND_OK: // query succeeded - no tuples
	    break;
	  case PGRES_TUPLES_OK: // query succeeded - N tuples
	    parse(res);
	    break;
	  case PGRES_SINGLE_TUPLE: // query succeeded - 1 of N tuples
	    parse(res);
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
      // FIXME res is null - dequeue next pending request and onto next one
    }
  } while (consumed);
}

void Store::parse(PGresult *res) {
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
  if (PQflush(m_conn) < 0) {
    ZtString e = PQerrorMessage(m_conn);
    ZeLOG(Fatal, ([e = ZuMv(e)](auto &s, const auto &) {
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
  openFn(OpenResult{ZeMEVENT(Error, ([id](auto &s, const auto &) {
    s << "open(" << id << ") failed";
  }))});
}

} // PQStore

namespace PQStoreTbl {

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
  ZuID id, ZtMFields fields, ZtMKeyFields keyFields,
  const reflection::Schema *schema) :
  m_id{id},
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

void StoreTbl::open() { }
void StoreTbl::close() { }

void StoreTbl::warmup() { }

void StoreTbl::drop() { }

void StoreTbl::maxima(MaxFn maxFn) { }

void StoreTbl::find(unsigned keyID, ZmRef<const AnyBuf> buf, RowFn rowFn) { }

void StoreTbl::recover(UN un, RowFn rowFn) { }

void StoreTbl::write(ZmRef<const AnyBuf> buf, CommitFn commitFn) { }

} // PQStoreTbl 

} // ZdbPQ

Zdb_::Store *ZdbStore()
{
  return new ZdbPQ::Store{};
}
