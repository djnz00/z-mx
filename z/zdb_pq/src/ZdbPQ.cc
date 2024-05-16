//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

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
    throw ZeMEVENT(Error, ([query, name](auto &s, const auto &) {
      s << "Store::init() \"" << query << "\" $1=\""
	<< name << "\" failed\n";
    }));
  }
  if (ZuUnlikely(
      PQnfields(res) != 1 ||
      PQntuples(res) != 1 ||
      PQgetlength(res, 0, 0) != 4))
    throw ZeMEVENT(Error, ([query, name](auto &s, const auto &) {
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

InitResult Store::init(ZvCf *cf, LogFn) {
  const auto &connection = cf->get<true>("connection");

  m_conn = PQconnectdb(connection);

  if (!m_conn || PQstatus(m_conn) != CONNECTION_OK) {
    ZtString error = PQerrorMessage(m_conn);
    if (m_conn) {
      PQfinish(m_conn);
      m_conn = nullptr;
    }
    return {ZeMEVENT(Error, ([error = ZuMv(error)](auto &s, const auto &) {
      s << "PQconnectdb() failed: " << error;
    }))};
  }

  try {
    m_oids.init(m_conn);
  } catch (const ZeMEvent &e) {
    return {ZuMv(const_cast<ZeMEvent &>(e))};
  }

  m_socket = PQsocket(m_conn);

  if (PQsetnonblocking(m_conn, 1) != 0) {
    // FIXME - failed
  }

  if (PQenterPipelineMode(m_conn) != 1) {
    // FIXME - failed
  }

  return {InitData{.replicated = true}};
}

void Store::final()
{
  if (m_conn) {
    PQfinish(m_conn);
    m_conn = nullptr;
  }
  m_socket = -1;
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
FBField fbField(
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
