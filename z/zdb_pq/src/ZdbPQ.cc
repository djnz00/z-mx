//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

#include <zlib/ZdbPQ.hh>

// #include <postgresql/server/catalog/pg_type_d.h>

namespace ZdbPQ { namespace PQStore {

#pragma pack(push, 1)
struct RxBool { uint8_t i; };
struct RxUInt8 { uint8_t i; };
struct RxInt8 { int8_t i; };
struct RxUInt16 { ZuBigEndian<uint16_t> i; };
struct RxInt16 { ZuBigEndian<int16_t> i; };
struct RxUInt32 { ZuBigEndian<uint32_t> i; };
struct RxInt32 { ZuBigEndian<int32_t> i; };
struct RxUInt64 { ZuBigEndian<uint64_t> i; };
struct RxInt64 { ZuBigEndian<int64_t> i; };
struct RxUInt128 { ZuBigEndian<uint128_t> i; };
struct RxInt128 { ZuBigEndian<int128_t> i; };
#pragma pack(pop)

InitResult Store::init(ZvCf *cf, LogFn) {
  // auto connection = cf->get<true>("connection");

  m_conn = PQconnectdb("test"/* connection */);

  if (!m_conn || PQstatus(m_conn) != CONNECTION_OK) {
    ZeLOG(Error, ([error = ZtString{PQerrorMessage(m_conn)}](auto &s) {
      s << "PQconnectdb() failed: " << error;
    }));
    if (m_conn) {
      PQfinish(m_conn);
      m_conn = nullptr;
    }
    return;
  }

  {
    Oid paramTypes[1] = { 25 };	// TEXTOID
    const char *paramValues[1] = { "bool" };
    int paramLengths[1] = { 4 };
    int paramFormats[1] = { 1 };
    PGresult *res = PQexecParams(m_conn,
      "SELECT oid FROM pg_type WHERE typname = $1::text",
      1, paramTypes, paramValues, paramLengths, paramFormats, 1);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
      // failed
      PQclear(res);
      PQfinish(m_conn);
      m_conn = nullptr;
    }
    assert(PQnfields(res) == 1); // columns
    assert(PQntuples(res) == 1); // rows
    assert(PQgetlength(res, 0, 0) == 4);
    uint32_t oid = reinterpret_cast<RxUInt32 *>(PQgetvalue(res, 0, 0))->i;
    ZeLOG(Debug, ([oid](auto &s) { s << "oid=" << oid; }));
  }

#if 0
  if (PQsetnonblocking(m_conn, 1) != 0) {
    // failed
  }
  if (PQenterPipelineMode(m_conn) != 1) {
    // failed
  }
#endif

  return {InitData{.replicated = true}};
}

void Store::final()
{
  if (m_conn) PQfinish(m_conn);
}

void Store::open(
  ZuID id,
  ZtMFields fields,
  ZtMKeyFields keyFields,
  const reflection::Schema *schema,
  MaxFn maxFn,
  OpenFn openFn)
{
}

} // PQStore

namespace PQStoreTbl {

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

} // ZdbPQ
