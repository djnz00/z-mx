//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Zdb in-memory data store

#include <zlib/ZdbMemStore.hh>

Zdb_::Store *ZdbStore()
{
  return new ZdbMem::Store{};
}

namespace ZdbMem {

void StoreTbl::count(unsigned keyID, ZmRef<const IOBuf> buf, CountFn countFn)
{
  run([keyID, buf = ZuMv(buf), countFn = ZuMv(countFn)]() mutable {
    ZmAssert(keyID < m_indices.length());

    const auto &keyFields = m_keyFields[keyID];
    const auto &xKeyFields = m_xKeyFields[keyID];

    unsigned nParams = m_keyGroup[keyID];

    auto key = loadTuple_(
      nParams, keyFields, xKeyFields, Zfb::GetAnyRoot(buf->data()));

    const auto &index = m_indices[keyID];
    auto row = index.find<ZmRBTreeGreater>(key);
    uint64_t i = 0;
    while (row && equals_(row->key(), key, nParams)) {
      ++i;
      row = index.next(row);
    }
    countFn(CountData{i});
  });
}

void StoreTbl::select(
  bool selectRow, bool selectNext, bool inclusive,
  unsigned keyID, ZmRef<const IOBuf> buf,
  unsigned limit, TupleFn tupleFn)
{
  run([
    selectRow, selectNext, inclusive,
    keyID, buf = ZuMv(buf), limit, tupleFn = ZuMv(tupleFn)
  ]() mutable {
    ZmAssert(keyID < m_indices.length());

    const auto &keyFields = m_keyFields[keyID];
    const auto &xKeyFields = m_xKeyFields[keyID];

    unsigned keyGroup = m_keyGroup[keyID];
    unsigned nParams = selectNext ? keyFields.length() : keyGroup;

    auto key = loadTuple_(
      nParams, keyFields, xKeyFields, Zfb::GetAnyRoot(buf->data()));

    const auto &index = m_indices[keyID];
    auto row = inclusive ?
      index.find<ZmRBTreeGreaterEqual>(key) :
      index.find<ZmRBTreeGreater>(key);
    unsigned i = 0;
    while (i++ < limit && row && equals_(row->key(), key, keyGroup)) {
      Zfb::IOBuilder fbb{m_bufAllocFn()};
      if (!selectRow) {
	auto key = extractKey(m_fields, m_keyFields, keyID, row->val()->data);
	fbb.Finish(saveTuple(fbb, xKeyFields, key));
      } else {
	fbb.Finish(saveTuple(fbb, m_xFields, row->val()->data));
      }
      TupleData tupleData{
	.keyID = selectRow ? ZuFieldKeyID::All : int(keyID),
	.buf = fbb.buf().constRef(),
	.count = i
      };
      tupleFn(TupleResult{ZuMv(tupleData)});
      row = index.next(row);
    }
    tupleFn(TupleResult{});
  });
}

void StoreTbl::find(unsigned keyID, ZmRef<const IOBuf> buf, RowFn rowFn)
{
  run([keyID, buf = ZuMv(buf), rowFn = ZuMv(rowFn)]() mutable {
    ZmAssert(keyID < m_indices.length());

    auto key = loadTuple(
      m_keyFields[keyID], m_xKeyFields[keyID], Zfb::GetAnyRoot(buf->data()));
    ZmRef<const MemRow> row = m_indices[keyID].findVal(key);
    if (row) {
      RowData data{.buf = saveRow<false>(row).constRef()};
      rowFn(RowResult{ZuMv(data)});
    } else {
      rowFn(RowResult{});
    }
  });
}

void StoreTbl::recover(UN un, RowFn rowFn)
{
  run([un, rowFn = ZuMv(rowFn)]() mutable {
    // build Recover buf and return it
    ZmRef<const MemRow> row = m_indexUN.find(un);
    if (row) {
      RowData data{.buf = saveRow<true>(row).constRef()};
      rowFn(RowResult{ZuMv(data)});
    } else {
      // missing is not an error, skip over updated/deleted records
      rowFn(RowResult{});
    }
  });
}

void StoreTbl::write(ZmRef<const IOBuf> buf, CommitFn commitFn)
{
  run([buf = ZuMv(buf), commitFn = ZuMv(commitFn)]() mutable {
    // idempotence check
    auto un = record_(msg_(buf->hdr()))->un();
    if (m_maxUN != ZdbNullUN() && un <= m_maxUN) {
      commitFn(ZuMv(buf), CommitResult{});
      return;
    }
    // load row, perform insert/update/delete
    ZmRef<MemRow> row = loadRow(buf).mutableRef();
    if (!row->vn)
      insert(ZuMv(row), ZuMv(buf), ZuMv(commitFn));
    else if (row->vn > 0)
      update(ZuMv(row), ZuMv(buf), ZuMv(commitFn));
    else
      del(ZuMv(row), ZuMv(buf), ZuMv(commitFn));
  });
}

void StoreTbl::insert(
  ZmRef<MemRow> row, ZmRef<const IOBuf> buf, CommitFn commitFn)
{
  ZeLOG(Debug, ([](auto &s) { }));
  m_maxUN = row->un, m_maxSN = row->sn;
  unsigned n = m_keyFields.length();
  for (unsigned i = 0; i < n; i++) {
    auto key = extractKey(m_fields, m_keyFields, i, row->data);
    ZmAssert(key.length() == m_keyFields[i].length());
    if (!i && m_indices[i].findVal(key)) {
      commitFn(ZuMv(buf), CommitResult{ZeVEVENT(Error,
	  ([id = this->id(), key = ZuMv(key)](auto &s, const auto &) {
	    s << id << " insert(" << ZtJoin(key, ", ")
	      << ") failed - record exists";
	  }))});
      return;
    }
    m_indices[i].add(key, row.constRef());
  }
  m_indexUN.addNode(ZuMv(row));
  commitFn(ZuMv(buf), CommitResult{});
}

void StoreTbl::update(
  ZmRef<MemRow> updRow, ZmRef<const IOBuf> buf, CommitFn commitFn)
{
  auto key = extractKey(m_fields, m_keyFields, 0, updRow->data);
  ZmRef<MemRow> row = m_indices[0].findVal(key).mutableRef();
  if (row) {
    m_maxUN = updRow->un, m_maxSN = updRow->sn;

    // remember original secondary index key values
    unsigned n = m_keyFields.length();
    auto origKeys = ZmAlloc(Tuple, n - 1);
    for (unsigned i = 1; i < n; i++) {
      auto key = extractKey(m_fields, m_keyFields, i, row->data);
      ZmAssert(key.length() == m_keyFields[i].length());
      new (&origKeys[i - 1]) Tuple(ZuMv(key)); // not Tuple{}
    }
    // remove from UN index
    m_indexUN.delNode(row);

    row->un = updRow->un;
    row->sn = updRow->sn;
    row->vn = updRow->vn;
    updTuple(m_fields, row->data, ZuMv(updRow->data));

    // add back to UN index
    m_indexUN.addNode(row);
    // update secondary indices if corresponding key changed
    for (unsigned i = 1; i < n; i++) {
      auto key = extractKey(m_fields, m_keyFields, i, row->data);
      if (key != origKeys[i - 1]) {
	m_indices[i].del(origKeys[i - 1]);
	m_indices[i].add(key, row.constRef());
      }
    }

    commitFn(ZuMv(buf), CommitResult{});
  } else {
    commitFn(ZuMv(buf), CommitResult{
	ZeVEVENT(Error, ([id = this->id(), key](auto &s, const auto &) {
	  s << id << " update(" << ZtJoin(key, ", ")
	    << ") failed - record missing";
	}))});
  }
}

void StoreTbl::del(
  ZmRef<MemRow> delRow, ZmRef<const IOBuf> buf, CommitFn commitFn)
{
  auto key = extractKey(m_fields, m_keyFields, 0, delRow->data);
  ZmRef<MemRow> row = m_indices[0].delVal(key).mutableRef();
  if (row) {
    m_maxUN = delRow->un, m_maxSN = delRow->sn;
    m_indexUN.delNode(row);
    unsigned n = m_keyFields.length();
    for (unsigned i = 1; i < n; i++) {
      auto key = extractKey(m_fields, m_keyFields, i, row->data);
      ZmAssert(key.length() == m_keyFields[i].length());
      m_indices[i].del(key);
    }
    commitFn(ZuMv(buf), CommitResult{});
  } else {
    commitFn(ZuMv(buf), CommitResult{
	ZeVEVENT(Error, ([id = this->id(), key](auto &s, const auto &) {
	  s << id << " del(" << ZtJoin(key, ", ")
	    << ") failed - record missing";
	}))});
  }
}

} // ZdbMem
