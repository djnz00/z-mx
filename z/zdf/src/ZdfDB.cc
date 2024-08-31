//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Frame Database

#include <zlib/ZdfDB.hh>

#include "zdf_dataframe_fbs.h"
#include "zdf_series_fbs.h"
#include "zdf_hdr_fbs.h"
#include "zdf_blk_fbs.h"

using namespace Zdf;

void DB::init(ZvCf *cf, Zdb *db)
{
  ZeAssert(m_state == DBState::Uninitialized,
    (state = m_state), "invalid state=" << state, return);

  static auto findAdd = [](DBCf &dbCf, ZuString key) {
    auto node = dbCf.tableCfs.find(key);
    using Node = decltype(*node);
    if (!node) dbCf.tableCfs.addNode(node = new Node{key});
    return node;
  }

  const auto *node = cf->getNode<true>("thread");
  if (!node || !node->data.is<ZtArray<ZtString>>())
    throw ZvCfRequired{cf, "thread"};
  const auto &thread = node->data.p<ZtArray<ZtString>>();
  auto &dbCf = const_cast<ZdbCf &>(db->config());
  findAdd(dbCf, "zdf.data_frame")->thread = thread;
  findAdd(dbCf, "zdf.series_fixed")->thread = thread;
  findAdd(dbCf, "zdf.series_float")->thread = thread;
  findAdd(dbCf, "zdf.blk_hdr_fixed")->thread = thread;
  findAdd(dbCf, "zdf.blk_hdr_float")->thread = thread;
  findAdd(dbCf, "zdf.blk_data")->thread = thread;

  m_dataFrameTbl = db->initTable<DB::DataFrame>("zdf.data_frame");
  m_seriesFixedTbl = db->initTable<DB::SeriesFixed>("zdf.series_fixed");
  m_seriesFloatTbl = db->initTable<DB::SeriesFloat>("zdf.series_float");
  m_blkHdrFixedTbl = db->initTable<DB::BlkHdrFixed>("zdf.blk_hdr_fixed");
  m_blkHdrFloatTbl = db->initTable<DB::BlkHdrFloat>("zdf.blk_hdr_float");
  m_blkDataTbl = db->initTable<DB::BlkData>("zdf.blk_data");

  m_sid = m_dataFrameTbl->config().sid;

  m_state = DB::Initialized;
}

void DB::final()
{
  m_state = DBState::Uninitialized;

  m_seriesFixedTbl = nullptr;
  m_seriesFloatTbl = nullptr;
  m_blkHdrFixedTbl = nullptr;
  m_blkHdrFloatTbl = nullptr;
  m_blkDataTbl = nullptr;
}

void DB::open(OpenFn fn)
{
  m_openFn = ZuMv(fn);
  open_recoverNextSeriesID_Fixed();
}
void DB::open_recoverNextSeriesID_Fixed();
{
  m_seriesFixedTbl->selectKeys<0>(ZuTuple<>{}, 1, [this](auto max, unsigned) {
    run(0, [this, max = ZuMv(max)]() mutable {
      using Key = ZuFieldKeyT<DB::Series, 0>;
      if (max.template is<Key>())
	m_nextSeriesID = max.template p<Key>().template p<0>() + 1;
      else
	open_recoverNextSeriesID_Float();
    });
  });
}
void DB::open_recoverNextSeriesID_Float()
{
  m_seriesFixedTbl->selectKeys<0>(ZuTuple<>{}, 1, [this](auto max, unsigned) {
    run(0, [this, max = ZuMv(max)]() mutable {
      using Key = ZuFieldKeyT<DB::Series, 0>;
      if (max.template is<Key>()) {
	auto nextSeriesID = max.template p<Key>().template p<0>() + 1;
	if (nextSeriesID > m_nextSeriesID) m_nextSeriesID = nextSeriesID;
      } else
	opened(true);
    });
  });
}
void DB::opened(bool ok)
{
  m_state = ok ? DBState::Opened : DBState::OpenFailed;
  auto fn = ZuMv(m_openFn);
  fn(ok);
}
