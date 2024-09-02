//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Frame backing data store

#include <zlib/ZdfStore.hh>

using namespace Zdf;

void Store::init(ZvCf *cf, Zdb *db)
{
  ZeAssert(m_state == StoreState::Uninitialized,
    (state = m_state), "invalid state=" << state, return);

  m_mx = db->mx();

  static auto findAdd = [](ZdbCf &dbCf, ZuString key) {
    auto node = dbCf.tableCfs.find(key);
    using Node = ZuDecay<decltype(*node)>;
    if (!node) dbCf.tableCfs.addNode(node = new Node{key});
    return &(node->data());
  };

  ZtArray<ZtString> defltThread; // empty - will default to zdb main thread
  const auto *thread = &defltThread;
  {
    const ZvCfNode *node;
    if (cf && (node = cf->getNode<false>("thread")) &&
	node->data.is<ZtArray<ZtString>>())
      thread = &node->data.p<ZtArray<ZtString>>();
  }
  auto &dbCf = const_cast<ZdbCf &>(db->config());
  findAdd(dbCf, "zdf.data_frame")->thread = *thread;
  findAdd(dbCf, "zdf.series_fixed")->thread = *thread;
  findAdd(dbCf, "zdf.series_float")->thread = *thread;
  findAdd(dbCf, "zdf.blk_hdr_fixed")->thread = *thread;
  findAdd(dbCf, "zdf.blk_hdr_float")->thread = *thread;
  findAdd(dbCf, "zdf.blk_data")->thread = *thread;

  m_seriesFixedTbl = db->initTable<DB::SeriesFixed>("zdf.series_fixed");
  m_seriesFloatTbl = db->initTable<DB::SeriesFloat>("zdf.series_float");
  m_blkFixedTbl = db->initTable<DB::BlkFixed>("zdf.blk_fixed");
  m_blkFloatTbl = db->initTable<DB::BlkFloat>("zdf.blk_float");
  m_blkDataTbl = db->initTable<DB::BlkData>("zdf.blk_data");

  // all the above tables are forced to have identical thread configuration,
  // any one copy can be cached locally
  m_sid = m_blkDataTbl->config().sid;

  m_state = StoreState::Initialized;
}

void Store::final()
{
  m_state = StoreState::Uninitialized;

  m_seriesFixedTbl = nullptr;
  m_seriesFloatTbl = nullptr;
  m_blkFixedTbl = nullptr;
  m_blkFloatTbl = nullptr;
  m_blkDataTbl = nullptr;
}

void Store::open(OpenFn fn)
{
  m_openFn = ZuMv(fn);
  open_recoverNextSeriesID_Fixed();
}
void Store::open_recoverNextSeriesID_Fixed()
{
  m_seriesFixedTbl->selectKeys<0>(ZuTuple<>{}, 1, [this](auto max, unsigned) {
    run(0, [this, max = ZuMv(max)]() mutable {
      using Key = ZuFieldKeyT<DB::SeriesFixed, 0>;
      if (max.template is<Key>())
	m_nextSeriesID = max.template p<Key>().template p<0>() + 1;
      else
	open_recoverNextSeriesID_Float();
    });
  });
}
void Store::open_recoverNextSeriesID_Float()
{
  m_seriesFloatTbl->selectKeys<0>(ZuTuple<>{}, 1, [this](auto max, unsigned) {
    run(0, [this, max = ZuMv(max)]() mutable {
      using Key = ZuFieldKeyT<DB::SeriesFloat, 0>;
      if (max.template is<Key>()) {
	auto nextSeriesID = max.template p<Key>().template p<0>() + 1;
	if (nextSeriesID > m_nextSeriesID) m_nextSeriesID = nextSeriesID;
      } else
	opened(true);
    });
  });
}
void Store::opened(bool ok)
{
  m_state = ok ? StoreState::Opened : StoreState::OpenFailed;
  auto fn = ZuMv(m_openFn);
  fn(ok);
}
