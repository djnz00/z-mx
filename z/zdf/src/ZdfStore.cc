//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Frame backing data store

#include <zlib/ZdfStore.hh>

using namespace Zdf;

void Store::dbCf(const ZvCf *cf, ZdbCf &dbCf)
{
  ZtArray<ZtString> threads = cf->getStrArray<false>("threads");

  static ZtArray<ZuCSpan> tables{
    "zdf.series_fixed",
    "zdf.series_float",
    "zdf.blk_fixed",
    "zdf.blk_float",
    "zdf.blk_data"
  };

  for (auto &&tblID: tables) {
    auto node = dbCf.tableCfs.find(tblID);
    using Node = ZuDecay<decltype(*node)>;
    if (!node) dbCf.tableCfs.addNode(node = new Node{tblID});
    node->data().threads = threads;
  }
}

void Store::init(Zdb *db)
{
  ZeAssert(m_state == StoreState::Uninitialized,
    (state = m_state), "invalid state=" << state, return);

  m_seriesFixedTbl = db->initTable<DB::SeriesFixed>("zdf.series_fixed");
  m_seriesFloatTbl = db->initTable<DB::SeriesFloat>("zdf.series_float");
  m_blkFixedTbl = db->initTable<DB::BlkFixed>("zdf.blk_fixed");
  m_blkFloatTbl = db->initTable<DB::BlkFloat>("zdf.blk_float");
  m_blkDataTbl = db->initTable<DB::BlkData>("zdf.blk_data");

  m_mx = db->mx();
  m_sids = m_blkDataTbl->config().sids;

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
