//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Data Frame

#include <zlib/Zdf.hh>

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
    if (!node) dbCf.tableCfs.addNode(node = new decltype(*node){key});
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

  m_dataFrameTbl = nullptr;
  m_seriesFixedTbl = nullptr;
  m_seriesFloatTbl = nullptr;
  m_blkHdrFixedTbl = nullptr;
  m_blkHdrFloatTbl = nullptr;
  m_blkDataTbl = nullptr;
}

void DB::open(OpenFn fn)
{
  m_openFn = ZuMv(fn);
  open_recoverNextDFID();
}
void DB::open_recoverNextDFID()
{
  m_dataFrameTbl->selectKeys<0>(ZuTuple<>{}, 1, [this](auto max, unsigned) {
    run(0, [this, max = ZuMv(max)]() mutable {
      using Key = ZuFieldKeyT<DB::DataFrame, 0>;
      if (max.template is<Key>())
	m_nextDFID = max.template p<Key>().template p<0>() + 1;
      else
	open_recoverNextPermID();
    });
  });
}
void DB::open_recoverNextSeriesID_Fixed()
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

void DB::openDF(
  Shard shard, ZtString name, bool create,
  const ZtVFieldArray &fields_, bool timeIndex, OpenDFFn fn)
{
  ZtArray<ZmRef<AnySeries>> series;
  ZtArray<const ZtVField *> fields;
  bool indexed = timeIndex;
  unsigned n = fields.length();
  series.size(n + timeIndex);
  fields.size(n + timeIndex);
  [name = ZuMv(name), fields, i = 0U](ZmRef<AnySeries> series) mutable {
    auto field = fields[i];
    // FIXME
    ZtString seriesName{name.length() + strlen(field->id) + 2};
    seriesName << name << '/' << field->id;
    if (field->type->code == ZtFieldTypeCode::Float) {
      openSeries<FloatDecoder>(shard, seriesName, create, fn);
    }
      m_series.unshift(new FloatSeries(this, 
    else if (fields[i]->props & ZtVFieldProp::Delta2())
    else if (fields[i]->props & ZtVFieldProp::Delta())
    else
      indexed = true;
      m_series.unshift(ZuMv(series));
      m_fields.unshift(fields[i]);
    } else {
      m_series.push(ZuMv(series));
      m_fields.push(fields[i]);
    }
  }
  if (timeIndex) {
    m_series.unshift(ZuPtr<Series>{new Series()});
    m_fields.unshift(static_cast<ZtVField *>(nullptr));
  }
}

ZuPtr<AnySeries> DataFrame::series(const ZtVField *field)
{
  using namespace ZtFieldTypeCode;

  auto code = field->type->code;
  if (code == Float)
    return new Series<FPDecoder>{
}

void DataFrame::init(Store *store)
{
  m_store = store;
  unsigned n = m_series.length();
  for (unsigned i = 0; i < n; i++) m_series[i]->init(store);
}

void DataFrame::open(OpenFn openFn)
{
  // FIXME
  if (ZuUnlikely(!m_store)) {
    openFn(OpenResult{ZeVEVENT(Error, "no backing store configured")});
    return;
  }
  {
    ZmGuard<ZmPLock> guard(m_lock);

    if (m_pending) {
      guard.unlock();
      openFn(OpenResult{ZeVEVENT(Error, "overlapping open/close")});
      return;
    }

    m_pending = 1;
    m_error = {};
    m_callback = Callback{ZuMv(openFn)};
  }

  load([this](Store_::LoadResult result) mutable {
    if (result.is<Event>()) {			// load error
      openFailed(OpenResult{ZuMv(result).p<Event>()});
      return;
    }
    if (result.is<Store_::LoadData>()) {	// loaded
      openSeries();
      return;
    }
    // missing - save new data frame, starting now
    m_epoch = Zm::now();
    save([this](Store_::SaveResult result) mutable {
      if (result.is<Event>()) {
	openFailed(OpenResult{ZuMv(result).p<Event>()});
	return;
      }
      openSeries();
    });
  });
}

void DataFrame::openSeries()
{
  // FIXME
  unsigned n = m_series.length();

  {
    ZmGuard<ZmPLock> guard(m_lock);

    m_pending = n;
  }

  for (unsigned i = 0; i < n; i++) {
    auto field = m_fields[i];
    OpenFn openFn_{this, [](DataFrame *this_, OpenResult result) {
      this_->openedSeries(ZuMv(result));
    }};
    if (i || field) {
      m_series[i]->open(m_name, field->id, ZuMv(openFn_)); // FIXME
    } else {
      m_series[i]->open(m_name, "_0", ZuMv(openFn_)); // FIXME
    }
  }
}

void DataFrame::openedSeries(OpenResult result)
{
  OpenFn openFn;

  {
    ZmGuard<ZmPLock> guard(m_lock);

    if (!m_pending) return; // should never happen

    if (result.is<Event>()) {
      if (!m_error) m_error = new Event{ZuMv(result).p<Event>()};
    }

    if (--m_pending) return;

    if (m_error) {
      result = OpenResult{ZuMv(*m_error)};
      m_error = {};
    }

    openFn = ZuMv(m_callback).p<OpenFn>();
    m_callback = Callback{};
  }

  openFn(ZuMv(result));
}

void DataFrame::openFailed(OpenResult result)
{
  OpenFn openFn;

  {
    ZmGuard<ZmPLock> guard(m_lock);

    m_pending = 0;
    m_error = {};
    openFn = ZuMv(m_callback).p<OpenFn>();
  }

  openFn(ZuMv(result));
}

void DataFrame::close(CloseFn closeFn)
{
  if (ZuUnlikely(!m_store)) {
    closeFn(CloseResult{ZeVEVENT(Error, "no backing store configured")});
    return;
  }
  {
    ZmGuard<ZmPLock> guard(m_lock);

    if (m_pending) {
      guard.unlock();
      closeFn(CloseResult{ZeVEVENT(Error, "overlapping open/close")});
      return;
    }

    m_pending = 1;
    m_error = {};
    m_callback = Callback{ZuMv(closeFn)};
  }

  save([this](Store_::SaveResult result) mutable {
    if (result.is<Event>()) {			// save error
      closeFailed(CloseResult{ZuMv(result).p<Event>()});
      return;
    }
    closeSeries();
    return;
  });
}

void DataFrame::closeSeries()
{
  unsigned n = m_series.length();

  {
    ZmGuard<ZmPLock> guard(m_lock);

    m_pending = n;
  }

  for (unsigned i = 0; i < n; i++) {
    auto field = m_fields[i];
    CloseFn closeFn_{this, [](DataFrame *this_, CloseResult result) {
      this_->closedSeries(ZuMv(result));
    }};
    if (i || field) {
      m_series[i]->close(ZuMv(closeFn_));
    } else {
      m_series[i]->close(ZuMv(closeFn_));
    }
  }
}

void DataFrame::closedSeries(CloseResult result)
{
  CloseFn closeFn;

  {
    ZmGuard<ZmPLock> guard(m_lock);

    if (!m_pending) return; // should never happen

    if (result.is<Event>()) {
      if (!m_error) m_error = new Event{ZuMv(result).p<Event>()};
    }

    if (--m_pending) return;

    if (m_error) {
      result = CloseResult{ZuMv(*m_error)};
      m_error = {};
    }

    closeFn = ZuMv(m_callback).p<CloseFn>();
    m_callback = Callback{};
  }

  closeFn(ZuMv(result));
}

void DataFrame::closeFailed(OpenResult result)
{
  CloseFn closeFn;

  {
    ZmGuard<ZmPLock> guard(m_lock);

    m_pending = 0;
    m_error = {};
    closeFn = ZuMv(m_callback).p<CloseFn>();
  }

  closeFn(ZuMv(result));
}
