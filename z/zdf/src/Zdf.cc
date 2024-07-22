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

DataFrame::DataFrame(
  Mgr *mgr, const ZtMFieldArray &fields, ZuString name, bool timeIndex) :
  m_name{name}
{
  bool indexed = timeIndex;
  unsigned n = fields.length();
  m_series.size(n + timeIndex);
  m_fields.size(n + timeIndex);
  for (unsigned i = 0; i < n; i++) {
    ZuPtr<Series> series = new Series();
    if (!indexed && (fields[i]->props & ZtMFieldProp::Index)) {
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
    m_fields.unshift(static_cast<ZtMField *>(nullptr));
  }
}

void DataFrame::init(Store *store)
{
  m_store = store;
  unsigned n = m_series.length();
  for (unsigned i = 0; i < n; i++) m_series[i]->init(store);
}

void DataFrame::open(OpenFn openFn)
{
  if (ZuUnlikely(!m_store)) {
    openFn(OpenResult{ZeMEVENT(Error, "no backing store configured")});
    return;
  }
  {
    ZmGuard<ZmPLock> guard(m_lock);

    if (m_pending) {
      guard.unlock();
      openFn(OpenResult{ZeMEVENT(Error, "overlapping open/close")});
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
      m_series[i]->open(m_name, field->id, ZuMv(openFn_));
    } else {
      m_series[i]->open(m_name, "_0", ZuMv(openFn_));
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
    closeFn(CloseResult{ZeMEVENT(Error, "no backing store configured")});
    return;
  }
  {
    ZmGuard<ZmPLock> guard(m_lock);

    if (m_pending) {
      guard.unlock();
      closeFn(CloseResult{ZeMEVENT(Error, "overlapping open/close")});
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

void DataFrame::load(Store_::LoadFn loadFn)
{
  using namespace Zfb::Load;
  m_store->loadDF(m_name,
      LoadFn{this, [](DataFrame *this_, ZuBytes data) {
	return this_->load_(data);
      }}, (1<<10) /* 1Kb */, ZuMv(loadFn));
}

bool DataFrame::load_(ZuBytes data)
{
  {
    Zfb::Verifier verifier(&data[0], data.length());
    if (!fbs::VerifyDataFrameBuffer(verifier)) return false;
  }
  using namespace Zfb::Load;
  auto df = fbs::GetDataFrame(&data[0]);
  m_epoch = dateTime(df->epoch()).as_time();
  return true;
}

void DataFrame::save(Store_::SaveFn saveFn)
{
  Zfb::Builder fbb;
  fbb.Finish(save_(fbb));
  m_store->saveDF(m_name, fbb, ZuMv(saveFn));
}

Zfb::Offset<fbs::DataFrame> DataFrame::save_(Zfb::Builder &fbb)
{
  using namespace Zfb::Save;
  auto v = dateTime(ZuDateTime{m_epoch});
  return fbs::CreateDataFrame(fbb, &v);
}
