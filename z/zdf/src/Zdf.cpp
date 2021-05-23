//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=l1,g0,N-s,j1,U1,i4

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

// Data Frame

#include <zlib/Zdf.hpp>

using namespace Zdf;

DataFrame::DataFrame(ZvVFieldArray fields, ZuString name, bool timeIndex) :
    m_name{name}
{
  bool indexed = timeIndex;
  unsigned n = fields.length();
  m_series.size(n + timeIndex);
  m_fields.size(n + timeIndex);
  for (unsigned i = 0; i < n; i++) {
    ZuPtr<Series> series = new Series();
    if (!indexed && (fields[i].flags & ZvFieldFlags::Index)) {
      indexed = true;
      m_series.unshift(ZuMv(series));
      m_fields.unshift(&fields[i]);
    } else {
      m_series.push(ZuMv(series));
      m_fields.push(&fields[i]);
    }
  }
  if (timeIndex) {
    m_series.unshift(ZuPtr<Series>{new Series()});
    m_fields.unshift(static_cast<ZvVField *>(nullptr));
  }
}

void DataFrame::init(Mgr *mgr)
{
  m_mgr = mgr;
  unsigned n = m_series.length();
  for (unsigned i = 0; i < n; i++) m_series[i]->init(mgr);
}

bool DataFrame::open(ZeError *e_)
{
  if (ZuUnlikely(!m_mgr)) { if (e_) *e_ = ZiENOENT; return false; }
  ZeError e;
  if (load(&e)) goto open;
  if (e.errNo() == ZiENOENT) {
    m_epoch.now();
    if (save(e_)) goto open;
  }
  if (e_) *e_ = e;
  return false;
open:
  unsigned n = m_series.length();
  if (ZuUnlikely(!n)) return false;
  for (unsigned i = 0; i < n; i++) {
    auto field = m_fields[i];
    if (i || field)
      m_series[i]->open(m_name, field->id);
    else
      m_series[i]->open(m_name, "_0");
  }
  return true;
}

bool DataFrame::close(ZeError *e)
{
  return save(e);
}

bool DataFrame::load(ZeError *e)
{
  using namespace Zfb::Load;
  return m_mgr->loadFile(m_name,
      LoadFn{this, [](DataFrame *this_, const uint8_t *buf, unsigned len) {
	return this_->load_(buf, len);
      }}, (1<<10) /* 1Kb */, e);
}

bool DataFrame::load_(const uint8_t *buf, unsigned len)
{
  {
    Zfb::Verifier verifier(buf, len);
    if (!fbs::VerifyDataFrameBuffer(verifier)) return false;
  }
  using namespace Zfb::Load;
  auto df = fbs::GetDataFrame(buf);
  m_epoch = dateTime(df->epoch()).zmTime();
  return true;
}

bool DataFrame::save(ZeError *e)
{
  Zfb::Builder fbb;
  fbb.Finish(save_(fbb));
  return m_mgr->saveFile(m_name, fbb, e);
}

Zfb::Offset<fbs::DataFrame> DataFrame::save_(Zfb::Builder &fbb)
{
  using namespace Zfb::Save;
  auto v = dateTime(ZtDate{m_epoch});
  return fbs::CreateDataFrame(fbb, &v);
}
