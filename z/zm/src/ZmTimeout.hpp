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

// timeout class with exponential backoff

#ifndef ZmTimeout_HPP
#define ZmTimeout_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZmLib_HPP
#include <zlib/ZmLib.hpp>
#endif

#include <zlib/ZmFn.hpp>
#include <zlib/ZmScheduler.hpp>
#include <zlib/ZmBackoff.hpp>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4251)
#endif

class ZmAPI ZmTimeout {
  ZmTimeout(const ZmTimeout &);
  ZmTimeout &operator =(const ZmTimeout &);	// prevent mis-use

public:
  ZmTimeout(ZmScheduler *scheduler,
	    const ZmBackoff &backoff,
	    int maxCount) :			// -ve or 0 - infinite
    m_scheduler(scheduler), m_backoff(backoff),
    m_maxCount(maxCount), m_count(0) { }

  void start(ZmFn<> retryFn, ZmFn<> finalFn = ZmFn<>()) {
    ZmGuard<ZmLock> guard(m_lock);

    m_retryFn = retryFn;
    m_finalFn = finalFn;
    start_();
  }

private:
  void start_() {
    m_count = 0;
    m_interval = m_backoff.initial();
    m_scheduler->add(
	[this]() { work(); }, ZmTimeNow() + m_interval, &m_timer);
  }

public:
  void reset() {
    ZmGuard<ZmLock> guard(m_lock);

    m_scheduler->del(&m_timer);
    start_();
  }

  void stop() {
    ZmGuard<ZmLock> guard(m_lock);

    m_scheduler->del(&m_timer);
    m_retryFn = ZmFn<>();
    m_finalFn = ZmFn<>();
  }

  void work() {
    ZmGuard<ZmLock> guard(m_lock);

    m_count++;
    if (m_maxCount <= 0 || m_count < m_maxCount) {
      if (m_retryFn) m_retryFn();
      m_interval = m_backoff.backoff(m_interval);
      m_scheduler->add(
	  [this]() { work(); }, ZmTimeNow() + m_interval, &m_timer);
    } else {
      m_retryFn = ZmFn<>();
      m_finalFn();
      m_finalFn = ZmFn<>();
    }
  }

private:
  ZmScheduler		*m_scheduler;
  ZmLock		m_lock;
    ZmFn<>		m_retryFn;
    ZmFn<>		m_finalFn;
    ZmBackoff		m_backoff;
    int			m_maxCount;
    int			m_count;
    ZmTime		m_interval;
    ZmScheduler::Timer	m_timer;
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZmTimeout_HPP */
