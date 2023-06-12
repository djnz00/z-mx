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

// command line interface

#include <zlib/ZrlCLI.hpp>

#include <zlib/ZtRegex.hpp>

using namespace Zrl;

namespace {
  static ZmSemaphore done;
  static bool openOK = false;
}

void CLI::init(App app)
{
  Guard guard(m_lock);
  switch (m_state) {
    case Running:
    case Opened:
    case Initialized:
      return;
    case Created:
      break;
  }
  m_state = Initialized;
  app.end = [end = ZuMv(app.end)]() { done.post(); end(); };
  app.sig = [sig = ZuMv(app.sig)](int i) {
    switch (i) {
      case SIGINT:
      case SIGQUIT:
	done.post();
	sig(i);
	return true;
      default:
	sig(i);
	return false;
    }
  };
  app.open = [open = ZuMv(app.open)](bool ok) {
    open(ok);
    openOK = ok;
    done.post();
  };
  Editor::init(ZuMv(app));
  m_sched = new ZmScheduler{ZmSchedParams{}.id("ZrlCLI").nThreads(1)};
  if (auto maps = ::getenv("ZRL_MAPS")) {
    ZtRegex::Captures c;
    if (ZtREGEX(":").split(maps, c) > 0)
      for (const auto &map : c)
	if (!loadMap(map, false))
	  std::cerr << loadError() << '\n' << std::flush;
  }
  if (auto map = ::getenv("ZRL_MAP"))
    if (!loadMap(map, true))
      std::cerr << loadError() << '\n' << std::flush;
  if (auto mapID = ::getenv("ZRL_MAPID"))
    map(mapID);
}

void CLI::final()
{
  Guard guard(m_lock);
  switch (m_state) {
    case Running:
      stop_();
    case Opened:
      close_();
    case Initialized:
      break;
    case Created:
      return;
  }
  final_();
  m_state = Created;
}

void CLI::final_()
{
  Editor::final();
  m_sched = nullptr;
}

bool CLI::open()
{
  Guard guard(m_lock);
  switch (m_state) {
    case Running:
    case Opened:
      return true;
    case Initialized:
      break;
    case Created:
      return false;
  }
  bool ok = open_();
  if (ok) m_state = Opened;
  return ok;
}

bool CLI::open_()
{
  m_sched->start();
  Editor::open(m_sched, 1);
  done.wait();
  return openOK;
}

void CLI::close()
{
  Guard guard(m_lock);
  switch (m_state) {
    case Running:
      stop_();
    case Opened:
      break;
    case Initialized:
    case Created:
      return;
  }
  close_();
  m_state = Initialized;
}

void CLI::close_()
{
  Editor::close();
  m_sched->stop();
}

bool CLI::start()
{
  Guard guard(m_lock);
  switch (m_state) {
    case Running:
      return true;
    case Opened:
      break;
    case Initialized:
    case Created:
      return false;
  }
  Editor::start();
  m_state = Running;
  return true;
}

void CLI::stop()
{
  Guard guard(m_lock);
  switch (m_state) {
    case Running:
      break;
    case Opened:
    case Initialized:
    case Created:
      return;
  }
  stop_();
  m_state = Opened;
}

void CLI::stop_()
{
  Editor::stop();
}

void CLI::join()
{
  done.wait();
}
