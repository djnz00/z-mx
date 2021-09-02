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

// ZvEngine - Connectivity Framework

#include <zlib/ZvEngine.hpp>

void ZvEngine::start_()
{
  mgrAddEngine();

  // appException(ZeEVENT(Info, "START"));

  auto i = m_links.readIterator();
  while (ZmRef<ZvAnyLink> link = i.iterate())
    rxRun(ZmFn<>{link, [](ZvAnyLink *link) { link->up_(false); }});
}

void ZvEngine::stop_()
{
  // appException(ZeEVENT(Info, "STOP"));

  auto i = m_links.readIterator();
  while (ZmRef<ZvAnyLink> link = i.iterate())
    rxRun(ZmFn<>{link, [](ZvAnyLink *link) { link->down_(false); }});

  mgrDelEngine();
}

void ZvEngine::start()
{
  int prev, next;

  {
    StateGuard stateGuard(m_stateLock);

    bool start = false;

    prev = m_state.load_();
    switch (prev) {
      case ZvEngineState::Stopped:
	m_state.store_(ZvEngineState::Starting);
	start = true;
	break;
      case ZvEngineState::Stopping:
	m_state.store_(ZvEngineState::StartPending);
	break;
      default:
	break;
    }
    next = m_state.load_();

    if (start) start_();
  }

  if (next != prev) mgrUpdEngine();
}

void ZvEngine::stop()
{
  int prev, next;

  {
    StateGuard stateGuard(m_stateLock);

    bool stop = false;

    prev = m_state.load_();
    switch (prev) {
      case ZvEngineState::Running:
	m_state.store_(ZvEngineState::Stopping);
	stop = true;
	break;
      case ZvEngineState::Starting:
	m_state.store_(ZvEngineState::StopPending);
	break;
      default:
	break;
    }
    next = m_state.load_();

    if (stop) stop_();
  }

  if (next != prev) mgrUpdEngine();
}

void ZvEngine::linkState(ZvAnyLink_ *link_, int prev, int next)
{
  auto link = static_cast<ZvAnyLink *>(link_);

  if (ZuUnlikely(next == prev)) return;
  switch (prev) {
    case ZvLinkState::Connecting:
    case ZvLinkState::Disconnecting:
    case ZvLinkState::ConnectPending:
    case ZvLinkState::DisconnectPending:
      switch (next) {
	case ZvLinkState::Connecting:
	case ZvLinkState::Disconnecting:
	case ZvLinkState::ConnectPending:
	case ZvLinkState::DisconnectPending:
	  return;
      }
      break;
  }

#if 0
  appException(ZeEVENT(Info,
    ([id = link->id(), prev, next](const ZeEvent &, ZmStream &s) {
      s << "link " << id << ' ' <<
      ZvLinkState::name(prev) << "->" << ZvLinkState::name(next); })));
#endif
  mgrUpdLink(link);

  int enginePrev, engineNext;

  {
    StateGuard stateGuard(m_stateLock);

    switch (prev) {
      case ZvLinkState::Down:
	--m_down;
	break;
      case ZvLinkState::Disabled:
	--m_disabled;
	break;
      case ZvLinkState::Connecting:
      case ZvLinkState::Disconnecting:
      case ZvLinkState::ConnectPending:
      case ZvLinkState::DisconnectPending:
	--m_transient;
	break;
      case ZvLinkState::Up:
	--m_up;
	break;
      case ZvLinkState::ReconnectPending:
      case ZvLinkState::Reconnecting:
	--m_reconn;
	break;
      case ZvLinkState::Failed:
	--m_failed;
	break;
      default:
	break;
    }

    switch (next) {
      case ZvLinkState::Down:
	++m_down;
	break;
      case ZvLinkState::Disabled:
	++m_disabled;
	break;
      case ZvLinkState::Connecting:
      case ZvLinkState::Disconnecting:
      case ZvLinkState::ConnectPending:
      case ZvLinkState::DisconnectPending:
	++m_transient;
	break;
      case ZvLinkState::Up:
	++m_up;
	break;
      case ZvLinkState::ReconnectPending:
      case ZvLinkState::Reconnecting:
	++m_reconn;
	break;
      case ZvLinkState::Failed:
	++m_failed;
	break;
      default:
	break;
    }

    bool start = false, stop = false;

    enginePrev = m_state.load_();
    switch (enginePrev) {
      case ZvEngineState::Starting:
	switch (prev) {
	  case ZvLinkState::Down:
	  case ZvLinkState::Connecting:
	  case ZvLinkState::Disconnecting:
	  case ZvLinkState::ConnectPending:
	  case ZvLinkState::DisconnectPending:
	    if (!(m_down + m_transient))
	      m_state.store_(ZvEngineState::Running);
	    break;
	}
	break;
      case ZvEngineState::StopPending:
	switch (prev) {
	  case ZvLinkState::Down:
	  case ZvLinkState::Connecting:
	  case ZvLinkState::Disconnecting:
	  case ZvLinkState::ConnectPending:
	  case ZvLinkState::DisconnectPending:
	    if (!(m_down + m_transient)) {
	      m_state.store_(ZvEngineState::Stopping);
	      stop = true;
	    }
	    break;
	}
	break;
      case ZvEngineState::Stopping:
	switch (prev) {
	  case ZvLinkState::Connecting:
	  case ZvLinkState::Disconnecting:
	  case ZvLinkState::ConnectPending:
	  case ZvLinkState::DisconnectPending:
	  case ZvLinkState::Up:
	    if (!(m_up + m_transient))
	      m_state.store_(ZvEngineState::Stopped);
	    break;
	}
	break;
      case ZvEngineState::StartPending:
	switch (prev) {
	  case ZvLinkState::Connecting:
	  case ZvLinkState::Disconnecting:
	  case ZvLinkState::ConnectPending:
	  case ZvLinkState::DisconnectPending:
	  case ZvLinkState::Up:
	    if (!(m_up + m_transient)) {
	      m_state.store_(ZvEngineState::Starting);
	      start = true;
	    }
	    break;
	}
	break;
    }
    engineNext = m_state.load_();

    if (start) start_();
    if (stop) stop_();
  }

  if (engineNext != enginePrev) mgrUpdEngine();
}

void ZvEngine::final()
{
  m_links.clean();
  m_txPools.clean();
}

void ZvEngine::telemetry(Telemetry &data) const
{
  data.id = m_id;
  data.mxID = m_mx->params().id();
  {
    StateReadGuard guard(m_stateLock);
    data.down = m_down;
    data.disabled = m_disabled;
    data.transient = m_transient;
    data.up = m_up;
    data.reconn = m_reconn;
    data.failed = m_failed;
    data.state = m_state.load_();
  }
  {
    ReadGuard guard(m_lock);
    data.nLinks = m_links.count();
  }
  data.rxThread = m_rxThread;
  data.txThread = m_txThread;
}

// connection state management

ZvAnyTx::ZvAnyTx(ZuID id) : m_id(id)
{
}

void ZvAnyTx::init(ZvEngine *engine)
{
  m_engine = engine;
  m_mx = engine->mx();
}

ZvAnyLink_::ZvAnyLink_(ZuID id) :
  ZvAnyTx(id),
  m_state(ZvLinkState::Down),
  m_reconnects(0)
{
}

void ZvAnyLink_::up_(bool enable)
{
  int prev, next;
  bool connect = false;
  bool running = false;

  switch ((int)engine()->state()) {
    case ZvEngineState::Starting:
    case ZvEngineState::Running:
      running = true;
      break;
  }

  // cancel reconnect
  mx()->del(&m_reconnTimer);

  {
    StateGuard stateGuard(m_stateLock);

    if (enable) m_enabled = true;

    // state machine
    prev = m_state.load_();
    switch (prev) {
      case ZvLinkState::Disabled:
      case ZvLinkState::Down:
      case ZvLinkState::Failed:
	if (running) {
	  m_state.store_(ZvLinkState::Connecting);
	  connect = true;
	} else
	  m_state.store_(ZvLinkState::Down);
	break;
      case ZvLinkState::Disconnecting:
	if (running && m_enabled)
	  m_state.store_(ZvLinkState::ConnectPending);
	break;
      case ZvLinkState::DisconnectPending:
	if (m_enabled)
	  m_state.store_(ZvLinkState::Connecting);
	break;
      default:
	break;
    }
    next = m_state.load_();
  }

  if (next != prev) engine()->linkState(this, prev, next);
  if (connect)
    engine()->rxInvoke(
	static_cast<ZvAnyLink *>(this),
	[](ZvAnyLink *link) { link->connect(); });
}

void ZvAnyLink_::down_(bool disable)
{
  int prev, next;
  bool disconnect = false;

  // cancel reconnect
  mx()->del(&m_reconnTimer);

  {
    StateGuard stateGuard(m_stateLock);

    if (disable) m_enabled = false;

    // state machine
    prev = m_state.load_();
    switch (prev) {
      case ZvLinkState::Down:
	if (!m_enabled) m_state.store_(ZvLinkState::Disabled);
	break;
      case ZvLinkState::Up:
      case ZvLinkState::ReconnectPending:
      case ZvLinkState::Reconnecting:
	m_state.store_(ZvLinkState::Disconnecting);
	disconnect = true;
	break;
      case ZvLinkState::Connecting:
	m_state.store_(ZvLinkState::DisconnectPending);
	break;
      case ZvLinkState::ConnectPending:
	m_state.store_(ZvLinkState::Disconnecting);
	break;
      default:
	break;
    }
    next = m_state.load_();
  }

  if (next != prev) engine()->linkState(this, prev, next);
  if (disconnect)
    engine()->rxInvoke(
	static_cast<ZvAnyLink *>(this),
	[](ZvAnyLink *link) { link->disconnect(); });
}

void ZvAnyLink_::connected()
{
  int prev, next;
  bool disconnect = false;

  // cancel reconnect
  mx()->del(&m_reconnTimer);

  {
    StateGuard stateGuard(m_stateLock);

    // state machine
    prev = m_state.load_();
    switch (prev) {
      case ZvLinkState::Connecting:
      case ZvLinkState::ReconnectPending:
      case ZvLinkState::Reconnecting:
	m_state.store_(ZvLinkState::Up);
      case ZvLinkState::Up:
	m_reconnects.store_(0);
	break;
      case ZvLinkState::DisconnectPending:
	m_state.store_(ZvLinkState::Disconnecting);
	disconnect = true;
	m_reconnects.store_(0);
	break;
      default:
	break;
    }
    next = m_state.load_();
  }

  if (next != prev) engine()->linkState(this, prev, next);
  if (disconnect) 
    engine()->rxRun(
	ZmFn<>{this, [](ZvAnyLink *link) { link->disconnect(); }});
}

void ZvAnyLink_::disconnected()
{
  int prev, next;
  bool connect = false;

  // cancel reconnect
  mx()->del(&m_reconnTimer);

  {
    StateGuard stateGuard(m_stateLock);

    // state machine
    prev = m_state.load_();
    switch (prev) {
      case ZvLinkState::Connecting:
      case ZvLinkState::DisconnectPending:
      case ZvLinkState::ReconnectPending:
      case ZvLinkState::Reconnecting:
      case ZvLinkState::Up:
	m_state.store_(m_enabled ? ZvLinkState::Failed : ZvLinkState::Disabled);
	m_reconnects.store_(0);
	break;
      case ZvLinkState::Disconnecting:
	m_state.store_(m_enabled ? ZvLinkState::Down : ZvLinkState::Disabled);
	m_reconnects.store_(0);
	break;
      case ZvLinkState::ConnectPending:
	if (m_enabled) {
	  m_state.store_(ZvLinkState::Connecting);
	  connect = true;
	} else
	  m_state.store_(ZvLinkState::Disabled);
	m_reconnects.store_(0);
	break;
      default:
	break;
    }
    next = m_state.load_();
  }

  if (next != prev) engine()->linkState(this, prev, next);
  if (connect)
    engine()->rxRun(
	ZmFn<>{this, [](ZvAnyLink *link) { link->connect(); }});
}

void ZvAnyLink_::reconnecting()
{
  int prev, next;

  // cancel reconnect
  mx()->del(&m_reconnTimer);

  {
    StateGuard stateGuard(m_stateLock);

    // state machine
    prev = m_state.load_();
    switch (prev) {
      case ZvLinkState::Up:
	m_state.store_(ZvLinkState::Connecting);
	break;
      default:
	break;
    }
    next = m_state.load_();
  }

  if (next != prev) engine()->linkState(this, prev, next);
}

void ZvAnyLink_::reconnect(bool immediate)
{
  int prev, next;
  bool reconnect = false, disconnect = false;
  ZmTime reconnTime;

  // cancel reconnect
  mx()->del(&m_reconnTimer);

  {
    StateGuard stateGuard(m_stateLock);

    // state machine
    prev = m_state.load_();
    switch (prev) {
      case ZvLinkState::Connecting:
      case ZvLinkState::Reconnecting:
      case ZvLinkState::Up:
	m_state.store_(ZvLinkState::ReconnectPending);
	reconnect = true;
	break;
      case ZvLinkState::DisconnectPending:
	disconnect = true;
	break;
      default:
	break;
    }
    next = m_state.load_();

    if (reconnect) {
      m_reconnects.store_(m_reconnects.load_() + 1);
      reconnTime.now(reconnInterval(m_reconnects.load_()));
    }
  }

  if (next != prev) engine()->linkState(this, prev, next);
  if (reconnect) {
    if (immediate)
      engine()->rxRun(
	  ZmFn<>{this, [](ZvAnyLink *link) { link->reconnect_(); }});
    else
      engine()->rxRun(
	  ZmFn<>{this, [](ZvAnyLink *link) { link->reconnect_(); }},
	  reconnTime, &m_reconnTimer);
  }
  if (disconnect) 
    engine()->rxRun(
	ZmFn<>{this, [](ZvAnyLink *link) { link->disconnect(); }});
}

void ZvAnyLink_::reconnect_()
{
  int prev, next;
  bool connect = false;

  {
    StateGuard stateGuard(m_stateLock);

    // state machine
    prev = m_state.load_();
    switch (prev) {
      case ZvLinkState::ReconnectPending:
	m_state.store_(ZvLinkState::Reconnecting);
	connect = true;
	break;
      default:
	break;
    }
    next = m_state.load_();
  }

  if (next != prev) engine()->linkState(this, prev, next);
  if (connect)
    engine()->rxRun(
	ZmFn<>{this, [](ZvAnyLink *link) { link->connect(); }});
}

void ZvAnyLink_::deleted_()
{
  int prev;
  {
    StateGuard stateGuard(m_stateLock);
    prev = m_state.load_();
    m_state.store_(ZvLinkState::Deleted);
  }
  if (prev != ZvLinkState::Deleted)
    engine()->linkState(this, prev, ZvLinkState::Deleted);
}

void ZvAnyLink_::telemetry(Telemetry &data) const
{
  data.id = id();
  data.rxSeqNo = rxSeqNo();
  data.txSeqNo = txSeqNo();
  data.reconnects = m_reconnects.load_();
  data.state = m_state.load_();
}
