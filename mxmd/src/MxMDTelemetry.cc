//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxMD telemetry

#include <mxmd/MxMDTelemetry.hh>

#include <mxmd/MxMDCore.hh>

void MxMDTelemetry::init(MxMDCore *core, const ZvCf *cf)
{
  using namespace MxTelemetry;

  MxMultiplex *mx = core->mx(cf->get("mx", "telemetry"));
  if (!mx) throw ZvCf::Required(cf, "mx");
  m_core = core;
  Server::init(mx, cf);
}

void MxMDTelemetry::final()
{
  Guard guard(m_lock);
  m_engines.clean();
  m_queues.clean();
  m_dbEnv = nullptr;
}

void MxMDTelemetry::run(MxTelemetry::Server::Cxn *cxn)
{
  using namespace MxTelemetry;

  // heaps
  ZmHeapMgr::all(ZmFn<ZmHeapCache *>{
      cxn, [](Cxn *cxn, ZmHeapCache *h) {
	cxn->transmit(heap(h));
      }});

  // hash tables
  ZmHashMgr::all(ZmFn<ZmAnyHash *>{
      cxn, [](Cxn *cxn, ZmAnyHash *h) {
	cxn->transmit(hashTbl(h));
      }});

  // threads
  ZmSpecific<ZmThreadContext>::all([cxn](ZmThreadContext *tc) {
    cxn->transmit(thread(tc));
  });

  // mutiplexers, thread queues, sockets
  m_core->allMx(ZmFn<MxMultiplex *>{
      cxn, [](Cxn *cxn, MxMultiplex *mx) {
	cxn->transmit(multiplexer(mx));
	{
	  uint64_t inCount, inBytes, outCount, outBytes;
	  for (unsigned tid = 1, n = mx->params().nThreads(); tid <= n; tid++) {
	    MxIDString queueID;
	    queueID << mx->params().id() << '.'
	      << mx->params().thread(tid).name();
	    {
	      const auto &ring = mx->ring(tid);
	      ring.stats(inCount, inBytes, outCount, outBytes);
	      cxn->transmit(queue(
		    queueID,
		    (uint64_t)0, (uint64_t)ring.count(),
		    inCount, inBytes, outCount, outBytes,
		    (uint32_t)ring.full(), (uint32_t)ring.params().size(),
		    (uint8_t)QueueType::Thread));
	    }
	    if (queueID.length() < MxIDStrSize - 1)
	      queueID << '_';
	    else
	      queueID[MxIDStrSize - 2] = '_';
	    {
	      const auto &overRing = mx->overRing(tid);
	      overRing.stats(inCount, outCount);
	      cxn->transmit(queue(
		    queueID,
		    (uint64_t)0, (uint64_t)overRing.count_(),
		    inCount, inCount * sizeof(ZmFn<>),
		    outCount, outCount * sizeof(ZmFn<>),
		    (uint32_t)0, (uint32_t)overRing.size_(),
		    (uint8_t)QueueType::Thread));
	    }
	  }
	}
	mx->allCxns([cxn](ZiConnection *cxn_) {
	    cxn->transmit(MxTelemetry::socket(cxn_)); });
      }});

  // IPC queues (MD broadcast)
  if (ZmRef<MxMDBroadcast::Ring> ring = m_core->broadcast().ring()) {
    uint64_t inCount, inBytes, outCount, outBytes;
    ring->stats(inCount, inBytes, outCount, outBytes);
    int count = ring->readStatus();
    if (count < 0) count = 0;
    cxn->transmit(queue(
	  ring->params().name(), (uint64_t)0, (uint64_t)count,
	  inCount, inBytes, outCount, outBytes,
	  (uint32_t)ring->full(), (uint32_t)ring->params().size(),
	  (uint8_t)QueueType::IPC));
  }

  {
    ReadGuard guard(m_lock);

    // I/O Engines & Links
    {
      auto i = m_engines.readIterator();
      while (ZmRef<MxEngine> engine = i.iterateVal()) {
	cxn->transmit(MxTelemetry::engine(engine.ptr()));
	engine->allLinks<MxAnyLink>([cxn](MxAnyLink *link) {
	  cxn->transmit(MxTelemetry::link(link));
	  return true;
	});
      }
    }
    // I/O Queues
    {
      auto i = m_queues.readIterator();
      while (Queues::Node *node = i.iterate()) {
	MxQueue *queue_ = node->val();
	const auto &key = node->key();
	uint64_t inCount, inBytes, outCount, outBytes;
	queue_->stats(inCount, inBytes, outCount, outBytes);
	cxn->transmit(queue(
	      key.p1(), queue_->head(), queue_->count_(),
	      inCount, inBytes, outCount, outBytes,
	      (uint32_t)0, (uint32_t)0,
	      (uint8_t)(key.p2() ? QueueType::Tx : QueueType::Rx)));
      }
    }
    // Databases
    if (m_dbEnv) {
      cxn->transmit(dbEnv(m_dbEnv.ptr()));
      m_dbEnv->allHosts([cxn](const ZdbHost *host) {
	cxn->transmit(dbHost(host));
	return true;
      });
      m_dbEnv->allDBs([cxn](const ZdbAny *db_) {
	cxn->transmit(db(db_));
	return true;
      });
    }
  }
}

void MxMDTelemetry::addEngine(MxEngine *engine)
{
  auto key = engine->id();
  Guard guard(m_lock);
  if (!m_engines.find(key)) m_engines.add(key, engine);
}

void MxMDTelemetry::addQueue(MxID id, bool tx, MxQueue *queue)
{
  auto key = ZuFwdTuple(id, tx);
  Guard guard(m_lock);
  if (!m_queues.find(key)) m_queues.add(key, queue);
}

void MxMDTelemetry::delQueue(MxID id, bool tx)
{
  auto key = ZuFwdTuple(id, tx);
  Guard guard(m_lock);
  m_queues.del(key);
}

void MxMDTelemetry::addDBEnv(ZdbEnv *env)
{
  Guard guard(m_lock);
  m_dbEnv = env;
}
