//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxMD internal API

#include <mxmd/MxMDCore.hh>

#include <stddef.h>

#include <zlib/ZmAtomic.hh>

#include <zlib/ZeLog.hh>

#include <zlib/ZiModule.hh>

#include <zlib/ZvCf.hh>
#include <zlib/ZvHeapCSV.hh>
#include <zlib/ZvHashCSV.hh>

#include <mxmd/MxMDCSV.hh>

#include <version.h>

unsigned MxMDCore::vmajor() { return MXMD_VMAJOR(MXMD_VERSION); }
unsigned MxMDCore::vminor() { return MXMD_VMINOR(MXMD_VERSION); }

class MxMDVenueMapCSV : public ZvCSV {
public:
  struct Data {
    MxID		inVenue;
    MxID		inSegment;
    MxUInt		inRank;
    MxID		outVenue;
    MxID		outSegment;
  };
  typedef ZuPOD<Data> POD;

  MxMDVenueMapCSV() {
    new ((m_pod = new POD())->ptr()) Data{};
    add(new MxIDCol("inVenue", offsetof(Data, inVenue)));
    add(new MxIDCol("inSegment", offsetof(Data, inSegment)));
    add(new MxUIntCol("inRank", offsetof(Data, inRank)));
    add(new MxIDCol("outVenue", offsetof(Data, outVenue)));
    add(new MxIDCol("outSegment", offsetof(Data, outSegment)));
  }

  void alloc(ZuRef<ZuAnyPOD> &pod) { pod = m_pod; }

  template <typename File>
  void read(const File &file, ZvCSVReadFn fn) {
    ZvCSV::readFile(file,
	ZvCSVAllocFn::Member<&MxMDVenueMapCSV::alloc>::fn(this), fn);
  }

  ZuInline POD *pod() { return m_pod.ptr(); }
  ZuInline Data *ptr() { return m_pod->ptr(); }

private:
  ZuRef<POD>	m_pod;
};

void MxMDCore::addVenueMapping_(ZuAnyPOD *pod)
{
  const auto &data = pod->as<MxMDVenueMapCSV::Data>();
  addVenueMapping(MxMDVenueMapKey(data.inVenue, data.inSegment),
      MxMDVenueMapping{data.outVenue, data.outSegment, data.inRank});
}

void MxMDCore::addTickSize_(ZuAnyPOD *pod)
{
  const auto &data = pod->as<MxMDTickSizeCSV::Data>();
  ZmRef<MxMDVenue> venue = this->venue(data.venue);
  if (!venue) throw ZtString{} << "unknown venue: " << data.venue; // FIXME
  MxMDTickSizeTbl *tbl = venue->addTickSizeTbl(data.id, data.pxNDP);
  tbl->addTickSize(data.minPrice, data.maxPrice, data.tickSize);
}

void MxMDCore::addInstrument_(ZuAnyPOD *pod)
{
  const auto &data = pod->as<MxMDInstrumentCSV::Data>();
  MxInstrKey key{data.id, data.venue, data.segment};
  MxMDInstrHandle instrHandle = instrument(key, data.shard);

  thread_local ZmSemaphore sem; // FIXME
  instrHandle.invokeMv([key, refData = data.refData,
      transactTime = data.transactTime, sem = &sem](
	MxMDShard *shard, ZmRef<MxMDInstrument> instr) {
    shard->addInstrument(ZuMv(instr), key, refData, transactTime);
    sem->post();
  });
  sem.wait();
}

void MxMDCore::addOrderBook_(ZuAnyPOD *pod)
{
  const auto &data = pod->as<MxMDOrderBookCSV::Data>();
  MxInstrKey instrKey{
    data.instruments[0], data.instrVenues[0], data.instrSegments[0]};
  MxMDInstrHandle instrHandle = instrument(instrKey);
  if (!instrHandle) throw ZtString{} << "unknown instrument: " << instrKey; // FIXME
  ZmRef<MxMDVenue> venue = this->venue(data.venue);
  if (!venue) throw ZtString{} << "unknown venue: " << data.venue; // FIXME
  MxMDTickSizeTbl *tbl = venue->addTickSizeTbl(data.tickSizeTbl, data.pxNDP);
  instrHandle.invokeMv(
      [data, venue = ZuMv(venue), tbl = ZuMv(tbl)](
	MxMDShard *shard, ZmRef<MxMDInstrument> instr) {
    if (data.legs == 1) {
      MxID venueID = data.instrVenues[0];
      if (!*venueID) venueID = data.venue;
      MxID segment = data.instrSegments[0];
      if (!*segment) segment = data.segment;
      MxIDString id = data.instruments[0];
      if (!id) id = data.id;
      instr->addOrderBook(
	  MxInstrKey{data.id, data.venue, data.segment}, tbl, data.lotSizes,
	  data.transactTime);
    } else {
      ZmRef<MxMDInstrument> instruments[MxMDNLegs];
      MxEnum sides[MxMDNLegs];
      MxRatio ratios[MxMDNLegs];
      for (unsigned i = 0, n = data.legs; i < n; i++) {
	MxID venueID = data.instrVenues[i];
	if (!*venueID) venueID = data.venue;
	MxID segment = data.instrSegments[i];
	if (!*segment) segment = data.segment;
	MxIDString id = data.instruments[i];
	if (!id) return;
	if (!i)
	  instruments[i] = ZuMv(instr);
	else {
	  instruments[i] = shard->instrument(MxInstrKey{id, venueID, segment});
	  if (!instruments[i]) return;
	}
	sides[i] = data.sides[i];
	ratios[i] = data.ratios[i];
      }
      venue->shard(shard)->addCombination(
	  data.segment, data.id, data.pxNDP, data.qtyNDP,
	  data.legs, instruments, sides, ratios, tbl, data.lotSizes,
	  data.transactTime);
    }
  });
}

static unsigned init_called_ = 0;

MxMDLib *MxMDLib::init(ZuString cf_, ZmFn<void(ZmScheduler *)> schedInitFn)
{
  auto init_called = reinterpret_cast<ZmAtomic<unsigned> *>(&init_called_);
  if (init_called->cmpXch(1, 0)) {
    ZeLOG(Error, "MxMDLib::init() called twice");
    while (*init_called < 2) Zm::yield();
    return ZmSingleton<MxMDCore, false>::instance();
  }
  ZuGuard guard([init_called]() { *init_called = 2; });

  ZmRef<ZvCf> cf = new ZvCf();

  ZmRef<MxMDCore> md;

  if (cf_)
    try {
      cf->fromFile(cf_, false);
    } catch (const ZvError &e) {
      std::cerr << (ZtString()
	  << "MxMDLib - configuration error: " << e << '\n') << std::flush;
      return nullptr;
    }
  else {
    cf->fromString(
      "mx {\n"
      "  core {\n"
      "    nThreads 4\n"	// thread IDs are 1-based
      "    threads {\n"
      "      1 { name ioRx isolated 1 }\n"
      "      2 { name ioTx isolated 1 }\n"
      "      3 { name record isolated 1 }\n"
      "      4 { name misc }\n"
      "    }\n"
      "    rxThread ioRx\n"	// I/O Rx
      "    txThread ioTx\n"	// I/O Tx
      "  }\n"
      "}\n"
      "record {\n"
      "  rxThread record\n"	// Record Rx - must be distinct from I/O Rx
      "  snapThread misc\n"	// Record snapshot - must be distinct from Rx
      "}\n"
      "replay {\n"
      "  rxThread misc\n"
      "}\n",
      false);
  }

  try {
    ZeLog::level(cf->getInt("log:level", 0, Ze::Fatal, Ze::Info));
    if (ZuString logFile = cf->get("log:file")) {
      ZeLog::sink(ZeLog::fileSink(ZeSinkOptions{}.path(logFile).
	    age(cf->getInt("log:age", 0, 1000, 8)).
	    tzOffset(cf->getInt("log:tzOffset", INT_MIN, INT_MAX, 0))));
    }
  } catch (...) { }
  ZeLog::start();

  try {
    if (ZuString heapCSV = cf->get("heap")) {
      ZeLOG(Info, "MxMDLib - configuring heap...");
      ZvHeapCSV::init(heapCSV);
    }

    if (ZuString hashCSV = cf->get("hash")) {
      ZeLOG(Info, "MxMDLib - configuring hash tables...");
      ZvHashCSV::init(hashCSV);
    }

    {
      using MxTbl = MxMDCore::MxTbl;
      using Mx = MxMDCore::Mx;

      Mx *coreMx = this->mx("core");
      if (!coreMx) throw ZvCf::Required(cf, "mx:core");

      ZeLOG(Info, "starting multiplexers...");
      {
	bool failed = false;
	{
	  auto i = mxTbl->readIterator();
	  while (MxTbl::Node *node = i.iterate()) {
	    Mx *mx = node->key();
	    if (schedInitFn) schedInitFn(mx);
	    if (!mx->start()) {
	      failed = true;
	      ZeLOG(Fatal, ([](auto &s) { s << node->key()->params().id() <<
		  " - multiplexer start failed"; }));
	      break;
	    }
	  }
	}
	if (failed) {
	  {
	    auto i = mxTbl->readIterator();
	    while (MxTbl::Node *node = i.iterate())
	      node->key()->stop(false);
	  }
	  return md = nullptr;
	}
      }

      md = new MxMDCore(ZuMv(mxTbl), coreMx);
    }

    md->init_(cf);

  } catch (const ZvError &e) {
    ZeLOG(Fatal, ([](auto &s) { s << "MxMDLib - configuration error: " << e; }));
    return md = nullptr;
  } catch (const ZtString &e) {
    ZeLOG(Fatal, ([](auto &s) { s << "MxMDLib - error: " << e; }));
    return md = nullptr;
  } catch (const ZeError &e) {
    ZeLOG(Fatal, ([](auto &s) { s << "MxMDLib - error: " << e; }));
    return md = nullptr;
  } catch (...) {
    ZeLOG(Fatal, "MxMDLib - unknown exception during init");
    return md = nullptr;
  }

  return ZmSingleton<MxMDCore, false>::instance(md);
}

MxMDLib *MxMDLib::instance()
{
  return ZmSingleton<MxMDCore, false>::instance();
}

MxMDCore::MxMDCore(ZmRef<MxTbl> mxTbl, Mx *mx) :
  MxMDLib(mx), m_mxTbl(ZuMv(mxTbl)), m_mx(mx)
{
}

void MxMDCore::init_(const ZvCf *cf)
{
  m_cf = cf;

  MxMDLib::init_(cf);

  // initialize telemetry first
  if (ZmRef<ZvCf> telCf = cf->getCf("telemetry")) {
    m_telemetry = new MxMDTelemetry();
    m_telemetry->init(this, telCf);
  }

  m_localFeed = new MxMDFeed(this, "_LOCAL", 3);
  addFeed(m_localFeed);

  if (ZmRef<ZvCf> feedsCf = cf->getCf("feeds")) {
    ZeLOG(Info, "MxMDLib - configuring feeds...");
    ZvCf::Iterator i(feedsCf);
    ZuString key;
    while (ZmRef<ZvCf> feedCf = i.subset(key)) {
      if (key == "_LOCAL") {
	ZvCf::Iterator j(feedCf);
	ZuString id;
	while (ZmRef<ZvCf> venueCf = j.subset(id))
	  addVenue(new MxMDVenue(this, m_localFeed, id,
	      venueCf->getEnum<MxMDOrderIDScope::Map>("orderIDScope"),
	      venueCf->getFlags<MxMDVenueFlags::Flags>("flags", 0)));
	continue;
      }
      ZtString e;
      ZiModule module;
      ZiModule::Path name = feedCf->get("module", true);
      int preload = feedCf->getBool("preload");
      if (preload) preload = ZiModule::Pre;
      if (module.load(name, preload, &e) < 0)
	// FIXME
	throw ZtString{} << "failed to load \"" << name << "\": " << ZuMv(e);
      MxMDFeedPluginFn pluginFn =
	(MxMDFeedPluginFn)module.resolve("MxMDFeed_plugin", &e);
      if (!pluginFn) {
	module.unload();
	// FIXME
	throw ZtString{} <<
	  "failed to resolve \"MxMDFeed_plugin\" in \"" <<
	  name << "\": " << ZuMv(e);
      }
      (*pluginFn)(this, feedCf);
    }
  }

  if (ZtString venueMap = cf->get("venueMap")) {
    MxMDVenueMapCSV csv;
    csv.read(venueMap,
	ZvCSVReadFn::Member<&MxMDCore::addVenueMapping_>::fn(this));
  }

  if (const ZtArray<ZtString> *tickSizes =
	cf->getMultiple("tickSizes", 0, INT_MAX)) {
    ZeLOG(Info, "MxMDLib - reading tick size data...");
    MxMDTickSizeCSV csv;
    for (unsigned i = 0, n = tickSizes->length(); i < n; i++)
      csv.read((*tickSizes)[i],
	  ZvCSVReadFn::Member<&MxMDCore::addTickSize_>::fn(this));
  }
  if (const ZtArray<ZtString> *instruments =
	cf->getMultiple("instruments", 0, INT_MAX)) {
    ZeLOG(Info, "MxMDLib - reading instrument reference data...");
    MxMDInstrumentCSV csv;
    for (unsigned i = 0, n = instruments->length(); i < n; i++)
      csv.read((*instruments)[i],
	  ZvCSVReadFn::Member<&MxMDCore::addInstrument_>::fn(this));
  }
  if (const ZtArray<ZtString> *orderBooks =
	cf->getMultiple("orderBooks", 0, INT_MAX)) {
    ZeLOG(Info, "MxMDLib - reading order book reference data...");
    MxMDOrderBookCSV csv;
    for (unsigned i = 0, n = orderBooks->length(); i < n; i++)
      csv.read((*orderBooks)[i],
	  ZvCSVReadFn::Member<&MxMDCore::addOrderBook_>::fn(this));
  }

  m_broadcast.init(this);

  if (ZmRef<ZvCf> cmdCf = cf->getCf("cmd")) {
    m_cmdServer = new MxMDCmdServer();
    Mx *mx = this->mx(cmdCf->get("mx", "cmd"));
    if (!mx) throw ZvCf::Required(cf, "cmd:mx");
    m_cmdServer->init(mx, cmdCf);
    initCmds();
  }

  m_record = new MxMDRecord();
  m_record->init(this, cf->getCf<true>("record"));
  m_replay = new MxMDReplay();
  m_replay->init(this, cf->getCf("replay"));

  if (ZmRef<ZvCf> publisherCf = cf->getCf("publisher")) {
    m_publisher = new MxMDPublisher();
    m_publisher->init(this, publisherCf);
  }
  if (ZmRef<ZvCf> subscriberCf = cf->getCf("subscriber")) {
    m_subscriber = new MxMDSubscriber();
    m_subscriber->init(this, subscriberCf);
  }

  ZeLOG(Info, "MxMDLib - initialized...");
}

void MxMDCore::initCmds()
{
  if (!m_cmdServer) return;

  m_cmdServer->addCmd(
      "l1", ZtString("c csv csv { type flag }\n") + lookupSyntax(),
      ZcmdFn::Member<&MxMDCore::l1>::fn(this),
      "dump L1 data",
      ZtString("usage: l1 SYMBOL [SYMBOL]... [OPTION]...\n"
	"Display level 1 market data for SYMBOL(s)\n\n"
	"Options:\n"
	"  -c, --csv\t\toutput CSV format\n") <<
	lookupOptions());
  m_cmdServer->addCmd(
      "l2", lookupSyntax(),
      ZcmdFn::Member<&MxMDCore::l2>::fn(this),
      "dump L2 data",
      ZtString("usage: l2 SYMBOL [OPTION]...\n"
	"Display level 2 market data for SYMBOL\n\nOptions:\n") <<
	lookupOptions());
  m_cmdServer->addCmd(
      "instrument", lookupSyntax(),
      ZcmdFn::Member<&MxMDCore::instrument_>::fn(this),
      "dump instrument reference data",
      ZtString("usage: instrument SYMBOL [OPTION]...\n"
	"Display instrument reference data (\"static data\") for SYMBOL\n\n"
	"Options:\n") << lookupOptions());
  m_cmdServer->addCmd(
      "ticksizes", "",
      ZcmdFn::Member<&MxMDCore::ticksizes>::fn(this),
      "dump tick sizes in CSV format",
      "usage: ticksizes [VENUE [SEGMENT]]\n"
      "dump tick sizes in CSV format");
  m_cmdServer->addCmd(
      "instruments", "",
      ZcmdFn::Member<&MxMDCore::instruments>::fn(this),
      "dump instruments in CSV format",
      "usage: instruments [VENUE [SEGMENT]]\n"
      "dump instruments in CSV format");
  m_cmdServer->addCmd(
      "orderbooks", "",
      ZcmdFn::Member<&MxMDCore::orderbooks>::fn(this),
      "dump order books in CSV format",
      "usage: orderbooks [VENUE [SEGMENT]]\n"
      "dump order books in CSV format");

#if 0
  m_cmdServer->addCmd(
      "subscribe",
      "s stop stop { type scalar }",
      ZcmdFn::Member<&MxMDCore::subscribeCmd>::fn(this),
      "subscribe to market data",
      "usage: subscribe IPCRING\n"
      "       subscribe -s ID\n"
      "subscribe to market data, receiving snapshot via ring buffer IPCRING\n"
      "Options:\n"
      "  -s, --stop=ID\tstop subscribing - detach subscriber ID\n");
#endif

  m_cmdServer->addCmd(
      "logAge", "",
      ZcmdFn{this,
	[](MxMDCore *, void *, const ZvCf *args, ZtString &out) {
	  ZuBox<int> argc = args->get("#");
	  if (argc != 1) throw ZcmdUsage{};
	  out << "ageing log files...\n";
	  ZeLog::age();
	}},
      "age log files",
      "usage: logAge\n");
  m_cmdServer->addCmd(
      "log", "",
      ZcmdFn{this,
	[](MxMDCore *md, void *, const ZvCf *args, ZtString &out) {
	  ZuBox<int> argc = args->get("#");
	  if (argc < 2) throw ZcmdUsage{};
	  ZtString message;
	  for (ZuBox<int> i = 1; i < argc; i++) {
	    if (i > 1) message << ' ';
	    message << args->get(ZuStringN<16>(i));
	  }
	  md->raise(ZeEVENT(Info, ([message](auto &s) { s << message; })));
	  out << message << '\n';
	}},
      "log informational message",
      "usage: log MESSAGE\n");
}

void MxMDCore::start()
{
  Guard guard(m_stateLock);

  if (m_telemetry) {
    raise(ZeEVENT(Info, "starting telemetry..."));
    m_telemetry->start();
  }

  if (m_cmdServer) {
    raise(ZeEVENT(Info, "starting cmd server..."));
    m_cmdServer->start();
  }

  if (m_publisher) {
    raise(ZeEVENT(Info, "starting publisher..."));
    m_publisher->start();
  }
  if (m_subscriber) {
    raise(ZeEVENT(Info, "starting subscriber..."));
    m_subscriber->start();
  }

  raise(ZeEVENT(Info, "starting feeds..."));
  allFeeds([](MxMDFeed *feed) { try { feed->start(); } catch (...) { } });
}

void MxMDCore::stop()
{
  Guard guard(m_stateLock);

  raise(ZeEVENT(Info, "stopping feeds..."));
  allFeeds([](MxMDFeed *feed) { try { feed->stop(); } catch (...) { } });

  if (m_subscriber) {
    raise(ZeEVENT(Info, "stopping subscriber..."));
    m_subscriber->stop();
    // wait for stop() to complete
    thread_local ZmSemaphore sem; // FIXME
    m_subscriber->rxInvoke([sem = &sem]() { sem->post(); });
    sem.wait();
  }
  if (m_publisher) {
    raise(ZeEVENT(Info, "stopping publisher..."));
    m_publisher->stop();
    // wait for stop() to complete
    thread_local ZmSemaphore sem; // FIXME
    m_publisher->rxInvoke([sem = &sem]() { sem->post(); });
    sem.wait();
  }

  stopReplaying();
  stopRecording();

  if (m_cmdServer) {
    raise(ZeEVENT(Info, "stopping command server..."));
    m_cmdServer->stop();
  }

  if (m_telemetry) {
    raise(ZeEVENT(Info, "stopping telemetry..."));
    m_telemetry->stop();
  }

  raise(ZeEVENT(Info, "stopping multiplexers..."));
  {
    auto i = m_mxTbl->readIterator();
    while (auto mx = i.iterateKey()) mx->stop(false);
  }
}

static unsigned final_called_ = 0;

void MxMDCore::final()
{
  {
    auto final_called = reinterpret_cast<ZmAtomic<unsigned> *>(&final_called_);
    if (final_called->cmpXch(0, 1)) {
      raise(ZeEVENT(Fatal, "MxMDCore::final() called twice"));
      return;
    }
  }

  raise(ZeEVENT(Info, "finalizing cmd server..."));

  if (m_cmdServer) m_cmdServer->final();

  raise(ZeEVENT(Info, "finalizing feeds..."));
  allFeeds([](MxMDFeed *feed) { try { feed->final(); } catch (...) { } });

  raise(ZeEVENT(Info, "finalizing telemetry..."));
  if (m_telemetry) m_telemetry->final();

  unsubscribe();
}

void MxMDCore::l1(void *, const ZvCf *args, ZtString &out)
{
  unsigned argc = ZuBox<unsigned>(args->get("#"));
  if (argc != 2) throw ZcmdUsage();
  bool csv = !!args->get("csv");
  if (csv)
    out << "stamp,status,base,last,lastQty,bid,bidQty,ask,askQty,tickDir,"
      "high,low,accVol,accVolQty,match,matchQty,surplusQty,flags\n";
  for (unsigned i = 1; i < argc; i++) {
    MxUniKey key = parseOrderBook(args, i);
    lookupOrderBook(key, 1, 1,
	[this, &out, csv](MxMDInstrument *, MxMDOrderBook *ob) -> bool {
      const MxMDL1Data &l1Data = ob->l1Data();
      unsigned pxNDP = l1Data.pxNDP;
      unsigned qtyNDP = l1Data.qtyNDP;
      MxMDFlagsStr flags;
      MxMDL1Flags::print(flags, ob->venueID(), l1Data.flags);
      if (csv) out <<
	timeFmt(l1Data.stamp) << ',' <<
	MxTradingStatus::name(l1Data.status) << ',' <<
	MxValNDP{l1Data.base, pxNDP} << ',' <<
	MxValNDP{l1Data.last, pxNDP} << ',' <<
	MxValNDP{l1Data.lastQty, qtyNDP} << ',' <<
	MxValNDP{l1Data.bid, pxNDP} << ',' <<
	MxValNDP{l1Data.bidQty, qtyNDP} << ',' <<
	MxValNDP{l1Data.ask, pxNDP} << ',' <<
	MxValNDP{l1Data.askQty, qtyNDP} << ',' <<
	MxTickDir::name(l1Data.tickDir) << ',' <<
	MxValNDP{l1Data.high, pxNDP} << ',' <<
	MxValNDP{l1Data.low, pxNDP} << ',' <<
	MxValNDP{l1Data.accVol, pxNDP} << ',' <<
	MxValNDP{l1Data.accVolQty, qtyNDP} << ',' <<
	MxValNDP{l1Data.match, pxNDP} << ',' <<
	MxValNDP{l1Data.matchQty, qtyNDP} << ',' <<
	MxValNDP{l1Data.surplusQty, qtyNDP} << ',' <<
	flags << '\n';
      else out <<
	"stamp: " << timeFmt(l1Data.stamp) <<
	"\nstatus: " << MxTradingStatus::name(l1Data.status) <<
	"\nbase: " << MxValNDP{l1Data.base, pxNDP} <<
	"\nlast: " << MxValNDP{l1Data.last, pxNDP} <<
	"\nlastQty: " << MxValNDP{l1Data.lastQty, qtyNDP} <<
	"\nbid: " << MxValNDP{l1Data.bid, pxNDP} <<
	"\nbidQty: " << MxValNDP{l1Data.bidQty, qtyNDP} <<
	"\nask: " << MxValNDP{l1Data.ask, pxNDP} <<
	"\naskQty: " << MxValNDP{l1Data.askQty, qtyNDP} <<
	"\ntickDir: " << MxTickDir::name(l1Data.tickDir) <<
	"\nhigh: " << MxValNDP{l1Data.high, pxNDP} <<
	"\nlow: " << MxValNDP{l1Data.low, pxNDP} <<
	"\naccVol: " << MxValNDP{l1Data.accVol, pxNDP} <<
	"\naccVolQty: " << MxValNDP{l1Data.accVolQty, qtyNDP} <<
	"\nmatch: " << MxValNDP{l1Data.match, pxNDP} <<
	"\nmatchQty: " << MxValNDP{l1Data.matchQty, qtyNDP} <<
	"\nsurplusQty: " << MxValNDP{l1Data.surplusQty, qtyNDP} <<
	"\nflags: " << flags << '\n';
      return true;
    });
  }
}

void MxMDCore::l2(void *, const ZvCf *args, ZtString &out)
{
  ZuBox<int> argc = args->get("#");
  if (argc != 2) throw ZcmdUsage();
  MxUniKey key = parseOrderBook(args, 1);
  lookupOrderBook(key, 1, 1,
      [this, &out](MxMDInstrument *, MxMDOrderBook *ob) -> bool {
    out << "bids:\n";
    l2_side(ob->bids(), out);
    out << "\nasks:\n";
    l2_side(ob->asks(), out);
    out << '\n';
    return true;
  });
}

void MxMDCore::l2_side(MxMDOBSide *side, ZtString &out)
{
  MxMDOrderBook *ob = side->orderBook();
  unsigned pxNDP = ob->pxNDP();
  out << "  vwap: " << MxValNDP{side->vwap(), pxNDP};
  MxID venueID = ob->venueID();
  side->allPxLevels([this, venueID, pxNDP, qtyNDP = ob->qtyNDP(), &out](
	MxMDPxLevel *pxLevel) -> bool {
    const MxMDPxLvlData &pxLvlData = pxLevel->data();
    MxMDFlagsStr flags;
    MxMDL2Flags::print(flags, venueID, pxLvlData.flags);
    out << "\n    price: " << MxValNDP{pxLevel->price(), pxNDP} <<
      " qty: " << MxValNDP{pxLvlData.qty, qtyNDP} <<
      " nOrders: " << pxLvlData.nOrders;
    if (flags) out << " flags: " << flags;
    out << " transactTime: " << timeFmt(pxLvlData.transactTime);
    return false;
  });
}

void MxMDCore::instrument_(void *, const ZvCf *args, ZtString &out)
{
  ZuBox<int> argc = args->get("#");
  if (argc != 2) throw ZcmdUsage();
  MxUniKey key = parseOrderBook(args, 1);
  lookupOrderBook(key, 1, 0,
      [&out](MxMDInstrument *instr, MxMDOrderBook *ob) -> bool {
    const MxMDInstrRefData &refData = instr->refData();
    out <<
      "ID: " << instr->id() <<
      "\nbaseAsset: " << refData.baseAsset <<
      "\nquoteAsset: " << refData.quoteAsset <<
      "\nIDSrc: " << refData.idSrc <<
      "\nsymbol: " << refData.symbol <<
      "\naltIDSrc: " << refData.altIDSrc <<
      "\naltSymbol: " << refData.altSymbol;
    if (refData.underVenue) out << "\nunderlying: " <<
      MxInstrKey{refData.underlying, refData.underVenue, refData.underSegment};
    if (*refData.mat) {
      out << "\nmat: " << refData.mat;
      if (*refData.putCall) out <<
	  "\nputCall: " << MxPutCall::name(refData.putCall) <<
	  "\nstrike: " << MxValNDP{refData.strike, refData.pxNDP};
    }
    if (*refData.outstandingUnits)
      out << "\noutstandingUnits: " << refData.outstandingUnits;
    if (*refData.adv)
      out << "\nADV: " << MxValNDP{refData.adv, refData.pxNDP};
    if (ob) {
      const MxMDLotSizes &lotSizes = ob->lotSizes();
      out <<
	"\nmarket: " << ob->venueID() <<
	"\nsegment: " << ob->segment() <<
	"\nID: " << ob->id() <<
	"\nlot sizes: " <<
	  MxValNDP{lotSizes.oddLotSize, refData.qtyNDP} << ',' <<
	  MxValNDP{lotSizes.lotSize, refData.qtyNDP} << ',' <<
	  MxValNDP{lotSizes.blockLotSize, refData.qtyNDP} <<
	"\ntick sizes:";
      ob->tickSizeTbl()->allTickSizes(
	  [pxNDP = refData.pxNDP, &out](const MxMDTickSize &ts) -> bool {
	out << "\n  " <<
	  MxValNDP{ts.minPrice(), pxNDP} << '-' <<
	  MxValNDP{ts.maxPrice(), pxNDP} << ' ' <<
	  MxValNDP{ts.tickSize(), pxNDP};
	return false;
      });
    }
    out << '\n';
    return true;
  });
}

static void writeTickSizes(
    const MxMDLib *md, MxMDTickSizeCSV &csv, ZvCSVWriteFn fn, MxID venueID)
{
  ZmFn<void(MxMDVenue *)> venueFn{[&csv, &fn](MxMDVenue *venue) {
    return venue->allTickSizeTbls(
	[&csv, &fn, venue](MxMDTickSizeTbl *tbl) {
      return tbl->allTickSizes(
	  [&csv, &fn, venue, tbl](const MxMDTickSize &ts) {
	new (csv.ptr()) MxMDTickSizeCSV::Data{MxEnum(),
	  venue->id(), tbl->id(), tbl->pxNDP(),
	  ts.minPrice(), ts.maxPrice(), ts.tickSize() };
	fn(csv.pod());
	return false;
      });
    });
  }};
  if (!*venueID)
    md->allVenues(venueFn);
  else
    if (ZmRef<MxMDVenue> venue = md->venue(venueID)) venueFn(venue);
  fn(static_cast<ZuAnyPOD *>(nullptr));
}

void MxMDCore::dumpTickSizes(ZuString path, MxID venueID)
{
  MxMDTickSizeCSV csv;
  writeTickSizes(this, csv, csv.writeFile(path), venueID);
}

void MxMDCore::ticksizes(void *, const ZvCf *args, ZtString &out)
{
  ZuBox<int> argc = args->get("#");
  if (argc < 1 || argc > 3) throw ZcmdUsage();
  MxID venueID;
  if (argc == 2) venueID = args->get("1");
  MxMDTickSizeCSV csv;
  writeTickSizes(this, csv, csv.writeData(out), venueID);
}

static void writeInstruments(
    const MxMDLib *md, MxMDInstrumentCSV &csv, ZvCSVWriteFn fn,
    MxID venueID, MxID segment)
{
  md->allInstruments(
      [&csv, &fn, venueID, segment](MxMDInstrument *instr) {
    if ((!*venueID || venueID == instr->primaryVenue()) &&
	(!*segment || segment == instr->primarySegment())) {
      new (csv.ptr()) MxMDInstrumentCSV::Data{
	instr->shard()->id(), MxMDStream::Type::AddInstrument, MxDateTime(),
	instr->primaryVenue(), instr->primarySegment(),
	instr->id(), instr->refData() };
      fn(csv.pod());
    }
    return false;
  });
  fn(static_cast<ZuAnyPOD *>(nullptr));
}

void MxMDCore::dumpInstruments(ZuString path, MxID venueID, MxID segment)
{
  MxMDInstrumentCSV csv;
  writeInstruments(this, csv, csv.writeFile(path), venueID, segment);
}

void MxMDCore::instruments(void *, const ZvCf *args, ZtString &out)
{
  ZuBox<int> argc = args->get("#");
  if (argc < 1 || argc > 3) throw ZcmdUsage();
  MxID venueID, segment;
  if (argc == 2) venueID = args->get("1");
  if (argc == 3) segment = args->get("2");
  MxMDInstrumentCSV csv;
  writeInstruments(this, csv, csv.writeData(out), venueID, segment);
}

static void writeOrderBooks(
    const MxMDLib *md, MxMDOrderBookCSV &csv, ZvCSVWriteFn fn,
    MxID venueID, MxID segment)
{
  md->allOrderBooks([&csv, &fn, venueID, segment](MxMDOrderBook *ob) -> bool {
    if ((!*venueID || venueID == ob->venueID()) &&
	(!*segment || segment == ob->segment())) {
      MxMDOrderBookCSV::Data *data =
	new (csv.ptr()) MxMDOrderBookCSV::Data{
	  ob->shard()->id(), MxMDStream::Type::AddOrderBook, MxDateTime(),
	  ob->venueID(), ob->segment(), ob->id(),
	  ob->pxNDP(), ob->qtyNDP(),
	  ob->legs(), ob->tickSizeTbl()->id(), ob->lotSizes() };
      for (unsigned i = 0, n = ob->legs(); i < n; i++) {
	MxMDInstrument *instr;
	if (!(instr = ob->instrument(i))) break;
	data->instrVenues[i] = instr->primaryVenue();
	data->instrSegments[i] = instr->primarySegment();
	data->instruments[i] = instr->id();
	data->sides[i] = ob->side(i);
	data->ratios[i] = ob->ratio(i);
      }
      fn(csv.pod());
    }
    return false;
  });
  fn(static_cast<ZuAnyPOD *>(nullptr));
}

void MxMDCore::dumpOrderBooks(ZuString path, MxID venueID, MxID segment)
{
  MxMDOrderBookCSV csv;
  writeOrderBooks(this, csv, csv.writeFile(path), venueID, segment);
}

void MxMDCore::orderbooks(void *, const ZvCf *args, ZtString &out)
{
  ZuBox<int> argc = args->get("#");
  if (argc < 1 || argc > 3) throw ZcmdUsage();
  MxID venueID, segment;
  if (argc == 2) venueID = args->get("1");
  if (argc == 3) segment = args->get("2");
  MxMDOrderBookCSV csv;
  writeOrderBooks(this, csv, csv.writeData(out), venueID, segment);
}

bool MxMDCore::record(ZuString path)
{
  return m_record->record(path);
}
ZtString MxMDCore::stopRecording()
{
  return m_record->stopRecording();
}

bool MxMDCore::replay(ZuString path, MxDateTime begin, bool filter)
{
  m_mx->del(&m_timer);
  return m_replay->replay(path, begin, filter);
}
ZtString MxMDCore::stopReplaying()
{
  return m_replay->stopReplaying();
}

#if 0
void MxMDCore::subscribeCmd(ZvCf *args, ZtArray<char> &out)
{
  ZuBox<int> argc = args->get("#");
  if (ZtString id_ = args->get("stop")) {
    ZuBox<int> id = ZvCf::toInt("spin", id_, 0, 63);
    MxMDStream::detach(m_broadcast, m_broadcast.id());
    m_broadcast.close();
    out << "detached " << id << "\n";
    return;
  }
  if (argc != 2) throw ZcmdUsage();
  ZiVBxRingParams ringParams(args->get("1"), m_broadcast.params());
  if (ZtString spin = args->get("spin"))
    ringParams.spin(ZvCf::toInt("spin", spin, 0, INT_MAX));
  if (ZtString timeout = args->get("timeout"))
    ringParams.timeout(ZvCf::toInt("timeout", timeout, 0, 3600));
  if (!m_broadcast.open())
    throw ZtString{} << '"' << m_broadcast.params().name() <<
	"\": failed to open IPC shared memory ring buffer";
  m_snapper.snap(ringParams);
}
#endif

void MxMDCore::pad(Hdr &hdr)
{
  using namespace MxMDStream;

  switch ((int)hdr.type) {
    case Type::AddVenue:	hdr.pad<AddVenue>(); break;
    case Type::AddTickSizeTbl:	hdr.pad<AddTickSizeTbl>(); break;
    case Type::ResetTickSizeTbl: hdr.pad<ResetTickSizeTbl>(); break;
    case Type::AddTickSize:	hdr.pad<AddTickSize>(); break;
    case Type::AddInstrument:	hdr.pad<AddInstrument>(); break;
    case Type::UpdateInstrument: hdr.pad<UpdateInstrument>(); break;
    case Type::AddOrderBook:	hdr.pad<AddOrderBook>(); break;
    case Type::DelOrderBook:	hdr.pad<DelOrderBook>(); break;
    case Type::AddCombination:	hdr.pad<AddCombination>(); break;
    case Type::DelCombination:	hdr.pad<DelCombination>(); break;
    case Type::UpdateOrderBook:	hdr.pad<UpdateOrderBook>(); break;
    case Type::TradingSession:	hdr.pad<TradingSession>(); break;
    case Type::L1:		hdr.pad<L1>(); break;
    case Type::PxLevel:		hdr.pad<PxLevel>(); break;
    case Type::L2:		hdr.pad<L2>(); break;
    case Type::AddOrder:	hdr.pad<AddOrder>(); break;
    case Type::ModifyOrder:	hdr.pad<ModifyOrder>(); break;
    case Type::CancelOrder:	hdr.pad<CancelOrder>(); break;
    case Type::ResetOB:		hdr.pad<ResetOB>(); break;
    case Type::AddTrade:	hdr.pad<AddTrade>(); break;
    case Type::CorrectTrade:	hdr.pad<CorrectTrade>(); break;
    case Type::CancelTrade:	hdr.pad<CancelTrade>(); break;
    case Type::RefDataLoaded:	hdr.pad<RefDataLoaded>(); break;
    default: break;
  }
}

void MxMDCore::apply(const Hdr &hdr, bool filter)
{
  using namespace MxMDStream;

  switch ((int)hdr.type) {
    case Type::AddVenue:
      {
	const AddVenue &obj = hdr.as<AddVenue>();
	this->venue_(obj.id, obj.orderIDScope, obj.flags);
      }
      break;
    case Type::AddTickSizeTbl:
      {
	const AddTickSizeTbl &obj = hdr.as<AddTickSizeTbl>();
	if (ZmRef<MxMDVenue> venue = this->venue(obj.venue))
	  venue->addTickSizeTbl(obj.id, obj.pxNDP);
      }
      break;
    case Type::ResetTickSizeTbl:
      {
	const ResetTickSizeTbl &obj = hdr.as<ResetTickSizeTbl>();
	if (ZmRef<MxMDVenue> venue = this->venue(obj.venue))
	  if (ZmRef<MxMDTickSizeTbl> tbl = venue->tickSizeTbl(obj.id))
	    tbl->reset();
      }
      break;
    case Type::AddTickSize:
      {
	const AddTickSize &obj = hdr.as<AddTickSize>();
	if (ZmRef<MxMDVenue> venue = this->venue(obj.venue))
	  if (ZmRef<MxMDTickSizeTbl> tbl = venue->tickSizeTbl(obj.id)) {
	    if (ZuUnlikely(tbl->pxNDP() != obj.pxNDP)) {
	      unsigned oldNDP = obj.pxNDP, newNDP = tbl->pxNDP();
#ifdef adjustNDP
#undef adjustNDP
#endif
#define adjustNDP(v) (MxValNDP{v, oldNDP}.adjust(newNDP))
	      tbl->addTickSize(
		adjustNDP(obj.minPrice),
		adjustNDP(obj.maxPrice),
		adjustNDP(obj.tickSize));
#undef adjustNDP
	    } else
	      tbl->addTickSize(obj.minPrice, obj.maxPrice, obj.tickSize);
	  }
      }
      break;
    case Type::TradingSession:
      {
	const TradingSession &obj = hdr.as<TradingSession>();
	if (ZmRef<MxMDVenue> venue = this->venue(obj.venue))
	  venue->tradingSession(
	      MxMDSegment{obj.segment, obj.session, obj.stamp});
      }
      break;
    case Type::AddInstrument:
      if (hdr.shard < nShards()) {
	const AddInstrument &obj = hdr.as<AddInstrument>();
	shard(hdr.shard, [obj = obj](MxMDShard *shard) {
	  shard->addInstrument(shard->instrument(obj.key),
	      obj.key, obj.refData, obj.transactTime);
	});
      }
      break;
    case Type::UpdateInstrument:
      if (hdr.shard < nShards()) {
	const UpdateInstrument &obj = hdr.as<UpdateInstrument>();
	shard(hdr.shard, [obj = obj](MxMDShard *shard) {
	  if (ZmRef<MxMDInstrument> instr = shard->instrument(obj.key))
	    instr->update(obj.refData, obj.transactTime);
	});
      }
      break;
    case Type::AddOrderBook:
      if (hdr.shard < nShards()) {
	const AddOrderBook &obj = hdr.as<AddOrderBook>();
	shard(hdr.shard, [obj = obj](MxMDShard *shard) mutable {
	  if (ZmRef<MxMDVenue> venue = shard->md()->venue(obj.key.venue))
	    if (ZmRef<MxMDTickSizeTbl> tbl =
		    venue->tickSizeTbl(obj.tickSizeTbl))
	      if (ZmRef<MxMDInstrument> instr =
		  shard->instrument(obj.instrument)) {
		if (ZuUnlikely(instr->refData().qtyNDP != obj.qtyNDP)) {
		  unsigned newNDP = instr->refData().qtyNDP;
#ifdef adjustNDP
#undef adjustNDP
#endif
#define adjustNDP(v) v = MxValNDP{v, obj.qtyNDP}.adjust(newNDP)
		  adjustNDP(obj.lotSizes.oddLotSize);
		  adjustNDP(obj.lotSizes.lotSize);
		  adjustNDP(obj.lotSizes.blockLotSize);
#undef adjustNDP
		  obj.qtyNDP = newNDP;
		}
		instr->addOrderBook(
		    obj.key, tbl, obj.lotSizes, obj.transactTime);
	      }
	});
      }
      break;
    case Type::DelOrderBook:
      if (hdr.shard < nShards()) {
	const DelOrderBook &obj = hdr.as<DelOrderBook>();
	shard(hdr.shard, [obj = obj](MxMDShard *shard) {
	  if (ZmRef<MxMDOrderBook> ob = shard->orderBook(obj.key))
	    ob->instrument()->delOrderBook(
	      obj.key.venue, obj.key.segment, obj.transactTime);
	});
      }
      break;
    case Type::AddCombination:
      if (hdr.shard < nShards()) {
	const AddCombination &obj = hdr.as<AddCombination>();
	shard(hdr.shard, [obj = obj](MxMDShard *shard) {
	  if (ZmRef<MxMDVenue> venue = shard->md()->venue(obj.key.venue))
	    if (ZmRef<MxMDTickSizeTbl> tbl =
		venue->tickSizeTbl(obj.tickSizeTbl)) {
	    ZmRef<MxMDInstrument> instruments[MxMDNLegs];
	    for (unsigned i = 0; i < obj.legs; i++)
	      if (!(instruments[i] = shard->instrument(obj.instruments[i])))
		return;
	    venue->shard(shard)->addCombination(
		obj.key.segment, obj.key.id,
		obj.pxNDP, obj.qtyNDP,
		obj.legs, instruments, obj.sides, obj.ratios,
		tbl, obj.lotSizes, obj.transactTime);
	  }
	});
      }
      break;
    case Type::DelCombination:
      if (hdr.shard < nShards()) {
	const DelCombination &obj = hdr.as<DelCombination>();
	shard(hdr.shard, [obj = obj](MxMDShard *shard) {
	  if (ZmRef<MxMDVenue> venue = shard->md()->venue(obj.key.venue))
	    venue->shard(shard)->delCombination(
		obj.key.segment, obj.key.id, obj.transactTime);
	});
      }
      break;
    case Type::UpdateOrderBook:
      if (hdr.shard < nShards()) {
	const UpdateOrderBook &obj = hdr.as<UpdateOrderBook>();
	shard(hdr.shard, [obj = obj](MxMDShard *shard) {
	  if (ZmRef<MxMDVenue> venue = shard->md()->venue(obj.key.venue))
	    if (ZmRef<MxMDTickSizeTbl> tbl =
		venue->tickSizeTbl(obj.tickSizeTbl))
	      if (ZmRef<MxMDOrderBook> ob = shard->orderBook(obj.key))
		ob->update(tbl, obj.lotSizes, obj.transactTime);
	});
      }
      break;
    case Type::L1:
      if (hdr.shard < nShards()) {
	const L1 &obj = hdr.as<L1>();
	shard(hdr.shard, [obj = obj, filter](MxMDShard *shard) mutable {
	  // inconsistent NDP handled within MxMDOrderBook::l1()
	  ZmRef<MxMDOrderBook> ob = shard->orderBook(obj.key);
	  if (ob && (!filter || ob->handler())) ob->l1(obj.data);
	});
      }
      break;
    case Type::PxLevel:
      if (hdr.shard < nShards()) {
	const PxLevel &obj = hdr.as<PxLevel>();
	shard(hdr.shard, [obj = obj, filter](MxMDShard *shard) mutable {
	  ZmRef<MxMDOrderBook> ob = shard->orderBook(obj.key);
	  if (ob && (!filter || ob->handler())) {
	    if (ZuUnlikely(ob->pxNDP() != obj.pxNDP))
	      obj.price = MxValNDP{obj.price, obj.pxNDP}.adjust(ob->pxNDP());
	    if (ZuUnlikely(ob->qtyNDP() != obj.qtyNDP))
	      obj.qty = MxValNDP{obj.qty, obj.qtyNDP}.adjust(ob->qtyNDP());
	    ob->pxLevel(obj.side, obj.transactTime, obj.delta,
		obj.price, obj.qty, obj.nOrders, obj.flags);
	  }
	});
      }
      break;
    case Type::L2:
      if (hdr.shard < nShards()) {
	const L2 &obj = hdr.as<L2>();
	shard(hdr.shard, [obj = obj, filter](MxMDShard *shard) mutable {
	  ZmRef<MxMDOrderBook> ob = shard->orderBook(obj.key);
	  if (ob && (!filter || ob->handler()))
	    ob->l2(obj.stamp, obj.updateL1);
	});
      }
      break;
    case Type::AddOrder:
      if (hdr.shard < nShards()) {
	const AddOrder &obj = hdr.as<AddOrder>();
	shard(hdr.shard, [obj = obj, filter](MxMDShard *shard) mutable {
	  ZmRef<MxMDOrderBook> ob = shard->orderBook(obj.key);
	  if (ob && (!filter || ob->handler())) {
	    if (ZuUnlikely(ob->pxNDP() != obj.pxNDP))
	      obj.price = MxValNDP{obj.price, obj.pxNDP}.adjust(ob->pxNDP());
	    if (ZuUnlikely(ob->qtyNDP() != obj.qtyNDP))
	      obj.qty = MxValNDP{obj.qty, obj.qtyNDP}.adjust(ob->qtyNDP());
	    ob->addOrder(obj.orderID, obj.transactTime,
		obj.side, obj.rank, obj.price, obj.qty, obj.flags);
	  }
	});
      }
      break;
    case Type::ModifyOrder:
      if (hdr.shard < nShards()) {
	const ModifyOrder &obj = hdr.as<ModifyOrder>();
	shard(hdr.shard, [obj = obj, filter](MxMDShard *shard) mutable {
	  ZmRef<MxMDOrderBook> ob = shard->orderBook(obj.key);
	  if (ob && (!filter || ob->handler())) {
	    if (ZuUnlikely(ob->pxNDP() != obj.pxNDP))
	      obj.price = MxValNDP{obj.price, obj.pxNDP}.adjust(ob->pxNDP());
	    if (ZuUnlikely(ob->qtyNDP() != obj.qtyNDP))
	      obj.qty = MxValNDP{obj.qty, obj.qtyNDP}.adjust(ob->qtyNDP());
	    ob->modifyOrder(obj.orderID, obj.transactTime,
		obj.side, obj.rank, obj.price, obj.qty, obj.flags);
	  }
	});
      }
      break;
    case Type::CancelOrder:
      if (hdr.shard < nShards()) {
	const CancelOrder &obj = hdr.as<CancelOrder>();
	shard(hdr.shard, [obj = obj, filter](MxMDShard *shard) mutable {
	  ZmRef<MxMDOrderBook> ob = shard->orderBook(obj.key);
	  if (ob && (!filter || ob->handler()))
	    ob->cancelOrder(obj.orderID, obj.transactTime, obj.side);
	});
      }
      break;
    case Type::ResetOB:
      if (hdr.shard < nShards()) {
	const ResetOB &obj = hdr.as<ResetOB>();
	shard(hdr.shard, [obj = obj, filter](MxMDShard *shard) mutable {
	  ZmRef<MxMDOrderBook> ob = shard->orderBook(obj.key);
	  if (ob && (!filter || ob->handler()))
	    ob->reset(obj.transactTime);
	});
      }
      break;
    case Type::AddTrade:
      if (hdr.shard < nShards()) {
	const AddTrade &obj = hdr.as<AddTrade>();
	shard(hdr.shard, [obj = obj, filter](MxMDShard *shard) mutable {
	  ZmRef<MxMDOrderBook> ob = shard->orderBook(obj.key);
	  if (ob && (!filter || ob->handler())) {
	    if (ZuUnlikely(ob->pxNDP() != obj.pxNDP))
	      obj.price = MxValNDP{obj.price, obj.pxNDP}.adjust(ob->pxNDP());
	    if (ZuUnlikely(ob->qtyNDP() != obj.qtyNDP))
	      obj.qty = MxValNDP{obj.qty, obj.qtyNDP}.adjust(ob->qtyNDP());
	    ob->addTrade(obj.tradeID, obj.transactTime, obj.price, obj.qty);
	  }
	});
      }
      break;
    case Type::CorrectTrade:
      if (hdr.shard < nShards()) {
	const CorrectTrade &obj = hdr.as<CorrectTrade>();
	shard(hdr.shard, [obj = obj, filter](MxMDShard *shard) mutable {
	  ZmRef<MxMDOrderBook> ob = shard->orderBook(obj.key);
	  if (ob && (!filter || ob->handler())) {
	    if (ZuUnlikely(ob->pxNDP() != obj.pxNDP))
	      obj.price = MxValNDP{obj.price, obj.pxNDP}.adjust(ob->pxNDP());
	    if (ZuUnlikely(ob->qtyNDP() != obj.qtyNDP))
	      obj.qty = MxValNDP{obj.qty, obj.qtyNDP}.adjust(ob->qtyNDP());
	    ob->correctTrade(
		obj.tradeID, obj.transactTime, obj.price, obj.qty);
	  }
	});
      }
      break;
    case Type::CancelTrade:
      if (hdr.shard < nShards()) {
	const CancelTrade &obj = hdr.as<CancelTrade>();
	shard(hdr.shard, [obj = obj, filter](MxMDShard *shard) mutable {
	  ZmRef<MxMDOrderBook> ob = shard->orderBook(obj.key);
	  if (ob && (!filter || ob->handler())) {
	    if (ZuUnlikely(ob->pxNDP() != obj.pxNDP))
	      obj.price = MxValNDP{obj.price, obj.pxNDP}.adjust(ob->pxNDP());
	    if (ZuUnlikely(ob->qtyNDP() != obj.qtyNDP))
	      obj.qty = MxValNDP{obj.qty, obj.qtyNDP}.adjust(ob->qtyNDP());
	    ob->cancelTrade(obj.tradeID, obj.transactTime, obj.price, obj.qty);
	  }
	});
      }
      break;
    case Type::RefDataLoaded:
      {
	const RefDataLoaded &obj = hdr.as<RefDataLoaded>();
	if (ZmRef<MxMDVenue> venue = this->venue(obj.venue))
	  this->loaded(venue);
      }
      break;
    case Type::HeartBeat:
    case Type::Wake:
    case Type::EndOfSnapshot:
    case Type::Login:
    case Type::ResendReq:
      break;
    default:
      raise(ZeEVENT(Error, "MxMDLib - unknown message type"));
      break;
  }
}

ZmRef<MxMDVenue> MxMDCore::venue_(MxID id, MxEnum orderIDScope, MxFlags flags)
{
  ZmRef<MxMDVenue> venue;
  if (ZuLikely(venue = MxMDLib::venue(id))) return venue;
  venue = new MxMDVenue(this, m_localFeed, id, orderIDScope, flags);
  addVenue(venue);
  return venue;
}

void MxMDCore::startTimer(MxDateTime begin)
{
  ZuTime next = !begin ? Zm::now() : begin.zmTime();
  {
    Guard guard(m_timerLock);
    m_timerNext = next;
  }
  m_mx->add(ZmFn<>::Member<&MxMDCore::timer>::fn(this), next, &m_timer);
}

void MxMDCore::stopTimer()
{
  m_mx->del(&m_timer);
  Guard guard(m_timerLock);
  m_timerNext = ZuTime();
}

void MxMDCore::timer()
{
  MxDateTime now, next;
  {
    Guard guard(m_timerLock);
    now = m_timerNext;
  }
  this->handler()->timer(now, next);
  {
    Guard guard(m_timerLock);
    m_timerNext = !next ? ZuTime() : next.zmTime();
  }
  if (!next)
    m_mx->del(&m_timer);
  else
    m_mx->add(ZmFn<>::Member<&MxMDCore::timer>::fn(this),
	next.zmTime(), &m_timer);
}
