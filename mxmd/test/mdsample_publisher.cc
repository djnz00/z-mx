#include <zlib/ZuLib.hh>

#include <stdio.h>
#include <signal.h>

#include <mxmd/MxMD.hh>

#include <zlib/ZmRandom.hh>
#include <zlib/ZmThread.hh>

#include <zlib/ZeLog.hh>

// the tickers to use

static const char *tickers[] = {
  "6502",
  "2914",
  0
};

ZmSemaphore sem;	// used to signal exit

extern "C" { void sigint(int); };
void sigint(int sig) { sem.post(); }	// CTRL-C signal handler

int initFeed();		// initialize synthetic feed
int startFeed(MxMDLib *md, MxMDFeed *feed);

bool startFailed = 0;

struct Feed : public MxMDFeed {
  Feed(MxMDLib *md, MxID id) : MxMDFeed(md, id, 3) { }
  void start() { if (startFeed(md(), this)) startFailed = 1; }
};

void publish();		// publisher thread - generates random ticks

int subscribe();	// subscribe to events

void usage() {
  std::cerr << "Usage: mdsample_publisher CONFIG\n" << std::flush;
  ::exit(1);
}

int main(int argc, char **argv)
{
  if (argc != 2) usage();

  // configure and start logging
  ZeLog::init("mdsample_publisher");	// program name
  ZeLog::sink(ZeLog::fileSink(ZeSinkOptions{}.path("&2"))); // log to stderr
  ZeLog::start();			// start logger thread

  signal(SIGINT, &sigint);		// handle CTRL-C

  try {
    MxMDLib *md = MxMDLib::init(argv[1]); // initialize market data library

    if (!md) return 1;

    if (subscribe() < 0) {		// subscribe to library events
      md->final();
      return 1;
    }

    if (initFeed() < 0) {		// initialize synthetic feed
      md->final();
      return 1;
    }

    md->record("pubfoo");

    md->start();			// start all feeds

    if (startFailed) { md->stop(); return 1; }

    md->startTimer();

    {
      ZmThread thread(0, []{ publish(); }, ZmThreadParams().name("publish"));
					// generate ticks in other thread
      thread.join();
    }

    md->stop();				// stop all feeds

    md->final();			// clean up

  } catch (...) { }

  return 0;
}

// subscriber

void l1(MxMDOrderBook *ob, const MxMDL1Data &l1Data)
{
  // if last traded price changed, print it
  if (!!*l1Data.last)
    printf("tick %s -> %f\n",
	   ob->id().data(),
	   (double)MxValNDP{l1Data.last, ob->pxNDP()}.fp());
}

void l2(MxMDOrderBook *ob, MxDateTime stamp)
{
  printf("L2 updated %s\n", ob->id().data());
}

void addOrder(MxMDOrder *order, MxDateTime stamp)
{
  MxMDOrderBook *ob = order->orderBook();
  printf("add order %s %s %s px=%f qty=%f\n",
      ob->id().data(),
      order->id().data(),
      MxSide::name(order->data().side),
      (double)MxValNDP{order->data().price, ob->pxNDP()}.fp(),
      (double)MxValNDP{order->data().qty, ob->pxNDP()}.fp());
}

void modifiedOrder(MxMDOrder *order, MxDateTime stamp)
{
  MxMDOrderBook *ob = order->orderBook();
  printf("modified order %s %s %s px=%f qty=%f\n",
      ob->id().data(),
      order->id().data(),
      MxSide::name(order->data().side),
      (double)MxValNDP{order->data().price, ob->pxNDP()}.fp(),
      (double)MxValNDP{order->data().qty, ob->pxNDP()}.fp());
}

void deletedOrder(MxMDOrder *order, MxDateTime stamp)
{
  MxMDOrderBook *ob = order->orderBook();
  printf("canceled order %s %s %s px=%f qty=%f\n",
      ob->id().data(),
      order->id().data(),
      MxSide::name(order->data().side),
      (double)MxValNDP{order->data().price, ob->pxNDP()}.fp(),
      (double)MxValNDP{order->data().qty, ob->pxNDP()}.fp());
}

void addPxLevel(MxMDPxLevel *pxLevel, MxDateTime stamp)
{
  MxMDOrderBook *ob = pxLevel->obSide()->orderBook();
  printf("add px level %s %s px=%f qty=%f\n",
      ob->id().data(),
      MxSide::name(pxLevel->side()),
      (double)MxValNDP{pxLevel->price(), pxLevel->pxNDP()}.fp(),
      (double)MxValNDP{pxLevel->data().qty, pxLevel->qtyNDP()}.fp());
}

void updatedPxLevel(MxMDPxLevel *pxLevel, MxDateTime stamp)
{
  MxMDOrderBook *ob = pxLevel->obSide()->orderBook();
  printf("updated px level %s %s px=%f qty=%f\n",
      ob->id().data(),
      MxSide::name(pxLevel->side()),
      (double)MxValNDP{pxLevel->price(), pxLevel->pxNDP()}.fp(),
      (double)MxValNDP{pxLevel->data().qty, pxLevel->qtyNDP()}.fp());
}

void deletedPxLevel(MxMDPxLevel *pxLevel, MxDateTime stamp)
{
  MxMDOrderBook *ob = pxLevel->obSide()->orderBook();
  printf("deleted px level %s %s px=%f qty=%f\n",
      ob->id().data(),
      MxSide::name(pxLevel->side()),
      (double)MxValNDP{pxLevel->price(), pxLevel->pxNDP()}.fp(),
      (double)MxValNDP{pxLevel->data().qty, pxLevel->qtyNDP()}.fp());
}

void timer(MxDateTime now, MxDateTime &next)
{
  auto &fmt = ZmTLS<ZuDateTimeFmt::ISO, timer>();
  std::cout << "TIMER " << now.iso(fmt) << '\n'; fflush(stdout);
  next = now + ZuTime(1);
}

void loaded(MxMDVenue *venue)
{
  MxMDLib *md = venue->md();

  ZmRef<MxMDInstrHandler> instrHandler = new MxMDInstrHandler();
  instrHandler->
    l1Fn(MxMDLevel1Fn{ZmFnPtr<&l1>{}}).
    addMktLevelFn(MxMDPxLevelFn{ZmFnPtr<&addPxLevel>{}}).
    updatedMktLevelFn(MxMDPxLevelFn{ZmFnPtr<&updatedPxLevel>{}}).
    deletedMktLevelFn(MxMDPxLevelFn{ZmFnPtr<&deletedPxLevel>{}}).
    addPxLevelFn(MxMDPxLevelFn{ZmFnPtr<&addPxLevel>{}}).
    updatedPxLevelFn(MxMDPxLevelFn{ZmFnPtr<&updatedPxLevel>{}}).
    deletedPxLevelFn(MxMDPxLevelFn{ZmFnPtr<&deletedPxLevel>{}}).
    l2Fn(MxMDOrderBookFn{ZmFnPtr<&l2>{}}).
    addOrderFn(MxMDOrderFn{ZmFnPtr<&addOrder>{}}).
    modifiedOrderFn(MxMDOrderFn{ZmFnPtr<&modifiedOrder>{}}).
    deletedOrderFn(MxMDOrderFn{ZmFnPtr<&deletedOrder>{}});

  // iterate through all tickers subscribing to market data updates

  for (const char **ticker = tickers; *ticker; ticker++) {

    // look up instrument

    md->instrInvoke(MxInstrKey{*ticker, "XTKS", MxID()},
	[instrHandler, ticker](MxMDInstrument *instr) {
      if (!instr) {
	ZeLOG(Error, ([](auto &s) { s <<
	    "instrument \"" << *ticker << "\" not found"; }));
	return;
      }
      instr->subscribe(instrHandler); // subscribe to L1/L2 data
    });
  }
}

int subscribe()
{
  try {
    MxMDLib *md = MxMDLib::instance();
    if (!md) throw ZtString("MxMDLib::instance() failed");

    md->subscribe(&((new MxMDLibHandler())->
	  exceptionFn(MxMDExceptionFn{ZmFnPtr<&exception>{}}).
	  timerFn(MxMDTimerFn{ZmFnPtr<&timer>{}}).
	  refDataLoadedFn(MxMDVenueFn{ZmFnPtr<&loaded>{}})));
  } catch (const ZtString &s) {
    ZeLOG(Error, ([](auto &s) { s << "error: " << s; }));
    return -1;
  } catch (...) {
    ZeLOG(Error, "unknown exception");
    return -1;
  }

  return 0;
}

// synthetic feed (market data publisher)

int initFeed()
{
  try {
    MxMDLib *md = MxMDLib::instance();

    // add a synthetic feed to generate market data events
   
    ZmRef<MxMDFeed> feed = new Feed(md,
      "XTKS");		// Tokyo Stock Exchange
    md->addFeed(feed);

  } catch (const ZtString &s) {
    ZeLOG(Error, s);
    return -1;
  } catch (...) {
    ZeLOG(Error, "Unknown Exception");
    return -1;
  }

  return 0;
}

int startFeed(MxMDLib *md, MxMDFeed *feed)
{
  try {
    // add the venue

    ZmRef<MxMDVenue> venue = new MxMDVenue(md, feed,
      "XTKS",				// Tokyo Stock Exchange
      MxMDOrderIDScope::OBSide,		// order IDs unique within OB side
      0);
    md->addVenue(venue);

    // add a tick size table

    MxMDTickSizeTbl *tickSizeTbl = venue->addTickSizeTbl("1", 0);
    if (!tickSizeTbl) throw ZtString("MxMDVenue::addTickSizeTbl() failed");
    // tick size 1 from 0 to infinity
    tickSizeTbl->addTickSize(0, MxValueMax, 1);

    // configure lot sizes

    MxMDLotSizes lotSizes{100, 100, 100}; // odd, round, block

    // add instruments

    for (const char **ticker = tickers; *ticker; ticker++) {
      MxMDInstrRefData refData;

      // primary instrument key (venue, segment and ID)
 
      MxInstrKey instrKey{
	      *ticker,			// ID (same as symbol in this case)
	      "XTKS",			// Tokyo Stock Exchange
	      MxID()};			// null segment (segment not used)

      // minimal reference data (just the native symbol and RIC)

      refData.baseAsset = *ticker;
      refData.quoteAsset = "JPY";
      refData.idSrc = MxInstrIDSrc::EXCH;
      refData.symbol = *ticker;
      refData.altIDSrc = MxInstrIDSrc::RIC;
      refData.altSymbol = *ticker; refData.altSymbol += ".T";
      refData.pxNDP = 2;
      refData.qtyNDP = 2;

      // add the instrument

      MxMDInstrHandle instr =
	md->instrument(instrKey, 0);	// default to shard 0

      ZtString error;
      auto &sem = ZmTLS<ZmSemaphore, startFeed>();
      instr.invokeMv([&error, sem = &sem,
	  instrKey, &refData, &tickSizeTbl, &lotSizes](
	    MxMDShard *shard, ZmRef<MxMDInstrument> instr) {

	// this runs inside shard 0

	instr = shard->addInstrument(
	    ZuMv(instr), instrKey, refData, MxDateTime()); 

	if (ZuUnlikely(!instr)) {
	  error = "MxMDLib::addInstrument() failed";
	  sem->post();
	  return;
	}

	// add the order book

	ZmRef<MxMDOrderBook> orderBook = instr->addOrderBook(
	  instrKey,			// primary key for order book
	  tickSizeTbl,			// tick sizes
	  lotSizes,			// lot sizes
	  MxDateTime());
	if (ZuUnlikely(!orderBook)) {
	  error = "MxMDInstrument::addOrderBook() failed";
	  sem->post();
	  return;
	}
	sem->post();
      });
      sem.wait();
      if (ZuUnlikely(error)) throw error;
    }
    md->loaded(venue);
  } catch (const ZtString &s) {
    ZeLOG(Error, s);
    return -1;
  } catch (...) {
    ZeLOG(Error, "Unknown Exception");
    return -1;
  }
  return 0;
}

void publish()
{
  try {
    MxMDLib *md = MxMDLib::instance();
    if (!md) throw ZtString("MxMDLib::instance() failed");

    do {

      // iterate through all tickers generating a random last trade price

      for (const char **ticker = tickers; *ticker; ticker++) {

	// look up instrument

	md->instrInvoke(MxInstrKey{*ticker, "XTKS", MxID()},
	    [ticker](MxMDInstrument *instr) {
	  if (!instr) {
	    ZeLOG(Error, ([](auto &s) { s <<
		"instrument \"" << *ticker << "\" not found"; }));
	    return;
	  }

	  // get order book

	  ZmRef<MxMDOrderBook> ob = instr->orderBook("XTKS", MxID());
	  if (!ob) {
	    ZeLOG(Error, ([](auto &s) { s <<
		"XTKS order book for \"" << *ticker << "\" not found"; }));
	    return;
	  }

	  // generate random last traded price

	  MxMDL1Data l1Data{
	    .stamp = MxNow(),
	    .last = MxValNDP{ZmRand::randExc(90) + 10, 2}.value // 10-100
	  };
	  ob->l1(l1Data);

	  ob->addOrder("foo",
	      l1Data.stamp, MxSide::Sell,
	      1, MxValNDP{100.0, 2}.value, MxValNDP{100.0, 2}.value, 0);
	  ob->modifyOrder("foo",
	      l1Data.stamp, MxSide::Sell,
	      1, MxValNDP{50.0, 2}.value, MxValNDP{50.0, 2}.value, 0);
	  ob->addOrder("bar",
	      l1Data.stamp, MxSide::Sell,
	      1, MxValNDP{50.0, 2}.value, MxValNDP{50.0, 2}.value, 0);
	  ob->reduceOrder("foo",
	      l1Data.stamp, MxSide::Sell, MxValNDP{50.0, 2}.value);
	  ob->cancelOrder("bar",
	      l1Data.stamp, MxSide::Sell);
	  ob->addOrder("foo",
	      l1Data.stamp, MxSide::Sell,
	      1, MxValNDP{100.0, 2}.value, MxValNDP{100.0, 2}.value, 0);
	  ob->reduceOrder("foo",
	      l1Data.stamp, MxSide::Sell, MxValNDP{50.0, 2}.value);
	  ob->modifyOrder("foo",
	      l1Data.stamp, MxSide::Sell,
	      1, MxValNDP{50.0, 2}.value, 0, 0);
	});
      }

    // wait one second between ticks
    } while (sem.timedwait(Zm::now(3)) < 0);

  } catch (const ZtString &s) {
    ZeLOG(Error, s);
  } catch (...) {
    ZeLOG(Error, "Unknown Exception");
  }
}
