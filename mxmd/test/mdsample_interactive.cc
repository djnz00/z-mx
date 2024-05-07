//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxMD sample subscriber application

#include <zlib/ZuLib.hh>

#include <stdio.h>
#include <signal.h>

#include <zlib/ZeLog.hh>

#include <mxmd/MxMD.hh>
#include <mxmd/MxMDCSV.hh>

#include <iostream>

ZmSemaphore stop;	// used to signal exit

extern "C" { void sigint(int); };
void sigint(int sig) { stop.post(); }	// CTRL-C signal handler

void usage() {
  std::cerr <<
    "usage: mdsample_interactive CONFIG [SYMBOLS]\n"
    "    CONFIG - configuration file\n"
    "    SYMBOLS - optional file containing symbols to subscribe to\n"
    << std::flush;
  Zm::exit(1);
}

void l1(MxMDOrderBook *ob, const MxMDL1Data &)
{
  using namespace ZuFmt;
  const MxMDL1Data &l1 = ob->l1Data();
  MxMDFlagsStr flags;
  MxMDL1Flags::print(flags, ob->venueID(), l1.flags);
  std::cout << ob->instrument()->id() <<
    " stamp: " <<
      ZuBox<unsigned>(l1.stamp.hhmmss()).fmt(Right<6>()) << '.' <<
	ZuBox<unsigned>(l1.stamp.nsec()).fmt(Right<9>()) <<
    ' ' << MxTradingStatus::name(l1.status) <<
    ' ' << MxTickDir::name(l1.tickDir) <<
    " last: " << MxValNDP{l1.last, l1.pxNDP}.fmt(FP<-3>()) << '/' <<
      MxValNDP{l1.lastQty, l1.qtyNDP}.fmt(FP<-3>()) <<
    " bid: " << MxValNDP{l1.bid, l1.pxNDP}.fmt(FP<-3>()) << '/' <<
      MxValNDP{l1.bidQty, l1.qtyNDP}.fmt(FP<-3>()) <<
    " ask: " << MxValNDP{l1.ask, l1.pxNDP}.fmt(FP<-3>()) << '/' <<
      MxValNDP{l1.askQty, l1.qtyNDP}.fmt(FP<-3>()) <<
    " high: " << MxValNDP{l1.high, l1.pxNDP}.fmt(FP<-3>()) <<
    " low: " << MxValNDP{l1.low, l1.pxNDP}.fmt(FP<-3>()) <<
    " accVol: " << MxValNDP{l1.accVol, l1.pxNDP}.fmt(FP<-3>()) << '/' <<
      MxValNDP{l1.accVolQty, l1.qtyNDP}.fmt(FP<-3>()) <<
    " match: " << MxValNDP{l1.match, l1.pxNDP}.fmt(FP<-3>()) << '/' <<
      MxValNDP{l1.matchQty, l1.qtyNDP}.fmt(FP<-3>()) <<
    " surplusQty: " << MxValNDP{l1.surplusQty, l1.qtyNDP}.fmt(FP<-3>()) <<
    " flags: " << flags << '\n';
}

void pxLevel(MxMDPxLevel *pxLevel, MxDateTime stamp)
{
  using namespace ZuFmt;
  const MxMDPxLvlData &pxLvlData = pxLevel->data();
  std::cout <<
    ZuBox<unsigned>(stamp.hhmmss()).fmt(Right<6>()) << '.' <<
      ZuBox<unsigned>(stamp.nsec()).fmt(Right<9>()) <<
    (pxLevel->side() == MxSide::Buy ? " bid" : " ask") <<
    " price: " << MxValNDP{pxLevel->price(), pxLevel->pxNDP()}.fmt(FP<-3>()) <<
    " qty: " << MxValNDP{pxLvlData.qty, pxLevel->qtyNDP()}.fmt(FP<-3>()) <<
    " nOrders: " << pxLvlData.nOrders.fmt(FP<-3>()) << '\n';
}

void deletedPxLevel(MxMDPxLevel *pxLevel, MxDateTime stamp)
{
  using namespace ZuFmt;
  // const MxMDPxLvlData &pxLvlData = pxLevel->data();
  std::cout <<
    ZuBox<unsigned>(stamp.hhmmss()).fmt(Right<6>()) << '.' <<
      ZuBox<unsigned>(stamp.nsec()).fmt(Right<9>()) <<
    (pxLevel->side() == MxSide::Buy ? " bid" : " ask") <<
    " price: " << MxValNDP{pxLevel->price(), pxLevel->pxNDP()}.fmt(FP<-3>()) <<
    " DELETED" << '\n';
}

void l2(MxMDOrderBook *ob, MxDateTime stamp)
{
  std::cout << "L2 updated\n";
}

typedef ZmHash<MxUniKey> Keys; // hash table of keys
static ZmRef<Keys> keys;

void refDataLoaded(MxMDVenue *venue)
{
  std::cout << "reference data loaded for " << venue->id() << '\n';
}

static ZmRef<MxMDInstrHandler> instrHandler;

void addInstrument(MxMDInstrument *instr, MxDateTime)
{
  if (!keys) return;
  bool matched = false;
  std::cerr << "SUBSCRIBED KEYS\n";
  { auto i = keys->readIterator(); while (MxUniKey key = i.iterateKey()) { std::cerr << key << '\n'; } }
  std::cerr << "RCVD KEYS\n";
  instr->keys([&matched](const MxUniKey &key) {
      if (keys->find(key)) matched = true;
      std::cerr << key << (matched ? " MATCHED\n" : " NOT MATCHED\n") << std::flush;
    });
  if (matched) instr->subscribe(instrHandler);
}

void subscribe(void *, const ZvCf *args, ZtString &out)
{
  MxMDLib *md = MxMDLib::instance();
  if (!md) throw ZtString("MxMDLib::instance() failed");
  ZmRef<MxMDInstrument> instrument;
  unsigned argc = ZuBox<unsigned>(args->get("#"));
  if (argc != 2) throw ZvCmdUsage();
  MxUniKey key = md->parseInstrument(args, 1);
  md->lookupInstrument(key, 0, [&key, &out](MxMDInstrument *instr) -> bool {
    if (!instr) {
      keys->add(key);
      out << "subscription pending\n";
    } else {
      instr->subscribe(instrHandler);
      out << "subscribed\n";
    }
    return true;
  });
}

int main(int argc, char **argv)
{
  if (argc < 2 || argc > 3) usage();
  if (!argv[1]) usage();

  ZeLog::init("mdsample_interactive");	// program name
  ZeLog::sink(ZeLog::fileSink(ZeSinkOptions{}.path("&2"))); // log to stderr
  ZeLog::start();			// start logger thread

  keys = new Keys();
  (instrHandler = new MxMDInstrHandler())->
    l1Fn(MxMDLevel1Fn::Ptr<&l1>::fn()).
    addPxLevelFn(MxMDPxLevelFn::Ptr<&pxLevel>::fn()).
    updatedPxLevelFn(MxMDPxLevelFn::Ptr<&pxLevel>::fn()).
    deletedPxLevelFn(MxMDPxLevelFn::Ptr<&deletedPxLevel>::fn()).
    l2Fn(MxMDOrderBookFn::Ptr<&l2>::fn());

  // read keys from file into hash table
  if (argv[2]) {
    MxUniKeyCSV csv;
    csv.read(argv[2], ZvCSVReadFn{[](ZuAnyPOD *pod) {
	keys->add(MxUniKeyCSV::key(pod));
      }});
  }

  signal(SIGINT, &sigint);		// handle CTRL-C

  try {
    MxMDLib *md = MxMDLib::init(argv[1]); // initialize market data library
    if (!md) return 1;

    // add interactive subscribe command
    md->addCmd("subscribe",
	md->lookupSyntax(),
	ZvCmdFn::Ptr<&subscribe>::fn(),
	"subscribe",
	ZtString{} << "usage: subscribe SYMBOL [OPTION]...\n\nOptions:\n" <<
	  md->lookupOptions());

    md->subscribe(&((new MxMDLibHandler())->
	exceptionFn(MxMDExceptionFn::Ptr<&exception>::fn()).
	refDataLoadedFn(MxMDVenueFn::Ptr<&refDataLoaded>::fn()).
	addInstrumentFn(MxMDInstrumentFn::Ptr<&addInstrument>::fn())));

    md->start();			// start all feeds

    stop.wait();			// wait for stop

    md->stop();				// stop
    md->final();			// clean up

  } catch (...) { }

  keys = nullptr;

  return 0;
}
