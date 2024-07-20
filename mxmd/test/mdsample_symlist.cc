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

#include <iostream>

ZmSemaphore stop;	// used to signal exit

extern "C" { void sigint(int); };
void sigint(int sig) { stop.post(); }	// CTRL-C signal handler

void usage() {
  std::cerr <<
    "Usage: mdsample_symlist CONFIG RICS\n"
    "    CONFIG - configuration file\n"
    "    RICS - file containing RICs to subscribe to\n"
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

typedef ZmLHash<MxIDString> Syms; // hash table of syms
static ZmRef<Syms> syms;

void refDataLoaded(MxMDVenue *venue)
{
  std::cout << "reference data loaded for " << venue->id() << '\n';
}

static ZmRef<MxMDInstrHandler> instrHandler;

void addInstrument(MxMDInstrument *instrument, MxDateTime)
{
  if (!syms) return;
  if (!syms->findKey(instrument->refData().symbol)) return;
  instrument->subscribe(instrHandler);
}

int main(int argc, char **argv)
{
  if (argc != 3) usage();

  syms = new Syms();
  (instrHandler = new MxMDInstrHandler())->
    l1Fn(MxMDLevel1Fn::Ptr<&l1>::fn()).
    addPxLevelFn(MxMDPxLevelFn::Ptr<&pxLevel>::fn()).
    updatedPxLevelFn(MxMDPxLevelFn::Ptr<&pxLevel>::fn()).
    deletedPxLevelFn(MxMDPxLevelFn::Ptr<&deletedPxLevel>::fn()).
    l2Fn(MxMDOrderBookFn::Ptr<&l2>::fn());

  // read rics from file into hash table
  {
    FILE *f = fopen(argv[2], "r");
    if (f) {
      int i = 0;
      do {
	MxIDString sym;
	if (!fgets(sym.data(), sym.size() - 1, f)) break;
	sym.calcLength();
	sym.chomp();
	syms->add(sym);
      } while (i < 10000);
      fclose(f);
    } else {
      std::cerr << "could not open " << argv[2] << '\n';
    }
  }

  signal(SIGINT, &sigint);		// handle CTRL-C

  try {
    MxMDLib *md = MxMDLib::init(argv[1]); // initialize market data library
    if (!md) return 1;

    md->subscribe(&((new MxMDLibHandler())->
	exceptionFn(MxMDExceptionFn::Ptr<&exception>::fn()).
	refDataLoadedFn(MxMDVenueFn::Ptr<&refDataLoaded>::fn()).
	addInstrumentFn(MxMDInstrumentFn::Ptr<&addInstrument>::fn())));

    md->start();			// start all feeds

    stop.wait();			// wait for stop

    md->stop();				// stop
    md->final();			// clean up

  } catch (...) { }

  syms = nullptr;

  return 0;
}
