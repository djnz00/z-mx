//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// MxMD recording tool

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
    "Usage: mdrecord CONFIG RECFILE [SYMBOLS]\n"
    "    CONFIG\t- configuration file\n"
    "    RECFILE\t- recording file\n"
    "    SYMBOLS\t- optional file containing symbols to subscribe to\n"
    << std::flush;
  Zm::exit(1);
}

void l1(MxMDOrderBook *, const MxMDL1Data &) { }
void l2(MxMDOrderBook *, MxDateTime) { }

typedef ZmLHash<MxIDString> Syms; // hash table of syms
static ZmRef<Syms> syms;

void refDataLoaded(MxMDVenue *venue)
{
  MxMDLib *md = venue->md();
  md->dumpTickSizes(MxTxtString() << venue->id() << "_tickSizes.csv");
  md->dumpInstruments(MxTxtString() << venue->id() << "_instruments.csv");
  md->dumpOrderBooks(MxTxtString() << venue->id() << "_orderBooks.csv");
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
  if (argc < 3 || argc > 4) usage();
  if (!argv[1] || !argv[2]) usage();

  syms = new Syms();
  (instrHandler = new MxMDInstrHandler())->
    l1Fn(MxMDLevel1Fn{ZmFnUnbound<&l1>{}}).
    l2Fn(MxMDOrderBookFn{ZmFnUnbound<&l2>{}});

  if (argc > 3) { // read symbols from file
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
	exceptionFn(MxMDExceptionFn{ZmFnUnbound<&exception>{}}).
	refDataLoadedFn(MxMDVenueFn{ZmFnUnbound<&refDataLoaded>{}}).
	addInstrumentFn(MxMDInstrumentFn{ZmFnUnbound<&addInstrument>{}})));

    md->record(argv[2]);		// start recording
    md->start();			// start all feeds

    stop.wait();			// wait for SIGINT

    md->stopRecording();		// stop recording
    md->stop();				// stop
    md->final();			// clean up

  } catch (...) { }

  syms = nullptr;

  return 0;
}
