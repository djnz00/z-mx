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

// MxMD recording tool

#include <zlib/ZuLib.hpp>

#include <stdio.h>
#include <signal.h>

#include <zlib/ZeLog.hpp>

#include <mxmd/MxMD.hpp>

#include <iostream>

ZmSemaphore stop;	// used to signal exit

extern "C" { void sigint(int); };
void sigint(int sig) { stop.post(); }	// CTRL-C signal handler

void usage() {
  std::cerr <<
    "usage: mdrecord CONFIG RECFILE [SYMBOLS]\n"
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
    l1Fn(MxMDLevel1Fn::Ptr<&l1>::fn()).
    l2Fn(MxMDOrderBookFn::Ptr<&l2>::fn());

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
	exceptionFn(MxMDExceptionFn::Ptr<&exception>::fn()).
	refDataLoadedFn(MxMDVenueFn::Ptr<&refDataLoaded>::fn()).
	addInstrumentFn(MxMDInstrumentFn::Ptr<&addInstrument>::fn())));

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
