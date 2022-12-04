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

// MxMD Test JNI

#include <iostream>

#include <jni.h>

#include <zlib/ZmRandom.hpp>

#include <zlib/ZJNI.hpp>

#include <mxmd/MxMD.hpp>

#include <mxmd/MxMDJNITest.hpp>

static const char *tickers[] = {
  "ETHBTC", // base-quote
  "LTCBTC", // base-quote
  0
};

struct Feed : public MxMDFeed {
  Feed(MxMDLib *md, MxID id) : MxMDFeed(md, id, 3) { }
  void start() {
    try {
      MxMDLib *md = MxMDLib::instance();

      if (!md) throw ZtString("MxMDLib::instance() failed");

      MxMDVenue *venue = new MxMDVenue(md, this,
	"JNITest",
	MxMDOrderIDScope::OBSide,
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
		*ticker,
		"JNITest",
		MxID()};	// ID (same as symbol in this case)

	// minimal reference data (just the native symbol and RIC)

	refData.idSrc = MxInstrIDSrc::EXCH;
	refData.symbol = *ticker;
	refData.pxNDP = 2;
	refData.qtyNDP = 2;

	// add the instrument

	MxMDInstrHandle instr =
	  md->instrument(instrKey, 0); // default to shard 0

	ZtString error;
	thread_local ZmSemaphore sem;
	instr.invokeMv([sem = &sem, &error,
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
	    tickSizeTbl,		// tick sizes
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

      md->raise(ZeEVENT(Info, "MxMDJNITest feed started"));
    } catch (const ZtString &s) {
      ZeLOG(Error, s);
    } catch (...) {
      ZeLOG(Error, "Unknown Exception");
    }
  }
};

void MxMDJNITest::init(JNIEnv *env, jobject obj)
{
  // (MxMDLib) -> void

  try {
    MxMDLib *md = MxMDLib::instance();
    if (!md) throw ZtString("MxMDLib::instance() failed");

    ZmRef<MxMDFeed> feed = new Feed(md, "JNITest");
    md->addFeed(feed);

    md->raise(ZeEVENT(Info, "MxMDJNITest initialized"));
  } catch (const ZtString &s) {
    ZeLOG(Error, s);
  } catch (...) {
    ZeLOG(Error, "Unknown Exception");
  }
}

static void publish_()
{
  try {
    MxMDLib *md = MxMDLib::instance();
    if (!md) throw ZtString("MxMDLib::instance() failed");

    for (const char **ticker = tickers; *ticker; ticker++) {
      md->instrInvoke(MxInstrKey{*ticker, "JNITest", MxID()},
	  [ticker](MxMDInstrument *instr) {
	if (!instr)
	  throw ZtString{} << "instrument \"" << *ticker << "\" not found";

	// get order book

	ZmRef<MxMDOrderBook> ob = instr->orderBook("JNITest", MxID());
	if (!ob)
	  throw ZtString{} << "order book for \"" << *ticker << "\" not found";

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
  } catch (const ZtString &s) {
    ZeLOG(Error, s);
  } catch (...) {
    ZeLOG(Error, "Unknown Exception");
  }
}

void MxMDJNITest::publish(JNIEnv *env, jobject obj)
{
  ZmThread thread{0, []() { publish_(); }};
  thread.join();
}

int MxMDJNITest::bind(JNIEnv *env)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
  static JNINativeMethod methods[] = {
    { "init", "()V", (void *)&MxMDJNITest::init },
    { "publish", "()V", (void *)&MxMDJNITest::publish }
  };
#pragma GCC diagnostic pop

  return ZJNI::bind(env, "com/shardmx/mxmd/MxMDJNITest",
        methods, sizeof(methods) / sizeof(methods[0]));
}

extern "C" {
  MxMDExtern jint JNI_OnLoad(JavaVM *, void *);
  MxMDExtern void JNI_OnUnload(JavaVM *, void *);
}

jint JNI_OnLoad(JavaVM *jvm, void *)
{
  jint v = ZJNI::load(jvm);

  if (v < 0) return -1;

  JNIEnv *env = ZJNI::env();

  if (MxMDJNITest::bind(env) < 0) return -1;

  return v;
}

void JNI_OnUnload(JavaVM *jvm, void *)
{
  ZJNI::unload(jvm);
}
