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

// MxMD JNI

#ifndef MxMDLibJNI_HPP
#define MxMDLibJNI_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxMDLib_HPP
#include <mxmd/MxMDLib.hpp>
#endif

#include <zlib/ZmLock.hpp>

#include <jni.h>

namespace MxMDLibJNI {
  // () -> MxMDLib
  jobject instance(JNIEnv *, jclass);

  // (String) -> MxMDLib
  jobject init(JNIEnv *, jclass, jstring);

  // () -> void
  void close(JNIEnv *, jobject);

  // () -> void
  void start(JNIEnv *, jobject);
  // () -> void
  void stop(JNIEnv *, jobject);

  // (String) -> boolean
  jboolean record(JNIEnv *, jobject, jstring);
  // () -> String
  jstring stopRecording(JNIEnv *, jobject);

  // (String, Instant, boolean) -> boolean
  jboolean replay(JNIEnv *, jobject, jstring, jobject, jboolean);
  // () -> String
  jstring stopReplaying(JNIEnv *, jobject);

  // (Instant) -> void
  void startTimer(JNIEnv *, jobject, jobject);
  // () -> void
  void stopTimer(JNIEnv *, jobject);

  // (MxMDLibHandler) -> void
  void subscribe(JNIEnv *, jobject, jobject);
  // () -> void
  void unsubscribe(JNIEnv *, jobject);
  // () -> MxMDLibHandler
  jobject handler(JNIEnv *, jobject);

  // (MxInstrKey) -> MxMDInstrHandle
  jobject instrument(JNIEnv *, jobject, jobject);
  // (MxInstrKey, MxMDInstrumentFn) -> void
  void instrument(JNIEnv *, jobject, jobject, jobject);
  // (MxMDAllInstrumentsFn) -> boolean
  jboolean allInstruments(JNIEnv *, jobject, jobject);

  // (MxInstrKey) -> MxMDOBHandle
  jobject orderBook(JNIEnv *, jobject, jobject);
  // (MxInstrKey, MxMDOrderBookFn) -> void
  void orderBook(JNIEnv *, jobject, jobject, jobject);
  // (MxMDAllOrderBooksFn) -> boolean
  jboolean allOrderBooks(JNIEnv *, jobject, jobject);

  // (String) -> MxMDFeed
  jobject feed(JNIEnv *, jobject, jstring);
  // (MxMDAllFeedsFn) -> boolean
  jboolean allFeeds(JNIEnv *, jobject, jobject);

  // (String) -> MxMDVenue
  jobject venue(JNIEnv *, jobject, jstring);
  // (MxMDAllVenuesFn) -> boolean
  jboolean allVenues(JNIEnv *, jobject, jobject);

  jobject instance_();
  int bind(JNIEnv *);
  void final(JNIEnv *);

  // generic unsubscribe
  template <typename T>
  void unsubscribe_(JNIEnv *env, T *owner) {
    if (jobject obj = owner->template libData<jobject>()) {
      owner->unsubscribe();
      env->DeleteGlobalRef(obj);
      owner->libData(nullptr);
    }
  }
}

#endif /* MxMDLibJNI_HPP */
