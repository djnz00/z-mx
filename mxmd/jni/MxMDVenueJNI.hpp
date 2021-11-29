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

#ifndef MxMDVenueJNI_HPP
#define MxMDVenueJNI_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxMDLib_HPP
#include <mxmd/MxMDLib.hpp>
#endif

#include <jni.h>

#include <mxmd/MxMD.hpp>

namespace MxMDVenueJNI {
  // (long) -> void
  void dtor_(JNIEnv *, jobject, jlong);

  // () -> MxMDLib
  jobject md(JNIEnv *, jobject);
  // () -> MxMDFeed
  jobject feed(JNIEnv *, jobject);

  // () -> String
  jstring id(JNIEnv *, jobject);

  // () -> MxMDOrderIDScope
  jobject orderIDScope(JNIEnv *, jobject);

  // () -> long
  jlong flags(JNIEnv *, jobject);

  // () -> boolean
  jboolean loaded(JNIEnv *, jobject);

  // (String) -> MxMDTickSizeTbl
  jobject tickSizeTbl(JNIEnv *, jobject, jstring);
  // (MxMDAllTickSizeTblsFn) -> boolean
  jboolean allTickSizeTbls(JNIEnv *, jobject, jobject);

  // (MxMDAllSegmentsFn) -> boolean
  jboolean allSegments(JNIEnv *, jobject, jobject);

  // (String) -> MxMDSegment
  jobject tradingSession(JNIEnv *, jobject, jstring);

  jobject ctor(JNIEnv *, ZmRef<MxMDVenue>);
  int bind(JNIEnv *);
  void final(JNIEnv *);
}

#endif /* MxMDVenueJNI_HPP */
