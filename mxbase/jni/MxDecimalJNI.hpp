//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2

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

// MxBase JNI

#ifndef MxDecimalJNI_HPP
#define MxDecimalJNI_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef MxBaseLib_HPP
#include <mxbase/MxBaseLib.hpp>
#endif

#include <mxbase/MxBase.hpp>

namespace MxDecimalJNI {
  MxBaseExtern MxDecimal j2c(JNIEnv *, jobject, bool dlr = false);
  MxBaseExtern jobject ctor(JNIEnv *, const MxDecimal &);

  void init(JNIEnv *env, jobject obj);
  void init_long(JNIEnv *env, jobject obj, jlong v);

  jboolean isNull(JNIEnv *env, jobject obj);
  jboolean isReset(JNIEnv *env, jobject obj);

  jobject add(JNIEnv *env, jobject obj, jobject v);
  jobject subtract(JNIEnv *env, jobject obj, jobject v);
  jobject multiply(JNIEnv *env, jobject obj, jobject v);
  jobject divide(JNIEnv *env, jobject obj, jobject v);

  jstring toString(JNIEnv *, jobject);
  void scan(JNIEnv *, jobject, jstring s);

  jboolean equals(JNIEnv *, jobject, jobject v);
  jint compareTo(JNIEnv *, jobject, jobject v);

  int bind(JNIEnv *);
  void final(JNIEnv *);
}

#endif /* MxDecimalJNI_HPP */
