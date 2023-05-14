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

#include <iostream>

#include <jni.h>

#include <zlib/ZJNI.hpp>

#include <mxmd/MxMDTickSizeJNI.hpp>

namespace MxMDTickSizeJNI {
  jclass	class_;

  // MxMDTickSizeTuple named constructor
  ZJNI::JavaMethod ctorMethod[] = {
    { "of", "(JJJ)Lcom/shardmx/mxmd/MxMDTickSizeTuple;" }
  };

#if 0
  // MxMDTickSize accessors
  ZJNI::JavaMethod methods[] = {
    { "minPrice", "()J" },
    { "maxPrice", "()J" },
    { "tickSize", "()J" },
  };
#endif
}

#if 0
MxMDTickSize MxMDTickSizeJNI::j2c(JNIEnv *env, jobject obj)
{
  return MxMDTickSize{
    env->CallLongMethod(obj, methods[0].mid),
    env->CallLongMethod(obj, methods[1].mid),
    env->CallLongMethod(obj, methods[2].mid)};
}
#endif

jobject MxMDTickSizeJNI::ctor(JNIEnv *env, const MxMDTickSize &ts)
{
  return env->CallStaticObjectMethod(class_, ctorMethod[0].mid,
      (jlong)ts.minPrice(), (jlong)ts.maxPrice(), (jlong)ts.tickSize());
}

int MxMDTickSizeJNI::bind(JNIEnv *env)
{
  class_ = ZJNI::globalClassRef(env, "com/shardmx/mxmd/MxMDTickSizeTuple");
  if (!class_) return -1;

  if (ZJNI::bindStatic(env, class_, ctorMethod, 1) < 0) return -1;

#if 0
  {
    jclass c = env->FindClass("com/shardmx/mxbase/MxMDTickSize");
    if (!c) return -1;
    if (ZJNI::bind(env, c, methods, sizeof(methods) / sizeof(methods[0])) < 0)
      return -1;
    env->DeleteLocalRef((jobject)c);
  }
#endif

  return 0;
}

void MxMDTickSizeJNI::final(JNIEnv *env)
{
  if (class_) env->DeleteGlobalRef(class_);
}
