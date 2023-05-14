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

#include <jni.h>

#include <zlib/ZJNI.hpp>

#include <mxbase/MxBase.hpp>

#include <mxbase/MxFutKeyJNI.hpp>

namespace MxFutKeyJNI {
  jclass	class_; // MxFutKeyTuple

  // MxFutKeyTuple named constructor
  ZJNI::JavaMethod ctorMethod[] = {
    { "of", "(I)Lcom/shardmx/mxbase/MxFutKeyTuple;" }
  };

  // MxFutKey accessors
  ZJNI::JavaMethod methods[] = {
    { "mat", "()I" }
  };
}

MxFutKey MxFutKeyJNI::j2c(JNIEnv *env, jobject obj, bool dlr)
{
  if (ZuUnlikely(!obj)) return MxFutKey();
  MxFutKey key{env->CallIntMethod(obj, methods[0].mid)};
  if (dlr) env->DeleteLocalRef(obj);
  return key;
}

jobject MxFutKeyJNI::ctor(JNIEnv *env, const MxFutKey &key)
{
  return env->CallStaticObjectMethod(class_, ctorMethod[0].mid, (jint)key);
}

int MxFutKeyJNI::bind(JNIEnv *env)
{
  class_ = ZJNI::globalClassRef(env, "com/shardmx/mxbase/MxFutKeyTuple");
  if (!class_) return -1;

  if (ZJNI::bindStatic(env, class_, ctorMethod, 1) < 0) return -1;

  {
    jclass c = env->FindClass("com/shardmx/mxbase/MxFutKey");
    if (!c) return -1;
    if (ZJNI::bind(env, c, methods, sizeof(methods) / sizeof(methods[0])) < 0)
      return -1;
    env->DeleteLocalRef((jobject)c);
  }

  return 0;
}

void MxFutKeyJNI::final(JNIEnv *env)
{
  if (class_) env->DeleteGlobalRef(class_);
}
