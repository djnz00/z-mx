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

#include <mxmd/MxMDSeverityJNI.hpp>

#include <mxmd/MxMDExceptionJNI.hpp>

namespace MxMDExceptionJNI {
  jclass	class_;

  // MxMDExceptionTuple named constructor
  ZJNI::JavaMethod ctorMethod[] = {
    { "of", "("
      "Ljava/time/Instant;"		// time
      "J"				// tid
      "Lcom/shardmx/mxmd/MxMDSeverity;"	// severity
      "Ljava/lang/String;"		// fileName
      "I"				// lineNumber
      "Ljava/lang/String;"		// function
      "Ljava/lang/String;"		// message
      ")Lcom/shardmx/mxmd/MxMDExceptionTuple;" }
  };
}

jobject MxMDExceptionJNI::ctor(JNIEnv *env, const ZeEvent *data)
{
  ZuStringN<BUFSIZ> s;
  s << *data;
  return env->CallStaticObjectMethod(class_, ctorMethod[0].mid,
      ZJNI::t2j(env, ZtDate{data->time()}),
      (jlong)data->tid(),
      MxMDSeverityJNI::ctor(env, data->severity()),
      ZJNI::s2j(env, data->fileName()),
      (jint)data->lineNumber(),
      ZJNI::s2j(env, data->function()),
      ZJNI::s2j(env, s));
}

int MxMDExceptionJNI::bind(JNIEnv *env)
{
  class_ = ZJNI::globalClassRef(env, "com/shardmx/mxmd/MxMDExceptionTuple");
  if (!class_) return -1;

  if (ZJNI::bindStatic(env, class_, ctorMethod, 1) < 0) return -1;
  return 0;
}

void MxMDExceptionJNI::final(JNIEnv *env)
{
  if (class_) env->DeleteGlobalRef(class_);
}
