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

#include <new>

#include <jni.h>

#include <zlib/ZJNI.hpp>

#include <mxmd/MxMD.hpp>

#include <mxbase/MxFutKeyJNI.hpp>
#include <mxbase/MxOptKeyJNI.hpp>

#include <mxmd/MxMDInstrumentJNI.hpp>

#include <mxmd/MxMDDerivativesJNI.hpp>

namespace MxMDDerivativesJNI {
  jclass	class_;

  ZJNI::JavaMethod ctorMethod[] = { { "<init>", "(J)V" } };
  ZJNI::JavaField ptrField[] = { { "ptr", "J" } };

  ZuInline MxMDDerivatives *ptr_(JNIEnv *env, jobject obj) {
    uintptr_t ptr_ = env->GetLongField(obj, ptrField[0].fid);
    if (ZuUnlikely(!ptr_)) return nullptr;
    ZmRef<MxMDDerivatives> *ZuMayAlias(ptr) = (ZmRef<MxMDDerivatives> *)&ptr_;
    return ptr->ptr();
  }

  // query callbacks
  ZJNI::JavaMethod allInstrumentsFn[] = {
    { "fn", "(Lcom/shardmx/mxmd/MxMDInstrument;)J" }
  };
}

void MxMDDerivativesJNI::dtor_(JNIEnv *env, jobject obj, jlong ptr_)
{
  // (long) -> void
  ZmRef<MxMDDerivatives> *ZuMayAlias(ptr) = (ZmRef<MxMDDerivatives> *)&ptr_;
  if (ptr) ptr->~ZmRef<MxMDDerivatives>();
}

jobject MxMDDerivativesJNI::future(JNIEnv *env, jobject obj, jobject key)
{
  // (MxFutKey) -> MxMDInstrument
  MxMDDerivatives *der = ptr_(env, obj);
  if (ZuUnlikely(!der)) return 0;
  return MxMDInstrumentJNI::ctor(env, der->future(MxFutKeyJNI::j2c(env, key)));
}

jboolean MxMDDerivativesJNI::allFutures(JNIEnv *env, jobject obj, jobject fn)
{
  // (MxMDAllInstrumentsFn) -> long
  MxMDDerivatives *der = ptr_(env, obj);
  if (ZuUnlikely(!der || !fn)) return false;
  return der->allFutures(
      [fn = ZJNI::globalRef(env, fn)](MxMDInstrument *instr) -> bool {
    if (JNIEnv *env = ZJNI::env())
      return env->CallBooleanMethod(fn, allInstrumentsFn[0].mid,
	  MxMDInstrumentJNI::ctor(env, instr));
    return false;
  });
}

jobject MxMDDerivativesJNI::option(JNIEnv *env, jobject obj, jobject key)
{
  // (MxOptKey) -> MxMDInstrument
  MxMDDerivatives *der = ptr_(env, obj);
  if (ZuUnlikely(!der)) return 0;
  return MxMDInstrumentJNI::ctor(env, der->option(MxOptKeyJNI::j2c(env, key)));
}

jboolean MxMDDerivativesJNI::allOptions(JNIEnv *env, jobject obj, jobject fn)
{
  // (MxMDAllInstrumentsFn) -> long
  MxMDDerivatives *der = ptr_(env, obj);
  if (ZuUnlikely(!der || !fn)) return false;
  return der->allOptions(
      [fn = ZJNI::globalRef(env, fn)](MxMDInstrument *instr) -> bool {
    if (JNIEnv *env = ZJNI::env())
      return env->CallBooleanMethod(fn, allInstrumentsFn[0].mid,
	  MxMDInstrumentJNI::ctor(env, instr));
    return false;
  });
}

jobject MxMDDerivativesJNI::ctor(JNIEnv *env, ZmRef<MxMDDerivatives> der)
{
  uintptr_t ptr;
  new ((void *)&ptr) ZmRef<MxMDDerivatives>(ZuMv(der));
  return env->NewObject(class_, ctorMethod[0].mid, (jlong)ptr);
}

int MxMDDerivativesJNI::bind(JNIEnv *env)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
  static JNINativeMethod methods[] = {
    { "dtor_",
      "(J)V",
      (void *)&MxMDDerivativesJNI::dtor_ },
    { "future",
      "(Lcom/shardmx/mxbase/MxFutKey;)Lcom/shardmx/mxmd/MxMDInstrument;",
      (void *)&MxMDDerivativesJNI::future },
    { "allFutures",
      "(Lcom/shardmx/mxmd/MxMDAllInstrumentsFn;)Z",
      (void *)&MxMDDerivativesJNI::allFutures },
    { "option",
      "(Lcom/shardmx/mxbase/MxOptKey;)Lcom/shardmx/mxmd/MxMDInstrument;",
      (void *)&MxMDDerivativesJNI::option },
    { "allOptions",
      "(Lcom/shardmx/mxmd/MxMDAllInstrumentsFn;)Z",
      (void *)&MxMDDerivativesJNI::allOptions },
  };
#pragma GCC diagnostic pop

  class_ = ZJNI::globalClassRef(env, "com/shardmx/mxmd/MxMDDerivatives");
  if (!class_) return -1;

  if (ZJNI::bind(env, class_,
        methods, sizeof(methods) / sizeof(methods[0])) < 0) return -1;

  if (ZJNI::bind(env, class_, ctorMethod, 1) < 0) return -1;
  if (ZJNI::bind(env, class_, ptrField, 1) < 0) return -1;

  if (ZJNI::bind(env, "com/shardmx/mxmd/MxMDAllInstrumentsFn",
	allInstrumentsFn, 1) < 0) return -1;

  return 0;
}

void MxMDDerivativesJNI::final(JNIEnv *env)
{
  if (class_) env->DeleteGlobalRef(class_);
}
