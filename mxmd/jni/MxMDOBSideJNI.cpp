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

#include <mxbase/MxSideJNI.hpp>

#include <mxmd/MxMD.hpp>

#include <mxmd/MxMDOrderBookJNI.hpp>
#include <mxmd/MxMDPxLevelJNI.hpp>
#include <mxmd/MxMDOBSideDataJNI.hpp>

#include <mxmd/MxMDOBSideJNI.hpp>

namespace MxMDOBSideJNI {
  jclass	class_;

  ZJNI::JavaMethod ctorMethod[] = { { "<init>", "(J)V" } };
  ZJNI::JavaField ptrField[] = { { "ptr", "J" } };

  ZuInline MxMDOBSide *ptr_(JNIEnv *env, jobject obj) {
    uintptr_t ptr_ = env->GetLongField(obj, ptrField[0].fid);
    if (ZuUnlikely(!ptr_)) return nullptr;
    ZmRef<MxMDOBSide> *ZuMayAlias(ptr) = (ZmRef<MxMDOBSide> *)&ptr_;
    return ptr->ptr();
  }

  // query callbacks
  ZJNI::JavaMethod allPxLevelsFn[] = {
    { "fn", "(Lcom/shardmx/mxmd/MxMDPxLevel;)J" }
  };
}

void MxMDOBSideJNI::dtor_(JNIEnv *env, jobject obj, jlong ptr_)
{
  // (long) -> void
  ZmRef<MxMDOBSide> *ZuMayAlias(ptr) = (ZmRef<MxMDOBSide> *)&ptr_;
  if (ptr) ptr->~ZmRef<MxMDOBSide>();
}

jobject MxMDOBSideJNI::orderBook(JNIEnv *env, jobject obj)
{
  // () -> MxMDOrderBook
  MxMDOBSide *obs = ptr_(env, obj);
  if (ZuUnlikely(!obs)) return 0;
  return MxMDOrderBookJNI::ctor(env, obs->orderBook());
}

jobject MxMDOBSideJNI::side(JNIEnv *env, jobject obj)
{
  // () -> MxSide
  MxMDOBSide *obs = ptr_(env, obj);
  if (ZuUnlikely(!obs)) return 0;
  return MxSideJNI::ctor(env, obs->side());
}

jobject MxMDOBSideJNI::data(JNIEnv *env, jobject obj)
{
  // () -> MxMDOBSideData
  MxMDOBSide *obs = ptr_(env, obj);
  if (ZuUnlikely(!obs)) return 0;
  return MxMDOBSideDataJNI::ctor(env, obs->data());
}

jlong MxMDOBSideJNI::vwap(JNIEnv *env, jobject obj)
{
  // () -> long
  MxMDOBSide *obs = ptr_(env, obj);
  if (ZuUnlikely(!obs)) return ZuCmp<jlong>::null();
  return obs->vwap();
}

jobject MxMDOBSideJNI::mktLevel(JNIEnv *env, jobject obj)
{
  // () -> MxMDPxLevel
  MxMDOBSide *obs = ptr_(env, obj);
  if (ZuUnlikely(!obs)) return 0;
  return MxMDPxLevelJNI::ctor(env, obs->mktLevel());
}

jboolean MxMDOBSideJNI::allPxLevels(JNIEnv *env, jobject obj, jobject fn)
{
  // (MxMDAllPxLevelsFn) -> long
  MxMDOBSide *obs = ptr_(env, obj);
  if (ZuUnlikely(!obs || !fn)) return false;
  return obs->allPxLevels(
      [fn = ZJNI::globalRef(env, fn)](MxMDPxLevel *pxLevel) -> bool {
    if (JNIEnv *env = ZJNI::env())
      return env->CallBooleanMethod(fn, allPxLevelsFn[0].mid,
	  MxMDPxLevelJNI::ctor(env, pxLevel));
    return false;
  });
}

jobject MxMDOBSideJNI::ctor(JNIEnv *env, ZmRef<MxMDOBSide> obs)
{
  uintptr_t ptr;
  new (&ptr) ZmRef<MxMDOBSide>(ZuMv(obs));
  return env->NewObject(class_, ctorMethod[0].mid, (jlong)ptr);
}

int MxMDOBSideJNI::bind(JNIEnv *env)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
  static JNINativeMethod methods[] = {
    { "dtor_",
      "(J)V",
      (void *)&MxMDOBSideJNI::dtor_ },
    { "orderBook",
      "()Lcom/shardmx/mxmd/MxMDOrderBook;",
      (void *)&MxMDOBSideJNI::orderBook },
    { "side",
      "()Lcom/shardmx/mxbase/MxSide;",
      (void *)&MxMDOBSideJNI::side },
    { "data",
      "()Lcom/shardmx/mxmd/MxMDOBSideData;",
      (void *)&MxMDOBSideJNI::data },
    { "vwap",
      "()J",
      (void *)&MxMDOBSideJNI::vwap },
    { "mktLevel",
      "()Lcom/shardmx/mxmd/MxMDPxLevel;",
      (void *)&MxMDOBSideJNI::mktLevel },
    { "allPxLevels",
      "(Lcom/shardmx/mxmd/MxMDAllPxLevelsFn;)Z",
      (void *)&MxMDOBSideJNI::allPxLevels },
  };
#pragma GCC diagnostic pop

  class_ = ZJNI::globalClassRef(env, "com/shardmx/mxmd/MxMDOBSide");
  if (!class_) return -1;

  if (ZJNI::bind(env, class_,
        methods, sizeof(methods) / sizeof(methods[0])) < 0) return -1;

  if (ZJNI::bind(env, class_, ctorMethod, 1) < 0) return -1;
  if (ZJNI::bind(env, class_, ptrField, 1) < 0) return -1;

  if (ZJNI::bind(env, "com/shardmx/mxmd/MxMDAllPxLevelsFn",
	allPxLevelsFn, 1) < 0) return -1;

  return 0;
}

void MxMDOBSideJNI::final(JNIEnv *env)
{
  if (class_) env->DeleteGlobalRef(class_);
}
