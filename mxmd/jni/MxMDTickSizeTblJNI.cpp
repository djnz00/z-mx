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

#include <mxmd/MxMDVenueJNI.hpp>

#include <mxmd/MxMDTickSizeJNI.hpp>

#include <mxmd/MxMDTickSizeTblJNI.hpp>

namespace MxMDTickSizeTblJNI {
  jclass	class_;

  ZJNI::JavaMethod ctorMethod[] = { { "<init>", "(J)V" } };
  ZJNI::JavaField ptrField[] = { { "ptr", "J" } };

  ZuInline MxMDTickSizeTbl *ptr_(JNIEnv *env, jobject obj) {
    uintptr_t ptr_ = env->GetLongField(obj, ptrField[0].fid);
    if (ZuUnlikely(!ptr_)) return nullptr;
    ZmRef<MxMDTickSizeTbl> *ZuMayAlias(ptr) = (ZmRef<MxMDTickSizeTbl> *)&ptr_;
    return ptr->ptr();
  }

  // query callbacks
  ZJNI::JavaMethod allTickSizesFn[] = {
    { "fn", "(Lcom/shardmx/mxmd/MxMDTickSize;)J" }
  };
}

void MxMDTickSizeTblJNI::dtor_(JNIEnv *env, jobject obj, jlong ptr_)
{
  // (long) -> void
  ZmRef<MxMDTickSizeTbl> *ZuMayAlias(ptr) = (ZmRef<MxMDTickSizeTbl> *)&ptr_;
  if (ptr) ptr->~ZmRef<MxMDTickSizeTbl>();
}

jobject MxMDTickSizeTblJNI::venue(JNIEnv *env, jobject obj)
{
  // () -> MxMDVenue
  MxMDTickSizeTbl *tbl = ptr_(env, obj);
  if (ZuUnlikely(!tbl)) return 0;
  return MxMDVenueJNI::ctor(env, tbl->venue());
}

jstring MxMDTickSizeTblJNI::id(JNIEnv *env, jobject obj)
{
  // () -> String
  MxMDTickSizeTbl *tbl = ptr_(env, obj);
  if (ZuUnlikely(!tbl)) return 0;
  return ZJNI::s2j(env, tbl->id());
}

jint MxMDTickSizeTblJNI::pxNDP(JNIEnv *env, jobject obj)
{
  // () -> int
  MxMDTickSizeTbl *tbl = ptr_(env, obj);
  if (ZuUnlikely(!tbl)) return 0;
  return tbl->pxNDP();
}

jlong MxMDTickSizeTblJNI::tickSize(JNIEnv *env, jobject obj, jlong price)
{
  // (long) -> long
  MxMDTickSizeTbl *tbl = ptr_(env, obj);
  if (ZuUnlikely(!tbl)) return 0;
  return tbl->tickSize(price);
}

jboolean MxMDTickSizeTblJNI::allTickSizes(JNIEnv *env, jobject obj, jobject fn)
{
  // (MxMDAllTickSizesFn) -> long
  MxMDTickSizeTbl *tbl = ptr_(env, obj);
  if (ZuUnlikely(!tbl || !fn)) return false;
  return tbl->allTickSizes(
      [fn = ZJNI::globalRef(env, fn)](const MxMDTickSize &ts) -> bool {
    if (JNIEnv *env = ZJNI::env())
      return env->CallBooleanMethod(fn, allTickSizesFn[0].mid,
	  MxMDTickSizeJNI::ctor(env, ts));
    return false;
  });
}

jobject MxMDTickSizeTblJNI::ctor(JNIEnv *env, ZmRef<MxMDTickSizeTbl> tbl)
{
  uintptr_t ptr;
  new (&ptr) ZmRef<MxMDTickSizeTbl>(ZuMv(tbl));
  return env->NewObject(class_, ctorMethod[0].mid, (jlong)ptr);
}

int MxMDTickSizeTblJNI::bind(JNIEnv *env)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
  static JNINativeMethod methods[] = {
    { "dtor_",
      "(J)V",
      (void *)&MxMDTickSizeTblJNI::dtor_ },
    { "venue",
      "()Lcom/shardmx/mxmd/MxMDVenue;",
      (void *)&MxMDTickSizeTblJNI::venue },
    { "id",
      "()Ljava/lang/String;",
      (void *)&MxMDTickSizeTblJNI::id },
    { "pxNDP",
      "()I",
      (void *)&MxMDTickSizeTblJNI::pxNDP },
    { "tickSize",
      "(J)J",
      (void *)&MxMDTickSizeTblJNI::tickSize },
    { "allTickSizes",
      "(Lcom/shardmx/mxmd/MxMDAllTickSizesFn;)Z",
      (void *)&MxMDTickSizeTblJNI::allTickSizes },
  };
#pragma GCC diagnostic pop

  class_ = ZJNI::globalClassRef(env, "com/shardmx/mxmd/MxMDTickSizeTbl");
  if (!class_) return -1;

  if (ZJNI::bind(env, class_,
        methods, sizeof(methods) / sizeof(methods[0])) < 0) return -1;

  if (ZJNI::bind(env, class_, ctorMethod, 1) < 0) return -1;
  if (ZJNI::bind(env, class_, ptrField, 1) < 0) return -1;

  if (ZJNI::bind(env, "com/shardmx/mxmd/MxMDAllTickSizesFn",
	allTickSizesFn, 1) < 0) return -1;

  return 0;
}

void MxMDTickSizeTblJNI::final(JNIEnv *env)
{
  if (class_) env->DeleteGlobalRef(class_);
}
