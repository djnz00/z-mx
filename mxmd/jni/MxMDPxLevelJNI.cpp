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

#include <mxmd/MxMDOBSideJNI.hpp>
#include <mxmd/MxMDPxLvlDataJNI.hpp>
#include <mxmd/MxMDOrderJNI.hpp>

#include <mxmd/MxMDPxLevelJNI.hpp>

namespace MxMDPxLevelJNI {
  jclass	class_;

  ZJNI::JavaMethod ctorMethod[] = { { "<init>", "(J)V" } };
  ZJNI::JavaField ptrField[] = { { "ptr", "J" } };

  ZuInline MxMDPxLevel *ptr_(JNIEnv *env, jobject obj) {
    uintptr_t ptr_ = env->GetLongField(obj, ptrField[0].fid);
    if (ZuUnlikely(!ptr_)) return nullptr;
    ZmRef<MxMDPxLevel> *ZuMayAlias(ptr) = (ZmRef<MxMDPxLevel> *)&ptr_;
    return ptr->ptr();
  }

  // query callbacks
  ZJNI::JavaMethod allOrdersFn[] = {
    { "fn", "(Lcom/shardmx/mxmd/MxMDOrder;)J" }
  };
}

void MxMDPxLevelJNI::dtor_(JNIEnv *env, jobject obj, jlong ptr_)
{
  // (long) -> void
  ZmRef<MxMDPxLevel> *ZuMayAlias(ptr) = (ZmRef<MxMDPxLevel> *)&ptr_;
  if (ptr) ptr->~ZmRef<MxMDPxLevel>();
}

jobject MxMDPxLevelJNI::obSide(JNIEnv *env, jobject obj)
{
  // () -> MxMDOBSide
  MxMDPxLevel *pxLevel = ptr_(env, obj);
  if (ZuUnlikely(!pxLevel)) return 0;
  return MxMDOBSideJNI::ctor(env, pxLevel->obSide());
}

jobject MxMDPxLevelJNI::side(JNIEnv *env, jobject obj)
{
  // () -> MxSide
  MxMDPxLevel *pxLevel = ptr_(env, obj);
  if (ZuUnlikely(!pxLevel)) return 0;
  return MxSideJNI::ctor(env, pxLevel->side());
}

jint MxMDPxLevelJNI::pxNDP(JNIEnv *env, jobject obj)
{
  // () -> int
  MxMDPxLevel *pxLevel = ptr_(env, obj);
  if (ZuUnlikely(!pxLevel)) return ZuCmp<jint>::null();
  return pxLevel->pxNDP();
}

jint MxMDPxLevelJNI::qtyNDP(JNIEnv *env, jobject obj)
{
  // () -> int
  MxMDPxLevel *pxLevel = ptr_(env, obj);
  if (ZuUnlikely(!pxLevel)) return ZuCmp<jint>::null();
  return pxLevel->qtyNDP();
}

jlong MxMDPxLevelJNI::price(JNIEnv *env, jobject obj)
{
  // () -> long
  MxMDPxLevel *pxLevel = ptr_(env, obj);
  if (ZuUnlikely(!pxLevel)) return ZuCmp<jlong>::null();
  return pxLevel->price();
}

jobject MxMDPxLevelJNI::data(JNIEnv *env, jobject obj)
{
  // () -> MxMDPxLvlData
  MxMDPxLevel *pxLevel = ptr_(env, obj);
  if (ZuUnlikely(!pxLevel)) return 0;
  return MxMDPxLvlDataJNI::ctor(env, pxLevel->data());
}

jboolean MxMDPxLevelJNI::allOrders(JNIEnv *env, jobject obj, jobject fn)
{
  // (MxMDAllOrdersFn) -> long
  MxMDPxLevel *pxLevel = ptr_(env, obj);
  if (ZuUnlikely(!pxLevel || !fn)) return false;
  return pxLevel->allOrders(
      [fn = ZJNI::globalRef(env, fn)](MxMDOrder *order) -> bool {
    if (JNIEnv *env = ZJNI::env())
      return env->CallBooleanMethod(fn, allOrdersFn[0].mid,
	  MxMDOrderJNI::ctor(env, order));
    return false;
  });
}

jobject MxMDPxLevelJNI::ctor(JNIEnv *env, ZmRef<MxMDPxLevel> pxLevel)
{
  uintptr_t ptr;
  new (&ptr) ZmRef<MxMDPxLevel>(ZuMv(pxLevel));
  return env->NewObject(class_, ctorMethod[0].mid, (jlong)ptr);
}

int MxMDPxLevelJNI::bind(JNIEnv *env)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
  static JNINativeMethod methods[] = {
    { "dtor_",
      "(J)V",
      (void *)&MxMDPxLevelJNI::dtor_ },
    { "obSide",
      "()Lcom/shardmx/mxmd/MxMDOBSide;",
      (void *)&MxMDPxLevelJNI::obSide },
    { "side",
      "()Lcom/shardmx/mxbase/MxSide;",
      (void *)&MxMDPxLevelJNI::side },
    { "pxNDP",
      "()I",
      (void *)&MxMDPxLevelJNI::pxNDP },
    { "qtyNDP",
      "()I",
      (void *)&MxMDPxLevelJNI::qtyNDP },
    { "price",
      "()J",
      (void *)&MxMDPxLevelJNI::price },
    { "data",
      "()Lcom/shardmx/mxmd/MxMDPxLvlData;",
      (void *)&MxMDPxLevelJNI::data },
    { "allOrders",
      "(Lcom/shardmx/mxmd/MxMDAllOrdersFn;)Z",
      (void *)&MxMDPxLevelJNI::allOrders },
  };
#pragma GCC diagnostic pop

  class_ = ZJNI::globalClassRef(env, "com/shardmx/mxmd/MxMDPxLevel");
  if (!class_) return -1;

  if (ZJNI::bind(env, class_,
        methods, sizeof(methods) / sizeof(methods[0])) < 0) return -1;

  if (ZJNI::bind(env, class_, ctorMethod, 1) < 0) return -1;
  if (ZJNI::bind(env, class_, ptrField, 1) < 0) return -1;

  if (ZJNI::bind(env, "com/shardmx/mxmd/MxMDAllOrdersFn",
	allOrdersFn, 1) < 0) return -1;

  return 0;
}

void MxMDPxLevelJNI::final(JNIEnv *env)
{
  if (class_) env->DeleteGlobalRef(class_);
}
