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

#include <mxbase/MxDecimalJNI.hpp>

namespace MxDecimalJNI {
  jclass	class_; // MxDecimal

  ZJNI::JavaMethod ctorMethod[] = { { "<init>", "(JJ)V" } };
  ZJNI::JavaField fields[] = {
    { "h", "J" },
    { "l", "J" }
  };
}

MxDecimal MxDecimalJNI::j2c(JNIEnv *env, jobject obj, bool dlr)
{
  if (ZuUnlikely(!obj)) return MxEnum();
  uint64_t h = env->GetLongField(obj, fields[0].fid);
  uint64_t l = env->GetLongField(obj, fields[1].fid);
  if (dlr) env->DeleteLocalRef(obj);
  return MxDecimal{MxDecimal::Unscaled, (int128_t)((((uint128_t)h)<<64) | l)};
}

jobject MxDecimalJNI::ctor(JNIEnv *env, const MxDecimal &v)
{
  return env->NewObject(class_, ctorMethod[0].mid,
      (jlong)(int64_t)(v.value>>64), (jlong)(int64_t)v.value);
}

void MxDecimalJNI::init(JNIEnv *env, jobject obj)
{
  MxDecimal v;
  env->SetLongField(obj, fields[0].fid, (int64_t)(v.value>>64));
  env->SetLongField(obj, fields[1].fid, (int64_t)(v.value));
}

void MxDecimalJNI::init_long(JNIEnv *env, jobject obj, jlong v_)
{
  MxDecimal v{v_};
  env->SetLongField(obj, fields[0].fid, (int64_t)(v.value>>64));
  env->SetLongField(obj, fields[1].fid, (int64_t)(v.value));
}

jboolean MxDecimalJNI::isNull(JNIEnv *env, jobject obj)
{
  return j2c(env, obj, true) == MxDecimal::null();
}

jboolean MxDecimalJNI::isReset(JNIEnv *env, jobject obj)
{
  return j2c(env, obj, true) == MxDecimal::reset();
}

jobject MxDecimalJNI::add(JNIEnv *env, jobject obj, jobject v)
{
  return ctor(env, j2c(env, obj, true) + j2c(env, v, true));
}

jobject MxDecimalJNI::subtract(JNIEnv *env, jobject obj, jobject v)
{
  return ctor(env, j2c(env, obj, true) - j2c(env, v, true));
}

jobject MxDecimalJNI::multiply(JNIEnv *env, jobject obj, jobject v)
{
  return ctor(env, j2c(env, obj, true) * j2c(env, v, true));
}

jobject MxDecimalJNI::divide(JNIEnv *env, jobject obj, jobject v)
{
  return ctor(env, j2c(env, obj, true) / j2c(env, v, true));
}

jstring MxDecimalJNI::toString(JNIEnv *env, jobject obj)
{
  if (!obj) return ZJNI::s2j(env, ZuString());
  return ZJNI::s2j(env, ZuStringN<44>() << j2c(env, obj, true));
}

void MxDecimalJNI::scan(JNIEnv *env, jobject obj, jstring s_)
{
  ZuStringN<44> s;
  ZJNI::j2s_ZuStringN(s, env, s_, true);
  MxDecimal v{s};
  env->SetLongField(obj, fields[0].fid, (int64_t)(v.value>>64));
  env->SetLongField(obj, fields[1].fid, (int64_t)(v.value));
}

jboolean MxDecimalJNI::equals(JNIEnv *env, jobject obj, jobject v_)
{
  return j2c(env, obj, true) == j2c(env, v_, true);
}

jint MxDecimalJNI::compareTo(JNIEnv *env, jobject obj, jobject v_)
{
  return j2c(env, obj, true).cmp(j2c(env, v_, true));
}

int MxDecimalJNI::bind(JNIEnv *env)
{
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#endif
  static JNINativeMethod methods[] = {
    { "init",
      "()V",
      (void *)&MxDecimalJNI::init },
    { "init",
      "(J)V",
      (void *)&MxDecimalJNI::init_long },
    { "isNull",
      "()Z",
      (void *)&MxDecimalJNI::isNull },
    { "isReset",
      "()Z",
      (void *)&MxDecimalJNI::isReset },
    { "add",
      "(Lcom/shardmx/mxbase/MxDecimal;)Lcom/shardmx/mxbase/MxDecimal;",
      (void *)&MxDecimalJNI::add },
    { "subtract",
      "(Lcom/shardmx/mxbase/MxDecimal;)Lcom/shardmx/mxbase/MxDecimal;",
      (void *)&MxDecimalJNI::subtract },
    { "multiply",
      "(Lcom/shardmx/mxbase/MxDecimal;)Lcom/shardmx/mxbase/MxDecimal;",
      (void *)&MxDecimalJNI::multiply },
    { "divide",
      "(Lcom/shardmx/mxbase/MxDecimal;)Lcom/shardmx/mxbase/MxDecimal;",
      (void *)&MxDecimalJNI::divide },
    { "toString",
      "()Ljava/lang/String;",
      (void *)&MxDecimalJNI::toString },
    { "scan",
      "(Ljava/lang/String;)V",
      (void *)&MxDecimalJNI::scan },
    { "equals",
      "(Lcom/shardmx/mxbase/MxDecimal;)Z",
      (void *)&MxDecimalJNI::equals },
    { "compareTo",
      "(Lcom/shardmx/mxbase/MxDecimal;)I",
      (void *)&MxDecimalJNI::compareTo }
  };
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

  class_ = ZJNI::globalClassRef(env, "com/shardmx/mxbase/MxDecimal");
  if (!class_) return -1;

  if (ZJNI::bind(env, class_, ctorMethod, 1) < 0) return -1;
  if (ZJNI::bind(env, class_, fields,
	sizeof(fields) / sizeof(fields[0])) < 0) return -1;

  if (ZJNI::bind(env, class_, methods,
	sizeof(methods) / sizeof(methods[0])) < 0) return -1;

  return 0;
}

void MxDecimalJNI::final(JNIEnv *env)
{
  if (class_) env->DeleteGlobalRef(class_);
}
