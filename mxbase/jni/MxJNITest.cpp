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

// MxBase JNI test

#ifndef MxBaseLib_HPP
#include <mxbase/MxBaseLib.hpp>
#endif

#include <iostream>

#include <jni.h>

#include <zlib/ZuBox.hpp>

#include <zlib/ZmThread.hpp>
#include <zlib/ZmRing.hpp>

#include <zlib/ZtString.hpp>

#include <zlib/ZJNI.hpp>

extern "C" {
  MxBaseExtern jint JNI_OnLoad(JavaVM *, void *);
  MxBaseExtern void JNI_OnUnload(JavaVM *, void *);
}

struct Msg {
  Msg() { }
  Msg(JNIEnv *env, jobject obj, jstring text) :
    m_obj(env->NewGlobalRef(obj)) {
    m_text = ZJNI::j2s_ZtString(env, text);
  }
  ~Msg() {
    if (m_obj) ZJNI::env()->DeleteGlobalRef(m_obj);
  }
  Msg(Msg &&msg) : m_obj(msg.m_obj), m_text(ZuMv(msg.m_text)) {
    msg.m_obj = 0;
  }
  Msg &operator =(Msg &&msg) {
    if (m_obj) ZJNI::env()->DeleteGlobalRef(m_obj);
    m_obj = msg.m_obj; msg.m_obj = 0;
    m_text = ZuMv(msg.m_text);
    return *this;
  }

  void operator ()(JNIEnv *env, jmethodID mid) {
    jstring text = ZJNI::s2j(env, m_text);
    env->CallVoidMethod(m_obj, mid, text);
    env->DeleteLocalRef(text);
  }

private:
  jobject	m_obj = 0;
  ZtString	m_text;
};

struct Ring : public ZmObject, public ZmRing<Msg> { };

static ZmRef<Ring> ring;

void loop()
{
  JNIEnv *env = ZJNI::env();
  jclass c = env->FindClass("com/shardmx/mxbase/MxJNITestObject");
  jmethodID mid = env->GetMethodID(c, "helloWorld_", "(Ljava/lang/String;)V");
  env->DeleteLocalRef(c);
  for (;;) {
    Msg *msg_ = ring->shift();
    if (!msg_) break;
    Msg msg = ZuMv(*msg_);
    ring->shift2();
    msg(env, mid);
  }
  ring->close();
  ZJNI::detach();
}

int init(JNIEnv *, jobject)
{
  ring = new Ring(ZmRingParams(1024)); // .ll(ll).spin(spin)

  if (ring->open(Ring::Read | Ring::Write) != Ring::OK) {
    std::cerr << "ring open failed\n" << std::flush;
    return -1;
  }

  ZmThread(0, []() { loop(); }, ZmThreadParams().name("JNITest"));

  return 0;
}

static void dtor_(JNIEnv *, jobject) { }

static jboolean helloWorld(JNIEnv *env, jobject obj, jstring text)
{
  ZJNI::env(env);

  {
    ZtDate now{ZtDate::Now};
    if (now != ZJNI::j2t(env, ZJNI::t2j(env, now))) return false;
    std::cout << "ZtDate / Instant conversion ok\n" << std::flush;
  }

  thread_local jfieldID fid = 0;
  if (!fid) fid = env->GetFieldID(env->GetObjectClass(obj), "ptr", "J");
  if (void *ptr = ring->push()) {
    new (ptr) Msg(env, obj, text);
    ring->push2(ptr);
    if (ZuLikely(fid)) env->SetLongField(obj, fid, (uintptr_t)ptr);
  }
  ring->eof();
  return true;
}

int MxJNITest_bind(JNIEnv *env)
{
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#endif
  static JNINativeMethod methods[] = {
    { "init", "()I", (void *)&init }
  };
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif
  return ZJNI::bind(env, "com/shardmx/mxbase/MxJNITest",
      methods, sizeof(methods) / sizeof(methods[0]));
}

int MxJNITestObject_bind(JNIEnv *env)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
  static JNINativeMethod methods[] = {
    { "dtor_", "()V", (void *)&dtor_ },
    { "helloWorld", "(Ljava/lang/String;)Z", (void *)&helloWorld }
  };
#pragma GCC diagnostic pop
  return ZJNI::bind(env, "com/shardmx/mxbase/MxJNITestObject",
      methods, sizeof(methods) / sizeof(methods[0]));
}

jint JNI_OnLoad(JavaVM *jvm, void *)
{
  std::cout << "JNI_OnLoad()\n" << std::flush;

  jint v = ZJNI::load(jvm);

  if (MxJNITest_bind(ZJNI::env()) < 0) return -1;
  if (MxJNITestObject_bind(ZJNI::env()) < 0) return -1;

  return v;
}

void JNI_OnUnload(JavaVM *jvm, void *)
{
  ZJNI::unload(jvm);
}
