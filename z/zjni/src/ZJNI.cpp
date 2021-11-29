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

// Z JNI library

#include <zlib/ZmSpecific.hpp>
#include <zlib/ZmCleanup.hpp>
#include <zlib/ZmThread.hpp>
#include <zlib/ZmSemaphore.hpp>

#include <zlib/ZJNI.hpp>

#include <jni.h>

extern "C" {
  extern void ZJNI_atexit();
#ifndef _WIN32
  extern void ZJNI_sig(int);
#else
  extern BOOL WINAPI ZJNI_handler(DWORD event);
#endif
};

static ZmSemaphore ZJNI_sem;

static uint32_t ZJNI_lock_ = 0;
void ZJNI_lock()
{
  ZmAtomic<uint32_t> *ZuMayAlias(lock) =
    reinterpret_cast<ZmAtomic<uint32_t> *>(&ZJNI_lock_);
  while (ZuUnlikely(lock->cmpXch(1, 0))) Zm::yield();
}
void ZJNI_unlock()
{
  ZmAtomic<uint32_t> *ZuMayAlias(lock) =
    reinterpret_cast<ZmAtomic<uint32_t> *>(&ZJNI_lock_);
  *lock = 0;
}

static unsigned ZJNI_exiting = 0;
static unsigned ZJNI_trapped = 0;
static ZmFn<> ZJNI_sigFn;

#ifndef _WIN32
static int ZJNI_signal = 0;
static void (*ZJNI_oldint)(int) = nullptr;
static void (*ZJNI_oldterm)(int) = nullptr;

void ZJNI_sig(int s)
{
  ZJNI_lock();
  ZJNI_signal = s;
  ZJNI_unlock();
  ZJNI_sem.post();
}
#else
BOOL WINAPI ZJNI_handler(DWORD)
{
  ZJNI_sem.post();
  return TRUE;
}
#endif

void ZJNI_sig()
{
  ZJNI_sem.wait();
  if (!ZJNI_exiting) {
    {
      ZJNI_lock();
      ZmFn<> fn = ZuMv(ZJNI_sigFn);
      ZJNI_unlock();
      fn();
    }
#ifndef _WIN32
    {
      void (*fn)(int) = nullptr;
      ZJNI_lock();
      int s = ZJNI_signal;
      ZJNI_unlock();
      if (s == SIGINT) fn = ZJNI_oldint;
      if (s == SIGTERM) fn = ZJNI_oldterm;
      if (fn) (*fn)(s);
    }
#endif
  }
}

void ZJNI_atexit()
{
  ZJNI_exiting = 1;
  ZJNI_sem.post();
}

ZmFn<> ZJNI::sigFn()
{
  ZJNI_lock();
  ZmFn<> fn = ZuMv(ZJNI_sigFn);
  ZJNI_unlock();
  return fn;
}

void ZJNI::sigFn(ZmFn<> fn)
{
  ZJNI_lock();
  ZJNI_sigFn = ZuMv(fn);
  ZJNI_unlock();
}

void ZJNI::trap()
{
  ZJNI_lock();
  if (ZJNI_trapped) {
    ZJNI_unlock();
    return;
  }
  ZJNI_trapped = 1;
  ZJNI_unlock();
  ::atexit(ZJNI_atexit);
  ZmThread(0, []() { ZJNI_sig(); }, ZmThreadParams().name("ZJNI_sig"));
#ifndef _WIN32
  struct sigaction s, o;
  memset(&s, 0, sizeof(struct sigaction));
  memset(&o, 0, sizeof(struct sigaction));
  s.sa_handler = ZJNI_sig;
  sigaction(SIGINT, &s, &o);
  ZJNI_oldint = o.sa_handler;
  sigaction(SIGTERM, &s, &o);
  ZJNI_oldterm = o.sa_handler;
#else
  SetConsoleCtrlHandler(&ZJNI_handler, TRUE);
#endif
}

static JavaVM *jvm = nullptr;

class TLS;

template <> struct ZmCleanup<TLS> {
  enum { Level = ZmCleanupLevel::Platform };
};

class TLS : public ZmObject {
public:
  TLS() {
    ZmThreadName name = ZmThreadContext::self()->name();
    JavaVMAttachArgs args{
      JNI_VERSION_1_4, const_cast<char *>(name.data()), 0 };
    jvm->AttachCurrentThread((void **)&m_env, &args);
  }
  TLS(JNIEnv *env) : m_env(env) { }
  ~TLS() { m_env = nullptr; }

  static void attach() {
    ZmSpecific<TLS>::instance();
  }
  static void detach() {
    ZmSpecific<TLS>::instance()->m_env = nullptr;
    jvm->DetachCurrentThread();
  }

  static void env(JNIEnv *env) {
    ZmSpecific<TLS>::instance(new TLS(env));
  }
  static JNIEnv *env() {
    return ZmSpecific<TLS>::instance()->m_env;
  }

private:
  JNIEnv	*m_env = nullptr;
};

static jclass nullptrex_class = 0;
static jmethodID nullptrex_ctor = 0;

static jclass instant_class = 0;
static jmethodID instant_ofEpochSecond = 0;
static jmethodID instant_getEpochSecond = 0;
static jmethodID instant_getNano = 0;

JavaVM *ZJNI::vm() { return jvm; }
JNIEnv *ZJNI::env() { return TLS::env(); }
void ZJNI::env(JNIEnv *ptr) { TLS::env(ptr); }

void ZJNI::attach()
{
  TLS::attach();
}

void ZJNI::detach()
{
  TLS::detach();
}

void ZJNI::throwNPE(JNIEnv *env, ZuString s)
{
  if (ZuUnlikely(!env)) return;
  env->Throw((jthrowable)env->NewObject(
	nullptrex_class, nullptrex_ctor, ZJNI::s2j(env, s)));
}

int ZJNI::bind(JNIEnv *env, const char *cname,
    const JNINativeMethod *methods, unsigned n)
{
  jclass c = env->FindClass(cname);
  if (!c) return -1;
  return bind(env, c, methods, n);
}

int ZJNI::bind(JNIEnv *env, jclass c,
    const JNINativeMethod *methods, unsigned n)
{
  return env->RegisterNatives(c, methods, n);
}

int ZJNI::bind(JNIEnv *env, const char *cname, JavaMethod *methods, unsigned n)
{
  jclass c = env->FindClass(cname);
  if (!c) return -1;
  return bind(env, c, methods, n);
}

int ZJNI::bind(JNIEnv *env, jclass c, JavaMethod *methods, unsigned n)
{
  for (unsigned i = 0; i < n; i++)
    if (!(methods[i].mid = env->GetMethodID(
	    c, methods[i].name, methods[i].signature))) return -1;
  return 0;
}

int ZJNI::bindStatic(
    JNIEnv *env, const char *cname, JavaMethod *methods, unsigned n)
{
  jclass c = env->FindClass(cname);
  if (!c) return -1;
  return bindStatic(env, c, methods, n);
}

int ZJNI::bindStatic(JNIEnv *env, jclass c, JavaMethod *methods, unsigned n)
{
  for (unsigned i = 0; i < n; i++)
    if (!(methods[i].mid = env->GetStaticMethodID(
	    c, methods[i].name, methods[i].signature))) return -1;
  return 0;
}

int ZJNI::bind(JNIEnv *env, const char *cname, JavaField *fields, unsigned n)
{
  jclass c = env->FindClass(cname);
  if (!c) return -1;
  return bind(env, c, fields, n);
}

int ZJNI::bind(JNIEnv *env, jclass c, JavaField *fields, unsigned n)
{
  for (unsigned i = 0; i < n; i++)
    if (!(fields[i].fid = env->GetFieldID(
	    c, fields[i].name, fields[i].type))) return -1;
  return 0;
}

int ZJNI::bindStatic(
    JNIEnv *env, const char *cname, JavaField *fields, unsigned n)
{
  jclass c = env->FindClass(cname);
  if (!c) return -1;
  return bindStatic(env, c, fields, n);
}

int ZJNI::bindStatic(JNIEnv *env, jclass c, JavaField *fields, unsigned n)
{
  for (unsigned i = 0; i < n; i++)
    if (!(fields[i].fid = env->GetStaticFieldID(
	    c, fields[i].name, fields[i].type))) return -1;
  return 0;
}

jint ZJNI::load(JavaVM *jvm_)
{
  jvm = jvm_;

  JNIEnv *env = 0;

  if (jvm_->GetEnv((void **)&env, JNI_VERSION_1_4) != JNI_OK || !env)
    return -1;

  TLS::env(env);

  {
    jclass c = env->FindClass("java/lang/NullPointerException");
    nullptrex_class = (jclass)env->NewGlobalRef(c);
    env->DeleteLocalRef(c);
    nullptrex_ctor = env->GetMethodID(nullptrex_class,
	"<init>", "(Ljava/lang/String;)V");
  }

  {
    jclass c = env->FindClass("java/time/Instant");
    instant_class = (jclass)env->NewGlobalRef(c);
    env->DeleteLocalRef(c);
    instant_ofEpochSecond =
      env->GetStaticMethodID(instant_class,
	  "ofEpochSecond", "(JJ)Ljava/time/Instant;");
    instant_getEpochSecond =
      env->GetMethodID(instant_class, "getEpochSecond", "()J");
    instant_getNano =
      env->GetMethodID(instant_class, "getNano", "()I");
  }

  return JNI_VERSION_1_4;
}

void ZJNI::unload(JavaVM *jvm_)
{
  jvm = jvm_;
  TLS::detach();
  jvm = nullptr;
}

void ZJNI::final(JNIEnv *env)
{
  if (nullptrex_class) env->DeleteGlobalRef(nullptrex_class);
  if (instant_class) env->DeleteGlobalRef(instant_class);
}

ZtDate ZJNI::j2t(JNIEnv *env, jobject obj, bool dlr)
{
  if (ZuUnlikely(!env || !obj)) return ZtDate();
  ZtDate d{
      (time_t)env->CallLongMethod(obj, instant_getEpochSecond),
      (int)env->CallIntMethod(obj, instant_getNano)};
  if (dlr) env->DeleteLocalRef(obj);
  return d;
}

// ZtDate -> Java Instant
jobject ZJNI::t2j(JNIEnv *env, const ZtDate &t) {
  if (ZuUnlikely(!env)) return nullptr;
  return env->CallStaticObjectMethod(instant_class,
      instant_ofEpochSecond, (jlong)t.time(), (jint)t.nsec());
}
