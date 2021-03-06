//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2

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

// Mx JNI

#include <jni.h>

#include <zlib/ZJNI.hpp>

#include <mxbase/MxDecimalJNI.hpp>

#include <mxbase/MxSideJNI.hpp>
#include <mxbase/MxInstrIDSrcJNI.hpp>
#include <mxbase/MxPutCallJNI.hpp>
#include <mxbase/MxTickDirJNI.hpp>
#include <mxbase/MxTradingSessionJNI.hpp>
#include <mxbase/MxTradingStatusJNI.hpp>

#include <mxbase/MxInstrKeyJNI.hpp>
#include <mxbase/MxFutKeyJNI.hpp>
#include <mxbase/MxOptKeyJNI.hpp>
#include <mxbase/MxUniKeyJNI.hpp>

#include <mxbase/MxBaseJNI.hpp>

extern "C" {
  MxBaseExtern jint JNI_OnLoad(JavaVM *, void *);
  MxBaseExtern void JNI_OnUnload(JavaVM *, void *);
}

MxBaseExtern jint JNI_OnLoad(JavaVM *jvm, void *)
{
  // std::cout << "JNI_OnLoad()\n" << std::flush;

  jint v = ZJNI::load(jvm);

  JNIEnv *env = ZJNI::env();

  if (!env || MxBaseJNI::bind(env) < 0) return -1;

  return v;
}

MxBaseExtern void JNI_OnUnload(JavaVM *jvm, void *)
{
  ZJNI::unload(jvm);
}

MxBaseExtern int MxBaseJNI::bind(JNIEnv *env)
{
  if (MxDecimalJNI::bind(env) < 0) return -1;

  if (MxSideJNI::bind(env) < 0) return -1;
  if (MxInstrIDSrcJNI::bind(env) < 0) return -1;
  if (MxPutCallJNI::bind(env) < 0) return -1;
  if (MxTickDirJNI::bind(env) < 0) return -1;
  if (MxTradingSessionJNI::bind(env) < 0) return -1;
  if (MxTradingStatusJNI::bind(env) < 0) return -1;

  if (MxInstrKeyJNI::bind(env) < 0) return -1;
  if (MxFutKeyJNI::bind(env) < 0) return -1;
  if (MxOptKeyJNI::bind(env) < 0) return -1;
  if (MxUniKeyJNI::bind(env) < 0) return -1;

  return 0;
}  

void MxBaseJNI::final(JNIEnv *env)
{
  MxInstrKeyJNI::final(env);
  MxFutKeyJNI::final(env);
  MxOptKeyJNI::final(env);
  MxUniKeyJNI::final(env);
  
  MxSideJNI::final(env);
  MxInstrIDSrcJNI::final(env);
  MxPutCallJNI::final(env);
  MxTickDirJNI::final(env);
  MxTradingSessionJNI::final(env);
  MxTradingStatusJNI::final(env);

  MxDecimalJNI::final(env);

  ZJNI::final(env);
}
