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

// ZvCmd locally hosted commands

#ifndef ZvCmdHost_HPP
#define ZvCmdHost_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

#include <zlib/ZuString.hpp>

#include <zlib/ZmPolymorph.hpp>
#include <zlib/ZmRef.hpp>
#include <zlib/ZmRBTree.hpp>

#include <zlib/ZiIOBuf.hpp>

#include <zlib/Zfb.hpp>

#include <zlib/ZvCf.hpp>

class ZvCmdHost;
class ZvCmdDispatcher;
namespace Ztls { class Random; }

struct ZvCmdContext {
  ZvCmdHost		*app_ = nullptr;	
  void			*link_ = nullptr;	// opaque to plugin
  void			*user_ = nullptr;	// ''
  ZmRef<ZvCf>		args;
  FILE			*file = nullptr;	// file output
  ZtString		out;			// string output
  int			code = 0;		// result code
  bool			interactive = false;	// true unless scripted

  template <typename T = ZvCmdHost>
  auto app() { return static_cast<T *>(app_); }
  template <typename T> auto link() { return static_cast<T *>(link_); }
  template <typename T> auto user() { return static_cast<T *>(user_); }
};

// command handler (context)
using ZvCmdFn = ZmFn<ZvCmdContext *>;
// can be thrown by command function
struct ZvCmdUsage { };

class ZvAPI ZvCmdHost {
public:
  void init();
  void final();

  void addCmd(ZuString name,
      ZuString syntax, ZvCmdFn fn, ZtString brief, ZtString usage);

  bool hasCmd(ZuString name);

  int processCmd(ZvCmdContext *, ZuArray<const ZtString> args);

  int loadMod(ZuString name, ZtString &out);

  void finalFn(ZmFn<>);

  virtual int executed(ZvCmdContext *) { return 0; }

  virtual ZvCmdDispatcher *dispatcher() { return nullptr; }
  virtual void send(void *link, ZmRef<ZiAnyIOBuf>) { }

  virtual void target(ZuString) { }
  virtual ZtString getpass(ZuString prompt, unsigned passLen) { return {}; }

  virtual Ztls::Random *rng() { return nullptr; }

private:
  int help(ZvCmdContext *);

private:
  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;

  struct CmdData {
    ZvCmdFn	fn;
    ZtString	brief;
    ZtString	usage;
  };
  using Cmds =
    ZmRBTreeKV<ZtString, CmdData,
      ZmRBTreeUnique<true,
	ZmRBTreeLock<ZmNoLock> > >;

  Lock			m_lock;
    ZmRef<ZvCf>		  m_syntax;
    Cmds		  m_cmds;
    ZtArray<ZmFn<>>	  m_finalFn;
};

// loadable module must export void ZCmd_plugin(ZCmdHost *)
extern "C" {
  typedef void (*ZvCmdInitFn)(ZvCmdHost *host);
}

#endif /* ZvCmdHost_HPP */
