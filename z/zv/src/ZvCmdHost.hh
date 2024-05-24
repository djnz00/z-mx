//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// ZvCmd locally hosted commands

#ifndef ZvCmdHost_HH
#define ZvCmdHost_HH

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#include <zlib/ZuString.hh>

#include <zlib/ZmPolymorph.hh>
#include <zlib/ZmRef.hh>
#include <zlib/ZmRBTree.hh>

#include <zlib/ZiIOBuf.hh>

#include <zlib/Zfb.hh>

#include <zlib/ZvCf.hh>

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

  void processCmd(ZvCmdContext *, ZuArray<const ZtString> args);

  void finalFn(ZmFn<>);

  void executed(int code, ZvCmdContext *ctx) {
    ctx->code = code;
    executed(ctx);
  }
  virtual void executed(ZvCmdContext *) { }

  virtual ZvCmdDispatcher *dispatcher() { return nullptr; }
  virtual void send(void *link, ZmRef<ZiAnyIOBuf>) { }

  virtual void target(ZuString) { }
  virtual ZtString getpass(ZuString prompt, unsigned passLen) { return {}; }

  virtual Ztls::Random *rng() { return nullptr; }

private:
  void helpCmd(ZvCmdContext *);
  void loadModCmd(ZvCmdContext *);

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

#endif /* ZvCmdHost_HH */
