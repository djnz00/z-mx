//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Zcmd locally hosted commands

#ifndef ZcmdHost_HH
#define ZcmdHost_HH

#ifndef ZcmdLib_HH
#include <zlib/ZcmdLib.hh>
#endif

#include <zlib/ZuString.hh>

#include <zlib/ZmPolymorph.hh>
#include <zlib/ZmRef.hh>
#include <zlib/ZmRBTree.hh>

#include <zlib/ZiIOBuf.hh>

#include <zlib/ZvCf.hh>
#include <zlib/ZvSeqNo.hh>

#include <zlib/Zcmd.hh>

class ZcmdHost;
class ZcmdDispatcher;
namespace Ztls { class Random; }

// on the server side the dest will be a link, which in turn owns
// a reference to the session that can be used to check permissions
struct ZcmdContext {
  using Dest = ZuUnion<FILE *, void *>;		// file or link output

  ZcmdHost		*host = nullptr;	
  Dest			dest;			// destination
  ZmRef<ZvCf>		args;
  ZtString		out;			// string output
  ZvSeqNo		seqNo = 0;
  int			code = 0;		// result code
  bool			interactive = false;	// true unless scripted
};

// command handler (context)
using ZcmdFn = ZmFn<void(ZcmdContext *)>;
// can be thrown by command function
struct ZcmdUsage { };

class ZvAPI ZcmdHost {
public:
  void init();
  void final();

  void addCmd(ZuString name,
      ZuString syntax, ZcmdFn fn, ZtString brief, ZtString usage);

  bool hasCmd(ZuString name);

  void processCmd(ZcmdContext *, ZuArray<const ZtString> args);

  void finalFn(ZmFn<>);

  void executed(int code, ZcmdContext *ctx) {
    ctx->code = code;
    executed(ctx);
  }
  virtual void executed(ZcmdContext *) { }

  virtual ZcmdDispatcher *dispatcher() { return nullptr; }

  virtual void target(ZuString) { }
  virtual ZtString getpass(ZuString prompt, unsigned passLen) { return {}; }

  virtual Ztls::Random *rng() { return nullptr; }

private:
  void helpCmd(ZcmdContext *);
  void loadModCmd(ZcmdContext *);

private:
  using Lock = ZmPLock;
  using Guard = ZmGuard<Lock>;

  struct CmdData {
    ZcmdFn	fn;
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
  typedef void (*ZcmdInitFn)(ZcmdHost *host);
}

#endif /* ZcmdHost_HH */
