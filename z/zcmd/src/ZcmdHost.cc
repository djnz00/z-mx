//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// Zcmd locally hosted commands

#include <zlib/ZcmdHost.hh>

#include <zlib/ZiModule.hh>

void ZcmdHost::init()
{
  m_syntax = new ZvCf();
  addCmd("help", "", ZcmdFn::Member<&ZcmdHost::helpCmd>::fn(this),
      "list commands", "usage: help [COMMAND]");
  addCmd("loadmod", "", ZcmdFn::Member<&ZcmdHost::loadModCmd>::fn(this),
      "load application-specific module", "usage: loadmod MODULE");
}

void ZcmdHost::final()
{
  while (auto fn = m_finalFn.pop()) fn();
  m_syntax = nullptr;
  m_cmds.clean();
}

void ZcmdHost::addCmd(
    ZuString name, ZuString syntax, ZcmdFn fn, ZtString brief, ZtString usage)
{
  Guard guard(m_lock);
  {
    ZmRef<ZvCf> cf = m_syntax->mkCf(name);
    cf->fromString(syntax);
    cf->set("help:type", "flag");
  }
  if (auto cmd = m_cmds.find(name))
    cmd->val() = CmdData{ZuMv(fn), ZuMv(brief), ZuMv(usage)};
  else
    m_cmds.add(name, CmdData{ZuMv(fn), ZuMv(brief), ZuMv(usage)});
}

bool ZcmdHost::hasCmd(ZuString name) { return m_cmds.find(name); }

int ZcmdHost::processCmd(Zum::Session *session, ZmRef<IOBuf> buf, AckFn fn)
{
  auto request = Zfb::GetRoot<Zcmd::fbs::Request>(buf->data());
  auto cmd_ = request->cmd();
  ZtArray<ZtString> args;
  args.length(cmd_->size());
  Zfb::Load::all(cmd_,
      [&args](unsigned i, auto arg_) { args[i] = Zfb::Load::str(arg_); });
  ZcmdContext ctx{
    .app_ = app(), .session = session
  };

  if (!args_) return;
  auto &out = ctx->out;
  const ZtString &name = args_[0];
  typename Cmds::NodeRef cmd;
  try {
    cmd = m_cmds.find(name);
    if (!cmd) throw ZtString("unknown command");
    auto &args = ctx->args;
    args = new ZvCf();
    args->fromArgs(m_syntax->getCf(name), args_);
    if (args->getBool("help"))
      out << cmd->val().usage << '\n';
    else
      (cmd->val().fn)(ctx);
  } catch (const ZcmdUsage &) {
    out << cmd->val().usage << '\n';
    executed(1, ctx);
  } catch (const ZvError &e) {
    out << e << '\n';
    executed(1, ctx);
  } catch (const ZeError &e) {
    out << '"' << name << "\": " << e << '\n';
    executed(1, ctx);
  } catch (const ZtString &s) {
    out << '"' << name << "\": " << s << '\n';
    executed(1, ctx);
  } catch (const ZtArray<char> &s) {
    out << '"' << name << "\": " << s << '\n';
    executed(1, ctx);
  } catch (...) {
    out << '"' << name << "\": unknown exception\n";
    executed(1, ctx);
  }

  fbb.Finish(Zcmd::fbs::CreateReqAck(
	fbb, in->seqNo(), ctx.code, Zfb::Save::str(fbb, ctx.out)));
}

void ZcmdHost::helpCmd(ZcmdContext *ctx)
{
  auto &out = ctx->out;
  const auto &args = ctx->args;
  int argc = ZuBox<int>{args->get("#")};
  if (argc > 2) throw ZcmdUsage();
  if (ZuUnlikely(argc == 2)) {
    auto cmd = m_cmds.find(args->get("1"));
    if (!cmd) {
      out << args->get("1") << ": unknown command\n";
      executed(1, ctx);
      return;
    }
    out << cmd->val().usage << '\n';
    executed(0, ctx);
    return;
  }
  out.size(m_cmds.count_() * 80 + 40);
  out << "commands:\n\n";
  {
    auto i = m_cmds.readIterator();
    while (auto cmd = i.iterate())
      out << cmd->key() << " -- " << cmd->val().brief << '\n';
  }
  executed(0, ctx);
}

void ZcmdHost::loadModCmd(ZcmdContext *ctx)
{
  auto &out = ctx->out;
  const auto &args = ctx->args;
  int argc = ZuBox<int>{args->get("#")};
  if (argc != 2) throw ZcmdUsage{};
  ZiModule module;
  ZiModule::Path name = args->get("1", true);
  ZtString e;
  if (module.load(name, false, &e) < 0) {
    out << "failed to load \"" << name << "\": " << ZuMv(e) << '\n';
    executed(1, ctx);
    return;
  }
  ZcmdInitFn initFn = reinterpret_cast<ZcmdInitFn>(
      module.resolve("Zcmd_plugin", &e));
  if (!initFn) {
    module.unload();
    out << "failed to resolve \"Zcmd_plugin\" in \"" <<
      name << "\": " << ZuMv(e) << '\n';
    executed(1, ctx);
    return;
  }
  (*initFn)(static_cast<ZcmdHost *>(this));
  out << "module \"" << name << "\" loaded\n";
  executed(0, ctx);
}

void ZcmdHost::finalFn(ZmFn<> fn)
{
  m_finalFn << ZuMv(fn);
}
