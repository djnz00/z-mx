//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// ZvCmd locally hosted commands

#include <zlib/ZvCmdHost.hh>

#include <zlib/ZiModule.hh>

void ZvCmdHost::init()
{
  m_syntax = new ZvCf();
  addCmd("help", "", ZvCmdFn::Member<&ZvCmdHost::helpCmd>::fn(this),
      "list commands", "usage: help [COMMAND]");
  addCmd("loadmod", "", ZvCmdFn::Member<&ZvCmdHost::loadModCmd>::fn(this),
      "load application-specific module", "usage: loadmod MODULE");
}

void ZvCmdHost::final()
{
  while (auto fn = m_finalFn.pop()) fn();
  m_syntax = nullptr;
  m_cmds.clean();
}

void ZvCmdHost::addCmd(
    ZuString name, ZuString syntax, ZvCmdFn fn, ZtString brief, ZtString usage)
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

bool ZvCmdHost::hasCmd(ZuString name) { return m_cmds.find(name); }

void ZvCmdHost::processCmd(ZvCmdContext *ctx, ZuArray<const ZtString> args_)
{
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
    if (args->getBool("help")) {
      out << cmd->val().usage << '\n';
      return;
    }
    (cmd->val().fn)(ctx);
  } catch (const ZvCmdUsage &) {
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
}

void ZvCmdHost::helpCmd(ZvCmdContext *ctx)
{
  auto &out = ctx->out;
  const auto &args = ctx->args;
  int argc = ZuBox<int>{args->get("#")};
  if (argc > 2) throw ZvCmdUsage();
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

void ZvCmdHost::loadModCmd(ZvCmdContext *ctx)
{
  auto &out = ctx->out;
  const auto &args = ctx->args;
  int argc = ZuBox<int>{args->get("#")};
  if (argc != 2) throw ZvCmdUsage{};
  ZiModule module;
  ZiModule::Path name = args->get("1", true);
  ZtString e;
  if (module.load(name, false, &e) < 0) {
    out << "failed to load \"" << name << "\": " << ZuMv(e) << '\n';
    executed(1, ctx);
    return;
  }
  ZvCmdInitFn initFn = reinterpret_cast<ZvCmdInitFn>(
      module.resolve("ZvCmd_plugin", &e));
  if (!initFn) {
    module.unload();
    out << "failed to resolve \"ZvCmd_plugin\" in \"" <<
      name << "\": " << ZuMv(e) << '\n';
    executed(1, ctx);
    return;
  }
  (*initFn)(static_cast<ZvCmdHost *>(this));
  out << "module \"" << name << "\" loaded\n";
  executed(0, ctx);
}

void ZvCmdHost::finalFn(ZmFn<> fn)
{
  m_finalFn << ZuMv(fn);
}
