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
  addCmd("help", "", ZcmdFn{this, ZmFnMember<&ZcmdHost::helpCmd>{}},
      "list commands", "Usage: help [COMMAND]");
  addCmd("loadmod", "", ZcmdFn{this, ZmFnMember<&ZcmdHost::loadModCmd>{}},
      "load application-specific module", "Usage: loadmod MODULE");
}

void ZcmdHost::final()
{
  while (auto fn = m_finalFn.pop()) fn();
  m_syntax = nullptr;
  m_cmds.clean();
}

void ZcmdHost::addCmd(
    ZuCSpan name, ZuCSpan syntax, ZcmdFn fn, ZtString brief, ZtString usage)
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

bool ZcmdHost::hasCmd(ZuCSpan name) { return m_cmds.find(name); }

void ZcmdHost::processCmd(ZcmdContext *ctx, ZuSpan<const ZtString> args_)
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
  out << "Commands:\n\n";
  {
    auto i = m_cmds.readIterator();
    while (auto cmd = i.iterate()) {
      ZuCSpan key = cmd->key();
      ZuCSpan tabs = "\t\t";
      if (key.length() >= 8) tabs.offset(1);
      out << key << tabs << cmd->val().brief << '\n';
    }
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
