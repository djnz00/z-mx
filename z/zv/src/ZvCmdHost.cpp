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

#include <zlib/ZvCmdHost.hpp>

#include <zlib/ZiModule.hpp>

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
    ZmRef<ZvCf> cf = m_syntax->subset(name, false, true);
    cf->fromString(syntax, false);
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
    args->fromArgs(m_syntax->subset(name, false), args_);
    if (args->getInt("help", 0, 1, 0)) {
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
