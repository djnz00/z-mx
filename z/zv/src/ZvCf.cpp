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

// application configuration

#include <zlib/ZuBox.hpp>

#include <zlib/ZmAssert.hpp>

#include <zlib/ZePlatform.hpp>

#include <zlib/ZtICmp.hpp>
#include <zlib/ZtRegex.hpp>

#include <zlib/ZvCf.hpp>

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4996) // getenv warning
#endif

namespace ZvCf_ {

int Cf::fromCLI(Cf *syntax, ZuString line)
{
  ZtArray<ZtString> args;
  parseCLI(line, args);
  if (!args.length()) return 0;
  return fromArgs(syntax->getCf(args[0]), args);
}

int Cf::fromCLI(const ZvOpt *opts, ZuString line)
{
  ZtArray<ZtString> args;
  parseCLI(line, args);
  if (!args.length()) return 0;
  return fromArgs(opts, args);
}

void Cf::parseCLI(ZuString line, ZtArray<ZtString> &args)
{
  args.length(0);
  ZtString value;
  const auto &cliValue = ZtREGEX("\G[^\"'`#;\s]+");
  const auto &cliSglQuote = ZtREGEX("\G'");
  const auto &cliSglQuotedValue = ZtREGEX("\G[^'`]+");
  const auto &cliDblQuote = ZtREGEX("\G\"");
  const auto &cliDblQuotedValue = ZtREGEX("\G[^\"`]+");
  const auto &cliQuoted = ZtREGEX("\G`.");	
  const auto &cliWhiteSpace = ZtREGEX("\G\s+");
  const auto &cliComment = ZtREGEX("\G#");
  const auto &cliSemicolon = ZtREGEX("\G;");
  ZtRegex::Captures c;
  unsigned off = 0;

  while (off < line.length()) {
    if (cliValue.m(line, c, off)) {
      off += c[1].length();
      value += c[1];
      continue;
    }
    if (cliSglQuote.m(line, c, off)) {
      off += c[1].length();
      while (off < line.length()) {
	if (cliSglQuotedValue.m(line, c, off)) {
	  off += c[1].length();
	  value += c[1];
	  continue;
	}
	if (cliQuoted.m(line, c, off)) {
	  off += c[1].length();
	  value += c[1];
	  continue;
	}
	if (cliSglQuote.m(line, c, off)) {
	  off += c[1].length();
	  break;
	}
      }
      continue;
    }
    if (cliDblQuote.m(line, c, off)) {
      off += c[1].length();
      while (off < line.length()) {
	if (cliDblQuotedValue.m(line, c, off)) {
	  off += c[1].length();
	  value += c[1];
	  continue;
	}
	if (cliQuoted.m(line, c, off)) {
	  off += c[1].length();
	  value += c[1];
	  continue;
	}
	if (cliDblQuote.m(line, c, off)) {
	  off += c[1].length();
	  break;
	}
      }
      continue;
    }
    if (cliQuoted.m(line, c, off)) {
      off += c[1].length();
      value += c[1];
      continue;
    }
    if (cliWhiteSpace.m(line, c, off)) {
      off += c[1].length();
      args.push(ZuMv(value));
      value.null();
      continue;
    }
    if (cliComment.m(line, c, off)) break;
    if (cliSemicolon.m(line, c, off)) break;
    ZmAssert(false);	// should not get here
    break;
  }
  if (value) args.push(ZuMv(value));
}

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4267)
#endif

int Cf::fromArgs(const ZvOpt *opts, int argc, char **argv)
{
  if (ZuUnlikely(argc < 0)) return 0;
  ZtArray<ZtString> args(argc);
  for (unsigned i = 0; i < static_cast<unsigned>(argc); i++)
    args.push(argv[i]);
  return fromArgs(opts, args);
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif

int Cf::fromArgs(Cf *options, const ZtArray<ZtString> &args)
{
  int i, j, n, l, p;
  const auto &argShort = ZtREGEX("^-(\w+)$");		// -a
  const auto &argLongFlag = ZtREGEX("^--([\w:]+)$");	// --arg
  const auto &argLongValue = ZtREGEX("^--([\w:]+)=");	// --arg=val
  ZtRegex::Captures c;
  ZmRef<Cf> option;

  p = 0;
  l = args.length();
  for (i = 0; i < l; i = n) {
    n = i + 1;
    if (argShort.m(args[i], c)) {
      int m = c[2].length();
      for (j = 0; j < m; j++) {
	ZtString shortOpt(ZtString::Copy, c[2].data() + j, 1);
	ZtString longOpt;
	if (!options ||
	    !(longOpt = options->get(shortOpt)) ||
	    !(option = options->getCf(longOpt)))
	  throw Usage{args[0], shortOpt};
	int type = option->getEnum<ZvOptTypes::Map, true>("type");
	if (type == ZvOptFlag) {
	  fromArg(longOpt, ZvOptFlag, "1");
	} else {
	  ZuString deflt = option->get("default");
	  if (deflt) {
	    if (n < l && args[n][0] != '-') {
	      fromArg(longOpt, type,
		      args[n].data() +
			(args[n][0] == '`' && args[n][1] == '-'));
	      n++;
	    } else {
	      fromArg(longOpt, type, deflt);
	    }
	  } else {
	    if (n == l) throw Usage{args[0], shortOpt};
	    fromArg(longOpt, type,
		    args[n].data() +
		      (args[n][0] == '`' && args[n][1] == '-'));
	    n++;
	  }
	}
      }
    } else if (argLongFlag.m(args[i], c)) {
      ZtString longOpt = c[2];
      if (!options ||
	  !(option = options->getCf(longOpt)))
	throw Usage{args[0], longOpt};
      int type = option->getEnum<ZvOptTypes::Map, true>("type");
      if (type == ZvOptFlag) {
	fromArg(longOpt, ZvOptFlag, "1");
      } else {
	ZuString deflt = option->get("default");
	if (!deflt) throw Usage{args[0], longOpt};
	fromArg(longOpt, type, deflt);
      }
    } else if (argLongValue.m(args[i], c)) {
      ZtString longOpt = c[2];
      if (!options || !(option = options->getCf(longOpt)))
	throw Usage{args[0], longOpt};
      fromArg(longOpt,
	  option->getEnum<ZvOptTypes::Map>("type", ZvOptScalar), c[3]);
    } else {
      fromArg(ZtString(ZuBox<int>{p++}), ZvOptScalar, args[i]);
    }
  }
  {
    TreeNodeRef node = m_tree.find("#");
    if (!node) m_tree.addNode(node = new TreeNode{this, "#"});
    auto &values = node->values.p<0>();
    values.null();
    values.push(ZuBox<int>{p});
  }
  return p;
}

int Cf::fromArgs(const ZvOpt *opts, const ZtArray<ZtString> &args)
{
  ZmRef<Cf> options = new Cf{};

  for (int i = 0; opts[i].m_long; i++) {
    ZmRef<Cf> option = new Cf{};
    static const char *types[] = { "flag", "scalar", "multi" };
    if (opts[i].m_type < 0 ||
	opts[i].m_type >= (int)(sizeof(types) / sizeof(types[0])))
      throw Usage{args[0], opts[i].m_long};
    option->set("type", types[opts[i].m_type]);
    if (opts[i].m_default) option->set("default", opts[i].m_default);
    options->setCf(opts[i].m_long, option);
    if (opts[i].m_short) options->set(opts[i].m_short, opts[i].m_long);
  }

  return fromArgs(options, args);
}

static ZuString scope_(ZuString &key)
{
  if (!key) return key;
  unsigned i = 0, n = key.length();
  for (; i < n && key[i] != ':'; ++i);
  ZuString s{key.data(), i};
  key.offset(i + (i < n));
  return s;
}

Cf *Cf::getScope(ZuString fullKey, ZuString &key) const
{
  const Cf *self = this;
  ZuString scope = scope_(fullKey);
  ZuString nscope;
  if (scope)
    while (nscope = scope_(fullKey)) {
      TreeNodeRef node = self->m_tree.find(scope);
      if (!node || node->values.type() != 1) return nullptr;
      const auto &cfs = node->values.p<1>();
      if (!cfs || !cfs[0]) return nullptr;
      self = cfs[0];
      scope = nscope;
    }
  key = scope;
  return const_cast<ZvCf *>(self);
}

Cf *Cf::mkScope(ZuString fullKey, ZuString &key)
{
  Cf *self = this;
  ZuString scope = scope_(fullKey);
  ZuString nscope;
  if (scope)
    while (nscope = scope_(fullKey)) {
      TreeNodeRef node = self->m_tree.find(scope);
      if (!node) self->m_tree.addNode(node = new TreeNode{self, scope});
      auto &values_ = node->values.p<1>();
      if (!values_ || !values_[0])
	node->set<1>(self = new Cf{node});
      else
	self = values_[0];
      scope = nscope;
    }
  key = scope;
  return self;
}

static bool flagValue(const ZtString &s)
{
  using Cmp = ZtICmp<ZtString>;
  return s == "1" || Cmp::equals(s, "y") || Cmp::equals(s, "yes");
}

void Cf::fromArg(ZuString fullKey, int type, ZuString argVal)
{
  ZuString key;
  auto self = mkScope(fullKey, key);
  TreeNodeRef node = self->m_tree.find(key);
  if (!node) self->m_tree.addNode(node = new TreeNode{self, key});
  auto &values = node->values.p<0>();
  values.null();

  if (type == ZvOptFlag) {
    values.push(flagValue(argVal) ? "1" : "0");
    return;
  }

  // type == ZvOptScalar || ZvOptMulti

  const auto &argValue = ZtREGEX("\G[^`]+");
  const auto &argValueMulti = ZtREGEX("\G[^`,]+");
  const auto &argValueQuoted = ZtREGEX("\G`(.)");
  const auto &argValueComma = ZtREGEX("\G,");
  ZtRegex::Captures c;
  unsigned off = 0;

value:
  ZtString value;
  bool multi = false;

append:
  if (type == ZvOptScalar ?
      argValue.m(argVal, c, off) :
      argValueMulti.m(argVal, c, off)) {
    off += c[1].length();
    value += c[1];
  }
  if (argValueQuoted.m(argVal, c, off)) {
    off += c[1].length();
    value += c[2];
    goto append;
  }
  if (type == ZvOptMulti && argValueComma.m(argVal, c, off)) {
    off += c[1].length();
    multi = true;
  }

  values.push(ZuMv(value));
  if (multi) goto value;
}

void Cf::fromString(ZuString in,
    bool validate, ZuString fileName, ZmRef<Defines> defines)
{
  unsigned n = in.length();

  if (!n) return;

  auto self = this;

  const auto &fileSkip = ZtREGEX("\G(?:#[^\n]*\n|\s+)");
  const auto &fileEndScope = ZtREGEX("\G\}");
  const auto &fileKey = ZtREGEX("\G(?:[^$#%`\"{},:=\s]+|[%=]\w+)");
  const auto &fileLine = ZtREGEX("\G[^\n]*\n");
  const auto &fileValue = ZtREGEX("\G[^$#`\"{},\s]+");
  const auto &fileValueQuoted = ZtREGEX("\G`(.)");
  const auto &fileDblQuote = ZtREGEX("\G\"");
  const auto &fileValueDblQuoted = ZtREGEX("\G[^`\"]+");
  const auto &fileBeginScope = ZtREGEX("\G\{");
  const auto &fileComma = ZtREGEX("\G,");
  const auto &fileDefine = ZtREGEX("([^$#%`\"{},:=\s]+)=(.+)");
  const auto &fileValueVar = ZtREGEX("\G\${([^$#%`\"{},:=\s]+)}");
  ZtRegex::Captures c;
  unsigned off = 0;

key:
  while (fileSkip.m(in, c, off))
    off += c[1].length();
  if (self->node() && fileEndScope.m(in, c, off)) {
    off += c[1].length();
    self = self->node()->owner;
    goto key;
  }
  if (!fileKey.m(in, c, off)) {
syntax:
    if (off < n - 1) {
      unsigned lpos = 0, line = 0;
      while (lpos < off && fileLine.m(in, c, lpos)) {
	lpos += c[1].length();
	line++;
      }
      if (!line) line = 1;
      throw Syntax{line, in[off], fileName};
    }
    return;
  }
  off += c[1].length();
  ZuString key = c[1];
  TreeNodeRef node = nullptr;
  if (key[0] != '%') {
    if (!(node = self->m_tree.find(key))) {
      if (validate) throw Invalid{self, key, fileName};
      self->m_tree.addNode(node = new TreeNode{self, key});
    }
  }
  ZtArray<ZtString> values;

value:
  while (fileSkip.m(in, c, off))
    off += c[1].length();

  ZtString value;
  bool multi = false;

append:
  if (fileValue.m(in, c, off)) {
    off += c[1].length();
    value += c[1];
  }
  if (fileValueQuoted.m(in, c, off)) {
    off += c[1].length();
    value += c[2];
    goto append;
  }
  if (fileValueVar.m(in, c, off)) {
    off += c[1].length();
    ZuString d = defines->findVal(c[2]);
    if (!d) { ZtString env{c[2]}; d = ::getenv(env); }
    if (d) value += d;
    goto append;
  }
  if (fileDblQuote.m(in, c, off)) {
    off += c[1].length();
quoted:
    if (fileValueDblQuoted.m(in, c, off)) {
      off += c[1].length();
      value += c[1];
    }
    if (fileValueQuoted.m(in, c, off)) {
      off += c[1].length();
      value += c[2];
      goto quoted;
    }
    if (off < n - 1) {
      off++;
      goto append;
    }
    values.push(ZuMv(value));
    node->values.p<0>(ZuMv(values));
    return;
  }
  if (fileBeginScope.m(in, c, off)) {
    if (!node || value || values.length()) goto syntax;
    off += c[1].length();
    if (!node->values.p<1>()) {
      if (validate) throw Invalid{self, key, fileName};
      node->set<1>(self = new Cf{node});
    } else
      self = node->values.p<1>()[0];
    goto key;
  }
  if (fileComma.m(in, c, off)) {
    off += c[1].length();
    multi = true;
  }

  values.push(ZuMv(value));
  if (multi) goto value;
  if (node) {
    node->values.p<0>(ZuMv(values));
    values.length(0);
  } else {
    if (key == "%include") {
      unsigned n = values.length();
      for (unsigned i = 0; i < n; i++) {
	ZmRef<Cf> incCf = new Cf{};
	incCf->fromFile(values[i], false);
	self->merge(incCf);
      }
    } else if (key == "%define") {
      unsigned n = values.length();
      for (unsigned i = 0; i < n; i++) {
	if (!fileDefine.m(values[i], c, 0))
	  throw BadDefine{values[i], fileName};
	defines->del(c[2]);
	defines->add(c[2], c[3]);
      }
    }
    // other % directives here
  }
  goto key;
}

// suppress security warning about getenv()
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4996)
#endif

void Cf::fromEnv(const char *name, bool validate)
{
  ZuString data = ::getenv(name);
  unsigned n = data.length();

  if (!data) return;

  auto self = this;
  bool first = true;

  unsigned off = 0;
  const auto &envEndScope = ZtREGEX("\G\}");
  const auto &envColon = ZtREGEX("\G:");
  const auto &envKey = ZtREGEX("\G[^#`\"{},:=\s]+");
  const auto &envEquals = ZtREGEX("\G=");
  const auto &envValue = ZtREGEX("\G[^`\"{},:]+");
  const auto &envValueQuoted = ZtREGEX("\G`([`\"{},:\s])");
  const auto &envDblQuote = ZtREGEX("\G\"");
  const auto &envValueDblQuoted = ZtREGEX("\G[^`\"]+");
  const auto &envBeginScope = ZtREGEX("\G\{");
  const auto &envComma = ZtREGEX("\G,");
  ZtRegex::Captures c;

key:
  if (self->node() && envEndScope.m(data, c, off)) {
    off += c[1].length();
    self = self->node()->owner;
    goto key;
  }
  if (!first) {
    if (!envColon.m(data, c, off)) {
      if (off < n - 1) throw EnvSyntax{off, data[off]};
      return;
    }
    off += c[1].length();
  }
  if (!envKey.m(data, c, off)) {
    if (off < n - 1 || !first) throw EnvSyntax{off, data[off]};
    return;
  }
  off += c[1].length();

  first = false;

  ZuString key = c[1];
  TreeNodeRef node = self->m_tree.find(key);
  if (!node) {
    if (validate) throw Invalid{self, key, ZuString{}};
    self->m_tree.addNode(node = new TreeNode{self, key});
  }

  ZtArray<ZtString> values;
value:
  if (!values.length()) {
    if (!envEquals.m(data, c, off))
      throw EnvSyntax{off, data[off]};
    off += c[1].length();
  }

  ZtString value;
  bool multi = false;

append:
  if (envValue.m(data, c, off)) {
    off += c[1].length();
    value += c[1];
  }
  if (envValueQuoted.m(data, c, off)) {
    off += c[1].length();
    value += c[2];
    goto append;
  }
  if (envDblQuote.m(data, c, off)) {
    off += c[1].length();
quoted:
    if (envValueDblQuoted.m(data, c, off)) {
      off += c[1].length();
      value += c[1];
    }
    if (envValueQuoted.m(data, c, off)) {
      off += c[1].length();
      value += c[2];
      goto quoted;
    }
    if (off < n - 1) {
      off++;
      goto append;
    }
    values.push(ZuMv(value));
    node->values.p<0>(ZuMv(values));
    return;
  }
  if (envBeginScope.m(data, c, off)) {
    if (!node || value || values.length()) throw EnvSyntax{off, data[off]};
    off += c[1].length();
    if (!node->values.p<1>()) {
      if (validate) throw Invalid{self, key, ZuString{}};
      node->set<1>(self = new Cf{node});
    } else
      self = node->values.p<1>()[0];
    first = true;
    goto key;
  }
  if (envComma.m(data, c, off)) {
    off += c[1].length();
    multi = true;
  }

  values.push(ZuMv(value));
  if (multi) goto value;
  node->values.p<0>(ZuMv(values));
  values.length(0);
  goto key;
}

#ifdef _MSC_VER
#pragma warning(pop)
#endif

void Cf::toArgs(int &argc, char **&argv) const
{
  ZtArray<ZtString> args;

  toArgs(args, "");
  argc = args.length();
  argv = static_cast<char **>(::malloc(argc * sizeof(char *)));
  ZmAssert(argv);
  if (!argv) throw std::bad_alloc();
  for (int i = 0; i < argc; i++) argv[i] = args[i].release();
}

void Cf::freeArgs(int argc, char **argv)
{
  for (int i = 0; i < argc; i++) ZtString::free(argv[i]);
  ::free(argv);
}

void Cf::toArgs(ZtArray<ZtString> &args, ZuString prefix) const
{
  auto i = m_tree.readIterator();
  while (auto node = i.iterate()) {
    if (node->values.type() == 0) {
      const auto &values = node->values.p<0>();
      int n = values.length();
      if (n) {
	ZtString arg;
	if (!ZtREGEX("^\d+$").m(node->CfNode::key))
	  arg << "--" << prefix << node->CfNode::key << '=';
	for (int i = 0; i < n; i++) {
	  arg << quoteArgValue(values[i]);
	  if (i < n - 1) arg << ',';
	}
	args.push(ZuMv(arg));
      }
    } else {
      if (const auto &values = node->values.p<1>())
	if (auto cf = values[0])
	  cf->toArgs(args, ZtString{} << prefix << node->CfNode::key << ':');
    }
  }
}

ZtString Cf::quoteArgValue(ZuString in)
{
  if (!in) return "\"\"";

  ZtString out = in;

  const auto &argQuote = ZtREGEX("[`,]");
  ZtRegex::Captures c;
  unsigned off = 0;

  while (off < out.length() && argQuote.m(out, c, off)) {
    off = c[0].length();
    out.splice(off, 0, "`", 1);
    off += c[1].length() + 1;
    off++;
  }

  return out;
}

void Cf::print(ZmStream &s, ZtString prefix) const
{
  auto i = m_tree.readIterator();
  while (auto node = i.iterate()) {
    if (node->values.type() == 0) {
      const auto &values = node->values.p<0>();
      unsigned n = values.length();
      if (n) {
	s << prefix << node->CfNode::key << ' ';
	for (int i = 0; i < n; i++) {
	  s << quoteValue(values[i]);
	  if (i < n - 1)
	    s << ", ";
	  else
	    s << '\n';
	}
      }
    } else {
      if (const auto &values = node->values.p<1>())
	if (auto cf = values[0])
	  s << prefix << node->CfNode::key << " {\n" <<
	    cf->prefixed(ZtString{} << "  " << prefix) <<
	    prefix << "}\n";
    }
  }
}

ZtString Cf::quoteValue(ZuString in)
{
  if (!in) return "\"\"";

  ZtString out = in;

  const auto &quote1 = ZtREGEX("[#`\"{},\s]");
  const auto &quote2 = ZtREGEX("[#`\",\s]");
  const auto &quoteValueDbl = ZtREGEX("[`\"]");
  const auto &quoteValue = ZtREGEX("[#`\"{},\s]");
  ZtRegex::Captures c;
  bool doubleQuote = false;
  unsigned off = 0;

  if (quote1.m(out, c, off)) {
    off = c[0].length() + c[1].length();
    if (quote2.m(out, c, off))
      doubleQuote = true;
  }

  off = 0;
  if (doubleQuote) {
    while (quoteValueDbl.m(out, c, off)) {
      off = c[0].length();
      out.splice(off, 0, "`", 1);
      off += c[1].length() + 1;
    }
    out.splice(0, 0, "\"", 1);
    out.splice(out.length(), 0, "\"", 1);
  } else {
    while (quoteValue.m(out, c, off)) {
      off = c[0].length();
      out.splice(off, 0, "`", 1);
      off += c[1].length() + 1;
    }
  }

  return out;
}

void Cf::toFile_(ZiFile &file)
{
  ZtString out;
  out << *this;
  ZeError e;
  if (file.write(out.data(), out.length(), &e) != Zi::OK) throw e;
}

Cf::TreeNodeRef Cf::mkNode(ZuString fullKey)
{
  ZuString key;
  auto self = mkScope(fullKey, key);
  TreeNodeRef node = self->m_tree.find(key);
  if (!node) self->m_tree.addNode(node = new TreeNode{self, key});
  return node;
}

void Cf::set(ZuString key, ZtString value)
{
  auto node = mkNode(key);
  node->set<0>(ZuMv(value));
}

ZtArray<ZtString> *Cf::setMultiple(ZuString key)
{
  auto node = mkNode(key);
  auto &values = node->values.p<0>();
  return &values;
}

void Cf::unset(ZuString fullKey)
{
  ZuString key;
  auto self = mkScope(fullKey, key);
  self->m_tree.del(key);
}

ZmRef<Cf> Cf::mkCf(ZuString key)
{
  TreeNodeRef node = mkNode(key);
  ZmRef<Cf> cf = new Cf{node};
  node->set<1>(cf);
  return cf;
}

void Cf::setCf(ZuString key, ZmRef<Cf> cf)
{
  TreeNodeRef node = mkNode(key);
  node->set<1>(ZuMv(cf));
}

void Cf::merge(const Cf *cf)
{
  auto i = cf->m_tree.readIterator();
  while (TreeNodeRef srcNode = i.iterate()) {
    TreeNodeRef dstNode = m_tree.find(srcNode->CfNode::key);
    if (!dstNode)
      m_tree.addNode(dstNode = new TreeNode{this, srcNode->CfNode::key});
    if (srcNode->values.type() == 0) {
      auto &srcValues = srcNode->values.p<0>();
      auto &dstValues = dstNode->values.p<0>();
      dstValues += srcValues;
    } else if (srcNode->values.type() == 1) {
      auto &srcCfs = srcNode->values.p<1>();
      auto &dstCfs = dstNode->values.p<1>();
      unsigned n = srcCfs.length();
      dstCfs.ensure(n);
      if (dstCfs.length() < n) dstCfs.length(n);
      for (unsigned i = 0; i < n; i++) {
	auto &srcCf = srcCfs[i];
	auto &dstCf = dstCfs[i];
	if (!dstCf) dstCf = new Cf{dstNode};
	dstCf->merge(srcCf);	// recursive
      }
    }
  }
}

} // ZvCf_

#ifdef _MSC_VER
#pragma warning(pop)
#endif
