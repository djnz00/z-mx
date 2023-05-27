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
  const auto &cliValue = ZtREGEX("\G[^\"'`#;\s]+");
  const auto &cliSglQuote = ZtREGEX("\G'");
  const auto &cliSglQuotedValue = ZtREGEX("\G[^'`]+");
  const auto &cliDblQuote = ZtREGEX("\G\"");
  const auto &cliDblQuotedValue = ZtREGEX("\G[^\"`]+");
  const auto &cliQuoted = ZtREGEX("\G`.");	
  const auto &cliWhiteSpace = ZtREGEX("\G\s+");
  const auto &cliComment = ZtREGEX("\G#");
  const auto &cliSemicolon = ZtREGEX("\G;");

  unsigned n = line.length();

  ZtString value;
  ZtRegex::Captures c;
  unsigned off = 0;

  args.length(0);

  while (off < n) {
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
  const auto &argLongFlag = ZtREGEX("^--([\w\.]+)$");	// --arg
  const auto &argLongValue = ZtREGEX("^--([\w\.]+)=");	// --arg=val
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
	      fromArg(
		  longOpt, type,
		  args[n].data() + (args[n][0] == '`' && args[n][1] == '-'));
	      n++;
	    } else {
	      fromArg(longOpt, type, deflt);
	    }
	  } else {
	    if (n == l) throw Usage{args[0], shortOpt};
	    fromArg(
		longOpt, type,
		args[n].data() + (args[n][0] == '`' && args[n][1] == '-'));
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
  for (; i < n && key[i] != '.'; ++i);
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
  // std::cerr << "fromArg(key=" << key << ")\n" << std::flush;
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

  // std::cerr << "fromArg(key=" << key << "): " << value << '\n' << std::flush;
  values.push(ZuMv(value));
  if (multi) goto value;
}

static ZuPair<ZtString, unsigned>
scanString(ZuString in, unsigned off, Cf::Defines *defines)
{
  unsigned n = in.length();

  if (!n) return {ZtString{}, 0U};

  const auto &strSpace = ZtREGEX("\G\s+");
  const auto &strValue = ZtREGEX("\G\w+");
  const auto &strValueQuoted = ZtREGEX("\G\\(.)");
  const auto &strRefVar = ZtREGEX("\G\${(\w+)}");
  const auto &strDblQuote = ZtREGEX("\G\"");
  const auto &strValueDblQuoted = ZtREGEX("\G[^\\\"]+");

  ZtString value;
  ZtRegex::Captures c;
  unsigned off_ = off;

  while (off < n) {
    if (strValue.m(in, c, off)) {
      off += c[1].length();
      value += c[1];
      continue;
    }
    if (strValueQuoted.m(in, c, off)) {
      off += c[1].length();
      value += c[2];
      continue;
    }
    if (strRefVar.m(in, c, off)) {
      off += c[1].length();
      ZuString d = defines->findVal(c[2]);
      if (!d) { ZtString env{c[2]}; d = ::getenv(env); }
      if (d) value += d;
      continue;
    }
    if (strDblQuote.m(in, c, off)) {
      off += c[1].length();
      while (off < n) {
	if (strValueDblQuoted.m(in, c, off)) {
	  off += c[1].length();
	  value += c[1];
	  continue;
	}
	if (strValueQuoted.m(in, c, off)) {
	  off += c[1].length();
	  value += c[2];
	  continue;
	}
	++off; // elide strDblQuote.m() of closing "
#if 0
	if (strDblQuote.m(in, c, off)) {
	  off += c[1].length();
	  break;
	}
#endif
	break;
      }
      continue;
    }
    break;
  }
  if (off > off_ && off < n && strSpace.m(in, c, off)) off += c[1].length();
  return {ZuMv(value), off - off_};
}

static ZtString quoteString(ZuString in)
{
  unsigned n = in.length();

  if (!n) return "\"\"";

  const auto &quoteNonWord = ZtREGEX("\W");
  const auto &quoteRegular = ZtREGEX("\G[^\\\"]+");

  ZtRegex::Captures c;

  if (!quoteNonWord.m(in, c, 0)) return in;

  ZtString out{n + (n>>3) + 2}; // 1+1/8 size estimation

  unsigned off = 0;

  out << '"';
  while (off < n) {
    if (quoteRegular.m(in, c, off)) {
      off += c[1].length();
      out << c[1];
      continue;
    }
    out << '\\' << in[off++];
  }
  out << '"';
  return out;
}

static ZtString quoteArg(ZuString in)
{
  unsigned n = in.length();

  if (!n) return "\"\"";

  const auto &quoteSpecial = ZtREGEX("[`,]");
  const auto &quoteRegular = ZtREGEX("\G[^`,]+");

  ZtRegex::Captures c;

  if (!quoteSpecial.m(in, c, 0)) return in;

  ZtString out{n + (n>>3)}; // 1+1/8 size estimation
  unsigned off = 0;

  while (off < n) {
    if (quoteRegular.m(in, c, off)) {
      off += c[1].length();
      out << c[1];
      continue;
    }
    out << '`' << in[off++];
  }
  return out;
}

void Cf::fromString(ZuString in, ZuString fileName, ZmRef<Defines> defines)
{
  unsigned n = in.length();

  if (!n) return;

  const auto &fileSpace = ZtREGEX("\G\s+");

  const auto &fileComment = ZtREGEX("\G#[^\n]*\n\s*");
  const auto &fileDirective = ZtREGEX("\G(%\w+)\s+");

  const auto &fileBeginScope = ZtREGEX("\G\{\s*");
  const auto &fileEndScope = ZtREGEX("\G\}\s*");

  const auto &fileBeginArray = ZtREGEX("\G\[\s*");
  const auto &fileEndArray = ZtREGEX("\G\]\s*");
  const auto &fileComma = ZtREGEX("\G,\s*");

  const auto &fileDefine = ZtREGEX("(\w+)\s+");

  const auto &fileLine = ZtREGEX("\G[^\n]*\n");

  enum {
    KVMask	= 0x0003,
    Key		= 0x0000,
    Value	= 0x0001,
    Next	= 0x0002,

    ArrayMask	= 0x000c,
    NoArray	= 0x0000,
    UnkArray	= 0x0004,
    ValArray	= 0x0008,
    CfArray	= 0x000c,
  };

  auto self = this;
  unsigned state = Key;
  unsigned index = 0;
  using State = ZuPair<unsigned, unsigned>; // state, index
  ZtArray<State> stack;
  TreeNodeRef node = nullptr;
  ZtRegex::Captures c;
  unsigned off = 0;
  
  if (fileSpace.m(in, c, off)) off += c[1].length();
  while (off < n) {
    // comments
    if (fileComment.m(in, c, off)) {
      off += c[1].length();
      continue;
    }
    if ((state & KVMask) == Key) {
      // directives
      if (fileDirective.m(in, c, off)) {
	off += c[1].length();
	if (c[2] == "%include") {
	  auto [file, o] = scanString(in, off, defines);
	  if (!file) goto syntax;
	  off += o;
	  ZmRef<Cf> incCf = new Cf{};
	  incCf->fromFile(file, defines);
	  self->merge(incCf);
	  continue;
	}
	if (c[2] == "%define") {
	  if (!fileDefine.m(in, c, off)) goto syntax;
	  off += c[1].length();
	  auto var = c[2];
	  auto [value, o] = scanString(in, off, defines);
	  if (!o) goto syntax;
	  off += o;
	  defines->del(var);
	  defines->add(var, ZuMv(value));
	  continue;
	}
	goto syntax;
      }
      // end scope
      if (fileEndScope.m(in, c, off)) {
	if (!stack) goto syntax;
	off += c[1].length();
	self = self->node()->owner;
	{ auto p = stack.pop(); state = p.p<0>(); index = p.p<1>(); }
	continue;
      }
      // key
      auto [key, o] = scanString(in, off, defines);
      if (!o) goto syntax;
      off += o;
      node = self->m_tree.find(key);
      if (!node) self->m_tree.addNode(node = new TreeNode{self, key});
      state = (state & ~KVMask) | Value;
      continue;
    }
    if ((state & KVMask) == Value) {
      // begin array
      if (fileBeginArray.m(in, c, off)) {
	if ((state & ArrayMask) != NoArray) goto syntax;
	off += c[1].length();
	state = (state & ~ArrayMask) | UnkArray;
	continue;
      }
      // begin scope
      if (fileBeginScope.m(in, c, off)) {
	if (node->values.type() == 0 && node->values.p<0>()) goto syntax;
	if ((state & ArrayMask) == ValArray) goto syntax;
	off += c[1].length();
	if (node->values.p<1>().length() <= index)
	  node->set<1>(self = new Cf{node}, index);
	else
	  self = node->values.p<1>()[index];
	if ((state & ArrayMask) == NoArray) {
	  state = (state & ~KVMask) | Key;
	  node = nullptr;
	} else {
	  if ((state & ArrayMask) == UnkArray)
	    state = (state & ~ArrayMask) | CfArray;
	  state = (state & ~KVMask) | Next;
	}
	stack.push(State{state, index});
	state = Key;
	index = 0;
	node = nullptr;
	continue;
      }
      // comma
      if (fileComma.m(in, c, off)) {
	if ((state & ArrayMask) == NoArray) goto syntax;
	off += c[1].length();
	++index;
	continue;
      }
      auto [value, o] = scanString(in, off, defines);
      if (!o) goto syntax;
      if (node->values.type() == 1 && node->values.p<1>()) goto syntax;
      if ((state & ArrayMask) == CfArray) goto syntax;
      off += o;
      node->set<0>(ZuMv(value), index);
      if ((state & ArrayMask) == NoArray) {
	state = (state & ~KVMask) | Key;
	node = nullptr;
      } else {
	if ((state & ArrayMask) == UnkArray)
	  state = (state & ~ArrayMask) | ValArray;
	state = (state & ~KVMask) | Next;
      }
      continue;
    }
    if ((state & KVMask) == Next) {
      // comma
      if (fileComma.m(in, c, off)) {
	off += c[1].length();
	state = (state & ~KVMask) | Value;
	++index;
	continue;
      }
      // end array
      if (fileEndArray.m(in, c, off)) {
	off += c[1].length();
	state = (state & ~(ArrayMask | KVMask)) | Key;
	index = 0;
	node = nullptr;
	continue;
      }
    }
  }
  return;

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
}

// suppress security warning about getenv()
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4996)
#endif

void Cf::fromEnv(const char *name, ZmRef<Defines> defines)
{
  ZuString in = ::getenv(name);
  unsigned n = in.length();

  if (!n) return;

  const auto &envEquals = ZtREGEX("\G=");
  const auto &envColon = ZtREGEX("\G:");

  const auto &envBeginScope = ZtREGEX("\G\{");
  const auto &envEndScope = ZtREGEX("\G\}");

  const auto &envBeginArray = ZtREGEX("\G\[");
  const auto &envEndArray = ZtREGEX("\G\]");
  const auto &envComma = ZtREGEX("\G,");

  enum {
    KVMask	= 0x0003,
    Key		= 0x0000,
    Value	= 0x0001,
    Next	= 0x0002,

    ArrayMask	= 0x000c,
    NoArray	= 0x0000,
    UnkArray	= 0x0004,
    ValArray	= 0x0008,
    CfArray	= 0x000c,

    First	= 0x0010
  };

  auto self = this;
  unsigned state = First | Key;
  unsigned index = 0;
  using State = ZuPair<unsigned, unsigned>; // state, index
  ZtArray<State> stack;
  TreeNodeRef node = nullptr;
  ZtRegex::Captures c;
  unsigned off = 0;
  
  while (off < n) {
    if ((state & KVMask) == Key) {
      // end scope
      if (envEndScope.m(in, c, off)) {
	if (!stack) goto syntax;
	off += c[1].length();
	self = self->node()->owner;
	{ auto p = stack.pop(); state = p.p<0>(); index = p.p<1>(); }
	continue;
      }
      // key
      if (!(state & First)) {
	if (!envColon.m(in, c, off)) goto syntax;
	off += c[1].length();
      } else
	state &= ~First;
      auto [key, o] = scanString(in, off, defines);
      if (!o) goto syntax;
      off += o;
      if (!envEquals.m(in, c, off)) goto syntax;
      off += c[1].length();
      node = self->m_tree.find(key);
      if (!node) self->m_tree.addNode(node = new TreeNode{self, key});
      state = (state & ~KVMask) | Value;
      continue;
    }
    if ((state & KVMask) == Value) {
      // begin array
      if (envBeginArray.m(in, c, off)) {
	if ((state & ArrayMask) != NoArray) goto syntax;
	off += c[1].length();
	state = (state & ~ArrayMask) | UnkArray;
	continue;
      }
      // begin scope
      if (envBeginScope.m(in, c, off)) {
	if (node->values.type() == 0 && node->values.p<0>()) goto syntax;
	if ((state & ArrayMask) == ValArray) goto syntax;
	off += c[1].length();
	if (node->values.p<1>().length() <= index)
	  node->set<1>(self = new Cf{node}, index);
	else
	  self = node->values.p<1>()[index];
	if ((state & ArrayMask) == NoArray) {
	  state = (state & ~KVMask) | Key;
	  node = nullptr;
	} else {
	  if ((state & ArrayMask) == UnkArray)
	    state = (state & ~ArrayMask) | CfArray;
	  state = (state & ~KVMask) | Next;
	}
	stack.push(State{state, index});
	state = First | Key;
	index = 0;
	node = nullptr;
	continue;
      }
      // comma
      if (envComma.m(in, c, off)) {
	if ((state & ArrayMask) == NoArray) goto syntax;
	off += c[1].length();
	++index;
	continue;
      }
      auto [value, o] = scanString(in, off, defines);
      if (node->values.type() == 1 && node->values.p<1>()) goto syntax;
      if ((state & ArrayMask) == CfArray) goto syntax;
      off += o;
      node->set<0>(ZuMv(value), index);
      if ((state & ArrayMask) == NoArray) {
	state = (state & ~KVMask) | Key;
	node = nullptr;
      } else {
	if ((state & ArrayMask) == UnkArray)
	  state = (state & ~ArrayMask) | ValArray;
	state = (state & ~KVMask) | Next;
      }
      continue;
    }
    if ((state & KVMask) == Next) {
      // comma
      if (envComma.m(in, c, off)) {
	off += c[1].length();
	state = (state & ~KVMask) | Value;
	++index;
	continue;
      }
      // end array
      if (envEndArray.m(in, c, off)) {
	off += c[1].length();
	state = (state & ~(ArrayMask | KVMask)) | Key;
	index = 0;
	node = nullptr;
	continue;
      }
    }
  }
  return;

syntax:
  throw EnvSyntax{off, in[off]};
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
	  arg << quoteArg(values[i]);
	  if (i < n - 1) arg << ',';
	}
	args.push(ZuMv(arg));
      }
    } else {
      if (const auto &values = node->values.p<1>())
	if (auto cf = values[0])
	  cf->toArgs(args, ZtString{} << prefix << node->CfNode::key << '.');
    }
  }
}

void Cf::print(ZmStream &s, ZtString &indent) const
{
  auto i = m_tree.readIterator();
  while (auto node = i.iterate()) {
    if (node->values.type() == 0) {
      const auto &values = node->values.p<0>();
      if (unsigned n = values.length()) {
	s << indent << node->CfNode::key << ' ';
	if (n > 1) s << "[ ";
	for (unsigned j = 0; j < n; j++) {
	  s << quoteString(values[j]);
	  if (j < n - 1) s << ", ";
	}
	if (n > 1)
	  s << " ]\n";
	else
	  s << "\n";
      }
    } else {
      const auto &values = node->values.p<1>();
      if (unsigned n = values.length()) {
	s << indent << node->CfNode::key << ' ';
	if (n > 1) s << "[ ";
	for (unsigned j = 0; j < n; j++) {
	  if (auto cf = values[j]) {
	    s << "{\n";
	    indent.append("  ", 2);
	    cf->print(s, indent);
	    indent.length_(indent.length() - 2);
	    s << indent << "}";
	  } else
	    s << "{}";
	  if (j < n - 1) s << ", ";
	}
	if (n > 1)
	  s << " ]\n";
	else
	  s << "\n";
      }
    }
  }
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

ZtArray<ZtString> *Cf::setArray(ZuString key)
{
  auto node = mkNode(key);
  auto &values = node->values.p<0>();
  return &values;
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

ZtArray<ZmRef<Cf>> *Cf::setCfArray(ZuString key)
{
  auto node = mkNode(key);
  auto &values = node->values.p<1>();
  return &values;
}

void Cf::unset(ZuString fullKey)
{
  ZuString key;
  auto self = mkScope(fullKey, key);
  self->m_tree.del(key);
}

void Cf::clean()
{
  m_tree.clean();
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
