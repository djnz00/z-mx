//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed under the MIT license (see LICENSE for details)

// application configuration

#include <zlib/ZmAssert.hh>

#include <zlib/ZePlatform.hh>

#include <zlib/ZtRegex.hh>

#include <zlib/ZvCf.hh>

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
  const auto &cliValue = ZtREGEX("\G[^\"'\\#;\s]+");
  const auto &cliSglQuote = ZtREGEX("\G'");
  const auto &cliSglQuotedValue = ZtREGEX("\G[^'\\]+");
  const auto &cliDblQuote = ZtREGEX("\G\"");
  const auto &cliDblQuotedValue = ZtREGEX("\G[^\"\\]+");
  const auto &cliQuoted = ZtREGEX("\G\\.");	
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
		  args[n].data() + (args[n][0] == '\\' && args[n][1] == '-'));
	      n++;
	    } else {
	      fromArg(longOpt, type, deflt);
	    }
	  } else {
	    if (n == l) throw Usage{args[0], shortOpt};
	    fromArg(
		longOpt, type,
		args[n].data() + (args[n][0] == '\\' && args[n][1] == '-'));
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
	  option->getEnum<ZvOptTypes::Map>("type", ZvOptValue), c[3]);
    } else {
      fromArg(ZtString{ZuBox<int>{p++}}, ZvOptValue, args[i]);
    }
  }
  {
    auto node = m_tree.find("#");
    if (!node) m_tree.addNode(node = new Node{this, "#"});
    node->set_<ZtString>(ZuBox<int>{p});
  }
  return p;
}

int Cf::fromArgs(const ZvOpt *opts, const ZtArray<ZtString> &args)
{
  ZmRef<Cf> options = new Cf{};

  for (int i = 0; opts[i].m_long; i++) {
    ZmRef<Cf> option = new Cf{};
    auto type = ZvOptTypes::Map::v2s(opts[i].m_type);
    if (!type) throw Usage{args[0], opts[i].m_long};
    option->set("type", type);
    if (opts[i].m_default) option->set("default", opts[i].m_default);
    options->setCf(opts[i].m_long, ZuMv(option));
    if (opts[i].m_short) options->set(opts[i].m_short, opts[i].m_long);
  }

  return fromArgs(options, args);
}

template <unsigned Q = Quoting::File>
ZuIfT<(Q & Quoting::Mask) == Quoting::File, ZuTuple<ZtString, unsigned>>
scanString(ZuString in, unsigned off, Cf::Defines *defines = nullptr)
{
  unsigned n = in.length();

  if (!n) return {ZtString{}, 0U};

  const auto &fileSpace = ZtREGEX("\G\s+");
  const auto &fileUnquoted = (Q & Quoting::Key) ?
    ZtREGEX("\G[^\\\"\$\s{}\[\]\.]+") :
    ZtREGEX("\G[^\\\"\$\s{}\[\],]+");
  const auto &fileQuoted = ZtREGEX("\G\\(.)");
  const auto &fileRefVar = ZtREGEX("\G\${(\w+)}");
  const auto &fileDblQuote = ZtREGEX("\G\"");
  const auto &fileDblUnquoted = ZtREGEX("\G[^\\\"]+");

  ZtString value;
  ZtRegex::Captures c;
  unsigned off_ = off;

  while (off < n) {
    if (fileUnquoted.m(in, c, off)) {
      off += c[1].length();
      value += c[1];
      continue;
    }
    if (fileQuoted.m(in, c, off)) {
      off += c[1].length();
      value += c[2];
      continue;
    }
    if (fileRefVar.m(in, c, off)) {
      off += c[1].length();
      ZuString d;
      if (defines) d = defines->findVal(c[2]);
      if (!d) { ZtString env{c[2]}; d = ::getenv(env); }
      if (d) value += d;
      continue;
    }
    if (fileDblQuote.m(in, c, off)) {
      off += c[1].length();
      while (off < n) {
	if (fileDblUnquoted.m(in, c, off)) {
	  off += c[1].length();
	  value += c[1];
	  continue;
	}
	if (fileQuoted.m(in, c, off)) {
	  off += c[1].length();
	  value += c[2];
	  continue;
	}
	++off; // elide fileDblQuote.m() of closing "
#if 0
	if (fileDblQuote.m(in, c, off)) {
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
  if (off > off_ && off < n && fileSpace.m(in, c, off)) off += c[1].length();
  return {ZuMv(value), off - off_};
}

template <unsigned Q = Quoting::File>
ZuIfT<(Q & Quoting::Mask) == Quoting::Env, ZuTuple<ZtString, unsigned>>
scanString(ZuString in, unsigned off, Cf::Defines *defines = nullptr)
{
  unsigned n = in.length();

  if (!n) return {ZtString{}, 0U};

  const auto &envUnquoted = (Q & Quoting::Key) ?
    ZtREGEX("\G[^\\\"\$:{}\[\]\.]+") :
    ZtREGEX("\G[^\\\"\$;{}\[\],]+");
  const auto &envQuoted = ZtREGEX("\G\\(.)");
  const auto &envRefVar = ZtREGEX("\G\${(\w+)}");
  const auto &envDblQuote = ZtREGEX("\G\"");
  const auto &envDblUnquoted = ZtREGEX("\G[^\\\"]+");

  ZtString value;
  ZtRegex::Captures c;
  unsigned off_ = off;

  while (off < n) {
    if (envUnquoted.m(in, c, off)) {
      off += c[1].length();
      value += c[1];
      continue;
    }
    if (envQuoted.m(in, c, off)) {
      off += c[1].length();
      value += c[2];
      continue;
    }
    if (envRefVar.m(in, c, off)) {
      off += c[1].length();
      ZuString d;
      if (defines) d = defines->findVal(c[2]);
      if (!d) { ZtString env{c[2]}; d = ::getenv(env); }
      if (d) value += d;
      continue;
    }
    if (envDblQuote.m(in, c, off)) {
      off += c[1].length();
      while (off < n) {
	if (envDblUnquoted.m(in, c, off)) {
	  off += c[1].length();
	  value += c[1];
	  continue;
	}
	if (envQuoted.m(in, c, off)) {
	  off += c[1].length();
	  value += c[2];
	  continue;
	}
	++off; // elide envDblQuote.m() of closing "
#if 0
	if (envDblQuote.m(in, c, off)) {
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
  return {ZuMv(value), off - off_};
}

template <unsigned Q = Quoting::File>
ZuIfT<(Q & Quoting::Mask) == Quoting::File, ZtString>
quoteString(ZuString in)
{
  unsigned n = in.length();

  if (!n) return "\"\"";

  const auto &strQuoted = ZtREGEX("\W");
  const auto &strUnquoted = ZtREGEX("\G[^\\\"]+");

  ZtRegex::Captures c;

  if (!strQuoted.m(in, c, 0)) return in;

  ZtString out{n + (n>>3) + 2}; // 1+1/8 size estimation

  unsigned off = 0;

  out << '"';
  while (off < n) {
    if (strUnquoted.m(in, c, off)) {
      off += c[1].length();
      out << c[1];
      continue;
    }
    out << '\\' << in[off++];
  }
  out << '"';
  return out;
}

template <unsigned Q = Quoting::File>
ZuIfT<(Q & Quoting::Mask) == Quoting::CLI, ZuTuple<ZtString, unsigned>>
scanString(ZuString in, unsigned off, Cf::Defines *defines = nullptr)
{
  unsigned n = in.length();

  if (!n) return {ZtString{}, 0U};

  const auto &argUnquoted = (Q & Quoting::Key) ?
    ZtREGEX("\G[^\\\$\.\[\]]+") :
    ZtREGEX("\G[^\\\$,]+");
  const auto &argQuoted = ZtREGEX("\G\\(.)");
  const auto &argRefVar = ZtREGEX("\G\${(\w+)}");

  ZtString value;
  ZtRegex::Captures c;
  unsigned off_ = off;

  while (off < n) {
    if (argUnquoted.m(in, c, off)) {
      off += c[1].length();
      value += c[1];
      continue;
    }
    if (argQuoted.m(in, c, off)) {
      off += c[1].length();
      value += c[2];
      continue;
    }
    if (argRefVar.m(in, c, off)) {
      off += c[1].length();
      ZuString d;
      if (defines) d = defines->findVal(c[2]);
      if (!d) { ZtString env{c[2]}; d = ::getenv(env); }
      if (d) value += d;
      continue;
    }
    break;
  }
  return {ZuMv(value), off - off_};
}

template <unsigned Q = Quoting::File>
ZuIfT<(Q & Quoting::Mask) == Quoting::CLI, ZtString>
quoteString(ZuString in)
{
  unsigned n = in.length();

  if (!n) return {};

  const auto &argQuoted =    ZtREGEX("\G[\\\$\.\[\],]");
  const auto &argUnquoted = ZtREGEX("\G[^\\\$\.\[\],]+");

  ZtRegex::Captures c;

  if (!argQuoted.m(in, c, 0)) return in;

  ZtString out{n + (n>>3)}; // 1+1/8 size estimation
  unsigned off = 0;

  while (off < n) {
    if (argUnquoted.m(in, c, off)) {
      off += c[1].length();
      out << c[1];
      continue;
    }
    out << '\\' << in[off++];
  }
  return out;
}

template <unsigned Q = Quoting::File>
ZuIfT<(Q & Quoting::Mask) == Quoting::Raw, ZuTuple<ZtString, unsigned>>
scanString(ZuString in, unsigned off, Cf::Defines *defines = nullptr)
{
  if (off >= in.length()) return {ZtString{}, 0U};
  in.offset(off);
  return {in, in.length()};
}

template <unsigned Q = Quoting::File>
ZuIfT<(Q & Quoting::Mask) == Quoting::Raw, ZtString>
quoteString(ZuString in)
{
  return in;
}

static const auto &indexMatch() { return ZtREGEX("\G\[(\d+)\]$"); }

template <unsigned Q = Quoting::File>
ZuTuple<ZtString, int, unsigned>
scanKey(ZuString in, unsigned off, int index, Cf::Defines *defines = nullptr)
{
  unsigned n = in.length();

  if (off >= n) {
null:
    return {ZtString{}, 0, 0U};
  }

  unsigned off_ = off;

  auto [key, o] = scanString<Q | Quoting::Key>(in, off, defines);
  if (!o) goto null;
  off += o;

  if constexpr ((Q & Quoting::Mask) == Quoting::CLI) {
    ZtRegex::Captures c;

    if (indexMatch().m(in, c, off)) {
      off += c[1].length();
      index = ZuBox<unsigned>{c[2]};
    }
  }

  return {ZuMv(key), index, off - off_};
}

static const auto &matchDot() { return ZtREGEX("\G\."); }

template <unsigned Q>
ZuTuple<Cf *, ZtString, int, unsigned>
Cf::getScope_(ZuString in, Cf::Defines *defines) const
{
  unsigned n = in.length();

  ZtRegex::Captures c;
  unsigned off = 0;

  auto this_ = const_cast<Cf *>(this);
  ZtString key;
  int index = -1;

  while (off < n) {
    auto [key_, index_, o] = scanKey<Q>(in, off, -1, defines);
    off += o;
    key = ZuMv(key_);
    index = index_;
    if (!matchDot().m(in, c, off)) break;
    off += c[1].length();
    auto node = this_->m_tree.find(key);
    if (!node) goto null;
    if (index < 0) {
      if (!node->CfNode::data.is<ZmRef<Cf>>()) goto null;
      this_ = node->get_<ZmRef<Cf>>();
    } else {
      if (!node->CfNode::data.is<CfArray>()) goto null;
      this_ = node->get_<CfArray>().get(index);
    }
    if (!this_) goto null;
  }

  return {this_, ZuMv(key), index, off};

null:
  return {nullptr, ZtString{}, 0, 0U};
}
  
template <unsigned Q>
ZuTuple<Cf *, ZtString, int, unsigned>
Cf::mkScope_(ZuString in, Cf::Defines *defines)
{
  unsigned n = in.length();

  ZtRegex::Captures c;
  unsigned off = 0;

  Cf *this_ = this;
  ZtString key;
  int index = -1;

  while (off < n) {
    auto [key_, index_, o] = scanKey<Q>(in, off, -1, defines);
    off += o;
    key = ZuMv(key_);
    index = index_;
    if (!matchDot().m(in, c, off)) break;
    off += c[1].length();
    auto node = this_->m_tree.find(key);
    if (!node) this_->m_tree.addNode(node = new Cf::Node{this_, key});
    if (index < 0) {
      this_ = node->get_<ZmRef<Cf>>();
      if (!this_) node->set_<ZmRef<Cf>>(this_ = new Cf{node});
    } else {
      this_ = node->getElem_<CfArray>(index);
      if (!this_) node->setElem_<CfArray>(index, this_ = new Cf{node});
    }
  }

  return {this_, ZuMv(key), index, off};
}

template <unsigned Q>
ZuTuple<Cf *, CfNode *, int, unsigned>
Cf::mkNode_(ZuString in)
{
  auto [this_, key, index, o] = mkScope_<Q>(in);
  auto node = this_->m_tree.find(key);
  if (!node) this_->m_tree.addNode(node = new Node{this_, key});
  return {this_, ZuMv(node), index, o};
}

void Cf::fromArg(ZuString key, int type, ZuString in)
{
  const auto &argComma = ZtREGEX("\G,");

  auto [this_, node, index, o] = mkNode_<Quoting::CLI>(key);

  switch (type) {
    case ZvOptFlag:
    case ZvOptValue: {
      auto [value, o] = scanString<Quoting::CLI>(in, 0);
      if (index < 0)
	node->set_<ZtString>(value);
      else
	node->setElem_<StrArray>(index, value);
    } break;
    case ZvOptArray: {
      unsigned n = in.length();

      ZtRegex::Captures c;
      unsigned off = 0;

      if (!node->data.is<StrArray>())
	new (node->data.new_<StrArray>()) StrArray{};
      auto &values = node->data.p<StrArray>();
      values.clear();

      if (off < n) do {
	auto [value, o] = scanString<Quoting::CLI>(in, off);
	values.push(value);
	off += o;
	if (off >= n || !argComma.m(in, c, off)) break;
	off += c[1].length();
      } while (true);
    } break;
  }
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
    StrArray_	= 0x0008,
    CfArray_	= 0x000c,
  };

  auto this_ = this;
  unsigned state = Key;
  int index = -1;
  using State = ZuTuple<unsigned, int>; // state, index
  ZtArray<State> stack;
  Node *node = nullptr;
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
	  this_->merge(incCf);
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
	this_ = this_->node()->owner;
	{
	  auto [ state_, index_ ] = stack.pop();
	  state = state_;
	  index = index_;
	}
	continue;
      }
      // key
      auto [key, index_, o] = scanKey(in, off, index, defines);
      if (!o) goto syntax;
      index = index_;
      off += o;
      node = this_->m_tree.find(key);
      if (!node) this_->m_tree.addNode(node = new Node{this_, key});
      state = (state & ~KVMask) | Value;
      continue;
    }
    if ((state & KVMask) == Value) {
      // begin array
      if (fileBeginArray.m(in, c, off)) {
	if ((state & ArrayMask) != NoArray) goto syntax;
	off += c[1].length();
	state = (state & ~ArrayMask) | UnkArray;
	index = 0;
	continue;
      }
      // begin scope
      if (fileBeginScope.m(in, c, off)) {
	switch (node->CfNode::data.type()) {
	  case Data::Index<Null>{}:
	    break;
	  case Data::Index<ZtString>{}:
	  case Data::Index<StrArray>{}:
	    goto syntax;
	  case Data::Index<ZmRef<Cf>>{}:
	    if (index >= 0) goto syntax;
	    break;
	  case Data::Index<CfArray>{}:
	    if (index < 0) goto syntax;
	    break;
	}
	if ((state & ArrayMask) == StrArray_) goto syntax;
	off += c[1].length();
	if (index < 0) {
	  this_ = node->get_<ZmRef<Cf>>();
	  if (!this_) node->set_<ZmRef<Cf>>(this_ = new Cf{node});
	} else {
	  this_ = node->getElem_<CfArray>(index);
	  if (!this_) node->setElem_<CfArray>(index, this_ = new Cf{node});
	}
	if ((state & ArrayMask) == NoArray) {
	  state = (state & ~KVMask) | Key;
	  node = nullptr;
	} else {
	  if ((state & ArrayMask) == UnkArray)
	    state = (state & ~ArrayMask) | CfArray_;
	  state = (state & ~KVMask) | Next;
	}
	stack.push(State{state, index});
	state = Key;
	index = -1;
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
      switch (node->CfNode::data.type()) {
	case Data::Index<Null>{}:
	  break;
	case Data::Index<ZtString>{}:
	  if (index >= 0) goto syntax;
	  break;
	case Data::Index<StrArray>{}:
	  if (index < 0) goto syntax;
	  break;
	case Data::Index<ZmRef<Cf>>{}:
	case Data::Index<CfArray>{}:
	  goto syntax;
      }
      if ((state & ArrayMask) == CfArray_) goto syntax;
      off += o;
      if (index < 0)
	node->set_<ZtString>(ZuMv(value));
      else
	node->setElem_<StrArray>(index, ZuMv(value));
      if ((state & ArrayMask) == NoArray) {
	state = (state & ~KVMask) | Key;
	node = nullptr;
      } else {
	if ((state & ArrayMask) == UnkArray)
	  state = (state & ~ArrayMask) | StrArray_;
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
	index = -1;
	node = nullptr;
	continue;
      }
      goto syntax;
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

  const auto &envColon = ZtREGEX("\G:");
  const auto &envSemiColon = ZtREGEX("\G;");

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
    StrArray_	= 0x0008,
    CfArray_	= 0x000c,

    First	= 0x0010
  };

  auto this_ = this;
  unsigned state = First | Key;
  int index = -1;
  using State = ZuTuple<unsigned, int>; // state, index
  ZtArray<State> stack;
  Node *node = nullptr;
  ZtRegex::Captures c;
  unsigned off = 0;
  
  while (off < n) {
    if ((state & KVMask) == Key) {
      // end scope
      if (envEndScope.m(in, c, off)) {
	if (!stack) goto syntax;
	off += c[1].length();
	this_ = this_->node()->owner;
	{
	  auto [ state_, index_ ] = stack.pop();
	  state = state_;
	  index = index_;
	}
	continue;
      }
      // key
      if (!(state & First)) {
	if (!envSemiColon.m(in, c, off)) goto syntax;
	off += c[1].length();
      } else
	state &= ~First;
      auto [key, index_, o] = scanKey<Quoting::Env>(in, off, index, defines);
      if (!o) goto syntax;
      index = index_;
      off += o;
      if (!envColon.m(in, c, off)) goto syntax;
      off += c[1].length();
      node = this_->m_tree.find(key);
      if (!node) this_->m_tree.addNode(node = new Node{this_, key});
      state = (state & ~KVMask) | Value;
      continue;
    }
    if ((state & KVMask) == Value) {
      // begin array
      if (envBeginArray.m(in, c, off)) {
	if ((state & ArrayMask) != NoArray) goto syntax;
	off += c[1].length();
	state = (state & ~ArrayMask) | UnkArray;
	index = 0;
	continue;
      }
      // begin scope
      if (envBeginScope.m(in, c, off)) {
	switch (node->CfNode::data.type()) {
	  case Data::Index<Null>{}:
	    break;
	  case Data::Index<ZtString>{}:
	  case Data::Index<StrArray>{}:
	    goto syntax;
	  case Data::Index<ZmRef<Cf>>{}:
	    if (index >= 0) goto syntax;
	    break;
	  case Data::Index<CfArray>{}:
	    if (index < 0) goto syntax;
	    break;
	}
	if ((state & ArrayMask) == StrArray_) goto syntax;
	off += c[1].length();
	if (index < 0) {
	  this_ = node->get_<ZmRef<Cf>>();
	  if (!this_) node->set_<ZmRef<Cf>>(this_ = new Cf{node});
	} else {
	  this_ = node->getElem_<CfArray>(index);
	  if (!this_) node->setElem_<CfArray>(index, this_ = new Cf{node});
	}
	if ((state & ArrayMask) == NoArray) {
	  state = (state & ~KVMask) | Key;
	  node = nullptr;
	} else {
	  if ((state & ArrayMask) == UnkArray)
	    state = (state & ~ArrayMask) | CfArray_;
	  state = (state & ~KVMask) | Next;
	}
	stack.push(State{state, index});
	state = First | Key;
	index = -1;
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
      auto [value, o] = scanString<Quoting::Env>(in, off, defines);
      switch (node->CfNode::data.type()) {
	case Data::Index<Null>{}:
	  break;
	case Data::Index<ZtString>{}:
	  if (index >= 0) goto syntax;
	  break;
	case Data::Index<StrArray>{}:
	  if (index < 0) goto syntax;
	  break;
	case Data::Index<ZmRef<Cf>>{}:
	case Data::Index<CfArray>{}:
	  goto syntax;
      }
      if ((state & ArrayMask) == CfArray_) goto syntax;
      off += o;
      if (index < 0)
	node->set_<ZtString>(ZuMv(value));
      else
	node->setElem_<StrArray>(index, ZuMv(value));
      if ((state & ArrayMask) == NoArray) {
	state = (state & ~KVMask) | Key;
	node = nullptr;
      } else {
	if ((state & ArrayMask) == UnkArray)
	  state = (state & ~ArrayMask) | StrArray_;
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
	index = -1;
	node = nullptr;
	continue;
      }
      goto syntax;
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
  for (int i = 0; i < argc; i++) argv[i] = ZuMv(args[i]).release();
}

void Cf::freeArgs(int argc, char **argv)
{
  for (int i = 0; i < argc; i++) ZtString::free(argv[i]);
  ::free(argv);
}

void Cf::toArgs(ZtArray<ZtString> &args, ZuString prefix) const
{
  auto i = m_tree.readIterator();
  while (auto node = i.iterate())
    switch (node->CfNode::data.type()) {
      case Data::Index<Null>{}:
	break;
      case Data::Index<ZtString>{}:
      case Data::Index<StrArray>{}: {
	ZtString arg;
	if (!ZtREGEX("^\d+$").m(node->CfNode::key))
	  arg << "--" << prefix << node->CfNode::key << '=';
	if (node->CfNode::data.is<ZtString>())
	  arg << quoteString<Quoting::CLI>(node->get_<ZtString>());
	else
	  node->CfNode::data.p<StrArray>().all(
	      [&arg, first = true](const ZtString &value) mutable {
	    if (ZuUnlikely(first)) first = false; else arg << ',';
	    arg << quoteString<Quoting::CLI>(value);
	  });
	args.push(ZuMv(arg));
      } break;
      case Data::Index<ZmRef<Cf>>{}:
	if (Cf *cf = node->get_<ZmRef<Cf>>())
	  cf->toArgs(args, ZtString{} << prefix << node->CfNode::key << '.');
	break;
      case Data::Index<CfArray>{}: {
	auto prefix_ = ZtString{} << prefix << node->CfNode::key << '.';
	node->CfNode::data.p<CfArray>().all(
	    [&args, prefix_ = ZuMv(prefix_)](Cf *cf) {
	  cf->toArgs(args, prefix_);
	});
      } break;
    }
}

void Cf::print(ZmStream &s, ZtString &indent) const
{
  auto i = m_tree.readIterator();
  while (auto node = i.iterate()) {
    s << indent << quoteString(node->CfNode::key) << ' ';
    switch (node->CfNode::data.type()) {
      case Data::Index<Null>{}:
	break;
      case Data::Index<ZtString>{}:
      case Data::Index<StrArray>{}: {
	if (node->CfNode::data.is<ZtString>())
	  s << quoteString(node->get_<ZtString>());
	else {
	  s << '[';
	  node->CfNode::data.p<StrArray>().all(
	      [&s, first = true](const ZtString &value) mutable {
	    if (ZuUnlikely(first)) first = false; else s << ", ";
	    s << quoteString(value);
	  });
	  s << ']';
	}
	s << '\n';
      } break;
      case Data::Index<ZmRef<Cf>>{}:
      case Data::Index<CfArray>{}: {
	auto output = [&indent](Cf *cf, ZmStream &s) mutable {
	  if (!cf || !cf->count()) { s << "{}"; return; }
	  s << "{\n";
	  indent.append("  ", 2);
	  cf->print(s, indent);
	  indent.length_(indent.length() - 2);
	  s << indent << '}';
	};
	if (node->CfNode::data.is<ZmRef<Cf>>())
	  output(node->get_<ZmRef<Cf>>(), s);
	else
	  node->CfNode::data.p<CfArray>().all(
	      [&s, output = ZuMv(output), first = true](Cf *cf) mutable {
	    if (ZuUnlikely(first)) first = false; else s << ",\n";
	    output(cf, s);
	  });
	s << '\n';
      } break;
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

ZuTuple<Cf *, ZtString> Cf::getScope(ZuString fullKey) const
{
  auto [this_, key, index, o] = getScope_<Quoting::Raw>(fullKey);
  return {this_, key};
}

CfNode *Cf::mkNode(ZuString fullKey)
{
  auto [this_, node, index, o] = mkNode_<Quoting::Raw>(fullKey);
  return node;
}

void Cf::set(ZuString key, ZtString value)
{
  auto [this_, node, index, o] = mkNode_<Quoting::Raw>(key);
  if (index < 0)
    node->set_<ZtString>(ZuMv(value));
  else
    node->setElem_<StrArray>(index, ZuMv(value));
}

ZmRef<Cf> Cf::mkCf(ZuString key)
{
  auto [this_, node, index, o] = mkNode_<Quoting::Raw>(key);
  ZmRef<Cf> cf = new Cf{node};
  if (index < 0)
    node->set_<ZmRef<Cf>>(cf);
  else
    node->setElem_<CfArray>(index, cf);
  return cf;
}

void Cf::setCf(ZuString key, ZmRef<Cf> cf)
{
  auto [this_, node, index, o] = mkNode_<Quoting::Raw>(key);
  cf->m_node = node;
  if (index < 0)
    node->set_<ZmRef<Cf>>(ZuMv(cf));
  else
    node->setElem_<CfArray>(index, ZuMv(cf));
}

void Cf::unset(ZuString fullKey)
{
  auto [this_, key, index, o] = getScope_<Quoting::Raw>(fullKey);
  if (this_) this_->m_tree.del(key);
}

void Cf::clean()
{
  m_tree.clean();
}

void Cf::merge(const Cf *cf)
{
  auto i = cf->m_tree.readIterator();
  while (auto srcNode = i.iterate()) {
    auto dstNode = m_tree.find(srcNode->CfNode::key);
    if (!dstNode)
      m_tree.addNode(dstNode = new Node{this, srcNode->CfNode::key});
    switch (srcNode->CfNode::data.type()) {
      case Data::Index<Null>{}:
	break;
      case Data::Index<ZtString>{}:
	dstNode->set_<ZtString>(srcNode->get_<ZtString>());
	break;
      case Data::Index<StrArray>{}:
	dstNode->set_<StrArray>(srcNode->get_<StrArray>());
	break;
      case Data::Index<ZmRef<Cf>>{}: {
	if (auto srcCf = srcNode->get_<ZmRef<Cf>>()) {
	  auto dstCf = dstNode->get_<ZmRef<Cf>>();
	  if (!dstCf) dstNode->set_<ZmRef<Cf>>(dstCf = new Cf{dstNode});
	  dstCf->merge(srcCf);
	}
      } break;
      case Data::Index<CfArray>{}: {
	for (unsigned i = 0, n = srcNode->get_<CfArray>().length();
	    i < n; i++)
	  if (auto srcCf = srcNode->getElem_<CfArray>(i)) {
	    auto dstCf = dstNode->getElem_<CfArray>(i);
	    if (!dstCf) dstNode->set_<CfArray>(dstCf = new Cf{dstNode});
	    dstCf->merge(srcCf);
	  }
      } break;
    }
  }
}

} // ZvCf_

#ifdef _MSC_VER
#pragma warning(pop)
#endif
