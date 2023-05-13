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

#ifndef ZvCf_HPP
#define ZvCf_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZvLib_HPP
#include <zlib/ZvLib.hpp>
#endif

#ifndef _MSC_VER
#include <unistd.h>
#endif

#include <zlib/ZuBox.hpp>

#include <zlib/ZmObject.hpp>
#include <zlib/ZmRef.hpp>
#include <zlib/ZmList.hpp>
#include <zlib/ZmRBTree.hpp>
#include <zlib/ZmBackTrace.hpp>

#include <zlib/ZtArray.hpp>
#include <zlib/ZtString.hpp>
#include <zlib/ZtRegex.hpp>

#include <zlib/ZiFile.hpp>

#include <zlib/ZvError.hpp>
#include <zlib/ZvEnum.hpp>

#define ZvCfMaxFileSize	(1<<20)	// 1Mb

#define ZvOptFlag	0
#define ZvOptScalar	1
#define ZvOptMulti	2

extern "C" {
  struct ZvOpt {
    const char	*m_long;
    const char	*m_short;
    int		m_type;		// ZvOptFlag, ZvOptScalar or ZvOptMulti
    const char	*m_default;
  };
};

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4251)
#endif

namespace ZvOptTypes {
  ZtEnumValues_(Flag = ZvOptFlag, Scalar = ZvOptScalar, Multi = ZvOptMulti);
  ZtEnumMap("ZvOptTypes", Map, "flag", Flag, "scalar", Scalar, "multi", Multi);
}

namespace ZvCf_ {

class Cf;

ZtString fullKey(const Cf *cf, ZtString key);

}

namespace ZvCfError {

using Cf = ZvCf_::Cf;

// thrown by all get methods for missing values when required is true
class Required : public ZvError {
public:
  Required(const Cf *cf, ZuString key) :
      m_key{fullKey(cf, key)}, m_bt{1} { }

  const ZtString &key() const { return m_key; }

  void print_(ZmStream &s) const {
    s << '"' << m_key << "\" missing at:\n" << m_bt;
  }

private:
  ZtString	m_key;
  ZmBackTrace	m_bt;
};

// template base class for NValues / Range exceptions
template <typename T> class Range_ : public ZvError {
public:
  Range_(ZtString key, T minimum, T maximum, T value) :
      m_key{ZuMv(key)},
      m_minimum{minimum}, m_maximum{maximum}, m_value{value} { }

  const ZtString &key() const { return m_key; }
  T minimum() const { return m_minimum; }
  T maximum() const { return m_maximum; }
  T value() const { return m_value; }

protected:
  void print__(ZmStream &s, ZuString msg) const {
    s << '"' << m_key << "\" " << msg << ' ' <<
      "min(" << m_minimum << ") <= " << m_value <<
      " <= max(" << m_maximum << ")";
  }

private:
  ZtString	m_key;
  T		m_minimum;
  T		m_maximum;
  T		m_value;
};
// thrown by getInt() on range error
template <typename T>
class Range : public Range_<T> {
  using Base = Range_<T>;
public:
  Range(const Cf *cf, ZuString key, T minimum, T maximum, T value) :
      Base{fullKey(cf, key), minimum, maximum, value} { }
  void print_(ZmStream &s) const {
    Base::print__(s, "out of range");
  }
};
// thrown by getMultiple() on number of values error
class NValues : public Range_<unsigned> {
  using T = unsigned;
  using Base = Range_<T>;
public:
  NValues(const Cf *cf, ZuString key, T minimum, T maximum, T value) :
      Base{fullKey(cf, key), minimum, maximum, value} { }
  void print_(ZmStream &s) const {
    Base::print__(s, "invalid number of values");
  }
};

// thrown by fromArgs() on error
class Usage : public ZvError {
public:
  Usage(ZuString cmd, ZuString option) :
    m_cmd(cmd), m_option(option) { }
  void print_(ZmStream &s) const {
    s << "\"" << m_cmd << "\": invalid option \"" << m_option << '"';
  }

private:
  ZtString	m_cmd;
  ZtString	m_option;
};

// thrown by fromString() and fromFile() for an invalid key
class Invalid : public ZvError {
public:
  Invalid(const Cf *cf, ZuString key, ZuString fileName) :
      m_key(fullKey(cf, key)), m_fileName(fileName) { }
  void print_(ZmStream &s) const {
    if (m_fileName) s << '"' << m_fileName << "\": ";
    s << "invalid key \"" << m_key << '"';
  }
  const ZtString &key() const { return m_key; }

private:
  ZtString	m_key;
  ZtString	m_fileName;
};

// thrown by fromString() and fromFile() on error
class Syntax : public ZvError {
public:
  Syntax(unsigned line, char ch, ZuString fileName) :
    m_line(line), m_ch(ch), m_fileName(fileName) { }
  void print_(ZmStream &s) const {
    if (m_fileName)
      s << '"' << m_fileName << "\":" << ZuBoxed(m_line) << " syntax error";
    else
      s << "syntax error at line " << ZuBoxed(m_line);
    s << " near '";
    if (m_ch >= 0x20 && m_ch < 0x7f)
      s << m_ch;
    else
      s << '\\' << ZuBoxed(static_cast<unsigned>(m_ch) & 0xff).
	fmt<ZuFmt::Hex<0, ZuFmt::Alt<ZuFmt::Right<2>>>>();
    s << '\'';
  }

private:
  unsigned	m_line;
  char		m_ch;
  ZtString	m_fileName;
};

// thrown by fromFile()
class FileOpenError : public ZvError {
public:
  FileOpenError(ZuString fileName, ZeError e) :
    m_fileName{fileName}, m_err{e} { }

  void print_(ZmStream &s) const {
    s << '"' << m_fileName << "\" " << m_err;
  }

private:
  ZtString	m_fileName;
  ZeError   m_err;
};

// thrown by fromFile()
class File2Big : public ZvError {
public:
  File2Big(ZuString fileName) : m_fileName{fileName} { }
  void print_(ZmStream &s) const {
    s << '"' << m_fileName << " file too big";
  };
private:
  ZtString	m_fileName;
};

// thrown by fromEnv() on error
class EnvSyntax : public ZvError {
public:
  EnvSyntax(unsigned pos, char ch) : m_pos{pos}, m_ch{ch} { }
  void print_(ZmStream &s) const {
    s << "syntax error at position " << ZuBoxed(m_pos) << " near '";
    if (m_ch >= 0x20 && m_ch < 0x7f)
      s << m_ch;
    else
      s << '\\' << ZuBoxed(static_cast<unsigned>(m_ch) & 0xff).
	fmt<ZuFmt::Hex<0, ZuFmt::Alt<ZuFmt::Right<2> > >>();
    s << '\'';
  }
private:
  unsigned	m_pos;
  char		m_ch;
};

// thrown by fromString() and fromFile() on bad %define directive
class BadDefine : public ZvError {
public:
  template <typename Define, typename FileName>
  BadDefine(Define &&define, FileName &&fileName) :
    m_define{ZuFwd<Define>(define)}, m_fileName{ZuFwd<FileName>(fileName)} { }
  void print_(ZmStream &s) const {
    if (m_fileName) s << '"' << m_fileName << "\": ";
    s << "bad %%define \"" << m_define << '"';
  }
private:
  ZtString	m_define;
  ZtString	m_fileName;
};

} // ZvCfError

namespace ZvCf_ {

using namespace ZvCfError;

template <typename T>
inline T toScalar(
    const Cf *cf, ZuString key, ZuString value,
    T minimum, T maximum, bool required = false, T def = {})
{
  if (!value) {
    if (required) throw Required{cf, key};
    return def;
  }
  ZuBoxT<T> v{value};
  if (v < minimum || v > maximum)
    throw Range<T>{cf, key, minimum, maximum, v};
  return v;
}
template <typename ...Args>
inline auto toInt(Args &&... args) {
  return toScalar<int>(ZuFwd<Args>(args)...);
}
template <typename ...Args>
inline auto toInt64(Args &&... args) {
  return toScalar<int64_t>(ZuFwd<Args>(args)...);
}
template <typename ...Args>
inline auto toDbl(Args &&... args) {
  return toScalar<double>(ZuFwd<Args>(args)...);
}

template <typename Map>
inline ZtEnum toEnum(
    const Cf *cf, ZuString key, ZuString value,
    bool required = false, ZtEnum def = {})
{
  if (!value) {
    if (required) throw Required{cf, key};
    return def;
  }
  return ZvEnum<Map>::instance()->s2v(key, value, def);
}

template <typename Map, typename T = uint32_t>
inline T toFlags(
    const Cf *cf, ZuString key, ZuString value,
    bool required = false, T def = 0)
{
  if (!value) {
    if (required) throw Required{cf, key};
    return def;
  }
  return ZvFlags<Map>::instance()->template scan<T>(key, value);
}
template <typename Map, typename ...Args>
inline uint64_t getFlags64(Args &&... args) {
  return getFlags<Map, uint64_t>(ZuFwd<Args>(args)...);
}

class Cf;

struct CfNode {
  Cf * const		owner = nullptr;
  const ZtString	key;
  ZtArray<ZtString>	values;
  ZmRef<Cf>		cf;

friend Cf;

private:
  CfNode() = delete;
  CfNode(const CfNode &) = delete;
  CfNode &operator =(const CfNode &) = delete;
  CfNode(CfNode &&) = delete;
  CfNode &operator =(CfNode &&) = delete;

protected:
  template <typename Key>
  CfNode(Cf *owner_, Key &&key_) : owner{owner_}, key{ZuFwd<Key>(key_)} { }

public:
  static const ZtString &KeyAxor(const CfNode &node) { return node.key; }

  ZtString get(bool required = false, ZtString def = {}) const {
    if (!values) {
      if (required) throw Required{owner, key};
      return def;
    }
    return values[0];
  }

  template <typename T>
  T getScalar(
      T minimum, T maximum,
      bool required = false, T def = {}) const {
    if (!values) {
      if (required) throw Required{owner, key};
      return def;
    }
    return toScalar<T>(owner, key, values[0], minimum, maximum, required, def);
  }
  template <typename ...Args>
  auto getInt(Args &&... args) const {
    return getScalar<int>(ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  auto getInt64(Args &&... args) const {
    return getScalar<int64_t>(ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  auto getDbl(Args &&... args) const {
    return getScalar<double>(ZuFwd<Args>(args)...);
  }

  template <typename Map>
  ZtEnum getEnum(bool required = false, ZtEnum def = {}) const {
    if (!values) {
      if (required) throw Required{owner, key};
      return def;
    }
    return toEnum<Map>(owner, key, values[0], required, def);
  }

  template <typename Map, typename T = uint32_t>
  T getFlags(bool required = false, T def = 0) const {
    if (!values) {
      if (required) throw Required{owner, key};
      return def;
    }
    return toFlags<Map, T>(owner, key, values[0], required, def);
  }
  template <typename Map, typename ...Args>
  uint64_t getFlags64(Args &&... args) const {
    return getFlags<Map, uint64_t>(ZuFwd<Args>(args)...);
  }
};

class ZvAPI Cf : public ZuObject {
  Cf(const Cf &);
  Cf &operator =(const Cf &);	// prevent mis-use

public:
  Cf() = default;
private:
  Cf(CfNode *node) : m_node{node} { }
public:
  static void parseCLI(ZuString line, ZtArray<ZtString> &args);

  // fromCLI() and fromArgs() return the number of positional arguments
  int fromCLI(Cf *syntax, ZuString line);
  int fromArgs(Cf *options, const ZtArray<ZtString> &args);
  int fromArgs(const ZvOpt *opts, int argc, char **argv);
  int fromCLI(const ZvOpt *opts, ZuString line); // deprecated
  int fromArgs(const ZvOpt *opts, const ZtArray<ZtString> &args); // deprecated

  using Defines_ = ZmRBTreeKV<ZtString, ZtString, ZmRBTreeUnique<true>>;
  struct Defines : public ZuObject, public Defines_ { };

  void fromString(
      ZuString in, bool validate,
      ZmRef<Defines> defines = new Defines()) {
    fromString(in, validate, {}, defines);
  }

  template <typename FileName>
  void fromFile(
      const FileName &fileName, bool validate,
      ZmRef<Defines> defines = new Defines()) {
    ZtString in;
    {
      ZiFile file;
      ZeError e;
      if (file.open(fileName, ZiFile::ReadOnly, 0, &e) < 0)
	throw FileOpenError{fileName, e};
      int n = static_cast<int>(file.size());
      if (n >= ZvCfMaxFileSize) throw File2Big{fileName};
      in.length(n);
      if (file.read(in.data(), n, &e) < 0) throw e;
      file.close();
    }
    ZtString dir = ZiFile::dirname(fileName);
    if (!defines->find("TOPDIR")) defines->add("TOPDIR", dir);
    defines->del("CURDIR"); defines->add("CURDIR", ZuMv(dir));
    fromString(in, validate, fileName, defines);
  }

  void fromEnv(const char *name, bool validate);

  // caller must call freeArgs() after toArgs()
  void toArgs(int &argc, char **&argv) const;
  static void freeArgs(int argc, char **argv);

  void print(ZmStream &s, ZtString prefix) const;

  template <typename S> void print(S &s_) const {
    ZmStream s{s_};
    print(s, "");
  }
  void print(ZmStream &s) const { print(s, ""); }

  struct Prefixed {
    const Cf		&cf;
    mutable ZtString	prefix;

    template <typename S> void print(S &s_) const {
      ZmStream s{s_};
      print(s);
    }
    void print(ZmStream &s) const { cf.print(s, ZuMv(prefix)); }
    friend ZuPrintFn ZuPrintType(Prefixed *);
  };
  template <typename Prefix>
  Prefixed prefixed(Prefix &&prefix) {
    return Prefixed{*this, ZuFwd<Prefix>(prefix)};
  }

  // toFile() will throw ZeError on I/O error
  template <typename FileName>
  void toFile(const FileName &fileName) {
    ZiFile file;
    ZeError e;
    if (file.open(fileName, ZiFile::Create | ZiFile::Truncate, 0777, &e) < 0)
      throw e;
    toFile_(file);
  }

private:
  void toFile_(ZiFile &file);

private:
  static const char *HeapID() { return "ZvCf"; }
  using Tree =
    ZmRBTree<CfNode,
      ZmRBTreeNode<CfNode,
	ZmRBTreeKey<CfNode::KeyAxor,
	  ZmRBTreeUnique<true,
	    ZmRBTreeHeapID<HeapID>>>>>;
  using TreeNode = Tree::Node;
  using TreeNodeRef = Tree::NodeRef;

  TreeNodeRef getNode(ZuString fullKey, bool required) const;
  TreeNodeRef mkNode(ZuString fullKey);

public:
  ZtString get(ZuString key, bool required, ZtString def) const {
    if (auto node = getNode(key, required))
      return node->get(required, def);
    if (required) throw Required{this, key};
    return def;
  }
  template <typename Key>
  ZtString get(const Key &key, bool required = false) const {
    return get(key, required, {});
  }

  template <typename Key>
  const ZtArray<ZtString> *getMultiple(const Key &key,
      unsigned minimum, unsigned maximum, bool required = false) const {
    if (auto node = getNode(key, required)) {
      unsigned n = node->values.length();
      if (n < minimum || n > maximum)
	throw NValues{this, key, minimum, maximum, n};
      return &node->values;
    }
    if (required) throw Required{this, key};
    return nullptr;
  }

  void set(ZuString key, ZtString val);
  ZtArray<ZtString> *setMultiple(ZuString key);

  void unset(ZuString key);

  ZmRef<Cf> subset(ZuString key) const {
    return const_cast<Cf *>(this)->subset(key, false, false);
  }
  ZmRef<Cf> subset(ZuString key, bool required) const {
    return const_cast<Cf *>(this)->subset(key, required, false);
  }
  ZmRef<Cf> subset(ZuString key,  bool required, bool create);
  void subset(ZuString key, Cf *cf);

  void merge(Cf *cf);

  template <typename T = int, typename Key>
  T getScalar(
      const Key &key, T minimum, T maximum,
      bool required = false, T def = {}) const {
    if (auto node = getNode(key, required))
      return node->template getScalar<T>(minimum, maximum, required, def);
    if (required) throw Required{this, key};
    return def;
  }
  template <typename ...Args>
  auto getInt(Args &&... args) const {
    return getScalar<int>(ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  auto getInt64(Args &&... args) const {
    return getScalar<int64_t>(ZuFwd<Args>(args)...);
  }
  template <typename ...Args>
  auto getDbl(Args &&... args) const {
    return getScalar<double>(ZuFwd<Args>(args)...);
  }

  template <typename Map, typename Key>
  ZtEnum getEnum(const Key &key, bool required = false, ZtEnum def = {}) const {
    if (auto node = getNode(key, required))
      return node->template getEnum<Map>(required, def);
    if (required) throw Required{this, key};
    return def;
  }

  template <typename Map, typename T = uint32_t, typename Key>
  T getFlags(const Key &key, bool required = false, T def = {}) const {
    if (auto node = getNode(key, required))
      return node->template getFlags<Map, T>(required, def);
    if (required) throw Required{this, key};
    return def;
  }
  template <typename Map, typename ...Args>
  uint64_t getFlags64(Args &&... args) const {
    return getFlags<Map, uint64_t>(ZuFwd<Args>(args)...);
  }

  unsigned count() const { return m_tree.count_(); }
  CfNode *node() const { return m_node; }

  template <typename L>
  void all(L l) {
    auto i = m_tree.iterator();
    while (auto node = i.iterate()) l(node);
  }

  friend ZuPrintFn ZuPrintType(Cf *);

private:
  ZmRef<Cf> getScope(ZuString fullKey, ZuString &key) const;
  ZmRef<Cf> mkScope(ZuString fullKey, ZuString &key);

  void fromArg(ZuString fullKey, int type, ZuString argVal);
  void fromString(ZuString in, bool validate,
      ZuString fileName, ZmRef<Defines> defines);

  void toArgs(ZtArray<ZtString> &args, ZuString prefix = {}) const;

  static ZtString quoteArgValue(ZuString value);
  static ZtString quoteValue(ZuString value);

  Tree		m_tree;
  CfNode	*m_node;
};

inline ZtString fullKey(const Cf *cf, ZtString key) {
  while (auto node = cf->node()) {
    key = ZtString{} << node->CfNode::key << ':' << key;
    if (!(cf = node->owner)) break;
  }
  return key;
}

} // ZvCf_

#if 0
using ZvCfRequired = ZvCf_::Required;
template <typename T> using ZvCfRange = ZvCf_::Range<T>;
using ZvCfNValues = ZvCf_::NValues;
using ZvCfUsage = ZvCf_::Usage;
using ZvCfInvalid = ZvCf_::Invalid;
using ZvCfSyntax = ZvCf_::Syntax;
using ZvCfFileOpenError = ZvCf_::FileOpenError;
using ZvCfFile2Big = ZvCf_::File2Big;
using ZvCfEnvSyntax = ZvCf_::EnvSyntax;
using ZvCfBadDefine = ZvCf_::BadDefine;
#endif

using ZvCf = ZvCf_::Cf;
using ZvCfNode = ZvCf_::CfNode;

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZvCf_HPP */
