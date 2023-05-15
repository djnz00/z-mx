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
#include <zlib/ZtField.hpp>

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

template <typename T, bool = ZuTraits<T>::IsPrimitive> struct Scan_;
template <typename T_>
struct Scan_<T_, true> { using T = ZuBox<T_>; };
template <typename T_>
struct Scan_<ZuBox<T_>, false> { using T = ZuBox<T_>; };
template <> struct Scan_<ZuFixed, false> { using T = ZuFixed; };
template <> struct Scan_<ZuDecimal, false> { using T = ZuDecimal; };
template <typename T> using Scan = typename Scan_<T>::T;
template <typename T, bool Required_ = false>
inline T toScalar(
    const Cf *cf, ZuString key, ZuString value,
    T minimum, T maximum, T deflt = {})
{
  if (!value) {
    if constexpr (Required_) throw Required{cf, key};
    return deflt;
  }
  Scan<T> v{value};
  if (v < minimum || v > maximum)
    throw Range<T>{cf, key, minimum, maximum, v};
  return v;
}
template <bool Required_ = false, typename ...Args>
inline auto toInt(Args &&... args) {
  return toScalar<int, Required_>(ZuFwd<Args>(args)...);
}
template <bool Required_ = false, typename ...Args>
inline auto toInt64(Args &&... args) {
  return toScalar<int64_t, Required_>(ZuFwd<Args>(args)...);
}
template <bool Required_ = false, typename ...Args>
inline auto toDbl(Args &&... args) {
  return toScalar<double, Required_>(ZuFwd<Args>(args)...);
}

template <typename Map, bool Required_ = false>
inline ZtEnum toEnum(
    const Cf *cf, ZuString key, ZuString value, ZtEnum deflt = {})
{
  if (!value) {
    if constexpr (Required_) throw Required{cf, key};
    return deflt;
  }
  return ZvEnum<Map>::instance()->s2v(key, value, deflt);
}

template <typename Map, typename T, bool Required_ = false>
inline T toFlags_(const Cf *cf, ZuString key, ZuString value, T deflt = 0)
{
  if (!value) {
    if constexpr (Required_) throw Required{cf, key};
    return deflt;
  }
  return ZvFlags<Map>::instance()->template scan<T>(key, value);
}
template <typename Map, bool Required_ = false, typename ...Args>
inline uint32_t toFlags(Args &&... args) {
  return toFlags<Map, uint32_t, Required_>(ZuFwd<Args>(args)...);
}
template <typename Map, bool Required_ = false, typename ...Args>
inline uint64_t toFlags64(Args &&... args) {
  return toFlags<Map, uint64_t, Required_>(ZuFwd<Args>(args)...);
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

  template <bool Required_ = false>
  ZtString get(ZtString deflt = {}) const {
    if (!values) {
      if constexpr (Required_) throw Required{owner, key};
      return deflt;
    }
    return values[0];
  }

  template <typename T, bool Required_ = false>
  T getScalar(T minimum, T maximum, T deflt = {}) const {
    if (!values) {
      if constexpr (Required_) throw Required{owner, key};
      return deflt;
    }
    return toScalar<T, Required_>(
	owner, key, values[0], minimum, maximum, deflt);
  }
  template <bool Required_ = false, typename ...Args>
  auto getInt(Args &&... args) const {
    return getScalar<int, Required_>(ZuFwd<Args>(args)...);
  }
  template <bool Required_ = false, typename ...Args>
  auto getInt64(Args &&... args) const {
    return getScalar<int64_t, Required_>(ZuFwd<Args>(args)...);
  }
  template <bool Required_ = false, typename ...Args>
  auto getDbl(Args &&... args) const {
    return getScalar<double, Required_>(ZuFwd<Args>(args)...);
  }

  template <typename Map, bool Required_ = false>
  ZtEnum getEnum(ZtEnum deflt = -1) const {
    if (!values) {
      if constexpr (Required_) throw Required{owner, key};
      return deflt;
    }
    return toEnum<Map, Required_>(owner, key, values[0], deflt);
  }

  template <typename Map, typename T, bool Required_ = false>
  T getFlags_(T deflt = 0) const {
    if (!values) {
      if constexpr (Required_) throw Required{owner, key};
      return deflt;
    }
    return toFlags_<Map, T, Required_>(owner, key, values[0], deflt);
  }
  template <typename Map, bool Required_ = false, typename ...Args>
  uint32_t getFlags(Args &&... args) const {
    return getFlags_<Map, uint32_t, Required_>(ZuFwd<Args>(args)...);
  }
  template <typename Map, bool Required_ = false, typename ...Args>
  uint64_t getFlags64(Args &&... args) const {
    return getFlags_<Map, uint64_t, Required_>(ZuFwd<Args>(args)...);
  }
};

template <typename O, typename Cf_>
struct Fielded_ {
  using FieldList = ZuFieldList<O>;

  template <typename U>
  struct AllFilter { enum { OK = !U::ReadOnly }; };
  using AllFields = ZuTypeGrep<AllFilter, FieldList>;

  template <typename U>
  struct UpdateFilter { enum { OK = U::Flags & ZtFieldFlags::Update }; };
  using UpdateFields = ZuTypeGrep<UpdateFilter, AllFields>;

  template <typename U>
  struct CtorFilter { enum { OK = U::Flags & ZtFieldFlags::Ctor_ }; };
  using CtorFields_ = ZuTypeGrep<CtorFilter, AllFields>;
  template <typename U>
  struct CtorIndex {
    enum { I = (U::Flags>>ZtFieldFlags::CtorShift) & ZtFieldFlags::CtorMask };
  };
  using CtorFields = ZuTypeSort<CtorIndex, CtorFields_>;

  template <typename U>
  struct InitFilter { enum { OK = !(U::Flags & ZtFieldFlags::Ctor_) }; };
  using InitFields = ZuTypeGrep<InitFilter, AllFields>;

  template <typename ...Fields>
  struct Ctor {
    static O ctor(const Cf_ *cf) {
      return O{cf->template getField<Fields>()...};
    }
    static void ctor(void *ptr, const Cf_ *cf) {
      new (ptr) O{cf->template getField<Fields>()...};
    }
  };
  static O ctor(const Cf_ *cf) {
    O o = ZuTypeApply<Ctor, CtorFields>::ctor(cf);
    ZuTypeAll<InitFields>::invoke([&o, cf]<typename Field>() {
      Field::set(o, cf->template getField<Field>());
    });
    return o;
  }
  static void ctor(void *ptr, const Cf_ *cf) {
    ZuTypeApply<Ctor, CtorFields>::ctor(ptr, cf);
    O &o = *reinterpret_cast<O *>(ptr);
    ZuTypeAll<InitFields>::invoke([&o, cf]<typename Field>() {
      Field::set(o, cf->template getField<Field>());
    });
  }

  template <typename ...Fields>
  struct Load__ : public O {
    Load__() = default;
    Load__(const Cf_ *cf) : O{cf->template getField<Fields>()...} { }
    template <typename ...Args>
    Load__(Args &&... args) : O{ZuFwd<Args>(args)...} { }
  };
  using Load_ = ZuTypeApply<Load__, CtorFields>;
  struct Load : public Load_ {
    Load() = default;
    Load(const Cf_ *cf) : Load_{cf} {
      ZuTypeAll<InitFields>::invoke([this, cf]<typename Field>() {
	Field::set(*this, cf->template getField<Field>());
      });
    }
    template <typename ...Args>
    Load(Args &&... args) : Load_{ZuFwd<Args>(args)...} { }
  };

  static void load(O &o, const Cf_ *cf) {
    ZuTypeAll<AllFields>::invoke([&o, cf]<typename Field>() {
      Field::set(o, cf->template getField<Field>());
    });
  }
  static void loadUpdate(O &o, const Cf_ *cf) {
    ZuTypeAll<UpdateFields>::invoke([&o, cf]<typename Field>() {
      Field::set(o, cf->template getField<Field>());
    });
  }

  template <typename ...Fields>
  struct Key {
    using Tuple = ZuTuple<typename Fields::T...>;
    static decltype(auto) tuple(const Cf_ *cf) {
      return Tuple{cf->template getField<Fields>()...};
    }
  };
  template <typename ...Fields>
  struct Key<ZuTypeList<Fields...>> : public Key<Fields...> { };

  template <unsigned KeyID>
  struct KeyFilter {
    template <typename U>
    struct T {
      enum { OK = U::keys() & (1<<KeyID) };
    };
  };
  template <unsigned KeyID = 0>
  static auto key(const Cf_ *cf) {
    using Fields = ZuTypeGrep<KeyFilter<KeyID>::template T, FieldList>;
    return Key<Fields>::tuple(cf);
  }
};
template <typename O, typename Cf_ = Cf>
using Fielded = Fielded_<ZuFielded<O>, Cf_>;

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

  template <bool Required_ = false>
  TreeNodeRef getNode(ZuString fullKey) const {
    ZuString key;
    auto self = getScope(fullKey, key);
    if (!self) {
      if constexpr (Required_) throw Required{this, fullKey};
      return nullptr;
    }
    TreeNodeRef node = self->m_tree.find(key);
    if (!node)
      if constexpr (Required_) throw Required{this, fullKey};
    return node;
  }
  TreeNodeRef mkNode(ZuString fullKey);

public:
  template <bool Required_ = false, typename Key>
  ZtString get(const Key &key, ZtString deflt) const {
    if (auto node = getNode<Required_>(key))
      return node->template get<Required_>(deflt);
    if constexpr (Required_) throw Required{this, key};
    return deflt;
  }
  template <bool Required_ = false, typename Key>
  ZtString get(const Key &key) const { return get<Required_>(key, {}); }

  template <bool Required_ = false, typename Key>
  const ZtArray<ZtString> *getMultiple(
      const Key &key, unsigned minimum, unsigned maximum) const {
    if (auto node = getNode<Required_>(key)) {
      unsigned n = node->values.length();
      if (n < minimum || n > maximum)
	throw NValues{this, key, minimum, maximum, n};
      return &node->values;
    }
    if constexpr (Required_) throw Required{this, key};
    return nullptr;
  }

  void set(ZuString key, ZtString val);
  ZtArray<ZtString> *setMultiple(ZuString key);

  void unset(ZuString key);

private:
  template <bool Required_ = false, bool Create = false>
  ZmRef<Cf> subset_(ZuString key) {
    TreeNodeRef node;
    if constexpr (!Create) {
      node = getNode<Required_>(key);
      if (!node->cf) {
	if constexpr (Required_) throw Required{this, key};
	return nullptr;
      }
    } else {
      node = mkNode(key);
      if (!node->cf) node->cf = new Cf{node};
    }
    return node->cf;
  }
public:
  template <bool Required_ = false, bool Create = false>
  ZuIfT<Create, ZmRef<Cf>> subset(ZuString key) {
    return subset_<Required_, Create>(key);
  }
  template <bool Required_ = false, bool Create = false>
  ZuIfT<!Create, ZmRef<Cf>> subset(ZuString key) const {
    return const_cast<Cf *>(this)->subset_<Required_, Create>(key);
  }
  void subset(ZuString key, Cf *cf);

  void merge(Cf *cf);

  template <typename T, bool Required_ = false, typename Key>
  T getScalar(const Key &key, T minimum, T maximum, T deflt = {}) const {
    if (auto node = getNode<Required_>(key))
      return node->template getScalar<T, Required_>(minimum, maximum, deflt);
    if constexpr (Required_) throw Required{this, key};
    return deflt;
  }
  template <bool Required_ = false, typename ...Args>
  auto getInt(Args &&... args) const {
    return getScalar<int, Required_>(ZuFwd<Args>(args)...);
  }
  template <bool Required_ = false, typename ...Args>
  auto getInt64(Args &&... args) const {
    return getScalar<int64_t, Required_>(ZuFwd<Args>(args)...);
  }
  template <bool Required_ = false, typename ...Args>
  auto getDbl(Args &&... args) const {
    return getScalar<double, Required_>(ZuFwd<Args>(args)...);
  }

  template <typename Map, bool Required_ = false, typename Key>
  ZtEnum getEnum(const Key &key, ZtEnum deflt = -1) const {
    if (auto node = getNode<Required_>(key))
      return node->template getEnum<Map, Required_>(deflt);
    if constexpr (Required_) throw Required{this, key};
    return deflt;
  }

  template <typename Map, typename T, bool Required_ = false, typename Key>
  T getFlags_(const Key &key, T deflt = {}) const {
    if (auto node = getNode<Required_>(key))
      return node->template getFlags_<Map, T, Required_>(deflt);
    if constexpr (Required_) throw Required{this, key};
    return deflt;
  }
  template <typename Map, bool Required_ = false, typename ...Args>
  uint32_t getFlags(Args &&... args) const {
    return getFlags_<Map, uint32_t, Required_>(ZuFwd<Args>(args)...);
  }
  template <typename Map, bool Required_ = false, typename ...Args>
  uint64_t getFlags64(Args &&... args) const {
    return getFlags_<Map, uint64_t, Required_>(ZuFwd<Args>(args)...);
  }

  template <typename Field>
  ZuIfT<Field::Type == ZtFieldType::String, typename Field::T>
  getField() {
    return get<Field::Flags & ZtFieldFlags::Required>(
	Field::id(), Field::deflt());
  }
  template <typename Field>
  ZuIfT<Field::Type == ZtFieldType::Composite ||
	Field::Type == ZtFieldType::Time, typename Field::T>
  getField() {
    using T = typename Field::T;
    auto s = get<Field::Flags & ZtFieldFlags::Required>(Field::id(), "");
    if (ZuUnlikely(!s)) return Field::deflt();
    return T{s};
  }
  template <typename Field>
  ZuIfT<Field::Type == ZtFieldType::Bool, typename Field::T>
  getField() {
    return getScalar<bool, Field::Flags & ZtFieldFlags::Required>(
	Field::id(), 0, 1, Field::deflt());
  }
  template <typename Field>
  ZuIfT<Field::Type == ZtFieldType::Int ||
	Field::Type == ZtFieldType::Float ||
	Field::Type == ZtFieldType::Fixed ||
	Field::Type == ZtFieldType::Decimal, typename Field::T>
  getField() {
    return getScalar<typename Field::T, Field::Flags & ZtFieldFlags::Required>(
	Field::id(), Field::minimum(), Field::maximum(), Field::deflt());
  }
  template <typename Field>
  ZuIfT<Field::Type == ZtFieldType::Hex, typename Field::T>
  getField() {
    using T = typename Field::T;
    return getScalar<T, Field::Flags & ZtFieldFlags::Required>(
	Field::id(), ZuCmp<T>::minimum(), ZuCmp<T>::maximum(), Field::deflt());
  }
  template <typename Field>
  ZuIfT<Field::Type == ZtFieldType::Enum, typename Field::T>
  getField() {
    using Map = typename Field::Map;
    return getEnum<Map, Field::Flags & ZtFieldFlags::Required>(
	Field::id(), Field::deflt());
  }
  template <typename Field>
  ZuIfT<Field::Type == ZtFieldType::Flags, typename Field::T>
  getField() {
    using Map = typename Field::Map;
    using T = typename Field::T;
    return getFlags_<Map, T, Field::Flags & ZtFieldFlags::Required>(
	Field::id(), Field::deflt());
  }

  template <typename O>
  inline O getCtor() const { return Fielded<O>::ctor(this); }
  template <typename O>
  inline void getCtor(void *ptr) const { Fielded<O>::ctor(ptr, this); }

  template <typename O> using GetLoad = typename Fielded<O>::Load;

  template <typename O>
  inline void getLoad(O &o) const { Fielded<O>::load(o, this); }
  template <typename O>
  inline void getUpdate(O &o) const { Fielded<O>::loadUpdate(o, this); }

  template <typename O, unsigned KeyID = 0>
  inline auto getKey() const {
    return Fielded<O>::template key<KeyID>(this);
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
