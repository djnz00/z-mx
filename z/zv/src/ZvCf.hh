//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// application configuration

#ifndef ZvCf_HH
#define ZvCf_HH

#ifndef ZvLib_HH
#include <zlib/ZvLib.hh>
#endif

#ifndef _MSC_VER
#include <unistd.h>
#endif

#include <zlib/ZuBox.hh>
#include <zlib/ZuICmp.hh>
#include <zlib/ZuBase64.hh>

#include <zlib/ZmObject.hh>
#include <zlib/ZmRef.hh>
#include <zlib/ZmList.hh>
#include <zlib/ZmRBTree.hh>
#include <zlib/ZmBackTrace.hh>

#include <zlib/ZtArray.hh>
#include <zlib/ZtString.hh>
#include <zlib/ZtScanBool.hh>
#include <zlib/ZtField.hh>

#include <zlib/ZiFile.hh>

#include <zlib/ZvError.hh>
#include <zlib/ZvEnum.hh>

#define ZvCfMaxFileSize	(1<<20)	// 1Mb

namespace ZvOptType {
  ZtEnumValues_(int8_t, Flag, Param, Array);
  ZtEnumMap(ZvOptTypes, Map, "flag", Flag, "param", Param, "array", Array);
}

struct ZvOpt {
  char		short_;
  const char	*long_;
  int		type;		// ZvOptType
  const char	*key;
};

namespace ZvCf_ {

class Cf;

ZtString fullKey(const Cf *cf, ZtString key);

} // ZvCf_

namespace ZvCfError {

using Cf = ZvCf_::Cf;

// thrown by all get methods for missing required values
class Required : public ZvError {
public:
  Required(const Cf *cf, ZuCSpan key) :
      m_key{fullKey(cf, key)}, m_bt{1} { }

  const ZtString &key() const { return m_key; }

  void print_(ZuVStream &s) const {
    s << '"' << m_key << "\" missing at:\n" << m_bt;
  }

private:
  ZtString	m_key;
  ZmBackTrace	m_bt;
};

// thrown by getBool() on error
class BadBool : public ZvError {
public:
  BadBool(ZtString key, ZtString value) :
      m_key{ZuMv(key)}, m_value{ZuMv(value)} { }
  void print_(ZuVStream &s) const {
    s << '"' << m_key << "\": invalid bool value \"" << m_value << '"';
  }

private:
  ZtString	m_key;
  ZtString	m_value;
};

// template base class for NElems / Range exceptions
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
  void print__(ZuVStream &s, ZuCSpan msg) const {
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
// thrown by getScalar() on range error
template <typename T>
class Range : public Range_<T> {
  using Base = Range_<T>;
public:
  Range(const Cf *cf, ZuCSpan key, T minimum, T maximum, T value) :
      Base{fullKey(cf, key), minimum, maximum, value} { }
  void print_(ZuVStream &s) const {
    Base::print__(s, "out of range");
  }
};
// thrown by all() on number of elements error
class NElems : public Range_<unsigned> {
  using T = unsigned;
  using Base = Range_<T>;
public:
  NElems(const Cf *cf, ZuCSpan key, T minimum, T maximum, T value) :
      Base{fullKey(cf, key), minimum, maximum, value} { }
  void print_(ZuVStream &s) const {
    Base::print__(s, "invalid number of values");
  }
};

// thrown by fromArgs() on error
class Usage : public ZvError {
public:
  Usage(ZuCSpan cmd, ZuCSpan option) :
    m_cmd(cmd), m_option(option) { }
  void print_(ZuVStream &s) const {
    s << '"' << m_cmd << "\": invalid option \"" << m_option << '"';
  }

private:
  ZtString	m_cmd;
  ZtString	m_option;
};

// thrown by fromString() and fromFile() on error
class Syntax : public ZvError {
public:
  Syntax(unsigned line, char ch, ZuCSpan fileName) :
    m_line(line), m_ch(ch), m_fileName(fileName) { }
  void print_(ZuVStream &s) const {
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
  FileOpenError(ZuCSpan fileName, ZeError e) :
    m_fileName{fileName}, m_err{e} { }

  void print_(ZuVStream &s) const {
    s << '"' << m_fileName << "\" " << m_err;
  }

private:
  ZtString	m_fileName;
  ZeError   m_err;
};

// thrown by fromFile()
class File2Big : public ZvError {
public:
  File2Big(ZuCSpan fileName) : m_fileName{fileName} { }
  void print_(ZuVStream &s) const {
    s << '"' << m_fileName << " file too big";
  };
private:
  ZtString	m_fileName;
};

// thrown by fromEnv() on error
class EnvSyntax : public ZvError {
public:
  EnvSyntax(unsigned pos, char ch) : m_pos{pos}, m_ch{ch} { }
  void print_(ZuVStream &s) const {
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
  void print_(ZuVStream &s) const {
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
template <typename T_, typename NTP_>
struct Scan_<ZuBox<T_, NTP_>, false> { using T = ZuBox<T_, NTP_>; };
template <> struct Scan_<ZuFixed, false> { using T = ZuFixed; };
template <> struct Scan_<ZuDecimal, false> { using T = ZuDecimal; };
template <typename T> using Scan = typename Scan_<T>::T;

// scan bool
template <bool Required_ = false>
inline bool scanBool(
    const Cf *cf, ZuCSpan key, ZuCSpan value, bool deflt = false)
{
  if (!value) {
    if constexpr (Required_) throw Required{cf, key};
    return deflt;
  }
  try {
    return ZtScanBool<true>(value);
  } catch (...) {
    throw BadBool{key, value};
  }
}

// scan generic scalar
template <typename T, bool Required_ = false>
inline T scanScalar(
    const Cf *cf, ZuCSpan key, ZuCSpan value,
    T minimum, T maximum, T deflt = ZuCmp<T>::null())
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
// scanScalar() shorthand forwarding functions
template <bool Required_ = false, typename ...Args>
inline auto scanInt(Args &&...args) {
  return scanScalar<int, Required_>(ZuFwd<Args>(args)...);
}
template <bool Required_ = false, typename ...Args>
inline auto scanInt64(Args &&...args) {
  return scanScalar<int64_t, Required_>(ZuFwd<Args>(args)...);
}
template <bool Required_ = false, typename ...Args>
inline auto scanDbl(Args &&...args) {
  return scanScalar<double, Required_>(ZuFwd<Args>(args)...);
}

// scan enum
template <typename Map, bool Required_ = false>
inline int scanEnum(
    const Cf *cf, ZuCSpan key, ZuCSpan value, int deflt = -1)
{
  if (!value) {
    if constexpr (Required_) throw Required{cf, key};
    return deflt;
  }
  if constexpr (Required_)
    return ZvEnum::s2v<Map, true>(key, value);
  else
    return ZvEnum::s2v<Map, false>(key, value, deflt);
}

// scan flags
template <typename Map, typename Flags, bool Required_ = false>
inline Flags scanFlags(
    const Cf *cf, ZuCSpan key, ZuCSpan value, Flags deflt = 0)
{
  if (!value) {
    if constexpr (Required_) throw Required{cf, key};
    return deflt;
  }
  return ZvEnum::scan<Map, Flags>(key, value);
}

namespace Quoting { // quoting types
  enum {
    Mask	= 0x003,
      File	= 0x000,
      Env	= 0x001,
      CLI	= 0x002,
      Raw	= 0x003,

    Key		= 0x004		// flag
  };
}

// data in a tree node
using Null = ZuNull;
using StrArray = ZtArray<ZtString>;
using CfArray = ZtArray<ZmRef<Cf>>;
using Data = ZuUnion<Null, ZtString, StrArray, ZmRef<Cf>, CfArray>;

// main configuration class
class Cf;

// configuration tree node
struct CfNode {
  Cf * const		owner = nullptr;
  const ZtString	key;
  Data			data;

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

  void null() { data.p<Null>(); }

  auto type() const { return data.type(); }

  // generic set()
  template <typename T, typename P>
  void set_(P &&v) { data.p<T>(ZuFwd<P>(v)); }
  // set() shorthand forwarding functions for ZtString and ZmRef<Cf>
  template <typename P> void set(P &&v) { set_<ZtString>(ZuFwd<P>(v)); }
  template <typename P> void setCf(P &&v) { set_<ZmRef<Cf>>(ZuFwd<P>(v)); }

  // generic get()
  template <typename T, bool Required_ = false>
  const T &get_() const { // optionally required, no specified default value
    if (!data.is<T>()) {
      if constexpr (Required_) throw Required{owner, key};
      return ZuNullRef<T>();
    }
    return data.p<T>();
  }
  template <typename T>
  T get_(T deflt) const { // not required, specified default value
    if (!data.is<T>()) return deflt;
    return data.p<T>();
  }
  // generic assure() - sets to a specified default value if unset
  template <typename T, typename L>
  const T &assure_(L l) { // not required, set default if unset
    if (!data.is<T>()) data.p<T>(l());
    return data.p<T>();
  }

  // get/assure ZtString
  template <bool Required_ = false>
  const ZtString &get() const { return get_<ZtString, Required_>(); }
  ZtString get(ZtString deflt) const { return get_<ZtString>(ZuMv(deflt)); }
  template <typename L>
  const ZtString &assure(L l) { return assure_<ZtString>(ZuMv(l)); }
  // get/assure ZtArray<ZtString>
  template <bool Required_ = false>
  const StrArray &getStrArray() const { return get_<StrArray, Required_>(); }
  template <typename L>
  const StrArray &assureStrArray(L l) { return assure_<StrArray>(ZuMv(l)); }
  // get/assure ZmRef<Cf>
  template <bool Required_ = false>
  const ZmRef<Cf> &getCf() const { return get_<ZmRef<Cf>, Required_>(); }
  template <typename L>
  const ZmRef<Cf> &assureCf(L l) { return assure_<ZmRef<Cf>>(ZuMv(l)); }
  // get/assure ZtArray<ZmRef<Cf>>
  template <bool Required_ = false>
  const CfArray &getCfArray() const { return get_<CfArray, Required_>(); }
  template <typename L>
  const CfArray &assureCfArray(L l) { return assure_<CfArray>(ZuMv(l)); }

  // generic set/get/assure array element
  template <typename T, typename P>
  void setElem(unsigned i, P &&v) {
    using Elem = typename T::T;
    if (!data.is<T>()) new (data.new_<T>()) T{};
    new (data.p<T>().set(i)) Elem{ZuFwd<P>(v)};
  }
  template <typename T, bool Required_ = false>
  const typename T::T &getElem(unsigned i) const {
    if (!data.is<T>()) {
      if constexpr (Required_) throw Required{owner, key};
      return ZuNullRef<typename T::T>();
    }
    const auto &elems = data.p<T>();
    if (i >= elems.length()) return ZuNullRef<typename T::T>();
    return elems.get(i);
  }
  template <typename T>
  typename T::T getElem(unsigned i, typename T::T deflt) const {
    if (!data.is<T>()) return deflt;
    const auto &elems = data.p<T>();
    if (i >= elems.length()) return deflt;
    return elems.get(i);
  }
  template <typename T, typename L>
  const typename T::T &assureElem(unsigned i, L l) {
    if (!data.is<T>()) new (data.new_<T>()) T{};
    if (i >= data.p<T>().length()) data.p<T>().set(i, l());
    return data.p<T>().get(i);
  }

  // get/assure bool
  template <bool Required_ = false>
  bool getBool() const {
    return scanBool<Required_>(owner, key, get<Required_>());
  }
  bool getBool(bool deflt) const {
    return scanBool(owner, key, get(), deflt);
  }
  bool assureBool(bool deflt) {
    return scanBool(
	owner, key, assure([deflt]() { return deflt ? "1" : "0"; }), deflt);
  }

  // generic get/assure scalar
  template <typename T, bool Required_ = false>
  T getScalar(T minimum, T maximum) const {
    return scanScalar<T, Required_>(
	owner, key, get<Required_>(),
	minimum, maximum);
  }
  template <typename T>
  T getScalar(T minimum, T maximum, T deflt) const {
    return scanScalar<T>(owner, key, get(), minimum, maximum, deflt);
  }
  template <typename T>
  T assureScalar(T minimum, T maximum, T deflt) {
    return scanScalar<T>(
	owner, key,
	assure([deflt = ZuMv(deflt)]() { return ZtString{} << deflt; }),
	minimum, maximum, deflt);
  }

  // getScalar/assureScalar shorthand forwarding functions for int
  template <bool Required_ = false>
  int getInt(int minimum, int maximum) const {
    return getScalar<int, Required_>(minimum, maximum);
  }
  int getInt(int minimum, int maximum, int deflt) const {
    return getScalar<int>(minimum, maximum, deflt);
  }
  int assureInt(int minimum, int maximum, int deflt) {
    return assureScalar<int>(minimum, maximum, deflt);
  }

  // getScalar/assureScalar shorthand forwarding functions for int64_t
  template <bool Required_ = false>
  int64_t getInt64(int64_t minimum, int64_t maximum) const {
    return getScalar<int64_t, Required_>(minimum, maximum);
  }
  int64_t getInt64(int64_t minimum, int64_t maximum, int64_t deflt) const {
    return getScalar<int64_t>(minimum, maximum, deflt);
  }
  int64_t assureInt64(int64_t minimum, int64_t maximum, int64_t deflt) {
    return assureScalar<int64_t>(minimum, maximum, deflt);
  }

  // getScalar/assureScalar shorthand forwarding functions for double
  template <bool Required_ = false>
  double getDbl(double minimum, double maximum) const {
    return getScalar<double, Required_>(minimum, maximum);
  }
  double getDbl(double minimum, double maximum, double deflt) const {
    return getScalar<double>(minimum, maximum, deflt);
  }
  double assureDbl(double minimum, double maximum, double deflt) {
    return assureScalar<double>(minimum, maximum, deflt);
  }

  // get/assure enum
  template <typename Map, bool Required_ = false>
  int getEnum() const {
    return scanEnum<Map, Required_>(owner, key, get<Required_>(), -1);
  }
  template <typename Map>
  int getEnum(int deflt) const {
    return scanEnum<Map>(owner, key, get(), deflt);
  }
  template <typename Map>
  int assureEnum(int deflt) {
    return scanEnum<Map>(
	owner, key, assure([deflt]() { return Map::v2s(deflt); }), deflt);
  }

  // get/assure flags
  template <typename Map, typename T, bool Required_ = false>
  T getFlags() const {
    return scanFlags<Map, T, Required_>(
	owner, key, get<Required_>(), 0);
  }
  template <typename Map, typename T>
  T getFlags(T deflt) const {
    return scanFlags<Map, T>(owner, key, get(), deflt);
  }
  template <typename Map, typename T>
  T assureFlags(T deflt) {
    using Print = typename Map::Print;
    return scanFlags<Map, T>(
	owner, key,
	assure([deflt]() { return ZtString{} << Print{deflt}; }), deflt);
  }
};

// ZtField integration
template <typename O, typename Cf_>
struct Handler_ {
  using Fields = ZuFields<O>;

  template <typename U>
  struct AllFilter : public ZuBool<!U::ReadOnly> { };
  using AllFields = ZuTypeGrep<AllFilter, Fields>;

  template <typename U> struct UpdateFilter :
      public ZuTypeIn<ZuFieldProp::Mutable, typename U::Props> { };
  using UpdateFields = ZuTypeGrep<UpdateFilter, AllFields>;

  template <typename U>
  using CtorFilter = ZuFieldProp::HasCtor<typename U::Props>;
  template <typename U>
  using CtorIndex = ZuFieldProp::GetCtor<typename U::Props>;
  using CtorFields = ZuTypeSort<CtorIndex, ZuTypeGrep<CtorFilter, AllFields>>;

  template <typename U>
  using InitFilter = ZuBool<!ZuFieldProp::HasCtor<typename U::Props>{}>;
  using InitFields = ZuTypeGrep<InitFilter, AllFields>;

  template <typename ...Fields_>
  struct Ctor {
    static O ctor(const Cf_ *cf) {
      return O{cf->template getField<Fields_>()...};
    }
    static void ctor(void *ptr, const Cf_ *cf) {
      new (ptr) O{cf->template getField<Fields_>()...};
    }
  };
  static O ctor(const Cf_ *cf) {
    O o = ZuTypeApply<Ctor, CtorFields>::ctor(cf);
    ZuUnroll::all<InitFields>([&o, cf]<typename Field>() {
      Field::set(o, cf->template getField<Field>());
    });
    return o;
  }
  static void ctor(void *ptr, const Cf_ *cf) {
    ZuTypeApply<Ctor, CtorFields>::ctor(ptr, cf);
    O &o = *reinterpret_cast<O *>(ptr);
    ZuUnroll::all<InitFields>([&o, cf]<typename Field>() {
      Field::set(o, cf->template getField<Field>());
    });
  }

  template <typename ...Fields_>
  struct Load__ : public O {
    Load__() = default;
    Load__(const Cf_ *cf) : O{cf->template getField<Fields_>()...} { }
    template <typename ...Args>
    Load__(Args &&...args) : O{ZuFwd<Args>(args)...} { }
  };
  using Load_ = ZuTypeApply<Load__, CtorFields>;
  struct Load : public Load_ {
    Load() = default;
    Load(const Cf_ *cf) : Load_{cf} {
      ZuUnroll::all<InitFields>([this, cf]<typename Field>() {
	Field::set(*this, cf->template getField<Field>());
      });
    }
    template <typename ...Args>
    Load(Args &&...args) : Load_{ZuFwd<Args>(args)...} { }
  };

  static void load(O &o, const Cf_ *cf) {
    ZuUnroll::all<AllFields>([&o, cf]<typename Field>() {
      Field::set(o, cf->template getField<Field>());
    });
  }
  static void update(O &o, const Cf_ *cf) {
    ZuUnroll::all<UpdateFields>([&o, cf]<typename Field>() {
      Field::set(o, cf->template getField<Field>());
    });
  }
};
template <typename O, typename Cf_ = Cf>
using Handler = Handler_<ZuFielded<O>, Cf_>;

// main configuration class - contains tree of CfNodes (key + data pairs)
class ZvAPI Cf : public ZuObject {
  Cf(const Cf &);
  Cf &operator =(const Cf &);	// prevent mis-use

public:
  Cf() = default;
private:
  Cf(CfNode *node) : m_node{node} { }
public:
  static ZtArray<ZtString> parseCLI(ZuCSpan line);
  static ZtArray<ZtString> args(int argc, char **argv);
  static ZmRef<Cf> options(const ZvOpt *opts);

  // fromCLI() and fromArgs() return the number of positional arguments
  unsigned fromCLI(Cf *syntax, ZuCSpan line);
  unsigned fromArgs(Cf *options, const ZtArray<ZtString> &args);

  using Defines_ = ZmRBTreeKV<ZtString, ZtString, ZmRBTreeUnique<true>>;
  struct Defines : public ZuObject, public Defines_ { };

  void fromString(ZuCSpan in, ZmRef<Defines> defines = new Defines{}) {
    fromString(in, {}, defines);
  }

  template <typename Path>
  void fromFile(const Path &path, ZmRef<Defines> defines = new Defines{}) {
    ZtString in;
    {
      ZiFile file;
      ZeError e;
      if (file.open(path, ZiFile::ReadOnly, 0, &e) < 0)
	throw FileOpenError{path, e};
      int n = static_cast<int>(file.size());
      if (n >= ZvCfMaxFileSize) throw File2Big{path};
      in.length(n);
      if (file.read(in.data(), n, &e) < 0) throw e;
      file.close();
    }
    ZtString dir = ZiFile::dirname(path);
    if (!defines->find("TOPDIR")) defines->add("TOPDIR", dir);
    defines->del("CURDIR"); defines->add("CURDIR", ZuMv(dir));
    fromString(in, path, defines);
  }

  void fromEnv(const char *name, ZmRef<Defines> defines = new Defines{});

  // caller must call freeArgs() after toArgs()
  void toArgs(int &argc, char **&argv) const;
  static void freeArgs(int argc, char **argv);

  void print(ZuVStream &s, ZtString &indent) const;

  template <typename S> void print(S &s_) const {
    ZuVStream s{s_};
    ZtString indent;
    print(s, indent);
  }
  void print(ZuVStream &s) const {
    ZtString indent;
    print(s, indent);
  }
  friend ZuPrintFn ZuPrintType(Cf *);

  // toFile() will throw ZeError on I/O error
  template <typename Path>
  void toFile(const Path &path) {
    ZiFile file;
    ZeError e;
    if (file.open(path, ZiFile::Create | ZiFile::Truncate, 0777, &e) < 0)
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
  using Node = Tree::Node;

  ZuTuple<Cf *, ZtString> getScope(ZuCSpan fullKey) const;

public:
  template <bool Required_ = false>
  const CfNode *getNode(ZuCSpan fullKey) const {
    auto [this_, key] = getScope(fullKey);
    if (!this_) {
      if constexpr (Required_) throw Required{this, fullKey};
      return nullptr;
    }
    Node *node = this_->m_tree.find(key);
    if (!node)
      if constexpr (Required_) throw Required{this, fullKey};
    return node;
  }
private:
  CfNode *mkNode(ZuCSpan fullKey);

public:
  // check if key exists
  bool exists(ZuCSpan fullKey) const { return getNode(fullKey); }

  // set/get/assure ZtString
  void set(ZuCSpan key, ZtString value);
  template <bool Required_ = false>
  const ZtString &get(ZuCSpan key) const {
    if (auto node = getNode<Required_>(key))
      return node->template get<Required_>();
    if constexpr (Required_) throw Required{this, key};
    return ZuNullRef<ZtString>();
  }
  ZtString get(ZuCSpan key, ZtString deflt) const {
    if (auto node = getNode(key)) return node->get(deflt);
    return deflt;
  }
  template <typename L>
  const ZtString &assure(ZuCSpan key, L l) {
    return mkNode(key)->assure(ZuMv(l));
  }

  // set/get/assure StrArray
  void setStrArray(ZuCSpan key, StrArray value);
  template <bool Required_ = false>
  const StrArray &getStrArray(ZuCSpan key) const {
    if (auto node = getNode<Required_>(key))
      return node->template getStrArray<Required_>();
    if constexpr (Required_) throw Required{this, key};
    return ZuNullRef<StrArray>();
  }
  template <typename L>
  const StrArray &assureStrArray(ZuCSpan key, L l) {
    return mkNode(key)->assureStrArray(ZuMv(l));
  }

  // set/get/assure ZmRef<Cf>
  ZmRef<Cf> mkCf(ZuCSpan key);
  void setCf(ZuCSpan key, ZmRef<Cf> cf);
  template <bool Required_ = false>
  const ZmRef<Cf> &getCf(ZuCSpan key) const {
    if (auto node = getNode<Required_>(key))
      return node->template getCf<Required_>();
    if constexpr (Required_) throw Required{this, key};
    return ZuNullRef<ZmRef<Cf>>();
  }
  template <typename L>
  const ZmRef<Cf> &assureCf(ZuCSpan key, L l) {
    return mkNode(key)->assureCf(ZuMv(l));
  }

  // set/get/assure CfArray
  void setCfArray(ZuCSpan key, CfArray value);
  template <bool Required_ = false>
  const CfArray &getCfArray(ZuCSpan key) const {
    if (auto node = getNode<Required_>(key))
      return node->template getCfArray<Required_>();
    if constexpr (Required_) throw Required{this, key};
    return ZuNullRef<CfArray>();
  }
  template <typename L>
  const CfArray &assureCfArray(ZuCSpan key, L l) {
    return mkNode(key)->assureCfArray(ZuMv(l));
  }

  // unset node
  void unset(ZuCSpan key);

  // iterate over nodes
  template <typename L>
  void all(L l) {
    auto i = m_tree.iterator();
    while (auto node = i.iterate()) l(node);
  }

  // clean tree
  void clean();

  // merge tree
  void merge(const Cf *cf);

  // get/assure bool
  template <bool Required_ = false>
  bool getBool(ZuCSpan key) const {
    if (auto node = getNode<Required_>(key))
      return node->template getBool<Required_>();
    if constexpr (Required_) throw Required{this, key};
    return false;
  }
  bool getBool(ZuCSpan key, bool deflt) const {
    if (auto node = getNode(key))
      return node->getBool(deflt);
    return deflt;
  }
  bool assureBool(ZuCSpan key, bool deflt) {
    return mkNode(key)->assureBool(deflt);
  }

  // generic get/assure scalar
  template <typename T, bool Required_ = false>
  T getScalar(ZuCSpan key, T minimum, T maximum) const {
    if (auto node = getNode<Required_>(key))
      return node->template getScalar<T, Required_>(minimum, maximum);
    if constexpr (Required_) throw Required{this, key};
    return 0;
  }
  template <typename T>
  T getScalar(ZuCSpan key, T minimum, T maximum, T deflt) const {
    if (auto node = getNode(key))
      return node->template getScalar<T>(minimum, maximum, deflt);
    return deflt;
  }
  template <typename T>
  T assureScalar(ZuCSpan key, T minimum, T maximum, T deflt) {
    return mkNode(key)->template assureScalar<T>(minimum, maximum, deflt);
  }

  // get/assure shorthand forwarding functions for int
  template <bool Required_ = false>
  int getInt(ZuCSpan key, int minimum, int maximum) const {
    return getScalar<int, Required_>(key, minimum, maximum);
  }
  int getInt(ZuCSpan key, int minimum, int maximum, int deflt) const {
    return getScalar<int>(key, minimum, maximum, deflt);
  }
  int assureInt(ZuCSpan key, int minimum, int maximum, int deflt) {
    return assureScalar<int>(key, minimum, maximum, deflt);
  }

  // get/assure shorthand forwarding functions for int64_t
  template <bool Required_ = false>
  int64_t getInt64(ZuCSpan key, int64_t minimum, int64_t maximum) const {
    return getScalar<int64_t, Required_>(key, minimum, maximum);
  }
  int64_t getInt64(
      ZuCSpan key, int64_t minimum, int64_t maximum, int64_t deflt) const {
    return getScalar<int64_t>(key, minimum, maximum, deflt);
  }
  int64_t assureInt64(
      ZuCSpan key, int64_t minimum, int64_t maximum, int64_t deflt) {
    return assureScalar<int64_t>(key, minimum, maximum, deflt);
  }

  // get/assure shorthand forwarding functions for double
  template <bool Required_ = false>
  double getDouble(ZuCSpan key, double minimum, double maximum) const {
    return getScalar<double, Required_>(key, minimum, maximum);
  }
  double getDouble(
      ZuCSpan key, double minimum, double maximum, double deflt) const {
    return getScalar<double>(key, minimum, maximum, deflt);
  }
  double assureDouble(
      ZuCSpan key, double minimum, double maximum, double deflt) {
    return assureScalar<double>(key, minimum, maximum, deflt);
  }

  // get/assure for enum
  template <typename Map, bool Required_ = false>
  int getEnum(ZuCSpan key) const {
    if (auto node = getNode<Required_>(key))
      return node->template getEnum<Map, Required_>();
    if constexpr (Required_) throw Required{this, key};
    return -1;
  }
  template <typename Map>
  int getEnum(ZuCSpan key, int deflt) const {
    if (auto node = getNode(key))
      return node->template getEnum<Map>(deflt);
    return deflt;
  }
  template <typename Map>
  int assureEnum(ZuCSpan key, int deflt) {
    return mkNode(key)->template assureEnum<Map>(deflt);
  }

  // generic get/assure for flags
  template <typename Map, typename T, bool Required_ = false>
  T getFlags(ZuCSpan key) const {
    if (auto node = getNode<Required_>(key))
      return node->template getFlags<Map, T, Required_>();
    if constexpr (Required_) throw Required{this, key};
    return 0;
  }
  template <typename Map, typename T>
  T getFlags(ZuCSpan key, T deflt) const {
    if (auto node = getNode(key))
      return node->template getFlags<Map, T>(deflt);
    return deflt;
  }
  template <typename Map, typename T>
  T assureFlags(ZuCSpan key, T deflt) {
    return mkNode(key)->template assureFlags<Map, T>(deflt);
  }

  // ZtField integration - get individual field
  template <typename Field>
  ZuIfT<Field::Type::Code == ZtFieldTypeCode::CString, typename Field::T>
  getField() {
    return get<ZuTypeIn<ZuFieldProp::Required, typename Field::Props>{}>(
	Field::id(), Field::deflt()).data();
  }
  template <typename Field>
  ZuIfT<Field::Type::Code == ZtFieldTypeCode::String, typename Field::T>
  getField() {
    return get<ZuTypeIn<ZuFieldProp::Required, typename Field::Props>{}>(
	Field::id(), Field::deflt());
  }
  template <typename Field>
  ZuIfT<Field::Type::Code == ZtFieldTypeCode::Bytes, typename Field::T>
  getField() {
    auto s = get<ZuTypeIn<ZuFieldProp::Required, typename Field::Props>{}>(
	Field::id(), Field::deflt());
    auto n = ZuBase64::declen(s.length());
    ZtArray<uint8_t> buf(n);
    buf.length(ZuBase64::decode(buf, ZuBytes{s}));
    return buf;
  }
  template <typename Field>
  ZuIfT<Field::Type::Code == ZtFieldTypeCode::UDT ||
	Field::Type::Code == ZtFieldTypeCode::Time ||
	Field::Type::Code == ZtFieldTypeCode::DateTime, typename Field::T>
  getField() {
    using T = typename Field::T;
    auto s = get<ZuTypeIn<ZuFieldProp::Required, typename Field::Props>{}>(
	Field::id(), "");
    if (ZuUnlikely(!s)) return Field::deflt();
    return T{s};
  }
  template <typename Field>
  ZuIfT<Field::Type::Code == ZtFieldTypeCode::Bool, typename Field::T>
  getField() {
    return getBool<ZuTypeIn<ZuFieldProp::Required, typename Field::Props>{}>(
	Field::id(), Field::deflt());
  }
  template <typename Field>
  ZuIfT<Field::Type::Code == ZtFieldTypeCode::Int8 ||
	Field::Type::Code == ZtFieldTypeCode::UInt8 ||
	Field::Type::Code == ZtFieldTypeCode::Int16 ||
	Field::Type::Code == ZtFieldTypeCode::UInt16 ||
	Field::Type::Code == ZtFieldTypeCode::Int32 ||
	Field::Type::Code == ZtFieldTypeCode::UInt32 ||
	Field::Type::Code == ZtFieldTypeCode::Int64 ||
	Field::Type::Code == ZtFieldTypeCode::UInt64 ||
	Field::Type::Code == ZtFieldTypeCode::Int128 ||
	Field::Type::Code == ZtFieldTypeCode::UInt128, typename Field::T>
  getField() {
    using Props = typename Field::Props;
    if constexpr (ZuFieldProp::HasEnum<Props>{}) {
      using Map = ZuFieldProp::GetEnum<Props>;
      return getEnum<
	Map, ZuTypeIn<ZuFieldProp::Required, Props>{}>(
	  Field::id(), Field::deflt());
    } else if constexpr (ZuFieldProp::HasFlags<Props>{}) {
      using Map = ZuFieldProp::GetFlags<Props>;
      using T = typename Field::T;
      return getFlags<
	Map, T, ZuTypeIn<ZuFieldProp::Required, Props>{}>(
	  Field::id(), Field::deflt());
    } else {
      return getScalar<
	typename Field::T,
	ZuTypeIn<ZuFieldProp::Required, Props>{}>(
	  Field::id(), Field::minimum(), Field::maximum(), Field::deflt());
    }
  }
  template <typename Field>
  ZuIfT<Field::Type::Code == ZtFieldTypeCode::Float ||
	Field::Type::Code == ZtFieldTypeCode::Fixed ||
	Field::Type::Code == ZtFieldTypeCode::Decimal, typename Field::T>
  getField() {
    return getScalar<
      typename Field::T,
      ZuTypeIn<ZuFieldProp::Required, typename Field::Props>{}>(
	Field::id(), Field::minimum(), Field::maximum(), Field::deflt());
  }
  template <typename Field>
  ZuIfT<Field::Type::Code == ZtFieldTypeCode::CStringVec, typename Field::T>
  getField() {
    using T = typename Field::T;
    CfNode *node = getNode(Field::id());
    if (!node || !node->data.is<StrArray>()) return {};
    const auto &elems = node->data.p<StrArray>();
    return T(ZuVArray<typename ZuTraits<T>::Elem>(
      elems, elems.length(),
      [](const void *ptr, unsigned i) {
	const auto &elems = *reinterpret_cast<const StrArray *>(ptr);
	return elems[i].data();
      }));
  }
  template <typename Field>
  ZuIfT<Field::Type::Code == ZtFieldTypeCode::StringVec, typename Field::T>
  getField() {
    using T = typename Field::T;
    CfNode *node = getNode(Field::id());
    if (!node || !node->data.is<StrArray>()) return {};
    const auto &elems = node->data.p<StrArray>();
    return T(ZuVArray<typename ZuTraits<T>::Elem>(
      elems, elems.length(),
      [](const void *ptr, unsigned i) {
	const auto &elems = *reinterpret_cast<const StrArray *>(ptr);
	return elems[i];
      }));
  }
  template <typename Field>
  ZuIfT<Field::Type::Code == ZtFieldTypeCode::BytesVec, typename Field::T>
  getField() {
    using T = typename Field::T;
    CfNode *node = getNode(Field::id());
    if (!node || !node->data.is<StrArray>()) return {};
    const auto &elems = node->data.p<StrArray>();
    return T(ZuVArray<typename ZuTraits<T>::Elem>(
      elems, elems.length(),
      [](const void *ptr, unsigned i) {
	const auto &elems = *reinterpret_cast<const StrArray *>(ptr);
	const auto &s = elems[i];
	auto n = ZuBase64::declen(s.length());
	ZtArray<uint8_t> buf(n);
	buf.length(ZuBase64::decode(buf, ZuBytes{s}));
	return buf;
      }));
  }
  template <typename Field>
  ZuIfT<Field::Type::Code == ZtFieldTypeCode::Int8Vec ||
	Field::Type::Code == ZtFieldTypeCode::UInt8Vec ||
	Field::Type::Code == ZtFieldTypeCode::Int16Vec ||
	Field::Type::Code == ZtFieldTypeCode::UInt16Vec ||
	Field::Type::Code == ZtFieldTypeCode::Int32Vec ||
	Field::Type::Code == ZtFieldTypeCode::UInt32Vec ||
	Field::Type::Code == ZtFieldTypeCode::Int64Vec ||
	Field::Type::Code == ZtFieldTypeCode::UInt64Vec ||
	Field::Type::Code == ZtFieldTypeCode::Int128Vec ||
	Field::Type::Code == ZtFieldTypeCode::UInt128Vec ||
	Field::Type::Code == ZtFieldTypeCode::FloatVec ||
	Field::Type::Code == ZtFieldTypeCode::FixedVec ||
	Field::Type::Code == ZtFieldTypeCode::DecimalVec, typename Field::T>
  getField() {
    using T = typename Field::T;
    CfNode *node = getNode(Field::id());
    if (!node || !node->data.is<StrArray>()) return {};
    const auto &elems = node->data.p<StrArray>();
    using Elem = typename ZuTraits<T>::Elem;
    return T(ZuVArray<Elem>(
      elems, elems.length(),
      [](const void *ptr, unsigned i) {
	const auto &elems = *reinterpret_cast<const StrArray *>(ptr);
	return Scan<Elem>{elems[i]};
      }));
  }
  template <typename Field>
  ZuIfT<Field::Type::Code == ZtFieldTypeCode::TimeVec ||
	Field::Type::Code == ZtFieldTypeCode::DateTimeVec, typename Field::T>
  getField() {
    using T = typename Field::T;
    CfNode *node = getNode(Field::id());
    if (!node || !node->data.is<StrArray>()) return {};
    const auto &elems = node->data.p<StrArray>();
    using Elem = typename ZuTraits<T>::Elem;
    return T(ZuVArray<Elem>(
      elems, elems.length(),
      [](const void *ptr, unsigned i) {
	const auto &elems = *reinterpret_cast<const StrArray *>(ptr);
	return Elem{elems[i]};
      }));
  }

  // ZtField integration - construct fielded object
  template <typename O>
  inline O ctor() const { return Handler<O>::ctor(this); }
  template <typename O>
  inline void ctor(void *ptr) const { Handler<O>::ctor(ptr, this); }

  // ZtField integration - load fielded object
  template <typename O> using Load = typename Handler<O>::Load;

  template <typename O>
  inline void load(O &o) const { Handler<O>::load(o, this); }
  template <typename O>
  inline void update(O &o) const { Handler<O>::update(o, this); }

  // ZtField integration - get key
  template <typename O, int KeyID = 0>
  inline auto key() const {
    return ctor<ZuFieldKeyT<O, KeyID>>();
  }

  // node count
  unsigned count() const { return m_tree.count_(); }

  // parent node (nullptr if root)
  CfNode *node() const { return m_node; }

private:
  void fromArg(ZuCSpan key, int type, ZuCSpan in);
  void fromString(ZuCSpan in, ZuCSpan path, ZmRef<Defines> defines);

  void toArgs(ZtArray<ZtString> &args, ZuCSpan prefix = {}) const;

  template <unsigned Q = Quoting::File>
  ZuTuple<Cf *, ZtString, int, unsigned>
  getScope_(ZuCSpan in, Cf::Defines *defines = nullptr) const;
  template <unsigned Q = Quoting::File>
  ZuTuple<Cf *, ZtString, int, unsigned>
  mkScope_(ZuCSpan in, Cf::Defines *defines = nullptr);
  template <unsigned Q = Quoting::File>
  ZuTuple<Cf *, CfNode *, int, unsigned> mkNode_(ZuCSpan in);

  Tree		m_tree;
  CfNode	*m_node;
};

// equivalent of pwd - returns the full key from a nested tree key
inline ZtString fullKey(const Cf *cf, ZtString key) {
  while (auto node = cf->node()) {
    key = ZtString{} << node->CfNode::key << '.' << key;
    if (!(cf = node->owner)) break;
  }
  return key;
}

} // ZvCf_

#if 0
using ZvCfRequired = ZvCf_::Required;
template <typename T> using ZvCfRange = ZvCf_::Range<T>;
using ZvCfNElems = ZvCf_::NElems;
using ZvCfUsage = ZvCf_::Usage;
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

#endif /* ZvCf_HH */
