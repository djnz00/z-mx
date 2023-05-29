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
#define ZvOptValue	1
#define ZvOptArray	2

extern "C" {
  struct ZvOpt {
    const char	*m_long;
    const char	*m_short;
    int		m_type;		// ZvOptFlag or ZvOptArg
    const char	*m_default;
  };
};

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4251)
#endif

namespace ZvOptTypes {
  ZtEnumValues_(Flag = ZvOptFlag, Value = ZvOptValue, Array = ZvOptArray);
  ZtEnumMap("ZvOptTypes", Map, "flag", Flag, "value", Value, "array", Array);
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
// thrown by getScalar() on range error
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
// thrown by all() on number of values error
class NElems : public Range_<unsigned> {
  using T = unsigned;
  using Base = Range_<T>;
public:
  NElems(const Cf *cf, ZuString key, T minimum, T maximum, T value) :
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
inline int toEnum(const Cf *cf, ZuString key, ZuString value, int deflt = -1)
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
inline auto toFlags(Args &&... args) {
  return toFlags_<Map, uint32_t, Required_>(ZuFwd<Args>(args)...);
}
template <typename Map, bool Required_ = false, typename ...Args>
inline auto toFlags64(Args &&... args) {
  return toFlags_<Map, uint64_t, Required_>(ZuFwd<Args>(args)...);
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

using Null = ZuNull;
using String = ZtString;
using StrArray = ZtArray<String>;
using CfRef = ZmRef<Cf>;
using CfArray = ZtArray<CfRef>;
using Data = ZuUnion<Null, String, StrArray, CfRef, CfArray>;

class Cf;

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

  void null() { data.v<Null>(); }

  auto type() const { return data.type(); }

  template <typename T, typename P>
  void set_(P &&v) { data.v<T>(ZuFwd<P>(v)); }
  template <typename P> void set(P &&v) { set_<String>(ZuFwd<P>(v)); }
  template <typename P> void setCf(P &&v) { set_<CfRef>(ZuFwd<P>(v)); }

  template <typename T, bool Required_ = false>
  const T &get_() const {
    if constexpr (Required_)
      if (!data.contains<T>()) throw Required{owner, key};
    return data.v<T>();
  }
  template <typename T>
  T get_(T deflt) const {
    if (!data.contains<T>()) return deflt;
    return data.v<T>();
  }
  template <typename T>
  const T &assure_(const T &deflt) {
    if (!data.contains<T>()) data.v<T>(deflt);
    return data.v<T>();
  }
  template <bool Required_ = false>
  const String &get() const { return get_<String, Required_>(); }
  String get(String deflt) const { return get_<String>(ZuMv(deflt)); }
  const String &assure(const String &deflt) { return assure_<String>(deflt); }
  template <bool Required_ = false>
  const CfRef &getCf() const { return get_<CfRef, Required_>(); }

  template <typename T, bool Required_ = false>
  T getScalar(T minimum, T maximum) const {
    return toScalar<T, Required_>(
	owner, key, get<Required_>(),
	minimum, maximum);
  }
  template <typename T>
  T getScalar(T minimum, T maximum, T deflt) const {
    return toScalar<T>(owner, key, get(), minimum, maximum, deflt);
  }
  template <typename T>
  T assureScalar(T minimum, T maximum, T deflt) {
    return toScalar<T>(
	owner, key, assure(ZtString{} << deflt),
	minimum, maximum, deflt);
  }

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

  template <typename Map, bool Required_ = false>
  int getEnum() const {
    return toEnum<Map, Required_>(owner, key, get<Required_>(), -1);
  }
  template <typename Map>
  int getEnum(int deflt) const {
    return toEnum<Map>(owner, key, get(), deflt);
  }
  template <typename Map>
  int assureEnum(int deflt) {
    return toEnum<Map>(owner, key, assure<String>(Map::v2s(deflt)), deflt);
  }

  template <typename Map, typename T, bool Required_ = false>
  T getFlags_() const {
    return toFlags_<Map, T, Required_>(
	owner, key, get<Required_>(), 0);
  }
  template <typename Map, typename T>
  T getFlags_(T deflt) const {
    return toFlags_<Map, T>(owner, key, get(), deflt);
  }
  template <typename Map, typename T>
  T assureFlags_(T deflt) {
    using Print = typename Map::Print;
    return toFlags_<Map, T>(
	owner, key, get(ZtString{} << Print{deflt}), deflt);
  }

  template <typename Map, bool Required_ = false> uint32_t getFlags() const {
    return getFlags_<Map, uint32_t, Required_>();
  }
  template <typename Map> uint32_t getFlags(uint32_t deflt) const {
    return getFlags_<Map, uint32_t>(deflt);
  }
  template <typename Map> uint32_t assureFlags(uint32_t deflt) {
    return assureFlags_<Map, uint32_t>(deflt);
  }

  template <typename Map, bool Required_ = false> uint64_t getFlags64() const {
    return getFlags_<Map, uint64_t, Required_>();
  }
  template <typename Map> uint64_t getFlags64(uint64_t deflt) const {
    return getFlags_<Map, uint64_t>(deflt);
  }
  template <typename Map> uint64_t assureFlags64(uint64_t deflt) {
    return assureFlags_<Map, uint64_t>(deflt);
  }

  template <typename T, typename L>
  void all_(L l) const { data.v<T>().all(ZuMv(l)); }
  template <typename T, typename L>
  void all_(unsigned minimum, unsigned maximum, L l) const {
    const auto &elems = data.v<T>();
    unsigned n = elems.length();
    if (n < minimum || n > maximum)
      throw NElems{owner, key, minimum, maximum, n};
    elems.all(ZuMv(l));
  }
  template <typename L>
  void all(L l) const { all_<StrArray>(ZuMv(l)); }
  template <typename T, typename L>
  void all(unsigned minimum, unsigned maximum, L l) const {
    all_<StrArray>(minimum, maximum, ZuMv(l));
  }
  template <typename L>
  void allCf(L l) const { all_<CfArray>(ZuMv(l)); }
  template <typename T, typename L>
  void allCf(unsigned minimum, unsigned maximum, L l) const {
    all_<CfArray>(minimum, maximum, ZuMv(l));
  }

  template <typename T, typename P>
  void setElem_(unsigned i, P &&v) {
    using Elem = typename T::T;
    new (data.v<T>().set(i)) Elem{ZuFwd<P>(v)};
  }
  template <typename T, bool Required_ = false>
  const typename T::T &getElem_(unsigned i) const {
    if (!data.contains<T>()) {
      if constexpr (Required_) throw Required{owner, key};
      return ZuNullRef<typename T::T>();
    }
    const auto &elems = data.v<T>();
    if (i >= elems.length()) return ZuNullRef<typename T::T>();
    return elems.get(i);
  }
  template <typename T>
  typename T::T getElem_(unsigned i, typename T::T deflt) const {
    if (!data.contains<T>()) return deflt;
    const auto &elems = data.v<T>();
    if (i >= elems.length()) return deflt;
    return elems.get(i);
  }
  template <typename T>
  const typename T::T &assureElem_(unsigned i, const typename T::T &deflt) {
    if (!data.contains<T>() || i >= data.v<T>().length())
      data.v<T>().set(i, deflt);
    return data.v<T>().get(i);
  }

  template <typename P>
  void setElem(unsigned i, P &&v) {
    return setElem_<StrArray>(i, ZuFwd<P>(v));
  }
  template <bool Required_ = false>
  const String &getElem(unsigned i) const {
    return getElem_<StrArray, Required_>(i);
  }
  String getElem(unsigned i, String deflt) const {
    return getElem_<StrArray>(i, ZuMv(deflt));
  }
  const String &assureElem(unsigned i, const String &deflt) {
    return assureElem_<StrArray>(i, deflt);
  }

  template <typename P>
  void setElemCf(unsigned i, P &&v) {
    return setElem_<CfArray>(i, ZuFwd<P>(v));
  }
  template <bool Required_ = false>
  const CfRef &getElemCf(unsigned i) const {
    return getElem_<CfArray, Required_>(i);
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
  static void update(O &o, const Cf_ *cf) {
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
  int fromCLI(const ZvOpt *opts, ZuString line);
  int fromArgs(const ZvOpt *opts, const ZtArray<ZtString> &args);

  using Defines_ = ZmRBTreeKV<ZtString, ZtString, ZmRBTreeUnique<true>>;
  struct Defines : public ZuObject, public Defines_ { };

  void fromString(ZuString in, ZmRef<Defines> defines = new Defines{}) {
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

  void print(ZmStream &s, ZtString &indent) const;

  template <typename S> void print(S &s_) const {
    ZmStream s{s_};
    ZtString indent;
    print(s, indent);
  }
  void print(ZmStream &s) const {
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

  ZuPair<Cf *, ZtString> getScope(ZuString fullKey) const;

  template <bool Required_ = false>
  CfNode *getNode(ZuString fullKey) const {
    auto [self, key] = getScope(fullKey);
    if (!self) {
      if constexpr (Required_) throw Required{this, fullKey};
      return nullptr;
    }
    Node *node = self->m_tree.find(key);
    if (!node)
      if constexpr (Required_) throw Required{this, fullKey};
    return node;
  }
  CfNode *mkNode(ZuString fullKey);

public:
  void set(ZuString key, ZtString value);
  template <bool Required_ = false>
  const ZtString &get(ZuString key) const {
    if (auto node = getNode<Required_>(key))
      return node->template get<Required_>();
    if constexpr (Required_) throw Required{this, key};
    return ZuNullRef<ZtString>();
  }
  ZtString get(ZuString key, ZtString deflt) const {
    if (auto node = getNode(key)) return node->get(deflt);
    return deflt;
  }
  const ZtString &assure(ZuString key, const ZtString &deflt) {
    return mkNode(key)->assure(deflt);
  }

  ZmRef<Cf> mkCf(ZuString key);
  void setCf(ZuString key, ZmRef<Cf> cf);
  template <bool Required_ = false>
  ZmRef<Cf> getCf(ZuString key) const {
    if (auto node = getNode<Required_>(key))
      return node->template get_<CfRef, Required_>();
    if constexpr (Required_) throw Required{this, key};
    return {};
  }

  template <bool Required_ = false, typename L>
  void all(ZuString key, L l) const {
    if (auto node = getNode<Required_>(key))
      node->template all<StrArray>(ZuMv(l));
    if constexpr (Required_) throw Required{this, key};
  }
  template <bool Required_ = false, typename L>
  void all(ZuString key, unsigned minimum, unsigned maximum, L l) const {
    if (auto node = getNode<Required_>(key))
      node->template all<StrArray>(minimum, maximum, ZuMv(l));
    if constexpr (Required_) throw Required{this, key};
  }
  template <bool Required_ = false, typename L>
  void allCf(ZuString key, L l) const {
    if (auto node = getNode<Required_>(key))
      node->template all<CfArray>(ZuMv(l));
    if constexpr (Required_) throw Required{this, key};
  }
  template <bool Required_ = false, typename L>
  void allCf(ZuString key, unsigned minimum, unsigned maximum, L l) const {
    if (auto node = getNode<Required_>(key))
      node->template all<CfArray>(minimum, maximum, ZuMv(l));
    if constexpr (Required_) throw Required{this, key};
  }

  template <typename P>
  void setElem(ZuString key, unsigned i, P &&v) {
    return mkNode(key)->setElem(i, ZuFwd<P>(v));
  }
  template <bool Required_ = false, typename P>
  const String &getElem(ZuString key, unsigned i) const {
    if (auto node = getNode<Required_>(key))
      return node->template getElem<Required_>(i);
    if constexpr (Required_) throw Required{this, key};
    return ZuNullRef<String>();
  }
  template <typename P>
  String getElem(ZuString key, unsigned i, String deflt) const {
    if (auto node = getNode(key)) return node->getElem(i, ZuMv(deflt));
    return deflt;
  }
  template <typename P>
  const String &assureElem(ZuString key, unsigned i, const String &deflt) {
    return mkNode(key)->assureElem(i, deflt);
  }

  template <typename P>
  void setElemCf(ZuString key, unsigned i, P &&v) {
    return mkNode(key)->setElemCf(i, ZuFwd<P>(v));
  }
  template <bool Required_ = false, typename P>
  const CfRef &getElemCf(ZuString key, unsigned i) const {
    if (auto node = getNode<Required_>(key))
      return node->template getElemCf<Required_>(i);
    if constexpr (Required_) throw Required{this, key};
    return ZuNullRef<CfRef>();
  }

  void unset(ZuString key);

  template <typename L>
  void all(L l) {
    auto i = m_tree.iterator();
    while (auto node = i.iterate()) l(node);
  }

  void clean();
  void merge(const Cf *cf);

  template <typename T, bool Required_ = false>
  T getScalar(ZuString key, T minimum, T maximum) const {
    if (auto node = getNode<Required_>(key))
      return node->template getScalar<T, Required_>(minimum, maximum);
    if constexpr (Required_) throw Required{this, key};
    return 0;
  }
  template <typename T>
  T getScalar(ZuString key, T minimum, T maximum, T deflt) const {
    if (auto node = getNode(key))
      return node->template getScalar<T>(minimum, maximum, deflt);
    return deflt;
  }
  template <typename T>
  T assureScalar(ZuString key, T minimum, T maximum, T deflt) {
    return mkNode(key)->template assureScalar<T>(minimum, maximum, deflt);
  }

  template <bool Required_ = false>
  int getInt(ZuString key, int minimum, int maximum) const {
    return getScalar<int, Required_>(key, minimum, maximum);
  }
  int getInt(ZuString key, int minimum, int maximum, int deflt) const {
    return getScalar<int>(key, minimum, maximum, deflt);
  }
  int assureInt(ZuString key, int minimum, int maximum, int deflt) {
    return assureScalar<int>(key, minimum, maximum, deflt);
  }

  template <bool Required_ = false>
  int64_t getInt64(ZuString key, int64_t minimum, int64_t maximum) const {
    return getScalar<int64_t, Required_>(key, minimum, maximum);
  }
  int64_t getInt64(
      ZuString key, int64_t minimum, int64_t maximum, int64_t deflt) const {
    return getScalar<int64_t>(key, minimum, maximum, deflt);
  }
  int64_t assureInt64(
      ZuString key, int64_t minimum, int64_t maximum, int64_t deflt) {
    return assureScalar<int64_t>(key, minimum, maximum, deflt);
  }

  template <bool Required_ = false>
  double getDbl(ZuString key, double minimum, double maximum) const {
    return getScalar<double, Required_>(key, minimum, maximum);
  }
  double getDbl(
      ZuString key, double minimum, double maximum, double deflt) const {
    return getScalar<double>(key, minimum, maximum, deflt);
  }
  double assureDbl(
      ZuString key, double minimum, double maximum, double deflt) {
    return assureScalar<double>(key, minimum, maximum, deflt);
  }

  template <typename Map, bool Required_ = false>
  int getEnum(ZuString key) const {
    if (auto node = getNode<Required_>(key))
      return node->template getEnum<Map, Required_>();
    if constexpr (Required_) throw Required{this, key};
    return -1;
  }
  template <typename Map>
  int getEnum(ZuString key, int deflt) const {
    if (auto node = getNode(key))
      return node->template getEnum<Map>(deflt);
    return deflt;
  }
  template <typename Map>
  int assureEnum(ZuString key, int deflt) {
    return mkNode(key)->template assureEnum<Map>(deflt);
  }

  template <typename Map, typename T, bool Required_ = false>
  T getFlags_(ZuString key) const {
    if (auto node = getNode<Required_>(key))
      return node->template getFlags_<Map, T, Required_>();
    if constexpr (Required_) throw Required{this, key};
    return 0;
  }
  template <typename Map, typename T>
  T getFlags_(ZuString key, T deflt) const {
    if (auto node = getNode(key))
      return node->template getFlags_<Map, T>(deflt);
    return deflt;
  }
  template <typename Map, typename T>
  T assureFlags_(ZuString key, T deflt) {
    return mkNode(key)->template assureFlags_<Map, T>(deflt);
  }

  template <typename Map, bool Required_ = false>
  uint32_t getFlags(ZuString key) const {
    return getFlags_<Map, uint32_t, Required_>(key);
  }
  template <typename Map>
  uint32_t getFlags(ZuString key, uint32_t deflt) const {
    return getFlags_<Map, uint32_t>(key, deflt);
  }
  template <typename Map>
  uint32_t assureFlags(ZuString key, uint32_t deflt) {
    return assureFlags_<Map, uint32_t>(key, deflt);
  }

  template <typename Map, bool Required_ = false>
  uint64_t getFlags64(ZuString key) const {
    return getFlags_<Map, uint64_t, Required_>(key);
  }
  template <typename Map>
  uint64_t getFlags64(ZuString key, uint64_t deflt) const {
    return getFlags_<Map, uint64_t>(key, deflt);
  }
  template <typename Map>
  uint64_t assureFlags64(ZuString key, uint64_t deflt) {
    return assureFlags_<Map, uint64_t>(key, deflt);
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
	Field::id(), Field::deflt(), Field::minimum(), Field::maximum());
  }
  template <typename Field>
  ZuIfT<Field::Type == ZtFieldType::Hex, typename Field::T>
  getField() {
    using T = typename Field::T;
    return getScalar<T, Field::Flags & ZtFieldFlags::Required>(
	Field::id(), Field::deflt(), ZuCmp<T>::minimum(), ZuCmp<T>::maximum());
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
  inline O ctor() const { return Fielded<O>::ctor(this); }
  template <typename O>
  inline void ctor(void *ptr) const { Fielded<O>::ctor(ptr, this); }

  template <typename O> using Load = typename Fielded<O>::Load;

  template <typename O>
  inline void load(O &o) const { Fielded<O>::load(o, this); }
  template <typename O>
  inline void update(O &o) const { Fielded<O>::update(o, this); }

  template <typename O, unsigned KeyID = 0>
  inline auto key() const {
    return Fielded<O>::template key<KeyID>(this);
  }

  unsigned count() const { return m_tree.count_(); }
  CfNode *node() const { return m_node; }

private:
  void fromArg(ZuString key, int type, ZuString in);
  void fromString(ZuString in, ZuString path, ZmRef<Defines> defines);

  void toArgs(ZtArray<ZtString> &args, ZuString prefix = {}) const;

  template <unsigned Q = Quoting::File>
  ZuTuple<Cf *, ZtString, int, unsigned>
  getScope_(ZuString in, Cf::Defines *defines = nullptr) const;
  template <unsigned Q = Quoting::File>
  ZuTuple<Cf *, ZtString, int, unsigned>
  mkScope_(ZuString in, Cf::Defines *defines = nullptr);
  template <unsigned Q = Quoting::File>
  ZuTuple<Cf *, CfNode *, int, unsigned> mkNode_(ZuString in);

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

#endif /* ZvCf_HPP */
