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
#include <zlib/ZmNoLock.hpp>
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
  ZtEnumValues_(
      Flag = ZvOptFlag, Scalar = ZvOptScalar, Multi = ZvOptMulti);
  ZtEnumMap("ZvOptTypes", Map, "flag", Flag, "scalar", Scalar, "multi", Multi);
}

class ZvAPI ZvCf : public ZuObject {
  ZvCf(const ZvCf &);
  ZvCf &operator =(const ZvCf &);	// prevent mis-use

public:
  ZvCf();
private:
  ZvCf(ZvCf *parent);
public:
  ~ZvCf();

  using Int = ZuBox<int32_t>;
  using Int64 = ZuBox<int64_t>;
  using Double = ZuBox<double>;
  using Enum = ZtEnum;
  using Flags = ZuBox<uint32_t>;
  using Flags64 = ZuBox<uint64_t>;

private:
  static ZtString fullKey(const ZvCf *cf, ZtString key) {
    while (const ZvCf *parent = cf->parent()) {
      ZvCf::Iterator i(parent);
      ZuString key_;
      while (ZmRef<ZvCf> cf_ = i.subset(key_)) {
	if (cf == cf_.ptr()) {
	  key = ZtString{} << key_ << ':' << key;
	  break;
	}
      }
      cf = parent;
    }
    return key;
  }

public:
  // thrown by fromArgs() on error
  class ZvAPI Usage : public ZvError {
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

  static void parseCLI(ZuString line, ZtArray<ZtString> &args);
  // fromCLI() and fromArgs() return the number of positional arguments
  int fromCLI(ZvCf *syntax, ZuString line);
  int fromArgs(ZvCf *options, const ZtArray<ZtString> &args);
  int fromArgs(const ZvOpt *opts, int argc, char **argv);
  int fromCLI(const ZvOpt *opts, ZuString line); // deprecated
  int fromArgs(const ZvOpt *opts, const ZtArray<ZtString> &args); // deprecated

  // thrown by fromString() and fromFile() for an invalid key
  class ZvAPI Invalid : public ZvError {
  public:
    Invalid(const ZvCf *cf, ZuString key, ZuString fileName) :
	m_key(fullKey(cf, key)), m_fileName(fileName) { }
    void print_(ZmStream &s) const {
      if (m_fileName) s << '"' << m_fileName << "\": ";
      s << "invalid key \"" << m_key << '"';
    }
    ZuInline const ZtString &key() const { return m_key; }

  private:
    ZtString	m_key;
    ZtString	m_fileName;
  };

  // thrown by fromString() and fromFile() on error
  class ZvAPI Syntax : public ZvError {
  public:
    Syntax(int line, char ch, ZuString fileName) :
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
	s << '\\' << ZuBoxed((unsigned)m_ch & 0xff).
	  fmt<ZuFmt::Hex<0, ZuFmt::Alt<ZuFmt::Right<2>>>>();
      s << '\'';
    }

  private:
    int		m_line;
    char	m_ch;
    ZtString	m_fileName;
  };

  class ZvAPI BadDefine : public ZvError {
  public:
    template <typename Define, typename FileName>
    BadDefine(Define &&define, FileName &&fileName) :
      m_define(ZuFwd<Define>(define)), m_fileName(ZuFwd<FileName>(fileName)) { }
    void print_(ZmStream &s) const {
      if (m_fileName) s << '"' << m_fileName << "\": ";
      s << "bad %%define \"" << m_define << '"';
    }
  private:
    ZtString	m_define;
    ZtString	m_fileName;
  };

  using Defines_ =
    ZmRBTreeKV<ZtString, ZtString,
      ZmRBTreeUnique<true,
	ZmRBTreeLock<ZmNoLock> > >;
  struct Defines : public ZuObject, public Defines_ { };

  void fromString(
      ZuString in, bool validate,
      ZmRef<Defines> defines = new Defines()) {
    fromString(in, validate, ZuString(), defines);
  }

  // thrown by fromFile() on error
  class ZvAPI File2Big : public ZvError {
  public:
    File2Big(ZuString fileName) : m_fileName(fileName) { }
    void print_(ZmStream &s) const {
      s << '"' << m_fileName << " file too big";
    };
  private:
    ZtString	m_fileName;
  };

  class ZvAPI FileOpenError : public ZvError {
  public:
    FileOpenError(ZuString fileName, ZeError e) :
      m_fileName(fileName), m_err(e) { }

    void print_(ZmStream &s) const {
      s << '"' << m_fileName << "\" " << m_err;
    }

  private:
    ZtString	m_fileName;
    ZeError   m_err;
  };

  template <typename FileName>
  void fromFile(const FileName &fileName, bool validate,
      ZmRef<Defines> defines = new Defines()) {
    ZtString in;
    {
      ZiFile file;
      ZeError e;

      if (file.open(fileName, ZiFile::ReadOnly, 0, &e) < 0)
	throw FileOpenError(fileName, e);

      int n = (int)file.size();

      if (n >= ZvCfMaxFileSize) throw File2Big(fileName);
      in.length(n);
      if (file.read(in.data(), n, &e) < 0) throw e;

      file.close();
    }
    ZtString dir = ZiFile::dirname(fileName);
    if (!defines->find("TOPDIR")) defines->add("TOPDIR", dir);
    defines->del("CURDIR"); defines->add("CURDIR", ZuMv(dir));
    fromString(in, validate, fileName, defines);
  }

  // thrown by fromEnv() on error
  class ZvAPI EnvSyntax : public ZvError {
  public:
    EnvSyntax(int pos, char ch) : m_pos(pos), m_ch(ch) { }
    void print_(ZmStream &s) const {
      s << "syntax error at position " << ZuBoxed(m_pos) << " near '";
      if (m_ch >= 0x20 && m_ch < 0x7f)
	s << m_ch;
      else
	s << '\\' << ZuBoxed((unsigned)m_ch & 0xff).
	  fmt<ZuFmt::Hex<0, ZuFmt::Alt<ZuFmt::Right<2> > >>();
      s << '\'';
    }
  private:
    int		m_pos;
    char	m_ch;
  };

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
    const ZvCf		&cf;
    mutable ZtString	prefix;

    template <typename S> void print(S &s_) const {
      ZmStream s{s_};
      print(s);
    }
    void print(ZmStream &s) const {
      cf.print(s, ZuMv(prefix));
    }
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

public:
  // thrown by all get methods for missing values when required is true
  class ZvAPI Required : public ZvError {
  public:
    Required(const ZvCf *cf, ZuString key) :
	m_key(fullKey(cf, key)), m_bt(1) { }
    const ZtString &key() const { return m_key; }
    void print_(ZmStream &s) const {
      s << '"' << m_key << "\" missing at:\n" << m_bt;
    }

  private:
    ZtString	m_key;
    ZmBackTrace	m_bt;
  };

  // template base class for NValues / RangeInt / RangeDbl exceptions
  template <typename T> class Range_ : public ZvError {
  public:
    Range_(ZtString key, T minimum, T maximum, T value) :
	m_key(ZuMv(key)),
	m_minimum(minimum), m_maximum(maximum), m_value(value) { }
    const ZtString &key() const { return m_key; }
    T minimum() const { return m_minimum; }
    T maximum() const { return m_maximum; }
    T value() const { return m_value; }

  protected:
    ZtString	m_key;
    T		m_minimum;
    T		m_maximum;
    T		m_value;
  };
  // thrown by getMultiple() on number of values error
  class ZvAPI NValues : public Range_<Int> {
  public:
    NValues(
	const ZvCf *cf, ZuString key, Int minimum, Int maximum, Int value) :
	Range_<Int>(fullKey(cf, key), minimum, maximum, value) { }
    void print_(ZmStream &s) const {
      s << '"' << m_key << "\" invalid number of values "
	"min(" << m_minimum << ") <= " << m_value <<
	" <= max(" << m_maximum << ")";
    }
  };
  // thrown by getInt() on range error
  class ZvAPI RangeInt : public Range_<Int> {
  public:
    RangeInt(
	const ZvCf *cf, ZuString key,
	Int minimum, Int maximum, Int value) :
	Range_<Int>(fullKey(cf, key), minimum, maximum, value) { }
    void print_(ZmStream &s) const {
      s << '"' << m_key << "\" out of range "
	"min(" << m_minimum << ") <= " << m_value <<
	" <= max(" << m_maximum << ")";
    }
  };
  // thrown by getInt64() on range error
  class ZvAPI RangeInt64 : public Range_<Int64> {
  public:
    RangeInt64(
	const ZvCf *cf, ZuString key,
	Int64 minimum, Int64 maximum, Int64 value) :
	Range_<Int64>(fullKey(cf, key), minimum, maximum, value) { }
    void print_(ZmStream &s) const {
      s << '"' << m_key << "\" out of range "
	"min(" << m_minimum << ") <= " << m_value <<
	" <= max(" << m_maximum << ")";
    }
  };
  // thrown by getDbl() on range error
  class ZvAPI RangeDbl : public Range_<Double> {
  public:
    RangeDbl(
	const ZvCf *cf, ZuString key,
	Double minimum, Double maximum, Double value) :
      Range_<Double>(fullKey(cf, key), minimum, maximum, value) { }
    void print_(ZmStream &s) const {
      s << '"' << m_key << "\" out of range "
	"min(" << m_minimum << ") <= " << m_value <<
	" <= max(" << m_maximum << ")";
    }
  };

  class ZvAPI BadFmt : public ZvError {
  public:
    BadFmt(const ZvCf *cf, ZuString key, ZuString value, ZuString fmt) :
	m_key(fullKey(cf, key)), m_value(value), m_fmt(fmt) { }
    void print_(ZmStream &s) const {
      s << '"' << m_key << "\" invalid value \""
	<< m_value << "\": not format " << m_fmt;
    }

  private:
    ZtString		m_key;
    ZtString		m_value;
    ZtString		m_fmt;
  };

  ZuString get(ZuString key, bool required, ZuString def) const;
  ZuString get(ZuString key, bool required = false) const {
    return get(key, required, ZuString{});
  }

  const ZtArray<ZtString> *getMultiple(ZuString key,
      unsigned minimum, unsigned maximum, bool required = false) const;

  void set(ZuString key, ZuString val);
  ZtArray<ZtString> *setMultiple(ZuString key);

  void unset(ZuString key);

  ZmRef<ZvCf> subset(ZuString key) const {
    return const_cast<ZvCf *>(this)->subset(key, false, false);
  }
  ZmRef<ZvCf> subset(ZuString key, bool required) const {
    return const_cast<ZvCf *>(this)->subset(key, required, false);
  }
  ZmRef<ZvCf> subset(ZuString key,  bool required, bool create);
  void subset(ZuString key, ZvCf *cf);

  void merge(const ZvCf *cf);

  static Int toInt(const ZvCf *cf, ZuString key, ZuString value,
      Int minimum, Int maximum, Int def = Int()) {
    if (!value) return def;
    Int i(value);
    if (i < minimum || i > maximum)
      throw RangeInt(cf, key, minimum, maximum, i);
    return i;
  }

  static Int64 toInt64(const ZvCf *cf, ZuString key, ZuString value,
      Int64 minimum, Int64 maximum, Int64 def = Int64()) {
    if (!value) return def;
    Int64 i(value);
    if (i < minimum || i > maximum)
      throw RangeInt64(cf, key, minimum, maximum, i);
    return i;
  }

  static Double toDbl(const ZvCf *cf, ZuString key, ZuString value,
      Double minimum, Double maximum, Double def = Double()) {
    if (!value) return def;
    ZuBox<Double> d(value);
    if (d < minimum || d > maximum)
      throw RangeDbl(cf, key, minimum, maximum, d);
    return d;
  }

  template <typename Key>
  Int getInt(const Key &key, Int minimum, Int maximum, bool required,
      Int def = Int()) const {
    return toInt(this, key, get(key, required), minimum, maximum, def);
  }

  template <typename Key>
  Int64 getInt64(const Key &key, Int64 minimum, Int64 maximum,
      bool required, Int64 def = Int64()) const {
    return toInt64(this, key, get(key, required), minimum, maximum, def);
  }

  template <typename Key>
  Double getDbl(const Key &key, Double minimum, Double maximum,
      bool required, Double def = Double()) const {
    return toDbl(this, key, get(key, required), minimum, maximum, def);
  }

  template <typename Map, typename Key, typename Value>
  static Enum toEnum(
      const Key &key, const Value &value, Enum def = Enum()) {
    return ZvEnum<Map>::instance()->s2v(key, value, def);
  }

  template <typename Map, typename Key>
  Enum getEnum(const Key &key, bool required, Enum def = Enum()) const {
    return toEnum<Map>(key, get(key, required), def);
  }

  template <typename Map, typename Key, typename Value>
  static Flags toFlags(
      const Key &key, const Value &value, Flags def = 0) {
    if (!value) return def;
    return ZvFlags<Map>::instance()->template scan<Flags>(key, value);
  }

  template <typename Map, typename Key, typename Value>
  static Flags64 toFlags64(
      const Key &key, const Value &value, Flags64 def = 0) {
    if (!value) return def;
    return ZvFlags<Map>::instance()->template scan<Flags64>(key, value);
  }

  template <typename Map, typename Key>
  Flags getFlags(
      const Key &key, bool required = false, Flags def = 0) const {
    return toFlags<Map>(key, get(key, required), def);
  }

  template <typename Map, typename Key>
  Flags64 getFlags64(
      const Key &key, bool required = false, Flags64 def = 0) const {
    return toFlags64<Map>(key, get(key, required), def);
  }

private:
  struct Node : public ZuObject {
    ZtArray<ZtString>	m_values;
    ZmRef<ZvCf>		m_cf;
  };

  using NodeRef = ZmRef<Node>;

  constexpr static const char *HeapID() { return "ZvCf"; }
  using Tree =
    ZmRBTreeKV<ZtString, NodeRef,
      ZmRBTreeUnique<true,
	ZmRBTreeHeapID<HeapID>>>;

public:
  unsigned count() const { return m_tree.count_(); }
  ZmRef<ZvCf> parent() const { return m_parent; }

  class Iterator;
friend Iterator;
  class ZvAPI Iterator {
  public:
    Iterator(const ZvCf *cf) : m_cf(cf), m_iterator(cf->m_tree) { }
    Iterator(const ZvCf *cf, ZuString prefix) :
	m_iterator(cf->m_tree, prefix) { }
    ~Iterator();

    ZuString get(ZuString &key);
    const ZtArray<ZtString> *getMultiple(ZuString &key,
	unsigned minimum, unsigned maximum);
    ZmRef<ZvCf> subset(ZuString &key);
    Int getInt(ZuString &key, Int minimum, Int maximum, Int def = Int()) {
      return toInt(m_cf, key, get(key), minimum, maximum, def);
    }
    Int64 getInt64(ZuString &key, Int64 minimum, Int64 maximum,
	Int64 def = Int64()) {
      return toInt64(m_cf, key, get(key), minimum, maximum, def);
    }
    Double getDbl(ZuString &key, Double minimum, Double maximum,
	Double def = Double()) {
      return toDbl(m_cf, key, get(key), minimum, maximum, def);
    }
    template <typename Map>
    Enum getEnum(ZuString &key, Enum def = Enum()) {
      return toEnum<Map>(key, get(key), def);
    }

  private:
    const ZvCf			*m_cf;
    Tree::ReadIterator<>	m_iterator;
  };

  friend ZuPrintFn ZuPrintType(ZvCf *);

private:
  ZmRef<ZvCf> scope(ZuString fullKey, ZuString &key, bool create);

  void fromArg(ZuString fullKey, int type, ZuString argVal);
  void fromString(ZuString in, bool validate,
      ZuString fileName, ZmRef<Defines> defines);

  void toArgs(ZtArray<ZtString> &args, ZuString prefix = {}) const;

  static ZtString quoteArgValue(ZuString value);
  static ZtString quoteValue(ZuString value);

  Tree		m_tree;
  ZvCf		*m_parent;
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

#endif /* ZvCf_HPP */
