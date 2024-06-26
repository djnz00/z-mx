//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// enum wrapper

#ifndef ZtEnum_HH
#define ZtEnum_HH

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZtLib_HH
#include <zlib/ZtLib.hh>
#endif

#include <zlib/ZuBox.hh>
#include <zlib/ZuTuple.hh>
#include <zlib/ZuAssert.hh>
#include <zlib/ZuObject.hh>
#include <zlib/ZuPP.hh>

#include <zlib/ZmObject.hh>
#include <zlib/ZmRef.hh>
#include <zlib/ZmLHash.hh>
#include <zlib/ZmNoLock.hh>

using ZtEnum = ZuBox_1(int8_t);

// ZtEnum class declaration macros
//   Note: use in this order: Values; Map; Flags;...

#define ZtEnumNames_(ID, ...) \
  inline ZuTuple<const char *const *const, unsigned> names() { \
    static const char *names[] = { __VA_ARGS__ }; \
    return ZuTuple<const char *const *const, unsigned>{ \
	names, static_cast<unsigned>(sizeof(names) / sizeof(names[0]))}; \
  } \
  inline const char *name(int i) { \
    ZuTuple<const char *const *const, unsigned> names_ = names(); \
    if (i >= (int)names_.p<1>()) return "Unknown"; \
    if (i < 0) return ""; \
    return names_.p<0>()[i]; \
  } \
  struct Map : public Map_<Map> { \
    constexpr static const char *id() { return #ID; } \
    Map() { for (unsigned i = 0; i < N; i++) this->add(name(i), i); } \
  }; \
  template <typename S> inline ZtEnum lookup(const S &s) { \
    return Map::s2v(s); \
  }
#define ZtEnumValues_(...) \
  enum { Invalid = -1, __VA_ARGS__, N }; \
  ZuAssert(N <= 1024); \
  enum { Bits = \
    N <= 2 ? 1 : N <= 4 ? 2 : N <= 8 ? 3 : N <= 16 ? 4 : N <= 32 ? 5 : \
    N <= 64 ? 6 : N <= 128 ? 7 : N <= 256 ? 8 : N <= 512 ? 9 : 10 \
  }; \
  template <typename T> struct Map_ : public ZuObject { \
  private: \
    using V2S = \
      ZmLHashKV<ZtEnum, ZuString, \
	ZmLHashStatic<Bits, \
	  ZmLHashLock<ZmNoLock> > >; \
    using S2V = \
      ZmLHashKV<ZuString, ZtEnum, \
	ZmLHashStatic<Bits, \
	  ZmLHashLock<ZmNoLock> > >; \
  protected: \
    void init(const char *s, int v, ...) { \
      if (ZuUnlikely(!s)) return; \
      add(s, v); \
      va_list args; \
      va_start(args, v); \
      while (s = va_arg(args, const char *)) \
	add(s, v = va_arg(args, int)); \
      va_end(args); \
    } \
    void add(ZuString s, ZtEnum v) { m_s2v->add(s, v); m_v2s->add(v, s); } \
    static T *instance() { return ZmSingleton<T>::instance(); } \
  private: \
    ZtEnum s2v_(ZuString s) const { return m_s2v->findVal(s); } \
    ZuString v2s_(ZtEnum v) const { return m_v2s->findVal(v); } \
    template <typename L> void all_(L l) const { \
      auto i = m_s2v->readIterator(); \
      while (auto kv = i.iterate()) { \
	l(kv->template p<0>(), kv->template p<1>()); \
      } \
    } \
  public: \
    Map_() { m_s2v = new S2V{}; m_v2s = new V2S{}; } \
    static ZtEnum s2v(ZuString s) { return instance()->s2v_(s); } \
    static ZuString v2s(ZtEnum v) { return instance()->v2s_(v); } \
    template <typename L> static void all(L l) { instance()->all_(ZuMv(l)); } \
  private: \
    ZmRef<S2V>	m_s2v; \
    ZmRef<V2S>	m_v2s; \
  }
#define ZtEnumValues(ID, ...) \
  ZtEnumValues_(__VA_ARGS__); \
  ZtEnumNames_(ID, ZuPP_Eval(ZuPP_MapComma(ZuPP_Q, __VA_ARGS__)))

#define ZtEnumMap(ID, Map, ...) \
  struct Map : public Map_<Map> { \
    constexpr static const char *id() { return #ID; } \
    Map() { this->init(__VA_ARGS__, (const char *)0); } \
  }

#define ZtEnumFlagsMap(ID, Map, ...) \
  class Map : public Map_<Map> { \
  public: \
    constexpr static const char *id() { return #ID; } \
    Map() { this->init(__VA_ARGS__, (const char *)0); } \
  private: \
    template <typename S, typename Flags_> \
    unsigned print_(S &s, Flags_ v, char delim = '|') const { \
      if (!v) return 0; \
      bool first = true; \
      unsigned n = 0; \
      Flags_ mask = 1; \
      for (unsigned i = 0; i < N; i++, (mask <<= 1)) { \
	if (v & mask) { \
	  if (ZuString s_ = this->v2s(i)) { \
	    if (!first) s << delim; \
	    s << s_; \
	    first = false; \
	    n++; \
	  } \
	} \
      } \
      return n; \
    } \
    template <typename Flags_> \
    Flags_ scan_(ZuString s, char delim) const { \
      if (!s) return 0; \
      Flags_ v = 0; \
      bool end = false; \
      unsigned len = 0, clen = 0; \
      const char *cstr = s.data(), *next; \
      char c = 0; \
      Flags_ mask = 1; \
      do { \
	for (next = cstr; \
	    ++len <= s.length() && (c = *next) != 0 && c != delim; \
	    clen++, next++); \
	if (len > s.length() || c == 0) end = true; \
	ZtEnum i = this->s2v(ZuString(cstr, clen)); \
	if (ZuUnlikely(!*i)) return 0; \
	v |= (mask<<static_cast<unsigned>(i)); \
	cstr = ++next; \
	clen = 0; \
      } while (!end); \
      return v; \
    } \
  public: \
    template <typename Flags_> \
    static Flags_ scan(ZuString s, char delim = '|') { \
      return instance()->template scan_<Flags_>(s, delim); \
    } \
    template <typename Flags_> \
    struct Print : public ZuPrintable { \
      Print(Flags_ v_, char delim_ = '|') : v(v_), delim(delim_) { } \
      template <typename S> void print(S &s) const { \
	ZmSingleton<Map>::instance()->print_(s, v, delim); \
      } \
      const Flags_	v; \
      char		delim; \
    }; \
    template <typename Flags_> \
    static Print<Flags_> print(Flags_ v, char delim = '|') { \
      return Print<Flags_>{v, delim}; \
    } \
  };

#define ZtEnumFlag_(V) V##_
#define ZtEnumFlagLookup_(V) #V, V##_
#define ZtEnumFlagValue_(V) V = (1<<V##_)

#define ZtEnumFlags_(ID, ...) \
  ZtEnumValues_(ZuPP_Eval(ZuPP_MapComma(ZtEnumFlag_, __VA_ARGS__))); \
  enum { \
    ZuPP_Eval(ZuPP_MapComma(ZtEnumFlagValue_, __VA_ARGS__)), \
    Mask = (1<<N) - 1 \
  }

#define ZtEnumFlags(ID, ...) \
  ZtEnumFlags_(ID, __VA_ARGS__); \
  ZtEnumFlagsMap(ID, Map, \
      ZuPP_Eval(ZuPP_MapComma(ZtEnumFlagLookup_, __VA_ARGS__)))

#endif /* ZtEnum_HH */
