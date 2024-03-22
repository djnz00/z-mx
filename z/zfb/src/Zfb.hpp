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

// flatbuffers integration

#ifndef Zfb_HPP
#define Zfb_HPP

#ifdef _MSC_VER
#pragma once
#endif

#ifndef ZfbLib_HPP
#include <zlib/ZfbLib.hpp>
#endif

#include <flatbuffers/flatbuffers.h>

#include <stdlib.h>
#include <string.h>

#include <zlib/ZuID.hpp>

#include <zlib/ZuDecimal.hpp>

#include <zlib/ZmBitmap.hpp>
#include <zlib/ZmAlloc.hpp>

#include <zlib/ZtDate.hpp>

#include <zlib/ZePlatform.hpp>

#include <zlib/ZiPlatform.hpp>
#include <zlib/ZiIOBuf.hpp>
#include <zlib/ZiIP.hpp>

#include <zlib/types_fbs.h>

namespace Zfb {

using namespace flatbuffers;

using Builder = FlatBufferBuilder;

// IOBuilder customizes FlatBufferBuilder with an allocator that
// builds directly into a detachable IOBuf for transmission/persistence
template <typename IOBuf_ = ZiIOBuf<>>
class IOBuilder : public Allocator, public Builder {
public:
  enum { BufSize = IOBuf_::BufSize };
  enum { Align = 8 };
  using IOBuf = IOBuf_;

  IOBuilder() : Builder{BufSize & ~(Align - 1), this, false, Align} { }

  // attach buffer to builder
  void buf(ZmRef<IOBuf> buf) {
    buf->clear();
    m_buf = ZuMv(buf);
  }

  // detach buffer from builder
  ZmRef<IOBuf> buf() {
    if (ZuUnlikely(!m_buf)) return nullptr;
    auto buf = ZuMv(m_buf);
    size_t size, skip;
    ReleaseRaw(size, skip);
    buf->skip = skip;
    buf->length = size - skip;
    Clear();
    return buf;
  }

  // read buffer without detaching
  const IOBuf *cbuf() const { return m_buf.ptr(); }

protected:
  uint8_t *allocate(size_t size) {
    if (ZuLikely(!m_buf)) m_buf = new IOBuf{};
    return m_buf->alloc(size);
  }

  void deallocate(uint8_t *ptr, size_t size) {
    if (m_buf) m_buf->free(ptr);
  }

  // override ZiIOBuf's default grow() with a pass-through because flatbuffers
  // has it's own buffer growth algorithm in vector_downward::reallocate()
private:
  static unsigned grow(unsigned, unsigned n) { return n; }
protected:
  uint8_t *reallocate_downward(
      uint8_t *old_p, size_t old_size, size_t new_size,
      size_t in_use_back, size_t in_use_front) {
    return m_buf->template realloc<grow>(
	old_size, new_size, in_use_front, in_use_back);
  }

private:
  ZmRef<IOBuf>	m_buf;
};

namespace Save {
  // compile-time-recursive vector push
  template <typename T, typename I>
  inline void push_(T *, I) { }
  template <typename T, typename I, typename Arg0, typename ...Args>
  inline void push_(T *buf, I i, Arg0 &&arg0, Args &&... args) {
    buf[i] = ZuFwd<Arg0>(arg0);
    push_(buf, ZuUnsigned<i + 1>{}, ZuFwd<Args>(args)...);
  }

  // compile-time-recursive vector push, with a lambda map function
  template <typename T, typename L, typename I>
  inline void lpush_(T *, L, I) { }
  template <typename T, typename L, typename I, typename Arg0, typename ...Args>
  inline void lpush_(T *buf, L l, I i, Arg0 &&arg0, Args &&... args) {
    buf[i] = l(ZuFwd<Arg0>(arg0));
    lpush_(buf, ZuMv(l), ZuUnsigned<i + 1>{}, ZuFwd<Args>(args)...);
  }

  // push uninitialized vector
  template <typename Builder, typename T>
  inline Offset<Vector<T>> pvector_(Builder &fbb, unsigned length, T *&data) {
    return fbb.CreateUninitializedVector(
	length, sizeof(T), AlignOf<T>(), reinterpret_cast<uint8_t **>(&data));
  }
  // inline creation of a vector of primitive scalars
  template <typename T, typename Builder, typename ...Args>
  inline Offset<Vector<T>> pvector(Builder &fbb, Args &&... args) {
    auto n = ZuUnsigned<sizeof...(Args)>{};
    T *buf = nullptr;
    auto r = pvector_(fbb, n, buf);
    if (r.IsNull() || !buf) return {};
    lpush_(buf, [](T v) { return EndianScalar(v); },
	ZuUnsigned<0>{}, ZuFwd<Args>(args)...);
    return r;
  }
  // iterated creation of a vector of primitive values
  template <typename T, typename Builder, typename L>
  inline Offset<Vector<T>> pvectorIter(Builder &fbb, unsigned n, L l) {
    T *buf = nullptr;
    auto r = pvector_(fbb, n, buf);
    if (r.IsNull() || !buf) return {};
    for (unsigned i = 0; i < n; i++) buf[i] = EndianScalar<T>(l(i));
    return r;
  }

  // Note: CreateUninitializedVector() cannot be used for vectors
  // of offsets. flatbuffers offsets are always unsigned and positive,
  // and the vector must therefore be lower in memory than the referenced
  // entities. Since flatbuffers are written downwards in memory, the
  // vector must be written following the entities, and the offsets
  // collected in a temporary buffer while they are being written.

  // inline creation of a vector of offsets
  template <typename T, typename Builder, typename ...Args>
  inline Offset<Vector<Offset<T>>> vector(Builder &fbb, Args &&... args) {
    auto n = ZuUnsigned<sizeof...(Args)>{};
    auto buf = ZmAlloc(Offset<T>, n);
    if (!buf) return {};
    push_(buf.ptr, ZuUnsigned<0>{}, ZuFwd<Args>(args)...);
    auto r = fbb.CreateVector(buf.ptr, n);
    return r;
  }
  // inline creation of a vector of lambda-transformed offsets
  template <typename T, typename Builder, typename L, typename ...Args>
  inline Offset<Vector<Offset<T>>> lvector(Builder &fbb, L l, Args &&... args) {
    auto n = ZuUnsigned<sizeof...(Args)>{};
    auto buf = ZmAlloc(Offset<T>, n);
    if (!buf) return {};
    lpush_(buf.ptr, ZuMv(l), ZuUnsigned<0>{}, ZuFwd<Args>(args)...);
    auto r = fbb.CreateVector(buf.ptr, n);
    return r;
  }
  // iterated creation of a vector of offsets
  template <typename T, typename Builder, typename L>
  inline Offset<Vector<Offset<T>>> vectorIter(Builder &fbb, unsigned n, L l) {
    auto buf = ZmAlloc(Offset<T>, n);
    if (!buf) return {};
    for (unsigned i = 0; i < n; i++) buf.ptr[i] = l(fbb, i);
    auto r = fbb.CreateVector(buf.ptr, n);
    return r;
  }

  // iterated creation of a vector of structs
  template <typename T, typename Builder, typename L>
  inline Offset<Vector<const T *>> structVecIter(Builder &fbb, unsigned n, L l) {
    return fbb.template CreateVectorOfStructs<T>(n,
	[l = ZuMv(l)](size_t i, T *ptr, void *) {
      l(ptr, i);
    }, static_cast<void *>(nullptr));
  }

  // inline creation of a vector of keyed offsets
  template <typename T, typename Builder, typename ...Args>
  inline Offset<Vector<Offset<T>>> keyVec(Builder &fbb, Args &&... args) {
    auto n = ZuUnsigned<sizeof...(Args)>{};
    auto buf = ZmAlloc(Offset<T>, n);
    if (!buf) return {};
    push_(buf.ptr, ZuUnsigned<0>{}, ZuFwd<Args>(args)...);
    auto r = fbb.CreateVectorOfSortedTables(buf.ptr, n);
    return r;
  }
  // inline creation of a vector of lambda-transformed keyed offsets
  template <typename T, typename Builder, typename L, typename ...Args>
  inline Offset<Vector<Offset<T>>> lkeyVec(Builder &fbb, L l, Args &&... args) {
    auto n = ZuUnsigned<sizeof...(Args)>{};
    auto buf = ZmAlloc(Offset<T>, n);
    if (!buf) return {};
    lpush_(buf.ptr, ZuMv(l), ZuUnsigned<0>{}, ZuFwd<Args>(args)...);
    auto r = fbb.CreateVectorOfSortedTables(buf.ptr, n);
    return r;
  }
  // iterated creation of a vector of lambda-transformed keyed offsets
  template <typename T, typename Builder, typename L>
  inline Offset<Vector<Offset<T>>> keyVecIter(Builder &fbb, unsigned n, L l) {
    auto buf = ZmAlloc(Offset<T>, n);
    if (!buf) return {};
    for (unsigned i = 0; i < n; i++) buf[i] = l(fbb, i);
    auto r = fbb.CreateVectorOfSortedTables(buf.ptr, n);
    return r;
  }

  // inline creation of a string (shorthand alias for CreateString)
  template <typename Builder>
  inline auto str(Builder &fbb, ZuString s) {
    return fbb.CreateString(s.data(), s.length());
  }
  // fixed-width string -> flatbuffers::span<const uint8_t>
  template <unsigned N>
  inline auto strN(ZuString s) -> span<const uint8_t, N> {
    return {span<const uint8_t, N>{
      reinterpret_cast<const uint8_t *>(s.data()), N}};
  }

  // inline creation of a vector of strings
  template <typename Builder, typename ...Args>
  inline auto strVec(Builder &fbb, Args &&... args) {
    return lvector<String>(fbb, [&fbb](const auto &s) {
      return str(fbb, s);
    }, ZuFwd<Args>(args)...);
  }
  // iterated creation of a vector of strings
  template <typename Builder, typename L>
  inline auto strVecIter(Builder &fbb, unsigned n, L l) {
    return vectorIter<String>(fbb, n, [l = ZuMv(l)](Builder &fbb, unsigned i) mutable {
      return str(fbb, l(i));
    });
  }

  // inline creation of a vector of bytes from raw data
  template <typename Builder>
  inline auto bytes(Builder &fbb, const void *data, unsigned len) {
    return fbb.CreateVector(static_cast<const uint8_t *>(data), len);
  }
  // inline creation of a vector of bytes from raw data
  template <typename Builder>
  inline auto bytes(Builder &fbb, ZuArray<const uint8_t> a) {
    return fbb.CreateVector(a.data(), a.length());
  }

  // bitmap
  template <typename Builder>
  inline Offset<Bitmap> bitmap(Builder &fbb, const ZmBitmap &v) {
    int n = v.last();
    if (ZuUnlikely(n <= 0)) return {};
    n = (n + 0x3f)>>6;
    return CreateBitmap(fbb, pvectorIter<uint32_t>(fbb, n, [&v](unsigned i) {
      return hwloc_bitmap_to_ith_ulong(v, i);
    }));
  }

  // uint128
  inline auto uint128(const uint128_t &v) {
    return UInt128{
      static_cast<uint64_t>(v>>64),
      static_cast<uint64_t>(v)};
  }

  // int128
  inline auto int128(const int128_t &v) {
    return Int128{
      static_cast<uint64_t>(v>>64),
      static_cast<uint64_t>(v)};
  }

  // fixed
  inline auto fixed(const ZuFixed &v) {
    return Fixed{
      static_cast<int64_t>(v.mantissa()),
      static_cast<uint8_t>(v.exponent())};
  }

  // decimal
  inline auto decimal(const ZuDecimal &v) {
    return Decimal{
      static_cast<uint64_t>(v.value>>64),
      static_cast<uint64_t>(v.value)};
  }

  // date/time
  inline DateTime dateTime(const ZtDate &v) {
    return {v.julian(), v.sec(), v.nsec()};
  }

  // IP address
  inline IP ip(ZiIP addr) {
    return {span<const uint8_t, 4>{
      reinterpret_cast<const uint8_t *>(&addr.s_addr), 4}};
  }

  // ZuID
  inline ID id(ZuID id) {
    return {span<const uint8_t, 8>{
      reinterpret_cast<const uint8_t *>(id.data()), 8}};
  }

  // save file
  ZfbExtern int save(
      const Zi::Path &path, Builder &fbb, unsigned mode, ZeError *e);

  // nest flatbuffer - l(Builder &fbb) must return Offset<RootType>
  template <typename L>
  inline Offset<Vector<uint8_t>> nest(Builder &fbb, L l) {
    auto o = fbb.GetSize();
    uoffset_t root = l(fbb).o;
    fbb.PushElement(fbb.ReferTo(root));
    o = fbb.GetSize() - o;
    return {fbb.PushElement(o)};
  }

} // Save

namespace Load {
  // shorthand iteration over fastbuffer [T] vectors
  template <typename T, typename L>
  inline void all(T *v, L l) {
    if (ZuLikely(v))
      for (unsigned i = 0, n = v->size(); i < n; i++) l(i, v->Get(i));
  }

  // inline zero-copy conversion of a FB string to a ZuString
  inline ZuString str(const flatbuffers::String *s) {
    if (!s) return {};
    return {reinterpret_cast<const char *>(s->Data()), s->size()};
  }
  // inline zero-copy conversion of a fixed-width FB string to a ZuString
  template <unsigned N>
  inline ZuString strN(const flatbuffers::Array<uint8_t, N> *s) {
    if (!s) return {};
    auto data = reinterpret_cast<const char *>(s->Data());
    if (data[N-1]) return {data, N};
    return {data}; // deferred strlen
  }

  // inline zero-copy conversion of a [uint8] to a ZuArray<const uint8_t>
  inline ZuArray<const uint8_t> bytes(const Vector<uint8_t> *v) {
    if (!v) return {};
    return {v->data(), v->size()};
  }

  // bitmap
  inline ZmBitmap bitmap(const Vector<uint64_t> *v) {
    if (!v) return ZmBitmap{};
    ZmBitmap m;
    if (unsigned n = v->size()) {
      --n;
      hwloc_bitmap_from_ith_ulong(m, n, (*v)[n]);
      while (n--) hwloc_bitmap_set_ith_ulong(m, n, (*v)[n]);
    }
    return m;
  }

  // uint128
  inline uint128_t uint128(const UInt128 *v) {
    return (static_cast<uint128_t>(v->h())<<64) | v->l();
  }

  // int128
  inline int128_t int128(const Int128 *v) {
    return (static_cast<int128_t>(v->h())<<64) | v->l();
  }

  // fixed
  inline ZuFixed fixed(const Fixed *v) {
    return ZuFixed{v->mantissa(), v->exponent()};
  }

  // decimal
  inline ZuDecimal decimal(const Decimal *v) {
    return ZuDecimal{ZuDecimal::Unscaled,
      (static_cast<int128_t>(v->h())<<64) | v->l()};
  }

  // date/time
  inline auto dateTime(const DateTime *v) {
    return ZtDate{ZtDate::Julian, v->julian(), v->sec(), v->nsec()};
  }

  // IP address
  inline ZiIP ip(const IP *v) {
    struct in_addr addr;
    addr.s_addr = *reinterpret_cast<const uint32_t *>(v->addr()->data());
    return ZiIP{ZuMv(addr)};
  }

  // ZuID
  inline ZuID id(const ID *v) {
    if (!v) return {};
    return {*reinterpret_cast<const uint64_t *>(v->data()->data())};
  }

  // load file
  using LoadFn = ZmFn<const uint8_t *, unsigned>;
  ZfbExtern int load(
      const Zi::Path &path, LoadFn, unsigned maxSize, ZeError *e);

} // Load

} // Zfb

#define ZfbEnum_Value(Enum, Value) Value = fbs::Enum##_##Value,
#define ZfbEnumValues(Enum, ...) \
  enum _ { Invalid = -1, \
    ZuPP_Eval(ZuPP_MapArg(ZfbEnum_Value, Enum, \
	  __VA_ARGS__ __VA_OPT__(,) MIN, MAX)) \
    N = fbs::Enum##_MAX + 1 }; \
  ZuAssert(N <= 1024); \
  enum { Bits = \
    N <= 2 ? 1 : N <= 4 ? 2 : N <= 8 ? 3 : N <= 16 ? 4 : N <= 32 ? 5 : \
    N <= 64 ? 6 : N <= 128 ? 7 : N <= 256 ? 8 : N <= 512 ? 9 : 10 \
  }; \
  template <typename T> struct Map_ : public ZuObject { \
  private: \
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
    void add(ZuString s, ZtEnum v) { m_s2v->add(s, v); } \
  private: \
    ZtEnum s2v_(ZuString s) const { return m_s2v->findVal(s); } \
    template <typename L> void all_(L l) const { \
      auto i = m_s2v->readIterator(); \
      while (auto kv = i.iterate()) { \
	l(S2V::KeyAxor(*kv), S2V::ValAxor(*kv)); \
      } \
    } \
  public: \
    constexpr static const char *id() { return #Enum; } \
    using Buffer = fbs::Enum; \
    Map_() { m_s2v = new S2V(); } \
    static T *instance() { return ZmSingleton<T>::instance(); } \
    static ZuString v2s(int v) { \
      return fbs::EnumName##Enum(static_cast<Buffer>(v)); \
    } \
    static ZtEnum s2v(ZuString s) { return instance()->s2v_(s); } \
    template <typename L> static void all(L l) { instance()->all_(ZuMv(l)); } \
  private: \
    ZmRef<S2V>	m_s2v; \
  }; \
  const char *name(int i) { \
    return fbs::EnumName##Enum(static_cast<fbs::Enum>(i)); \
  } \
  struct Map : public Map_<Map> { \
    Map() { for (unsigned i = 0; i < N; i++) this->add(name(i), i); } \
  }; \
  template <typename S> ZtEnum lookup(const S &s) { \
    return Map::s2v(s); \
  }
#define ZfbEnumMatch_Assert(Namespace, Value) \
  ZuAssert(Value == Namespace::Value);
#define ZfbEnumMatch(Enum, Namespace, ...) \
  ZfbEnumValues(Enum, __VA_ARGS__) \
  ZuPP_Eval(ZuPP_MapArg(ZfbEnumMatch_Assert, Namespace, __VA_ARGS__))
#define ZfbEnum_Type(T) fbs::T
#define ZfbEnum_Assert(T) \
  ZuAssert(static_cast<int>(T) == static_cast<int>(TypeIndex<fbs::T>{}));
#define ZfbEnumUnion(ID, First_, ...) \
  ZfbEnumValues(ID, First_, __VA_ARGS__) \
  enum { First = First_ }; \
  using TypeList = ZuTypeList< \
    ZuPP_Eval(ZuPP_MapComma(ZfbEnum_Type, First_, __VA_ARGS__))>; \
  template <unsigned I> \
  using Type = ZuType<I - First, TypeList>; \
  template <typename T> \
  using TypeIndex = ZuUnsigned<ZuTypeIndex<T, TypeList>{} + First>; \
  ZuPP_Eval(ZuPP_Map(ZfbEnum_Assert, First_, __VA_ARGS__)) \
  ZuAssert( \
    static_cast<int>(First) == static_cast<int>(TypeIndex<Type<First>>{})); \
  ZuAssert(static_cast<int>(MAX) == static_cast<int>(TypeIndex<Type<MAX>>{}));

#endif /* Zfb_HPP */
