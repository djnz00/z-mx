//  -*- mode:c++; indent-tabs-mode:t; tab-width:8; c-basic-offset:2; -*-
//  vi: noet ts=8 sw=2 cino=+0,(s,l1,m1,g0,N-s,j1,U1,W2,i2

// (c) Copyright 2024 Psi Labs
// This code is licensed by the MIT license (see LICENSE for details)

// shard, handle
// ZmHandle is a special union of a ZmRef of a sharded object and a pointer
// to a shard; it can be used to specify a specific shard for deferred
// instantiation of a new object, or a reference to an existing instance

#ifndef ZmShard_HH
#define ZmShard_HH

#ifndef ZmLib_HH
#include <zlib/ZmLib.hh>
#endif

#include <zlib/ZmRef.hh>
#include <zlib/ZmScheduler.hh>

class ZmShard {
public:
  ZmShard(ZmScheduler *sched, unsigned tid) : m_sched(sched), m_tid(tid) { }

  ZmScheduler *sched() const { return m_sched; }
  unsigned tid() const { return m_tid; }

  template <typename... Args> void run(Args &&...args) const {
    m_sched->run(m_tid, ZuFwd<Args>(args)...);
  }
  template <typename... Args> void invoke(Args &&...args) const {
    m_sched->invoke(m_tid, ZuFwd<Args>(args)...);
  }

private:
  ZmScheduler *m_sched;
  unsigned m_tid;
};

template <class Shard_> class ZmSharded {
public:
  using Shard = Shard_;

  ZmSharded(Shard *shard) : m_shard(shard) { }

  Shard *shard() const { return m_shard; }

private:
  Shard		*m_shard;
};

template <typename T_> class ZmHandle {
public:
  using T = T_;
  using Shard = typename T::Shard;

private:
  // 64bit pointer-packing - uses bit 63
  constexpr static const uintptr_t Owned = (static_cast<uintptr_t>(1)<<63);

  constexpr static bool owned(uintptr_t o) { return o & Owned; }
  constexpr static uintptr_t own(uintptr_t o) { return o | Owned; }
  template <typename O>
  constexpr static uintptr_t own(O *o) {
    return own(reinterpret_cast<uintptr_t>(o));
  }
  constexpr static uintptr_t disown(uintptr_t o) { return o & ~Owned; }
  template <typename O = T>
  static O *ptr(uintptr_t o) {
    return reinterpret_cast<O *>(o & ~Owned);
  }
  template <typename O = T>
  static O *ptr_(uintptr_t o) {
    return reinterpret_cast<O *>(o);
  }

public:
  ZmHandle() : m_ptr(0) { }
  ZmHandle(Shard *shard) : m_ptr{reinterpret_cast<uintptr_t>(shard)} { }
  ZmHandle(T *o) : m_ptr{own(o)} { ZmREF(o); }
  ZmHandle(ZmRef<T> o) : m_ptr{own(o.release())} { }
  ~ZmHandle() { if (owned(m_ptr)) ZmDEREF(ptr(m_ptr)); }

  Shard *shard() const {
    if (!m_ptr) return nullptr;
    if (!owned(m_ptr)) return ptr_<Shard>(m_ptr);
    return ptr(m_ptr)->shard();
  }
  int shardID() const {
    Shard *shard = this->shard();
    return shard ? static_cast<int>(shard->id()) : -1;
  }
  T *object() const {
    return owned(m_ptr) ? ptr(m_ptr) : nullptr;
  }

  ZmHandle(const ZmHandle &h) : m_ptr(h.m_ptr) {
    if (owned(m_ptr)) ZmREF(ptr(m_ptr));
  }
  ZmHandle(ZmHandle &&h) : m_ptr{h.m_ptr} {
    if (owned(m_ptr)) {
      h.m_ptr = 0;
#ifdef ZmObject_DEBUG
      ZmMVREF(ptr(m_ptr), &h, this);
#endif
    }
  }
  ZmHandle &operator =(const ZmHandle &h) {
    if (ZuLikely(this != &h)) {
      if (owned(h.m_ptr)) ZmREF(ptr(h.m_ptr));
      if (owned(m_ptr)) ZmDEREF(ptr(m_ptr));
      m_ptr = h.m_ptr;
    }
    return *this;
  }
  ZmHandle &operator =(ZmHandle &&h) {
    if (owned(m_ptr)) ZmDEREF(ptr(m_ptr));
    m_ptr = h.m_ptr;
    if (owned(m_ptr)) {
      h.m_ptr = 0;
#ifdef ZmObject_DEBUG
      ZmMVREF(ptr(m_ptr), &h, this);
#endif
    }
    return *this;
  }

  ZmHandle &operator =(T *o) {
    if (owned(m_ptr)) ZmDEREF(ptr(m_ptr));
    ZmREF(o);
    m_ptr = own(o);
    return *this;
  }
  ZmHandle &operator =(const ZmRef<T> &o) {
    if (owned(m_ptr)) ZmDEREF(ptr(m_ptr));
    ZmREF(o);
    m_ptr = own(o.ptr());
    return *this;
  }
  ZmHandle &operator =(ZmRef<T> &&o) {
    if (owned(m_ptr)) ZmDEREF(ptr(m_ptr));
    m_ptr = own(o.release());
    return *this;
  }

  bool operator !() const { return !owned(m_ptr); }
  ZuOpBool

  template <typename L>
  void invoke(L l) const {
    T *o;
    Shard *shard;
    if (ZuUnlikely(!m_ptr)) {
      o = nullptr;
      shard = nullptr;
    } else if (ZuUnlikely(!owned(m_ptr))) {
      o = nullptr;
      shard = ptr_<Shard>(m_ptr);
    } else {
      o = ptr(m_ptr);
      shard = o->shard();
    }
    shard->invoke([l = ZuMv(l), shard, o]() mutable { l(shard, o); });
  }
  template <typename L>
  void invokeMv(L l) {
    if (ZuUnlikely(!m_ptr)) {
      l(static_cast<Shard *>(nullptr), static_cast<T *>(nullptr));
      return;
    }
    if (!owned(m_ptr)) {
      l(ptr_<Shard>(m_ptr), static_cast<T *>(nullptr));
      return;
    }
    auto o = ZmRef<T>::acquire(ptr(m_ptr));
    auto shard = o->shard();
    m_ptr = reinterpret_cast<uintptr_t>(shard);
    shard->invoke([l = ZuMv(l), shard, o = ZuMv(o)]() mutable {
      l(shard, ZuMv(o));
    });
  }

private:
  uintptr_t	m_ptr;
};

#endif /* ZmShard_HH */
